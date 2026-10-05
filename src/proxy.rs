use crate::cfproxy::*;
use crate::config::*;
use crate::crypto::*;
use crate::ws::*;
use crate::{ldebug, linfo, lwarn};
use byteorder::{ByteOrder, LittleEndian};
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Target resolution
// ---------------------------------------------------------------------------

pub fn resolve_configured_target(dc: i32, is_media: bool) -> Option<String> {
    let map = DC_OPT.read();
    if is_media {
        if let Some(t) = map.get(&(-dc)) {
            if !t.is_empty() {
                return Some(t.clone());
            }
        }
    }
    if let Some(t) = map.get(&dc) {
        if !t.is_empty() {
            return Some(t.clone());
        }
    }
    None
}

pub fn resolve_fallback_target(dc: i32, _is_media: bool) -> String {
    DC_DEFAULT_IPS
        .get(&dc)
        .map(|s| s.to_string())
        .unwrap_or_default()
}

pub fn ws_domains(dc: i32, is_media: bool) -> Vec<String> {
    let mut effective_dc = dc;
    if let Some(o) = DC_OVERRIDES.get(&dc) {
        effective_dc = *o;
    }
    if is_media {
        vec![
            format!("kws{}-1.web.telegram.org", effective_dc),
            format!("kws{}.web.telegram.org", effective_dc),
        ]
    } else {
        // 非媒体统一走 zws1-1 网关（zws 不受 kws 的 DC 绑定限制，
        // 内层 MTProto 自带 dc_id，zws 网关按 dc_id 路由到对应 DC）
        vec![
            "zws1-1.web.telegram.org".to_string(),
            "zws1.web.telegram.org".to_string(),
        ]
    }
}

pub fn media_tag(is_media: bool) -> &'static str {
    if is_media {
        "m"
    } else {
        ""
    }
}

pub fn is_media_int(b: bool) -> i32 {
    if b {
        1
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// WsPool
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct DcSlot {
    pub dc: i32,
    pub is_media: i32,
}

pub struct PoolEntry {
    pub ws: RawWebSocket,
    pub created: i64,
}

struct SlotState {
    queue: Mutex<std::collections::VecDeque<PoolEntry>>,
    refilling: AtomicI32,
}

pub struct WsPool {
    slots: Mutex<HashMap<DcSlot, Arc<SlotState>>>,
    cancel_token: CancellationToken,
}

impl WsPool {
    pub fn new(cancel_token: CancellationToken) -> WsPool {
        WsPool {
            slots: Mutex::new(HashMap::new()),
            cancel_token,
        }
    }

    async fn get_slot(&self, slot: DcSlot) -> Arc<SlotState> {
        let mut map = self.slots.lock().await;
        map.entry(slot)
            .or_insert_with(|| {
                Arc::new(SlotState {
                    queue: Mutex::new(std::collections::VecDeque::with_capacity(16)),
                    refilling: AtomicI32::new(0),
                })
            })
            .clone()
    }

    pub async fn get(
        self: &Arc<Self>,
        dc: i32,
        is_media: bool,
        target_ip: String,
        domains: Vec<String>,
    ) -> Option<RawWebSocket> {
        let slot = DcSlot {
            dc,
            is_media: is_media_int(is_media),
        };
        let state = self.get_slot(slot).await;
        let now = now_unix();

        let mut ws: Option<RawWebSocket> = None;
        {
            let mut q = state.queue.lock().await;
            loop {
                match q.pop_front() {
                    Some(entry) => {
                        if is_pool_entry_usable(&entry, now) {
                            ws = Some(entry.ws);
                            STATS.pool_hits.fetch_add(1, Ordering::Relaxed);
                            break;
                        } else {
                            let e = entry;
                            tokio::spawn(async move {
                                e.ws.close().await;
                            });
                            continue;
                        }
                    }
                    None => {
                        STATS.pool_misses.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }

        if state
            .refilling
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let pool = self.clone();
            let st = state.clone();
            tokio::spawn(async move {
                pool.refill(st, target_ip, domains).await;
            });
        }

        ws
    }

    async fn refill(
        self: Arc<Self>,
        state: Arc<SlotState>,
        target_ip: String,
        domains: Vec<String>,
    ) {
        let cur_len = state.queue.lock().await.len();
        let needed = POOL_SIZE.load(Ordering::Relaxed) as usize;
        let needed = needed.saturating_sub(cur_len);
        if needed == 0 {
            state.refilling.store(0, Ordering::SeqCst);
            return;
        }

        let mut handles = Vec::new();
        for _ in 0..needed {
            let target_ip = target_ip.clone();
            let domains = domains.clone();
            let cancel = self.cancel_token.clone();
            handles.push(tokio::spawn(async move {
                tokio::select! {
                    _ = cancel.cancelled() => None,
                    r = connect_one_ws(&target_ip, &domains) => r,
                }
            }));
        }

        for h in handles {
            if let Ok(Some(ws)) = h.await {
                let now = now_unix();
                let mut q = state.queue.lock().await;
                if q.len() < 16 {
                    q.push_back(PoolEntry { ws, created: now });
                } else {
                    drop(q);
                    let ws = ws;
                    tokio::spawn(async move {
                        ws.close().await;
                    });
                }
            }
        }

        state.refilling.store(0, Ordering::SeqCst);
    }

    pub async fn warmup(self: &Arc<Self>, dc_opt_map: &HashMap<i32, String>) {
        for (dc, target_ip) in dc_opt_map {
            if target_ip.is_empty() {
                continue;
            }
            for is_media in [false, true] {
                let domains = ws_domains(*dc, is_media);
                let slot = DcSlot {
                    dc: *dc,
                    is_media: is_media_int(is_media),
                };
                let state = self.get_slot(slot).await;
                if state
                    .refilling
                    .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    let pool = self.clone();
                    let st = state.clone();
                    let ip = target_ip.clone();
                    let doms = domains.clone();
                    tokio::spawn(async move {
                        pool.refill(st, ip, doms).await;
                    });
                }
            }
        }
    }

    pub async fn idle_count(&self) -> usize {
        let map = self.slots.lock().await;
        let mut count = 0;
        for s in map.values() {
            count += s.queue.lock().await.len();
        }
        count
    }

    pub async fn close_all(&self) {
        let map = self.slots.lock().await;
        for s in map.values() {
            let mut q = s.queue.lock().await;
            for e in q.drain(..) {
                tokio::spawn(async move {
                    e.ws.close().await;
                });
            }
        }
    }
}

fn is_pool_entry_usable(e: &PoolEntry, now: i64) -> bool {
    if e.ws.is_closed() {
        return false;
    }
    if now - e.created > WS_POOL_REUSE_MAX_AGE as i64 {
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// HTTP transport detection
// ---------------------------------------------------------------------------

pub fn is_http_transport(data: &[u8]) -> bool {
    if data.len() < 4 {
        return false;
    }
    &data[..4] == b"POST"
        || &data[..3] == b"GET"
        || &data[..4] == b"HEAD"
        || (data.len() >= 7 && &data[..7] == b"OPTIONS")
}

/// TLS ClientHello 分片（绕过 SNI 检测）：record 前 96 字节 2 字节/片，其余 8 字节/片
fn split_clienthello(data: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    if data.len() >= 5 && data[0] == 0x16 && data[1] == 0x03 {
        let rec_len = ((data[3] as usize) << 8) | data[4] as usize;
        let total = (5 + rec_len).min(data.len());
        let head = &data[..total];
        let mut i = 0;
        while i < head.len() {
            let n = if i < 96 { 2 } else { 8 };
            let end = (i + n).min(head.len());
            out.push(head[i..end].to_vec());
            i = end;
        }
        if total < data.len() {
            out.push(data[total..].to_vec());
        }
    } else {
        out.push(data.to_vec());
    }
    out
}

/// 域名/IP → 直连 IP（同大佬方案 hosts 规则）
/// TG 官方 DC 段（149.154.0.0/16 等）→ 161.145（无 SNI 时是 MTProxy 后端，吃 MTProto 混淆）
fn http_host_to_ip(host: &str) -> String {
    let h = host.trim_start_matches('*').trim().trim_end_matches('.').to_lowercase();
    if h.ends_with("web.telegram.org") || h.starts_with("zws") || h.starts_with("kws") {
        return "149.154.170.200".to_string();
    }
    if h.ends_with("telegram.org") || h.ends_with(".t.me") || h == "t.me" {
        return "149.154.161.145".to_string();
    }
    if h.starts_with("149.154.")
        || h.starts_with("91.108.")
        || h.starts_with("185.76.151.")
        || h.parse::<std::net::Ipv4Addr>().is_ok()
    {
        return "149.154.161.145".to_string();
    }
    crate::config::ZWS_DEFAULT_IP.to_string()
}

/// 双向透传 + 上行 TLS 分片（纯隧道模式共用）
async fn tunnel_split(mut conn: TcpStream, mut up: TcpStream) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let _ = up.set_nodelay(true);
    let (cr, cw) = tokio::io::split(conn);
    let (ur, uw) = tokio::io::split(up);
    let up_task = tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(cr);
        let mut w = uw;
        let mut first = true;
        loop {
            let mut b = [0u8; 16384];
            match tokio::time::timeout(Duration::from_secs(120), reader.read(&mut b)).await {
                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                Ok(Ok(n)) => {
                    if first {
                        for c in split_clienthello(&b[..n]) {
                            if w.write_all(&c).await.is_err() {
                                return;
                            }
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                        first = false;
                    } else if w.write_all(&b[..n]).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    let down_task = tokio::spawn(async move {
        let mut r = ur;
        let mut w = cw;
        loop {
            let mut b = [0u8; 16384];
            match tokio::time::timeout(Duration::from_secs(120), r.read(&mut b)).await {
                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                Ok(Ok(n)) => {
                    if w.write_all(&b[..n]).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    let _ = tokio::join!(up_task, down_task);
}

/// SOCKS5 纯隧道（TG 手机端支持）：05 握手 → CONNECT → 域名/IP 直连 → 透传+分片
async fn handle_socks5(mut conn: TcpStream, mut buf: Vec<u8>, cancel_token: CancellationToken) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn read_more(conn: &mut TcpStream, buf: &mut Vec<u8>, need: usize) -> bool {
        while buf.len() < need {
            let mut tmp = [0u8; 512];
            match tokio::time::timeout(Duration::from_secs(10), conn.read(&mut tmp)).await {
                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => return false,
                Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
            }
        }
        true
    }
    if !read_more(&mut conn, &mut buf, 2).await || buf[0] != 0x05 {
        return;
    }
    let nmethods = buf[1] as usize;
    if !read_more(&mut conn, &mut buf, 2 + nmethods).await {
        return;
    }
    if conn.write_all(&[0x05, 0x00]).await.is_err() {
        return;
    }
    let mut req = buf[2 + nmethods..].to_vec();
    if !read_more(&mut conn, &mut req, 4).await || req[0] != 0x05 || req[1] != 0x01 {
        let _ = conn.write_all(&[0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0]).await;
        return;
    }
    let atyp = req[3];
    let (host, need): (String, usize) = match atyp {
        0x01 => {
            if !read_more(&mut conn, &mut req, 10).await { return; }
            let ip = format!("{}.{}.{}.{}", req[4], req[5], req[6], req[7]);
            (ip, 10)
        }
        0x03 => {
            let l = req[4] as usize;
            if !read_more(&mut conn, &mut req, 5 + l + 2).await { return; }
            let host = String::from_utf8_lossy(&req[5..5 + l]).to_string();
            (host, 5 + l + 2)
        }
        0x04 => {
            if !read_more(&mut conn, &mut req, 22).await { return; }
            let mut ip6: Vec<String> = Vec::new();
            for i in 0..8 {
                ip6.push(format!("{:x}", ((req[4 + i * 2] as u16) << 8) | req[5 + i * 2] as u16));
            }
            (ip6.join(":"), 22)
        }
        _ => {
            let _ = conn.write_all(&[0x05, 0x08, 0x00, 0x01, 0, 0, 0, 0, 0, 0]).await;
            return;
        }
    };
    let port = ((req[need - 2] as u16) << 8) | req[need - 1] as u16;
    let ip = http_host_to_ip(&host);
    linfo!(" SOCKS5 CONNECT {}:{} -> {}（纯隧道+分片）", host, port, ip);
    let up = match tokio::time::timeout(
        Duration::from_secs(8),
        TcpStream::connect(format!("{}:{}", ip, port)),
    )
    .await
    {
        Ok(Ok(c)) => c,
        _ => {
            let _ = conn.write_all(&[0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0]).await;
            return;
        }
    };
    if conn
        .write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await
        .is_err()
    {
        return;
    }
    tunnel_split(conn, up).await;
    let _ = cancel_token;
}

/// HTTP CONNECT 纯隧道：TG 客户端原生 TLS/MTProto 原样直通，TLS 分片绕 SNI 检测
async fn handle_http_connect(
    mut conn: TcpStream,
    head_buf: &mut Vec<u8>,
    cancel_token: CancellationToken,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // 读满请求头
    while !head_buf.windows(4).any(|w| w == b"\r\n\r\n") && head_buf.len() < 8192 {
        let mut tmp = [0u8; 1024];
        match tokio::time::timeout(Duration::from_secs(10), conn.read(&mut tmp)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => {
                let _ = conn.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
                return;
            }
            Ok(Ok(n)) => head_buf.extend_from_slice(&tmp[..n]),
        }
    }
    let head_str = String::from_utf8_lossy(head_buf);
    let first_line = head_str.lines().next().unwrap_or("").trim();
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    if parts.len() < 3 || parts[0].to_uppercase() != "CONNECT" {
        let _ = conn.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
        return;
    }
    let target = parts[1];
    let (host, port_str) = target.rsplit_once(':').unwrap_or((target, "443"));
    let port: u16 = port_str.parse().unwrap_or(443);
    let ip = http_host_to_ip(host);
    linfo!(" HTTP CONNECT {}:{} -> {}（纯隧道+分片）", host, port, ip);

    let up = match tokio::time::timeout(
        Duration::from_secs(8),
        TcpStream::connect(format!("{}:{}", ip, port)),
    )
    .await
    {
        Ok(Ok(c)) => c,
        _ => {
            let _ = conn.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
            return;
        }
    };
    if conn.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n").await.is_err() {
        return;
    }
    tunnel_split(conn, up).await;
}

// ---------------------------------------------------------------------------
// Bridge WS
// ---------------------------------------------------------------------------

pub async fn bridge_ws(
    conn: TcpStream,
    ws: RawWebSocket,
    _label: String,
    _dc: i32,
    _dst: String,
    _port: u16,
    _is_media: bool,
    mut splitter: Option<MsgSplitter>,
    mut clt_dec: TrackedStream,
    mut clt_enc: TrackedStream,
    mut tg_enc: TrackedStream,
    mut tg_dec: TrackedStream,
    cancel_token: CancellationToken,
    raw: bool,
) {
    let ws = Arc::new(ws);
    let last_activity = Arc::new(Mutex::new(std::time::Instant::now()));
    let cancel = Arc::new(tokio::sync::Notify::new());

    let (mut conn_read, mut conn_write) = conn.into_split();

    // ping keepalive
    let ws_ping = ws.clone();
    let la_ping = last_activity.clone();
    let cancel_ping = cancel.clone();
    let cancel_token_ping = cancel_token.clone();
    let ping_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(BRIDGE_PING_INTERVAL);
        interval.tick().await;
        loop {
            tokio::select! {
                _ = cancel_token_ping.cancelled() => return,
                _ = cancel_ping.notified() => return,
                _ = interval.tick() => {
                    let idle = la_ping.lock().await.elapsed();
                    if idle > BRIDGE_PING_INTERVAL {
                        if ws_ping.send_ping().await.is_err() {
                            cancel_ping.notify_waiters();
                            return;
                        }
                    }
                }
            }
        }
    });

    // up: client -> ws
    let ws_up = ws.clone();
    let la_up = last_activity.clone();
    let cancel_up = cancel.clone();
    let cancel_token_up = cancel_token.clone();
    let up_task = tokio::spawn(async move {
        let mut buf = vec![0u8; WS_BRIDGE_CHUNK_SIZE];
        let mut up_cnt: u64 = 0;
        let mut up_bytes: u64 = 0;
        loop {
            let read_res = tokio::select! {
                _ = cancel_token_up.cancelled() => break,
                _ = cancel_up.notified() => break,
                r = tokio::time::timeout(BRIDGE_READ_TIMEOUT, conn_read.read(&mut buf)) => r,
            };
            let n = match read_res {
                Ok(Ok(0)) => {
                    // EOF: flush splitter tail（裸隧道无 splitter，直接断开）
                    if !raw {
                        if let Some(sp) = splitter.as_mut() {
                            let tail = sp.flush();
                            if !tail.is_empty() {
                                let r = if tail.len() > 1 {
                                    ws_up.send_batch(&tail).await
                                } else {
                                    ws_up.send(&tail[0]).await
                                };
                                if r.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    break;
                }
                Ok(Ok(n)) => n,
                Ok(Err(_)) => break,
                Err(_) => break, // read timeout
            };

            let chunk = &mut buf[..n];
            STATS.bytes_up.fetch_add(n as i64, Ordering::Relaxed);
            *la_up.lock().await = std::time::Instant::now();

            if raw {
                // 裸隧道：纯字节透传，不加解密
                if ws_up.send(chunk).await.is_err() {
                    break;
                }
                continue;
            }

            clt_dec.xor(chunk);
            tg_enc.xor(chunk);

            let send_err = {
                if let Some(sp) = splitter.as_mut() {
                    let parts = sp.split(chunk);
                    if parts.len() > 1 {
                        ws_up.send_batch(&parts).await.is_err()
                    } else if parts.len() == 1 {
                        ws_up.send(&parts[0]).await.is_err()
                    } else {
                        false
                    }
                } else {
                    ws_up.send(chunk).await.is_err()
                }
            };
            if send_err {
                break;
            }
            up_cnt += 1;
            up_bytes += n as u64;
            if up_cnt % 64 == 0 {
                linfo!(" 上行包={} 字节={}KB", up_cnt, up_bytes / 1024);
            }
        }
        cancel_up.notify_waiters();
    });

    // down: ws -> client
    let ws_down = ws.clone();
    let la_down = last_activity.clone();
    let cancel_down = cancel.clone();
    let cancel_token_down = cancel_token.clone();
    let down_task = tokio::spawn(async move {
        let mut dn_cnt: u64 = 0;
        let mut dn_bytes: u64 = 0;
        loop {
            let recv_res = tokio::select! {
                _ = cancel_token_down.cancelled() => break,
                _ = cancel_down.notified() => break,
                r = ws_down.recv_with_timeout(BRIDGE_READ_TIMEOUT) => r,
            };
            let mut data = match recv_res {
                Ok(d) => d,
                Err(_) => break,
            };
            let n = data.len();
            STATS.bytes_down.fetch_add(n as i64, Ordering::Relaxed);
            *la_down.lock().await = std::time::Instant::now();

            if raw {
                // 裸隧道：原样回写客户端
                if conn_write.write_all(&data).await.is_err() {
                    break;
                }
                continue;
            }

            tg_dec.xor(&mut data);
            clt_enc.xor(&mut data);
            if conn_write.write_all(&data).await.is_err() {
                break;
            }
            dn_cnt += 1;
            dn_bytes += n as u64;
            if dn_cnt % 64 == 0 {
                linfo!(" 下行包={} 字节={}KB", dn_cnt, dn_bytes / 1024);
            }
        }
        cancel_down.notify_waiters();
    });

    let _ = up_task.await;
    let _ = down_task.await;
    cancel.notify_waiters();
    ping_task.abort();

    ws.close().await;
}

// ---------------------------------------------------------------------------
// Bridge TCP
// ---------------------------------------------------------------------------

pub async fn bridge_tcp(
    mut client: TcpStream,
    mut remote: TcpStream,
    _label: String,
    _dc: i32,
    _dst: String,
    _port: u16,
    _is_media: bool,
    clt_dec: TrackedStream,
    clt_enc: TrackedStream,
    tg_enc: TrackedStream,
    tg_dec: TrackedStream,
    cancel_token: CancellationToken,
) {
    let (mut c_read, mut c_write) = client.split();
    let (mut r_read, mut r_write) = remote.split();

    let clt_dec = Arc::new(Mutex::new(clt_dec));
    let clt_enc = Arc::new(Mutex::new(clt_enc));
    let tg_enc = Arc::new(Mutex::new(tg_enc));
    let tg_dec = Arc::new(Mutex::new(tg_dec));

    let cancel = Arc::new(tokio::sync::Notify::new());

    let cancel_up = cancel.clone();
    let cancel_token_up = cancel_token.clone();
    let clt_dec_up = clt_dec.clone();
    let tg_enc_up = tg_enc.clone();
    let up = async move {
        let mut buf = vec![0u8; 131072];
        loop {
            let n = tokio::select! {
                _ = cancel_token_up.cancelled() => break,
                _ = cancel_up.notified() => break,
                r = tokio::time::timeout(BRIDGE_READ_TIMEOUT, c_read.read(&mut buf)) => match r {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => n,
                    _ => break,
                },
            };
            let chunk = &mut buf[..n];
            STATS.bytes_up.fetch_add(n as i64, Ordering::Relaxed);
            clt_dec_up.lock().await.xor(chunk);
            tg_enc_up.lock().await.xor(chunk);
            if r_write.write_all(chunk).await.is_err() {
                break;
            }
        }
        cancel_up.notify_waiters();
    };

    let cancel_down = cancel.clone();
    let cancel_token_down = cancel_token.clone();
    let tg_dec_down = tg_dec.clone();
    let clt_enc_down = clt_enc.clone();
    let down = async move {
        let mut buf = vec![0u8; 131072];
        loop {
            let n = tokio::select! {
                _ = cancel_token_down.cancelled() => break,
                _ = cancel_down.notified() => break,
                r = tokio::time::timeout(BRIDGE_READ_TIMEOUT, r_read.read(&mut buf)) => match r {
                    Ok(Ok(0)) => break,
                    Ok(Ok(n)) => n,
                    _ => break,
                },
            };
            let chunk = &mut buf[..n];
            STATS.bytes_down.fetch_add(n as i64, Ordering::Relaxed);
            tg_dec_down.lock().await.xor(chunk);
            clt_enc_down.lock().await.xor(chunk);
            if c_write.write_all(chunk).await.is_err() {
                break;
            }
        }
        cancel_down.notify_waiters();
    };

    tokio::join!(up, down);
    let _ = (clt_dec, clt_enc, tg_enc, tg_dec);
}

// ---------------------------------------------------------------------------
// TCP fallback
// ---------------------------------------------------------------------------

pub async fn tcp_fallback(
    client: TcpStream,
    dst: &str,
    port: u16,
    init: &[u8],
    label: String,
    dc: i32,
    is_media: bool,
    clt_dec: TrackedStream,
    clt_enc: TrackedStream,
    tg_enc: TrackedStream,
    tg_dec: TrackedStream,
    cancel_token: CancellationToken,
) -> bool {
    let addr = format!("{}:{}", dst, port);
    let mut remote =
        match tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(&addr)).await {
            Ok(Ok(r)) => r,
            _ => return false,
        };
    let _ = remote.set_nodelay(true);

    STATS.connections_tcp_fallback.fetch_add(1, Ordering::Relaxed);
    linfo!(" DC{}{} 已通过 TCP 连接", dc, media_tag(is_media));
    if remote.write_all(init).await.is_err() {
        return false;
    }
    bridge_tcp(
        client,
        remote,
        label,
        dc,
        dst.to_string(),
        port,
        is_media,
        clt_dec,
        clt_enc,
        tg_enc,
        tg_dec,
        cancel_token,
    )
    .await;
    true
}

// ---------------------------------------------------------------------------
// Cfproxy fallback
// ---------------------------------------------------------------------------

async fn try_cfproxy_base_domain(dc: i32, base_domain: &str) -> (Option<RawWebSocket>, String) {
    let base_domain = if CFPROXY.read().user_domain == base_domain {
        normalize_user_cf_domain(base_domain)
    } else {
        normalize_builtin_cf_domain(base_domain)
    };
    if base_domain.is_empty() {
        return (None, String::new());
    }
    let remaining = cfproxy_429_cooldown_remaining(&base_domain);
    if remaining > Duration::ZERO {
        ldebug!(
            " CF skip {}: 429 cooldown {:.0}s",
            base_domain,
            remaining.as_secs_f64().ceil()
        );
        return (None, String::new());
    }
    let _permit = match acquire_cfproxy_attempt_slot().await {
        Some(p) => p,
        None => return (None, String::new()),
    };

    let domain = format!("kws{}.{}", dc, base_domain);
    ldebug!(" CF try {}", domain);

    let (ws, resolved_ip, err) = cf_connect_domain(&domain, "/apiws", 5.0).await;
    if let Some(e) = err {
        // 重要（同 Go 实现）：仅在 HTTP 429 时设置冷却，否则
        // 任何 reset/超时都会烧掉域名并引发冷却雪崩。
        if is_http_status_error(&e, 429) {
            mark_cfproxy_429_cooldown(&base_domain, &e);
        }
        if !resolved_ip.is_empty() {
            log_cf_conn_error(
                &format!(" CF fail {} via {}: {}", domain, resolved_ip, e.compact()),
                &e,
            );
        } else {
            log_cf_conn_error(&format!(" CF fail {}: {}", domain, e.compact()), &e);
        }
        return (None, String::new());
    }

    clear_cfproxy_429_cooldown(&base_domain);
    if !resolved_ip.is_empty() {
        ldebug!(" CF ok {} via {}", domain, resolved_ip);
    } else {
        ldebug!(" CF ok {} via hostname", domain);
    }
    (ws, base_domain)
}

// 仅通过 CF 建立 WS 连接，不动 conn。
// 返回 (ws, chosen_domain)。这样 CF 失败时
// 可用同一 conn 做 TCP 回退（语义与 Go 一致）。
async fn cfproxy_acquire_ws(
    dc: i32,
    is_media: bool,
    cancel_token: &CancellationToken,
) -> Option<(RawWebSocket, String)> {
    let (enabled, _active, domains) = {
        let cfg = CFPROXY.read();
        (
            CFPROXY_ENABLED.load(Ordering::Relaxed),
            cfg.active.clone(),
            cfg.domains.clone(),
        )
    };
    if !enabled || domains.is_empty() {
        return None;
    }

    let ordered = crate::balancer::BALANCER.read().get_domains_for_dc(dc);
    if ordered.is_empty() {
        return None;
    }

    let m_tag = media_tag(is_media);
    ldebug!(" CF 回退 DC{}{}: {} 个域名", dc, m_tag, ordered.len());

    let mut ws: Option<RawWebSocket> = None;
    let mut chosen_domain = String::new();

    if !ordered.is_empty() && !ordered[0].is_empty() {
        let (w, d) = try_cfproxy_base_domain(dc, &ordered[0]).await;
        ws = w;
        chosen_domain = d;
    }

    if ws.is_none() && ordered.len() > 1 {
        let remaining_domains: Vec<String> = ordered[1..].to_vec();
        let sem = Arc::new(tokio::sync::Semaphore::new(CFPROXY_FALLBACK_PARALLEL));
        let mut handles = Vec::new();
        for bd in remaining_domains {
            let sem = sem.clone();
            let cancel = cancel_token.clone();
            handles.push(tokio::spawn(async move {
                tokio::select! {
                    _ = cancel.cancelled() => None,
                    r = async {
                        let _p = sem.acquire().await.ok()?;
                        let (w, d) = try_cfproxy_base_domain(dc, &bd).await;
                        w.map(|ws| (ws, d))
                    } => r,
                }
            }));
        }
        for h in handles {
            if let Ok(Some((w, d))) = h.await {
                if ws.is_none() {
                    ws = Some(w);
                    chosen_domain = d;
                } else {
                    tokio::spawn(async move {
                        w.close().await;
                    });
                }
            }
        }
    }

    match ws {
        Some(w) => {
            if !chosen_domain.is_empty() {
                if crate::balancer::BALANCER.write().update_domain_for_dc(dc, &chosen_domain) {
                    linfo!(" CF 域名 DC{} -> {}", dc, chosen_domain);
                }
            }
            Some((w, chosen_domain))
        }
        None => {
            lwarn!(" CF 回退 DC{}{}: 所有 CF 域名均不可用", dc, m_tag);
            None
        }
    }
}

// ---------------------------------------------------------------------------
// doFallback — 现在 CF 失败时不会消耗 conn；CF 失败后
// 同一 conn 进入 TCP 回退（与 Go doFallback 完全一致）。
// ---------------------------------------------------------------------------

pub async fn do_fallback(
    conn: TcpStream,
    relay_init: &[u8],
    label: String,
    dc: i32,
    is_media: bool,
    splitter: Option<MsgSplitter>,
    clt_dec: &TrackedStream,
    clt_enc: &TrackedStream,
    tg_enc: &TrackedStream,
    tg_dec: &TrackedStream,
    cancel_token: CancellationToken,
) -> bool {
    // 克隆流（同 Go Clone()）
    let clt_dec = clt_dec.clone_state();
    let clt_enc = clt_enc.clone_state();
    let tg_enc = tg_enc.clone_state();
    let tg_dec = tg_dec.clone_state();

    let fallback_dst = resolve_fallback_target(dc, is_media);
    let use_cf = CFPROXY_ENABLED.load(Ordering::Relaxed);

    if use_cf {
        // 先通过 CF 获取 WS，不碰 conn。
        if let Some((ws, chosen_domain)) =
            cfproxy_acquire_ws(dc, is_media, &cancel_token).await
        {
            STATS.connections_cfproxy.fetch_add(1, Ordering::Relaxed);
            linfo!(" DC{}{} 已通过 CF 连接", dc, media_tag(is_media));

            if ws.send(relay_init).await.is_err() {
                ws.close().await;
                // CF 在握手后立即失效——尝试用同一 conn 走 TCP。
                if !fallback_dst.is_empty() {
                    return tcp_fallback(
                        conn,
                        &fallback_dst,
                        443,
                        relay_init,
                        label,
                        dc,
                        is_media,
                        clt_dec,
                        clt_enc,
                        tg_enc,
                        tg_dec,
                        cancel_token,
                    )
                    .await;
                }
                return false;
            }

            bridge_ws(
                conn,
                ws,
                label,
                dc,
                chosen_domain,
                443,
                is_media,
                splitter,
                clt_dec,
                clt_enc,
                tg_enc,
                tg_dec,
                cancel_token,
                false, // 池路径始终是 WS 模式
            )
            .await;
            return true;
        }
        // CF 未能提供 ws — conn 未被使用，进入下方 TCP 回退。
    }

    if !fallback_dst.is_empty() {
        return tcp_fallback(
            conn,
            &fallback_dst,
            443,
            relay_init,
            label,
            dc,
            is_media,
            clt_dec,
            clt_enc,
            tg_enc,
            tg_dec,
            cancel_token,
        )
        .await;
    }

    false
}

// ---------------------------------------------------------------------------
// Client handler (dd-only)
// ---------------------------------------------------------------------------

pub async fn handle_client(pool: Arc<WsPool>, mut conn: TcpStream, cancel_token: CancellationToken) {
    STATS.connections_total.fetch_add(1, Ordering::Relaxed);
    STATS.connections_active.fetch_add(1, Ordering::Relaxed);
    struct ActiveGuard;
    impl Drop for ActiveGuard {
        fn drop(&mut self) {
            if STATS.connections_active.load(Ordering::Relaxed) > 0 {
                STATS.connections_active.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
    let _guard = ActiveGuard;

    let peer = conn
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let label = peer;

    let _ = conn.set_nodelay(true);

    let current_secret = PROXY_SECRET.read().clone();
    let secret_bytes = hex::decode(&current_secret).unwrap_or_default();

    // 先读 4 字节判断传输类型：HTTP CONNECT（纯隧道） / MTProxy 握手
    let mut first4 = [0u8; 4];
    match tokio::time::timeout(Duration::from_secs(10), conn.read_exact(&mut first4)).await {
        Ok(Ok(_)) => {}
        _ => return,
    }
    if first4[0] == 0x05 {
        STATS.connections_http_reject.fetch_add(1, Ordering::Relaxed);
        linfo!(" SOCKS5 传输（纯隧道模式）: {:02x} {:02x} {:02x} {:02x}", first4[0], first4[1], first4[2], first4[3]);
        let head_buf = first4.to_vec();
        handle_socks5(conn, head_buf, cancel_token).await;
        return;
    }
    if &first4 == b"CONN"
        || &first4 == b"POST"
        || &first4 == b"GET "
        || &first4 == b"HEAD"
        || &first4 == b"OPTI"
    {
        STATS.connections_http_reject.fetch_add(1, Ordering::Relaxed);
        linfo!(" HTTP 传输（纯隧道模式）: {:?}", String::from_utf8_lossy(&first4));
        let mut head_buf = first4.to_vec();
        handle_http_connect(conn, &mut head_buf, cancel_token).await;
        return;
    }

    // MTProxy：读剩余 60 字节凑满 64
    let mut rest = [0u8; 60];
    match tokio::time::timeout(Duration::from_secs(10), conn.read_exact(&mut rest)).await {
        Ok(Ok(_)) => {}
        _ => return,
    }
    let mut handshake = [0u8; 64];
    handshake[..4].copy_from_slice(&first4);
    handshake[4..].copy_from_slice(&rest);

    let clt_dec_prekey = &handshake[8..40];
    let clt_dec_iv = &handshake[40..56];
    let mut hash_dec = Sha256::new();
    hash_dec.update(clt_dec_prekey);
    hash_dec.update(&secret_bytes);
    let mut clt_decryptor = new_aes_ctr(&hash_dec.finalize(), clt_dec_iv);

    let mut decrypted = handshake;
    clt_decryptor.xor(&mut decrypted);

    let proto_tag = &decrypted[56..60];
    let proto = LittleEndian::read_u32(proto_tag);
    if !valid_proto(proto) {
        STATS.connections_bad.fetch_add(1, Ordering::Relaxed);
        return;
    }

    let dc_raw = LittleEndian::read_u16(&decrypted[60..62]) as i16;
    let mut dc = dc_raw as i32;
    if dc < 0 {
        dc = -dc;
    }
    let is_media = dc_raw < 0;
    let m_tag = media_tag(is_media);

    let mut clt_enc_prekey_and_iv = [0u8; 48];
    for i in 0..48 {
        clt_enc_prekey_and_iv[i] = handshake[8 + 47 - i];
    }
    let mut hash_enc = Sha256::new();
    hash_enc.update(&clt_enc_prekey_and_iv[..32]);
    hash_enc.update(&secret_bytes);
    let clt_encryptor = new_aes_ctr(&hash_enc.finalize(), &clt_enc_prekey_and_iv[32..]);

    // relayInit 生成（与 Go 完全一致）
    let mut relay_init = [0u8; 64];
    loop {
        rand::thread_rng().fill_bytes(&mut relay_init);
        if relay_init[0] == 0xEF {
            continue;
        }
        let s = &relay_init[..4];
        if s == b"HEAD"
            || s == b"POST"
            || s == b"GET "
            || s == &[0xee, 0xee, 0xee, 0xee]
            || s == &[0xdd, 0xdd, 0xdd, 0xdd]
        {
            continue;
        }
        if relay_init[0] == 0x16
            && relay_init[1] == 0x03
            && relay_init[2] == 0x01
            && relay_init[3] == 0x02
        {
            continue;
        }
        if relay_init[4] == 0 && relay_init[5] == 0 && relay_init[6] == 0 && relay_init[7] == 0 {
            continue;
        }
        break;
    }

    let mut tg_dec_prekey_and_iv = [0u8; 48];
    for i in 0..48 {
        tg_dec_prekey_and_iv[i] = relay_init[8 + 47 - i];
    }

    let mut tg_encryptor = new_aes_ctr(&relay_init[8..40], &relay_init[40..56]);
    let tg_decryptor = new_aes_ctr(&tg_dec_prekey_and_iv[..32], &tg_dec_prekey_and_iv[32..]);

    let mut dc_bytes = [0u8; 2];
    let dc_idx = if is_media { -dc } else { dc };
    LittleEndian::write_u16(&mut dc_bytes, dc_idx as u16);

    let mut tail_plain = [0u8; 8];
    tail_plain[0..4].copy_from_slice(proto_tag);
    tail_plain[4..6].copy_from_slice(&dc_bytes);
    rand::thread_rng().fill_bytes(&mut tail_plain[6..8]);

    let mut encrypted_full = relay_init;
    tg_encryptor.xor(&mut encrypted_full);

    let mut keystream_tail = [0u8; 8];
    for i in 0..8 {
        keystream_tail[i] = encrypted_full[56 + i] ^ relay_init[56 + i];
        relay_init[56 + i] = tail_plain[i] ^ keystream_tail[i];
    }

    // —— 标准 MTProxy hello（裸隧道/直连 MTProxy 服务器用）——
    // 服务器只认 0xEF magic + SHA256(key+secret) 派生；公共 MTProxy 无 secret → 空派生。
    // tg-ws-proxy 的 kws 私有 relay_init（随机 magic + 直接 key/iv）发标准服务器会被 400 拒。
    let mut std_init = [0u8; 64];
    std_init[0..4].copy_from_slice(&[0xEFu8, 0xEF, 0xEF, 0xEF]);
    rand::thread_rng().fill_bytes(&mut std_init[4..8]);
    rand::thread_rng().fill_bytes(&mut std_init[8..56]); // key[8..40] + iv[40..56]
    // 派生与入站客户端 hello 一致（界面 secret，默认 0000...0000）——MTProxy 服务器同派生解密
    let mut hash_std = Sha256::new();
    hash_std.update(&std_init[8..40]);
    hash_std.update(&secret_bytes);
    let std_tail_key = hash_std.finalize();
    let mut std_tail_enc = new_aes_ctr(&std_tail_key, &std_init[40..56]);
    let mut std_tail = [0u8; 8];
    std_tail[0..4].copy_from_slice(&[0xEFu8, 0xEF, 0xEF, 0xEF]); // 标准校验 magic（服务器解密后校验）
    std_tail[4..6].copy_from_slice(&dc_bytes);
    rand::thread_rng().fill_bytes(&mut std_tail[6..8]);
    std_tail_enc.xor(&mut std_tail);
    std_init[56..64].copy_from_slice(&std_tail);
    // 上行加密器 = tail 加密器本身（AES-CTR 连续流：tail 已消耗前 8 字节，数据从偏移 8 继续）
    let std_tg_encryptor = std_tail_enc.clone_state();
    // 下行解密器：官方 obfuscated2 反转派生（rev48 = 反转(key+iv)，decKey = SHA256(rev48 + secret)，iv = rev48[32..48]）
    let mut rev48 = [0u8; 48];
    for i in 0..48 {
        rev48[i] = std_init[8 + 47 - i];
    }
    let mut hash_std3 = Sha256::new();
    hash_std3.update(&rev48);
    hash_std3.update(&secret_bytes);
    let std_tg_dec_key = hash_std3.finalize();
    let std_tg_decryptor = new_aes_ctr(&std_tg_dec_key, &rev48[32..48]);
    linfo!(
        " std hello 派生: up={}..{}.. down={}..{}.. rev48={}..{}..",
        hex::encode(&std_tail_key[..4]),
        hex::encode(&std_init[40..44]),
        hex::encode(&std_tg_dec_key[..4]),
        hex::encode(&rev48[32..36]),
        hex::encode(&rev48[..4]),
        hex::encode(&rev48[44..48])
    );

    let dc_key = (dc, is_media_int(is_media));
    let now = now_unix_f64();

    let splitter = MsgSplitter::new(&relay_init, proto);

    let target_opt = resolve_configured_target(dc, is_media);
    // 非媒体：无条件走 zws WS 主路径（界面填的 IP 优先，没填用默认 zws IP）
    // 媒体：保留原逻辑（界面优先，没配则走 do_fallback）
    let dc_configured = target_opt.is_some() || !is_media;
    let target = if let Some(t) = target_opt {
        t
    } else if is_media {
        resolve_fallback_target(dc, true)
    } else {
        crate::config::ZWS_DEFAULT_IP.to_string()
    };

    let blacklisted = WS_BLACKLIST.read().get(&dc_key).copied().unwrap_or(false);

    if !dc_configured || blacklisted {
        do_fallback(
            conn,
            &relay_init,
            label,
            dc,
            is_media,
            splitter,
            &clt_decryptor,
            &clt_encryptor,
            &tg_encryptor,
            &tg_decryptor,
            cancel_token,
        )
        .await;
        return;
    }

    let fail_until = DC_FAIL_UNTIL.read().get(&dc_key).copied().unwrap_or(0.0);
    let ws_timeout = if now < fail_until {
        WS_FAIL_TIMEOUT
    } else {
        10.0
    };

    let domains = ws_domains(dc, is_media);
    let (mut ws_opt, ws_failed_redirect, all_redirects) =
        if let Some(w) = pool.get(dc, is_media, target.clone(), domains.clone()).await {
            (Some(w), false, false)
        } else {
            connect_direct_ws(&target, &domains, ws_timeout).await
        };

    if ws_opt.is_none() {
        lwarn!(" DC{}{}: 所有 WS 尝试均失败（DPI/网络）", dc, m_tag);
        if ws_failed_redirect && all_redirects {
            WS_BLACKLIST.write().insert(dc_key, true);
            lwarn!(" DC{}{} 已被封锁（302）", dc, m_tag);
        } else {
            DC_FAIL_UNTIL.write().insert(dc_key, now + DC_FAIL_COOLDOWN);
        }
        let splitter_fb = MsgSplitter::new(&relay_init, proto);
        do_fallback(
            conn,
            &relay_init,
            label,
            dc,
            is_media,
            splitter_fb,
            &clt_decryptor,
            &clt_encryptor,
            &tg_encryptor,
            &tg_decryptor,
            cancel_token,
        )
        .await;
        return;
    }

    // send direct init
    let mut ws = ws_opt.take().unwrap();
    // 裸隧道：发标准 MTProxy hello（0xEF + 空 secret 派生）；
    // WS 路径：发 kws 私有 relay_init
    let init_payload: Vec<u8> = if ws.is_raw_mode() {
        std_init.to_vec()
    } else {
        relay_init.to_vec()
    };
    let mut send_ok = ws.send(&init_payload).await.is_ok();
    if send_ok {
        ldebug!(" direct relayInit sent DC{}{}", dc, m_tag);
    } else {
        lwarn!(" direct relayInit write fail DC{}{}: closed", dc, m_tag);
        ws.close().await;

        DC_FAIL_UNTIL.write().insert(dc_key, now + DC_FAIL_COOLDOWN);

        lwarn!(" direct retry fresh ws DC{}{}", dc, m_tag);
        let (retry_ws, retry_failed_redirect, retry_all_redirects) =
            connect_direct_ws(&target, &domains, ws_timeout).await;
        match retry_ws {
            None => {
                if retry_failed_redirect && retry_all_redirects {
                    WS_BLACKLIST.write().insert(dc_key, true);
                    lwarn!(" DC{}{} 已被封锁（302）", dc, m_tag);
                }
                lwarn!(" direct fallback DC{}{}", dc, m_tag);
                let splitter_fb = MsgSplitter::new(&relay_init, proto);
                do_fallback(
                    conn,
                    &relay_init,
                    label,
                    dc,
                    is_media,
                    splitter_fb,
                    &clt_decryptor,
                    &clt_encryptor,
                    &tg_encryptor,
                    &tg_decryptor,
                    cancel_token,
                )
                .await;
                return;
            }
            Some(rws) => {
                let retry_payload: Vec<u8> = if rws.is_raw_mode() {
                    std_init.to_vec()
                } else {
                    relay_init.to_vec()
                };
                if rws.send(&retry_payload).await.is_err() {
                    lwarn!(" direct relayInit write fail DC{}{}: closed", dc, m_tag);
                    rws.close().await;
                    lwarn!(" direct fallback DC{}{}", dc, m_tag);
                    let splitter_fb = MsgSplitter::new(&relay_init, proto);
                    do_fallback(
                        conn,
                        &relay_init,
                        label,
                        dc,
                        is_media,
                        splitter_fb,
                        &clt_decryptor,
                        &clt_encryptor,
                        &tg_encryptor,
                        &tg_decryptor,
                        cancel_token,
                    )
                    .await;
                    return;
                }
                ws = rws;
                send_ok = true;
            }
        }
    }
    let _ = send_ok;

    DC_FAIL_UNTIL.write().remove(&dc_key);
    let _ = &pool;
    STATS.connections_ws.fetch_add(1, Ordering::Relaxed);

    let is_raw = ws.is_raw_mode();
    // 裸隧道：标准 hello + 空 secret 派生加解密，无 kws 分帧
    let used_splitter = if is_raw { None } else { splitter };
    let used_tg_enc = if is_raw { std_tg_encryptor } else { tg_encryptor };
    let used_tg_dec = if is_raw { std_tg_decryptor } else { tg_decryptor };
    bridge_ws(
        conn,
        ws,
        label,
        dc,
        target,
        443,
        is_media,
        used_splitter,
        clt_decryptor,
        clt_encryptor,
        used_tg_enc,
        used_tg_dec,
        cancel_token,
        false,
    )
    .await;
}

// connectDirectWS
pub async fn connect_direct_ws(
    target: &str,
    domains: &[String],
    timeout: f64,
) -> (Option<RawWebSocket>, bool, bool) {
    if domains.is_empty() {
        return (None, false, false);
    }
    let mut ws_failed_redirect = false;
    let mut all_redirects = true;

    // v1.4：裸 TLS 隧道优先（MTProxy over TLS 直连 zws1-1，不发 WS 升级）。
    // 用户网络实测：DC5 段 IP（170.200）TLS 握手被 DPI 拦，161.145 稳定可握手；
    // zws 网关按内层 MTProto dc_id 路由到任意 DC（含 DC5）。
    let tunnel_timeout = if timeout > 0.0 { timeout } else { 10.0 };
    let mut tunnel_ips: Vec<String> = Vec::new();
    if !target.is_empty() {
        tunnel_ips.push(target.to_string());
    }
    tunnel_ips.push(crate::config::ZWS_DEFAULT_IP.to_string());
    for tip in tunnel_ips {
        match crate::ws::tls_tunnel_once(&tip, &tip, Duration::from_secs_f64(tunnel_timeout)).await {
            Ok(ws) => return (Some(ws), false, false),
            Err(_) => {}
        }
    }

    for dom in domains {
        match ws_connect(target, dom, "/apiws", timeout).await {
            Ok(ws) => return (Some(ws), ws_failed_redirect, false),
            Err(e) => {
                STATS.ws_errors.fetch_add(1, Ordering::Relaxed);
                if let Some(h) = e.handshake() {
                    if h.is_redirect() {
                        ws_failed_redirect = true;
                    } else {
                        all_redirects = false;
                    }
                } else {
                    all_redirects = false;
                }
            }
        }
    }
    (None, ws_failed_redirect, all_redirects)
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

pub async fn run_proxy(
    pool: Arc<WsPool>,
    host: String,
    port: u16,
    dc_opt_map: HashMap<i32, String>,
    cancel_root: CancellationToken,
    listener: TcpListener,
) -> std::io::Result<()> {
    {
        let mut m = DC_OPT.write();
        *m = dc_opt_map.clone();
    }

    start_cfproxy_refresh();

    {
        let p = pool.clone();
        let map = dc_opt_map.clone();
        tokio::spawn(async move {
            p.warmup(&map).await;
        });
    }

    linfo!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    linfo!("  TG WS Proxy 已启动");
    linfo!("  地址: {}:{}", host, port);

    let cancel_stats = cancel_root.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await;
        loop {
            tokio::select! {
                _ = cancel_stats.cancelled() => return,
                _ = interval.tick() => {
                    linfo!(" {}", STATS.summary_ru());
                }
            }
        }
    });

    loop {
        tokio::select! {
            _ = cancel_root.cancelled() => {
                break;
            }
            accept = listener.accept() => {
                match accept {
                    Ok((conn, _)) => {
                        let p = pool.clone();
                        let cancel = cancel_root.child_token();
                        tokio::spawn(async move {
                            handle_client(p, conn, cancel).await;
                        });
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }

    drop(listener);
    cancel_root.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
    pool.close_all().await;
    Ok(())
}

pub fn parse_cidr_pool(cidrs_str: &str) -> HashMap<i32, String> {
    let mut result = HashMap::new();
    if cidrs_str.trim().is_empty() {
        return result;
    }
    for pair in cidrs_str.split(',') {
        let parts: Vec<&str> = pair.split(':').collect();
        if parts.len() == 2 {
            let dc_raw = parts[0].trim();
            let ip_raw = parts[1].trim();
            if let Ok(dc) = dc_raw.parse::<i32>() {
                if !ip_raw.is_empty() {
                    if let Ok(ip) = ip_raw.parse::<std::net::IpAddr>() {
                        result.insert(dc, ip.to_string());
                    }
                }
            }
        }
    }
    result
}
