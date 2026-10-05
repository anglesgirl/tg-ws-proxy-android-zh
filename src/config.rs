use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Constants & Configuration
// ---------------------------------------------------------------------------

pub const DEFAULT_PORT: u16 = 1443;
pub const TCP_NODELAY: bool = true;
pub const DEFAULT_RECV_BUF: usize = 256 * 1024;
pub const DEFAULT_SEND_BUF: usize = 256 * 1024;
pub const DEFAULT_POOL_SZ: i32 = 4;

pub const DC_FAIL_COOLDOWN: f64 = 30.0;
pub const WS_FAIL_TIMEOUT: f64 = 2.0;

pub const BRIDGE_READ_TIMEOUT: Duration = Duration::from_secs(120);
pub const BRIDGE_PING_INTERVAL: Duration = Duration::from_secs(30);
pub const WS_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub const WS_CONTROL_TIMEOUT: Duration = Duration::from_secs(2);
pub const WS_READ_TIMEOUT: Duration = Duration::from_secs(30);
pub const WS_BRIDGE_CHUNK_SIZE: usize = 64 * 1024;
pub const POOLED_FRAME_CAP: usize = WS_BRIDGE_CHUNK_SIZE + 32;

pub const WS_POOL_REUSE_MAX_AGE: f64 = 120.0;
pub const WS_POOL_CONNECT_TIMEOUT: f64 = 8.0;

pub const CFPROXY_CACHE_FILE_NAME: &str = "cfproxy-domains-cache.txt";
pub const CFPROXY_ACTIVE_FILE_NAME: &str = "cfproxy-active-domain.txt";
pub const CFPROXY_REFRESH_INTERVAL: Duration = Duration::from_secs(12 * 3600);
pub const CFPROXY_DIAL_PHASE_TIMEOUT: Duration = Duration::from_secs(4);
pub const CFPROXY_FALLBACK_PARALLEL: usize = 2;
pub const CFPROXY_429_COOLDOWN: Duration = Duration::from_secs(45);
pub const CFPROXY_429_MAX_COOLDOWN: Duration = Duration::from_secs(300);
pub const CFPROXY_GLOBAL_PARALLEL: usize = 4;

pub static RECV_BUF: AtomicI32 = AtomicI32::new(DEFAULT_RECV_BUF as i32);
pub static SEND_BUF: AtomicI32 = AtomicI32::new(DEFAULT_SEND_BUF as i32);
pub static POOL_SIZE: AtomicI32 = AtomicI32::new(DEFAULT_POOL_SZ);
pub static LOG_VERBOSE: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
pub struct Cfproxy429State {
    pub until: Option<Instant>,
    pub strikes: i32,
}

impl Default for Cfproxy429State {
    fn default() -> Self {
        Cfproxy429State { until: None, strikes: 0 }
    }
}

// Cloudflare proxy config
pub static CFPROXY_ENABLED: AtomicBool = AtomicBool::new(true);

pub struct CfproxyConfig {
    pub user_domain: String,
    pub domains: Vec<String>,
    pub active: String,
    pub cache_dir: String,
}

pub static CFPROXY: Lazy<RwLock<CfproxyConfig>> = Lazy::new(|| {
    RwLock::new(CfproxyConfig {
        user_domain: String::new(),
        domains: Vec::new(),
        active: String::new(),
        cache_dir: String::new(),
    })
});

pub static CFPROXY_429: Lazy<RwLock<HashMap<String, Cfproxy429State>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub const CFPROXY_DOMAINS_URL: &str =
    "https://raw.githubusercontent.com/Flowseal/tg-ws-proxy/main/.github/cfproxy-domains.txt";

// MTProto proxy secret
pub static PROXY_SECRET: Lazy<RwLock<String>> =
    Lazy::new(|| RwLock::new("00000000000000000000000000000000".to_string()));

pub static CFPROXY_ENC: &[&str] = &[
    "virkgj.com",
    "vmmzovy.com",
    "mkuosckvso.com",
    "zaewayzmplad.com",
    "twdmbzcm.com",
    "awzwsldi.com",
    "clngqrflngqin.com",
    "tjacxbqtj.com",
    "bxaxtxmrw.com",
    "dmohrsgmohcrwb.com",
    "vwbmtmoi.com",
    "khgrre.com",
    "ulihssf.com",
    "tmhqsdqmfpmk.com",
    "xwuwoqbm.com",
];

// DC default IPs — 实测可用（0304 record 直连，绕过 DPI SNI 阻断）
// DC1: 149.154.161.145 实测 kws1-1/zws1-1.web.telegram.org 握手成功
// DC5: 149.154.170.200 实测 kws5/kws5-1/zws5-1.web.telegram.org 握手成功
// 界面设置里填写的 IP 优先于本表；IP 更换时直接改界面即可
pub static DC_DEFAULT_IPS: Lazy<HashMap<i32, &'static str>> = Lazy::new(|| {
    let mut m = HashMap::new();
    m.insert(1, "149.154.161.145");
    m.insert(2, "149.154.167.51");
    m.insert(3, "149.154.175.100");
    m.insert(4, "149.154.167.91");
    m.insert(5, "149.154.170.200");
    m.insert(203, "91.105.192.100");
    m
});

// 非媒体 WS 统一连接的 zws 网关默认 IP（zws1-1.web.telegram.org，实测 0304 握手成功）
// 界面设置里填写的 IP 优先于本默认值；IP 更换时直接改界面即可
pub const ZWS_DEFAULT_IP: &str = "149.154.161.145";


// ---------------------------------------------------------------------------
// Fixed IP range (替代 DoH 结果，用于 CF 域名)
// ---------------------------------------------------------------------------
// 用户可指定形如 "104.16.0.1-104.20.255.255" 的区间；允许
// 多个区间用逗号/分号/空格/换行分隔。
// 指定区间后完全不使用 DoH：IP 直接从区间取出，
// 循环遍历（每次尝试推进游标，
// 因此大区间也能逐步遍历完）。
pub static FIXED_IP_RANGES: Lazy<RwLock<Vec<(u32, u32)>>> =
    Lazy::new(|| RwLock::new(Vec::new()));
pub static FIXED_IP_CURSOR: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
// 上次成功连接的 IP 优先尝试。
pub static FIXED_IP_LAST_GOOD: Lazy<RwLock<String>> =
    Lazy::new(|| RwLock::new(String::new()));

// 单次连接尝试从区间中试多少个 IP。
pub const FIXED_IP_TRIES_PER_CONNECT: usize = 4;

/// 把 "a.b.c.d-e.f.g.h"（一条或多条）解析为区间列表。
pub fn parse_fixed_ip_ranges(raw: &str) -> Vec<(u32, u32)> {
    let mut out: Vec<(u32, u32)> = Vec::new();
    for tok in raw.split(|c: char| {
        c == ',' || c == ';' || c == '\n' || c == '\r' || c == '\t' || c == ' '
    }) {
        let t = tok.trim();
        if t.is_empty() {
            continue;
        }
        let (a, b) = match t.split_once('-') {
            Some((a, b)) => (a.trim(), b.trim()),
            None => (t, t),
        };
        let (Ok(s), Ok(e)) = (a.parse::<std::net::Ipv4Addr>(), b.parse::<std::net::Ipv4Addr>())
        else {
            continue;
        };
        let (mut s, mut e) = (u32::from(s), u32::from(e));
        if s > e {
            std::mem::swap(&mut s, &mut e);
        }
        out.push((s, e));
    }
    out
}

/// 是否配置了固定区间（非空且合法）。
pub fn fixed_ip_range_active() -> bool {
    !FIXED_IP_RANGES.read().is_empty()
}

/// 从区间中取出接下来 `count` 个 IP（自游标起循环）。
/// 上次成功的 IP 总是排最前。
pub fn fixed_range_next_ips(count: usize) -> Vec<String> {
    let ranges = FIXED_IP_RANGES.read().clone();
    let total: u64 = ranges
        .iter()
        .map(|(s, e)| (*e as u64 - *s as u64) + 1)
        .sum();
    if total == 0 || count == 0 {
        return Vec::new();
    }

    let mut out: Vec<String> = Vec::with_capacity(count + 1);
    let last_good = FIXED_IP_LAST_GOOD.read().clone();
    if !last_good.is_empty() {
        out.push(last_good);
    }

    let start = FIXED_IP_CURSOR.fetch_add(count as u64, Ordering::Relaxed) % total;
    for i in 0..count as u64 {
        let idx = (start + i) % total;
        let mut acc: u64 = 0;
        for (s, e) in &ranges {
            let len = (*e as u64 - *s as u64) + 1;
            if idx < acc + len {
                let ip = std::net::Ipv4Addr::from((*s as u64 + (idx - acc)) as u32).to_string();
                if !out.contains(&ip) {
                    out.push(ip);
                }
                break;
            }
            acc += len;
        }
    }
    out
}

pub fn set_fixed_ip_last_good(ip: &str) {
    if ip.is_empty() {
        return;
    }
    let mut g = FIXED_IP_LAST_GOOD.write();
    if *g != ip {
        *g = ip.to_string();
    }
}


// Telegram protocols & DC mapping
pub fn valid_proto(p: u32) -> bool {
    matches!(p, 0xEFEFEFEF | 0xEEEEEEEE | 0xDDDDDDDD)
}

pub static DC_OVERRIDES: Lazy<HashMap<i32, i32>> = Lazy::new(|| {
    let mut m = HashMap::new();
    m.insert(203, 2);
    m
});

// Global state
pub static DC_OPT: Lazy<RwLock<HashMap<i32, String>>> = Lazy::new(|| RwLock::new(HashMap::new()));
pub static WS_BLACKLIST: Lazy<RwLock<HashMap<(i32, i32), bool>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));
pub static DC_FAIL_UNTIL: Lazy<RwLock<HashMap<(i32, i32), f64>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub static ZERO64: [u8; 64] = [0u8; 64];

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct Stats {
    pub connections_total: AtomicI64,
    pub connections_active: AtomicI64,
    pub connections_ws: AtomicI64,
    pub connections_tcp_fallback: AtomicI64,
    pub connections_cfproxy: AtomicI64,
    pub connections_http_reject: AtomicI64,
    pub connections_passthrough: AtomicI64,
    pub connections_bad: AtomicI64,
    pub ws_errors: AtomicI64,
    pub bytes_up: AtomicI64,
    pub bytes_down: AtomicI64,
    pub pool_hits: AtomicI64,
    pub pool_misses: AtomicI64,
}

pub static STATS: Lazy<Stats> = Lazy::new(Stats::default);

impl Stats {
    pub fn summary(&self) -> String {
        let ph = self.pool_hits.load(Ordering::Relaxed);
        let pm = self.pool_misses.load(Ordering::Relaxed);
        format!(
            "total={} active={} ws={} tcp_fb={} cf={} bad={} err={} pool={}/{} up={} down={}",
            self.connections_total.load(Ordering::Relaxed),
            self.connections_active.load(Ordering::Relaxed),
            self.connections_ws.load(Ordering::Relaxed),
            self.connections_tcp_fallback.load(Ordering::Relaxed),
            self.connections_cfproxy.load(Ordering::Relaxed),
            self.connections_bad.load(Ordering::Relaxed),
            self.ws_errors.load(Ordering::Relaxed),
            ph,
            ph + pm,
            human_bytes(self.bytes_up.load(Ordering::Relaxed)),
            human_bytes(self.bytes_down.load(Ordering::Relaxed)),
        )
    }

    pub fn summary_ru(&self) -> String {
        let mut parts = vec![format!("活动:{}", self.connections_active.load(Ordering::Relaxed))];
        let ws = self.connections_ws.load(Ordering::Relaxed);
        if ws > 0 {
            parts.push(format!("ws:{}", ws));
        }
        let cf = self.connections_cfproxy.load(Ordering::Relaxed);
        if cf > 0 {
            parts.push(format!("cf:{}", cf));
        }
        let tcp = self.connections_tcp_fallback.load(Ordering::Relaxed);
        if tcp > 0 {
            parts.push(format!("tcp:{}", tcp));
        }
        let err = self.ws_errors.load(Ordering::Relaxed);
        if err > 0 {
            parts.push(format!("错误:{}", err));
        }
        parts.push(format!(
            "↑{} ↓{}",
            human_bytes(self.bytes_up.load(Ordering::Relaxed)),
            human_bytes(self.bytes_down.load(Ordering::Relaxed))
        ));
        parts.join(" | ")
    }

    pub fn reset(&self) {
        self.connections_total.store(0, Ordering::Relaxed);
        self.connections_active.store(0, Ordering::Relaxed);
        self.connections_ws.store(0, Ordering::Relaxed);
        self.connections_tcp_fallback.store(0, Ordering::Relaxed);
        self.connections_cfproxy.store(0, Ordering::Relaxed);
        self.connections_http_reject.store(0, Ordering::Relaxed);
        self.connections_passthrough.store(0, Ordering::Relaxed);
        self.connections_bad.store(0, Ordering::Relaxed);
        self.ws_errors.store(0, Ordering::Relaxed);
        self.bytes_up.store(0, Ordering::Relaxed);
        self.bytes_down.store(0, Ordering::Relaxed);
        self.pool_hits.store(0, Ordering::Relaxed);
        self.pool_misses.store(0, Ordering::Relaxed);
    }
}

pub fn human_bytes(n: i64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut f = n as f64;
    for (i, u) in units.iter().enumerate() {
        if f.abs() < 1024.0 || i == units.len() - 1 {
            return format!("{:.1}{}", f, u);
        }
        f /= 1024.0;
    }
    format!("{:.1}TB", f)
}

// ---------------------------------------------------------------------------
// 日志器（Android log + stderr，前缀一一对应）
// ---------------------------------------------------------------------------

#[cfg(target_os = "android")]
fn android_log_line(line: &str) {
    use std::ffi::CString;
    unsafe extern "C" {
        fn __android_log_print(prio: i32, tag: *const i8, fmt: *const i8, ...) -> i32;
    }
    const ANDROID_LOG_INFO: i32 = 4;
    if let (Ok(tag), Ok(fmt), Ok(msg)) =
        (CString::new("TgWsProxy"), CString::new("%s"), CString::new(line))
    {
        unsafe {
            __android_log_print(
                ANDROID_LOG_INFO,
                tag.as_ptr() as *const i8,
                fmt.as_ptr() as *const i8,
                msg.as_ptr() as *const i8,
            );
        }
    }
}

#[cfg(not(target_os = "android"))]
fn android_log_line(_line: &str) {}

fn emit(prefix: &str, msg: &str) {
    let line = format!("{}{}", prefix, msg);
    eprintln!("{}", line);
    android_log_line(&line);
}

pub fn log_info(msg: &str) {
    emit("", msg);
}
pub fn log_warn(msg: &str) {
    emit("[WARN] ", msg);
}
pub fn log_error(msg: &str) {
    emit("[ERROR] ", msg);
}
pub fn log_debug(msg: &str) {
    if LOG_VERBOSE.load(Ordering::Relaxed) {
        emit("[DEBUG] ", msg);
    }
}

#[macro_export]
macro_rules! linfo  { ($($a:tt)*) => { $crate::config::log_info(&format!($($a)*)) }; }
#[macro_export]
macro_rules! lwarn  { ($($a:tt)*) => { $crate::config::log_warn(&format!($($a)*)) }; }
#[macro_export]
macro_rules! lerror { ($($a:tt)*) => { $crate::config::log_error(&format!($($a)*)) }; }
#[macro_export]
macro_rules! ldebug { ($($a:tt)*) => { $crate::config::log_debug(&format!($($a)*)) }; }

pub fn init_logging(verbose: bool) {
    LOG_VERBOSE.store(verbose, Ordering::Relaxed);
}

pub fn now_unix_f64() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

pub fn now_unix() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}