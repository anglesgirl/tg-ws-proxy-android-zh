pub mod cfproxy;
pub mod config;
pub mod crypto;
pub mod proxy;
pub mod ws;
pub mod balancer;

use config::*;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use proxy::{parse_cidr_pool, run_proxy, WsPool};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

// 全局运行时——从不被释放
static RUNTIME: OnceCell<Runtime> = OnceCell::new();

struct ProxyState {
    pool: Arc<WsPool>,
    handle: tokio::task::JoinHandle<()>,
    cancel_tasks: CancellationToken,
}

static STATE: OnceCell<Mutex<Option<ProxyState>>> = OnceCell::new();

fn state_cell() -> &'static Mutex<Option<ProxyState>> {
    STATE.get_or_init(|| Mutex::new(None))
}

fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        // 多线程运行时，工作线程数适配手机
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("tgwsproxy-rt")
            .enable_all()
            .build()
            .expect("failed to build global tokio runtime")
    })
}

fn cstr_to_string(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    unsafe { CStr::from_ptr(p).to_string_lossy().into_owned() }
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

/// # Safety
/// 指针必须是合法的 C 字符串（或 null）。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn StartProxy(
    c_host: *const c_char,
    port: c_int,
    c_dc_ips: *const c_char,
    c_secret: *const c_char,
    verbose: c_int,
) -> c_int {
    let cell = state_cell();
    let mut guard = cell.lock();

    if guard.is_some() {
        return -1;
    }

    let host = cstr_to_string(c_host);
    let go_port = port as u16;
    let dc_ips_str = cstr_to_string(c_dc_ips);
    let secret_str = cstr_to_string(c_secret);
    let is_verbose = verbose != 0;

    init_logging(is_verbose);
    cfproxy::clear_cfproxy_429_cooldowns();

    if secret_str.len() == 32 {
        if hex::decode(&secret_str).is_ok() {
            *PROXY_SECRET.write() = secret_str.clone();
        }
    }

    cfproxy::init_cfproxy_domains();

    let dc_opt_map: HashMap<i32, String> = parse_cidr_pool(&dc_ips_str);

    let rt = runtime();
    let cancel_tasks = CancellationToken::new();
    let pool = Arc::new(WsPool::new(cancel_tasks.clone()));

    // 就绪通道：返回前等待 bind 成功
    let (tx, rx) = std::sync::mpsc::channel::<Result<(), String>>();

    let pool_task = pool.clone();
    let host_task = host.clone();
    let map_task = dc_opt_map.clone();
    let cancel_root = cancel_tasks.clone();

    let handle = rt.spawn(async move {
        // 为就绪信号预先 bind
        let addr = format!("{}:{}", host_task, go_port);
        match tokio::net::TcpListener::bind(&addr).await {
            Ok(listener) => {
                let _ = tx.send(Ok(()));
                if let Err(e) =
                    run_proxy(pool_task, host_task, go_port, map_task, cancel_root, listener).await
                {
                    lerror!("listen on {}: {}", addr, e);
                }
            }
            Err(e) => {
                let _ = tx.send(Err(format!("listen on {}: {}", addr, e)));
            }
        }
    });

    // 等待 bind 结果
    match rx.recv() {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            handle.abort();
            return -3;
        }
        Err(_) => {
            handle.abort();
            return -3;
        }
    }

    *guard = Some(ProxyState {
        pool,
        handle,
        cancel_tasks,
    });

    0
}

#[unsafe(no_mangle)]
pub extern "C" fn StopProxy() -> c_int {
    let cell = state_cell();
    let mut guard = cell.lock();

    let state = match guard.take() {
        Some(s) => s,
        None => return -1,
    };

    // 优雅关闭——不释放运行时
    linfo!("StopProxy: cancelling all tasks");
    state.cancel_tasks.cancel();

    let rt = runtime();
    let pool = state.pool.clone();
    let handle = state.handle;
    rt.block_on(async move {
        linfo!("StopProxy: waiting for proxy tasks to finish (max 2s)");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
        linfo!("StopProxy: closing pool connections");
        pool.close_all().await;
        linfo!("StopProxy: done");
    });

    STATS.reset();
    WS_BLACKLIST.write().clear();
    DC_FAIL_UNTIL.write().clear();
    cfproxy::clear_cfproxy_429_cooldowns();

    linfo!("StopProxy: exit");
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn SetPoolSize(size: c_int) {
    let mut n = size;
    if n < 2 {
        n = 2;
    }
    if n > 16 {
        n = 16;
    }
    POOL_SIZE.store(n, Ordering::Relaxed);
}

/// # Safety
/// `c_cache_dir` — 合法 C 字符串或 null。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SetCfProxyCacheDir(c_cache_dir: *const c_char) {
    let dir = cstr_to_string(c_cache_dir);
    CFPROXY.write().cache_dir = dir.trim().to_string();
}

/// # Safety
/// `c_user_domain` — 合法 C 字符串或 null。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SetCfProxyConfig(
    enabled: c_int,
    _priority: c_int,
    c_user_domain: *const c_char,
) {
    CFPROXY_ENABLED.store(enabled != 0, Ordering::Relaxed);
    let user_domain = cfproxy::normalize_user_cf_domain(&cstr_to_string(c_user_domain));
    let mut cfg = CFPROXY.write();
    cfg.user_domain = user_domain.clone();
    if !user_domain.is_empty() {
        cfg.domains = vec![user_domain.clone()];
        cfg.active = user_domain;
    }
}

/// # Safety
/// `c_secret` — 合法 C 字符串或 null。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SetSecret(c_secret: *const c_char) {
    let s = cstr_to_string(c_secret);
    if s.len() != 32 {
        return;
    }
    if hex::decode(&s).is_err() {
        return;
    }
    *PROXY_SECRET.write() = s;
}

/// # Safety
/// `c_range` — 合法 C 字符串或 null。格式："a.b.c.d-e.f.g.h"，
/// 多个区间用逗号/分号/空格分隔。
/// 空字符串 = 固定区间关闭（改用 DoH）。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SetFixedIpRange(c_range: *const c_char) {
    let raw = cstr_to_string(c_range);
    let ranges = parse_fixed_ip_ranges(&raw);
    let total: u64 = ranges
        .iter()
        .map(|(s, e)| (*e as u64 - *s as u64) + 1)
        .sum();
    if raw.trim().is_empty() {
        linfo!("SetFixedIpRange: 空 — 固定区间关闭（DoH）");
    } else if ranges.is_empty() {
        lerror!(
            "SetFixedIpRange: 解析 '{}' 失败（应为 a.b.c.d-e.f.g.h）",
            raw.trim()
        );
    } else {
        linfo!(
            "SetFixedIpRange: {} 个区间，共 {} 个地址",
            ranges.len(),
            total
        );
    }
    FIXED_IP_CURSOR.store(0, Ordering::Relaxed);
    *FIXED_IP_LAST_GOOD.write() = String::new();
    *FIXED_IP_RANGES.write() = ranges;
}

#[unsafe(no_mangle)]
pub extern "C" fn GetStats() -> *mut c_char {
    let s = STATS.summary();
    CString::new(s).unwrap_or_default().into_raw()
}

#[unsafe(no_mangle)]
pub extern "C" fn GetSecretWithPrefix() -> *mut c_char {
    let sec = PROXY_SECRET.read().clone();
    CString::new(format!("dd{}", sec)).unwrap_or_default().into_raw()
}

/// # Safety
/// `p` 必须是由本库此前返回的指针。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FreeString(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    unsafe {
        let _ = CString::from_raw(p);
    }
}
