//! Nested `join` must not deadlock, even when the pool has no spare
//! threads to steal work. A watchdog thread bounds each test so a
//! scheduling deadlock fails instead of hanging the test runner.

use rayon_core::ThreadPoolBuilder;
use std::sync::mpsc;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(60);

fn run_with_watchdog<R: Send + 'static>(
    f: impl FnOnce() -> R + Send + 'static,
) -> Result<R, &'static str> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(TIMEOUT).map_err(|_| "nested join deadlocked")
}

/// The scenario from the report: nested `join` on a custom pool with a
/// single thread. The inner subtasks cannot be stolen, so they must be
/// executed inline by the waiting worker itself.
#[test]
fn nested_join_single_thread_pool() {
    let pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let result = run_with_watchdog(move || {
        pool.join(
            || pool.join(|| 1, || 2),
            || pool.join(|| 3, || 4),
        )
    });
    assert_eq!(result, Ok(((1, 2), (3, 4))));
}

/// Deeper recursion, quicksort-style: every level splits in two, all on
/// one worker thread.
#[test]
fn deep_nested_join_single_thread_pool() {
    fn tree(depth: u32) -> u64 {
        if depth == 0 {
            return 1;
        }
        let (a, b) = rayon_core::join(|| tree(depth - 1), || tree(depth - 1));
        a + b
    }

    let pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let result = run_with_watchdog(move || pool.install(|| tree(20)));
    assert_eq!(result, Ok(1 << 20));
}

/// Two single-thread pools whose tasks join into each other: each pool's
/// only worker is blocked waiting on the other pool. The waiting workers
/// must keep executing their own pool's injected work to make progress.
#[test]
fn cross_pool_nested_join() {
    let pool_a = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let pool_b = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let result = run_with_watchdog(move || {
        pool_a.install(|| {
            pool_b.join(
                || pool_a.join(|| 1, || 2),
                || pool_a.join(|| 3, || 4),
            )
        })
    });
    assert_eq!(result, Ok(((1, 2), (3, 4))));
}
