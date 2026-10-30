use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};
use temporalio_workflow::runtime::is_sdk_wake;

/// Persists across polls to accumulate non-SDK wake detection. Each poll creates a lightweight
/// waker via [`WakeTracker::new_per_poll_waker`] that shares the detection flag but has the
/// current parent waker baked in, which avoids sharing mutable waker state across polls.
pub(crate) struct WakeTracker {
    non_sdk_wake_detected: Arc<AtomicBool>,
}

impl WakeTracker {
    pub(crate) fn new() -> Self {
        Self {
            non_sdk_wake_detected: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create a waker for this poll that forwards to `parent_waker` and sets the shared detection
    /// flag on non-SDK wakes.
    pub(crate) fn new_per_poll_waker(&self, parent_waker: &Waker) -> Waker {
        Waker::from(Arc::new(PerPollWakeTracker {
            non_sdk_wake_detected: self.non_sdk_wake_detected.clone(),
            parent_waker: parent_waker.clone(),
        }))
    }

    pub(crate) fn take_non_sdk_wake(&self) -> bool {
        self.non_sdk_wake_detected.swap(false, Ordering::AcqRel)
    }
}

struct PerPollWakeTracker {
    non_sdk_wake_detected: Arc<AtomicBool>,
    parent_waker: Waker,
}

impl Wake for PerPollWakeTracker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !is_sdk_wake() {
            self.non_sdk_wake_detected.store(true, Ordering::Release);
        }
        self.parent_waker.wake_by_ref();
    }
}

struct ExecutorShared {
    ready_queue: parking_lot::Mutex<VecDeque<u64>>,
    /// Waker to notify when tasks are enqueued so the driver can park while tasks wait on external
    /// events.
    waker: parking_lot::Mutex<Option<Waker>>,
}

impl ExecutorShared {
    fn enqueue(&self, task_id: u64) {
        self.ready_queue.lock().push_back(task_id);
        if let Some(w) = self.waker.lock().as_ref() {
            w.wake_by_ref();
        }
    }
}

struct TaskNotifier {
    task_id: u64,
    shared: Arc<ExecutorShared>,
}

impl Wake for TaskNotifier {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.shared.enqueue(self.task_id);
    }
}

struct LocalTask {
    future: Pin<Box<dyn Future<Output = ()>>>,
    waker: Waker,
}

struct TaskHandleInner<T> {
    result: RefCell<Option<T>>,
    waker: RefCell<Option<Waker>>,
}

/// A `!Send` join handle returned by [`WorkflowExecutor::spawn`].
///
/// Resolves to the spawned future's output when the executor completes it.
pub(crate) struct TaskHandle<T> {
    inner: Rc<TaskHandleInner<T>>,
}

/// Error returned by [`TaskHandle`] if the task was dropped without completing.
#[derive(Debug)]
pub(crate) struct TaskDroppedError;

impl std::fmt::Display for TaskDroppedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("task was dropped before completing")
    }
}

impl std::error::Error for TaskDroppedError {}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T, TaskDroppedError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.inner.result.borrow_mut().take() {
            Poll::Ready(Ok(result))
        } else {
            *self.inner.waker.borrow_mut() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// A minimal single-threaded async executor for workflow futures.
///
/// Replaces [tokio::task::LocalSet] + `spawn_local` for workflow tasks. Runs inside the existing
/// tokio runtime (driven as an async future) but provides its own task management with custom
/// wakers, enabling nondeterminism detection via [WakeTracker]
///
/// All spawned futures are `!Send` (they use `Rc<RefCell<...>>` internally). The executor itself is
/// `!Send` and must be driven from a `LocalSet` or single-threaded context.
pub(crate) struct WorkflowExecutor {
    tasks: RefCell<HashMap<u64, LocalTask>>,
    next_id: Cell<u64>,
    shared: Arc<ExecutorShared>,
}

impl Default for WorkflowExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowExecutor {
    pub(crate) fn new() -> Self {
        Self {
            tasks: RefCell::new(HashMap::new()),
            next_id: Cell::new(0),
            shared: Arc::new(ExecutorShared {
                ready_queue: parking_lot::Mutex::new(VecDeque::new()),
                waker: parking_lot::Mutex::new(None),
            }),
        }
    }

    /// Spawn a future onto this executor and return a handle that resolves once the executor drains
    /// that task to completion.
    pub(crate) fn spawn<F, T>(&self, future: F) -> TaskHandle<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let id = self.next_id.get();
        self.next_id.set(id + 1);

        let handle_inner = Rc::new(TaskHandleInner {
            result: RefCell::new(None),
            waker: RefCell::new(None),
        });

        let inner_clone = handle_inner.clone();
        let wrapped = async move {
            let output = future.await;
            *inner_clone.result.borrow_mut() = Some(output);
            if let Some(w) = inner_clone.waker.borrow_mut().take() {
                w.wake();
            }
        };

        self.tasks.borrow_mut().insert(
            id,
            LocalTask {
                future: Box::pin(wrapped),
                waker: Waker::from(Arc::new(TaskNotifier {
                    task_id: id,
                    shared: self.shared.clone(),
                })),
            },
        );

        self.shared.enqueue(id);

        TaskHandle {
            inner: handle_inner,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tasks.borrow().is_empty()
    }

    pub(crate) async fn drive(&self) {
        std::future::poll_fn(|cx| {
            *self.shared.waker.lock() = Some(cx.waker().clone());
            self.process_tasks();
            Poll::<()>::Pending
        })
        .await
    }

    /// Keep draining until no tasks remain because workflow shutdown must flush spawned handlers
    /// before the activation can be considered quiescent.
    pub(crate) async fn shutdown(&self) {
        std::future::poll_fn(|cx| {
            *self.shared.waker.lock() = Some(cx.waker().clone());
            self.process_tasks();
            if self.is_empty() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }

    /// Poll each ready task once so wake-ups that happen during polling are re-queued instead of
    /// recursively draining the executor on the same stack.
    pub(crate) fn process_tasks(&self) {
        loop {
            let task_id = self.shared.ready_queue.lock().pop_front();
            let Some(task_id) = task_id else { break };

            let Some(mut task) = self.tasks.borrow_mut().remove(&task_id) else {
                continue;
            };

            let mut task_cx = Context::from_waker(&task.waker);
            if task.future.as_mut().poll(&mut task_cx).is_pending() {
                self.tasks.borrow_mut().insert(task_id, task);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_workflow::runtime::SdkWakeGuard;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn executor_spawn_and_complete() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let executor = WorkflowExecutor::new();
                let handle = executor.spawn(async { 42 });
                executor.shutdown().await;
                let result = handle.await.unwrap();
                assert_eq!(result, 42);
            })
            .await;
    }

    #[tokio::test]
    async fn executor_multiple_tasks() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let executor = WorkflowExecutor::new();
                let h1 = executor.spawn(async { 1 });
                let h2 = executor.spawn(async { 2 });
                let h3 = executor.spawn(async { 3 });
                executor.shutdown().await;
                assert_eq!(h1.await.unwrap(), 1);
                assert_eq!(h2.await.unwrap(), 2);
                assert_eq!(h3.await.unwrap(), 3);
            })
            .await;
    }

    #[tokio::test]
    async fn executor_wake_forwarding_through_oneshot() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let executor = Rc::new(WorkflowExecutor::new());
                let (tx, rx) = oneshot::channel::<i32>();

                let handle = executor.spawn(async move { rx.await.unwrap() });

                let executor_clone = executor.clone();
                tokio::task::spawn_local(async move {
                    executor_clone.shutdown().await;
                });

                // Send after a yield to ensure the future has parked
                tokio::task::yield_now().await;
                tx.send(99).unwrap();

                let result = handle.await.unwrap();
                assert_eq!(result, 99);
            })
            .await;
    }

    #[tokio::test]
    async fn spawn_while_parked_drains_new_task() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let executor = WorkflowExecutor::new();
                let (tx, rx) = oneshot::channel::<()>();
                let (polled_tx, polled_rx) = oneshot::channel::<()>();
                let handle = executor.spawn(async move {
                    polled_tx.send(()).unwrap();
                    rx.await.unwrap();
                    42
                });

                tokio::select! {
                    // Keep polling executor to make sure we make progress
                    _ = executor.drive() => unreachable!("executor driver cannot finish"),
                    result = async {
                        // Ensure spawned task has started and is waiting
                        polled_rx.await.unwrap();
                        // Release spawned task
                        tx.send(()).unwrap();
                        handle.await.unwrap()
                    } => assert_eq!(result, 42),
                }
            })
            .await;
    }

    #[test]
    fn sdk_wake_guard_nesting() {
        assert!(!is_sdk_wake());

        let guard1 = SdkWakeGuard::new();
        assert!(is_sdk_wake());

        {
            let _guard2 = SdkWakeGuard::new();
            assert!(is_sdk_wake());
        }
        assert!(is_sdk_wake());

        drop(guard1);
        assert!(!is_sdk_wake());
    }

    #[test]
    fn sdk_wake_guard_panic_safety() {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = SdkWakeGuard::new();
            panic!("test panic");
        }));
        assert!(result.is_err());
        assert!(!is_sdk_wake());
    }

    #[test]
    fn wake_tracker_detects_non_sdk_wake() {
        let tracker = WakeTracker::new();
        let noop = Waker::noop();
        let waker = tracker.new_per_poll_waker(noop);

        waker.wake_by_ref();
        assert!(tracker.take_non_sdk_wake());

        let _guard = SdkWakeGuard::new();
        waker.wake_by_ref();
        assert!(!tracker.take_non_sdk_wake());
    }

    #[test]
    fn wake_tracker_cross_thread_detection() {
        let tracker = WakeTracker::new();
        let noop = Waker::noop();
        let waker = tracker.new_per_poll_waker(noop);

        let _guard = SdkWakeGuard::new();

        let handle = std::thread::spawn(move || {
            waker.wake_by_ref();
        });
        handle.join().unwrap();

        assert!(tracker.take_non_sdk_wake());
    }
}
