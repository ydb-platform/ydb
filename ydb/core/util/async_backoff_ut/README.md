`TAsyncBackoff` coordinates callers of one dependency inside one actor. It wraps
the standard `TBackoff` and keeps waiting state in coroutine frames. It does not
persist tablet state or retry an external operation itself.

Call `co_await backoff.Wait(requestTimeout)` before dispatch and pass its returned
version to `OnSuccess` or `OnFailure` when the request finishes. Apply the same
finite, positive timeout to the request. A stale report affects neither the
schedule nor the current backoff position; processing its actual operation result
remains the caller's responsibility. All access belongs to the owning actor,
which must outlive its waiters.

The tests use the real actor coroutine runtime with virtual time:

- Healthy calls proceed concurrently without timers.
- A shared failure delays every caller using standard jitter and capped delays.
- At the exact cooldown boundary, one recovery call advances the version before
  dispatch. Other callers continue waiting on one active timer.
- Stale or duplicate outcomes cannot move the schedule or reset recovery.
- Current success wakes all waiters and resets the backoff; healthy success does
  not invalidate concurrent calls.
- A missing outcome permits the next recovery call at the request timeout.
- Cancelling the timer waiter transfers its responsibility. Cancelling all
  waiters permits a fresh waiter; caller deadlines do not strand others.
- A retired timer cannot release callers during a newer cooldown.
- Graceful death and hard mailbox teardown destroy waiting frames safely.
