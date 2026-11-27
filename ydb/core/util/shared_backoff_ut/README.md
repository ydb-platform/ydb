# Shared admission scenarios

`TSharedBackoff` wraps standard `TBackoff` and records one dependency's volatile
health. It does not store callers, schedule timers, replay operations or use
coroutines. Access belongs to its owning actor.

Call `TryAcquire(now, reservation)` before dispatch. A returned version permits
one call; an empty result means the owner must wait until `GetNextAllowedAt()`
and recheck. Pass the version to `OnSuccess` or `OnFailure`; the owner then
reconsiders pending callers. The finite positive reservation bounds a missing
outcome; actual request lifetime must be bounded as well.

The helper tests use explicit monotonic timestamps:

- Healthy callers proceed concurrently with the same version.
- At the exact cooldown boundary only one recovery call proceeds.
- Stale outcomes cannot change newer failure state.
- Current success releases all callers; missing outcomes allow another probe at
  the reservation boundary and fence the older probe.
- Retired wakeups recheck current state and cannot bypass a newer cooldown.
- Repeated failures use standard jitter and the configured cap.

Waiting and cancellation belong to the service. Its tests in
`security/iam_delegation/ut/iam_delegation_actor_ut.cpp` exercise token readiness,
shared admission, exact polling deadlines, stale RPC replies, bulk expiry of
512 and 4096 waiting requests with a surviving caller, graceful poison and hard
runtime teardown. Each request wait has independent cancellation;
expiry of any request cannot remove another request's wakeup.
