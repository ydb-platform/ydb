/*
 * ErasureNone baseline: one VDisk actor, one logical request, and no XOR
 * fan-out. The model keeps the start/found/diff/result protocol and checks
 * that OK is never returned before the only part is durably written. Delivery
 * is reliable, the payload bytes are omitted, and the local write is
 * abstracted to one transition. Queue failures, retries, and fallback are out
 * of scope.
 */

#define CHANNEL_CAPACITY 2

mtype = { VPATCH_START, FOUND, VPATCH_DIFF, PART_OK };

/* VDisk is the sole disk_in receiver; DSProxy is the sole proxy_in receiver. */
chan disk_in = [CHANNEL_CAPACITY] of { mtype };
chan proxy_in = [CHANNEL_CAPACITY] of { mtype };

/* DSProxy owns the client-visible request lifecycle. */
bool request_started;
bool client_replied;
bool client_ok;

/* VDisk owns the durable state. */
bool part_patched;

proctype VDisk() {
    disk_in?VPATCH_START;
    proxy_in!FOUND;
    disk_in?VPATCH_DIFF;
    part_patched = true;
    proxy_in!PART_OK
}

proctype DSProxy() {
    request_started = true;
    disk_in!VPATCH_START;
    proxy_in?FOUND;
    disk_in!VPATCH_DIFF;
    proxy_in?PART_OK;
    client_ok = true;
    client_replied = true;
    assert(part_patched)
}

ltl safe_ok_requires_part { [] (client_ok -> part_patched) }

/* Verified with weak process fairness; see the run manifest in README.md. */
ltl live_eventual_reply { [] (request_started -> <> client_replied) }

init {
    atomic {
        run DSProxy();
        run VDisk()
    }
}
