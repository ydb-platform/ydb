/*
 * ErasureNone baseline: one VDisk actor, no XOR fan-out. The model keeps the
 * same start/found/diff/result protocol as VPatch and checks that OK is never
 * returned before the only part is durably written.
 */

mtype = { VPATCH_START, FOUND, VPATCH_DIFF, PART_OK };

chan disk_in = [2] of { mtype };
chan proxy_in = [2] of { mtype };

bool request_started;
bool part_patched;
bool client_replied;
bool client_ok;

proctype VDisk()
{
    disk_in?VPATCH_START;
    proxy_in!FOUND;
    disk_in?VPATCH_DIFF;
    part_patched = true;
    proxy_in!PART_OK
}

proctype DSProxy()
{
    request_started = true;
    disk_in!VPATCH_START;
    proxy_in?FOUND;
    disk_in!VPATCH_DIFF;
    proxy_in?PART_OK;
    client_ok = true;
    client_replied = true;
    assert(part_patched)
}

init
{
    atomic {
        run DSProxy();
        run VDisk()
    }
}

ltl safe_ok_requires_part { [] (client_ok -> part_patched) }
ltl live_eventual_reply { [] (request_started -> <> client_replied) }
