/* One writer emits two requests, one manager consumes intrusive nodes.
 * Queue publication and full detachment are abstract operations. C++ weak
 * memory, allocation, registry membership and queue internal links are excluded.
 * Safety: a producer cannot overwrite the queue-owned lifetime slot.
 * Liveness: the last published request is eventually observed by the manager.
 */
#ifndef RESET_BEFORE
#define RESET_BEFORE 1
#endif
byte version = 0;
byte observed = 0;
bool queued = false;
bool linked = false;
bool owner_slot = false;
bool writer_done = false;
bool done = false;

proctype Writer() {
    bool already_queued;
    do
    :: version < 2 ->
        version++;
        atomic {
            already_queued = queued;
            queued = true;
        }
        if
        :: !already_queued ->
            assert(!owner_slot);
            owner_slot = true;
            linked = true;
        :: else -> skip;
        fi;
    :: version == 2 -> break;
    od;
    writer_done = true;
}

proctype Manager() {
    do
    :: linked ->
        linked = false;
        assert(owner_slot);
        owner_slot = false;
#if RESET_BEFORE
        queued = false;
#endif
        observed = version;
#if !RESET_BEFORE
        queued = false;
#endif
    :: writer_done && !queued && !linked ->
        assert(observed == 2);
        done = true;
        break;
    od;
}

ltl live_last_request { <> done }

init {
    atomic {
        run Writer();
        run Manager();
    }
}
