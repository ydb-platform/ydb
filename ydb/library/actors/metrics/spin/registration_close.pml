/*
 * One queued registration, its single writer, and the manager actor. The handle
 * exists before the reader; publishing Ready transfers initialized reader data.
 * Close can happen before registration or between initialization and publication.
 * No writer operation overlaps its own Close. SC abstraction of the release CAS
 * and acquire status loads; delivery is reliable, shutdown/pools are omitted.
 * USE_READY_CAS=0 models an unconditional Ready store (negative control).
 */
#ifndef USE_READY_CAS
#define USE_READY_CAS 1
#endif
mtype = { Pending, Ready, Closed };
mtype status = Pending;
bool initialized = false;
bool owned = false;
bool closed = false;
bool writer_done = false;
bool manager_done = false;
byte writes = 0;

proctype Writer() {
    do
    :: writes < 2 && status == Ready ->
        assert(initialized && owned);
        writes++
    :: true ->
        atomic { closed = true; status = Closed }
        writer_done = true;
        break
    od
}

proctype Manager() {
    if
    :: status == Pending ->
        owned = true;
        initialized = true;
        atomic {
#if USE_READY_CAS
            if
            :: status == Pending -> status = Ready
            :: else -> skip
            fi
#else
            status = Ready
#endif
            assert(!closed || status != Ready)
        }
    :: else -> skip
    fi;
    if
    :: owned ->
        status == Closed;
        owned = false
    :: else -> skip
    fi;
    manager_done = true
}

ltl closed_reclaimed { <> (writer_done && manager_done && !owned) }

init {
    atomic { run Writer(); run Manager() }
}
