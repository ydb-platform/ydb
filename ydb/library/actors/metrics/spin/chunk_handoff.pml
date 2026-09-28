/* One chunk, one writer, one snapshot releaser, one metadata owner.
 * Queue Push/TryPop are abstracted as publication/detachment. The queue's
 * internal linking and C++ weak memory are outside this model. A queue pin
 * prevents reuse while a sealed notification is pending, including Close.
 */
#ifndef QUEUE_PIN
#define QUEUE_PIN 1
#endif
byte pins = 1;
bool sealed_queued = false;
bool retired = false;
bool released_queued = false;
bool writer_done = false;
bool free = false;

inline release_pin() {
    atomic {
        assert(pins > 0);
        pins--;
        if
        :: retired && pins == 0 ->
            assert(!released_queued);
            released_queued = true;
        :: else -> skip;
        fi;
    }
}

proctype Writer() {
    atomic {
        pins = pins + QUEUE_PIN;
        sealed_queued = true;
        writer_done = true;
    }
}

proctype Reader() {
    release_pin();
}

proctype Manager() {
    writer_done;
    do
    :: !retired ->
        atomic {
            retired = true;
            if
            :: pins == 0 ->
                assert(!sealed_queued);
                free = true;
            :: else -> skip;
            fi;
        }
    :: sealed_queued ->
        sealed_queued = false;
#if QUEUE_PIN
        release_pin();
#endif
    :: released_queued ->
        atomic {
            released_queued = false;
            assert(retired && pins == 0 && !sealed_queued && !free);
            free = true;
        }
    :: free -> break;
    od;
}

ltl live_reclaimed { <> free }

init {
    atomic {
        run Writer();
        run Reader();
        run Manager();
    }
}
