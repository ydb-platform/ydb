/*
 * Two chunks, a bounded producer/evictor and concurrent borrowing snapshots.
 * Checks pin/unlink, State-first retirement, last-reader reclamation and reuse
 * over multiple incarnations. PIN_READERS=0 is a negative control that omits
 * the production reader counter while retaining ghost pins for assertions.
 *
 * Publishing a complete chunk is collapsed under the allocation abstraction;
 * concurrent append/publication is checked in snapshot_publication.pml.
 * Victim selection may choose either sealed chunk (timestamps are omitted).
 * Atomic blocks abstract per-line locks, free-list locks or one RMW operation
 * with ghost accounting. Retirement's State store and Readers CAS are separate.
 * Payload contains an incarnation marker. No line deletion, heap generations,
 * weak-memory reorderings, unbounded producers or crash failures are modeled.
 */
#ifndef PIN_READERS
#define PIN_READERS 1
#endif
#ifndef WRITES
#define WRITES 3
#endif
#ifndef READERS
#define READERS 2
#endif
#define CHUNKS 2
#define RETIRING_BIAS (-16)

mtype = { FREE, SEALED, RETIRING };
mtype state[CHUNKS];
short readers[CHUNKS];
byte pins[CHUNKS];
byte epoch[CHUNKS];
byte payload[CHUNKS];
bool linked[CHUNKS];
byte returned[CHUNKS];
byte evicted = 0;

inline publish(c) {
    atomic { state[c] == FREE ->
            assert(pins[c] == 0);
            assert(readers[c] == 0);
            assert(returned[c] == epoch[c]);
            epoch[c]++;
            payload[c] = epoch[c];
            state[c] = SEALED;
            linked[c] = true
        }
}

inline unlink_chunk(c, selected) {
    atomic { linked[c] && state[c] == SEALED ->
            linked[c] = false;
            selected = c
        }
}

inline return_chunk(c) {
    atomic {
        assert(!linked[c]);
        assert(state[c] == RETIRING);
        assert(readers[c] == RETIRING_BIAS);
        assert(pins[c] == 0);
        assert(returned[c] + 1 == epoch[c]);
        returned[c]++;
        readers[c] = 0;
        state[c] = FREE
    }
}

proctype Writer() {
    byte i = 0;
    do
    :: i < WRITES ->
        if
        :: publish(0)
        :: publish(1)
        fi;
        i++
    :: else ->
        break
    od
}

proctype Evictor() {
    byte selected;
    short expected;
    do
    :: evicted < WRITES ->
        if
        :: unlink_chunk(0, selected)
        :: unlink_chunk(1, selected)
        fi;
        state[selected] = RETIRING;
        expected = readers[selected];
        do
        :: atomic { readers[selected] == expected ->
                assert(expected >= 0);
                readers[selected] = RETIRING_BIAS + expected;
                break
            }
        :: atomic { readers[selected] != expected ->
                expected = readers[selected]
            }
        od;
        if
        :: expected == 0 ->
            return_chunk(selected)
        :: else ->
            skip
        fi;
        evicted++
    :: else ->
        break
    od
}

proctype SnapshotReader() {
    byte c = 0;
    bool held[CHUNKS];
    byte saved_epoch[CHUNKS];
    short previous;

    do
    :: c < CHUNKS ->
        atomic {
            if
            :: linked[c] ->
                assert(readers[c] >= 0);
#if PIN_READERS
                readers[c]++;
#endif
                pins[c]++;
                saved_epoch[c] = epoch[c];
                held[c] = true
            :: else ->
                skip
            fi
        }
        c++
    :: else ->
        break
    od;
    c = 0;
    do
    :: c < CHUNKS ->
        if
        :: held[c] ->
            assert(epoch[c] == saved_epoch[c]);
            assert(payload[c] == saved_epoch[c]);
            atomic {
                assert(pins[c] > 0);
                pins[c]--;
#if PIN_READERS
                previous = readers[c];
                readers[c]--;
#endif
            }
#if PIN_READERS
            if
            :: previous == RETIRING_BIAS + 1 ->
                return_chunk(c)
            :: else ->
                skip
            fi
#endif
        :: else ->
            skip
        fi;
        c++
    :: else ->
        break
    od
}

/* Finite processes; no polling self-loops or fairness assumption. */
ltl live_all_returned {
    <> (evicted == WRITES && state[0] == FREE && state[1] == FREE)
}

init {
    byte i = 0;
    atomic {
        state[0] = FREE;
        state[1] = FREE;
        run Writer();
        run Evictor();
        do
        :: i < READERS ->
            run SnapshotReader();
            i++
        :: else ->
            break
        od
    }
}
