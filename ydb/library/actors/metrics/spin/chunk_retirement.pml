/*
 * Model of TInMemoryMetricsBackend retirement in PR #36913 (8fb1ff26d393).
 * STATE_FIRST=0 models the original CAS-before-State order; 1 swaps the order.
 * One chunk, two evictors, one reuse, INITIAL_READERS existing pins and one
 * optional concurrent snapshot pin. Both retirements use the selected order.
 *
 * Pin versus unlink is serialized by Storage.Lock. Heap selection, free-list
 * publication and acquisition are serialized by VictimLock. Atomic blocks
 * below abstract these critical sections or one atomic RMW plus ghost state.
 * CAS failures reload Readers; spurious failures are omitted as stuttering.
 * Releases may run between every load, CAS and State store in retirement.
 *
 * Epoch, pins and returns are ghost accounting, not additional synchronization.
 * The next writer abstracts any allocator, including the stealing writer.
 * Payload, timestamps, line deletion, full heap ordering and the post-retirement
 * free-list fast path are outside scope. This is an SC interleaving model, not
 * a model of the full C++ memory model or the entire backend.
 */
#ifndef STATE_FIRST
#define STATE_FIRST 1
#endif
#ifndef INITIAL_READERS
#define INITIAL_READERS 2
#endif
#define RETIRING_BIAS (-16)

mtype = { FREE, WRITABLE, SEALED, RETIRING };
mtype state = SEALED;
short readers = INITIAL_READERS;
byte pins = INITIAL_READERS;
byte epoch = 0;
bool linked = true;
bool in_heap = true;
bool in_free_list = false;
byte returns = 0;

inline store_retiring(owned_epoch) {
    atomic {
        state = RETIRING;
        /* An old retirement must not mutate a returned or reused chunk. */
        assert(epoch == owned_epoch);
        assert(!in_free_list);
        assert(!in_heap)
    }
}

inline return_to_free() {
    /* ReturnChunkToFree holds VictimLock, including FinalizeReturnedChunk. */
    atomic {
        assert(readers == RETIRING_BIAS);
        assert(pins == 0);
        assert(!linked && !in_heap && !in_free_list);
        assert(returns == epoch);
        state = FREE;
        readers = 0;
        in_free_list = true;
        returns++
    }
}

inline release_pin(pinned_epoch, previous_readers) {
    atomic {
        assert(epoch == pinned_epoch);
        assert(pins > 0);
        previous_readers = readers;
        readers--;
        pins--
    }
    if
    :: previous_readers == RETIRING_BIAS + 1 ->
        return_to_free()
    :: else ->
        skip
    fi
}

proctype ExistingReader() {
    short previous_readers;
    release_pin(0, previous_readers)
}

proctype SnapshotReader() {
    byte pinned_epoch;
    short previous_readers;
    bool pinned = false;

    if
    :: atomic { linked ->
            assert(readers >= 0);
            pinned_epoch = epoch;
            readers++;
            pins++;
            pinned = true
        }
    :: skip
    fi;
    if
    :: pinned ->
        release_pin(pinned_epoch, previous_readers)
    :: else ->
        skip
    fi
}

proctype Evictor() {
    byte owned_epoch;
    short expected_readers;

    atomic { in_heap ->
            assert(state == SEALED);
            in_heap = false;
            owned_epoch = epoch
        }
    atomic {
        assert(epoch == owned_epoch);
        assert(linked);
        linked = false
    }
#if STATE_FIRST
    store_retiring(owned_epoch);
#endif
    expected_readers = readers;
    do
    :: atomic { readers == expected_readers ->
            assert(expected_readers >= 0);
            readers = RETIRING_BIAS + expected_readers;
            break
        }
    :: atomic { readers != expected_readers ->
            expected_readers = readers
        }
    od;
#if !STATE_FIRST
    store_retiring(owned_epoch);
#endif
    if
    :: expected_readers == 0 ->
        return_to_free()
    :: else ->
        skip
    fi
}

proctype Writer() {
    atomic { in_free_list ->
            assert(state == FREE);
            assert(pins == 0);
            assert(readers == 0);
            in_free_list = false;
            epoch++
        }
    /* ResetChunkForWrite and attaching under Storage.Lock. */
    atomic {
        state = WRITABLE;
        linked = true
    }
    state = SEALED;
    atomic {
        assert(state == SEALED);
        in_heap = true
    }
}

/* Finite producers/readers; all enabled operations eventually terminate.
 * CAS retries require a concurrent change and cannot spin forever on their own.
 */
ltl live_reclaimed { <> (returns == 2) }

init {
    byte i = 0;
    atomic {
        run Evictor();
        run Evictor();
        run Writer();
        run SnapshotReader();
        do
        :: i < INITIAL_READERS ->
            run ExistingReader();
            i++
        :: else ->
            break
        od
    }
}
