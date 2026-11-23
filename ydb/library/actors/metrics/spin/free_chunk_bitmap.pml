/*
 * Two free chunks share one atomic bitmap word; two workers allocate and return
 * them twice. Loads and CAS are separate transitions. Ghost owner/clean fields
 * check exclusive allocation and cleanup-before-availability. No payload,
 * snapshot pinning or eviction is modeled: return is called only by the owner
 * after the existing retirement/pinning protocol has permitted reclamation.
 * SC abstraction; C++ uses release publication and acquire successful CAS.
 * USE_CAS=0 deliberately replaces CAS with an unchecked store (negative control).
 */
#ifndef USE_CAS
#define USE_CAS 1
#endif
#define WORKERS 2
#define ROUNDS 2
#define CHUNKS 2
#define ALL_FREE 3

byte available = ALL_FREE;
byte owner[CHUNKS];
bool clean[CHUNKS];
byte done = 0;

proctype Worker(byte id) {
    byte round = 0;
    byte bits;
    byte chunk;
    bool acquired;

    do
    :: round < ROUNDS ->
        acquired = false;
        bits = available;
        do
        :: bits != 0 ->
            if
            :: (bits & 1) != 0 -> chunk = 0
            :: else -> chunk = 1
            fi;
            atomic {
                if
#if USE_CAS
                :: available == bits ->
#else
                :: true ->
#endif
                    available = bits & ~(1 << chunk);
                    assert(owner[chunk] == 0);
                    assert(clean[chunk]);
                    owner[chunk] = id;
                    acquired = true
#if USE_CAS
                :: else -> bits = available
#endif
                fi
            }
            if
            :: acquired -> break
            :: else -> skip
            fi
        :: else -> break
        od;
        if
        :: acquired ->
            clean[chunk] = false;
            assert(owner[chunk] == id);
            clean[chunk] = true;
            owner[chunk] = 0;
            atomic { available = available | (1 << chunk) }
        :: else -> skip
        fi;
        round++
    :: else -> break
    od;
    atomic {
        done++;
        if
        :: done == WORKERS ->
            assert(available == ALL_FREE);
            assert(owner[0] == 0 && owner[1] == 0)
        :: else -> skip
        fi
    }
}

ltl live_returned { <> (done == WORKERS && available == ALL_FREE) }

init {
    atomic {
        clean[0] = true;
        clean[1] = true;
        run Worker(1);
        run Worker(2)
    }
}
