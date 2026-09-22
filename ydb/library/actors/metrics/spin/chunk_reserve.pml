/*
 * Two single-writer lines and one manager, each with a two-slot SPSC reserve.
 * Slot access and index publication are separate transitions. Three pops per
 * writer; every pop exchanges the common notification flag, then sends only
 * if it was clear. The manager clears before scanning both queues.
 * Checks FIFO publication, slot reuse, capacity and eventual replenishment.
 * SC abstraction of the C++ acquire/release handoffs; no allocation failure,
 * shutdown, eviction or weak-memory execution is modeled. Delivery is reliable.
 * RESET_AFTER=1 is the lost-wakeup negative control.
 */
#ifndef RESET_AFTER
#define RESET_AFTER 0
#endif
#define LINES 2
#define CAPACITY 2
#define POPS 3

typedef Queue {
    byte slots[CAPACITY];
    byte read;
    byte write;
}
Queue queues[LINES];
bool scheduled = false;
byte events = 0;
byte done = 0;

proctype Writer(byte id) {
    byte read;
    byte write;
    byte value;
    byte consumed = 0;
    bool send;

    do
    :: consumed < POPS ->
        read = queues[id].read;
        write = queues[id].write;
        if
        :: read != write ->
            value = queues[id].slots[read % CAPACITY];
            assert(value == read + 1);
            queues[id].read = read + 1;
            atomic {
                send = !scheduled;
                scheduled = true
            }
            if
            :: send -> events++
            :: else -> skip
            fi;
            consumed++
        :: else -> skip
        fi
    :: else -> break
    od;
    done++
}

proctype Manager() {
    byte id;
    byte read;
    byte write;

end_wait:
    do
    :: events > 0 ->
        events--;
#if !RESET_AFTER
        scheduled = false;
#endif
        id = 0;
        do
        :: id < LINES ->
            write = queues[id].write;
            read = queues[id].read;
            if
            :: write - read < CAPACITY ->
                queues[id].slots[write % CAPACITY] = write + 1;
                queues[id].write = write + 1;
                assert(queues[id].write - queues[id].read <= CAPACITY)
            :: else -> id++
            fi
        :: else -> break
        od;
#if RESET_AFTER
        scheduled = false;
#endif
    :: done == LINES && events == 0 && !scheduled ->
        assert(queues[0].write - queues[0].read == CAPACITY);
        assert(queues[1].write - queues[1].read == CAPACITY);
        break
    od
}

ltl refilled {
    <> (done == LINES && queues[0].write - queues[0].read == CAPACITY
        && queues[1].write - queues[1].read == CAPACITY)
}

init {
    byte id = 0;
    atomic {
        do
        :: id < LINES ->
            queues[id].slots[0] = 1;
            queues[id].slots[1] = 2;
            queues[id].write = CAPACITY;
            run Writer(id);
            id++
        :: else -> break
        od;
        run Manager()
    }
}
