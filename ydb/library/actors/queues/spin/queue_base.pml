#ifndef QUEUE_BASE_PML
#define QUEUE_BASE_PML

#ifndef QUEUE_SIZE
#define QUEUE_SIZE 4
#endif

#include "atomics.pml"

typedef Slot {
    unsigned generation:3;
    bool isEmpty;
}

#define EQ_SLOT(slot1, slot2) ((slot1.generation == slot2.generation) && (slot1.isEmpty == slot2.isEmpty))

typedef Queue {
    unsigned tail:4;
    unsigned head:4;
    Slot buffer[QUEUE_SIZE];
}

#define QUEUE_SLOT(counter) queue.buffer[(counter) % QUEUE_SIZE]


inline increment_queue_head(currentHead) {
    blind_atomic_compare_exchange(queue.head, currentHead, currentHead + 1)
}

inline increment_queue_tail(currentTail) {
    blind_atomic_compare_exchange(queue.tail, currentTail, currentTail + 1)
}

inline invalidate_slot(counter, current_generation) {
    atomic {
        QUEUE_SLOT(counter).generation = current_generation + 1;
        QUEUE_SLOT(counter).isEmpty = true;
    }
}

inline save_slot_value(counter, saved_generation) {
    atomic {
        QUEUE_SLOT(counter).isEmpty = false;
        QUEUE_SLOT(counter).generation = saved_generation;
    }
}

inline store_slot(counter, new_generation, new_is_empty) {
    atomic {
        QUEUE_SLOT(counter).generation = new_generation;
        QUEUE_SLOT(counter).isEmpty = new_is_empty;
    }
}

inline copy_slot(source, destination) {
    atomic {
        destination.generation = source.generation;
        destination.isEmpty = source.isEmpty;
    }
}

inline read_slot(counter, destination) {
    atomic {
        copy_slot(QUEUE_SLOT(counter), destination);
    }
}

inline compare_exchange_slot(counter, expectedSlot, new_generation, new_is_empty, success) {
    atomic {
        if
        :: !EQ_SLOT(expectedSlot, QUEUE_SLOT(counter)) ->
            read_slot(counter, expectedSlot);
            success = false;
        :: else ->
            success = true;
            store_slot(counter, new_generation, new_is_empty);
        fi
    }
}

inline JUMP_SLOT_CHANGED(head, slot, label) {
    if
    :: !EQ_SLOT(slot, QUEUE_SLOT(head)) ->
        read_slot(head, slot);
        goto label
    :: else -> skip
    fi
}

inline init_queue() {
    atomic {
        queue.tail = 0;
        queue.head = 0;
        int i = 0;
        do
        :: i < QUEUE_SIZE ->
            invalidate_slot(i, -1);
            i++
        :: i >= QUEUE_SIZE -> break
        od
    }
}

#endif // QUEUE_BASE_PML
