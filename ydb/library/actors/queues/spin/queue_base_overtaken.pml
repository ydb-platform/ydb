#ifndef QUEUE_BASE_PML
#define QUEUE_BASE_PML

#ifndef QUEUE_SIZE
#define QUEUE_SIZE 4
#endif

#include "atomics.pml"

typedef Slot {
    bool isOvertaken;
    bool isEmpty;
}

#define EQ_SLOT(slot1, slot2) ((slot1.isOvertaken == slot2.isOvertaken) && (slot1.isEmpty == slot2.isEmpty))

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

inline clear_slot(counter) {
    atomic {
        QUEUE_SLOT(counter).isOvertaken = false;
        QUEUE_SLOT(counter).isEmpty = true;
    }
}


inline overtake_slot(counter) {
    atomic {
        QUEUE_SLOT(counter).isOvertaken = true;
        QUEUE_SLOT(counter).isEmpty = true;
    }
}


inline save_slot_value(counter) {
    atomic {
        QUEUE_SLOT(counter).isEmpty = false;
        QUEUE_SLOT(counter).isOvertaken = false;
    }
}


inline copy_slot(source, destination) {
    atomic {
        destination.isOvertaken = source.isOvertaken;
        destination.isEmpty = source.isEmpty;
    }
}

inline read_slot(counter, destination) {
    atomic {
        copy_slot(QUEUE_SLOT(counter), destination);
    }
}

inline try_to_save_slot(counter, expectedSlot, success) {
    atomic {
        if
        :: !EQ_SLOT(expectedSlot, QUEUE_SLOT(counter)) ->
            read_slot(counter, expectedSlot);
            success = false;
        :: else ->
            success = true;
            save_slot_value(counter);
        fi
    }
}

inline try_to_overtake_slot(counter, expectedSlot, success) {
    atomic {
        if
        :: !EQ_SLOT(expectedSlot, QUEUE_SLOT(counter)) ->
            read_slot(counter, expectedSlot);
            success = false;
        :: else ->
            success = true;
            overtake_slot(counter);
        fi
    }
}

inline try_to_clear_slot(counter, expectedSlot, success) {
    atomic {
        if
        :: !EQ_SLOT(expectedSlot, QUEUE_SLOT(counter)) ->
            read_slot(counter, expectedSlot);
            success = false;
        :: else ->
            success = true;
            clear_slot(counter);
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
            clear_slot(i);
            i++
        :: i >= QUEUE_SIZE -> break
        od
    }
}

#endif // QUEUE_BASE_PML
