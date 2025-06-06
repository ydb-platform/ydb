#ifndef SECOND_QUEUE_PML
#define SECOND_QUEUE_PML

#ifndef ELEMENTS
#define ELEMENTS 3
#endif

#include "atomics.pml"

chan second_queue = [ELEMENTS] of {bool};
unsigned second_queue_size:3 = 0;

#define NO_OVERFLOW_SECOND_QUEUE (second_queue_size < 7)


inline push_to_second_queue(value) {
    atomic_increment(second_queue_size);
    second_queue!value;
}

inline pop_from_second_queue(value, is_success) {
    if
    :: second_queue?value ->
        atomic_decrement(second_queue_size);
        is_success = true;
    :: else ->
        is_success = false;
    fi
}

#endif // SECOND_QUEUE_PML
