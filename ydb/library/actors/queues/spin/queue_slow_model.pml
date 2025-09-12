#ifndef QUEUE_SIZE
#define QUEUE_SIZE 1
#endif

#ifndef CONSUMERS
#define CONSUMERS 2
#endif

#ifndef PRODUCERS
#define PRODUCERS 2
#endif

#ifndef ELEMENTS
#define ELEMENTS 1
#endif

#include "atomics.pml"
#include "second_queue.pml"
#include "queue_base.pml"

typedef Producer {
    bool success_push = false;
}

typedef Consumer {
    bool success_pop = false;
}

Queue queue;
Producer producers[PRODUCERS];
Consumer consumers[CONSUMERS];


inline try_increment_tail(id) {
    atomic_compare_exchange(queue.tail, currentTail, currentTail + 1, is_success);
    if
    :: is_success ->
        currentTail = currentTail + 1;
    :: else -> skip
    fi
}

inline try_push(id) {
    printf("Producer %d: try_push start\n", id);
    // ui64 currentTail = Tail.load(std::memory_order_acquire);
    atomic {
        atomic_load(queue.tail, currentTail);
        printf("Producer %d: try to push. tail=%d, generation=%d\n", id, currentTail, generation);
    }

    // for (ui32 it = 0;; ++it) {
    do
    :: true ->
        // ui32 generation = currentTail / MaxSize;
        // std::atomic<ui64> &currentSlot = Buffer[currentTail % MaxSize];
        // TSlot slot;
        // ui64 expected = TSlot::MakeEmpty(generation);
        atomic {
            generation = currentTail / QUEUE_SIZE;
            currentSlot.generation = generation;
            currentSlot.isEmpty = true;
        }

        // do {
        //    if (currentSlot.compare_exchange_weak(expected, val)) {
        //        Tail.compare_exchange_strong(currentTail, currentTail + 1);
        //        return true;
        //    }
        //    slot = TSlot::Recognise(expected);
        // } while (slot.Generation <= generation && slot.IsEmpty);
        do
        :: true ->
            atomic {
                compare_exchange_slot(currentTail, currentSlot, generation, false, is_success);
                if
                :: is_success ->
                    printf("Producer %d: success push. tail=%d, generation=%d, slot_generation=%d, slot_isEmpty=%d\n", id, currentTail, generation, currentSlot.generation, currentSlot.isEmpty);
                    producers[id].success_push = true;
                    goto try_push_success
                :: else ->
                    printf("Producer %d: failed compare_exchange_slot. tail=%d, generation=%d, slot_generation=%d, slot_isEmpty=%d\n", id, currentTail, generation, currentSlot.generation, currentSlot.isEmpty);
                fi
                if
                :: currentSlot.generation <= generation && currentSlot.isEmpty ->
                    skip
                :: else ->
                    printf("Producer %d: try to write to slot %d but it is impossible. tail=%d, generation=%d, slot_generation=%d, slot_isEmpty=%d\n", id, currentTail % QUEUE_SIZE, currentTail, generation, currentSlot.generation, currentSlot.isEmpty);
                    break
                fi
            }
        od

        // if (!slot.IsEmpty) {
        //    ui64 currentHead = Head.load(std::memory_order_acquire);
        //    if (currentHead + MaxSize <= currentTail + std::min<ui64>(64, MaxSize - 1)) {
        //        return false;
        //    }
        // }
        if
        :: !currentSlot.isEmpty ->
            atomic_load(queue.head, currentHead);
            atomic {
                printf("Producer %d: check slow push. head=%d, tail=%d, generation=%d\n", id, currentHead, currentTail, generation);
                if
                :: currentHead + QUEUE_SIZE <= currentTail ->
                    printf("Producer %d: change to slow push, break, head=%d, tail=%d, generation=%d\n", id, currentHead, currentTail, generation);
                    producers[id].success_push = false;
                    goto try_push_end
                :: else -> skip
                fi
            }
        :: else -> skip
        fi

        // TryIncrementTail(currentTail);
        atomic {
            try_increment_tail(id);
            printf("Producer %d: to next iteration. tail=%d, generation=%d\n", id, queue.tail, currentTail + 1, generation);
        }
    od

try_push_success:
    atomic {
        blind_atomic_compare_exchange(queue.tail, currentTail, currentTail + 1);
        producers[id].success_push = true;
    }

try_push_end:
    printf("Producer %d: try_push end\n", id);
}

inline try_increment_head(id) {
    atomic_compare_exchange(queue.head, currentHead, currentHead + 1, is_success);
    if
    :: is_success ->
        currentHead = currentHead + 1;
    :: else -> skip
    fi
}

inline try_pop(id) {
    printf("Consumer %d: try_pop start\n", id);
    // if (!currentHead) {
    //     currentHead = Head.load(std::memory_order_acquire);
    // }
    // ui32 generation = currentHead / MaxSize;
    atomic {
        atomic_load(queue.head, currentHead);
        generation = currentHead / QUEUE_SIZE;
        printf("Consumer %d: try to pop. head=%d, generation=%d\n", id, currentHead, generation);
    }

    // for (ui32 it = 0;; ++it) {
    do
    :: true ->
try_pop_iteration_start:
        // generation = currentHead / MaxSize;
        // std::atomic<ui64> &currentSlot = Buffer[currentHead % MaxSize];
        // ui64 expected = currentSlot.load(std::memory_order_relaxed);
        // TSlot slot = TSlot::Recognise(expected);

        atomic {
            generation = currentHead / QUEUE_SIZE;
            read_slot(currentHead, currentSlot);
            printf("Consumer %d: read slot %d. head=%d, generation=%d, slot_generation=%d, slot_isEmpty=%d\n", id, currentHead % QUEUE_SIZE, currentHead, generation, currentSlot.generation, currentSlot.isEmpty);
        }

        // if (slot.Generation > generation) {
        //     TryIncrementHead(currentHead);
        //     continue;
        // }
        atomic {
            if
            :: currentSlot.isEmpty && currentSlot.generation > generation ->
                printf("Consumer %d: see slot %d from future generation %d. head=%d, generation=%d\n", id, currentHead, currentSlot.generation, currentHead, generation);
                try_increment_head(id);
                goto try_pop_iteration_start;
            :: else -> skip
            fi
        }

        // while (generation > slot.Generation || !slot.IsEmpty) {
        //    if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
        //        if (!slot.IsEmpty) {
        //            TryIncrementHead(currentHead);
        //            return slot.Value;
        //        }
        //        break;
        //    }
        //    slot = TSlot::Recognise(expected);
        // }
        do
        :: generation > currentSlot.generation || !currentSlot.isEmpty ->
            atomic {
                compare_exchange_slot(currentHead, currentSlot, generation + 1, true, is_success);
                if
                :: is_success && !currentSlot.isEmpty ->
                    printf("Consumer %d: success pop. head=%d, generation=%d\n", id, currentHead, generation);
                    invalidate_slot(currentHead, generation);
                    goto try_pop_success
                :: is_success && currentSlot.isEmpty ->
                    invalidate_slot(currentHead, generation - 1);
                    currentSlot.generation = generation;
                    break;
                :: else -> skip
                fi
            }
        :: else -> break;
        od

        // if (slot.Generation > generation) {
        //     TryIncrementHead(currentHead);
        //     continue;
        // }
        if
        :: currentSlot.isEmpty && currentSlot.generation > generation ->
            atomic {
                try_increment_head(id);
                printf("Consumer %d: seen slot from future generation %d. head=%d, generation=%d\n", id, currentSlot.generation, currentHead, generation);
                goto try_pop_iteration_start;
            }
        :: else -> skip
        fi

        // ui64 currentTail = Tail.load(std::memory_order_acquire);
        // if (currentTail <= currentHead) {
        //     return std::nullopt;
        // }
        atomic {
            atomic_load(queue.tail, currentTail);
            if
            :: currentTail <= currentHead ->
                printf("Consumer %d: tail %d is less than head %d. head=%d, generation=%d\n", id, currentTail, currentHead, currentHead, generation);
                consumers[id].success_pop = false;
                goto try_pop_end
            :: else -> skip
            fi
        }

        // while (slot.Generation <= generation || !slot.IsEmpty) {
        //    if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
        //        if (!slot.IsEmpty) {
        //            TryIncrementHead(currentHead);
        //            return slot.Value;
        //        }
        //        break;
        //    }
        //    slot = TSlot::Recognise(expected);
        // }
        do
        :: currentSlot.generation <= generation || !currentSlot.isEmpty ->
            atomic {
                compare_exchange_slot(currentHead, currentSlot, generation + 1, true, is_success);
                if
                :: is_success && !currentSlot.isEmpty ->
                    printf("Consumer %d: success pop. head=%d, generation=%d\n", id, currentHead, generation);
                    invalidate_slot(currentHead, generation);
                    goto try_pop_success
                :: is_success && currentSlot.isEmpty ->
                    invalidate_slot(currentHead, generation);
                    break;
                :: else -> skip
                fi
            }
        :: else -> break;
        od

        // currentHead = Head.load(std::memory_order_acquire);
        atomic {
            atomic_load(queue.head, currentHead);
        }
    od


try_pop_success:
    atomic {
        printf("Consumer %d: try_to_increment_head. head=%d, generation=%d\n", id, currentHead, generation);
        try_increment_head(id);
    }
try_pop_end:
    printf("Consumer %d: try_pop end\n", id);
    skip
}

unsigned pushed_elements:3 = 0;
unsigned planned_pushed_elements:3 = ELEMENTS;
unsigned popped_elements:3 = 0;
unsigned planned_popped_elements:3 = ELEMENTS;


proctype ProducerProc(unsigned id:2) {
    unsigned currentTail:4;
    unsigned currentHead:4;
    unsigned generation:3;
    Slot currentSlot;
    bool is_success = false;
    do
    :: pushed_elements < ELEMENTS ->
        atomic {
            if
            :: planned_pushed_elements > 0 ->
                printf("Producer %d: decrement planned_pushed_elements from %d to %d\n", id, planned_pushed_elements, planned_pushed_elements - 1);
                planned_pushed_elements = planned_pushed_elements - 1;
            :: else ->
                printf("Producer %d: planned_pushed_elements == 0, break\n", id);
                break
            fi
        }

        if
        :: second_queue_size > 0 ->
            printf("Producer %d: push to second queue\n", id);
            push_to_second_queue(true);
            atomic {
                printf("Producer %d: success push to second queue, increment pushed_elements from %d to %d\n", id, pushed_elements, pushed_elements + 1);
                pushed_elements = pushed_elements + 1;
            }
        :: else ->
            try_push(id);
            if
            :: producers[id].success_push ->
                atomic {
                    printf("Producer %d: success push, increment pushed_elements from %d to %d\n", id, pushed_elements, pushed_elements + 1);
                    pushed_elements = pushed_elements + 1;
                    producers[id].success_push = false;
                }
            :: else ->
                printf("Producer %d: failed push, push to second queue\n", id);
                push_to_second_queue(true);
                atomic {
                    printf("Producer %d: success push to second queue, increment pushed_elements from %d to %d\n", id, pushed_elements, pushed_elements + 1);
                    pushed_elements = pushed_elements + 1;
                }
            fi
        fi
    :: else ->
        break
    od
    printf("Producer %d: end\n", id);
}


proctype ConsumerProc(unsigned id:2) {
    unsigned currentTail:4;
    unsigned currentHead:4;
    unsigned generation:3;
    Slot currentSlot;
    bool is_success = false;
    unsigned last_seen_planned_pushed_elements:3 = 0;
    unsigned last_seen_pushed_elements:3 = 0;
consumer_proc_retry:
    do
    :: popped_elements < ELEMENTS &&
        (last_seen_planned_pushed_elements != planned_pushed_elements ||
        last_seen_pushed_elements != pushed_elements)
    ->
        atomic {
            last_seen_planned_pushed_elements = planned_pushed_elements;
            last_seen_pushed_elements = pushed_elements;
            printf("Consumer %d: seen planned_pushed_elements %d and pushed_elements %d\n", id, planned_pushed_elements, pushed_elements);
        }

        bool tmp_value;
        if
        :: planned_popped_elements > 0 ->
            atomic {
                if
                :: planned_popped_elements > 0 ->
                    printf("Consumer %d: decrement planned_popped_elements from %d to %d\n", id, planned_popped_elements, planned_popped_elements - 1);
                    planned_popped_elements = planned_popped_elements - 1;
                :: else ->
                    printf("Consumer %d: planned_popped_elements == 0, goto consumer_proc_retry\n", id);
                    goto consumer_proc_retry
                fi
            }
        :: popped_elements == ELEMENTS ->
            printf("Consumer %d: popped_elements == ELEMENTS, break\n", id);
            break
        fi

        try_pop(id);

        if
        :: consumers[id].success_pop ->
            atomic {
                printf("Consumer %d: success pop, increment popped_elements from %d to %d\n", id, popped_elements, popped_elements + 1);
                popped_elements = popped_elements + 1;
                consumers[id].success_pop = false;
            }
        :: else ->
            if
            :: second_queue?tmp_value ->
                atomic {
                    printf("Consumer %d: pop from second queue, decrement second_queue_size from %d to %d\n", id, second_queue_size, second_queue_size - 1);
                    second_queue_size = second_queue_size - 1;
                    popped_elements = popped_elements + 1;
                }
            :: timeout ->
                atomic {
                    printf("Consumer %d: timeout, increment planned_popped_elements from %d to %d\n", id, planned_popped_elements, planned_popped_elements + 1);
                    planned_popped_elements = planned_popped_elements + 1;
                }
            fi
        fi

    :: popped_elements >= ELEMENTS ->
        break
    od
    printf("Consumer %d: end\n", id);
}


inline run_producers() {
    atomic {
        do
        :: producer_count > 0 ->
            producer_count = producer_count - 1;
            run ProducerProc(producer_count);
        :: else -> break
        od
    }
}

inline run_consumers() {
    atomic {
        do
        :: consumer_count > 0 ->
            consumer_count = consumer_count - 1;
            run ConsumerProc(consumer_count);
        :: else -> break
        od
    }
}

unsigned consumer_count:2 = CONSUMERS;
unsigned producer_count:2 = PRODUCERS;

init {
    atomic {
        init_queue();
        run_producers();
        run_consumers();
    }
}

#define PUSHED_SLOT_ALWAYS_WILL_BE_POPPED(idx) (idx >= QUEUE_SIZE || [](!queue.buffer[idx].isEmpty -> <>(queue.buffer[idx].isEmpty)))


ltl no_overflow {
    [] ((queue.head <= queue.tail + PRODUCERS) &&
        (queue.tail - queue.head <= QUEUE_SIZE + CONSUMERS) &&
        (queue.tail <= ELEMENTS * 2) &&
        (queue.head <= ELEMENTS * 2))
}

ltl pushed_slot_always_will_be_popped {
    PUSHED_SLOT_ALWAYS_WILL_BE_POPPED(0)
    && PUSHED_SLOT_ALWAYS_WILL_BE_POPPED(1)
    && PUSHED_SLOT_ALWAYS_WILL_BE_POPPED(2)
}
