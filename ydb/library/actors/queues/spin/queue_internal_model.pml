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
#define ELEMENTS 2
#endif

#include "atomics.pml"
#include "second_queue.pml"
#include "queue_base_overtaken.pml"

typedef Producer {
    bool success_push = false;
}

typedef Consumer {
    bool success_pop = false;
}

Queue queue;
Producer producers[PRODUCERS];
Consumer consumers[CONSUMERS];

unsigned overtaken_slots:3 = 0;
chan overtaken_slots_chan = [ELEMENTS] of { unsigned };


inline try_push(unsigned id:2) {

    printf("Producer %d: try_push start\n", id);

    // for (ui32 it = 0;; ++it) {
    do
    :: true ->
        // ui64 currentTail = Tail.fetch_add(1, std::memory_order_relaxed);
        atomic {
            currentTail = queue.tail;
            queue.tail = currentTail + 1;
            printf("Producer %d: to next iteration. tail=%d, generation=%d\n", id, queue.tail, currentTail);
        }

        // ui64 slotIdx = ConvertIdx(currentTail);
        // std::atomic<ui64> &currentSlot = Buffer[slotIdx];
        // TSlot slot;
        // ui64 expected = TSlot::MakeEmpty(0);
        atomic {
            currentSlot.generation = 0;
            currentSlot.isEmpty = true;
        }

        // do {
        //    if (currentSlot.compare_exchange_strong(expected, val, std::memory_order_acq_rel)) {
        //        if (slot.IsOvertaken) {
        //            AddOvertakenSlot(slotIdx);
        //            OvertakenSlots.fetch_add(1, std::memory_order_acq_rel);
        //        }
        //        return true;
        //    }
        //    slot = TSlot::Recognise(expected);
        //} while (slot.IsEmpty);
        do
        :: currentSlot.isEmpty ->
            try_to_save_slot(currentTail, currentSlot, is_success);
            atomic {
                if
                :: is_success ->
                    printf("Producer %d: success push. tail=%d, generation=%d, slot_generation=%d, slot_isEmpty=%d\n", id, currentTail, generation, currentSlot.generation, currentSlot.isEmpty);
                    producers[id].success_push = true;
                    if
                    :: currentSlot.isOvertaken ->
                        overtaken_slots_chan ! currentTail;
                        overtaken_slots = overtaken_slots + 1;
                    :: else -> skip
                    fi
                    goto try_push_end
                :: else -> skip
                fi
            }
        :: else ->
            printf("Producer %d: slot is not empty, break\n", id);
            break
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
    od

try_push_end:
    printf("Producer %d: try_push end\n", id);

}

inline try_pop(unsigned id:2) {
    printf("Consumer %d: try_pop start\n", id);

    /*
    ui64 overtakenSlots = OvertakenSlots.load(std::memory_order_acquire);
    */
    atomic {
        overtakenSlots = overtaken_slots;
    }

    if
    :: overtakenSlots > 0 ->
        atomic {
            is_success = false;
            if
            :: overtakenSlots > 0 ->
                overtakenSlots = overtakenSlots - 1;
                is_success = true;
            :: else -> skip
            fi
        }
        if
        :: is_success ->
            overtaken_slots_chan ! overtakenSlots;
        :: else -> skip
        fi
    :: else -> skip
    fi

    /*
    if (overtakenSlots) {
        bool success = false;
        while (overtakenSlots) {
            if (OvertakenSlots.compare_exchange_strong(overtakenSlots, overtakenSlots - 1, std::memory_order_acq_rel)) {
                success = true;
                break;
            }
            SpinLockPause();
        }
        if (success) {
            for (;;) {
                if (auto el = TryPopFromOvertakenSlots()) {
                    OBSERVE(SuccessOvertakenPop);
                    return el;
                }
                SpinLockPause();
            }
        }
    }

    for (ui32 it = 0;; ++it) {
        OBSERVE_WITH_CONDITION(LongFastPop10It, it == 10);
        OBSERVE_WITH_CONDITION(LongFastPop100It, it == 100);
        OBSERVE_WITH_CONDITION(LongFastPop1000It, it == 1000);

        ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);
        OBSERVE(AfterReserveSlotInFastPop);
        ui32 generation = currentHead / MaxSize;

        ui64 slotIdx = ConvertIdx(currentHead);
        std::atomic<ui64> &currentSlot = Buffer[slotIdx];

        ui64 expected = currentSlot.load(std::memory_order_relaxed);
        TSlot slot = TSlot::Recognise(expected);

        bool skipIteration = false;
        while (generation >= slot.Generation) {
            if (slot.IsEmpty) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeOvertaken(0))) {
                    break;
                }
            } else if (slot.IsOvertaken) {
                skipIteration = true;
                break;
            } else {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(0))) {
                    if (!slot.IsEmpty) {
                        OBSERVE(SuccessFastPop);
                        return slot.Value;
                    }
                    break;
                }
            }
            slot = TSlot::Recognise(expected);
        }
        if (skipIteration) {
            OBSERVE(FailedFastPopAttempt);
            SpinLockPause();
            continue;
        }

        if (slot.Generation > generation) {
            OBSERVE(FailedFastPopAttempt);
            SpinLockPause();
            continue;
        }

        ui64 currentTail = Tail.load(std::memory_order_acquire);
        if (currentTail <= currentHead) {
            OBSERVE(FailedFastPop);
            return std::nullopt;
        }

        OBSERVE(FailedFastPopAttempt);
        SpinLockPause();
    }
    */


try_pop_end:
    printf("Consumer %d: try_pop end\n", id);

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
            if
            :: true ->
                try_push_slow(id);
            :: true ->
                try_push_fast(id);
            fi

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
    unsigned overtakenSlots:3 = 0;
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
