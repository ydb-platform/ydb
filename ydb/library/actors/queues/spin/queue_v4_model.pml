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

inline atomic_compare_exchange(location, expected, desired, success) {
    atomic {
        if
        :: location == expected ->
            location = desired
            success = true
        :: else ->
            success = false
            expected = location
        fi
    }
}

#define SLOT_EMPTY 0
#define SLOT_HAS_VALUE 1
#define SLOT_OVERTAKEN 2

typedef Slot {
    unsigned state:2 = SLOT_EMPTY;
    unsigned value:2 = 0;
};

bool overflow = false;
Slot buffer[QUEUE_SIZE];
bool write_seen[ELEMENTS] = {false, false, false};
bool read_seen[ELEMENTS] = {false, false, false};
unsigned tail:4 = 0;
unsigned head:4 = 0;
unsigned counter:2 = 0;

chan overtaken_queue = [QUEUE_SIZE] of { int };
unsigned overtaken_slots:3 = 0;

inline push_element(id) {
    // for (;;) {
    //     ui64 currentTail = Tail.fetch_add(1, std::memory_order_relaxed);

    //     ui64 slotIdx = ConvertIdx(currentTail);

    //     std::atomic<ui64> &currentSlot = Buffer[slotIdx];
    //     ui64 expected = TSlot::MakeEmpty();
    //     TSlot slot = TSlot::Recognise(expected);
    //     while (slot.IsEmpty) {
    //         if (!slot.IsOvertaken) {
    //             if (currentSlot.compare_exchange_strong(expected, val, std::memory_order_acq_rel)) {
    //                 return true;
    //             }
    //         } else {
    //             if (currentSlot.compare_exchange_strong(expected, TSlot::MakeEmpty(), std::memory_order_acq_rel)) {
    //                 if (slot.IsOvertaken) {
    //                     AddOvertakenSlot(val);
    //                 }
    //                 return true;
    //             }
    //         }
    //         slot = TSlot::Recognise(expected);
    //     }

    //     if (!slot.IsEmpty) {
    //         ui64 currentHead = Head.load(std::memory_order_acquire);
    //         if (currentHead + MaxSize <= currentTail + std::min<ui64>(FullQueueWindow, static_cast<ui64>(MaxSize) - 1)) {
    //             return false;
    //         }
    //     }
    // }

    unsigned current_tail:4;
    unsigned slot_idx:1;
    unsigned expected:2;
    unsigned head_snapshot:4;

    success = false;

    // Implements: for (;;) {
    do
    :: true ->
        // Implements: ui64 currentTail = Tail.fetch_add(1, std::memory_order_relaxed);
        // Implements: ui64 slotIdx = ConvertIdx(currentTail);
        //             std::atomic<ui64> &currentSlot = Buffer[slotIdx];
        //             ui64 expected = TSlot::MakeEmpty();
        atomic {
            current_tail = tail;
            tail = tail + 1;
            slot_idx = current_tail % QUEUE_SIZE;
            expected = SLOT_EMPTY;
            printf("Producer %d: get current_tail=%d tail=%d\n", id, current_tail, tail);
        }

        // Implements: TSlot slot = TSlot::Recognise(expected);
        //             while (slot.IsEmpty) { ... }

push_begin_change_slot:
        do
        :: expected == SLOT_EMPTY ->
            // Implements: if (!slot.IsOvertaken) { if (currentSlot.compare_exchange_strong(..., val)) { return true; } }
            atomic {
                atomic_compare_exchange(buffer[slot_idx].state, expected, SLOT_HAS_VALUE, success);
                if
                :: success ->
                    printf("Producer %d: success push to slot %d with current_tail=%d value=%d\n", id, slot_idx, current_tail, counter);
                    buffer[slot_idx].value = counter;
                    write_seen[counter] = !write_seen[counter];
                    printf("Producer %d: set write_seen[%d] to %d\n", id, counter, write_seen[counter]);
                    counter = counter + 1;
                    goto push_end;
                :: else ->
                    printf("Producer %d: failed to push to slot (slot is not empty) %d with current_tail=%d\n", id, slot_idx, current_tail);
                    goto push_begin_change_slot;
                fi
            }
        :: expected == SLOT_OVERTAKEN ->
            // Implements: else { if (currentSlot.compare_exchange_strong(..., TSlot::MakeEmpty())) { if (slot.IsOvertaken) { AddOvertakenSlot(val); } return true; } }
            atomic {
                if
                :: buffer[slot_idx].state == SLOT_OVERTAKEN && buffer[slot_idx].value <= 1 ->
                    buffer[slot_idx].state = SLOT_EMPTY;
                    buffer[slot_idx].value = 0;
                :: buffer[slot_idx].state == SLOT_OVERTAKEN ->
                    buffer[slot_idx].value = buffer[slot_idx].value - 1;
                :: else ->
                    expected = buffer[slot_idx].state;
                    goto push_begin_change_slot;
                fi
            }
            atomic {
                overtaken_queue!counter;
                printf("Producer %d: success push to overtaken_queue slot_idx=%d with current_tail=%d value=%d\n", id, slot_idx, current_tail, counter);
                write_seen[counter] = !write_seen[counter];
                printf("Producer %d: set write_seen[%d] to %d\n", id, counter, write_seen[counter]);
                counter = counter + 1;
            }
            atomic {
                overtaken_slots = overtaken_slots + 1;
                success = true;
                goto push_end;
            }
        :: else ->
            // Implements: slot = TSlot::Recognise(expected); exit loop when slot no longer empty.
            atomic {
                printf("Producer %d: failed to push to slot (slot has value) %d with current_tail=%d\n", id, slot_idx, current_tail);
                success = false;
                goto push_end;
            }
        od

    od

push_end:
}

inline pop_element(id) {
    // ui64 overtakenSlots = OvertakenSlots.load(std::memory_order_acquire);
    // if (overtakenSlots) {
    //     bool success = false;
    //     while (overtakenSlots) {
    //         if (OvertakenSlots.compare_exchange_strong(overtakenSlots, overtakenSlots - 1, std::memory_order_acq_rel)) {
    //             success = true;
    //             break;
    //         }
    //     }
    //     if (success) {
    //         for (;;) {
    //             if (auto el = TryPopFromOvertakenSlots()) {
    //                 return el;
    //             }
    //         }
    //     }
    // }

    // for (;;) {
    //     ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);

    //     ui64 slotIdx = ConvertIdx(currentHead);
    //     std::atomic<ui64> &currentSlot = Buffer[slotIdx];

    //     ui64 expected = currentSlot.load(std::memory_order_relaxed);
    //     TSlot slot = TSlot::Recognise(expected);

    //     while (!slot.IsEmpty || !slot.IsOvertaken) {
    //         if (slot.IsEmpty) {
    //             if (currentSlot.compare_exchange_weak(expected, TSlot::MakeOvertaken())) {
    //                 break;
    //             }
    //         } else {
    //             if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty())) {
    //                 return slot.Value;
    //             }
    //         }
    //         slot = TSlot::Recognise(expected);
    //     }

    //     ui64 currentTail = Tail.load(std::memory_order_acquire);
    //     if (currentTail <= currentHead) {
    //         return std::nullopt;
    //     }

    // }

    // return std::nullopt;

    unsigned current_head:4;
    unsigned slot_idx:1;
    unsigned expected:2;
    unsigned tail_snapshot:4;

    success = false;

    // Implements: ui64 overtakenSlots = OvertakenSlots.load(...); if (overtakenSlots) { ... }
    unsigned overtaken_slots_snapshot:3 = overtaken_slots;
    atomic {
        success = false;
        if
        :: overtaken_slots_snapshot > 0 && overtaken_slots > 0 ->
            overtaken_slots = overtaken_slots - 1;
            printf("Consumer %d: success book overtaken slot, decrement overtaken_slots from %d to %d\n", id, overtaken_slots + 1, overtaken_slots);
        :: else ->
            printf("Consumer %d: no overtaken slots overtaken_slots_snapshot=%d overtaken_slots=%d\n", id, overtaken_slots_snapshot, overtaken_slots);
            goto pop_element_main_cycle;
        fi
    }

    atomic {
        overtaken_queue?expected;
        read_seen[expected] = !read_seen[expected];
        success = true;
        printf("Consumer %d: success book overtaken slot value=%d\n", id, expected);
        goto pop_end;
    }

pop_element_main_cycle:
    // Implements: for (;;) {
    do
    :: true ->
        // Implements: ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);
        atomic {
            current_head = head;
            head = head + 1;
            printf("Consumer %d: get current_head=%d head=%d\n", id, current_head, head);
        }

        atomic {
            // Implements: ui64 slotIdx = ConvertIdx(currentHead);
            slot_idx = current_head % QUEUE_SIZE;
            // Implements: ui64 expected = currentSlot.load(...); TSlot slot = TSlot::Recognise(expected);
            expected = buffer[slot_idx].state;
            printf("Consumer %d: get expected=%d for slot_idx=%d\n", id, expected, slot_idx);
        }

        // Implements: while (!slot.IsEmpty || !slot.IsOvertaken) { ... }
pop_begin_change_slot:
        do
        :: expected == SLOT_EMPTY ->
            // Implements: if (slot.IsEmpty) { if (currentSlot.compare_exchange_weak(expected, TSlot::MakeOvertaken())) { break; } }
            atomic {
                atomic_compare_exchange(buffer[slot_idx].state, expected, SLOT_OVERTAKEN, success);
                if
                :: success ->
                    buffer[slot_idx].value = 1;
                    success = false;
                    printf("Consumer %d: success make slot_idx=%d overtaken\n", id, slot_idx);
                    goto pop_check_tail;
                :: else -> goto pop_begin_change_slot;
                fi
            }
        :: expected == SLOT_HAS_VALUE ->
            // Implements: else { if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty())) { return slot.Value; } }
            atomic {
                atomic_compare_exchange(buffer[slot_idx].state, expected, SLOT_EMPTY, success);
                if
                :: success ->
                    expected = buffer[slot_idx].value;
                    read_seen[expected] = !read_seen[expected];
                    printf("Consumer %d: success pop from slot_idx=%d value=%d\n", id, slot_idx, expected);
                    goto pop_end;
                :: else -> goto pop_begin_change_slot;
                fi
            }
        :: expected == SLOT_OVERTAKEN ->
            // Implements: slot = TSlot::Recognise(expected); continue retry loop when overtaken observed.
            atomic {
                if
                :: buffer[slot_idx].state == SLOT_OVERTAKEN && buffer[slot_idx].value >= 1 ->
                    buffer[slot_idx].value = buffer[slot_idx].value + 1;
                    printf("Consumer %d: failed to pop from slot_idx=%d (slot is overtaken)\n", id, slot_idx);
                    success = false;
                    goto pop_end;
                :: else ->
                    break
                fi
            }
        :: else ->
            break
        od

        // Implements: ui64 currentTail = Tail.load(...); if (currentTail <= currentHead) { return std::nullopt; }
pop_check_tail:
        atomic {
            if
            :: tail < current_head ->
                success = false;
                printf("Consumer %d: failed to pop from slot_idx=%d (tail < current_head)\n", id, slot_idx);
                goto pop_end;
            :: else -> skip
            fi
        }
    od

pop_end:
}

unsigned pushed_elements:2 = 0;
unsigned planned_pushed_elements:2 = ELEMENTS;
unsigned popped_elements:2 = 0;
unsigned planned_popped_elements:2 = ELEMENTS;

proctype ProducerProc(unsigned id:2) {
    bool success = false;
    producer_proc_retry:
    do
    :: pushed_elements < ELEMENTS ->

        atomic {

            if
            :: overflow ->
                printf("Producer %d: precheck overflow, retry later\n", id);
                if
                :: pushed_elements == popped_elements && planned_pushed_elements == pushed_elements ->
                    printf("Producer %d: precheck overflow, waited queue is empty pushed_elements=%d popped_elements=%d\n", id, pushed_elements, popped_elements);
                    overflow = false;
                fi
            :: else -> skip
            fi

            if
            :: planned_pushed_elements > 0 ->
                planned_pushed_elements = planned_pushed_elements - 1;
            :: timeout ->
                break
            fi
        }


        push_element(id);
        atomic {
            if
            :: success ->
                printf("Producer %d: success push to internal channel, increment pushed_elements from %d to %d\n", id, pushed_elements, pushed_elements + 1);
                pushed_elements = pushed_elements + 1;
            :: else ->
                printf("Producer %d: internal channel full, retry later\n", id);
                planned_pushed_elements = planned_pushed_elements + 1;
                overflow = true
                if
                :: pushed_elements == popped_elements && planned_pushed_elements == pushed_elements ->
                    printf("Producer %d: waited queue is empty pushed_elements=%d popped_elements=%d\n", id, pushed_elements, popped_elements);
                    overflow = false;
                fi
                goto producer_proc_retry
            fi
        }
    :: else ->
        break
    od
    printf("Producer %d: end\n", id);
}


proctype ConsumerProc(unsigned id:2) {
    unsigned last_seen_pushed_elements:2 = ELEMENTS + 1;
    unsigned last_seen_popped_elements:2 = ELEMENTS + 1;
    bool success = false;
consumer_proc_retry:
    do
    :: popped_elements < ELEMENTS && (last_seen_pushed_elements != pushed_elements || last_seen_popped_elements != popped_elements)
    ->
        atomic {

            if
            :: planned_popped_elements > 0 ->
                planned_popped_elements = planned_popped_elements - 1;
            :: timeout ->
                break
            fi

            last_seen_pushed_elements = pushed_elements;
            last_seen_popped_elements = popped_elements;
            printf("Consumer %d: seen pushed_elements %d\n", id, pushed_elements);
        }

        pop_element(id);
        atomic {
            if
            :: success ->
                printf("Consumer %d: success pop, increment popped_elements from %d to %d\n", id, popped_elements, popped_elements + 1);
                popped_elements = popped_elements + 1;
                goto consumer_proc_retry;
            :: else ->
                printf("Consumer %d: internal channel empty, retry later\n", id);
                planned_popped_elements = planned_popped_elements + 1;
            fi
        }

    :: popped_elements >= ELEMENTS ->
        break
    od
    printf("Consumer %d: end\n", id);
}


inline run_producers() {
    atomic {
        do
        :: producer_count > 0 ->
            run ProducerProc(producer_count);
            producer_count = producer_count - 1;
        :: else -> break
        od
    }
}

inline run_consumers() {
    atomic {
        do
        :: consumer_count > 0 ->
            run ConsumerProc(consumer_count);
            consumer_count = consumer_count - 1;
        :: else -> break
        od
    }
}

unsigned consumer_count:2 = CONSUMERS;
unsigned producer_count:2 = PRODUCERS;

init {
    atomic {
        run_producers();
        run_consumers();
    }
}




ltl all {
    [] (tail <= ELEMENTS + PRODUCERS) &&
    // если head стал больше нуля, то он никогда не будет меньше нуля
    []((head > 0) -> [](head != 0)) &&
    []((tail > 0) -> [](tail != 0)) &&

    [] (write_seen[0] -> <>(read_seen[0])) &&
    [] (write_seen[1] -> <>(read_seen[1])) &&
    //[] (write_seen[2] -> <>(read_seen[2])) &&

    <> (write_seen[0]) && <> (write_seen[1]) && // <> (write_seen[2]) &&
    [] (write_seen[0] -> []write_seen[0]) &&
    [] (write_seen[1] -> []write_seen[1]) &&
    //[] (write_seen[2] -> []write_seen[2]) &&

    <> (read_seen[0]) && <> (read_seen[1]) && // <> (read_seen[2]) &&
    [] (read_seen[0] -> []read_seen[0]) &&
    [] (read_seen[1] -> []read_seen[1]) &&
    //[] (read_seen[2] -> []read_seen[2]) &&
    true
}

ltl head_and_tail_invariant {
    [] (tail <= ELEMENTS + PRODUCERS) &&
    // если head стал больше нуля, то он никогда не будет меньше нуля
    []((head > 0) -> [](head != 0)) &&
    []((tail > 0) -> [](tail != 0)) &&
    true
}

ltl write_next_read_invariant {
    [] (write_seen[0] -> <>(read_seen[0])) &&
    [] (write_seen[1] -> <>(read_seen[1])) &&
    [] (write_seen[2] -> <>(read_seen[2])) &&
    true
}

ltl after_set_always_write {
    <> (write_seen[0]) && <> (write_seen[1]) &&
    [] (write_seen[0] -> []write_seen[0]) &&
    [] (write_seen[1] -> []write_seen[1]) &&
    [] (write_seen[2] -> []write_seen[2]) &&
    true
}

ltl after_set_always_read {
    <> (read_seen[0]) && <> (read_seen[1]) &&
    [] (read_seen[0] -> []read_seen[0]) &&
    [] (read_seen[1] -> []read_seen[1]) &&
    [] (read_seen[2] -> []read_seen[2]) &&
    true
}