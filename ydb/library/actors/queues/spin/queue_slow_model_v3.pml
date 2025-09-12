#ifndef QUEUE_SIZE
#define QUEUE_SIZE 2
#endif

#ifndef CONSUMERS
#define CONSUMERS 2
#endif

#ifndef PRODUCERS
#define PRODUCERS 2
#endif

#ifndef ELEMENTS
#define ELEMENTS 5
#endif

#include "atomics.pml"
#include "context_switch.pml"
#include "second_queue.pml"
#include "queue_base.pml"
#include "macros.pml"

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
    
    SWITH_CONTEXT(try_push_start);
    SWITHING_GATE(try_push_start);
    atomic_load(queue.tail, currentTail);
    printf("Producer %d: try to push. tail=%d, generation=%d\n", id, currentTail, generation);


    // for (ui32 it = 0;; ++it) {
    do
    :: true ->
        // ui32 generation = currentTail / MaxSize;
        // std::atomic<ui64> &currentSlot = Buffer[currentTail % MaxSize];
        // TSlot slot;
        // ui64 expected = TSlot::MakeEmpty(generation);
        
        SWITH_CONTEXT(try_push_iteration_start);
        SWITHING_GATE(try_push_iteration_start);
        generation = currentTail / QUEUE_SIZE;
        currentSlot.generation = generation;
        currentSlot.isEmpty = true;

        // do {
        //    if (currentSlot.compare_exchange_weak(expected, val)) {
        //        Tail.compare_exchange_strong(currentTail, currentTail + 1);
        //        return true;
        //    }
        //    slot = TSlot::Recognise(expected);
        // } while (slot.Generation <= generation && slot.IsEmpty);
        /*
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
        */
        SWITH_CONTEXT(try_push_write_slot);
        SWITHING_GATE(try_push_write_slot);
        if
        :: false ->
try_push_reread_slot:
            printf("Reread slot %d. tail=%d, generation=%d, slot_generation=%d, slot_isEmpty=%d\n", id, currentTail, generation, currentSlot.generation, currentSlot.isEmpty);
            skip
        :: else -> skip
        fi
        if
        :: currentSlot.generation <= generation && currentSlot.isEmpty ->
            JUMP_SLOT_CHANGED(currentTail, currentSlot, try_push_reread_slot);
            save_slot_value(currentTail, generation);
            printf("Producer %d: success push. tail=%d, generation=%d, slot_generation=%d, slot_isEmpty=%d\n", id, currentTail, generation, currentSlot.generation, currentSlot.isEmpty);
            producers[id].success_push = true;
            goto try_push_success
        :: else ->
            printf("Producer %d: try to write to slot %d but it is impossible. tail=%d, generation=%d, slot_generation=%d, slot_isEmpty=%d\n", id, currentTail % QUEUE_SIZE, currentTail, generation, currentSlot.generation, currentSlot.isEmpty);
        fi
        // if (!slot.IsEmpty) {
        //    ui64 currentHead = Head.load(std::memory_order_acquire);
        //    if (currentHead + MaxSize <= currentTail + std::min<ui64>(64, MaxSize - 1)) {
        //        return false;
        //    }
        // }

        if
        :: !currentSlot.isEmpty ->
            SWITH_CONTEXT(try_push_check_slow_push);
            SWITHING_GATE(try_push_check_slow_push);
            atomic_load(queue.head, currentHead);
            printf("Producer %d: check slow push. head=%d, tail=%d, generation=%d\n", id, currentHead, currentTail, generation);
            if
            :: currentHead + QUEUE_SIZE <= currentTail ->
                printf("Producer %d: change to slow push, break, head=%d, tail=%d, generation=%d\n", id, currentHead, currentTail, generation);
                producers[id].success_push = false;
                goto try_push_end
                :: else -> skip
                fi
        :: else -> skip
        fi

        // TryIncrementTail(currentTail);
        SWITH_CONTEXT(try_push_increment_tail);
        SWITHING_GATE(try_push_increment_tail);
        try_increment_tail(id);
        printf("Producer %d: to next iteration. tail=%d, generation=%d\n", id, currentTail + 1, generation);
    od

try_push_success:
    SWITH_CONTEXT(try_push_success);
    SWITHING_GATE(try_push_success);
    blind_atomic_compare_exchange(queue.tail, currentTail, currentTail + 1);
    producers[id].success_push = true;

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
    SWITH_CONTEXT(try_pop_start);
    SWITHING_GATE(try_pop_start);
    atomic_load(queue.head, currentHead);
    generation = currentHead / QUEUE_SIZE;
    printf("Consumer %d: try to pop. head=%d, generation=%d\n", id, currentHead, generation);

    // for (ui32 it = 0;; ++it) {
    do
    :: true ->
try_pop_iteration_start:
        // generation = currentHead / MaxSize;
        // std::atomic<ui64> &currentSlot = Buffer[currentHead % MaxSize];
        // ui64 expected = currentSlot.load(std::memory_order_relaxed);
        // TSlot slot = TSlot::Recognise(expected);
        SWITH_CONTEXT(try_pop_iteration_start);
        SWITHING_GATE(try_pop_iteration_start);
        atomic {
            generation = currentHead / QUEUE_SIZE;
            read_slot(currentHead, currentSlot);
            printf("Consumer %d: read slot %d. head=%d, generation=%d, slot_generation=%d, slot_isEmpty=%d\n", id, currentHead % QUEUE_SIZE, currentHead, generation, currentSlot.generation, currentSlot.isEmpty);
        }

        // if (slot.Generation > generation) {
        //     TryIncrementHead(currentHead);
        //     continue;
        // }
        if
        :: currentSlot.isEmpty && currentSlot.generation > generation ->
            SWITH_CONTEXT(try_pop_increment_head_after_slot_from_future_generation);
            SWITHING_GATE(try_pop_increment_head_after_slot_from_future_generation);
            printf("Consumer %d: see slot %d from future generation %d. head=%d, generation=%d\n", id, currentHead % QUEUE_SIZE, currentSlot.generation, currentHead, generation);
            try_increment_head(id);
            goto try_pop_iteration_start;
        :: else -> skip
        fi

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
        /*
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
        */
        SWITH_CONTEXT(try_pop_read_slot_1);
        SWITHING_GATE(try_pop_read_slot_1);
        if
        :: false ->
try_pop_reread_slot_1:
            printf("Consumer %d: reread slot#1 %d. head=%d, generation=%d, slot_generation=%d, slot_isEmpty=%d\n", id, currentHead, generation, currentSlot.generation, currentSlot.isEmpty);
        :: else -> skip
        fi
        if
        :: !currentSlot.isEmpty ->
            JUMP_SLOT_CHANGED(currentHead, currentSlot, try_pop_reread_slot_1);
            printf("Consumer %d: success pop. head=%d, generation=%d\n", id, currentHead, generation);
            invalidate_slot(currentHead, generation);
            goto try_pop_success
        :: generation > currentSlot.generation && currentSlot.isEmpty ->
            JUMP_SLOT_CHANGED(currentHead, currentSlot, try_pop_reread_slot_1);
            invalidate_slot(currentHead, generation - 1);
            currentSlot.generation = generation;
        :: else -> 
            printf("Consumer %d: see empty slot with equal or greater generation. head=%d, generation=%d, slot_generation=%d, slot_isEmpty=%d\n", id, currentHead, generation, currentSlot.generation, currentSlot.isEmpty);
        fi

        // if (slot.Generation > generation) {
        //     TryIncrementHead(currentHead);
        //     continue;
        // }
        // ui64 currentTail = Tail.load(std::memory_order_acquire);
        // if (currentTail <= currentHead) {
        //     return std::nullopt;
        // }
        if
        :: currentSlot.isEmpty && currentSlot.generation > generation ->
            SWITH_CONTEXT(try_pop_greater_generation_2);
            SWITHING_GATE(try_pop_greater_generation_2);
            try_increment_head(id);
            printf("Consumer %d: seen slot from future generation %d. head=%d, generation=%d\n", id, currentSlot.generation, currentHead, generation);
            goto try_pop_iteration_start;
        :: else ->
            SWITH_CONTEXT(try_pop_check_tail);
            SWITHING_GATE(try_pop_check_tail);
            atomic_load(queue.tail, currentTail);
            if
            :: currentTail <= currentHead ->
                printf("Consumer %d: tail %d is less or equal than head %d. head=%d, generation=%d\n", id, currentTail, currentHead, currentHead, generation);
                consumers[id].success_pop = false;
                goto try_pop_end
            :: else -> 
                printf("Consumer %d: tail %d is greater than head %d. head=%d, generation=%d\n", id, currentTail, currentHead, currentHead, generation);
            fi
        fi

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
        /*
        do
        :: consumers[id].currentSlot.generation <= consumers[id].generation || !consumers[id].currentSlot.isEmpty ->
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
        */
        SWITH_CONTEXT(try_pop_read_slot_2);
        SWITHING_GATE(try_pop_read_slot_2);
        if
        :: false ->
try_pop_reread_slot_2:
            printf("Consumer %d: reread slot#2 %d. head=%d, generation=%d, slot_generation=%d, slot_isEmpty=%d\n", id, currentHead, generation, currentSlot.generation, currentSlot.isEmpty);
        :: else -> skip
        fi
        if
        :: !currentSlot.isEmpty ->
            JUMP_SLOT_CHANGED(currentHead, currentSlot, try_pop_reread_slot_2);
            printf("Consumer %d: success pop. head=%d, generation=%d\n", id, currentHead, generation);
            invalidate_slot(currentHead, generation);
            goto try_pop_success
        :: generation >= currentSlot.generation && currentSlot.isEmpty ->
            JUMP_SLOT_CHANGED(currentHead, currentSlot, try_pop_reread_slot_2);
            invalidate_slot(currentHead, generation);
            currentSlot.generation = generation;
            SWITH_CONTEXT(try_pop_increment_head_after_invalidate);
            SWITHING_GATE(try_pop_increment_head_after_invalidate);
            try_increment_head(id);
            goto try_pop_iteration_start;
        :: else ->
            printf("Consumer %d: see slot with greater generation. head=%d, generation=%d, slot_generation=%d, slot_isEmpty=%d\n", id, currentHead, generation, currentSlot.generation, currentSlot.isEmpty);
        fi

        // currentHead = Head.load(std::memory_order_acquire);
        SWITH_CONTEXT(try_pop_next_iteration);
        SWITHING_GATE(try_pop_next_iteration);
        atomic_load(queue.head, currentHead);
    od


try_pop_success:
    SWITH_CONTEXT(try_pop_success);
    SWITHING_GATE(try_pop_success);
    printf("Consumer %d: try_to_increment_head. head=%d, generation=%d\n", id, currentHead, generation);
    try_increment_head(id);
    consumers[id].success_pop = true;

try_pop_end:
    printf("Consumer %d: try_pop end\n", id);
    skip
}

unsigned pushed_elements:3 = 0;
unsigned planned_pushed_elements:3 = ELEMENTS;
unsigned popped_elements:3 = 0;
unsigned planned_popped_elements:3 = ELEMENTS;


proctype ProducerProc(unsigned id:2) {
    unsigned currentHead:4;
    unsigned currentTail:4;
    unsigned generation:3;
    Slot currentSlot;
    bool is_success = false;

    atomic {
        do
        :: pushed_elements < ELEMENTS ->
            if
            :: planned_pushed_elements > 0 ->
                printf("Producer %d: decrement planned_pushed_elements from %d to %d\n", id, planned_pushed_elements, planned_pushed_elements - 1);
                planned_pushed_elements = planned_pushed_elements - 1;
            :: else ->
                printf("Producer %d: planned_pushed_elements == 0, break\n", id);
                break
            fi
            
            if
            :: second_queue_size > 0 ->
            producer_push_to_second_queue:
                SWITH_CONTEXT(producer_push_to_second_queue);
                SWITHING_GATE(producer_push_to_second_queue);
                printf("Producer %d: push to second queue\n", id);
                atomic_increment(second_queue_size);
                SWITH_CONTEXT(producer_pushed_to_second_queue_before_send);
                SWITHING_GATE(producer_pushed_to_second_queue_before_send);
                second_queue!true;
                printf("Producer %d: success push to second queue, increment pushed_elements from %d to %d\n", id, pushed_elements, pushed_elements + 1);
                pushed_elements = pushed_elements + 1;
            :: else ->
                try_push(id);
                if
                :: producers[id].success_push ->
                    printf("Producer %d: success push, increment pushed_elements from %d to %d\n", id, pushed_elements, pushed_elements + 1);
                    pushed_elements = pushed_elements + 1;
                    producers[id].success_push = false;
                :: else ->
                    printf("Producer %d: failed push, push to second queue\n", id);
                    goto producer_push_to_second_queue;
                fi
            fi
        :: else ->
            break
        od
    }

    if
    :: true -> skip;
    :: else ->
        JUMP_POINT(producer_push_to_second_queue);
        JUMP_POINT(producer_pushed_to_second_queue_before_send);
        JUMP_POINT(try_push_start);
        JUMP_POINT(try_push_iteration_start);
        JUMP_POINT(try_push_write_slot);
        JUMP_POINT(try_push_check_slow_push);
        JUMP_POINT(try_push_increment_tail);
        JUMP_POINT(try_push_success);
    fi

    printf("Producer %d: end\n", id);
}


proctype ConsumerProc(unsigned id:2) {
    unsigned currentHead:4;
    unsigned currentTail:4;
    unsigned generation:3;
    Slot currentSlot;
    bool is_success = false;
    bool tmp_value;
    unsigned last_seen_planned_pushed_elements:3 = 0;
    unsigned last_seen_pushed_elements:3 = 0;
    atomic {
consumer_proc_retry:
        do
        :: popped_elements < ELEMENTS && (last_seen_pushed_elements != pushed_elements || last_seen_planned_pushed_elements != planned_pushed_elements)
        ->
            last_seen_pushed_elements = pushed_elements;
            last_seen_planned_pushed_elements = planned_pushed_elements;
            printf("Consumer %d: seen planned_pushed_elements %d and pushed_elements %d\n", id, planned_pushed_elements, pushed_elements);

            if
            :: planned_popped_elements > 0 ->

                printf("Consumer %d: decrement planned_popped_elements from %d to %d\n", id, planned_popped_elements, planned_popped_elements - 1);
                planned_popped_elements = planned_popped_elements - 1;

            :: popped_elements == ELEMENTS ->
                printf("Consumer %d: popped_elements == ELEMENTS, break\n", id);
                break
            fi

            try_pop(id);
            SWITH_CONTEXT(after_try_pop);
            SWITHING_GATE(after_try_pop);

            if
            :: consumers[id].success_pop ->
                printf("Consumer %d: success pop, increment popped_elements from %d to %d\n", id, popped_elements, popped_elements + 1);
                popped_elements = popped_elements + 1;
                consumers[id].success_pop = false;
            :: else ->
                if
                :: len(second_queue) > 0 ->
                    second_queue?tmp_value;
                    printf("Consumer %d: pop from second queue, decrement second_queue_size from %d to %d\n", id, second_queue_size, second_queue_size - 1);
                    SWITH_CONTEXT(after_read_from_second_queue_but_before_decrement_size);
                    SWITHING_GATE(after_read_from_second_queue_but_before_decrement_size);
                    second_queue_size = second_queue_size - 1;
                    popped_elements = popped_elements + 1;
                :: else -> 
                    SWITH_CONTEXT(after_empty_second_queue);
                    SWITHING_GATE(after_empty_second_queue);
                    printf("Consumer %d: timeout, increment planned_popped_elements from %d to %d\n", id, planned_popped_elements, planned_popped_elements + 1);
                    planned_popped_elements = planned_popped_elements + 1;
                fi
            fi

        :: popped_elements >= ELEMENTS ->
            break
        od
    }

    if
    :: true -> skip;
    :: else ->
        JUMP_POINT(after_try_pop);
        JUMP_POINT(after_read_from_second_queue_but_before_decrement_size);
        JUMP_POINT(after_empty_second_queue);
        JUMP_POINT(try_pop_success);
        JUMP_POINT(try_pop_next_iteration);
        JUMP_POINT(try_pop_read_slot_2);
        JUMP_POINT(try_pop_greater_generation_2);
        JUMP_POINT(try_pop_check_tail);
        JUMP_POINT(try_pop_read_slot_1);
        JUMP_POINT(try_pop_iteration_start);
        JUMP_POINT(try_pop_start);
        JUMP_POINT(try_pop_increment_head_after_slot_from_future_generation);
        JUMP_POINT(try_pop_increment_head_after_invalidate);
    fi

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

#define NO_OVERFLOW_TAIL_AND_HEAD (queue.tail < 15 && queue.head < 15)
#define NO_OVERFLOW_GENERATION(idx) (queue.buffer[idx].generation < 7)

ltl no_overflow {
    [] ((queue.head <= queue.tail + PRODUCERS) &&
        (queue.tail - queue.head <= QUEUE_SIZE * 2 + CONSUMERS) && 
        (queue.tail <= ELEMENTS * 2) && 
        (queue.head <= ELEMENTS * 2)
        && NO_OVERFLOW_SECOND_QUEUE
        && NO_OVERFLOW_TAIL_AND_HEAD
        && FOR_ALL(QUEUE_SIZE, NO_OVERFLOW_GENERATION)
    )
}

#define PUSHED_SLOT_ALWAYS_WILL_BE_POPPED(idx) [](!queue.buffer[idx].isEmpty -> <>(queue.buffer[idx].isEmpty))

ltl pushed_slot_always_will_be_popped {
    FOR_ALL(QUEUE_SIZE, PUSHED_SLOT_ALWAYS_WILL_BE_POPPED)
}
