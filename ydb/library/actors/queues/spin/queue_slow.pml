#ifndef QUEUE_SIZE
#define QUEUE_SIZE 3
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

#define OVERFLOW_QUEUE_TIMES (ELEMENTS/ (QUEUE_SIZE + CONSUMERS - 1))

#ifdef WITH_BLOCKING
#define BLOCK_PROC_IF_QUEUE_IS_FULL true
#define BLOCK_PROC_IF_QUEUE_IS_EMPTY true
#else
#define BLOCK_PROC_IF_QUEUE_IS_FULL false
#define BLOCK_PROC_IF_QUEUE_IS_EMPTY false
#endif


typedef Slot {
    byte generation;
    bool isEmpty;
}

typedef Queue {
    byte tail;
    byte head;
    Slot buffer[QUEUE_SIZE];
}

typedef Producer {
    bool success_push = false;
    bool slow_push = false;
    int next_slot_for_read = -1;
}

typedef Consumer {
    bool success_pop = false;
}

Queue queue;
Producer producers[PRODUCERS];
Consumer consumers[CONSUMERS];


inline increment_queue_head(currentHead) {
    atomic {
        if
        :: queue.head <= currentHead ->
            queue.head = currentHead + 1
        :: else -> skip
        fi
    }
}

inline increment_queue_tail(currentTail) {
    atomic {
        if 
        :: queue.tail <= currentTail ->
            queue.tail = currentTail + 1
        :: else -> skip
        fi
    }
}

inline invalidate_slot(currentHead, current_generation) {
    atomic {
        queue.buffer[currentHead % QUEUE_SIZE].generation = current_generation + 1;
        queue.buffer[currentHead % QUEUE_SIZE].isEmpty = true;
    }
}

inline save_slot_value(currentHead, saved_generation) {
    atomic {
        queue.buffer[currentHead % QUEUE_SIZE].isEmpty = false;
        queue.buffer[currentHead % QUEUE_SIZE].generation = saved_generation;
    }
}

inline read_slot(currentHead, destination) {
    atomic {
        destination.generation = queue.buffer[currentHead % QUEUE_SIZE].generation;
        destination.isEmpty = queue.buffer[currentHead % QUEUE_SIZE].isEmpty;
    }
}

byte lagged_pops = 0;

inline increment_if_it_is_lagged_pop(currentHead) {
    atomic {
        byte producer_idx = 0

        do
        :: producer_idx < PRODUCERS ->
            if
            :: producers[producer_idx].next_slot_for_read == currentHead || ((producers[producer_idx].next_slot_for_read == -1 || producers[producer_idx].next_slot_for_read  < currentHead) && (queue.head <= currentHead)) ->
                goto iiiilp_end
            :: else -> skip
            fi
            producer_idx = producer_idx + 1
        :: else -> break;
        od

iiiilp_increment:
        if
        :: currentHead + QUEUE_SIZE <= queue.tail ->
            lagged_pops = lagged_pops + 1;
        :: else -> skip
        fi
iiiilp_end:
    }
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

inline try_push_slow(id) {
    byte currentTail;
    byte generation;
    Slot currentSlot;
    bool success_push = false;

    do
    :: true -> 
        atomic {
            currentTail = queue.tail;
            generation = currentTail / QUEUE_SIZE;
            printf("INFO: Writer %d with tail %d and generation %d\n", id, currentTail, generation);
            if
            :: producers[id].slow_push && queue.tail >= queue.head + QUEUE_SIZE ->
                printf("INFO: Writer %d with tail %d and generation %d is slow and queue is full [slow_push]\n", id, currentTail, generation);
                break
            :: else -> skip
            fi
            producers[id].next_slot_for_read = currentTail;
        }

        atomic {
            read_slot(currentTail, currentSlot);
            if 
            :: currentSlot.isEmpty && currentSlot.generation <= generation ->
                save_slot_value(currentTail, generation);
                success_push = true;
                printf("INFO: Writer %d wrote to slot %d\n", id, currentTail % QUEUE_SIZE);
            :: currentSlot.isEmpty && currentSlot.generation > generation -> 
                printf("INFO: Writer %d try to write to slot %d but it is from future generation %d\n", id, currentTail % QUEUE_SIZE, currentSlot.generation);
            :: !currentSlot.isEmpty ->
                printf("INFO: Writer %d try to write to slot %d but it is not empty\n", id, currentTail % QUEUE_SIZE);
            fi
            producers[id].next_slot_for_read = -1;
        }

        atomic {
            if
            :: success_push -> 
                printf("INFO: Writer %d try to increment tail from %d to %d\n", id, queue.tail, currentTail + 1);
                increment_queue_tail(currentTail);
                producers[id].success_push = true;
                producers[id].slow_push = false;
                break
            :: else -> skip
            fi

            if
            :: !currentSlot.isEmpty ->
                if
                :: queue.head + QUEUE_SIZE <= currentTail ->
                    producers[id].success_push = false;
                    printf("INFO: Writer %d failed to push to slot %d because head is too far %d\n", id, currentTail, queue.head);
                    producers[id].slow_push = true;
                    break;
                :: else ->
                    printf("INFO: Writer %d try to increment tail from %d to %d because slot already has value head %d\n", id, queue.tail, currentTail + 1, queue.head);
                    increment_queue_tail(currentTail);
                    producers[id].slow_push = true;
                fi
            :: currentSlot.isEmpty && currentSlot.generation > generation ->
                printf("INFO: Writer %d try to increment tail from %d to %d by greater generation %d\n", id, queue.tail, currentTail + 1, currentSlot.generation);
                increment_queue_tail(currentTail);
                producers[id].slow_push = true;
            :: else -> skip
            fi
        }
    od
}

inline try_pop_slow(id) {
    byte currentHead;
    byte generation;
    Slot currentSlot;

    do
    :: true ->
        atomic {
            currentHead = queue.head;
            generation = currentHead / QUEUE_SIZE;
            printf("INFO: Consumer %d with head %d and generation %d\n", id, currentHead, generation);
        }

        atomic {
            read_slot(currentHead, currentSlot);
            if
            :: currentSlot.generation > generation ->
                printf("INFO: Consumer %d with generation %d see slot %d from future generation %d\n", id, generation, currentHead, currentSlot.generation);
                printf("INFO: Consumer %d with generation %d try to increment head from %d to %d\n", id, generation, queue.head, currentHead + 1);
                increment_queue_head(currentHead);
                goto next_iteration
            :: else -> skip
            fi
        }

        atomic {
            read_slot(currentHead, currentSlot);
            if
            :: currentSlot.isEmpty && currentSlot.generation < generation ->
                invalidate_slot(currentHead, generation);
                currentSlot.generation = generation + 1;
                printf("INFO: Consumer %d with generation %d found slot %d with generation %d and invalidate it\n", id, generation, currentHead, currentSlot.generation);
            :: !currentSlot.isEmpty ->
                invalidate_slot(currentHead, generation);
                consumers[id].success_pop = true;
                increment_if_it_is_lagged_pop(currentHead);
                printf("INFO: Consumer %d with generation %d and find %d success pop\n", id, generation, currentHead);
            :: else ->
                printf("INFO: Consumer %d with generation %d and find empty slot %d with generation %d\n", id, generation, currentHead, currentSlot.generation);
            fi
        }

        atomic {
            if
            :: !currentSlot.isEmpty ->
                printf("INFO: Consumer %d with generation %d try to increment head from %d to %d\n", id, generation, queue.head, currentHead + 1);
                increment_queue_head(currentHead);
                break;
            :: else -> skip
            fi
        
            if
            :: currentSlot.generation > generation ->
                printf("INFO: Consumer %d with generation %d try to increment head from %d to %d\n", id, generation, queue.head, currentHead + 1);
                increment_queue_head(currentHead);
                goto next_iteration
            :: else -> skip
            fi;
            
            if
            :: queue.tail <= currentHead -> 
                printf("INFO: Consumer %d with generation %d and tail %d is less than head %d\n", id, generation, queue.tail, currentHead);
                break
            :: else -> skip
            fi;
        }

        atomic {
            read_slot(currentHead, currentSlot);
            if
            :: currentSlot.generation <= generation && currentSlot.isEmpty ->
                if
                :: currentSlot.generation <= generation ->
                    invalidate_slot(currentHead, generation);
                    if
                    :: !currentSlot.isEmpty ->
                        consumers[id].success_pop = true;
                        printf("INFO: Consumer %d with generation %d and find %d success pop\n", id, generation, currentHead);
                        increment_if_it_is_lagged_pop(currentHead);
                        break
                    :: else -> skip
                    fi
                    printf("INFO: Consumer %d with generation %d try to increment head from %d to %d\n", id, generation, queue.head, currentHead + 1);
                    increment_queue_head(currentHead);
                :: else -> skip
                fi
            :: else -> skip
            fi
        }
next_iteration:
        skip
    od
}

byte pushed_elements = 0;
byte planned_pushed_elements = ELEMENTS;
byte popped_elements = 0;
byte planned_popped_elements = ELEMENTS;

proctype ProducerProc(int id) {
    do
    :: pushed_elements < ELEMENTS ->
        atomic {
            if
            :: planned_pushed_elements > 0 ->
                planned_pushed_elements = planned_pushed_elements - 1;
            :: else -> break
            fi
        }
        if
        :: queue.tail < queue.head + QUEUE_SIZE ->
            try_push_slow(id);
        :: !BLOCK_PROC_IF_QUEUE_IS_FULL ->
            skip
        fi
        atomic {
            if
            :: producers[id].success_push ->
                pushed_elements = pushed_elements + 1;
                producers[id].success_push = false;
            :: else -> 
                planned_pushed_elements = planned_pushed_elements + 1;
            fi
        }
    :: else ->
        break
    od
}

proctype ConsumerProc(int id) {
    do
    :: popped_elements < ELEMENTS ->
        atomic {
            if
            :: planned_popped_elements > 0 ->
                planned_popped_elements = planned_popped_elements - 1;
            :: else -> break
            fi
        }
        if
        :: queue.tail > queue.head ->
            try_pop_slow(id);
        :: !BLOCK_PROC_IF_QUEUE_IS_EMPTY ->
            skip
        fi
        atomic {
            if
            :: consumers[id].success_pop ->
                popped_elements = popped_elements + 1;
                consumers[id].success_pop = false;
            :: else ->
                planned_popped_elements = planned_popped_elements + 1;
            fi
        }
    consumer_next_iteration:
        skip
    :: else ->
        break
    od
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

byte consumer_count = CONSUMERS;
byte producer_count = PRODUCERS;

init {
    init_queue();
    atomic {
        run_producers();
        run_consumers();
    }
}

#define PUSHED_SLOT_ALWAYS_WILL_BE_POPPED(idx) (idx >= QUEUE_SIZE || [](!queue.buffer[idx].isEmpty -> <>(queue.buffer[idx].isEmpty)))




ltl no_overflow {
    [] ((queue.tail - queue.head <= QUEUE_SIZE + CONSUMERS) && 
        (queue.tail <= ELEMENTS + lagged_pops) && 
        (queue.head <= ELEMENTS + lagged_pops) &&
        (lagged_pops <= (ELEMENTS/ (QUEUE_SIZE + CONSUMERS - 1)) * (CONSUMERS - 1)))
    && PUSHED_SLOT_ALWAYS_WILL_BE_POPPED(0)
    && PUSHED_SLOT_ALWAYS_WILL_BE_POPPED(1)
    && PUSHED_SLOT_ALWAYS_WILL_BE_POPPED(2) 
}
