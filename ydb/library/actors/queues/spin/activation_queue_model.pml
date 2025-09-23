#ifndef QUEUE_SIZE
#define QUEUE_SIZE 2
#endif

#ifndef CONSUMERS
#define CONSUMERS 3
#endif

#ifndef PRODUCERS
#define PRODUCERS 3
#endif

#ifndef ELEMENTS
#define ELEMENTS 5
#endif

#include "atomics.pml"
#include "second_queue.pml"


chan internal_queue = [QUEUE_SIZE] of { bool };

unsigned pushed_elements:3 = 0;
unsigned planned_pushed_elements:3 = ELEMENTS;
unsigned popped_elements:3 = 0;
unsigned planned_popped_elements:3 = ELEMENTS;


proctype ProducerProc(unsigned id:2) {
    bool is_success = false;
producer_proc_retry:
    do
    :: pushed_elements < ELEMENTS ->

        atomic {
            if
            :: planned_pushed_elements > 0 ->
                planned_pushed_elements = planned_pushed_elements - 1;
            :: timeout ->
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
            atomic {
                if
                :: len(internal_queue) < QUEUE_SIZE ->
                    printf("Producer %d: success push to internal channel, increment pushed_elements from %d to %d\n", id, pushed_elements, pushed_elements + 1);
                    internal_queue!true;
                    pushed_elements = pushed_elements + 1;
                    goto producer_proc_retry;
                :: else ->
                    printf("Producer %d: internal channel full, push to second queue\n", id);
                fi
            }

            push_to_second_queue(true);
            atomic {
                printf("Producer %d: success push to second queue, increment pushed_elements from %d to %d\n", id, pushed_elements, pushed_elements + 1);
                pushed_elements = pushed_elements + 1;
            }
        fi
    :: else ->
        break
    od
    printf("Producer %d: end\n", id);
}


proctype ConsumerProc(unsigned id:2) {
    bool is_success = false;
    unsigned last_seen_pushed_elements:3 = ELEMENTS + 1;
    unsigned last_seen_popped_elements:3 = ELEMENTS + 1;
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

        bool tmp_value;

        atomic {
            if
            :: len(internal_queue) > 0 ->
                printf("Consumer %d: pop from internal channel\n", id);
                internal_queue?tmp_value;
                printf("Consumer %d: success pop, increment popped_elements from %d to %d\n", id, popped_elements, popped_elements + 1);
                popped_elements = popped_elements + 1;
                goto consumer_proc_retry;
            :: else ->
                printf("Consumer %d: internal channel empty, try second queue\n", id);
            fi
        }

        atomic {
            if
            :: len(second_queue) > 0 ->
                second_queue?tmp_value;
                printf("Consumer %d: success pop from second queue, increment popped_elements from %d to %d\n", id, popped_elements, popped_elements + 1);
                popped_elements = popped_elements + 1;
            :: else ->
                planned_popped_elements = planned_popped_elements + 1;
                goto consumer_proc_retry;
            fi
        }
        atomic {
            printf("Consumer %d: pop from second queue, decrement second_queue_size from %d to %d\n", id, second_queue_size, second_queue_size - 1);
            second_queue_size = second_queue_size - 1;
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
