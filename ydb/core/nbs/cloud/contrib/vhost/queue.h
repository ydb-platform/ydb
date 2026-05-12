/*
 * Lists and queues.
 *
 * Relevant OSes provide BSD-originated sys/queue.h, so just use it here, with
 * a few extensions.
 */

#pragma once

#include <sys/queue.h>
#include "catomic.h"

/*
 * Atomically insert a new list head
 */
#define SLIST_INSERT_HEAD_ATOMIC(head, elm, field)      ({                        \
    typeof(elm) old_slh_first;                                                    \
    do {                                                                          \
        /* Grab the current head and make the new element point to it */          \
        (elm)->field.sle_next = catomic_read(&(head)->slh_first);                 \
        old_slh_first = (elm)->field.sle_next;                                    \
                                                                                  \
        /* Repeat until slh_first matches old_slh_first at the time of cmpxchg */ \
    } while (catomic_cmpxchg(&(head)->slh_first, old_slh_first, (elm)) !=         \
             old_slh_first);                                                      \
    old_slh_first;      })

/*
 * Atomically move the list into 'dest' leaving 'src' empty
 */
#define SLIST_MOVE_ATOMIC(dest, src) do {                            \
    (dest)->slh_first = catomic_xchg(&(src)->slh_first, NULL);       \
} while (0)

/*
 * Read the current list head with consume
 */
#define SLIST_FIRST_RCU(head)       catomic_rcu_read(&(head)->slh_first)
