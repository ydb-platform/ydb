/*
 * Generic reference counting infrastructure.
 *
 * Note: refcounts need more relaxed memory ordering that regular atomics.
 *
 * The increments provide no ordering, because it's expected that the object is
 * held by something else that provides ordering.
 *
 * The decrements provide release order, such that all the prior loads and
 * stores will be issued before, it also provides a control dependency, which
 * will order against the subsequent free().
 *
 * The control dependency is against the load of the cmpxchg (ll/sc) that
 * succeeded. This means the stores aren't fully ordered, but this is fine
 * because the 1->0 transition indicates no concurrency.
 *
 * The decrements dec_and_test() and sub_and_test() also provide acquire
 * ordering on success.
 */

#pragma once

#include <stdbool.h>
#include "catomic.h"
#include "platform.h"

struct objref {
    unsigned long refcount;
    void (*release)(struct objref *objref);
};

static inline void objref_init(struct objref *objref,
                               void (*release)(struct objref *objref))
{
    objref->release = release;
    catomic_set(&objref->refcount, 1);
}

static inline unsigned int objref_read(struct objref *objref)
{
    return catomic_read(&objref->refcount);
}

static inline void refcount_inc(unsigned long *ptr)
{
    __atomic_fetch_add(ptr, 1, __ATOMIC_RELAXED);
}

static inline void objref_get(struct objref *objref)
{
    refcount_inc(&objref->refcount);
}

static inline bool refcount_dec_and_test(unsigned long *ptr)
{
    const int memory_order =
    #if VHD_HAS_FEATURE(thread_sanitizer)
        __ATOMIC_ACQ_REL;
    #else
        __ATOMIC_RELEASE;
    #endif
    unsigned long old = __atomic_fetch_sub(ptr, 1, memory_order);

    if (old == 1) {
        smp_mb_acquire();
        return true;
    }
    return false;
}

/*
 * Decrement refcount for object, and call @release if it drops to zero.
 * Return true if the object was removed, otherwise return false.
 * Note: only "true" is trustworthy, "false" doesn't prevent another thread
 * from releasing the object.
 */
static inline bool objref_put(struct objref *objref)
{
    if (refcount_dec_and_test(&objref->refcount)) {
        objref->release(objref);
        return true;
    }
    return false;
}
