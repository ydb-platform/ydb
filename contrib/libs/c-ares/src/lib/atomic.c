#include "atomic.h"

#if defined(_WIN32)
  #include <windows.h>
#else
  #include <sched.h>
#endif

static void yield_thread(void)
{
#if defined(_WIN32)
    SwitchToThread();
#else
    sched_yield();
#endif
}

void acquire_lock(atomic_t *lock)
{
    while (__atomic_exchange_n(lock, 1, __ATOMIC_ACQUIRE)) {
        while (__atomic_load_n(lock, __ATOMIC_RELAXED)) {
            yield_thread();
        }
    }
}

void release_lock(atomic_t *lock)
{
    __atomic_store_n(lock, 0, __ATOMIC_RELEASE);
}
