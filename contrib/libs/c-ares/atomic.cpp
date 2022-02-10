#include <util/system/spinlock.h> 
 
#include "atomic.h" 
 
EXTERN_C void acquire_lock(atomic_t *lock) 
{ 
    AcquireAdaptiveLock(lock); 
} 
 
EXTERN_C void release_lock(atomic_t *lock) 
{ 
    ReleaseAdaptiveLock(lock); 
} 
