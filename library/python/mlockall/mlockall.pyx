cdef extern from "<util/system/error.h>":
    int LastSystemError()

cdef extern from "<util/system/mlock.h>":
    cdef enum ELockAllMemoryFlag:
        LockCurrentMemory
        LockFutureMemory
    cppclass ELockAllMemoryFlags:
        operator=(ELockAllMemoryFlag)
    void LockAllMemory(ELockAllMemoryFlags flags) except+

def mlockall_current():
    cdef ELockAllMemoryFlags flags
    try:
        flags = LockCurrentMemory
        LockAllMemory(flags)
        return 0
    except Exception:
        return LastSystemError()
