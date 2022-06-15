#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/system/platform.h>

class TFutexLike {
private:
#ifdef _linux_
    int Value;
#else
    TAtomic Value;
    TMutex Mutex;
    TCondVar CondVar;
#endif

public:
    TFutexLike()
        : Value(0)
    {
    }

    int AddAndGet(int add) {
#ifdef _linux_
        //return __atomic_add_fetch(&Value, add, __ATOMIC_SEQ_CST);
        return __sync_add_and_fetch(&Value, add);
#else
        return AtomicAdd(Value, add);
#endif
    }

    int GetAndAdd(int add) {
        return AddAndGet(add) - add;
    }

// until we have modern GCC
#if 0
    int GetAndSet(int newValue) {
#ifdef _linux_
        return __atomic_exchange_n(&Value, newValue, __ATOMIC_SEQ_CST);
#else
        return AtomicSwap(&Value, newValue);
#endif
    }
#endif

    int Get() {
#ifdef _linux_
        //return __atomic_load_n(&Value, __ATOMIC_SEQ_CST);
        __sync_synchronize();
        return Value;
#else
        return AtomicGet(Value);
#endif
    }

    void Set(int newValue) {
#ifdef _linux_
        //__atomic_store_n(&Value, newValue, __ATOMIC_SEQ_CST);
        Value = newValue;
        __sync_synchronize();
#else
        AtomicSet(Value, newValue);
#endif
    }

    int GetAndIncrement() {
        return AddAndGet(1) - 1;
    }

    int IncrementAndGet() {
        return AddAndGet(1);
    }

    int GetAndDecrement() {
        return AddAndGet(-1) + 1;
    }

    int DecrementAndGet() {
        return AddAndGet(-1);
    }

    void Wake(size_t count = Max<size_t>());

    void Wait(int expected);
};
