#pragma once

extern "C" {
    #include "hack.h"
    #include "spinlock.h"
}

#define SPINLOCK_INITIALIZER { _SPINLOCK_INITIALIZER }

struct TCMalloc_SpinLock {
    volatile spinlock_t private_lockword_;

    inline void Init() noexcept {
        private_lockword_ = _SPINLOCK_INITIALIZER;
    }

    inline void Finalize() noexcept {
    }

    inline void Lock() noexcept {
        _SPINLOCK(&private_lockword_);
    }

    inline void Unlock() noexcept {
        _SPINUNLOCK(&private_lockword_);
    }
};

class TCMalloc_SpinLockHolder {
    private:
        TCMalloc_SpinLock* lock_;

    public:
        inline explicit TCMalloc_SpinLockHolder(TCMalloc_SpinLock* l)
            : lock_(l)
        {
            l->Lock();
        }

        inline ~TCMalloc_SpinLockHolder() {
            lock_->Unlock();
        }
};

// Short-hands for convenient use by tcmalloc.cc
typedef TCMalloc_SpinLock SpinLock;
typedef TCMalloc_SpinLockHolder SpinLockHolder;
