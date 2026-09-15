#include "rw_binary_semaphore.h"

namespace NLsp {

void TRWBinarySemaphore::AcquireRead() noexcept {
    with_lock (ReadersMutex_) {
        Readers_ += 1;
        if (Readers_ == 1) {
            Resource_.Acquire();
        }
    }
}

void TRWBinarySemaphore::ReleaseRead() noexcept {
    with_lock (ReadersMutex_) {
        Y_ENSURE(0 < Readers_);
        Readers_ -= 1;
        if (Readers_ == 0) {
            Resource_.Release();
        }
    }
}

void TRWBinarySemaphore::AcquireWrite() noexcept {
    Resource_.Acquire();
}

void TRWBinarySemaphore::ReleaseWrite() noexcept {
    Resource_.Release();
}

} // namespace NLsp
