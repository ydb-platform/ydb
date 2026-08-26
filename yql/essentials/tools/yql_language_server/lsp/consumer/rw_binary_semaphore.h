#pragma once

#include <util/generic/yexception.h>

#include <util/system/mutex.h>
#include <util/system/sem.h>

namespace NLsp {

/// @note that writers starvation is possible.
class TRWBinarySemaphore final {
public:
    void AcquireRead() noexcept;
    void ReleaseRead() noexcept;

    void AcquireWrite() noexcept;
    void ReleaseWrite() noexcept;

private:
    TFastSemaphore Resource_{1};
    TMutex ReadersMutex_;
    size_t Readers_ = 0;
};

} // namespace NLsp
