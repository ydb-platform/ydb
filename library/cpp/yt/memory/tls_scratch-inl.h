#ifndef TLS_SCRATH_INL_H_
#error "Direct inclusion of this file is not allowed, include tls_scratch.h"
// For the sake of sane code completion.
#include "tls_scratch.h"
#endif

#include <library/cpp/yt/misc/tls.h>

#include <util/generic/bitops.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
YT_PREVENT_TLS_CACHING TMutableRange<T> GetTlsScratchBuffer(size_t size)
{
    thread_local std::unique_ptr<T[]> scratchBuffer;
    thread_local size_t scratchBufferSize;
    if (scratchBufferSize < size) {
        scratchBufferSize = FastClp2(size);
        scratchBuffer = std::unique_ptr<T[]>(new T[scratchBufferSize]);
    }
    std::fill(scratchBuffer.get(), scratchBuffer.get() + size, T());
    return TMutableRange(scratchBuffer.get(), size);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
