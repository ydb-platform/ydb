#ifndef TLS_SCRATH_INL_H_
#error "Direct inclusion of this file is not allowed, include tls_scratch.h"
// For the sake of sane code completion.
#include "tls_scratch.h"
#endif

#include <library/cpp/yt/misc/tls.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
YT_PREVENT_TLS_CACHING TMutableRange<T> GetTlsScratchBuffer(size_t size)
{
    // This is a workround for std::vector<bool>.
    using TBoxed = std::array<T, 1>;
    thread_local std::vector<TBoxed> tlsVector;
    tlsVector.reserve(size);
    auto range = TMutableRange(reinterpret_cast<T*>(tlsVector.data()), size);
    std::fill(range.begin(), range.end(), T());
    return range;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
