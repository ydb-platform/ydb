#ifndef TLS_SCRATH_INL_H_
#error "Direct inclusion of this file is not allowed, include tls_scratch.h"
// For the sake of sane code completion.
#include "tls_scratch.h"
#endif

#include <library/cpp/yt/misc/tls.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TMutableRange<T> GetTlsScratchBuffer(size_t size)
{
    // This is a workround for std::vector<bool>.
    using TBoxed = std::array<T, 1>;
    YT_THREAD_LOCAL(std::vector<TBoxed>) tlsVector;
    auto& vector = GetTlsRef(tlsVector);
    vector.reserve(size);
    auto range = TMutableRange(reinterpret_cast<T*>(vector.data()), size);
    std::fill(range.begin(), range.end(), T());
    return range;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
