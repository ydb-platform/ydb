#ifndef ROW_BUFFER_INL_H_
#error "Direct inclusion of this file is not allowed, include row_buffer.h"
// For the sake of sane code completion.
#include "row_buffer.h"
#endif
#undef ROW_BUFFER_INL_H_

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TTag>
TRowBuffer::TRowBuffer(
    TTag /*tag*/,
    size_t startChunkSize,
    IMemoryUsageTrackerPtr tracker,
    bool allowMemoryOvercommit)
    : Pool_(
        GetRefCountedTypeCookie<TTag>(),
        GetTrackedMemoryChunkProvider(std::move(tracker), allowMemoryOvercommit),
        startChunkSize)
{
    static_assert(IsEmptyClass<TTag>());
}

template <class TTag>
TRowBuffer::TRowBuffer(
    TTag /*tag*/,
    IMemoryChunkProviderPtr chunkProvider)
    : Pool_(
        GetRefCountedTypeCookie<TTag>(),
        std::move(chunkProvider))
{
    static_assert(IsEmptyClass<TTag>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
