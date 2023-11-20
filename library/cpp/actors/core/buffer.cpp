#include "buffer.h"

#include <util/system/yassert.h>

#include <algorithm>

TBufferBase::TBufferBase(size_t size) noexcept
    : Size(size)
{
}

size_t
TBufferBase::GetSize() const noexcept {
    return Size;
}

void TBufferBase::SetSize(size_t size) noexcept {
    Size = size;
}

/////////////////////////////////////////////////////////////////////

template <typename PointerType>
TBufferBaseT<PointerType>::TBufferBaseT(PointerType data, size_t size) noexcept
    : TBufferBase(size)
    , Data(data)
{
}

template <typename PointerType>
PointerType
TBufferBaseT<PointerType>::GetPointer() const noexcept {
    return Data;
}

template <typename PointerType>
void TBufferBaseT<PointerType>::Assign(PointerType data, size_t size) noexcept {
    Data = data;
    Size = size;
}

template <>
void TBufferBaseT<void*>::Cut(size_t offset) noexcept {
    Y_DEBUG_ABORT_UNLESS(offset <= Size);
    Data = static_cast<char*>(Data) + offset;
    TBufferBase::Size -= offset;
}

template <>
void TBufferBaseT<const void*>::Cut(size_t offset) noexcept {
    Y_DEBUG_ABORT_UNLESS(offset <= Size);
    Data = static_cast<const char*>(Data) + offset;
    TBufferBase::Size -= offset;
}

template class TBufferBaseT<void*>;
template class TBufferBaseT<const void*>;

/////////////////////////////////////////////////////////////////////

TConstBuffer::TConstBuffer(const void* data, size_t size) noexcept
    : TBufferBaseT<const void*>(data, size)
{
}

TConstBuffer::TConstBuffer(const TMutableBuffer& buffer) noexcept
    : TBufferBaseT<const void*>(buffer.GetPointer(), buffer.GetSize())
{
}

TConstBuffer
TConstBuffer::Offset(ptrdiff_t offset, size_t size) const noexcept {
    return TConstBuffer(static_cast<const char*>(Data) + offset, std::min(Size - offset, size));
}

////////////////////////////////////////////////////////////////////////////////

TMutableBuffer::TMutableBuffer(void* data, size_t size) noexcept
    : TBufferBaseT<void*>(data, size)
{
}

TMutableBuffer
TMutableBuffer::Offset(ptrdiff_t offset, size_t size) const noexcept {
    return TMutableBuffer(static_cast<char*>(Data) + offset, std::min(Size - offset, size));
}

size_t
TMutableBuffer::CopyFrom(const TConstBuffer& buffer) const noexcept {
    const auto size = std::min(Size, buffer.Size);
    std::memcpy(Data, buffer.Data, size);
    return size;
}
