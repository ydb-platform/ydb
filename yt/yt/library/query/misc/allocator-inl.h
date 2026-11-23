#ifndef ALLOCATOR_INL_H_
#error "Direct inclusion of this file is not allowed, include allocator.h"
// For the sake of sane code completion.
#include "allocator.h"
#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TAllocatorOverChunkProvider<T>::TAllocatorOverChunkProvider(
    IMemoryChunkProviderPtr memoryChunkProvider,
    TRefCountedTypeCookie tagCookie)
    : Provider_(std::move(memoryChunkProvider))
    , Cookie_(std::move(tagCookie))
{ }

template <class T>
size_t TAllocatorOverChunkProvider<T>::max_size() const
{
    return std::numeric_limits<size_type>::max() / sizeof(T);
}

template <class T>
T* TAllocatorOverChunkProvider<T>::allocate(size_t n)
{
    auto bytesRequired = n * sizeof(T);
    auto bytesAllocated = bytesRequired + sizeof(TAllocationHolder*);
    auto* allocationHolder = Provider_->Allocate(bytesAllocated, Cookie_).release();
    char* userDataBegin = allocationHolder->GetRef().data();
    char* userDataEnd = userDataBegin + bytesRequired;

    *reinterpret_cast<TAllocationHolder**>(userDataEnd) = allocationHolder;

    return reinterpret_cast<T*>(userDataBegin);
}

template <class T>
void TAllocatorOverChunkProvider<T>::deallocate(T* p, size_t n) noexcept
{
    void* userDataEnd = p + n;
    auto* allocationHolder = *reinterpret_cast<TAllocationHolder**>(userDataEnd);
    delete allocationHolder;
}

template <class T>
template <class U>
bool TAllocatorOverChunkProvider<T>::operator==(const TAllocatorOverChunkProvider<U>& /*other*/) const noexcept
{
    return true;
}

template <class T>
template <class U>
bool TAllocatorOverChunkProvider<T>::operator!=(const TAllocatorOverChunkProvider<U>& /*other*/) const noexcept
{
    return false;
}

template <class T>
template <class U>
TAllocatorOverChunkProvider<T>::TAllocatorOverChunkProvider(const TAllocatorOverChunkProvider<U>& other)
{
    Provider_ = other.Provider_;
    Cookie_ = other.Cookie_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
