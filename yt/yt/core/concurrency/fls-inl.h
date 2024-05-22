#ifndef FLS_INL_H_
#error "Direct inclusion of this file is not allowed, include fls.h"
// For the sake of sane code completion.
#include "fls.h"
#endif
#undef FLS_INL_H_

#include <library/cpp/yt/memory/memory_tag.h>

#include <library/cpp/yt/misc/tls.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

using TFlsSlotCtor = TFls::TCookie(*)();
using TFlsSlotDtor = void(*)(TFls::TCookie cookie);

int AllocateFlsSlot(TFlsSlotDtor dtor);
TFls* GetPerThreadFls();

YT_DECLARE_THREAD_LOCAL(TFls*, CurrentFls);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TFls::TCookie TFls::Get(int index) const
{
    // NB: This produces the best assembly so far, with just one
    // additional compare + branch (which is perfectly predictable).
    // Reinterpret casts are required since incrementing a pointer
    // past the allocated storage is UB.
    auto ptr = reinterpret_cast<uintptr_t>(Slots_.data()) + index * sizeof(TCookie);
    if (Y_UNLIKELY(ptr >= reinterpret_cast<uintptr_t>(Slots_.data() + Slots_.size()))) {
        return nullptr;
    }
    return *reinterpret_cast<const TCookie*>(ptr);
}

////////////////////////////////////////////////////////////////////////////////

inline TFls* GetCurrentFls()
{
    auto* fls = NDetail::CurrentFls();
    if (Y_UNLIKELY(!fls)) {
        fls = NDetail::GetPerThreadFls();
    }
    return fls;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFlsSlot<T>::TFlsSlot()
    : Index_(NDetail::AllocateFlsSlot([] (TFls::TCookie cookie) {
        delete static_cast<T*>(cookie);
    }))
{ }

template <class T>
Y_FORCE_INLINE T* TFlsSlot<T>::operator->()
{
    return GetOrCreate();
}

template <class T>
Y_FORCE_INLINE const T* TFlsSlot<T>::operator->() const
{
    return GetOrCreate();
}

template <class T>
Y_FORCE_INLINE T& TFlsSlot<T>::operator*()
{
    return *GetOrCreate();
}

template <class T>
Y_FORCE_INLINE const T& TFlsSlot<T>::operator*() const
{
    return *GetOrCreate();
}

template <class T>
Y_FORCE_INLINE T* TFlsSlot<T>::GetOrCreate() const
{
    auto cookie = GetCurrentFls()->Get(Index_);
    if (Y_UNLIKELY(!cookie)) {
        cookie = Create();
    }
    return static_cast<T*>(cookie);
}

template <class T>
T* TFlsSlot<T>::Create() const
{
    TMemoryTagGuard guard(NullMemoryTag);
    auto cookie = new T();
    GetCurrentFls()->Set(Index_, cookie);
    return static_cast<T*>(cookie);
}

template <class T>
const T* TFlsSlot<T>::Get(const TFls& fls) const
{
    return static_cast<const T*>(fls.Get(Index_));
}

template <class T>
Y_FORCE_INLINE bool TFlsSlot<T>::IsInitialized() const
{
    return static_cast<bool>(GetCurrentFls()->Get(Index_));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
