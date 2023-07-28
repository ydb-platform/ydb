#ifndef CALLBACK_INTERNAL_INL_H_
#error "Direct inclusion of this file is not allowed, include callback_internal.h"
// For the sake of sane code completion.
#include "callback_internal.h"
#endif
#undef CALLBACK_INTERNAL_INL_H_

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

inline TBindStateBase::TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    const TSourceLocation& location
#endif
    )
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    : Location(location)
#endif
{ }

inline TCallbackBase::operator bool() const
{
    return static_cast<bool>(BindState);
}

inline void TCallbackBase::Reset()
{
    BindState = nullptr;
    UntypedInvoke = nullptr;
}

inline void* TCallbackBase::GetHandle() const
{
    return (void*)((size_t)(void*)BindState.Get() ^ (size_t)(void*)UntypedInvoke);
}

inline void TCallbackBase::Swap(TCallbackBase& other)
{
    auto tempBindState = std::move(other.BindState);
    auto tempUntypedInvoke = std::move(other.UntypedInvoke);

    other.BindState = std::move(BindState);
    other.UntypedInvoke = std::move(UntypedInvoke);

    BindState = std::move(tempBindState);
    UntypedInvoke = std::move(tempUntypedInvoke);
}

#ifndef __cpp_impl_three_way_comparison
inline bool TCallbackBase::operator == (const TCallbackBase& other) const
{
    return
        BindState == other.BindState &&
        UntypedInvoke == other.UntypedInvoke;
}

inline bool TCallbackBase::operator != (const TCallbackBase& other) const
{
    return !(*this == other);
}
#endif

inline TCallbackBase::TCallbackBase(TIntrusivePtr<TBindStateBase>&& bindState)
    : BindState(std::move(bindState))
    , UntypedInvoke(nullptr)
{ }

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NDetail
