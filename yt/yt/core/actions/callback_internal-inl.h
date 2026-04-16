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
    return static_cast<bool>(BindState_);
}

inline void TCallbackBase::Reset()
{
    BindState_ = nullptr;
    UntypedInvoke_ = nullptr;
}

inline void* TCallbackBase::GetHandle() const
{
    return (void*)((size_t)(void*)BindState_.Get() ^ (size_t)(void*)UntypedInvoke_);
}

inline void TCallbackBase::Swap(TCallbackBase& other) noexcept
{
    auto tempBindState = std::move(other.BindState_);
    auto tempUntypedInvoke = other.UntypedInvoke_;

    other.BindState_ = std::move(BindState_);
    other.UntypedInvoke_ = UntypedInvoke_;

    BindState_ = std::move(tempBindState);
    UntypedInvoke_ = tempUntypedInvoke;
}

#ifndef __cpp_impl_three_way_comparison
inline bool TCallbackBase::operator==(const TCallbackBase& other) const
{
    return
        BindState_ == other.BindState_ &&
        UntypedInvoke_ == other.UntypedInvoke_;
}

inline bool TCallbackBase::operator!=(const TCallbackBase& other) const
{
    return !(*this == other);
}
#endif

inline TCallbackBase::TCallbackBase(TIntrusivePtr<TBindStateBase>&& bindState) noexcept
    : BindState_(std::move(bindState))
    , UntypedInvoke_(nullptr)
{ }

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NDetail
