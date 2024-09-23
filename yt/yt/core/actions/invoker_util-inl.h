#ifndef INVOKER_UTIL_INL_H_
#error "Direct inclusion of this file is not allowed, include invoker_util.h"
// For the sake of sane code completion.
#include "invoker_util.h"
#endif
#undef INVOKER_UTIL_INL_H_

#include <yt/yt/core/misc/finally.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <CInvocable<void()> TOnSuccess, CInvocable<void()> TOnCancel>
void GuardedInvoke(
    const IInvokerPtr& invoker,
    TOnSuccess onSuccess,
    TOnCancel onCancel)
{
    YT_VERIFY(invoker);

    class TGuard
    {
    public:
        TGuard(TOnSuccess onSuccess, TOnCancel onCancel)
            : OnSuccess_(std::move(onSuccess))
            , OnCancel_(std::move(onCancel))
        { }

        TGuard(TGuard&& other)
            : OnSuccess_(std::move(other.OnSuccess_))
            , OnCancel_(std::move(other.OnCancel_))
            , WasInvoked_(std::exchange(other.WasInvoked_, true))
        { }

        void operator()()
        {
            WasInvoked_ = true;
            OnSuccess_();
        }

        ~TGuard()
        {
            if (!WasInvoked_) {
                OnCancel_();
            }
        }

    private:
        TOnSuccess OnSuccess_;
        TOnCancel OnCancel_;

        bool WasInvoked_ = false;
    };

    invoker->Invoke(BIND_NO_PROPAGATE(TGuard(std::move(onSuccess), std::move(onCancel))));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
