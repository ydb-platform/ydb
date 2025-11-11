#pragma once

#include <yt/yt/core/actions/callback.h>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

template <class TSignature>
class TCGCaller;

template <class TResult, class... TArgs>
class TCGCaller<TResult(TArgs...)>
    : public NDetail::TBindStateBase
{
private:
    using TSignature = TResult(*)(TArgs...);

public:
    explicit TCGCaller(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
        const TSourceLocation& location,
#endif
        TRefCountedPtr module,
        TSignature function)
        : NDetail::TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
            location
#endif
    )
        , Module_(std::move(module))
        , FunctionPointer_(function)
    { }

    TResult Run(TCallArg<TArgs>... args)
    {
        return FunctionPointer_(std::forward<TArgs>(args)...);
    }

    static TResult StaticInvoke(TCallArg<TArgs>... args, NYT::NDetail::TBindStateBase* stateBase)
    {
        auto* state = static_cast<TCGCaller*>(stateBase);
        return state->Run(std::forward<TArgs>(args)...);
    }

private:
    TRefCountedPtr Module_;
    TSignature FunctionPointer_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

}
