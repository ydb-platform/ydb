#include "cancelation_token.h"

#include "cancelable_context.h"
#include "future.h"

#include <yt/yt/core/concurrency/fls.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TNullToken
{
    friend bool TagInvoke(TTagInvokeTag<IsCancelationRequested>, const TNullToken&) noexcept
    {
        return false;
    }

    friend const TError& TagInvoke(TTagInvokeTag<GetCancelationError>, const TNullToken&) noexcept
    {
        YT_ABORT();
    }
};

static_assert(CCancelationToken<TNullToken>);

////////////////////////////////////////////////////////////////////////////////

struct TTokenForCancelableContext
{
    TCancelableContextPtr Context;

    friend bool TagInvoke(TTagInvokeTag<IsCancelationRequested>, const TTokenForCancelableContext& token) noexcept
    {
        return token.Context->IsCanceled();
    }

    friend const TError& TagInvoke(TTagInvokeTag<GetCancelationError>, const TTokenForCancelableContext& token)
    {
        return token.Context->GetCancelationError();
    }
};

static_assert(CCancelationToken<TTokenForCancelableContext>);

////////////////////////////////////////////////////////////////////////////////

struct TTokenForFuture
{
    explicit TTokenForFuture(TFutureState<void>* futureState)
        : FutureState(futureState)
    { }

    TFutureState<void>* FutureState;

    friend bool TagInvoke(TTagInvokeTag<IsCancelationRequested>, const TTokenForFuture& token) noexcept
    {
        return token.FutureState->IsCanceled();
    }

    friend const TError& TagInvoke(TTagInvokeTag<GetCancelationError>, const TTokenForFuture& token)
    {
        return token.FutureState->GetCancelationError();
    }
};

static_assert(CCancelationToken<TTokenForFuture>);

////////////////////////////////////////////////////////////////////////////////

TCurrentCancelationTokenGuard MakeFutureCurrentTokenGuard(void* opaqueFutureState)
{
    return TCurrentCancelationTokenGuard{TTokenForFuture{static_cast<NDetail::TFutureState<void>*>(opaqueFutureState)}};
}

TCurrentCancelationTokenGuard MakeCancelableContextCurrentTokenGuard(const TCancelableContextPtr& context)
{
    return TCurrentCancelationTokenGuard{TTokenForCancelableContext{.Context = context}};
}

////////////////////////////////////////////////////////////////////////////////

NConcurrency::TFlsSlot<TAnyCancelationToken> GlobalToken = {};

namespace {

void EnsureInitialized()
{
    [[unlikely]] if (!GlobalToken.IsInitialized()) {
        *GlobalToken = TNullToken{};
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCurrentCancelationTokenGuard::TCurrentCancelationTokenGuard(TAnyCancelationToken nextToken)
{
    EnsureInitialized();
    PrevToken_ = std::exchange(*GlobalToken, std::move(nextToken));
}

TCurrentCancelationTokenGuard::~TCurrentCancelationTokenGuard()
{
    std::exchange(*GlobalToken, std::move(PrevToken_));
}

////////////////////////////////////////////////////////////////////////////////

const TAnyCancelationToken& GetCurrentCancelationToken()
{
    EnsureInitialized();
    return *GlobalToken;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
