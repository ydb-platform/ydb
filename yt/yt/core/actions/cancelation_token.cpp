#include "cancelation_token.h"

#include "cancelable_context.h"
#include "future.h"

#include <yt/yt/core/concurrency/fls.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

void TAnyCancelationToken::TStorage::Set(void* ptr) noexcept
{
    std::construct_at<void*>(reinterpret_cast<void**>(Storage_), ptr);
}

void TAnyCancelationToken::TStorage::Set() noexcept
{
    std::construct_at(Storage_);
}

////////////////////////////////////////////////////////////////////////////////

TAnyCancelationToken::TVTable::TDtor TAnyCancelationToken::TVTable::Dtor() const noexcept
{
    return Dtor_;
}

TAnyCancelationToken::TVTable::TCopyCtor TAnyCancelationToken::TVTable::CopyCtor() const noexcept
{
    return CopyCtor_;
}

TAnyCancelationToken::TVTable::TMoveCtor TAnyCancelationToken::TVTable::MoveCtor() const noexcept
{
    return MoveCtor_;
}

TAnyCancelationToken::TVTable::TIsCancelationRequested TAnyCancelationToken::TVTable::IsCancelationRequested() const noexcept
{
    return IsCancelationRequested_;
}

TAnyCancelationToken::TVTable::TCancellationError TAnyCancelationToken::TVTable::GetCancelationError() const noexcept
{
    return CancellationError_;
}

////////////////////////////////////////////////////////////////////////////////

TAnyCancelationToken::TAnyCancelationToken(TAnyCancelationToken&& other) noexcept
    : VTable_(other.VTable_)
{
    if (VTable_) {
        auto moveCtor = VTable_->MoveCtor();
        moveCtor(Storage_, std::move(other.Storage_));

        other.VTable_ = nullptr;
    }
}

TAnyCancelationToken& TAnyCancelationToken::operator= (TAnyCancelationToken&& other) noexcept
{
    if (this == &other) {
        return *this;
    }

    Reset();

    VTable_ = other.VTable_;

    if (VTable_) {
        auto moveCtor = VTable_->MoveCtor();
        moveCtor(Storage_, std::move(other.Storage_));

        other.VTable_ = nullptr;
    }

    return *this;
}

TAnyCancelationToken::TAnyCancelationToken(const TAnyCancelationToken& other) noexcept
    : VTable_(other.VTable_)
{
    if (VTable_) {
        auto copyCtor = VTable_->CopyCtor();
        copyCtor(Storage_, other.Storage_);
    }
}

TAnyCancelationToken& TAnyCancelationToken::operator= (const TAnyCancelationToken& other) noexcept
{
    if (this == &other) {
        return *this;
    }

    Reset();
    VTable_ = other.VTable_;

    if (VTable_) {
        auto copyCtor = VTable_->CopyCtor();
        copyCtor(Storage_, other.Storage_);
    }

    return *this;
}

TAnyCancelationToken::operator bool() const noexcept
{
    return VTable_ != nullptr;
}

TAnyCancelationToken::~TAnyCancelationToken()
{
    Reset();
}

bool TAnyCancelationToken::IsCancelationRequested() const noexcept
{
    if (!VTable_) {
        return false;
    }

    return VTable_->IsCancelationRequested()(Storage_);
}

const TError& TAnyCancelationToken::GetCancelationError() const noexcept
{
    YT_VERIFY(VTable_);
    return VTable_->GetCancelationError()(Storage_);
}

void TAnyCancelationToken::Reset() noexcept
{
    if (VTable_) {
        auto dtor = VTable_->Dtor();
        dtor(Storage_);
        VTable_ = nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

NConcurrency::TFlsSlot<TAnyCancelationToken> GlobalToken = {};

////////////////////////////////////////////////////////////////////////////////

TCurrentCancelationTokenGuard::TCurrentCancelationTokenGuard(TAnyCancelationToken nextToken)
    : PrevToken_(std::exchange(*GlobalToken, std::move(nextToken)))
{ }

TCurrentCancelationTokenGuard::~TCurrentCancelationTokenGuard()
{
    std::exchange(*GlobalToken, std::move(PrevToken_));
}

////////////////////////////////////////////////////////////////////////////////

struct TTokenForCancelableContext
{
    TCancelableContextPtr Context;

    bool IsCancelationRequested() const noexcept
    {
        return Context->IsCanceled();
    }

    const TError& GetCancelationError() const
    {
        return Context->GetCancelationError();
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

    bool IsCancelationRequested() const noexcept
    {
        return FutureState->IsCanceled();
    }

    const TError& GetCancelationError() const
    {
        return FutureState->GetCancelationError();
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

const TAnyCancelationToken& GetCurrentCancelationToken()
{
    return *GlobalToken;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
