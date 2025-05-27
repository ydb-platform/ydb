#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/memory/type_erasure.h>
#include <library/cpp/yt/misc/tag_invoke_cpo.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TIsCancelationRequestedFn
    : public TTagInvokeCpoBase<TIsCancelationRequestedFn>
{ };

struct TGetCancelationErrorFn
    : public TTagInvokeCpoBase<TGetCancelationErrorFn>
{ };

inline constexpr TIsCancelationRequestedFn IsCancelationRequested = {};
inline constexpr TGetCancelationErrorFn GetCancelationError = {};

////////////////////////////////////////////////////////////////////////////////

// CancelToken is an entity which you can ask whether cancellation has been
// requested or not. If it has been requested, you can ask for cancelation error.
template <class T>
concept CCancelationToken =
    std::copyable<T> &&
    CTagInvocableS<TIsCancelationRequestedFn, bool(const T&) noexcept> &&
    CTagInvocableS<TGetCancelationErrorFn, const TError&(const T&)>;

////////////////////////////////////////////////////////////////////////////////

// We need to read/write global variable which satisfies concept CCancelationToken.
using TAnyCancelationToken = ::NYT::TAnyObject<
    TOverload<IsCancelationRequested, bool(const TErasedThis&) noexcept>,
    TOverload<GetCancelationError, const TError&(const TErasedThis&)>>;

////////////////////////////////////////////////////////////////////////////////

class TCurrentCancelationTokenGuard
{
public:
    explicit TCurrentCancelationTokenGuard(TAnyCancelationToken nextToken);
    ~TCurrentCancelationTokenGuard();

    TCurrentCancelationTokenGuard(const TCurrentCancelationTokenGuard& other) = delete;
    TCurrentCancelationTokenGuard& operator= (const TCurrentCancelationTokenGuard& other) = delete;

private:
    TAnyCancelationToken PrevToken_;
};

////////////////////////////////////////////////////////////////////////////////

TCurrentCancelationTokenGuard MakeFutureCurrentTokenGuard(void* opaqueFutureState);
TCurrentCancelationTokenGuard MakeCancelableContextCurrentTokenGuard(const TCancelableContextPtr& context);

////////////////////////////////////////////////////////////////////////////////

const TAnyCancelationToken& GetCurrentCancelationToken();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
