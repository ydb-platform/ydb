#pragma once

#include "client_common.h"
#include "public.h"

#include <yt/yt/client/prerequisite_client/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TPrerequisitePingOptions
{
    bool EnableRetries = false;
};

struct TPrerequisiteAbortOptions
    : public TMutatingOptions
{
    bool Force = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IPrerequisite
    : public virtual TRefCounted
{
    using TAbortedHandlerSignature = void(const TError& error);
    using TAbortedHandler = TCallback<TAbortedHandlerSignature>;
    DECLARE_INTERFACE_SIGNAL(TAbortedHandlerSignature, Aborted);

    virtual IClientPtr GetClient() const = 0;
    virtual TDuration GetTimeout() const = 0;

    virtual TFuture<void> Ping(const TPrerequisitePingOptions& options = {}) = 0;
    virtual TFuture<void> Abort(const TPrerequisiteAbortOptions& options = {}) = 0;

    virtual NPrerequisiteClient::TPrerequisiteId GetId() const = 0;

    //! Verified dynamic casts to a more specific interface.
    template <class TDerived>
    TDerived* As();
    template <class TDerived>
    TDerived* TryAs();

    template <class TDerived>
    const TDerived* As() const;
    template <class TDerived>
    const TDerived* TryAs() const;
};

DEFINE_REFCOUNTED_TYPE(IPrerequisite)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

#define PREREQUISITE_INL_H_
#include "prerequisite-inl.h"
#undef PREREQUISITE_INL_H_
