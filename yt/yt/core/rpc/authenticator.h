#pragma once

#include "public.h"

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TAuthenticationResult
{
    std::string User;
    TString Realm;
    TString UserTicket;
};

struct TAuthenticationContext
{
    const NRpc::NProto::TRequestHeader* Header;
    NNet::TNetworkAddress UserIP;
    //! True iff the request comes from the current process.
    bool IsLocal;
};

struct IAuthenticator
    : public virtual TRefCounted
{
    //! Returns true if #context contains credentials that can be parsed by
    //! this authenticator.
    //!
    //! If this method returns true, AsyncAuthenticate may still return an error,
    //! although in such case the composite authenticator will not proceed with
    //! another underlying authenticator.
    //!
    //! Also, this method may be invoked multiple times for the same context.
    //! Moreover, the returned values are allowed to be different which is useful
    //! in rare occasions, e.g. in event of dynamic configuration change.
    virtual bool CanAuthenticate(const TAuthenticationContext& context) = 0;

    //! Validates authentication credentials in #context.
    //! Returns either an error or authentication result containing
    //! the actual (and validated) username.
    //!
    //! Should be called only after CanAuthenticate(context) returns true.
    virtual TFuture<TAuthenticationResult> AsyncAuthenticate(
        const TAuthenticationContext& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAuthenticator)

////////////////////////////////////////////////////////////////////////////////

//! Returns the authenticator that sequentially forwards the request to
//! all elements of #authenticators until a non-null response arrives.
//! If no such response is discovered, an error is returned.
IAuthenticatorPtr CreateCompositeAuthenticator(
    std::vector<IAuthenticatorPtr> authenticators);

//! Returns the authenticator that accepts any request.
IAuthenticatorPtr CreateNoopAuthenticator();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
