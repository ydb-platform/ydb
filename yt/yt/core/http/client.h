#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

class IActiveRequest
    : public virtual TRefCounted
{
public:
    virtual TFuture<IResponsePtr> Finish() = 0;
    virtual NConcurrency::IAsyncOutputStreamPtr GetRequestStream() = 0;
    virtual IResponsePtr GetResponse() = 0;
};

DEFINE_REFCOUNTED_TYPE(IActiveRequest)

////////////////////////////////////////////////////////////////////////////////

struct IClient
    : public virtual TRefCounted
{
    virtual TFuture<IResponsePtr> Get(
        const TString& url,
        const THeadersPtr& headers = nullptr) = 0;

    virtual TFuture<IResponsePtr> Post(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers = nullptr) = 0;

    virtual TFuture<IResponsePtr> Patch(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers = nullptr) = 0;

    virtual TFuture<IResponsePtr> Put(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers = nullptr) = 0;

    virtual TFuture<IResponsePtr> Delete(
        const TString& url,
        const THeadersPtr& headers = nullptr) = 0;

    virtual TFuture<IActiveRequestPtr> StartPost(
        const TString& url,
        const THeadersPtr& headers = nullptr) = 0;

    virtual TFuture<IActiveRequestPtr> StartPatch(
        const TString& url,
        const THeadersPtr& headers = nullptr) = 0;

    virtual TFuture<IActiveRequestPtr> StartPut(
        const TString& url,
        const THeadersPtr& headers = nullptr) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    const TClientConfigPtr& config,
    const NNet::IDialerPtr& dialer,
    const IInvokerPtr& invoker);
IClientPtr CreateClient(
    const TClientConfigPtr& config,
    const NConcurrency::IPollerPtr& poller);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
