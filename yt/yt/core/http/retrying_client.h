#pragma once

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/json/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

struct IResponseChecker
    : public virtual TRefCounted
{
    virtual TError CheckError(const IResponsePtr& response) = 0;
    virtual bool IsRetriableError(const TError& error) = 0;
    virtual NYTree::INodePtr GetFormattedResponse() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IResponseChecker)

////////////////////////////////////////////////////////////////////////////////

using TJsonErrorChecker = TCallback<TError(const IResponsePtr&, const NYTree::INodePtr&)>;
using TRetryChecker = TCallback<bool(const TError&)>;

IResponseCheckerPtr CreateJsonResponseChecker(
    const NJson::TJsonFormatConfigPtr& jsonFormatConfig,
    TJsonErrorChecker errorChecker,
    TRetryChecker retryChecker = {});

////////////////////////////////////////////////////////////////////////////////

struct IRetryingClient
    : public virtual TRefCounted
{
    virtual TFuture<IResponsePtr> Get(
        const IResponseCheckerPtr& responseChecker,
        const TString& url,
        const THeadersPtr& headers = nullptr) = 0;

    virtual TFuture<IResponsePtr> Post(
        const IResponseCheckerPtr& responseChecker,
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers = nullptr) = 0;

    virtual TFuture<IResponsePtr> Patch(
        const IResponseCheckerPtr& responseChecker,
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers = nullptr) = 0;

    virtual TFuture<IResponsePtr> Put(
        const IResponseCheckerPtr& responseChecker,
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers = nullptr) = 0;

    virtual TFuture<IResponsePtr> Delete(
        const IResponseCheckerPtr& responseChecker,
        const TString& url,
        const THeadersPtr& headers = nullptr) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRetryingClient)

////////////////////////////////////////////////////////////////////////////////

IRetryingClientPtr CreateRetryingClient(
    TRetryingClientConfigPtr config,
    IClientPtr client,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
