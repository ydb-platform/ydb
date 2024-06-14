#include "config.h"

#include "retrying_client.h"
#include "http.h"
#include "private.h"

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/public.h>
#include <yt/yt/core/json/json_parser.h>

namespace NYT::NHttp {

using namespace NNet;
using namespace NYTree;
using namespace NConcurrency;

static constexpr auto& Logger = HttpLogger;

////////////////////////////////////////////////////////////////////////////////

class TResponseCheckerBase
    : public IResponseChecker
{
public:
    TResponseCheckerBase(TRetryChecker retryChecker = {})
        : RetryChecker_(retryChecker ? std::move(retryChecker) : BIND(&DefaultRetryChecker))
    { }

    virtual bool IsRetriableError(const TError& error) override
    {
        return RetryChecker_(error);
    }

    virtual TError CheckError(const IResponsePtr& response) override = 0;
    virtual NYTree::INodePtr GetFormattedResponse() const override = 0;

protected:
    static bool DefaultRetryChecker(const TError& /*error*/)
    {
        return true;
    }

private:
    TRetryChecker RetryChecker_;
};

////////////////////////////////////////////////////////////////////////////////

class TJsonResponseChecker
    : public TResponseCheckerBase
{
public:
    TJsonResponseChecker(
        NJson::TJsonFormatConfigPtr jsonFormatConfig,
        TJsonErrorChecker errorChecker,
        TRetryChecker retryChecker)
        : TResponseCheckerBase(std::move(retryChecker))
        , JsonFormatConfig_(std::move(jsonFormatConfig))
        , ErrorChecker_(std::move(errorChecker))
    { }

    TError CheckError(const IResponsePtr& response) override
    {
        try {
            auto body = response->ReadAll();
            TMemoryInput stream(body.Begin(), body.Size());
            auto factory = NYTree::CreateEphemeralNodeFactory();
            auto builder = NYTree::CreateBuilderFromFactory(factory.get());
            NJson::ParseJson(&stream, builder.get(), JsonFormatConfig_);
            Json_ = builder->EndTree();
        } catch (const std::exception& ex) {
            return TError("Error parsing response")
                << ex;
        }

        if (!Json_) {
            return TError("Got empty result");
        }

        try {
            return ErrorChecker_(response, Json_);
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    NYTree::INodePtr GetFormattedResponse() const override
    {
        return Json_;
    }

private:
    const NJson::TJsonFormatConfigPtr JsonFormatConfig_;
    const TJsonErrorChecker ErrorChecker_;

    INodePtr Json_;
};

////////////////////////////////////////////////////////////////////////////////

IResponseCheckerPtr CreateJsonResponseChecker(
    const NJson::TJsonFormatConfigPtr& jsonFormatConfig,
    TJsonErrorChecker errorChecker,
    TRetryChecker retryChecker)
{
    return New<TJsonResponseChecker>(jsonFormatConfig,
        std::move(errorChecker),
        std::move(retryChecker));
}

////////////////////////////////////////////////////////////////////////////////

class TRetryingClient
    : public IRetryingClient
{
public:
    TRetryingClient(
        TRetryingClientConfigPtr config,
        IClientPtr client,
        IInvokerPtr invoker)
        : Config_(std::move(config))
        , Invoker_(std::move(invoker))
        , UnderlyingClient_(std::move(client))
    { }

    TFuture<IResponsePtr> Get(
        const IResponseCheckerPtr& responseChecker,
        const TString& url,
        const THeadersPtr& headers) override
    {
        return MakeRequest(&IClient::Get, responseChecker, url, headers);
    }

    TFuture<IResponsePtr> Post(
        const IResponseCheckerPtr& responseChecker,
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers) override
    {
        return MakeRequest(&IClient::Post, responseChecker, url, body, headers);
    }

    TFuture<IResponsePtr> Patch(
        const IResponseCheckerPtr& responseChecker,
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers) override
    {
        return MakeRequest(&IClient::Patch, responseChecker, url, body, headers);
    }

    TFuture<IResponsePtr> Put(
        const IResponseCheckerPtr& responseChecker,
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers) override
    {
        return MakeRequest(&IClient::Put, responseChecker, url, body, headers);
    }

    TFuture<IResponsePtr> Delete(
        const IResponseCheckerPtr& responseChecker,
        const TString& url,
        const THeadersPtr& headers) override
    {
        return MakeRequest(&IClient::Delete, responseChecker, url, headers);
    }

private:
    const TRetryingClientConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const IClientPtr UnderlyingClient_;

private:
    template <typename TCallable, typename... Args>
    TFuture<IResponsePtr> MakeRequest(
        TCallable&& func,
        const IResponseCheckerPtr& responseChecker,
        const TString& url,
        Args&&... args)
    {
        return BIND([=, this, this_ = MakeStrong(this), func = std::move(func), ...args = std::move(args)] {
            return DoMakeRequest(std::move(func), responseChecker, url, std::forward<Args>(args)...);
        }).AsyncVia(Invoker_).Run();
    }

    template <typename TCallable, typename... Args>
    IResponsePtr DoMakeRequest(
        TCallable&& func,
        const IResponseCheckerPtr& responseChecker,
        const TString& url,
        Args&&... args)
    {
        const auto deadline = TInstant::Now() + Config_->RequestTimeout;
        const auto sanitizedUrl = SanitizeUrl(url);

        YT_LOG_DEBUG("Making request (Url: %v, Deadline: %v, MaxAttemptCount: %v)",
            sanitizedUrl,
            deadline,
            Config_->MaxAttemptCount);

        std::vector<TError> accumulatedErrors;
        int attempt = 0;

        const auto shouldRetry = [&] (const TError& error) {
            const auto isRetriableError = responseChecker->IsRetriableError(error);
            auto attemptError = TError("Request attempt %v failed", attempt)
                << error
                << TErrorAttribute("attempt", attempt);

            YT_LOG_WARNING(
                attemptError,
                "Request attempt failed (Url: %v, Attempt: %v, Retriable: %v)",
                sanitizedUrl,
                attempt,
                isRetriableError);

            accumulatedErrors.push_back(std::move(attemptError));
            return isRetriableError && TInstant::Now() < deadline && attempt < Config_->MaxAttemptCount;
        };

        while (true) {
            ++attempt;
            auto future = BIND(func, UnderlyingClient_, url, std::forward<Args>(args)...)();
            const auto rspOrError = WaitFor(future.WithTimeout(Config_->AttemptTimeout));
            if (!rspOrError.IsOK()) {
                if (!shouldRetry(rspOrError)) {
                    break;
                }
                continue;
            }

            const auto& rsp = rspOrError.Value();
            const auto checkError = responseChecker->CheckError(rsp);
            if (checkError.IsOK()) {
                return rsp;
            }

            if (!shouldRetry(checkError)) {
                break;
            }

            auto now = TInstant::Now();
            TDelayedExecutor::WaitForDuration(std::min(Config_->BackoffTimeout, deadline - now));
        }

        THROW_ERROR_EXCEPTION("HTTP request failed")
            << std::move(accumulatedErrors)
            << TErrorAttribute("url", sanitizedUrl)
            << TErrorAttribute("attempt_count", attempt)
            << TErrorAttribute("max_attempt_count", Config_->MaxAttemptCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

IRetryingClientPtr CreateRetryingClient(
    TRetryingClientConfigPtr config,
    IClientPtr client,
    IInvokerPtr invoker)
{
    return New<TRetryingClient>(std::move(config), std::move(client), std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

}
