#include "config.h"

#include "retriable_client.h"
#include "private.h"

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/public.h>
#include <yt/yt/core/json/json_parser.h>

namespace NYT::NHttp {

using namespace NNet;
using namespace NYTree;
using namespace NConcurrency;

static const auto& Logger = HttpLogger;

////////////////////////////////////////////////////////////////////////////////

class TJsonResponseChecker
    : public IResponseChecker
{
public:
    TJsonResponseChecker(
        TJsonErrorChecker errorChecker,
        NJson::TJsonFormatConfigPtr jsonFormatConfig)
        : JsonFormatConfig_(std::move(jsonFormatConfig))
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
            auto result = ErrorChecker_(response, Json_);
            return result;
        } catch (const std::exception& err) {
            return err;
        }
    }

    NYTree::INodePtr GetFormattedResponse() const override
    {
        return Json_;
    }


private:
    const NJson::TJsonFormatConfigPtr JsonFormatConfig_;
    INodePtr Json_;
    TJsonErrorChecker ErrorChecker_;
    TError Error_;
};

////////////////////////////////////////////////////////////////////////////////

IResponseCheckerPtr CreateJsonResponseChecker(
    TJsonErrorChecker errorChecker,
    const NJson::TJsonFormatConfigPtr& jsonFormatConfig)
{
    return New<TJsonResponseChecker>(std::move(errorChecker), jsonFormatConfig);
}

////////////////////////////////////////////////////////////////////////////////

class TRetrialbeClient
    : public IRetriableClient
{
public:
    TRetrialbeClient(
        TRetrialbeClientConfigPtr config,
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
    const TRetrialbeClientConfigPtr Config_;
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
        return BIND([=, this, this_ = MakeStrong(this), func = std::move(func), ...args = std::move(args)] () {
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
        while (attempt == 0 || (TInstant::Now() < deadline && attempt < Config_->MaxAttemptCount)) {
            ++attempt;
            auto future = BIND(func, UnderlyingClient_, url, std::forward<Args>(args)...)();
            auto rspOrError = WaitFor(future.WithTimeout(Config_->AttemptTimeout));
            if (!rspOrError.IsOK()) {
                auto error = TError("Request attempt %v failed", attempt)
                    << rspOrError
                    << TErrorAttribute("attempt", attempt);

                YT_LOG_WARNING(
                    error,
                    "Request attempt failed (Url: %v, Attempt: %v)",
                    sanitizedUrl,
                    attempt);
                accumulatedErrors.push_back(std::move(error));
                continue;
            }

            auto& rsp = rspOrError.Value();
            const auto checkError = responseChecker->CheckError(rsp);
            if (checkError.IsOK()) {
                return rsp;
            }

            auto error = TError("Error checking response")
                << checkError
                << TErrorAttribute("attempt", attempt);
            YT_LOG_WARNING(
                error,
                "Request attempt failed while checking response (Url: %v, Attempt: %v)",
                sanitizedUrl,
                attempt);
            accumulatedErrors.push_back(std::move(error));

            auto now = TInstant::Now();
            if (now > deadline) {
                break;
            }
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

IRetriableClientPtr CreateRetriableClient(
    TRetrialbeClientConfigPtr config,
    IClientPtr client,
    IInvokerPtr invoker)
{
    return New<TRetrialbeClient>(std::move(config), std::move(client), std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

}
