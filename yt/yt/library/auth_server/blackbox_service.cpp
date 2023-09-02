#include "blackbox_service.h"

#include "config.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/http.h>

#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>

#include <yt/yt/core/json/json_parser.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/library/tvm/service/tvm_service.h>

namespace NYT::NAuth {

using namespace NConcurrency;
using namespace NHttp;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TBlackboxService
    : public IBlackboxService
{
public:
    TBlackboxService(
        TBlackboxServiceConfigPtr config,
        ITvmServicePtr tvmService,
        IPollerPtr poller,
        NProfiling::TProfiler profiler)
        : Config_(std::move(config))
        , TvmService_(std::move(tvmService))
        , HttpClient_(Config_->Secure
            ? NHttps::CreateClient(Config_->HttpClient, std::move(poller))
            : NHttp::CreateClient(Config_->HttpClient, std::move(poller)))
        , BlackboxCalls_(profiler.Counter("/blackbox_calls"))
        , BlackboxCallErrors_(profiler.Counter("/blackbox_call_errors"))
        , BlackboxCallFatalErrors_(profiler.Counter("/blackbox_call_fatal_errors"))
        , BlackboxCallTime_(profiler.Timer("/blackbox_call_time"))
    { }

    TFuture<INodePtr> Call(
        const TString& method,
        const THashMap<TString, TString>& params) override
    {
        return BIND(&TBlackboxService::DoCall, MakeStrong(this), method, params)
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
    }

    TErrorOr<TString> GetLogin(const NYTree::INodePtr& reply) const override
    {
        if (Config_->UseLowercaseLogin) {
            return GetByYPath<TString>(reply, "/attributes/1008");
        } else {
            return GetByYPath<TString>(reply, "/login");
        }
    }

private:
    const TBlackboxServiceConfigPtr Config_;
    const ITvmServicePtr TvmService_;

    const NHttp::IClientPtr HttpClient_;

    NProfiling::TCounter BlackboxCalls_;
    NProfiling::TCounter BlackboxCallErrors_;
    NProfiling::TCounter BlackboxCallFatalErrors_;
    NProfiling::TEventTimer BlackboxCallTime_;

private:
    INodePtr DoCall(
        const TString& method,
        const THashMap<TString, TString>& params)
    {
        auto deadline = TInstant::Now() + Config_->RequestTimeout;
        auto callId = TGuid::Create();

        TSafeUrlBuilder builder;
        builder.AppendString(Format("%v://%v:%v/blackbox?",
            Config_->Secure ? "https" : "http",
            Config_->Host,
            Config_->Port));
        builder.AppendParam(TStringBuf("method"), method);
        for (const auto& param : params) {
            builder.AppendChar('&');
            builder.AppendParam(param.first, param.second);
        }
        builder.AppendChar('&');
        builder.AppendParam("attributes", "1008");
        builder.AppendChar('&');
        builder.AppendParam("format", "json");

        auto realUrl = builder.FlushRealUrl();
        auto safeUrl = builder.FlushSafeUrl();

        auto httpHeaders = New<THeaders>();
        if (TvmService_) {
            httpHeaders->Add("X-Ya-Service-Ticket",
                TvmService_->GetServiceTicket(Config_->BlackboxServiceId));
        }

        std::vector<TError> accumulatedErrors;

        for (int attempt = 1; TInstant::Now() < deadline || attempt == 1; ++attempt) {
            INodePtr result;
            try {
                BlackboxCalls_.Increment();
                result = DoCallOnce(
                    callId,
                    attempt,
                    realUrl,
                    safeUrl,
                    httpHeaders,
                    deadline);
            } catch (const std::exception& ex) {
                BlackboxCallErrors_.Increment();
                YT_LOG_WARNING(
                    ex,
                    "Blackbox call attempt failed, backing off (CallId: %v, Attempt: %v)",
                    callId,
                    attempt);
                auto error = TError("Blackbox call attempt %v failed", attempt)
                    << ex
                    << TErrorAttribute("call_id", callId)
                    << TErrorAttribute("attempt", attempt);
                accumulatedErrors.push_back(std::move(error));
            }

            // Check for known exceptions to retry.
            if (result) {
                auto exceptionNode = result->AsMap()->FindChild("exception");
                if (!exceptionNode || exceptionNode->GetType() != ENodeType::Map) {
                    // No exception information, go as-is.
                    return result;
                }

                auto exceptionIdNode = exceptionNode->AsMap()->FindChild("id");
                if (!exceptionIdNode || exceptionIdNode->GetType() != ENodeType::Int64) {
                    // No exception information, go as-is.
                    return result;
                }

                auto errorNode = result->AsMap()->FindChild("error");
                auto blackboxError =
                    errorNode && errorNode->GetType() == ENodeType::String
                    ? TError(errorNode->GetValue<TString>())
                    : TError("Blackbox did not provide any human-readable error details");

                switch (static_cast<EBlackboxException>(exceptionIdNode->GetValue<i64>())) {
                    case EBlackboxException::Ok:
                        return result;
                    case EBlackboxException::DBFetchFailed:
                    case EBlackboxException::DBException:
                        YT_LOG_WARNING(blackboxError,
                            "Blackbox has raised an exception, backing off (CallId: %v, Attempt: %v)",
                            callId,
                            attempt);
                        break;
                    default:
                        YT_LOG_WARNING(blackboxError,
                            "Blackbox has raised an exception (CallId: %v, Attempt: %v)",
                            callId,
                            attempt);
                        BlackboxCallFatalErrors_.Increment();
                        THROW_ERROR_EXCEPTION("Blackbox has raised an exception")
                            << TErrorAttribute("call_id", callId)
                            << TErrorAttribute("attempt", attempt)
                            << blackboxError;
                }
            }

            auto now = TInstant::Now();
            if (now > deadline) {
                break;
            }

            TDelayedExecutor::WaitForDuration(std::min(Config_->BackoffTimeout, deadline - now));
        }

        BlackboxCallFatalErrors_.Increment();
        THROW_ERROR_EXCEPTION("Blackbox call failed")
            << std::move(accumulatedErrors)
            << TErrorAttribute("call_id", callId);
    }

    static NJson::TJsonFormatConfigPtr MakeJsonFormatConfig()
    {
        auto config = New<NJson::TJsonFormatConfig>();
        config->EncodeUtf8 = false; // Hipsters use real Utf8.
        return config;
    }

    INodePtr DoCallOnce(
        TGuid callId,
        int attempt,
        const TString& realUrl,
        const TString& safeUrl,
        const THeadersPtr& headers,
        TInstant deadline)
    {
        auto onError = [&] (TError error) {
            error.MutableAttributes()->Set("call_id", callId);
            YT_LOG_DEBUG(error);
            THROW_ERROR(error);
        };

        NProfiling::TWallTimer timer;
        auto timeout = std::min(deadline - TInstant::Now(), Config_->AttemptTimeout);

        YT_LOG_DEBUG("Calling Blackbox (Url: %v, CallId: %v, Attempt: %v, Timeout: %v)",
            safeUrl,
            callId,
            attempt,
            timeout);

        auto rspOrError = WaitFor(HttpClient_->Get(realUrl, headers).WithTimeout(timeout));
        if (!rspOrError.IsOK()) {
            onError(TError("Blackbox call failed")
                << rspOrError);
        }

        const auto& rsp = rspOrError.Value();
        if (rsp->GetStatusCode() != EStatusCode::OK) {
            onError(TError("Blackbox call returned HTTP status code %v",
                static_cast<int>(rsp->GetStatusCode())));
        }

        INodePtr rootNode;
        try {

            YT_LOG_DEBUG("Started reading response body from Blackbox (CallId: %v, Attempt: %v)",
                callId,
                attempt);

            auto body = rsp->ReadAll();

            YT_LOG_DEBUG("Finished reading response body from Blackbox (CallId: %v, Attempt: %v)",
                callId,
                attempt);

            TMemoryInput stream(body.Begin(), body.Size());
            auto factory = NYTree::CreateEphemeralNodeFactory();
            auto builder = NYTree::CreateBuilderFromFactory(factory.get());
            static const auto Config = MakeJsonFormatConfig();
            NJson::ParseJson(&stream, builder.get(), Config);
            rootNode = builder->EndTree();

            BlackboxCallTime_.Record(timer.GetElapsedTime());
            YT_LOG_DEBUG("Parsed Blackbox daemon reply (CallId: %v, Attempt: %v)",
                callId,
                attempt);
        } catch (const std::exception& ex) {
            onError(TError(
                "Error parsing Blackbox response")
                << ex);
        }

        if (rootNode->GetType() != ENodeType::Map) {
            THROW_ERROR_EXCEPTION("Blackbox has returned an improper result")
                << TErrorAttribute("expected_result_type", ENodeType::Map)
                << TErrorAttribute("actual_result_type", rootNode->GetType());
        }

        return rootNode;
    }
};

IBlackboxServicePtr CreateBlackboxService(
    TBlackboxServiceConfigPtr config,
    ITvmServicePtr tvmService,
    IPollerPtr poller,
    NProfiling::TProfiler profiler)
{
    return New<TBlackboxService>(
        std::move(config),
        std::move(tvmService),
        std::move(poller),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
