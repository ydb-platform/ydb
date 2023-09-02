#include "oauth_service.h"

#include "config.h"
#include "private.h"
#include "helpers.h"

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/retrying_client.h>

#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/poller.h>

#include <yt/yt/core/json/json_parser.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NAuth {

using namespace NConcurrency;
using namespace NHttp;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TOAuthService
    : public IOAuthService
{
public:
    TOAuthService(
        TOAuthServiceConfigPtr config,
        IPollerPtr poller,
        NProfiling::TProfiler profiler)
        : Config_(std::move(config))
        , HttpClient_(
            CreateRetryingClient(
                Config_->RetryingClient,
                Config_->Secure
                    ? NHttps::CreateClient(Config_->HttpClient, poller)
                    : NHttp::CreateClient(Config_->HttpClient, poller),
                poller->GetInvoker()))
        , OAuthCalls_(profiler.Counter("/oauth_calls"))
        , OAuthCallErrors_(profiler.Counter("/oauth_call_errors"))
        , OAuthCallTime_(profiler.Timer("/oauth_call_time"))
    { }

    TFuture<TOAuthUserInfoResult> GetUserInfo(const TString& accessToken) override
    {
        return BIND(&TOAuthService::DoGetUserInfo, MakeStrong(this), accessToken)
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
    }

private:
    const TOAuthServiceConfigPtr Config_;
    const NHttp::IRetryingClientPtr HttpClient_;

    NProfiling::TCounter OAuthCalls_;
    NProfiling::TCounter OAuthCallErrors_;
    NProfiling::TEventTimer OAuthCallTime_;

    static NJson::TJsonFormatConfigPtr MakeJsonFormatConfig()
    {
        auto config = New<NJson::TJsonFormatConfig>();
        // Additional string conversion is not necessary in this case.
        config->EncodeUtf8 = false;
        return config;
    }

    TOAuthUserInfoResult DoGetUserInfo(const TString& accessToken)
    {
        OAuthCalls_.Increment();

        auto callId = TGuid::Create();
        auto httpHeaders = New<THeaders>();
        httpHeaders->Add("Authorization", Format("%v %v", Config_->AuthorizationHeaderPrefix, accessToken));

        auto jsonResponseChecker = CreateJsonResponseChecker(
            BIND(&TOAuthService::DoCheckUserInfoResponse, MakeStrong(this)),
            MakeJsonFormatConfig());

        const auto url = Format("%v://%v:%v/%v",
            Config_->Secure ? "https" : "http",
            Config_->Host,
            Config_->Port,
            Config_->UserInfoEndpoint);

        YT_LOG_DEBUG("Calling OAuth get user info (Url: %v, CallId: %v)",
            NHttp::SanitizeUrl(url),
            callId);

        auto result = [&] {
            NProfiling::TWallTimer timer;
            auto result = WaitFor(HttpClient_->Get(jsonResponseChecker, url, httpHeaders));
            OAuthCallTime_.Record(timer.GetElapsedTime());
            return result;
        }();

        if (!result.IsOK()) {
            OAuthCallErrors_.Increment();
            auto error = TError(NRpc::EErrorCode::InvalidCredentials, "OAuth call failed")
                << result
                << TErrorAttribute("call_id", callId);
            YT_LOG_WARNING(error);
            THROW_ERROR(error);
        }

        const auto& formattedResponose = jsonResponseChecker->GetFormattedResponse()->AsMap();
        auto userInfo = TOAuthUserInfoResult{
            .Login = formattedResponose->GetChildValueOrThrow<TString>(Config_->UserInfoLoginField),
        };

        if (Config_->UserInfoSubjectField) {
            userInfo.Subject = formattedResponose->GetChildValueOrThrow<TString>(*Config_->UserInfoSubjectField);
        }

        return userInfo;
    }

    TError DoCheckUserInfoResponse(const IResponsePtr& rsp, const NYTree::INodePtr& rspNode) const
    {
        if (rsp->GetStatusCode() != EStatusCode::OK) {
            auto error = TError("OAuth response has non-ok status code: %v", static_cast<int>(rsp->GetStatusCode()));

            if (rspNode->GetType() == ENodeType::Map && Config_->UserInfoErrorField) {
                auto errorNode = rspNode->AsMap()->FindChild(*Config_->UserInfoErrorField);
                error = error
                    << TErrorAttribute("error_field_message", ConvertToYsonString(errorNode))
                    << TErrorAttribute("error_field", *Config_->UserInfoErrorField);
            }

            return error;
        }

        if (rspNode->GetType() != ENodeType::Map) {
            return TError("OAuth response content has unexpected node type")
                << TErrorAttribute("expected_result_type", ENodeType::Map)
                << TErrorAttribute("actual_result_type", rspNode->GetType());
        }

        auto loginNode = rspNode->AsMap()->FindChild(Config_->UserInfoLoginField);
        if (!loginNode || loginNode->GetType() != ENodeType::String) {
            return TError("OAuth response content has no login field or login node type is unexpected")
                << TErrorAttribute("login_field", Config_->UserInfoLoginField);
        }

        if (Config_->UserInfoSubjectField) {
            auto subjectNode = rspNode->AsMap()->FindChild(*Config_->UserInfoSubjectField);
            if (!subjectNode || subjectNode->GetType() != ENodeType::String) {
                return TError("OAuth response content has no subject field or subject node type is unexpected")
                    << TErrorAttribute("subject_field", Config_->UserInfoSubjectField);
            }
        }

        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

IOAuthServicePtr CreateOAuthService(
    TOAuthServiceConfigPtr config,
    NConcurrency::IPollerPtr poller,
    NProfiling::TProfiler profiler)
{
    return New<TOAuthService>(
        std::move(config),
        std::move(poller),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
