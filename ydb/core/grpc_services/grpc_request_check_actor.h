#pragma once
#include "defs.h"
#include "audit_log.h"
#include "service_ratelimiter_events.h"
#include "local_rate_limiter.h"
#include "operation_helpers.h"
#include "rpc_calls.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/grpc_services/counters/proxy_counters.h>
#include <ydb/core/security/secure_request.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <util/string/split.h>

namespace NKikimr {
namespace NGRpcService {

template <typename TEvent>
class TGrpcRequestCheckActor
    : public TActorBootstrappedSecureRequest<TGrpcRequestCheckActor<TEvent>>
    , public ICheckerIface
{
    using TSelf = TGrpcRequestCheckActor<TEvent>;
    using TBase = TActorBootstrappedSecureRequest<TGrpcRequestCheckActor>;
public:
    void OnAccessDenied(const TEvTicketParser::TError& error, const TActorContext& ctx) {
        LOG_INFO(ctx, NKikimrServices::GRPC_SERVER, error.ToString());
        if (error.Retryable) {
            GrpcRequestBaseCtx_->UpdateAuthState(NGrpc::TAuthState::AS_UNAVAILABLE);
        } else {
            GrpcRequestBaseCtx_->UpdateAuthState(NGrpc::TAuthState::AS_FAIL);
        }
        GrpcRequestBaseCtx_->RaiseIssue(NYql::TIssue{error.Message});
        ReplyBackAndDie();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ_AUTH;
    }

    static const TVector<TString>& GetPermissions();

    void InitializeAttributesFromSchema(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
        CheckedDatabaseName_ = CanonizePath(schemeData.GetPath());
        if (!GrpcRequestBaseCtx_->TryCustomAttributeProcess(schemeData, this)) {
            ProcessCommonAttributes(schemeData);
        }
    }

    void ProcessCommonAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
        static std::vector<TString> allowedAttributes = {"folder_id", "service_account_id", "database_id"};
        TVector<std::pair<TString, TString>> attributes;
        attributes.reserve(schemeData.GetPathDescription().UserAttributesSize());
        for (const auto& attr : schemeData.GetPathDescription().GetUserAttributes()) {
            if (std::find(allowedAttributes.begin(), allowedAttributes.end(), attr.GetKey()) != allowedAttributes.end()) {
                attributes.emplace_back(attr.GetKey(), attr.GetValue());
            }
        }
        if (!attributes.empty()) {
            SetEntries({{GetPermissions(), attributes}});
        }
    }

    void SetEntries(const TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry>& entries) {
        TBase::SetEntries(entries);
    }

    void InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData);

    TGrpcRequestCheckActor(
        const TActorId& owner,
        const TSchemeBoardEvents::TDescribeSchemeResult& schemeData,
        TIntrusivePtr<TSecurityObject> securityObject,
        TAutoPtr<TEventHandle<TEvent>> request,
        IGRpcProxyCounters::TPtr counters,
        bool skipCheckConnectRigths)
        : Owner_(owner)
        , Request_(std::move(request))
        , Counters_(counters)
        , SecurityObject_(std::move(securityObject))
        , SkipCheckConnectRigths_(skipCheckConnectRigths)
    {
        GrpcRequestBaseCtx_ = Request_->Get();
        TMaybe<TString> authToken = GrpcRequestBaseCtx_->GetYdbToken();
        if (authToken) {
            TString peerName = GrpcRequestBaseCtx_->GetPeerName();
            TBase::SetSecurityToken(authToken.GetRef());
            TBase::SetPeerName(peerName);
            InitializeAttributes(schemeData);
            TBase::SetDatabase(CheckedDatabaseName_);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        TBase::Become(&TSelf::DbAccessStateFunc);

        if (AppData()->FeatureFlags.GetEnableDbCounters()) {
            Counters_ = WrapGRpcProxyDbCounters(Counters_);
        }

        GrpcRequestBaseCtx_->SetCounters(Counters_);

        if (!CheckedDatabaseName_.empty()) {
            GrpcRequestBaseCtx_->UseDatabase(CheckedDatabaseName_);
            Counters_->UseDatabase(CheckedDatabaseName_);
        }

        {
            auto [error, issue] = CheckConnectRight();
            if (error) {
                ReplyUnauthorizedAndDie(*issue);
                return;
            }
        }

        if (AppData(ctx)->FeatureFlags.GetEnableGrpcAudit()) {
            AuditLog(GrpcRequestBaseCtx_, CheckedDatabaseName_, GetSubject(), ctx);
        }

        // Simple rps limitation
        static NRpcService::TRlConfig rpsRlConfig(
            "serverless_rt_coordination_node_path",
            "serverless_rt_base_resource_rps",
                {
                    NRpcService::TRlConfig::TOnReqAction {
                        1
                    }
                }
            );

        // Limitation RU for unary calls in time of response
        static NRpcService::TRlConfig ruRlConfig(
            "serverless_rt_coordination_node_path",
            "serverless_rt_base_resource_ru",
                {
                    NRpcService::TRlConfig::TOnReqAction {
                        1
                    },
                    NRpcService::TRlConfig::TOnRespAction {
                    }
                }
            );

        // Limitation ru for calls with internall rl support (read table)
        static NRpcService::TRlConfig ruRlProgressConfig(
            "serverless_rt_coordination_node_path",
            "serverless_rt_base_resource_ru",
                {
                    NRpcService::TRlConfig::TOnReqAction {
                        1
                    }
                }
            );

        // Just set RlPath
        static NRpcService::TRlConfig ruRlManualConfig(
            "serverless_rt_coordination_node_path",
            "serverless_rt_base_resource_ru",
                {
                    // no actions
                }
            );

        auto rlMode = Request_->Get()->GetRlMode();
        switch (rlMode) {
            case TRateLimiterMode::Rps:
                RlConfig = &rpsRlConfig;
                break;
            case TRateLimiterMode::Ru:
                RlConfig = &ruRlConfig;
                break;
            case TRateLimiterMode::RuOnProgress:
                RlConfig = &ruRlProgressConfig;
                break;
            case TRateLimiterMode::RuManual:
                RlConfig = &ruRlManualConfig;
                break;
            case TRateLimiterMode::Off:
                break;
        }

        if (!RlConfig) {
            // No rate limit config for this request
            return SetTokenAndDie();
        } else {
            THashMap<TString, TString> attributes;
            for (const auto& [attrName, attrValue] : Attributes_) {
                attributes[attrName] = attrValue;
            }
            return ProcessRateLimit(attributes, ctx);
        }
    }

    void SetTokenAndDie() {
        if (GrpcRequestBaseCtx_->IsClientLost()) {
            LOG_DEBUG(*TlsActivationContext, NKikimrServices::GRPC_SERVER,
                "Client was disconnected before processing request (check actor)");
            const NYql::TIssues issues;
            ReplyUnavailableAndDie(issues);
        } else {
            GrpcRequestBaseCtx_->UpdateAuthState(NGrpc::TAuthState::AS_OK);
            GrpcRequestBaseCtx_->SetInternalToken(TBase::GetParsedToken());
            ReplyBackAndDie();
        }
    }

    STATEFN(DbAccessStateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPoisonPill, HandlePoison);
        }
    }

    void HandlePoison(TEvents::TEvPoisonPill::TPtr&) {
        TBase::PassAway();
    }

private:
    TString GetSubject() const {
        const auto sid = TBase::GetUserSID();
        return sid ? sid : "no subject";
    }

    static NYql::TIssues GetRlIssues(const Ydb::RateLimiter::AcquireResourceResponse& resp) {
        NYql::TIssues opIssues;
        NYql::IssuesFromMessage(resp.operation().issues(), opIssues);
        return opIssues;
    }

    void ProcessOnRequest(Ydb::RateLimiter::AcquireResourceRequest&& req, const TActorContext& ctx) {
        auto time = TInstant::Now();
        auto cb = [this, time](Ydb::RateLimiter::AcquireResourceResponse resp) {
            TDuration delay = TInstant::Now() - time;
            switch (resp.operation().status()) {
                case Ydb::StatusIds::SUCCESS:
                    Counters_->ReportThrottleDelay(delay);
                    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "Request delayed for " << delay << " by ratelimiter");
                    SetTokenAndDie();
                    break;
                case Ydb::StatusIds::TIMEOUT:
                    Counters_->IncDatabaseRateLimitedCounter();
                    LOG_INFO(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "Throughput limit exceeded");
                    ReplyOverloadedAndDie(MakeIssue(NKikimrIssues::TIssuesIds::YDB_RESOURCE_USAGE_LIMITED, "Throughput limit exceeded"));
                    break;
                default:
                    {
                        auto issues = GetRlIssues(resp);
                        const TString error = Sprintf("RateLimiter status: %d database: %s, issues: %s",
                                              resp.operation().status(),
                                              CheckedDatabaseName_.c_str(),
                                              issues.ToString().c_str());
                        LOG_ERROR(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "%s", error.c_str());

                        ReplyUnavailableAndDie(issues); // same as cloud-go serverless proxy
                    }
                    break;
            }
        };

        req.mutable_operation_params()->mutable_operation_timeout()->set_nanos(200000000); // same as cloud-go serverless proxy

        NKikimr::NRpcService::RateLimiterAcquireUseSameMailbox(
            std::move(req),
            CheckedDatabaseName_,
            TBase::GetSerializedToken(),
            std::move(cb),
            ctx);
    }

    TRespHook CreateRlRespHook(Ydb::RateLimiter::AcquireResourceRequest&& req) {
        const auto& databasename = CheckedDatabaseName_;
        auto token = TBase::GetSerializedToken();
        auto counters = Counters_;
        return [req{std::move(req)}, databasename, token, counters](TRespHookCtx::TPtr ctx) mutable {

            LOG_DEBUG(*TlsActivationContext, NKikimrServices::GRPC_SERVER,
                "Response hook called to report RU usage, database: %s, request: %s, consumed: %d",
                databasename.c_str(), ctx->GetRequestName().c_str(), ctx->GetConsumedRu());

            counters->AddConsumedRequestUnits(ctx->GetConsumedRu());

            if (ctx->GetConsumedRu() >= 1) {
                // We already count '1' on start request
                req.set_used(ctx->GetConsumedRu() - 1);

                // No need to handle result of rate limiter response on the response hook
                // just report ru usage
                auto noop = [](Ydb::RateLimiter::AcquireResourceResponse) {};
                NKikimr::NRpcService::RateLimiterAcquireUseSameMailbox(
                    std::move(req),
                    databasename,
                    token,
                    std::move(noop),
                    TActivationContext::AsActorContext());
            }

            ctx->Pass();
        };
    }

    void ProcessRateLimit(const THashMap<TString, TString>& attributes, const TActorContext& ctx) {
        // Match rate limit config and database attributes
        auto rlPath = NRpcService::Match(*RlConfig, attributes);
        if (!rlPath) {
            return SetTokenAndDie();
        } else {
            auto actions = NRpcService::MakeRequests(*RlConfig, rlPath.GetRef());
            GrpcRequestBaseCtx_->SetRlPath(std::move(rlPath));

            Ydb::RateLimiter::AcquireResourceRequest req;
            bool hasOnReqAction = false;
            for (auto& action : actions) {
                switch (action.first) {
                case NRpcService::Actions::OnReq:
                    req = std::move(action.second);
                    hasOnReqAction = true;
                    break;
                case NRpcService::Actions::OnResp:
                    GrpcRequestBaseCtx_->SetRespHook(CreateRlRespHook(std::move(action.second)));
                    break;
                }
            }

            if (hasOnReqAction) {
                return ProcessOnRequest(std::move(req), ctx);
            } else {
                return SetTokenAndDie();
            }
        }
    }

private:
    void ReplyUnauthorizedAndDie(const NYql::TIssue& issue) {
        GrpcRequestBaseCtx_->RaiseIssue(issue);
        GrpcRequestBaseCtx_->ReplyWithYdbStatus(Ydb::StatusIds::UNAUTHORIZED);
        TBase::PassAway();
    }

    void ReplyUnavailableAndDie(const NYql::TIssue& issue) {
        GrpcRequestBaseCtx_->RaiseIssue(issue);
        GrpcRequestBaseCtx_->ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
        TBase::PassAway();
    }

    void ReplyUnavailableAndDie(const NYql::TIssues& issue) {
        GrpcRequestBaseCtx_->RaiseIssues(issue);
        GrpcRequestBaseCtx_->ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
        TBase::PassAway();
    }

    void ReplyUnauthenticatedAndDie() {
        GrpcRequestBaseCtx_->ReplyUnauthenticated("Unknown database");
        TBase::PassAway();
    }

    void ReplyOverloadedAndDie(const NYql::TIssue& issue) {
        GrpcRequestBaseCtx_->RaiseIssue(issue);
        GrpcRequestBaseCtx_->ReplyWithYdbStatus(Ydb::StatusIds::OVERLOADED);
        TBase::PassAway();
    }

    void ReplyBackAndDie() {
        TlsActivationContext->Send(Request_->Forward(Owner_));
        TBase::PassAway();
    }

    std::pair<bool, std::optional<NYql::TIssue>> CheckConnectRight() {
        if (SkipCheckConnectRigths_) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY_NO_CONNECT_ACCESS,
                        "Skip check permission connect db, AllowYdbRequestsWithoutDatabase is off, there is no db provided from user"
                        << ", database: " << CheckedDatabaseName_
                        << ", user: " << TBase::GetUserSID()
                        << ", from ip: " << GrpcRequestBaseCtx_->GetPeerName());
            return {false, std::nullopt};
        }

        if (TBase::IsUserAdmin()) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY_NO_CONNECT_ACCESS,
                        "Skip check permission connect db, user is a admin"
                        << ", database: " << CheckedDatabaseName_
                        << ", user: " << TBase::GetUserSID()
                        << ", from ip: " << GrpcRequestBaseCtx_->GetPeerName());
            return {false, std::nullopt};
        }

        if (!TBase::GetSecurityToken()) {
            if (!TBase::IsTokenRequired()) {
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY_NO_CONNECT_ACCESS,
                            "Skip check permission connect db, token is not required, there is no token provided"
                            << ", database: " << CheckedDatabaseName_
                            << ", user: " << TBase::GetUserSID()
                            << ", from ip: " << GrpcRequestBaseCtx_->GetPeerName());
                return {false, std::nullopt};
            }
        }

        if (!SecurityObject_) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY_NO_CONNECT_ACCESS,
                        "Skip check permission connect db, no SecurityObject_"
                        << ", database: " << CheckedDatabaseName_
                        << ", user: " << TBase::GetUserSID()
                        << ", from ip: " << GrpcRequestBaseCtx_->GetPeerName());
            return {false, std::nullopt};
        }

        const ui32 access = NACLib::ConnectDatabase;
        const auto& parsedToken = TBase::GetParsedToken();
        if (parsedToken && SecurityObject_->CheckAccess(access, *parsedToken)) {
            return {false, std::nullopt};
        }

        const TString error = TStringBuilder()
            << "User has no permission to perform query on this database"
            << ", database: " << CheckedDatabaseName_
            << ", user: " << TBase::GetUserSID()
            << ", from ip: " << GrpcRequestBaseCtx_->GetPeerName();
        LOG_INFO(*TlsActivationContext, NKikimrServices::GRPC_PROXY_NO_CONNECT_ACCESS, "%s", error.c_str());

        Counters_->IncDatabaseAccessDenyCounter();

        if (!AppData()->FeatureFlags.GetCheckDatabaseAccessPermission()) {
            return {false, std::nullopt};
        }

        LOG_INFO(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "%s", error.c_str());
        return {true, MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error)};
    }

    const TActorId Owner_;
    TAutoPtr<TEventHandle<TEvent>> Request_;
    IGRpcProxyCounters::TPtr Counters_;
    TIntrusivePtr<TSecurityObject> SecurityObject_;
    TString CheckedDatabaseName_;
    IRequestProxyCtx* GrpcRequestBaseCtx_;
    NRpcService::TRlConfig* RlConfig = nullptr;
    bool SkipCheckConnectRigths_ = false;
    std::vector<std::pair<TString, TString>> Attributes_;
};

// default behavior - attributes in schema
template <typename TEvent>
void TGrpcRequestCheckActor<TEvent>::InitializeAttributes(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData) {
    for (const auto& attr : schemeData.GetPathDescription().GetUserAttributes()) {
        Attributes_.emplace_back(std::make_pair(attr.GetKey(), attr.GetValue()));
    }
    InitializeAttributesFromSchema(schemeData);
}

// default permissions
template <typename TEvent>
const TVector<TString>& TGrpcRequestCheckActor<TEvent>::GetPermissions() {
    static const TVector<TString> permissions = {
                "ydb.databases.list",
                "ydb.databases.create",
                "ydb.databases.connect"
            };
    return permissions;
}

template <typename TEvent>
IActor* CreateGrpcRequestCheckActor(
    const TActorId& owner,
    const TSchemeBoardEvents::TDescribeSchemeResult& schemeData,
    TIntrusivePtr<TSecurityObject> securityObject,
    TAutoPtr<TEventHandle<TEvent>> request,
    IGRpcProxyCounters::TPtr counters,
    bool skipCheckConnectRigths) {

    return new TGrpcRequestCheckActor<TEvent>(owner, schemeData, std::move(securityObject), std::move(request), counters, skipCheckConnectRigths);
}

}
}
