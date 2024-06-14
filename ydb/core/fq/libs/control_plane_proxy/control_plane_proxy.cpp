#include "config.h"
#include "control_plane_proxy.h"
#include "probes.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/compute/ydb/control_plane/compute_database_control_plane_service.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/control_plane_config/control_plane_config.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/request_validators.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/quota_manager/quota_manager.h>
#include <ydb/core/fq/libs/rate_limiter/events/control_plane_events.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>
#include <ydb/core/fq/libs/test_connection/events/events.h>
#include <ydb/core/fq/libs/test_connection/test_connection.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/control_plane_proxy/actors/control_plane_storage_requester_actor.h>
#include <ydb/core/fq/libs/control_plane_proxy/actors/request_actor.h>
#include <ydb/core/fq/libs/control_plane_proxy/actors/utils.h>
#include <ydb/core/fq/libs/control_plane_proxy/actors/ydb_schema_query_actor.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/control_plane_proxy/utils/utils.h>
#include <ydb/public/lib/fq/scope.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/ycloud/impl/access_service.h>
#include <ydb/library/ycloud/impl/mock_access_service.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <ydb/library/security/util.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/retry/retry_policy.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>

#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/folder_service/events.h>

#include <contrib/libs/fmt/include/fmt/format.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/string/ascii.h>
#include <util/string/join.h>
#include <util/string/strip.h>

namespace NFq {
namespace {

using namespace NActors;
using namespace ::NFq::NConfig;
using namespace NKikimr;
using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace ::NFq::NPrivate;

LWTRACE_USING(YQ_CONTROL_PLANE_PROXY_PROVIDER);

template<class TEventRequest, class TResponseProxy>
class TGetQuotaActor : public NActors::TActorBootstrapped<TGetQuotaActor<TEventRequest, TResponseProxy>> {
    using TBase = NActors::TActorBootstrapped<TGetQuotaActor<TEventRequest, TResponseProxy>>;
    using TBase::SelfId;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Become;

    TActorId Sender;
    TEventRequest Event;
    ui32 Cookie;

public:
    TGetQuotaActor(TActorId sender, TEventRequest event, ui32 cookie)
        : Sender(sender)
        , Event(event)
        , Cookie(cookie)
    {}

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_GET_QUOTA";

    void Bootstrap() {
        CPP_LOG_T("Get quotas bootstrap. Cloud id: " << Event->Get()->CloudId << " Actor id: " << SelfId());
        Become(&TGetQuotaActor::StateFunc, TDuration::Seconds(10), new NActors::TEvents::TEvWakeup());
        Send(MakeQuotaServiceActorId(SelfId().NodeId()), new TEvQuotaService::TQuotaGetRequest(SUBJECT_TYPE_CLOUD, Event->Get()->CloudId));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvQuotaService::TQuotaGetResponse, Handle);
        cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
    )

    void Handle(TEvQuotaService::TQuotaGetResponse::TPtr& ev) {
        Event->Get()->Quotas = std::move(ev->Get()->Quotas);
        CPP_LOG_T("Cloud id: " << Event->Get()->CloudId << " Quota count: " << (Event->Get()->Quotas ? TMaybe<size_t>(Event->Get()->Quotas->size()) : Nothing()));
        TActivationContext::Send(Event->Forward(ControlPlaneProxyActorId()));
        PassAway();
    }

    void HandleTimeout() {
        CPP_LOG_W("Quota request timeout. Cloud id: " << Event->Get()->CloudId << " Actor id: " << SelfId());
        Send(MakeQuotaServiceActorId(SelfId().NodeId()), new TEvQuotaService::TQuotaGetRequest(SUBJECT_TYPE_CLOUD, Event->Get()->CloudId, true));
    }
};

TString CutBearer(TString token) {
    auto bearer = "Bearer "sv;
    if (!AsciiHasPrefixIgnoreCase(token, bearer)) {
        return token;
    }
    // cut prefix and strip
    token = token.substr(bearer.size());
    StripInPlace(token);
    return token;
}

template<class TEventRequest, class TResponseProxy>
class TResolveSubjectTypeActor : public NActors::TActorBootstrapped<TResolveSubjectTypeActor<TEventRequest, TResponseProxy>> {
    using TBase = NActors::TActorBootstrapped<TResolveSubjectTypeActor<TEventRequest, TResponseProxy>>;
    using TBase::SelfId;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Become;
    using TBase::Register;
    using IRetryPolicy = IRetryPolicy<NCloud::TEvAccessService::TEvAuthenticateResponse::TPtr&>;

    const ::NFq::TControlPlaneProxyConfig Config;
    const TActorId Sender;
    const TRequestCommonCountersPtr Counters;
    const TString Token;
    const std::function<void(const TDuration&, bool, bool)> Probe;
    TEventRequest Event;
    const ui32 Cookie;
    const TInstant StartTime;
    const IRetryPolicy::IRetryState::TPtr RetryState;
    const TActorId AccessService;

public:
    TResolveSubjectTypeActor(const TRequestCommonCountersPtr& counters,
                        TActorId sender, const ::NFq::TControlPlaneProxyConfig& config,
                        const TString& token,
                        const std::function<void(const TDuration&, bool, bool)>& probe,
                        TEventRequest event,
                        ui32 cookie, const TActorId& accessService)
        : Config(config)
        , Sender(sender)
        , Counters(counters)
        , Token(CutBearer(token))
        , Probe(probe)
        , Event(event)
        , Cookie(cookie)
        , StartTime(TInstant::Now())
        , RetryState(GetRetryPolicy()->CreateRetryState())
        , AccessService(accessService)
    {
    }

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_RESOLVE_SUBJECT_TYPE";

    void Bootstrap() {
        CPP_LOG_T("Resolve subject type bootstrap. Token: " << MaskTicket(Token) << " Actor id: " << SelfId());
        Become(&TResolveSubjectTypeActor::StateFunc, Config.RequestTimeout, new NActors::TEvents::TEvWakeup());
        Counters->InFly->Inc();
        Send(AccessService, CreateRequest().release(), 0, 0);
    }

    std::unique_ptr<NCloud::TEvAccessService::TEvAuthenticateRequest> CreateRequest() {
        auto request = std::make_unique<NCloud::TEvAccessService::TEvAuthenticateRequest>();
        request->Request.set_iam_token(Token);
        return request;
    }

    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        hFunc(NCloud::TEvAccessService::TEvAuthenticateResponse, Handle);
    )

    void HandleTimeout() {
        CPP_LOG_W("Resolve subject type timeout. Token: " << MaskTicket(Token) << " Actor id: " << SelfId());
        NYql::TIssues issues;
        NYql::TIssue issue = MakeErrorIssue(TIssuesIds::TIMEOUT, "Request (resolve subject type) timeout. Try repeating the request later");
        issues.AddIssue(issue);
        Counters->Error->Inc();
        Counters->Timeout->Inc();
        const TDuration delta = TInstant::Now() - StartTime;
        Probe(delta, false, true);
        Send(Sender, new TResponseProxy(issues, {}), 0, Cookie);
        PassAway();
    }

    void Handle(NCloud::TEvAccessService::TEvAuthenticateResponse::TPtr& ev) {
        const auto& response = ev->Get()->Response;
        const auto& status = ev->Get()->Status;
        if (!status.Ok() || !response.has_subject()) {
            TString errorMessage = "Msg: " + status.Msg + " Details: " + status.Details + " Code: " + ToString(status.GRpcStatusCode) + " InternalError: " + ToString(status.InternalError);
            auto delay = RetryState->GetNextRetryDelay(ev);
            if (delay) {
                Counters->Retry->Inc();
                CPP_LOG_E("Resolve subject type error. Retry with delay " << *delay << ", " << errorMessage);
                TActivationContext::Schedule(*delay, new IEventHandle(AccessService, static_cast<const TActorId&>(SelfId()), CreateRequest().release()));
                return;
            }
            const TDuration delta = TInstant::Now() - StartTime;
            Counters->InFly->Dec();
            Counters->LatencyMs->Collect((delta).MilliSeconds());
            Counters->Error->Inc();
            CPP_LOG_E(errorMessage);
            NYql::TIssues issues;
            NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, "Resolve subject type error");
            issues.AddIssue(issue);
            Probe(delta, false, false);
            Send(Sender, new TResponseProxy(issues, {}), 0, Cookie);
            PassAway();
            return;
        }

        Counters->InFly->Dec();
        Counters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        Counters->Ok->Inc();
        TString subjectType = GetSubjectType(response.subject());
        Event->Get()->SubjectType = subjectType;
        CPP_LOG_T("Subject Type: " << subjectType << " Token: " << MaskTicket(Token));

        TActivationContext::Send(Event->Forward(ControlPlaneProxyActorId()));
        PassAway();
    }

private:
    static TString GetSubjectType(const yandex::cloud::priv::servicecontrol::v1::Subject& subject) {
        switch (subject.type_case()) {
            case yandex::cloud::priv::servicecontrol::v1::Subject::TYPE_NOT_SET:
            case yandex::cloud::priv::servicecontrol::v1::Subject::kAnonymousAccount:
                return "unknown";
            case yandex::cloud::priv::servicecontrol::v1::Subject::kUserAccount:
                return subject.user_account().federation_id() ? "federated_account" : "user_account";
            case yandex::cloud::priv::servicecontrol::v1::Subject::kServiceAccount:
                return "service_account";
        }
    }

    static const IRetryPolicy::TPtr& GetRetryPolicy() {
        static IRetryPolicy::TPtr policy = IRetryPolicy::GetExponentialBackoffPolicy([](NCloud::TEvAccessService::TEvAuthenticateResponse::TPtr& ev) {
            const auto& response = ev->Get()->Response;
            const auto& status = ev->Get()->Status;
            return !status.Ok() || !response.has_subject() ? ERetryErrorClass::ShortRetry : ERetryErrorClass::NoRetry;
        }, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(30), 5);
        return policy;
    }
};

template<class TEventRequest, class TResponseProxy>
class TResolveFolderActor : public NActors::TActorBootstrapped<TResolveFolderActor<TEventRequest, TResponseProxy>> {
    using TBase = NActors::TActorBootstrapped<TResolveFolderActor<TEventRequest, TResponseProxy>>;
    using TBase::SelfId;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Become;
    using TBase::Register;
    using IRetryPolicy = IRetryPolicy<NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse::TPtr&>;

    ::NFq::TControlPlaneProxyConfig Config;
    TActorId Sender;
    TRequestCommonCountersPtr Counters;
    TString FolderId;
    TString Token;
    std::function<void(const TDuration&, bool, bool)> Probe;
    TEventRequest Event;
    ui32 Cookie;
    TInstant StartTime;
    const bool QuotaManagerEnabled;
    IRetryPolicy::IRetryState::TPtr RetryState;


public:
    TResolveFolderActor(const TRequestCommonCountersPtr& counters,
                        TActorId sender, const ::NFq::TControlPlaneProxyConfig& config,
                        const TString& scope, const TString& token,
                        const std::function<void(const TDuration&, bool, bool)>& probe,
                        TEventRequest event,
                        ui32 cookie, bool quotaManagerEnabled)
        : Config(config)
        , Sender(sender)
        , Counters(counters)
        , FolderId(NYdb::NFq::TScope(scope).ParseFolder())
        , Token(token)
        , Probe(probe)
        , Event(event)
        , Cookie(cookie)
        , StartTime(TInstant::Now())
        , QuotaManagerEnabled(quotaManagerEnabled)
        , RetryState(GetRetryPolicy()->CreateRetryState())
    {}

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_RESOLVE_FOLDER";

    void Bootstrap() {
        CPP_LOG_T("Resolve folder bootstrap. Folder id: " << FolderId << " Actor id: " << SelfId());
        Become(&TResolveFolderActor::StateFunc, Config.RequestTimeout, new NActors::TEvents::TEvWakeup());
        Counters->InFly->Inc();
        Send(NKikimr::NFolderService::FolderServiceActorId(), CreateRequest().release(), 0, 0);
    }

    std::unique_ptr<NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderRequest> CreateRequest() {
        auto request = std::make_unique<NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderRequest>();
        request->FolderId = FolderId;
        request->Token = Token;
        return request;
    }

    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        hFunc(NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse, Handle);
    )

    void HandleTimeout() {
        CPP_LOG_W("Resolve folder timeout. Folder id: " << FolderId << " Actor id: " << SelfId());
        NYql::TIssues issues;
        NYql::TIssue issue = MakeErrorIssue(TIssuesIds::TIMEOUT, "Request timeout. Try repeating the request later");
        issues.AddIssue(issue);
        Counters->Error->Inc();
        Counters->Timeout->Inc();
        const TDuration delta = TInstant::Now() - StartTime;
        Probe(delta, false, true);
        Send(Sender, new TResponseProxy(issues, {}), 0, Cookie);
        PassAway();
    }

    void Handle(NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse::TPtr& ev) {

        const auto& status = ev->Get()->Status;
        if (!status.Ok() || ev->Get()->CloudId.empty()) {
            TString errorMessage = "Msg: " + status.Msg + " Details: " + status.Details + " Code: " + ToString(status.GRpcStatusCode) + " InternalError: " + ToString(status.InternalError);
            auto delay = RetryState->GetNextRetryDelay(ev);
            if (delay) {
                Counters->Retry->Inc();
                CPP_LOG_E("Folder resolve error. Retry with delay " << *delay << ", " << errorMessage);
                TActivationContext::Schedule(*delay, new IEventHandle(NKikimr::NFolderService::FolderServiceActorId(), static_cast<const TActorId&>(SelfId()), CreateRequest().release()));
                return;
            }
            const TDuration delta = TInstant::Now() - StartTime;
            Counters->InFly->Dec();
            Counters->LatencyMs->Collect((delta).MilliSeconds());
            Counters->Error->Inc();
            CPP_LOG_E(errorMessage);
            NYql::TIssues issues;
            NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, "Resolve folder error");
            issues.AddIssue(issue);
            Probe(delta, false, false);
            Send(Sender, new TResponseProxy(issues, {}), 0, Cookie);
            PassAway();
            return;
        }

        Counters->InFly->Dec();
        Counters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        Counters->Ok->Inc();
        TString cloudId = ev->Get()->CloudId;
        Event->Get()->CloudId = cloudId;
        CPP_LOG_T("Cloud id: " << cloudId << " Folder id: " << FolderId);

        if (QuotaManagerEnabled) {
            Register(new TGetQuotaActor<TEventRequest, TResponseProxy>(Sender, Event, Cookie));
        } else {
            TActivationContext::Send(Event->Forward(ControlPlaneProxyActorId()));
        }
        PassAway();
    }

private:
    static const IRetryPolicy::TPtr& GetRetryPolicy() {
        static IRetryPolicy::TPtr policy = IRetryPolicy::GetExponentialBackoffPolicy([](NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse::TPtr& ev) {
            const auto& status = ev->Get()->Status;
            return !status.Ok() || ev->Get()->CloudId.empty() ? ERetryErrorClass::ShortRetry : ERetryErrorClass::NoRetry;
        }, TDuration::MilliSeconds(10), TDuration::MilliSeconds(200), TDuration::Seconds(30), 5);
        return policy;
    }
};

template<class TEventRequest, class TResponseProxy>
class TCreateComputeDatabaseActor : public NActors::TActorBootstrapped<TCreateComputeDatabaseActor<TEventRequest, TResponseProxy>> {
    using TBase = NActors::TActorBootstrapped<TCreateComputeDatabaseActor<TEventRequest, TResponseProxy>>;
    using TBase::SelfId;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Become;
    using TBase::Register;

    ::NFq::TControlPlaneProxyConfig Config;
    ::NFq::TComputeConfig ComputeConfig;
    TActorId Sender;
    TRequestCommonCountersPtr Counters;
    TString CloudId;
    TString Scope;
    TString Token;
    std::function<void(const TDuration&, bool, bool)> Probe;
    TEventRequest Event;
    ui32 Cookie;
    TInstant StartTime;

public:
    TCreateComputeDatabaseActor(const TRequestCommonCountersPtr& counters,
                                TActorId sender,
                                const ::NFq::TControlPlaneProxyConfig& config,
                                const ::NFq::TComputeConfig& computeConfig,
                                const TString& cloudId,
                                const TString& scope,
                                const std::function<void(const TDuration&, bool, bool)>& probe,
                                TEventRequest event,
                                ui32 cookie)
        : Config(config)
        , ComputeConfig(computeConfig)
        , Sender(sender)
        , Counters(counters)
        , CloudId(cloudId)
        , Scope(scope)
        , Probe(probe)
        , Event(event)
        , Cookie(cookie)
        , StartTime(TInstant::Now()) { }

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_CREATE_DATABASE";

    void Bootstrap() {
        CPP_LOG_T("Create database bootstrap. CloudId: " << CloudId << " Scope: " << Scope << " Actor id: " << SelfId());
        if (!ComputeConfig.YdbComputeControlPlaneEnabled(Scope)) {
            Event->Get()->ComputeDatabase = FederatedQuery::Internal::ComputeDatabaseInternal{};
            TActivationContext::Send(Event->Forward(ControlPlaneProxyActorId()));
            PassAway();
            return;
        }
        Become(&TCreateComputeDatabaseActor::StateFunc, Config.RequestTimeout, new NActors::TEvents::TEvWakeup());
        Counters->InFly->Inc();
        Send(::NFq::ComputeDatabaseControlPlaneServiceActorId(), CreateRequest().release(), 0, 0);
    }

    std::unique_ptr<TEvYdbCompute::TEvCreateDatabaseRequest> CreateRequest() {
        return std::make_unique<TEvYdbCompute::TEvCreateDatabaseRequest>(CloudId, Scope);
    }

    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        hFunc(TEvYdbCompute::TEvCreateDatabaseResponse, Handle);
    )

    void HandleTimeout() {
        CPP_LOG_W("Create database timeout. CloudId: " << CloudId << " Scope: " << Scope << " Actor id: " << SelfId());
        NYql::TIssues issues;
        NYql::TIssue issue = MakeErrorIssue(TIssuesIds::TIMEOUT, "Create database: request timeout. Try repeating the request later");
        issues.AddIssue(issue);
        Counters->Error->Inc();
        Counters->Timeout->Inc();
        const TDuration delta = TInstant::Now() - StartTime;
        Probe(delta, false, true);
        Send(Sender, new TResponseProxy(issues, {}), 0, Cookie);
        PassAway();
    }

    void Handle(TEvYdbCompute::TEvCreateDatabaseResponse::TPtr& ev) {
        Counters->InFly->Dec();
        Counters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        if (ev->Get()->Issues) {
            Counters->Error->Inc();
            CPP_LOG_E(ev->Get()->Issues.ToOneLineString());
            const TDuration delta = TInstant::Now() - StartTime;
            Probe(delta, false, false);
            Send(Sender, new TResponseProxy(ev->Get()->Issues, {}), 0, Cookie);
            PassAway();
            return;
        }
        Counters->Ok->Inc();
        Event->Get()->ComputeDatabase = ev->Get()->Result;
        TActivationContext::Send(Event->Forward(ControlPlaneProxyActorId()));
        PassAway();
    }
};



class TControlPlaneProxyActor : public NActors::TActorBootstrapped<TControlPlaneProxyActor> {
private:
    TCounters Counters;
    const ::NFq::TControlPlaneProxyConfig Config;
    const TYqSharedResources::TPtr YqSharedResources;
    const NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    const bool QuotaManagerEnabled;
    NConfig::TComputeConfig ComputeConfig;
    TActorId AccessService;
    ::NFq::TSigner::TPtr Signer;

public:
    TControlPlaneProxyActor(
        const NConfig::TControlPlaneProxyConfig& config,
        const NConfig::TControlPlaneStorageConfig& storageConfig,
        const NConfig::TComputeConfig& computeConfig,
        const NConfig::TCommonConfig& commonConfig,
        const NYql::TS3GatewayConfig& s3Config,
        const ::NFq::TSigner::TPtr& signer,
        const TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        bool quotaManagerEnabled)
        : Counters(counters)
        , Config(config, storageConfig, computeConfig, commonConfig, s3Config)
        , YqSharedResources(yqSharedResources)
        , CredentialsProviderFactory(credentialsProviderFactory)
        , QuotaManagerEnabled(quotaManagerEnabled)
        , Signer(signer)
        {}

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY";

    void Bootstrap() {
        CPP_LOG_D("Starting yandex query control plane proxy. Actor id: " << SelfId());

        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(YQ_CONTROL_PLANE_PROXY_PROVIDER));

        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            ::NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "yq_control_plane_proxy", "YQ Control Plane Proxy", false,
                TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
        }

        const auto& accessServiceProto = Config.Proto.GetAccessService();
        if (accessServiceProto.GetEnable()) {
            NCloud::TAccessServiceSettings asSettings;
            asSettings.Endpoint = accessServiceProto.GetEndpoint();
            if (accessServiceProto.GetPathToRootCA()) {
                asSettings.CertificateRootCA = TUnbufferedFileInput(accessServiceProto.GetPathToRootCA()).ReadAll();
            }
            AccessService = Register(NCloud::CreateAccessServiceWithCache(asSettings));
        } else {
            AccessService = Register(NCloud::CreateMockAccessServiceWithCache());
        }

        Become(&TControlPlaneProxyActor::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvControlPlaneProxy::TEvCreateQueryRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvListQueriesRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvDescribeQueryRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvGetQueryStatusRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvModifyQueryRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvDeleteQueryRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvControlQueryRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvGetResultDataRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvListJobsRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvDescribeJobRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvCreateConnectionRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvListConnectionsRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvDescribeConnectionRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvModifyConnectionRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvDeleteConnectionRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvTestConnectionRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvCreateBindingRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvListBindingsRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvDescribeBindingRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvModifyBindingRequest, Handle);
        hFunc(TEvControlPlaneProxy::TEvDeleteBindingRequest, Handle);
        hFunc(NMon::TEvHttpInfo, Handle);
    )

    template<typename T>
    NYql::TIssues ValidatePermissions(T& ev, const TVector<TString>& requiredPermissions) {
        NYql::TIssues issues;
        if (!Config.Proto.GetEnablePermissions()) {
            return issues;
        }

        for (const auto& requiredPermission : requiredPermissions) {
            if (!IsIn(ev->Get()->Permissions, requiredPermission)) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED, "No permission " + requiredPermission + " in a given scope " + ev->Get()->Scope));
            }
        }

        return issues;
    }

    template<class TProxyRequest, class TProxyResponse, class TProbe>
    void ValidationFailedHandler(typename TProxyRequest::TPtr ev,
                                 const NYql::TIssues& issues,
                                 TRequestCounters& requestCounters,
                                 const TInstant& startTime,
                                 const TProbe& probe,
                                 const TString& requestName) {
        CPS_LOG_E(requestName << ", validation failed: " << ev->Get()->Scope << " "
                              << ev->Get()->User << " "
                              << NKikimr::MaskTicket(ev->Get()->Token) << " "
                              << ev->Get()->Request.DebugString()
                              << " error: " << issues.ToString());
        Send(ev->Sender, new TProxyResponse(issues, ev->Get()->SubjectType), 0, ev->Cookie);
        requestCounters.IncError();
        TDuration delta = TInstant::Now() - startTime;
        requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
        probe(delta, false, false);
    }

    template<class TProxyRequest, class TProxyResponse, class TProbe>
    bool ValidateNameUniquenessConstraint(typename TProxyRequest::TPtr& ev,
                                          TRequestCounters& requestCounters,
                                          const TInstant& startTime,
                                          const TProbe& probe,
                                          const TString& requestName) {
        bool entityWithSameNameExists = ev->Get()->EntityWithSameNameType.Defined();
        if (entityWithSameNameExists) {
            TString errorMessage;
            switch (*ev->Get()->EntityWithSameNameType) {
                case TEvControlPlaneProxy::EEntityType::Connection:
                    errorMessage =
                        "Connection with the same name already exists. Please choose another name";
                    break;

                case TEvControlPlaneProxy::EEntityType::Binding:
                    errorMessage =
                        "Binding with the same name already exists. Please choose another name";
                    break;
            }

            ValidationFailedHandler<TProxyRequest, TProxyResponse>(
                std::move(ev),
                NYql::TIssues{NYql::TIssue{errorMessage}},
                requestCounters,
                startTime,
                probe,
                requestName);
        }
        return !entityWithSameNameExists;
    }

    void Handle(TEvControlPlaneProxy::TEvCreateQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::CreateQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("CreateQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(CreateQueryRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvCreateQueryRequest::TPtr,
                                             TEvControlPlaneProxy::TEvCreateQueryResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_CREATE_QUERY, RTC_CREATE_QUERY);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.create@as"});
        if (issues) {
            CPS_LOG_E("CreateQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvCreateQueryResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvCreateQueryRequest::TPtr,
                                    TEvControlPlaneProxy::TEvCreateQueryResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        if (!ev->Get()->ComputeDatabase) {
            Register(new TCreateComputeDatabaseActor<TEvControlPlaneProxy::TEvCreateQueryRequest::TPtr,
                                                TEvControlPlaneProxy::TEvCreateQueryResponse>
                                                (Counters.GetCommonCounters(RTC_CREATE_COMPUTE_DATABASE),
                                                 sender, Config, Config.ComputeConfig, cloudId,
                                                 scope, probe, ev, cookie));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::QUERY_INVOKE
            | TPermissions::TPermission::MANAGE_PUBLIC
        };

        Register(new TCreateQueryRequestActor(ev,
                                              Config,
                                              ControlPlaneStorageServiceActorId(),
                                              requestCounters,
                                              probe,
                                              availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvListQueriesRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ListQueriesRequest request = ev->Get()->Request;
        CPP_LOG_T("ListQueriesRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ListQueriesRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvListQueriesRequest::TPtr,
                                             TEvControlPlaneProxy::TEvListQueriesResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_LIST_QUERIES, RTC_LIST_QUERIES);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.get@as"});
        if (issues) {
            CPS_LOG_E("ListQueriesRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvListQueriesResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvListQueriesRequest::TPtr,
                                    TEvControlPlaneProxy::TEvListQueriesResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::ListQueriesRequest,
                                   TEvControlPlaneStorage::TEvListQueriesRequest,
                                   TEvControlPlaneStorage::TEvListQueriesResponse,
                                   TEvControlPlaneProxy::TEvListQueriesRequest,
                                   TEvControlPlaneProxy::TEvListQueriesResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DescribeQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("DescribeQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString queryId = request.query_id();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(DescribeQueryRequest, scope, user, queryId, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvDescribeQueryRequest::TPtr,
                                             TEvControlPlaneProxy::TEvDescribeQueryResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DESCRIBE_QUERY, RTC_DESCRIBE_QUERY);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.get@as"});
        if (issues) {
            CPS_LOG_E("DescribeQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDescribeQueryResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvDescribeQueryRequest::TPtr,
                                    TEvControlPlaneProxy::TEvDescribeQueryResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_AST
            | TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
            | TPermissions::VIEW_QUERY_TEXT
        };

        Register(new TRequestActor<FederatedQuery::DescribeQueryRequest,
                                   TEvControlPlaneStorage::TEvDescribeQueryRequest,
                                   TEvControlPlaneStorage::TEvDescribeQueryResponse,
                                   TEvControlPlaneProxy::TEvDescribeQueryRequest,
                                   TEvControlPlaneProxy::TEvDescribeQueryResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvGetQueryStatusRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::GetQueryStatusRequest request = ev->Get()->Request;
        CPP_LOG_T("GetStatusRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString queryId = request.query_id();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(GetQueryStatusRequest, scope, user, queryId, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvGetQueryStatusRequest::TPtr,
                                             TEvControlPlaneProxy::TEvGetQueryStatusResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_GET_QUERY_STATUS, RTC_GET_QUERY_STATUS);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.getStatus@as"});
        if (issues) {
            CPS_LOG_E("GetQueryStatusRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvGetQueryStatusResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvGetQueryStatusRequest::TPtr,
                                    TEvControlPlaneProxy::TEvGetQueryStatusResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::GetQueryStatusRequest,
                                   TEvControlPlaneStorage::TEvGetQueryStatusRequest,
                                   TEvControlPlaneStorage::TEvGetQueryStatusResponse,
                                   TEvControlPlaneProxy::TEvGetQueryStatusRequest,
                                   TEvControlPlaneProxy::TEvGetQueryStatusResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvModifyQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ModifyQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("ModifyQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString queryId = request.query_id();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ModifyQueryRequest, scope, user, queryId, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvModifyQueryRequest::TPtr,
                                             TEvControlPlaneProxy::TEvModifyQueryResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_MODIFY_QUERY, RTC_MODIFY_QUERY);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.update@as"});
        if (issues) {
            CPS_LOG_E("ModifyQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvModifyQueryResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvModifyQueryRequest::TPtr,
                                    TEvControlPlaneProxy::TEvModifyQueryResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        if (!ev->Get()->ComputeDatabase) {
            Register(new TCreateComputeDatabaseActor<TEvControlPlaneProxy::TEvModifyQueryRequest::TPtr,
                                                TEvControlPlaneProxy::TEvModifyQueryResponse>
                                                (Counters.GetCommonCounters(RTC_CREATE_COMPUTE_DATABASE),
                                                 sender, Config, Config.ComputeConfig, cloudId,
                                                 scope, probe, ev, cookie));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::QUERY_INVOKE
            | TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::ModifyQueryRequest,
                                   TEvControlPlaneStorage::TEvModifyQueryRequest,
                                   TEvControlPlaneStorage::TEvModifyQueryResponse,
                                   TEvControlPlaneProxy::TEvModifyQueryRequest,
                                   TEvControlPlaneProxy::TEvModifyQueryResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvDeleteQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DeleteQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("DeleteQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString queryId = request.query_id();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(DeleteQueryRequest, scope, user, queryId, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvDeleteQueryRequest::TPtr,
                                             TEvControlPlaneProxy::TEvDeleteQueryResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DELETE_QUERY, RTC_DELETE_QUERY);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.delete@as"});
        if (issues) {
            CPS_LOG_E("DeleteQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDeleteQueryResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvDeleteQueryRequest::TPtr,
                                    TEvControlPlaneProxy::TEvDeleteQueryResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::DeleteQueryRequest,
                                   TEvControlPlaneStorage::TEvDeleteQueryRequest,
                                   TEvControlPlaneStorage::TEvDeleteQueryResponse,
                                   TEvControlPlaneProxy::TEvDeleteQueryRequest,
                                   TEvControlPlaneProxy::TEvDeleteQueryResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvControlQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ControlQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("ControlQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString queryId = request.query_id();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ControlQueryRequest, scope, user, queryId, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvControlQueryRequest::TPtr,
                                             TEvControlPlaneProxy::TEvControlQueryResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_CONTROL_QUERY, RTC_CONTROL_QUERY);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.control@as"});
        if (issues) {
            CPS_LOG_E("ControlQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvControlQueryResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvControlQueryRequest::TPtr,
                                    TEvControlPlaneProxy::TEvControlQueryResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::ControlQueryRequest,
                                   TEvControlPlaneStorage::TEvControlQueryRequest,
                                   TEvControlPlaneStorage::TEvControlQueryResponse,
                                   TEvControlPlaneProxy::TEvControlQueryRequest,
                                   TEvControlPlaneProxy::TEvControlQueryResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvGetResultDataRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::GetResultDataRequest request = ev->Get()->Request;
        CPP_LOG_T("GetResultDataRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString queryId = request.query_id();
        const int32_t resultSetIndex = request.result_set_index();
        const int64_t limit = request.limit();
        const int64_t offset = request.offset();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(GetResultDataRequest, scope, user, queryId, resultSetIndex, offset, limit, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvGetResultDataRequest::TPtr,
                                             TEvControlPlaneProxy::TEvGetResultDataResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_GET_RESULT_DATA, RTC_GET_RESULT_DATA);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.getData@as"});
        if (issues) {
            CPS_LOG_E("GetResultDataRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvGetResultDataResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvGetResultDataRequest::TPtr,
                                    TEvControlPlaneProxy::TEvGetResultDataResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::GetResultDataRequest,
                                   TEvControlPlaneStorage::TEvGetResultDataRequest,
                                   TEvControlPlaneStorage::TEvGetResultDataResponse,
                                   TEvControlPlaneProxy::TEvGetResultDataRequest,
                                   TEvControlPlaneProxy::TEvGetResultDataResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvListJobsRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ListJobsRequest request = ev->Get()->Request;
        CPP_LOG_T("ListJobsRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString queryId = request.query_id();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ListJobsRequest, scope, user, queryId, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvListJobsRequest::TPtr,
                                             TEvControlPlaneProxy::TEvListJobsResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_LIST_JOBS, RTC_LIST_JOBS);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.jobs.get@as"});
        if (issues) {
            CPS_LOG_E("ListJobsRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvListJobsResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvListJobsRequest::TPtr,
                                    TEvControlPlaneProxy::TEvListJobsResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::ListJobsRequest,
                                   TEvControlPlaneStorage::TEvListJobsRequest,
                                   TEvControlPlaneStorage::TEvListJobsResponse,
                                   TEvControlPlaneProxy::TEvListJobsRequest,
                                   TEvControlPlaneProxy::TEvListJobsResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeJobRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DescribeJobRequest request = ev->Get()->Request;
        CPP_LOG_T("DescribeJobRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString jobId = request.job_id();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(DescribeJobRequest, scope, user, jobId, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvDescribeJobRequest::TPtr,
                                             TEvControlPlaneProxy::TEvDescribeJobResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DESCRIBE_JOB, RTC_DESCRIBE_JOB);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.jobs.get@as"});
        if (issues) {
            CPS_LOG_E("DescribeJobRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDescribeJobResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvDescribeJobRequest::TPtr,
                                    TEvControlPlaneProxy::TEvDescribeJobResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
            | TPermissions::TPermission::VIEW_AST
            | TPermissions::VIEW_QUERY_TEXT
        };

        Register(new TRequestActor<FederatedQuery::DescribeJobRequest,
                                   TEvControlPlaneStorage::TEvDescribeJobRequest,
                                   TEvControlPlaneStorage::TEvDescribeJobResponse,
                                   TEvControlPlaneProxy::TEvDescribeJobRequest,
                                   TEvControlPlaneProxy::TEvDescribeJobResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::CreateConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("CreateConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(CreateConnectionRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr,
                                             TEvControlPlaneProxy::TEvCreateConnectionResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_CREATE_CONNECTION, RTC_CREATE_CONNECTION);
        TVector<TString> requiredPermissions = {"yq.connections.create@as"};
        if (ExtractServiceAccountId(request)) {
            requiredPermissions.push_back("iam.serviceAccounts.use@as");
        }

        NYql::TIssues issues = ValidatePermissions(ev, requiredPermissions);
        if (issues) {
            CPS_LOG_E("CreateConnectionRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvCreateConnectionResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr,
                                    TEvControlPlaneProxy::TEvCreateConnectionResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        if (!ev->Get()->ComputeDatabase) {
            Register(new TCreateComputeDatabaseActor<TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr,
                                                TEvControlPlaneProxy::TEvCreateConnectionResponse>
                                                (Counters.GetCommonCounters(RTC_CREATE_COMPUTE_DATABASE),
                                                 sender, Config, Config.ComputeConfig, cloudId,
                                                 scope, probe, ev, cookie));
            return;
        }

        const auto isYDBOperationEnabled =
            Config.ComputeConfig.IsYDBSchemaOperationsEnabled(
                ev->Get()->Scope, ev->Get()->Request.content().setting().connection_case());
        if (isYDBOperationEnabled && !ev->Get()->RequestValidationPassed) {
            auto requestValidationIssues =
                ::NFq::ValidateConnection(ev,
                                          Config.StorageConfig.Proto.GetMaxRequestSize(),
                                          Config.StorageConfig.AvailableConnections,
                                          Config.StorageConfig.Proto.GetDisableCurrentIam(),
                                          false);
            if (requestValidationIssues) {
                CPS_LOG_E("CreateConnectionRequest, validation failed: "
                          << scope << " " << user << " " << NKikimr::MaskTicket(token)
                          << " " << request.DebugString()
                          << " error: " << requestValidationIssues.ToString());
                Send(ev->Sender,
                     new TEvControlPlaneProxy::TEvCreateConnectionResponse(
                         requestValidationIssues, subjectType),
                     0,
                     ev->Cookie);
                requestCounters.IncError();
                TDuration delta = TInstant::Now() - startTime;
                requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
                probe(delta, false, false);
                return;
            }
            ev->Get()->RequestValidationPassed = true;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::VIEW_PUBLIC
        };

        if (isYDBOperationEnabled) {
            if (!ev->Get()->ConnectionsWithSameNameWereListed) {
                Register(MakeListConnectionIdsActor(ControlPlaneProxyActorId(),
                                                    ev,
                                                    Counters,
                                                    Config.RequestTimeout,
                                                    availablePermissions));
                return;
            }
            if (!ev->Get()->BindingWithSameNameWereListed) {
                Register(MakeListBindingIdsActor(ControlPlaneProxyActorId(),
                                                 ev,
                                                 Counters,
                                                 Config.RequestTimeout,
                                                 availablePermissions));
                return;
            }
            if (!ValidateNameUniquenessConstraint<
                    TEvControlPlaneProxy::TEvCreateConnectionRequest,
                    TEvControlPlaneProxy::TEvCreateConnectionResponse>(
                    ev, requestCounters, startTime, probe, "TEvCreateConnectionRequest")) {
                return;
            }
        }

        if (isYDBOperationEnabled) {
            if (!ev->Get()->YDBClient) {
                ev->Get()->YDBClient = CreateNewTableClient(ev,
                                                            Config.ComputeConfig,
                                                            YqSharedResources,
                                                            CredentialsProviderFactory);
            }

            if (!ev->Get()->ComputeYDBOperationWasPerformed) {
                Register(NPrivate::MakeCreateConnectionActor(ControlPlaneProxyActorId(),
                                                             std::move(ev),
                                                             Config.RequestTimeout,
                                                             Counters,
                                                             availablePermissions,
                                                             Config.CommonConfig,
                                                             Config.ComputeConfig,
                                                             Signer));
                return;
            }
        }

        Register(new TRequestActor<FederatedQuery::CreateConnectionRequest,
                                   TEvControlPlaneStorage::TEvCreateConnectionRequest,
                                   TEvControlPlaneStorage::TEvCreateConnectionResponse,
                                   TEvControlPlaneProxy::TEvCreateConnectionRequest,
                                   TEvControlPlaneProxy::TEvCreateConnectionResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvListConnectionsRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ListConnectionsRequest request = ev->Get()->Request;
        CPP_LOG_T("ListConnectionsRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ListConnectionsRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvListConnectionsRequest::TPtr,
                                             TEvControlPlaneProxy::TEvListConnectionsResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_LIST_CONNECTIONS, RTC_LIST_CONNECTIONS);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.connections.get@as"});
        if (issues) {
            CPS_LOG_E("ListConnectionsRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvListConnectionsResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvListConnectionsRequest::TPtr,
                                    TEvControlPlaneProxy::TEvListConnectionsResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::ListConnectionsRequest,
                                   TEvControlPlaneStorage::TEvListConnectionsRequest,
                                   TEvControlPlaneStorage::TEvListConnectionsResponse,
                                   TEvControlPlaneProxy::TEvListConnectionsRequest,
                                   TEvControlPlaneProxy::TEvListConnectionsResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DescribeConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("DescribeConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString connectionId = request.connection_id();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(DescribeConnectionRequest, scope, user, connectionId, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvDescribeConnectionRequest::TPtr,
                                             TEvControlPlaneProxy::TEvDescribeConnectionResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DESCRIBE_CONNECTION, RTC_DESCRIBE_CONNECTION);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.connections.get@as"});
        if (issues) {
            CPS_LOG_E("DescribeConnectionRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDescribeConnectionResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvDescribeConnectionRequest::TPtr,
                                    TEvControlPlaneProxy::TEvDescribeConnectionResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::DescribeConnectionRequest,
                                   TEvControlPlaneStorage::TEvDescribeConnectionRequest,
                                   TEvControlPlaneStorage::TEvDescribeConnectionResponse,
                                   TEvControlPlaneProxy::TEvDescribeConnectionRequest,
                                   TEvControlPlaneProxy::TEvDescribeConnectionResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ModifyConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("ModifyConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString connectionId = request.connection_id();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ModifyConnectionRequest, scope, user, connectionId, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr,
                                             TEvControlPlaneProxy::TEvModifyConnectionResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_MODIFY_CONNECTION, RTC_MODIFY_CONNECTION);
        TVector<TString> requiredPermissions = {"yq.connections.update@as"};
        if (ExtractServiceAccountId(request)) {
            requiredPermissions.push_back("iam.serviceAccounts.use@as");
        }

        NYql::TIssues issues = ValidatePermissions(ev, requiredPermissions);
        if (issues) {
            CPS_LOG_E("ModifyConnectionRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvModifyConnectionResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr,
                                    TEvControlPlaneProxy::TEvModifyConnectionResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        if (!ev->Get()->ComputeDatabase) {
            Register(new TCreateComputeDatabaseActor<TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr,
                                                TEvControlPlaneProxy::TEvModifyConnectionResponse>
                                                (Counters.GetCommonCounters(RTC_CREATE_COMPUTE_DATABASE),
                                                 sender, Config, Config.ComputeConfig, cloudId,
                                                 scope, probe, ev, cookie));
            return;
        }

        const auto isYDBOperationEnabled = Config.ComputeConfig.IsYDBSchemaOperationsEnabled(
            ev->Get()->Scope, ev->Get()->Request.content().setting().connection_case());

        if (isYDBOperationEnabled && !ev->Get()->RequestValidationPassed) {
            auto requestValidationIssues =
                ::NFq::ValidateConnection(ev,
                                          Config.StorageConfig.Proto.GetMaxRequestSize(),
                                          Config.StorageConfig.AvailableConnections,
                                          Config.StorageConfig.Proto.GetDisableCurrentIam(),
                                          false);
            if (requestValidationIssues) {
                CPS_LOG_E("ModifyConnectionRequest, validation failed: "
                          << scope << " " << user << " " << NKikimr::MaskTicket(token)
                          << " " << request.DebugString()
                          << " error: " << requestValidationIssues.ToString());
                Send(ev->Sender,
                     new TEvControlPlaneProxy::TEvModifyConnectionResponse(
                         requestValidationIssues, subjectType),
                     0,
                     ev->Cookie);
                requestCounters.IncError();
                TDuration delta = TInstant::Now() - startTime;
                requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
                probe(delta, false, false);
                return;
            }
            ev->Get()->RequestValidationPassed = true;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
            | TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        if (isYDBOperationEnabled && !ev->Get()->OldConnectionContent) {
            auto permissions = ExtractPermissions(ev, availablePermissions);
            Register(MakeDiscoverYDBConnectionContentActor(
                ControlPlaneProxyActorId(), ev, Counters, Config.RequestTimeout, permissions));
            return;
        }
        if (isYDBOperationEnabled && !ev->Get()->OldBindingNamesDiscoveryFinished) {
            auto permissions = ExtractPermissions(ev, availablePermissions);
            Register(MakeListBindingIdsActor(
                ControlPlaneProxyActorId(), ev, Counters, Config.RequestTimeout, permissions));
            return;
        }
        if (isYDBOperationEnabled && ev->Get()->OldBindingIds.size() != ev->Get()->OldBindingContents.size()) {
            auto permissions = ExtractPermissions(ev, availablePermissions);
            Register(MakeDescribeListedBindingActor(
                ControlPlaneProxyActorId(), ev, Counters, Config.RequestTimeout, permissions));
            return;
        }
        if (!ev->Get()->ControlPlaneYDBOperationWasPerformed) {
            Register(new TRequestActor<FederatedQuery::ModifyConnectionRequest,
                                       TEvControlPlaneStorage::TEvModifyConnectionRequest,
                                       TEvControlPlaneStorage::TEvModifyConnectionResponse,
                                       TEvControlPlaneProxy::TEvModifyConnectionRequest,
                                       TEvControlPlaneProxy::TEvModifyConnectionResponse>(
                ev,
                Config,
                ControlPlaneStorageServiceActorId(),
                requestCounters,
                probe,
                availablePermissions,
                !isYDBOperationEnabled));
            return;
        }

        if (isYDBOperationEnabled) {
            if (!ev->Get()->YDBClient) {
                ev->Get()->YDBClient = CreateNewTableClient(ev,
                                                            Config.ComputeConfig,
                                                            YqSharedResources,
                                                            CredentialsProviderFactory);
            }

            if (!ev->Get()->ComputeYDBOperationWasPerformed) {
                Register(MakeModifyConnectionActor(
                    ControlPlaneProxyActorId(),
                    ev,
                    Config.RequestTimeout,
                    Counters,
                    Config.CommonConfig,
                    Config.ComputeConfig,
                    Signer));
                return;
            }

            Send(sender, ev->Get()->Response.release());
            return;
        }
    }

    void Handle(TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DeleteConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("DeleteConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString connectionId = request.connection_id();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(DeleteConnectionRequest, scope, user, connectionId, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr,
                                             TEvControlPlaneProxy::TEvDeleteConnectionResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DELETE_CONNECTION, RTC_DELETE_CONNECTION);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.connections.delete@as"});
        if (issues) {
            CPS_LOG_E("DeleteConnectionRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDeleteConnectionResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr,
                                    TEvControlPlaneProxy::TEvDeleteConnectionResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        if (!ev->Get()->ComputeDatabase) {
            Register(new TCreateComputeDatabaseActor<TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr,
                                                TEvControlPlaneProxy::TEvDeleteConnectionResponse>
                                                (Counters.GetCommonCounters(RTC_CREATE_COMPUTE_DATABASE),
                                                 sender, Config, Config.ComputeConfig, cloudId,
                                                 scope, probe, ev, cookie));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
            | TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        if (Config.ComputeConfig.YdbComputeControlPlaneEnabled(ev->Get()->Scope) && !ev->Get()->ConnectionContent) {
            auto permissions = ExtractPermissions(ev, availablePermissions);
            Register(MakeDiscoverYDBConnectionContentActor(
                ControlPlaneProxyActorId(), ev, Counters, Config.RequestTimeout, permissions));
            return;
        }

        const auto isYDBOperationEnabled =
            ev->Get()->ConnectionContent
                ? Config.ComputeConfig.IsYDBSchemaOperationsEnabled(
                      ev->Get()->Scope,
                      ev->Get()->ConnectionContent->setting().connection_case())
                : false;

        if (!ev->Get()->ControlPlaneYDBOperationWasPerformed) {
            Register(new TRequestActor<FederatedQuery::DeleteConnectionRequest,
                                       TEvControlPlaneStorage::TEvDeleteConnectionRequest,
                                       TEvControlPlaneStorage::TEvDeleteConnectionResponse,
                                       TEvControlPlaneProxy::TEvDeleteConnectionRequest,
                                       TEvControlPlaneProxy::TEvDeleteConnectionResponse>(
                ev,
                Config,
                ControlPlaneStorageServiceActorId(),
                requestCounters,
                probe,
                availablePermissions,
                !isYDBOperationEnabled));
            return;
        }

        if (isYDBOperationEnabled) {
            if (!ev->Get()->YDBClient) {
                ev->Get()->YDBClient = CreateNewTableClient(ev,
                                                            Config.ComputeConfig,
                                                            YqSharedResources,
                                                            CredentialsProviderFactory);
            }

            if (!ev->Get()->ComputeYDBOperationWasPerformed) {
                Register(MakeDeleteConnectionActor(ControlPlaneProxyActorId(),
                                                   ev,
                                                   Config.RequestTimeout,
                                                   Counters,
                                                   Config.CommonConfig,
                                                   Signer));
                return;
            }

            Send(sender, ev->Get()->Response.release());
        }
    }

    void Handle(TEvControlPlaneProxy::TEvTestConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::TestConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("TestConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;

        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(TestConnectionRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvTestConnectionRequest::TPtr,
                                             TEvControlPlaneProxy::TEvTestConnectionResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_TEST_CONNECTION, RTC_TEST_CONNECTION);
        TVector<TString> requiredPermissions = {"yq.connections.create@as"};
        if (ExtractServiceAccountId(request)) {
            requiredPermissions.push_back("iam.serviceAccounts.use@as");
        }

        NYql::TIssues issues = ValidatePermissions(ev, requiredPermissions);
        if (issues) {
            CPS_LOG_E("TestConnectionRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvTestConnectionResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvTestConnectionRequest::TPtr,
                                    TEvControlPlaneProxy::TEvTestConnectionResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        Register(new TRequestActor<FederatedQuery::TestConnectionRequest,
                                   TEvTestConnection::TEvTestConnectionRequest,
                                   TEvTestConnection::TEvTestConnectionResponse,
                                   TEvControlPlaneProxy::TEvTestConnectionRequest,
                                   TEvControlPlaneProxy::TEvTestConnectionResponse>(
            ev, Config, TestConnectionActorId(), requestCounters, probe, {}));
    }

    void Handle(TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::CreateBindingRequest request = ev->Get()->Request;
        CPP_LOG_T("CreateBindingRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const bool ydbOperationWasPerformed = ev->Get()->ComputeYDBOperationWasPerformed;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(CreateBindingRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr,
                                             TEvControlPlaneProxy::TEvCreateBindingResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_CREATE_BINDING, RTC_CREATE_BINDING);

        auto requiredParams = TVector<TString>{"yq.bindings.create@as", "yq.connections.get@as"};

        NYql::TIssues issues = ValidatePermissions(ev, requiredParams);
        if (issues) {
            CPS_LOG_E("CreateBindingRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvCreateBindingResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr,
                                    TEvControlPlaneProxy::TEvCreateBindingResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        if (!ev->Get()->ComputeDatabase) {
            Register(new TCreateComputeDatabaseActor<TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr,
                                                TEvControlPlaneProxy::TEvCreateBindingResponse>
                                                (Counters.GetCommonCounters(RTC_CREATE_COMPUTE_DATABASE),
                                                 sender, Config, Config.ComputeConfig, cloudId,
                                                 scope, probe, ev, cookie));
            return;
        }

        if (Config.ComputeConfig.YdbComputeControlPlaneEnabled(ev->Get()->Scope) &&
            !ev->Get()->RequestValidationPassed) {
            auto requestValidationIssues =
                ::NFq::ValidateBinding(ev,
                                       Config.StorageConfig.Proto.GetMaxRequestSize(),
                                       Config.StorageConfig.AvailableBindings,
                                       Config.StorageConfig.GeneratorPathsLimit);
            if (requestValidationIssues) {
                CPS_LOG_E("CreateBindingRequest, validation failed: "
                          << scope << " " << user << " " << NKikimr::MaskTicket(token)
                          << " " << request.DebugString()
                          << " error: " << requestValidationIssues.ToString());
                Send(ev->Sender,
                     new TEvControlPlaneProxy::TEvCreateBindingResponse(
                         requestValidationIssues, subjectType),
                     0,
                     ev->Cookie);
                requestCounters.IncError();
                TDuration delta = TInstant::Now() - startTime;
                requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
                probe(delta, false, false);
                return;
            }
            ev->Get()->RequestValidationPassed = true;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::MANAGE_PUBLIC
        };

        bool isYDBOperationEnabled = Config.ComputeConfig.IsYDBSchemaOperationsEnabled(
            ev->Get()->Scope,
            ev->Get()->Request.content().setting().binding_case());

        if (isYDBOperationEnabled) {
            if (!ev->Get()->ConnectionsWithSameNameWereListed) {
                Register(MakeListConnectionIdsActor(ControlPlaneProxyActorId(),
                                                    ev,
                                                    Counters,
                                                    Config.RequestTimeout,
                                                    availablePermissions));
                return;
            }
            if (!ev->Get()->BindingWithSameNameWereListed) {
                Register(MakeListBindingIdsActor(ControlPlaneProxyActorId(),
                                                 ev,
                                                 Counters,
                                                 Config.RequestTimeout,
                                                 availablePermissions));
                return;
            }
            if (!ValidateNameUniquenessConstraint<TEvControlPlaneProxy::TEvCreateBindingRequest,
                                                  TEvControlPlaneProxy::TEvCreateBindingResponse>(
                    ev, requestCounters, startTime, probe, "TEvCreateBindingRequest")) {
                return;
            }
        }

        if (isYDBOperationEnabled && !ydbOperationWasPerformed) {
            if (!ev->Get()->ConnectionContent) {
                auto permissions = ExtractPermissions(ev, availablePermissions);
                Register(MakeDiscoverYDBConnectionContentActor(ControlPlaneProxyActorId(),
                                                               std::move(ev),
                                                               Counters,
                                                               Config.RequestTimeout,
                                                               permissions));
                return;
            }

            if (!ev->Get()->YDBClient) {
                ev->Get()->YDBClient = CreateNewTableClient(ev,
                                                            Config.ComputeConfig,
                                                            YqSharedResources,
                                                            CredentialsProviderFactory);
            }

            Register(MakeCreateBindingActor(ControlPlaneProxyActorId(),
                                            std::move(ev),
                                            Config.RequestTimeout,
                                            Counters,
                                            availablePermissions,
                                            Config.ComputeConfig));
            return;
        }

        Register(new TRequestActor<FederatedQuery::CreateBindingRequest,
                                   TEvControlPlaneStorage::TEvCreateBindingRequest,
                                   TEvControlPlaneStorage::TEvCreateBindingResponse,
                                   TEvControlPlaneProxy::TEvCreateBindingRequest,
                                   TEvControlPlaneProxy::TEvCreateBindingResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvListBindingsRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ListBindingsRequest request = ev->Get()->Request;
        CPP_LOG_T("ListBindingsRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ListBindingsRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvListBindingsRequest::TPtr,
                                             TEvControlPlaneProxy::TEvListBindingsResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_LIST_BINDINGS, RTC_LIST_BINDINGS);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.get@as"});
        if (issues) {
            CPS_LOG_E("ListBindingsRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvListBindingsResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvListBindingsRequest::TPtr,
                                    TEvControlPlaneProxy::TEvListBindingsResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::ListBindingsRequest,
                                   TEvControlPlaneStorage::TEvListBindingsRequest,
                                   TEvControlPlaneStorage::TEvListBindingsResponse,
                                   TEvControlPlaneProxy::TEvListBindingsRequest,
                                   TEvControlPlaneProxy::TEvListBindingsResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DescribeBindingRequest request = ev->Get()->Request;
        CPP_LOG_T("DescribeBindingRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString bindingId = request.binding_id();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(DescribeBindingRequest, scope, user, bindingId, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvDescribeBindingRequest::TPtr,
                                             TEvControlPlaneProxy::TEvDescribeBindingResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DESCRIBE_BINDING, RTC_DESCRIBE_BINDING);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.get@as"});
        if (issues) {
            CPS_LOG_E("DescribeBindingRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDescribeBindingResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvDescribeBindingRequest::TPtr,
                                    TEvControlPlaneProxy::TEvDescribeBindingResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::DescribeBindingRequest,
                                   TEvControlPlaneStorage::TEvDescribeBindingRequest,
                                   TEvControlPlaneStorage::TEvDescribeBindingResponse,
                                   TEvControlPlaneProxy::TEvDescribeBindingRequest,
                                   TEvControlPlaneProxy::TEvDescribeBindingResponse>(
            ev,
            Config,
            ControlPlaneStorageServiceActorId(),
            requestCounters,
            probe,
            availablePermissions));
    }

    void Handle(TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ModifyBindingRequest request = ev->Get()->Request;
        CPP_LOG_T("ModifyBindingRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString bindingId = request.binding_id();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ModifyBindingRequest, scope, user, bindingId, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr,
                                             TEvControlPlaneProxy::TEvModifyBindingResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_MODIFY_BINDING, RTC_MODIFY_BINDING);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.update@as"});
        if (issues) {
            CPS_LOG_E("ModifyBindingRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvModifyBindingResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr,
                                    TEvControlPlaneProxy::TEvModifyBindingResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        if (!ev->Get()->ComputeDatabase) {
            Register(new TCreateComputeDatabaseActor<TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr,
                                                TEvControlPlaneProxy::TEvModifyBindingResponse>
                                                (Counters.GetCommonCounters(RTC_CREATE_COMPUTE_DATABASE),
                                                 sender, Config, Config.ComputeConfig, cloudId,
                                                 scope, probe, ev, cookie));
            return;
        }

        if (Config.ComputeConfig.YdbComputeControlPlaneEnabled(ev->Get()->Scope) &&
            !ev->Get()->RequestValidationPassed) {
            auto requestValidationIssues =
                ::NFq::ValidateBinding(ev,
                                       Config.StorageConfig.Proto.GetMaxRequestSize(),
                                       Config.StorageConfig.AvailableBindings,
                                       Config.StorageConfig.GeneratorPathsLimit);
            if (requestValidationIssues) {
                CPS_LOG_E("ModifyBindingRequest, validation failed: "
                          << scope << " " << user << " " << NKikimr::MaskTicket(token)
                          << " " << request.DebugString()
                          << " error: " << requestValidationIssues.ToString());
                Send(ev->Sender,
                     new TEvControlPlaneProxy::TEvModifyBindingResponse(
                         requestValidationIssues, subjectType),
                     0,
                     ev->Cookie);
                requestCounters.IncError();
                TDuration delta = TInstant::Now() - startTime;
                requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
                probe(delta, false, false);
                return;
            }
            ev->Get()->RequestValidationPassed = true;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
            | TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        auto isComputeYDBSyncEnabled = Config.ComputeConfig.IsYDBSchemaOperationsEnabled(
            ev->Get()->Scope, ev->Get()->Request.content().setting().binding_case());

        if (isComputeYDBSyncEnabled && !ev->Get()->OldBindingContent) {
            auto permissions = ExtractPermissions(ev, availablePermissions);
            Register(MakeDiscoverYDBBindingContentActor(ControlPlaneProxyActorId(),
                                                        ev,
                                                        Counters,
                                                        Config.RequestTimeout,
                                                        permissions));
            return;
        }
        if (isComputeYDBSyncEnabled && !ev->Get()->ConnectionContent) {
            auto permissions = ExtractPermissions(ev, availablePermissions);
            Register(MakeDiscoverYDBConnectionContentActor(ControlPlaneProxyActorId(),
                                                           ev,
                                                           Counters,
                                                           Config.RequestTimeout,
                                                           permissions));
            return;
        }

        if (!ev->Get()->ControlPlaneYDBOperationWasPerformed) {
            Register(new TRequestActor<FederatedQuery::ModifyBindingRequest,
                                       TEvControlPlaneStorage::TEvModifyBindingRequest,
                                       TEvControlPlaneStorage::TEvModifyBindingResponse,
                                       TEvControlPlaneProxy::TEvModifyBindingRequest,
                                       TEvControlPlaneProxy::TEvModifyBindingResponse>(
                ev,
                Config,
                ControlPlaneStorageServiceActorId(),
                requestCounters,
                probe,
                availablePermissions,
                !isComputeYDBSyncEnabled));
            return;
        }

        if (isComputeYDBSyncEnabled) {
            if (!ev->Get()->YDBClient) {
                ev->Get()->YDBClient = CreateNewTableClient(ev,
                                                            Config.ComputeConfig,
                                                            YqSharedResources,
                                                            CredentialsProviderFactory);
            }
            if (!ev->Get()->ComputeYDBOperationWasPerformed) {
                Register(MakeModifyBindingActor(ControlPlaneProxyActorId(),
                                                std::move(ev),
                                                Config.RequestTimeout,
                                                Counters,
                                                Config.ComputeConfig));
                return;
            }
            Send(sender, ev->Get()->Response.release());
        }
    }

    void Handle(TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DeleteBindingRequest request = ev->Get()->Request;
        CPP_LOG_T("DeleteBindingRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = ev->Get()->Scope;
        TString user = ev->Get()->User;
        TString token = ev->Get()->Token;
        const TString bindingId = request.binding_id();
        const int byteSize = request.ByteSize();
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(DeleteBindingRequest, scope, user, bindingId, delta, byteSize, isSuccess, isTimeout);
        };

        if (!cloudId) {
            Register(new TResolveFolderActor<TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr,
                                             TEvControlPlaneProxy::TEvDeleteBindingResponse>
                                             (Counters.GetCommonCounters(RTC_RESOLVE_FOLDER), sender,
                                              Config, scope, token,
                                              probe, ev, cookie, QuotaManagerEnabled));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DELETE_BINDING, RTC_DELETE_BINDING);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.delete@as"});
        if (issues) {
            CPS_LOG_E("DeleteBindingRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDeleteBindingResponse(issues, subjectType), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        if (!subjectType) {
            Register(new TResolveSubjectTypeActor<TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr,
                                    TEvControlPlaneProxy::TEvDeleteBindingResponse>
                                    (Counters.GetCommonCounters(RTC_RESOLVE_SUBJECT_TYPE), sender,
                                    Config, token,
                                    probe, ev, cookie, AccessService));
            return;
        }

        if (!ev->Get()->ComputeDatabase) {
            Register(new TCreateComputeDatabaseActor<TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr,
                                                TEvControlPlaneProxy::TEvDeleteBindingResponse>
                                                (Counters.GetCommonCounters(RTC_CREATE_COMPUTE_DATABASE),
                                                 sender, Config, Config.ComputeConfig, cloudId,
                                                 scope, probe, ev, cookie));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
            | TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        if (Config.ComputeConfig.YdbComputeControlPlaneEnabled(ev->Get()->Scope) &&
            !ev->Get()->OldBindingContent) {
            auto permissions = ExtractPermissions(ev, availablePermissions);
            Register(MakeDiscoverYDBBindingContentActor(
                ControlPlaneProxyActorId(), ev, Counters, Config.RequestTimeout, permissions));
            return;
        }

        auto bindingCase = ev->Get()->OldBindingContent
                               ? ev->Get()->OldBindingContent->setting().binding_case()
                               : FederatedQuery::BindingSetting::BINDING_NOT_SET;
        auto isComputeYDBSyncEnabled =
            Config.ComputeConfig.IsYDBSchemaOperationsEnabled(ev->Get()->Scope,
                                                              bindingCase);
        if (!ev->Get()->ControlPlaneYDBOperationWasPerformed) {
            Register(new TRequestActor<FederatedQuery::DeleteBindingRequest,
                                       TEvControlPlaneStorage::TEvDeleteBindingRequest,
                                       TEvControlPlaneStorage::TEvDeleteBindingResponse,
                                       TEvControlPlaneProxy::TEvDeleteBindingRequest,
                                       TEvControlPlaneProxy::TEvDeleteBindingResponse>(
                ev,
                Config,
                ControlPlaneStorageServiceActorId(),
                requestCounters,
                probe,
                availablePermissions,
                !isComputeYDBSyncEnabled));
            return;
        }

        if (isComputeYDBSyncEnabled) {
            if (!ev->Get()->YDBClient) {
                ev->Get()->YDBClient = CreateNewTableClient(ev,
                                                            Config.ComputeConfig,
                                                            YqSharedResources,
                                                            CredentialsProviderFactory);
            }

            if (!ev->Get()->ComputeYDBOperationWasPerformed) {
                Register(MakeDeleteBindingActor(ControlPlaneProxyActorId(),
                                                std::move(ev),
                                                Config.RequestTimeout,
                                                Counters));
                return;
            }

            Send(sender, ev->Get()->Response.release());
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        HTML(str) {
            PRE() {
                str << "Current config:" << Endl;
                str << Config.Proto.DebugString() << Endl;
                str << Endl;
            }
        }
        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }
};

} // namespace

TActorId ControlPlaneProxyActorId() {
    constexpr TStringBuf name = "YQCTLPRX";
    return NActors::TActorId(0, name);
}

IActor* CreateControlPlaneProxyActor(
    const NConfig::TControlPlaneProxyConfig& config,
    const NConfig::TControlPlaneStorageConfig& storageConfig,
    const NConfig::TComputeConfig& computeConfig,
    const NConfig::TCommonConfig& commonConfig,
    const NYql::TS3GatewayConfig& s3Config,
    const ::NFq::TSigner::TPtr& signer,
    const TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    bool quotaManagerEnabled) {
    return new TControlPlaneProxyActor(
        config,
        storageConfig,
        computeConfig,
        commonConfig,
        s3Config,
        signer,
        yqSharedResources,
        credentialsProviderFactory,
        counters,
        quotaManagerEnabled);
}

}  // namespace NFq
