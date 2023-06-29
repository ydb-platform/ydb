#include "config.h"
#include "control_plane_proxy.h"
#include "probes.h"
#include "utils.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/common/cache.h>
#include <ydb/core/fq/libs/compute/ydb/control_plane/compute_database_control_plane_service.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/control_plane_config/control_plane_config.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include <ydb/core/fq/libs/quota_manager/quota_manager.h>
#include <ydb/core/fq/libs/rate_limiter/events/control_plane_events.h>
#include <ydb/core/fq/libs/test_connection/events/events.h>
#include <ydb/core/fq/libs/test_connection/test_connection.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/control_plane_proxy/control_plane_proxy.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actor.h>

#include <ydb/core/base/kikimr_issue.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/ycloud/impl/access_service.h>
#include <ydb/library/ycloud/impl/mock_access_service.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <ydb/library/security/util.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/retry/retry_policy.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>

#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/folder_service/events.h>

#include <contrib/libs/fmt/include/fmt/format.h>

#include <util/string/join.h>

namespace NFq {
namespace {

using namespace NActors;
using namespace NFq::NConfig;
using namespace NKikimr;
using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

LWTRACE_USING(YQ_CONTROL_PLANE_PROXY_PROVIDER);

struct TRequestScopeCounters: public virtual TThrRefBase {
    const TString Name;

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFly;
    ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
    ::NMonitoring::TDynamicCounters::TCounterPtr Error;
    ::NMonitoring::TDynamicCounters::TCounterPtr Timeout;
    ::NMonitoring::TDynamicCounters::TCounterPtr Retry;

    explicit TRequestScopeCounters(const TString& name)
        : Name(name)
    { }

    void Register(const ::NMonitoring::TDynamicCounterPtr& counters) {
        Counters = counters;
        ::NMonitoring::TDynamicCounterPtr subgroup = counters->GetSubgroup("request_scope", Name);
        InFly = subgroup->GetCounter("InFly", false);
        Ok = subgroup->GetCounter("Ok", true);
        Error = subgroup->GetCounter("Error", true);
        Timeout = subgroup->GetCounter("Timeout", true);
        Timeout = subgroup->GetCounter("Retry", true);
    }

    virtual ~TRequestScopeCounters() override {
        Counters->RemoveSubgroup("request_scope", Name);
    }

private:
    static ::NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets() {
        return ::NMonitoring::ExplicitHistogram({0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 50000, 500000});
    }
};

struct TRequestCommonCounters: public virtual TThrRefBase {
    const TString Name;

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFly;
    ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
    ::NMonitoring::TDynamicCounters::TCounterPtr Error;
    ::NMonitoring::TDynamicCounters::TCounterPtr Timeout;
    ::NMonitoring::TDynamicCounters::TCounterPtr Retry;
    ::NMonitoring::THistogramPtr LatencyMs;

    explicit TRequestCommonCounters(const TString& name)
        : Name(name)
    { }

    void Register(const ::NMonitoring::TDynamicCounterPtr& counters) {
        Counters = counters;
        ::NMonitoring::TDynamicCounterPtr subgroup = counters->GetSubgroup("request_common", Name);
        InFly = subgroup->GetCounter("InFly", false);
        Ok = subgroup->GetCounter("Ok", true);
        Error = subgroup->GetCounter("Error", true);
        Timeout = subgroup->GetCounter("Timeout", true);
        Retry = subgroup->GetCounter("Retry", true);
        LatencyMs = subgroup->GetHistogram("LatencyMs", GetLatencyHistogramBuckets());
    }

    virtual ~TRequestCommonCounters() override {
        Counters->RemoveSubgroup("request_common", Name);
    }

private:
    static ::NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets() {
        return ::NMonitoring::ExplicitHistogram({0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 50000, 500000});
    }
};

using TRequestScopeCountersPtr = TIntrusivePtr<TRequestScopeCounters>;
using TRequestCommonCountersPtr = TIntrusivePtr<TRequestCommonCounters>;

struct TRequestCounters {
    TRequestScopeCountersPtr Scope;
    TRequestCommonCountersPtr Common;

    void IncInFly() {
        Scope->InFly->Inc();
        Common->InFly->Inc();
    }

    void DecInFly() {
        Scope->InFly->Dec();
        Common->InFly->Dec();
    }

    void IncOk() {
        Scope->Ok->Inc();
        Common->Ok->Inc();
    }

    void DecOk() {
        Scope->Ok->Dec();
        Common->Ok->Dec();
    }

    void IncError() {
        Scope->Error->Inc();
        Common->Error->Inc();
    }

    void DecError() {
        Scope->Error->Dec();
        Common->Error->Dec();
    }

    void IncTimeout() {
        Scope->Timeout->Inc();
        Common->Timeout->Inc();
    }

    void DecTimeout() {
        Scope->Timeout->Dec();
        Common->Timeout->Dec();
    }

    void IncRetry() {
        Scope->Retry->Inc();
        Common->Retry->Inc();
    }

    void DecRetry() {
        Scope->Retry->Dec();
        Common->Retry->Dec();
    }
};

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
        CPP_LOG_D("Quota request timeout. Cloud id: " << Event->Get()->CloudId << " Actor id: " << SelfId());
        Send(MakeQuotaServiceActorId(SelfId().NodeId()), new TEvQuotaService::TQuotaGetRequest(SUBJECT_TYPE_CLOUD, Event->Get()->CloudId, true));
    }
};

template<class TEventRequest, class TResponseProxy>
class TResolveSubjectTypeActor : public NActors::TActorBootstrapped<TResolveSubjectTypeActor<TEventRequest, TResponseProxy>> {
    using TBase = NActors::TActorBootstrapped<TResolveSubjectTypeActor<TEventRequest, TResponseProxy>>;
    using TBase::SelfId;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Become;
    using TBase::Register;
    using IRetryPolicy = IRetryPolicy<NCloud::TEvAccessService::TEvAuthenticateResponse::TPtr&>;

    ::NFq::TControlPlaneProxyConfig Config;
    TActorId Sender;
    TRequestCommonCountersPtr Counters;
    TString Token;
    std::function<void(const TDuration&, bool, bool)> Probe;
    TEventRequest Event;
    ui32 Cookie;
    TInstant StartTime;
    IRetryPolicy::IRetryState::TPtr RetryState;
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
        , Token(token)
        , Probe(probe)
        , Event(event)
        , Cookie(cookie)
        , StartTime(TInstant::Now())
        , RetryState(GetRetryPolicy()->CreateRetryState())
        , AccessService(accessService)
    {}

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
        CPP_LOG_D("Resolve subject type timeout. Token: " << MaskTicket(Token) << " Actor id: " << SelfId());
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
        Counters->InFly->Dec();
        Counters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
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
            Counters->Error->Inc();
            CPP_LOG_E(errorMessage);
            NYql::TIssues issues;
            NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, "Resolve subject type error");
            issues.AddIssue(issue);
            Counters->Error->Inc();
            const TDuration delta = TInstant::Now() - StartTime;
            Probe(delta, false, false);
            Send(Sender, new TResponseProxy(issues, {}), 0, Cookie);
            PassAway();
            return;
        }

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

TString EncloseAndEscapeString(const TString& value, char enclosingChar) {
    auto escapedValue = value;
    SubstGlobal(
        escapedValue, TString{enclosingChar}, TStringBuilder{} << '\\' << enclosingChar);
    return TStringBuilder{} << enclosingChar << escapedValue << enclosingChar;
}

class TCreateConnectionInYDBActor :
    public NActors::TActorBootstrapped<TCreateConnectionInYDBActor> {
    struct TEvPrivate {
        enum EEv {
            EvCreateSessionResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvCreateConnectionExecutionResponse,
            EvEnd
        };

        static_assert(
            EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE),
            "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvCreateSessionResponse :
            NActors::TEventLocal<TEvCreateSessionResponse, EvCreateSessionResponse> {
            TAsyncCreateSessionResult Result;

            TEvCreateSessionResponse(TAsyncCreateSessionResult result)
                : Result(std::move(result)) { }
        };

        struct TEvCreateConnectionExecutionResponse :
            NActors::TEventLocal<TEvCreateConnectionExecutionResponse, EvCreateConnectionExecutionResponse> {
            TAsyncStatus Result;

            TEvCreateConnectionExecutionResponse(TAsyncStatus result)
                : Result(std::move(result)) { }
        };
    };

    using TBase = NActors::TActorBootstrapped<TCreateConnectionInYDBActor>;
    using TBase::Become;
    using TBase::PassAway;
    using TBase::Register;
    using TBase::SelfId;
    using TBase::Send;
    using IRetryPolicy =
        IRetryPolicy<NCloud::TEvAccessService::TEvAuthenticateResponse::TPtr&>;
    using TTableClientPtr = std::unique_ptr<NYdb::NTable::TTableClient>;

    using TEventRequest = TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr;
    using TResponce = TEvControlPlaneProxy::TEvCreateConnectionResponse;

    TActorId Sender;
    TRequestCommonCountersPtr Counters;
    TEventRequest Event;
    ui32 Cookie;
    TDuration RequestTimeout;
    TInstant StartTime;
    TString Scope;
    TTableClientPtr TableClient;
    TString ObjectStorageEndpoint;

public:
    TCreateConnectionInYDBActor(
        const TRequestCommonCountersPtr& counters,
        const NFq::TComputeConfig& computeConfig,
        const TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TString& objectStorageEndpoint,
        TActorId sender,
        TEventRequest event,
        ui32 cookie,
        const TString& scope,
        TDuration requestTimeout)
        : Sender(sender)
        , Counters(counters)
        , Event(event)
        , Cookie(cookie)
        , RequestTimeout(requestTimeout)
        , StartTime(TInstant::Now())
        , Scope(scope)
        , TableClient(CreateNewTableClient(
              computeConfig, yqSharedResources, credentialsProviderFactory))
        , ObjectStorageEndpoint(objectStorageEndpoint) { }

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_CREATE_CONNECTION_IN_YDB";

    void Bootstrap() {
        CPP_LOG_T("Create connection in YDB. Actor id: " << SelfId());

        if (auto issues = ValidateRequest(Event->Get()->Request); !issues.Empty()) {
            NYql::TIssue issue = MakeErrorIssue(
                TIssuesIds::INTERNAL_ERROR, "CreateConnectionRequest is not valid");
            for (auto& subIssue : issues) {
                issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
            }
            SendErrorMessageToSender(std::move(issue));
            return;
        }

        Become(
            &TCreateConnectionInYDBActor::StateFunc,
            RequestTimeout,
            new NActors::TEvents::TEvWakeup());
        Counters->InFly->Inc();
        InitiateConnectionCreation();
    }

    NYql::TIssues ValidateRequest(const FederatedQuery::CreateConnectionRequest& request) {
        NYql::TIssues issues;
        if (request.content().name().Contains('/')) {
            issues.AddIssue(MakeErrorIssue(
                TIssuesIds::INTERNAL_ERROR, "'/' is not allowed in connection name"));
        }
        return issues;
    }

    void InitiateConnectionCreation() {
        auto request = Event->Get()->Request;
        TableClient->CreateSession().Subscribe(
            [actorSystem = NActors::TActivationContext::ActorSystem(),
             self = SelfId()](const TAsyncCreateSessionResult& future) {
                actorSystem->Send(
                    self, new TEvPrivate::TEvCreateSessionResponse(std::move(future)));
            });
    }

    void Handle(TEvPrivate::TEvCreateSessionResponse::TPtr& event) {
        using namespace fmt::literals;
        auto createSessionResult = event->Get()->Result.GetValueSync();
        if (!createSessionResult.IsSuccess()) {
            TString errorMessage = TStringBuilder{}
                                   << "Couldn't create YDB session. Status"
                                   << createSessionResult.GetStatus();
            CPP_LOG_E(errorMessage);

            NYql::TIssue issue =
                MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, "Couldn't create YDB session");
            for (auto& subIssue : createSessionResult.GetIssues()) {
                issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
            }
            SendErrorMessageToSender(std::move(issue));
            return;
        }

        auto request = Event->Get()->Request;
        auto session = createSessionResult.GetSession();
        auto bucketName = request.content().setting().object_storage().bucket();
        SubstGlobal(bucketName, TString{'"'}, "\\\"");
        session
            .ExecuteSchemeQuery(fmt::format(
                R"(
                    CREATE EXTERNAL DATA SOURCE {external_source} WITH (
                        SOURCE_TYPE="ObjectStorage",
                        LOCATION="{location}",
                        AUTH_METHOD="NONE"
                    );
                )",
                "external_source"_a = EncloseAndEscapeString(request.content().name(), '`'),
                "location"_a = ObjectStorageEndpoint + "/" + bucketName + "/"))
            .Subscribe([actorSystem = NActors::TActivationContext::ActorSystem(),
                        self = SelfId()](const TAsyncStatus& future) {
                actorSystem->Send(
                    self,
                    new TEvPrivate::TEvCreateConnectionExecutionResponse(std::move(future)));
            });
    }

    void Handle(TEvPrivate::TEvCreateConnectionExecutionResponse::TPtr& event) {
        Counters->InFly->Dec();
        Counters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());

        const auto& status = event->Get()->Result.GetValueSync();
        if (!status.IsSuccess()) {
            TString errorMessage;
            if (status.GetStatus() == NYdb::EStatus::ALREADY_EXISTS) {
                errorMessage = "External data source with such name already exists";
            } else {
                errorMessage = TStringBuilder{}
                               << "Couldn't create external data source in YDB. Status"
                               << status.GetStatus();
            }

            CPP_LOG_E(errorMessage);

            NYql::TIssue issue = MakeErrorIssue(
                TIssuesIds::INTERNAL_ERROR, "Couldn't create external data source in YDB");
            for (auto& subIssue : status.GetIssues()) {
                issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
            }
            SendErrorMessageToSender(std::move(issue));
            return;
        }

        CPP_LOG_T("External data source in YDB was successfully created");
        Counters->Ok->Inc();
        Event->Get()->ComputeYDBOperationWasPerformed = true;

        TActivationContext::Send(Event->Forward(ControlPlaneProxyActorId()));
        PassAway();
    }

    void PassAway() override { TActor::PassAway(); }

    STRICT_STFUNC(StateFunc, cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
                  hFunc(TEvPrivate::TEvCreateSessionResponse, Handle);
                  hFunc(TEvPrivate::TEvCreateConnectionExecutionResponse, Handle);)

    void HandleTimeout() {
        CPP_LOG_D(
            "Timeout occurred while creating external data source in YDB. Actor id: "
            << SelfId());
        Counters->Timeout->Inc();
        SendErrorMessageToSender(MakeErrorIssue(
            TIssuesIds::TIMEOUT,
            "Timeout occurred while creating external data source in YDB. Try repeating the request later"));
    }

    void SendErrorMessageToSender(NYql::TIssue issue) {
        Counters->Error->Inc();
        NYql::TIssues issues;
        issues.AddIssue(issue);
        Send(
            Sender,
            new TEvControlPlaneProxy::TEvCreateConnectionResponse(issues, {}),
            0,
            Cookie); // Change to template
        PassAway();
    }

private:
    TTableClientPtr CreateNewTableClient(
        const NFq::TComputeConfig& computeConfig,
        const TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) {

        NFq::NConfig::TYdbStorageConfig computeConnection = computeConfig.GetConnection(Scope);
        computeConnection.set_endpoint(Event->Get()->ComputeDatabase->connection().endpoint());
        computeConnection.set_database(Event->Get()->ComputeDatabase->connection().database());
        computeConnection.set_usessl(Event->Get()->ComputeDatabase->connection().usessl());

        auto tableSettings = GetClientSettings<NYdb::NTable::TClientSettings>(
            computeConnection, credentialsProviderFactory);
        return std::make_unique<NYdb::NTable::TTableClient>(
            yqSharedResources->UserSpaceYdbDriver, tableSettings);
    }
};

class TCreateBindingInYDBActor :
    public NActors::TActorBootstrapped<TCreateBindingInYDBActor> {
    struct TEvPrivate {
        enum EEv {
            EvCreateSessionResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvCreateBindingExecutionResponse,
            EvEnd
        };

        static_assert(
            EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE),
            "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvCreateSessionResponse :
            NActors::TEventLocal<TEvCreateSessionResponse, EvCreateSessionResponse> {
            TAsyncCreateSessionResult Result;

            TEvCreateSessionResponse(TAsyncCreateSessionResult result)
                : Result(std::move(result)) { }
        };

        struct TEvCreateBindingExecutionResponse :
            NActors::TEventLocal<TEvCreateBindingExecutionResponse, EvCreateBindingExecutionResponse> {
            TAsyncStatus Result;

            TEvCreateBindingExecutionResponse(TAsyncStatus result)
                : Result(std::move(result)) { }
        };
    };

    using TBase = NActors::TActorBootstrapped<TCreateBindingInYDBActor>;
    using TBase::Become;
    using TBase::PassAway;
    using TBase::Register;
    using TBase::SelfId;
    using TBase::Send;
    using IRetryPolicy =
        IRetryPolicy<NCloud::TEvAccessService::TEvAuthenticateResponse::TPtr&>;
    using TTableClientPtr = std::unique_ptr<NYdb::NTable::TTableClient>;

    using TEventRequest = TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr;
    using TResponce = TEvControlPlaneProxy::TEvCreateBindingResponse;
    using TResponseProxy = TEvControlPlaneProxy::TEvCreateBindingResponse;

    TActorId Sender;
    TRequestCommonCountersPtr Counters;
    TEventRequest Event;
    ui32 Cookie;
    TInstant StartTime;
    TPermissions Permissions;
    TDuration RequestTimeout;
    TString Scope;
    TTableClientPtr TableClient;
    TString ConnectionName;

public:
    TCreateBindingInYDBActor(
        const TRequestCommonCountersPtr& counters,
        const NFq::TComputeConfig& computeConfig,
        const TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        TActorId sender,
        TEventRequest event,
        ui32 cookie,
        TPermissions permissions,
        const TString& scope,
        TDuration requestTimeout)
        : Sender(sender)
        , Counters(counters)
        , Event(event)
        , Cookie(cookie)
        , StartTime(TInstant::Now())
        , Permissions(std::move(permissions))
        , RequestTimeout(requestTimeout)
        , Scope(scope)
        , TableClient(CreateNewTableClient(
              computeConfig, yqSharedResources, credentialsProviderFactory)) { }

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_CREATE_BINDING_IN_YDB";

    void Bootstrap() {
        CPP_LOG_T("Create external table in YDB. Actor id: " << SelfId());

        if (auto issues = ValidateRequest(Event->Get()->Request); !issues.Empty()) {
            NYql::TIssue issue = MakeErrorIssue(
                TIssuesIds::INTERNAL_ERROR, "CreateBindingRequest is not valid");
            for (auto& subIssue : issues) {
                issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
            }
            SendErrorMessageToSender(std::move(issue));
            return;
        }

        Become(
            &TCreateBindingInYDBActor::StateFunc,
            RequestTimeout,
            new NActors::TEvents::TEvWakeup());
        Counters->InFly->Inc();
        ResolveConnectionId();
    }

    void ResolveConnectionId() {
        FederatedQuery::DescribeConnectionRequest request;
        auto connectionId = Event->Get()->Request.content().connection_id();
        request.set_connection_id(connectionId);
        CPP_LOG_T(
            "Create external table in YDB. Resolving connection id. Actor id: "
            << SelfId() << " connection_id: " << connectionId);
        auto event = new TEvControlPlaneStorage::TEvDescribeConnectionRequest(
            Scope,
            request,
            Event->Get()->User,
            Event->Get()->Token,
            Event->Get()->CloudId,
            Permissions,
            Event->Get()->Quotas,
            Event->Get()->TenantInfo,
            {});
        Send(ControlPlaneStorageServiceActorId(), event);
    }

    void Handle(TEvControlPlaneStorage::TEvDescribeConnectionResponse::TPtr& event) {
        const auto& issues = event->Get()->Issues;
        if (!issues.Empty()) {
            CPP_LOG_E(
                "Couldn't resolve connection id. Actor id: " << SelfId() << " Status: "
                                                             << issues.ToOneLineString());

            NYql::TIssue issue =
                MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, "Couldn't resolve connection id");
            for (auto& subIssue : issues) {
                issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
            }
            SendErrorMessageToSender(std::move(issue));
            return;
        }

        ConnectionName = event->Get()->Result.connection().content().name();
        CPP_LOG_T(
            "Create external table in YDB. Resolved connection name. Actor id: "
            << SelfId() << " Connection name: " << ConnectionName);
        InitiateConnectionCreation();
    }

    void InitiateConnectionCreation() {
        auto request = Event->Get()->Request;
        TableClient->CreateSession().Subscribe(
            [actorSystem = NActors::TActivationContext::ActorSystem(),
             self = SelfId()](const TAsyncCreateSessionResult& future) {
                actorSystem->Send(
                    self, new TEvPrivate::TEvCreateSessionResponse(std::move(future)));
            });
    }

    void Handle(TEvPrivate::TEvCreateSessionResponse::TPtr& event) {
        auto createSessionResult = event->Get()->Result.GetValueSync();
        if (!createSessionResult.IsSuccess()) {
            CPP_LOG_E(
                "Couldn't create YDB session. Actor id: "
                << SelfId() << " Status: " << createSessionResult.GetStatus());

            NYql::TIssue issue =
                MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, "Couldn't create YDB session");
            for (auto& subIssue : createSessionResult.GetIssues()) {
                issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
            }
            SendErrorMessageToSender(std::move(issue));
            return;
        }

        auto session = createSessionResult.GetSession();
        auto query = CreateSchemaQuery(Event->Get()->Request);
        CPP_LOG_T(
            "Create external table in YDB. Actor id: " << SelfId() << " Query: " << query);
        session.ExecuteSchemeQuery(query).Subscribe(
            [actorSystem = NActors::TActivationContext::ActorSystem(),
             self = SelfId()](const TAsyncStatus& future) {
                actorSystem->Send(
                    self, new TEvPrivate::TEvCreateBindingExecutionResponse(future));
            });
    }

    NYql::TIssues ValidateRequest(const FederatedQuery::CreateBindingRequest& request) {
        NYql::TIssues issues;
        if (request.content().setting().binding_case() !=
            FederatedQuery::BindingSetting::BindingCase::kObjectStorage) {
            issues.AddIssue(
                MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, "Unsupported binding type"));
        }
        if (request.content().setting().object_storage().subset().size() != 1) {
            issues.AddIssue(MakeErrorIssue(
                TIssuesIds::INTERNAL_ERROR,
                "Cannot create external table due to wrong amount of subsets in request"));
        }
        if (request.content().name().Contains('/')) {
            issues.AddIssue(MakeErrorIssue(
                TIssuesIds::INTERNAL_ERROR, "'/' is not allowed in binding name"));
        }
        return issues;
    }

    TString CreateSchemaQuery(const FederatedQuery::CreateBindingRequest& request) {
        using namespace fmt::literals;

        auto bindingName = request.content().name();
        auto objectStorageParams = request.content().setting().object_storage();
        const auto& subset = objectStorageParams.subset(0);

        // Schema
        auto columnsTransformFunction = [](const Ydb::Column& column) -> TString {
            return fmt::format(
                "    {columnName} {columnType}",
                "columnName"_a = EncloseAndEscapeString(column.name(), '`'),
                "columnType"_a =
                    NYdb::TType{column.type()}
                        .ToString()); // TODO: check if this conversion could lead to valnurability
        };
        auto columnsBegin =
            MakeMappedIterator(subset.schema().column().begin(), columnsTransformFunction);
        auto columnsEnd =
            MakeMappedIterator(subset.schema().column().end(), columnsTransformFunction);

        // WithOptiuons
        auto withOptions = std::unordered_map<TString, TString>{};
        withOptions.insert(
            {"DATA_SOURCE", TStringBuilder{} << '"' << ConnectionName << '"'});
        withOptions.insert({"LOCATION", EncloseAndEscapeString(subset.path_pattern(), '"')});
        if (!subset.format().Empty()) {
            withOptions.insert({"FORMAT", EncloseAndEscapeString(subset.format(), '"')});
        }
        if (!subset.compression().Empty()) {
            withOptions.insert(
                {"COMPRESSION", EncloseAndEscapeString(subset.compression(), '"')});
        }
        for (auto& kv : subset.format_setting()) {
            withOptions.insert(
                {EncloseAndEscapeString(kv.first, '"'),
                 EncloseAndEscapeString(kv.second, '"')});
        };

        if (!subset.partitioned_by().empty()) {
            auto stringEscapeMapper = [](const TString& value) {
                return EncloseAndEscapeString(value, '"');
            };

            auto partitionBy =
                TStringBuilder{}
                << '['
                << JoinRange(
                       ", ",
                       MakeMappedIterator(subset.partitioned_by().begin(), stringEscapeMapper),
                       MakeMappedIterator(subset.partitioned_by().end(), stringEscapeMapper))
                << ']';
            withOptions.insert({"PARTITIONED_BY", partitionBy});
        }

        if (!subset.projection().empty()) {
            auto keyValueToStringMapper =
                [](const std::pair<TString, TString>& kv) -> TString {
                return fmt::format(
                    "   {propertyName}={propertyValue}",
                    "propertyName"_a = EncloseAndEscapeString(kv.first, '"'),
                    "propertyValue"_a = EncloseAndEscapeString(kv.second, '"'));
            };

            auto projection =
                TStringBuilder{}
                << "{\n"
                << JoinRange(
                       ",\n",
                       MakeMappedIterator(subset.projection().begin(), keyValueToStringMapper),
                       MakeMappedIterator(subset.projection().end(), keyValueToStringMapper))
                << "\n}";
            withOptions.insert({"PROJECTION", projection});
        }

        auto concatEscapedKeyValueMapper =
            [](const std::pair<TString, TString>& kv) -> TString {
            return TStringBuilder{} << "   " << kv.first << " = " << kv.second;
        };

        auto withOptionsBegin =
            MakeMappedIterator(withOptions.begin(), concatEscapedKeyValueMapper);
        auto withOptionsEnd =
            MakeMappedIterator(withOptions.end(), concatEscapedKeyValueMapper);

        return fmt::format(
            R"(
                CREATE EXTERNAL TABLE {externalTableName} (
                    {columns}
                ) WITH (
                    {withOptions}
                );)",
            "externalTableName"_a = EncloseAndEscapeString(bindingName, '`'),
            "columns"_a = JoinRange(",\n", columnsBegin, columnsEnd),
            "withOptions"_a = JoinRange(",\n", withOptionsBegin, withOptionsEnd));
    }

    void Handle(TEvPrivate::TEvCreateBindingExecutionResponse::TPtr& event) {
        Counters->InFly->Dec();
        Counters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());

        const auto& status = event->Get()->Result.GetValueSync();
        if (!status.IsSuccess()) {
            TString errorMessage;
            if (status.GetStatus() == NYdb::EStatus::ALREADY_EXISTS) {
                errorMessage = "External table with such name already exists";
            } else {
                errorMessage = TStringBuilder{}
                               << "Couldn't create external table in YDB. Status: "
                               << status.GetStatus();
            }

            CPP_LOG_E(errorMessage);

            NYql::TIssue issue = MakeErrorIssue(
                TIssuesIds::INTERNAL_ERROR, "Couldn't create external table in YDB");
            for (auto& subIssue : status.GetIssues()) {
                issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(subIssue));
            }
            SendErrorMessageToSender(std::move(issue));
            return;
        }

        Counters->Ok->Inc();
        Event->Get()->ComputeYDBOperationWasPerformed = true;
        CPP_LOG_T("External table in YDB was successfully created");

        TActivationContext::Send(Event->Forward(ControlPlaneProxyActorId()));
        PassAway();
    }

    STRICT_STFUNC(StateFunc, cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
                  hFunc(TEvControlPlaneStorage::TEvDescribeConnectionResponse, Handle);
                  hFunc(TEvPrivate::TEvCreateSessionResponse, Handle);
                  hFunc(TEvPrivate::TEvCreateBindingExecutionResponse, Handle);)

    void HandleTimeout() {
        CPP_LOG_D("Create external table in YDB timeout. Actor id: " << SelfId());
        Counters->Timeout->Inc();
        SendErrorMessageToSender(MakeErrorIssue(
            TIssuesIds::TIMEOUT,
            "Create external table in YDB timeout. Try repeating the request later"));
    }

    void SendErrorMessageToSender(NYql::TIssue issue) {
        Counters->Error->Inc();
        NYql::TIssues issues;
        issues.AddIssue(issue);
        Send(Sender, new TResponseProxy(issues, {}), 0,
             Cookie); // Change to template
        PassAway();
    }

private:
    TTableClientPtr CreateNewTableClient(
        const NFq::TComputeConfig& computeConfig,
        const TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) {

        NFq::NConfig::TYdbStorageConfig computeConnection = computeConfig.GetConnection(Scope);
        computeConnection.set_endpoint(Event->Get()->ComputeDatabase->connection().endpoint());
        computeConnection.set_database(Event->Get()->ComputeDatabase->connection().database());
        computeConnection.set_usessl(Event->Get()->ComputeDatabase->connection().usessl());

        auto tableSettings = GetClientSettings<NYdb::NTable::TClientSettings>(
            computeConnection, credentialsProviderFactory);
        return std::make_unique<NYdb::NTable::TTableClient>(
            yqSharedResources->UserSpaceYdbDriver, tableSettings);
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
                        const TString& folderId, const TString& token,
                        const std::function<void(const TDuration&, bool, bool)>& probe,
                        TEventRequest event,
                        ui32 cookie, bool quotaManagerEnabled)
        : Config(config)
        , Sender(sender)
        , Counters(counters)
        , FolderId(folderId)
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
        CPP_LOG_D("Resolve folder timeout. Folder id: " << FolderId << " Actor id: " << SelfId());
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
        Counters->InFly->Dec();
        Counters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
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
            Counters->Error->Inc();
            CPP_LOG_E(errorMessage);
            NYql::TIssues issues;
            NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, "Resolve folder error");
            issues.AddIssue(issue);
            const TDuration delta = TInstant::Now() - StartTime;
            Probe(delta, false, false);
            Send(Sender, new TResponseProxy(issues, {}), 0, Cookie);
            PassAway();
            return;
        }

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
    TString FolderId;
    TString Scope;
    TString Token;
    std::function<void(const TDuration&, bool, bool)> Probe;
    TEventRequest Event;
    ui32 Cookie;
    TInstant StartTime;

public:
    TCreateComputeDatabaseActor(const TRequestCommonCountersPtr& counters,
                         TActorId sender, const ::NFq::TControlPlaneProxyConfig& config,
                         const ::NFq::TComputeConfig& computeConfig, const TString& cloudId,
                         const TString& folderId, const TString& scope,
                         const std::function<void(const TDuration&, bool, bool)>& probe,
                         TEventRequest event, ui32 cookie)
        : Config(config)
        , ComputeConfig(computeConfig)
        , Sender(sender)
        , Counters(counters)
        , CloudId(cloudId)
        , FolderId(folderId)
        , Scope(scope)
        , Probe(probe)
        , Event(event)
        , Cookie(cookie)
        , StartTime(TInstant::Now())
    {}

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_CREATE_DATABASE";

    void Bootstrap() {
        CPP_LOG_T("Create database bootstrap. CloudId: " << CloudId << " FolderId: " << FolderId << " Scope: " << Scope << " Actor id: " << SelfId());
        if (!ComputeConfig.YdbComputeControlPlaneEnabled()) {
            Event->Get()->ComputeDatabase = FederatedQuery::Internal::ComputeDatabaseInternal{};
            TActivationContext::Send(Event->Forward(ControlPlaneProxyActorId()));
            PassAway();
            return;
        }
        Become(&TCreateComputeDatabaseActor::StateFunc, Config.RequestTimeout, new NActors::TEvents::TEvWakeup());
        Counters->InFly->Inc();
        Send(NFq::ComputeDatabaseControlPlaneServiceActorId(), CreateRequest().release(), 0, 0);
    }

    std::unique_ptr<TEvYdbCompute::TEvCreateDatabaseRequest> CreateRequest() {
        return std::make_unique<TEvYdbCompute::TEvCreateDatabaseRequest>(CloudId, Scope);
    }

    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        hFunc(TEvYdbCompute::TEvCreateDatabaseResponse, Handle);
    )

    void HandleTimeout() {
        CPP_LOG_D("Create database timeout. CloudId: " << CloudId << " FolderId: " << FolderId << " Scope: " << Scope << " Actor id: " << SelfId());
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

template<class TRequestProto, class TRequest, class TResponse, class TResponseProxy>
class TRequestActor : public NActors::TActorBootstrapped<TRequestActor<TRequestProto, TRequest, TResponse, TResponseProxy>> {
protected:
    using TBase = NActors::TActorBootstrapped<TRequestActor<TRequestProto, TRequest, TResponse, TResponseProxy>>;
    using TBase::SelfId;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Become;
    using TBase::Schedule;

    ::NFq::TControlPlaneProxyConfig Config;
    TRequestProto RequestProto;
    TString Scope;
    TString FolderId;
    TString User;
    TString Token;
    TActorId Sender;
    ui32 Cookie;
    TActorId ServiceId;
    TRequestCounters Counters;
    TInstant StartTime;
    std::function<void(const TDuration&, bool /* isSuccess */, bool /* isTimeout */)> Probe;
    TPermissions Permissions;
    TString CloudId;
    TString SubjectType;
    const TMaybe<TQuotaMap> Quotas;
    TTenantInfo::TPtr TenantInfo;
    TMaybe<FederatedQuery::Internal::ComputeDatabaseInternal> ComputeDatabase;
    ui32 RetryCount = 0;

public:
    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_REQUEST_ACTOR";

    explicit TRequestActor(const ::NFq::TControlPlaneProxyConfig& config,
                           TActorId sender, ui32 cookie,
                           const TString& scope, const TString& folderId, TRequestProto&& requestProto,
                           TString&& user, TString&& token, const TActorId& serviceId,
                           const TRequestCounters& counters,
                           const std::function<void(const TDuration&, bool, bool)>& probe,
                           TPermissions permissions,
                           const TString& cloudId, const TString& subjectType, TMaybe<TQuotaMap>&& quotas = Nothing(),
                           TMaybe<FederatedQuery::Internal::ComputeDatabaseInternal>&& computeDatabase = Nothing())
        : Config(config)
        , RequestProto(std::forward<TRequestProto>(requestProto))
        , Scope(scope)
        , FolderId(folderId)
        , User(std::move(user))
        , Token(std::move(token))
        , Sender(sender)
        , Cookie(cookie)
        , ServiceId(serviceId)
        , Counters(counters)
        , StartTime(TInstant::Now())
        , Probe(probe)
        , Permissions(permissions)
        , CloudId(cloudId)
        , SubjectType(subjectType)
        , Quotas(std::move(quotas))
        , ComputeDatabase(std::move(computeDatabase))
    {
        Counters.IncInFly();
    }

public:

    void Bootstrap() {
        CPP_LOG_T("Request actor. Actor id: " << SelfId());
        Become(&TRequestActor::StateFunc, Config.RequestTimeout, new NActors::TEvents::TEvWakeup());
        Send(ControlPlaneConfigActorId(), new TEvControlPlaneConfig::TEvGetTenantInfoRequest());
        OnBootstrap();
    }

    virtual void OnBootstrap() {}

    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        hFunc(TResponse, Handle);
        cFunc(TEvControlPlaneConfig::EvGetTenantInfoRequest, HandleRetry);
        hFunc(TEvControlPlaneConfig::TEvGetTenantInfoResponse, Handle);
    )

    void HandleRetry() {
        Send(ControlPlaneConfigActorId(), new TEvControlPlaneConfig::TEvGetTenantInfoRequest());
    }

    void Handle(TEvControlPlaneConfig::TEvGetTenantInfoResponse::TPtr& ev) {
        TenantInfo = std::move(ev->Get()->TenantInfo);
        if (TenantInfo) {
            SendRequestIfCan();
        } else {
            RetryCount++;
            Schedule(Now() + Config.ConfigRetryPeriod * (1 << RetryCount), new TEvControlPlaneConfig::TEvGetTenantInfoRequest());
        }
    }

    void HandleTimeout() {
        CPP_LOG_D("Request timeout. " << RequestProto.DebugString());
        NYql::TIssues issues;
        NYql::TIssue issue = MakeErrorIssue(TIssuesIds::TIMEOUT, "Request timeout. Try repeating the request later");
        issues.AddIssue(issue);
        Counters.IncTimeout();
        ReplyWithError(issues, true);
    }

    void Handle(typename TResponse::TPtr& ev) {
        auto& response = *ev->Get();
        ProcessResponse(response);
    }

    template<typename T>
    void ProcessResponse(const T& response) {
        if (response.Issues) {
            ReplyWithError(response.Issues);
        } else {
            ReplyWithSuccess(response.Result);
        }
    }

    template<typename T> requires requires (T t) { t.AuditDetails; }
    void ProcessResponse(const T& response) {
        if (response.Issues) {
            ReplyWithError(response.Issues);
        } else {
            ReplyWithSuccess(response.Result, response.AuditDetails);
        }
    }

    void ReplyWithError(const NYql::TIssues& issues, bool isTimeout = false) {
        const TDuration delta = TInstant::Now() - StartTime;
        Counters.IncError();
        Probe(delta, false, isTimeout);
        Send(Sender, new TResponseProxy(issues, SubjectType), 0, Cookie);
        PassAway();
    }

    template <class... TArgs>
    void ReplyWithSuccess(TArgs&&... args) {
        const TDuration delta = TInstant::Now() - StartTime;
        Counters.IncOk();
        Probe(delta, true, false);
        Send(Sender, new TResponseProxy(std::forward<TArgs>(args)..., SubjectType), 0, Cookie);
        PassAway();
    }

    virtual bool CanSendRequest() const {
        return bool(TenantInfo);
    }

    void SendRequestIfCan() {
        if (CanSendRequest()) {
            Send(ServiceId, new TRequest(Scope, RequestProto, User, Token, CloudId, Permissions, Quotas, TenantInfo, ComputeDatabase.GetOrElse({})), 0, Cookie);
        }
    }

    virtual ~TRequestActor() {
        Counters.DecInFly();
        Counters.Common->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
    }
};

class TCreateQueryRequestActor : public TRequestActor<FederatedQuery::CreateQueryRequest,
                                                      TEvControlPlaneStorage::TEvCreateQueryRequest,
                                                      TEvControlPlaneStorage::TEvCreateQueryResponse,
                                                      TEvControlPlaneProxy::TEvCreateQueryResponse>
{
    bool QuoterResourceCreated = false;
public:
    using TBaseRequestActor = TRequestActor<FederatedQuery::CreateQueryRequest,
                                            TEvControlPlaneStorage::TEvCreateQueryRequest,
                                            TEvControlPlaneStorage::TEvCreateQueryResponse,
                                            TEvControlPlaneProxy::TEvCreateQueryResponse>;
    using TBaseRequestActor::TBaseRequestActor;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRateLimiter::TEvCreateResourceResponse, Handle);
        default:
            return TBaseRequestActor::StateFunc(ev);
        }
    }

    void OnBootstrap() override {
        Become(&TCreateQueryRequestActor::StateFunc);
        if (Quotas) {
            SendCreateRateLimiterResourceRequest();
        } else {
            SendRequestIfCan();
        }
    }

    void SendCreateRateLimiterResourceRequest() {
        if (auto quotaIt = Quotas->find(QUOTA_CPU_PERCENT_LIMIT); quotaIt != Quotas->end()) {
            const double cloudLimit = static_cast<double>(quotaIt->second.Limit.Value * 10); // percent -> milliseconds
            CPP_LOG_T("Create rate limiter resource for cloud with limit " << cloudLimit << "ms");
            Send(RateLimiterControlPlaneServiceId(), new TEvRateLimiter::TEvCreateResource(CloudId, cloudLimit));
        } else {
            NYql::TIssues issues;
            NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, TStringBuilder() << "CPU quota for cloud \"" << CloudId << "\" was not found");
            issues.AddIssue(issue);
            CPP_LOG_W("Failed to get cpu quota for cloud " << CloudId);
            ReplyWithError(issues);
        }
    }

    void Handle(TEvRateLimiter::TEvCreateResourceResponse::TPtr& ev) {
        CPP_LOG_D("Create response from rate limiter service. Success: " << ev->Get()->Success);
        if (ev->Get()->Success) {
            QuoterResourceCreated = true;
            SendRequestIfCan();
        } else {
            NYql::TIssue issue("Failed to create rate limiter resource");
            for (const NYql::TIssue& i : ev->Get()->Issues) {
                issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
            }
            NYql::TIssues issues;
            issues.AddIssue(issue);
            ReplyWithError(issues);
        }
    }

    bool CanSendRequest() const override {
        return (QuoterResourceCreated || !Quotas) && TBaseRequestActor::CanSendRequest();
    }
};

class TControlPlaneProxyActor : public NActors::TActorBootstrapped<TControlPlaneProxyActor> {
    enum ERequestTypeScope {
        RTS_CREATE_QUERY,
        RTS_LIST_QUERIES,
        RTS_DESCRIBE_QUERY,
        RTS_GET_QUERY_STATUS,
        RTS_MODIFY_QUERY,
        RTS_DELETE_QUERY,
        RTS_CONTROL_QUERY,
        RTS_GET_RESULT_DATA,
        RTS_LIST_JOBS,
        RTS_DESCRIBE_JOB,
        RTS_CREATE_CONNECTION,
        RTS_LIST_CONNECTIONS,
        RTS_DESCRIBE_CONNECTION,
        RTS_MODIFY_CONNECTION,
        RTS_DELETE_CONNECTION,
        RTS_TEST_CONNECTION,
        RTS_CREATE_BINDING,
        RTS_LIST_BINDINGS,
        RTS_DESCRIBE_BINDING,
        RTS_MODIFY_BINDING,
        RTS_DELETE_BINDING,
        RTS_MAX,
    };

    enum ERequestTypeCommon {
        RTC_RESOLVE_FOLDER,
        RTC_CREATE_QUERY,
        RTC_LIST_QUERIES,
        RTC_DESCRIBE_QUERY,
        RTC_GET_QUERY_STATUS,
        RTC_MODIFY_QUERY,
        RTC_DELETE_QUERY,
        RTC_CONTROL_QUERY,
        RTC_GET_RESULT_DATA,
        RTC_LIST_JOBS,
        RTC_DESCRIBE_JOB,
        RTC_CREATE_CONNECTION,
        RTC_LIST_CONNECTIONS,
        RTC_DESCRIBE_CONNECTION,
        RTC_MODIFY_CONNECTION,
        RTC_DELETE_CONNECTION,
        RTC_TEST_CONNECTION,
        RTC_CREATE_BINDING,
        RTC_LIST_BINDINGS,
        RTC_DESCRIBE_BINDING,
        RTC_MODIFY_BINDING,
        RTC_DELETE_BINDING,
        RTC_RESOLVE_SUBJECT_TYPE,
        RTC_CREATE_CONNECTION_IN_YDB,
        RTC_CREATE_BINDING_IN_YDB,
        RTC_CREATE_COMPUTE_DATABASE,
        RTC_MAX,
    };

    class TCounters: public virtual TThrRefBase {
        struct TMetricsScope {
            TString CloudId;
            TString Scope;

            TMetricsScope() = default;

            TMetricsScope(const TString& cloudId, const TString& scope)
                : CloudId(cloudId)
                , Scope(scope)
            {}

            bool operator<(const TMetricsScope& right) const {
                return std::make_pair(CloudId, Scope) < std::make_pair(right.CloudId, right.Scope);
            }
        };

        using TScopeCounters = std::array<TRequestScopeCountersPtr, RTS_MAX>;
        using TScopeCountersPtr = std::shared_ptr<TScopeCounters>;

        std::array<TRequestCommonCountersPtr, RTC_MAX> CommonRequests = CreateArray<RTC_MAX, TRequestCommonCountersPtr>({
            { MakeIntrusive<TRequestCommonCounters>("ResolveFolder") },
            { MakeIntrusive<TRequestCommonCounters>("CreateQuery") },
            { MakeIntrusive<TRequestCommonCounters>("ListQueries") },
            { MakeIntrusive<TRequestCommonCounters>("DescribeQuery") },
            { MakeIntrusive<TRequestCommonCounters>("GetQueryStatus") },
            { MakeIntrusive<TRequestCommonCounters>("ModifyQuery") },
            { MakeIntrusive<TRequestCommonCounters>("DeleteQuery") },
            { MakeIntrusive<TRequestCommonCounters>("ControlQuery") },
            { MakeIntrusive<TRequestCommonCounters>("GetResultData") },
            { MakeIntrusive<TRequestCommonCounters>("ListJobs") },
            { MakeIntrusive<TRequestCommonCounters>("DescribeJob") },
            { MakeIntrusive<TRequestCommonCounters>("CreateConnection") },
            { MakeIntrusive<TRequestCommonCounters>("ListConnections") },
            { MakeIntrusive<TRequestCommonCounters>("DescribeConnection") },
            { MakeIntrusive<TRequestCommonCounters>("ModifyConnection") },
            { MakeIntrusive<TRequestCommonCounters>("DeleteConnection") },
            { MakeIntrusive<TRequestCommonCounters>("TestConnection") },
            { MakeIntrusive<TRequestCommonCounters>("CreateBinding") },
            { MakeIntrusive<TRequestCommonCounters>("ListBindings") },
            { MakeIntrusive<TRequestCommonCounters>("DescribeBinding") },
            { MakeIntrusive<TRequestCommonCounters>("ModifyBinding") },
            { MakeIntrusive<TRequestCommonCounters>("DeleteBinding") },
            { MakeIntrusive<TRequestCommonCounters>("ResolveSubjectType") },
            { MakeIntrusive<TRequestCommonCounters>("CreateConnectionInYDB") },
            { MakeIntrusive<TRequestCommonCounters>("CreateBindingInYDB") },
            { MakeIntrusive<TRequestCommonCounters>("CreateComputeDatabase") },
        });

        TTtlCache<TMetricsScope, TScopeCountersPtr, TMap> ScopeCounters{TTtlCacheSettings{}.SetTtl(TDuration::Days(1))};
        ::NMonitoring::TDynamicCounterPtr Counters;

    public:
        explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
        {
            for (auto& request: CommonRequests) {
                request->Register(Counters);
            }
        }

        TRequestCounters GetCounters(const TString& cloudId, const TString& scope, ERequestTypeScope scopeType, ERequestTypeCommon commonType) {
            return {GetScopeCounters(cloudId, scope, scopeType), GetCommonCounters(commonType)};
        }

        TRequestCommonCountersPtr GetCommonCounters(ERequestTypeCommon type) {
            return CommonRequests[type];
        }

        TRequestScopeCountersPtr GetScopeCounters(const TString& cloudId, const TString& scope, ERequestTypeScope type) {
            TMetricsScope key{cloudId, scope};
            TMaybe<TScopeCountersPtr> cacheVal;
            ScopeCounters.Get(key, &cacheVal);
            if (cacheVal) {
                return (**cacheVal)[type];
            }

            auto scopeRequests = std::make_shared<TScopeCounters>(CreateArray<RTS_MAX, TRequestScopeCountersPtr>({
                { MakeIntrusive<TRequestScopeCounters>("CreateQuery") },
                { MakeIntrusive<TRequestScopeCounters>("ListQueries") },
                { MakeIntrusive<TRequestScopeCounters>("DescribeQuery") },
                { MakeIntrusive<TRequestScopeCounters>("GetQueryStatus") },
                { MakeIntrusive<TRequestScopeCounters>("ModifyQuery") },
                { MakeIntrusive<TRequestScopeCounters>("DeleteQuery") },
                { MakeIntrusive<TRequestScopeCounters>("ControlQuery") },
                { MakeIntrusive<TRequestScopeCounters>("GetResultData") },
                { MakeIntrusive<TRequestScopeCounters>("ListJobs") },
                { MakeIntrusive<TRequestScopeCounters>("DescribeJob") },
                { MakeIntrusive<TRequestScopeCounters>("CreateConnection") },
                { MakeIntrusive<TRequestScopeCounters>("ListConnections") },
                { MakeIntrusive<TRequestScopeCounters>("DescribeConnection") },
                { MakeIntrusive<TRequestScopeCounters>("ModifyConnection") },
                { MakeIntrusive<TRequestScopeCounters>("DeleteConnection") },
                { MakeIntrusive<TRequestScopeCounters>("TestConnection") },
                { MakeIntrusive<TRequestScopeCounters>("CreateBinding") },
                { MakeIntrusive<TRequestScopeCounters>("ListBindings") },
                { MakeIntrusive<TRequestScopeCounters>("DescribeBinding") },
                { MakeIntrusive<TRequestScopeCounters>("ModifyBinding") },
                { MakeIntrusive<TRequestScopeCounters>("DeleteBinding") },
            }));

            auto scopeCounters = Counters
                        ->GetSubgroup("cloud_id", cloudId)
                        ->GetSubgroup("scope", scope);

            for (auto& request: *scopeRequests) {
                request->Register(scopeCounters);
            }
            cacheVal = scopeRequests;
            ScopeCounters.Put(key, cacheVal);
            return (*scopeRequests)[type];
        }
    };

    TCounters Counters;
    const ::NFq::TControlPlaneProxyConfig Config;
    const TYqSharedResources::TPtr YqSharedResources;
    const NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    const bool QuotaManagerEnabled;
    NConfig::TComputeConfig ComputeConfig;
    TActorId AccessService;

public:
    TControlPlaneProxyActor(
        const NConfig::TControlPlaneProxyConfig& config,
        const NConfig::TComputeConfig& computeConfig,
        const NConfig::TCommonConfig& commonConfig,
        const TYqSharedResources::TPtr& yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        bool quotaManagerEnabled)
        : Counters(counters)
        , Config(config, computeConfig, commonConfig)
        , YqSharedResources(yqSharedResources)
        , CredentialsProviderFactory(credentialsProviderFactory)
        , QuotaManagerEnabled(quotaManagerEnabled) { }

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

    inline static const TMap<TString, TPermissions::TPermission> PermissionsItems = {
        {"yq.resources.viewPublic@as", TPermissions::VIEW_PUBLIC},
        {"yq.resources.viewPrivate@as", TPermissions::VIEW_PRIVATE},
        {"yq.queries.viewAst@as", TPermissions::VIEW_AST},
        {"yq.resources.managePublic@as", TPermissions::MANAGE_PUBLIC},
        {"yq.resources.managePrivate@as", TPermissions::MANAGE_PRIVATE},
        {"yq.queries.invoke@as", TPermissions::QUERY_INVOKE},
        {"yq.queries.viewQueryText@as", TPermissions::VIEW_QUERY_TEXT},
    };

    template<typename T>
    TPermissions ExtractPermissions(T& ev, const TPermissions& availablePermissions) {
        TPermissions permissions;
        for (const auto& permission: ev->Get()->Permissions) {
            if (auto it = PermissionsItems.find(permission); it != PermissionsItems.end()) {
                // cut off permissions that should not be used in other services
                if (availablePermissions.Check(it->second)) {
                    permissions.Set(it->second);
                }
            }
        }
        return permissions;
    }

    template<typename T>
    NYql::TIssues ValidatePermissions(T& ev, const TVector<TString>& requiredPermissions) {
        NYql::TIssues issues;
        if (!Config.Proto.GetEnablePermissions()) {
            return issues;
        }

        for (const auto& requiredPermission: requiredPermissions) {
            if (!IsIn(ev->Get()->Permissions, requiredPermission)) {
                issues.AddIssue(MakeErrorIssue(TIssuesIds::ACCESS_DENIED, "No permission " + requiredPermission + " in a given scope yandexcloud://" + ev->Get()->FolderId));
            }
        }

        return issues;
    }

    void Handle(TEvControlPlaneProxy::TEvCreateQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::CreateQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("CreateQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                                 folderId, scope, probe, ev, cookie));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::QUERY_INVOKE
            | TPermissions::TPermission::MANAGE_PUBLIC
        };

        Register(new TCreateQueryRequestActor
                                            (Config, sender, cookie, scope, folderId,
                                            std::move(request), std::move(user), std::move(token),
                                            ControlPlaneStorageServiceActorId(),
                                            requestCounters,
                                            probe, ExtractPermissions(ev, availablePermissions),
                                            cloudId, subjectType, std::move(ev->Get()->Quotas),
                                            std::move(ev->Get()->ComputeDatabase)));
    }

    void Handle(TEvControlPlaneProxy::TEvListQueriesRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ListQueriesRequest request = ev->Get()->Request;
        CPP_LOG_T("ListQueriesRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                   TEvControlPlaneProxy::TEvListQueriesResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId, subjectType));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DescribeQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("DescribeQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                   TEvControlPlaneProxy::TEvDescribeQueryResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId, subjectType));
    }

    void Handle(TEvControlPlaneProxy::TEvGetQueryStatusRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::GetQueryStatusRequest request = ev->Get()->Request;
        CPP_LOG_T("GetStatusRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                   TEvControlPlaneProxy::TEvGetQueryStatusResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId, subjectType));
    }

    void Handle(TEvControlPlaneProxy::TEvModifyQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ModifyQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("ModifyQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                                 folderId, scope, probe, ev, cookie));
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
                                   TEvControlPlaneProxy::TEvModifyQueryResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe, ExtractPermissions(ev, availablePermissions),
                                    cloudId, subjectType, {}, std::move(ev->Get()->ComputeDatabase)));
    }

    void Handle(TEvControlPlaneProxy::TEvDeleteQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DeleteQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("DeleteQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                   TEvControlPlaneProxy::TEvDeleteQueryResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId, subjectType));
    }

    void Handle(TEvControlPlaneProxy::TEvControlQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ControlQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("ControlQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                   TEvControlPlaneProxy::TEvControlQueryResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId, subjectType));
    }

    void Handle(TEvControlPlaneProxy::TEvGetResultDataRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::GetResultDataRequest request = ev->Get()->Request;
        CPP_LOG_T("GetResultDataRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                   TEvControlPlaneProxy::TEvGetResultDataResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId, subjectType));
    }

    void Handle(TEvControlPlaneProxy::TEvListJobsRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ListJobsRequest request = ev->Get()->Request;
        CPP_LOG_T("ListJobsRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                   TEvControlPlaneProxy::TEvListJobsResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId, subjectType));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeJobRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DescribeJobRequest request = ev->Get()->Request;
        CPP_LOG_T("DescribeJobRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                   TEvControlPlaneProxy::TEvDescribeJobResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId, subjectType));
    }

    void Handle(TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::CreateConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("CreateConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const bool ydbOperationWasPerformed = ev->Get()->ComputeYDBOperationWasPerformed;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                                 folderId, scope, probe, ev, cookie));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
        };

        if (Config.ComputeConfig.YdbComputeControlPlaneEnabled() && !ydbOperationWasPerformed) {
            Register(new TCreateConnectionInYDBActor(
                Counters.GetCommonCounters(RTC_CREATE_CONNECTION_IN_YDB),
                Config.ComputeConfig,
                YqSharedResources,
                CredentialsProviderFactory,
                Config.CommonConfig.GetObjectStorageEndpoint(),
                sender,
                ev,
                cookie,
                scope,
                Config.RequestTimeout));
            return;
        }

        Register(new TRequestActor<FederatedQuery::CreateConnectionRequest,
                                   TEvControlPlaneStorage::TEvCreateConnectionRequest,
                                   TEvControlPlaneStorage::TEvCreateConnectionResponse,
                                   TEvControlPlaneProxy::TEvCreateConnectionResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe, ExtractPermissions(ev, availablePermissions),
                                    cloudId, subjectType, {}, std::move(ev->Get()->ComputeDatabase)));
    }

    void Handle(TEvControlPlaneProxy::TEvListConnectionsRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ListConnectionsRequest request = ev->Get()->Request;
        CPP_LOG_T("ListConnectionsRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                   TEvControlPlaneProxy::TEvListConnectionsResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId, subjectType));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DescribeConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("DescribeConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                   TEvControlPlaneProxy::TEvDescribeConnectionResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId, subjectType));
    }

    void Handle(TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ModifyConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("ModifyConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                                 folderId, scope, probe, ev, cookie));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::ModifyConnectionRequest,
                                   TEvControlPlaneStorage::TEvModifyConnectionRequest,
                                   TEvControlPlaneStorage::TEvModifyConnectionResponse,
                                   TEvControlPlaneProxy::TEvModifyConnectionResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions),
                                    cloudId, subjectType, {}, std::move(ev->Get()->ComputeDatabase)));
    }

    void Handle(TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DeleteConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("DeleteConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                                 folderId, scope, probe, ev, cookie));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::DeleteConnectionRequest,
                                   TEvControlPlaneStorage::TEvDeleteConnectionRequest,
                                   TEvControlPlaneStorage::TEvDeleteConnectionResponse,
                                   TEvControlPlaneProxy::TEvDeleteConnectionResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions),
                                    cloudId, subjectType, {}, std::move(ev->Get()->ComputeDatabase)));
    }

    void Handle(TEvControlPlaneProxy::TEvTestConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::TestConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("TestConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                   TEvControlPlaneProxy::TEvTestConnectionResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    TestConnectionActorId(),
                                    requestCounters,
                                    probe, ExtractPermissions(ev, {}), cloudId, subjectType));
    }

    void Handle(TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::CreateBindingRequest request = ev->Get()->Request;
        CPP_LOG_T("CreateBindingRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const bool ydbOperationWasPerformed = ev->Get()->ComputeYDBOperationWasPerformed;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                                 folderId, scope, probe, ev, cookie));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        if (Config.ComputeConfig.YdbComputeControlPlaneEnabled() && !ydbOperationWasPerformed) {
            auto permissions = ExtractPermissions(ev, availablePermissions);
            Register(new TCreateBindingInYDBActor(
                Counters.GetCommonCounters(RTC_CREATE_BINDING_IN_YDB),
                Config.ComputeConfig,
                YqSharedResources,
                CredentialsProviderFactory,
                sender,
                ev,
                cookie,
                std::move(permissions),
                scope,
                Config.RequestTimeout));
            return;
        }

        Register(new TRequestActor<FederatedQuery::CreateBindingRequest,
                                   TEvControlPlaneStorage::TEvCreateBindingRequest,
                                   TEvControlPlaneStorage::TEvCreateBindingResponse,
                                   TEvControlPlaneProxy::TEvCreateBindingResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe, ExtractPermissions(ev, availablePermissions),
                                    cloudId, subjectType, {}, std::move(ev->Get()->ComputeDatabase)));
    }

    void Handle(TEvControlPlaneProxy::TEvListBindingsRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ListBindingsRequest request = ev->Get()->Request;
        CPP_LOG_T("ListBindingsRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                   TEvControlPlaneProxy::TEvListBindingsResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId, subjectType));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DescribeBindingRequest request = ev->Get()->Request;
        CPP_LOG_T("DescribeBindingRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                   TEvControlPlaneProxy::TEvDescribeBindingResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId, subjectType));
    }

    void Handle(TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::ModifyBindingRequest request = ev->Get()->Request;
        CPP_LOG_T("ModifyBindingRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                                 folderId, scope, probe, ev, cookie));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::ModifyBindingRequest,
                                   TEvControlPlaneStorage::TEvModifyBindingRequest,
                                   TEvControlPlaneStorage::TEvModifyBindingResponse,
                                   TEvControlPlaneProxy::TEvModifyBindingResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions),
                                    cloudId, subjectType, {}, std::move(ev->Get()->ComputeDatabase)));
    }

    void Handle(TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        FederatedQuery::DeleteBindingRequest request = ev->Get()->Request;
        CPP_LOG_T("DeleteBindingRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
        const TString subjectType = ev->Get()->SubjectType;
        const TString scope = "yandexcloud://" + folderId;
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
                                              Config, folderId, token,
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
                                                 folderId, scope, probe, ev, cookie));
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<FederatedQuery::DeleteBindingRequest,
                                   TEvControlPlaneStorage::TEvDeleteBindingRequest,
                                   TEvControlPlaneStorage::TEvDeleteBindingResponse,
                                   TEvControlPlaneProxy::TEvDeleteBindingResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions),
                                    cloudId, subjectType, {}, std::move(ev->Get()->ComputeDatabase)));
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
    const NConfig::TComputeConfig& computeConfig,
    const NConfig::TCommonConfig& commonConfig,
    const TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    bool quotaManagerEnabled) {
    return new TControlPlaneProxyActor(
        config,
        computeConfig,
        commonConfig,
        yqSharedResources,
        credentialsProviderFactory,
        counters,
        quotaManagerEnabled);
}

}  // namespace NFq
