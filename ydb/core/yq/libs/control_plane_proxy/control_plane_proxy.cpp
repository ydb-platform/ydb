#include "control_plane_proxy.h"
#include "probes.h"
#include "utils.h"

#include <ydb/core/yq/libs/actors/logging/log.h>
#include <ydb/core/yq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <ydb/core/yq/libs/control_plane_storage/util.h>
#include <ydb/core/yq/libs/quota_manager/quota_manager.h>
#include <ydb/core/yq/libs/test_connection/test_connection.h>
#include <ydb/core/yq/libs/test_connection/events/events.h>
#include <ydb/core/yq/libs/ydb/util.h>
#include <ydb/core/yq/libs/ydb/ydb.h>

#include <ydb/core/yq/libs/config/yq_issue.h>
#include <ydb/core/yq/libs/control_plane_proxy/events/events.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actor.h>

#include <ydb/core/base/kikimr_issue.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <ydb/library/security/util.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/folder_service/events.h>


namespace NYq {
namespace {

using namespace NActors;
using namespace NYq::NConfig;
using namespace NKikimr;
using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

LWTRACE_USING(YQ_CONTROL_PLANE_PROXY_PROVIDER);

struct TRequestScopeCounters: public virtual TThrRefBase {
    const TString Name;

    ::NMonitoring::TDynamicCounters::TCounterPtr InFly;
    ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
    ::NMonitoring::TDynamicCounters::TCounterPtr Error;
    ::NMonitoring::TDynamicCounters::TCounterPtr Timeout;

    explicit TRequestScopeCounters(const TString& name)
        : Name(name)
    { }

    void Register(const ::NMonitoring::TDynamicCounterPtr& counters) {
        ::NMonitoring::TDynamicCounterPtr subgroup = counters->GetSubgroup("request_scope", Name);
        InFly = subgroup->GetCounter("InFly", false);
        Ok = subgroup->GetCounter("Ok", true);
        Error = subgroup->GetCounter("Error", true);
        Timeout = subgroup->GetCounter("Timeout", true);
    }

private:
    static ::NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets() {
        return ::NMonitoring::ExplicitHistogram({0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 50000, 500000});
    }
};

struct TRequestCommonCounters: public virtual TThrRefBase {
    const TString Name;

    ::NMonitoring::TDynamicCounters::TCounterPtr InFly;
    ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
    ::NMonitoring::TDynamicCounters::TCounterPtr Error;
    ::NMonitoring::TDynamicCounters::TCounterPtr Timeout;
    ::NMonitoring::THistogramPtr LatencyMs;

    explicit TRequestCommonCounters(const TString& name)
        : Name(name)
    { }

    void Register(const ::NMonitoring::TDynamicCounterPtr& counters) {
        ::NMonitoring::TDynamicCounterPtr subgroup = counters->GetSubgroup("request_common", Name);
        InFly = subgroup->GetCounter("InFly", false);
        Ok = subgroup->GetCounter("Ok", true);
        Error = subgroup->GetCounter("Error", true);
        Timeout = subgroup->GetCounter("Timeout", true);
        LatencyMs = subgroup->GetHistogram("LatencyMs", GetLatencyHistogramBuckets());
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
        Scope->InFly->Inc();
        Common->InFly->Inc();
    }

    void IncOk() {
        Scope->Ok->Inc();
        Common->Ok->Inc();
    }

    void DecOk() {
        Scope->Ok->Inc();
        Common->Ok->Inc();
    }

    void IncError() {
        Scope->Error->Inc();
        Common->Error->Inc();
    }

    void DecError() {
        Scope->Error->Inc();
        Common->Error->Inc();
    }

    void IncTimeout() {
        Scope->Timeout->Inc();
        Common->Timeout->Inc();
    }

    void DecTimeout() {
        Scope->Timeout->Inc();
        Common->Timeout->Inc();
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
        Event->Get()->Quotas = ev->Get()->Quotas;
        CPP_LOG_T("Cloud id: " << Event->Get()->CloudId << " Quota count: " << Event->Get()->Quotas.size());
        TActivationContext::Send(Event->Forward(ControlPlaneProxyActorId()));
        PassAway();
    }

    void HandleTimeout() {
        CPP_LOG_D("Quota request timeout. Cloud id: " << Event->Get()->CloudId << " Actor id: " << SelfId());
        Send(MakeQuotaServiceActorId(SelfId().NodeId()), new TEvQuotaService::TQuotaGetRequest(SUBJECT_TYPE_CLOUD, Event->Get()->CloudId, true));
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

    NConfig::TControlPlaneProxyConfig Config;
    TActorId Sender;
    TRequestCommonCountersPtr Counters;
    TString FolderId;
    TString Token;
    std::function<void(const TDuration&, bool, bool)> Probe;
    TEventRequest Event;
    ui32 Cookie;
    TInstant StartTime;
    bool GetQuotas;

public:
    TResolveFolderActor(const TRequestCommonCountersPtr& counters,
                        TActorId sender, const NConfig::TControlPlaneProxyConfig& config,
                        const TString& folderId, const TString& token,
                        const std::function<void(const TDuration&, bool, bool)>& probe,
                        TEventRequest event,
                        ui32 cookie, bool getQuotas)
        : Config(config)
        , Sender(sender)
        , Counters(counters)
        , FolderId(folderId)
        , Token(token)
        , Probe(probe)
        , Event(event)
        , Cookie(cookie)
        , StartTime(TInstant::Now())
        , GetQuotas(getQuotas)
    {}

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_RESOLVE_FOLDER";

    void Bootstrap() {
        CPP_LOG_T("Resolve folder bootstrap. Folder id: " << FolderId << " Actor id: " << SelfId());
        Become(&TResolveFolderActor::StateFunc, GetDuration(Config.GetRequestTimeout(), TDuration::Seconds(30)), new NActors::TEvents::TEvWakeup());
        auto request = std::make_unique<NKikimr::NFolderService::TEvFolderService::TEvGetFolderRequest>();
        request->Request.set_folder_id(FolderId);
        request->Token = Token;
        Counters->InFly->Inc();
        Send(NKikimr::NFolderService::FolderServiceActorId(), request.release(), 0, 0);
    }

    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        hFunc(NKikimr::NFolderService::TEvFolderService::TEvGetFolderResponse, Handle);
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
        Send(Sender, new TResponseProxy(issues), 0, Cookie);
        PassAway();
    }

    void Handle(NKikimr::NFolderService::TEvFolderService::TEvGetFolderResponse::TPtr& ev) {
        Counters->InFly->Dec();
        Counters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        const auto& response = ev->Get()->Response;
        const auto& status = ev->Get()->Status;
        if (!status.Ok() || !ev->Get()->Response.has_folder()) {
            Counters->Error->Inc();
            TString errorMessage = "Msg: " + status.Msg + " Details: " + status.Details + " Code: " + ToString(status.GRpcStatusCode) + " InternalError: " + ToString(status.InternalError);
            CPP_LOG_E(errorMessage);
            NYql::TIssues issues;
            NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, "Resolve folder error");
            issues.AddIssue(issue);
            Counters->Error->Inc();
            Counters->Timeout->Inc();
            const TDuration delta = TInstant::Now() - StartTime;
            Probe(delta, false, false);
            Send(Sender, new TResponseProxy(issues), 0, Cookie);
            PassAway();
            return;
        }

        Counters->Ok->Inc();
        TString cloudId = response.folder().cloud_id();
        Event->Get()->CloudId = cloudId;
        CPP_LOG_T("Cloud id: " << cloudId << " Folder id: " << FolderId);

        if (GetQuotas) {
            Register(new TGetQuotaActor<TEventRequest, TResponseProxy>(Sender, Event, Cookie));
        } else {
            TActivationContext::Send(Event->Forward(ControlPlaneProxyActorId()));
        }
        PassAway();
    }
};

template<class TRequestProto, class TRequest, class TResponse, class TResponseProxy>
class TRequestActor : public NActors::TActorBootstrapped<TRequestActor<TRequestProto, TRequest, TResponse, TResponseProxy>> {
    using TBase = NActors::TActorBootstrapped<TRequestActor<TRequestProto, TRequest, TResponse, TResponseProxy>>;
    using TBase::SelfId;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Become;

    NConfig::TControlPlaneProxyConfig Config;
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
    std::function<void(const TDuration&, bool, bool)> Probe;
    TPermissions Permissions;
    TString CloudId;
    TQuotaMap Quotas;

public:
    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_REQUEST_ACTOR";

    explicit TRequestActor(const NConfig::TControlPlaneProxyConfig& config,
                           TActorId sender, ui32 cookie,
                           const TString& scope, const TString& folderId, TRequestProto&& requestProto,
                           TString&& user, TString&& token, const TActorId& serviceId,
                           const TRequestCounters& counters,
                           const std::function<void(const TDuration&, bool, bool)>& probe,
                           TPermissions permissions,
                           const TString& cloudId, const TQuotaMap& quotas = {})
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
        , Quotas(quotas)
    {
        Counters.IncInFly();
        FillDefaultParameters(Config);
    }

public:

    void Bootstrap() {
        CPP_LOG_T("Request actor. Actor id: " << SelfId());
        Become(&TRequestActor::StateFunc, GetDuration(Config.GetRequestTimeout(), TDuration::Seconds(30)), new NActors::TEvents::TEvWakeup());
        Send(ServiceId, new TRequest(Scope, RequestProto, User, Token, CloudId, Permissions, Quotas), 0, Cookie);
    }

    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        hFunc(TResponse, Handle);
    )

    void HandleTimeout() {
        CPP_LOG_D("Request timeout. " << RequestProto.DebugString());
        NYql::TIssues issues;
        NYql::TIssue issue = MakeErrorIssue(TIssuesIds::TIMEOUT, "Request timeout. Try repeating the request later");
        issues.AddIssue(issue);
        Counters.IncError();
        Counters.IncTimeout();
        const TDuration delta = TInstant::Now() - StartTime;
        Probe(delta, false, true);
        Send(Sender, new TResponseProxy(issues), 0, Cookie);
        PassAway();
    }

    void Handle(typename TResponse::TPtr& ev) {
        const TDuration delta = TInstant::Now() - StartTime;
        auto& response = *ev->Get();
        ProcessResponse(delta, response);
    }

    template<typename T>
    void ProcessResponse(const TDuration& delta, const T& response) {
        if (response.Issues) {
            Counters.IncError();
            Probe(delta, false, false);
            Send(Sender, new TResponseProxy(response.Issues), 0, Cookie);
        } else {
            Counters.IncOk();
            Probe(delta, true, false);
            Send(Sender, new TResponseProxy(response.Result), 0, Cookie);
        }
        PassAway();
    }

    template<typename T> requires requires (T t) { t.AuditDetails; }
    void ProcessResponse(const TDuration& delta, const T& response) {
        if (response.Issues) {
            Counters.IncError();
            Probe(delta, false, false);
            Send(Sender, new TResponseProxy(response.Issues), 0, Cookie);
        } else {
            Counters.IncOk();
            Probe(delta, true, false);
            Send(Sender, new TResponseProxy(response.Result, response.AuditDetails), 0, Cookie);
        }
        PassAway();
    }

    virtual ~TRequestActor() {
        Counters.DecInFly();
        Counters.Common->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
    }

    TDuration GetDuration(const TString& value, const TDuration& defaultValue)
    {
        TDuration result = defaultValue;
        TDuration::TryParse(value, result);
        return result;
    }

    void FillDefaultParameters(NConfig::TControlPlaneProxyConfig& config)
    {
        if (!config.GetRequestTimeout()) {
            config.SetRequestTimeout("30s");
        }
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
        RTC_MAX,
    };

    class TCounters: public virtual TThrRefBase {
        struct TMetricsScope {
            TString CloudId;
            TString Scope;

            bool operator<(const TMetricsScope& right) const {
                return std::make_pair(CloudId, Scope) < std::make_pair(right.CloudId, right.Scope);
            }
        };

        TDuration MetricsTtl = TDuration::Days(1);

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
        });

        struct TScopeValue {
            TScopeCountersPtr Counters;
            TInstant LastAccess;
        };

        TMap<TMetricsScope, TScopeValue> ScopeCounters;
        TMap<TInstant, TSet<TMetricsScope>> LastAccess;
        ::NMonitoring::TDynamicCounterPtr Counters;

    public:
        explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
        {
            for (auto& request: CommonRequests) {
                request->Register(Counters);
            }
        }

        void CleanupByTtl() {
            auto now = TInstant::Now();
            auto it = LastAccess.begin();
            for (; it != LastAccess.end() && (now - it->first) > MetricsTtl; ++it) {
                for (const auto& scope: it->second) {
                    ScopeCounters.erase(scope);
                }
            }
            LastAccess.erase(LastAccess.begin(), it);
        }

        TRequestCounters GetCounters(const TString& cloudId, const TString& scope, ERequestTypeScope scopeType, ERequestTypeCommon commonType) {
            return {GetScopeCounters(cloudId, scope, scopeType), GetCommonCounters(commonType)};
        }

        TRequestCommonCountersPtr GetCommonCounters(ERequestTypeCommon type) {
            return CommonRequests[type];
        }

        TRequestScopeCountersPtr GetScopeCounters(const TString& cloudId, const TString& scope, ERequestTypeScope type) {
            CleanupByTtl();
            TMetricsScope key{cloudId, scope};
            auto it = ScopeCounters.find(key);
            if (it != ScopeCounters.end()) {
                auto& value = it->second;
                auto& l = LastAccess[value.LastAccess];
                l.erase(key);
                if (l.empty()) {
                    LastAccess.erase(value.LastAccess);
                }
                value.LastAccess = TInstant::Now();
                LastAccess[value.LastAccess].insert(key);
                return (*value.Counters)[type];
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
            auto now = TInstant::Now();
            LastAccess[now].insert(key);
            ScopeCounters[key] = TScopeValue{scopeRequests, now};
            return (*scopeRequests)[type];
        }
    };

    TCounters Counters;
    NConfig::TControlPlaneProxyConfig Config;
    bool GetQuotas;

public:
    TControlPlaneProxyActor(const NConfig::TControlPlaneProxyConfig& config, const ::NMonitoring::TDynamicCounterPtr& counters, bool getQuotas)
        : Counters(counters)
        , Config(config)
        , GetQuotas(getQuotas)
    {
    }

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
        {"yq.connections.use@as", TPermissions::CONNECTIONS_USE},
        {"yq.bindings.use@as", TPermissions::BINDINGS_USE},
        {"yq.queries.invoke@as", TPermissions::QUERY_INVOKE},
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
        if (!Config.GetEnablePermissions()) {
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
        YandexQuery::CreateQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("CreateQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_CREATE_QUERY, RTC_CREATE_QUERY);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.create@as"});
        if (issues) {
            CPS_LOG_E("CreateQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvCreateQueryResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::QUERY_INVOKE
            | TPermissions::TPermission::CONNECTIONS_USE
            | TPermissions::TPermission::BINDINGS_USE
            | TPermissions::TPermission::MANAGE_PUBLIC
        };

        Register(new TRequestActor<YandexQuery::CreateQueryRequest,
                                   TEvControlPlaneStorage::TEvCreateQueryRequest,
                                   TEvControlPlaneStorage::TEvCreateQueryResponse,
                                   TEvControlPlaneProxy::TEvCreateQueryResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe, ExtractPermissions(ev, availablePermissions), cloudId, ev->Get()->Quotas));
    }

    void Handle(TEvControlPlaneProxy::TEvListQueriesRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::ListQueriesRequest request = ev->Get()->Request;
        CPP_LOG_T("ListQueriesRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_LIST_QUERIES, RTC_LIST_QUERIES);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.get@as"});
        if (issues) {
            CPS_LOG_E("ListQueriesRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvListQueriesResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::ListQueriesRequest,
                                   TEvControlPlaneStorage::TEvListQueriesRequest,
                                   TEvControlPlaneStorage::TEvListQueriesResponse,
                                   TEvControlPlaneProxy::TEvListQueriesResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::DescribeQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("DescribeQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DESCRIBE_QUERY, RTC_DESCRIBE_QUERY);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.get@as"});
        if (issues) {
            CPS_LOG_E("DescribeQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDescribeQueryResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_AST
            | TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::DescribeQueryRequest,
                                   TEvControlPlaneStorage::TEvDescribeQueryRequest,
                                   TEvControlPlaneStorage::TEvDescribeQueryResponse,
                                   TEvControlPlaneProxy::TEvDescribeQueryResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvGetQueryStatusRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::GetQueryStatusRequest request = ev->Get()->Request;
        CPP_LOG_T("GetStatusRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_GET_QUERY_STATUS, RTC_GET_QUERY_STATUS);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.getStatus@as"});
        if (issues) {
            CPS_LOG_E("GetQueryStatusRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvGetQueryStatusResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::GetQueryStatusRequest,
                                   TEvControlPlaneStorage::TEvGetQueryStatusRequest,
                                   TEvControlPlaneStorage::TEvGetQueryStatusResponse,
                                   TEvControlPlaneProxy::TEvGetQueryStatusResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvModifyQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::ModifyQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("ModifyQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_MODIFY_QUERY, RTC_MODIFY_QUERY);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.update@as"});
        if (issues) {
            CPS_LOG_E("ModifyQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvModifyQueryResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::QUERY_INVOKE
            | TPermissions::TPermission::CONNECTIONS_USE
            | TPermissions::TPermission::BINDINGS_USE
            | TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::ModifyQueryRequest,
                                   TEvControlPlaneStorage::TEvModifyQueryRequest,
                                   TEvControlPlaneStorage::TEvModifyQueryResponse,
                                   TEvControlPlaneProxy::TEvModifyQueryResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvDeleteQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::DeleteQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("DeleteQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DELETE_QUERY, RTC_DELETE_QUERY);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.delete@as"});
        if (issues) {
            CPS_LOG_E("DeleteQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDeleteQueryResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::DeleteQueryRequest,
                                   TEvControlPlaneStorage::TEvDeleteQueryRequest,
                                   TEvControlPlaneStorage::TEvDeleteQueryResponse,
                                   TEvControlPlaneProxy::TEvDeleteQueryResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvControlQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::ControlQueryRequest request = ev->Get()->Request;
        CPP_LOG_T("ControlQueryRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_CONTROL_QUERY, RTC_CONTROL_QUERY);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.control@as"});
        if (issues) {
            CPS_LOG_E("ControlQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvControlQueryResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::ControlQueryRequest,
                                   TEvControlPlaneStorage::TEvControlQueryRequest,
                                   TEvControlPlaneStorage::TEvControlQueryResponse,
                                   TEvControlPlaneProxy::TEvControlQueryResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvGetResultDataRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::GetResultDataRequest request = ev->Get()->Request;
        CPP_LOG_T("GetResultDataRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_GET_RESULT_DATA, RTC_GET_RESULT_DATA);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.getData@as"});
        if (issues) {
            CPS_LOG_E("GetResultDataRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvGetResultDataResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::GetResultDataRequest,
                                   TEvControlPlaneStorage::TEvGetResultDataRequest,
                                   TEvControlPlaneStorage::TEvGetResultDataResponse,
                                   TEvControlPlaneProxy::TEvGetResultDataResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvListJobsRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::ListJobsRequest request = ev->Get()->Request;
        CPP_LOG_T("ListJobsRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_LIST_JOBS, RTC_LIST_JOBS);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.jobs.get@as"});
        if (issues) {
            CPS_LOG_E("ListJobsRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvListJobsResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::ListJobsRequest,
                                   TEvControlPlaneStorage::TEvListJobsRequest,
                                   TEvControlPlaneStorage::TEvListJobsResponse,
                                   TEvControlPlaneProxy::TEvListJobsResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeJobRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::DescribeJobRequest request = ev->Get()->Request;
        CPP_LOG_T("DescribeJobRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DESCRIBE_JOB, RTC_DESCRIBE_JOB);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.jobs.get@as"});
        if (issues) {
            CPS_LOG_E("DescribeJobRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDescribeJobResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::DescribeJobRequest,
                                   TEvControlPlaneStorage::TEvDescribeJobRequest,
                                   TEvControlPlaneStorage::TEvDescribeJobResponse,
                                   TEvControlPlaneProxy::TEvDescribeJobResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::CreateConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("CreateConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
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
            Send(ev->Sender, new TEvControlPlaneProxy::TEvCreateConnectionResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
        };

        Register(new TRequestActor<YandexQuery::CreateConnectionRequest,
                                   TEvControlPlaneStorage::TEvCreateConnectionRequest,
                                   TEvControlPlaneStorage::TEvCreateConnectionResponse,
                                   TEvControlPlaneProxy::TEvCreateConnectionResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe, ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvListConnectionsRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::ListConnectionsRequest request = ev->Get()->Request;
        CPP_LOG_T("ListConnectionsRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_LIST_CONNECTIONS, RTC_LIST_CONNECTIONS);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.connections.get@as"});
        if (issues) {
            CPS_LOG_E("ListConnectionsRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvListConnectionsResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::ListConnectionsRequest,
                                   TEvControlPlaneStorage::TEvListConnectionsRequest,
                                   TEvControlPlaneStorage::TEvListConnectionsResponse,
                                   TEvControlPlaneProxy::TEvListConnectionsResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::DescribeConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("DescribeConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DESCRIBE_CONNECTION, RTC_DESCRIBE_CONNECTION);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.connections.get@as"});
        if (issues) {
            CPS_LOG_E("DescribeConnectionRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDescribeConnectionResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::DescribeConnectionRequest,
                                   TEvControlPlaneStorage::TEvDescribeConnectionRequest,
                                   TEvControlPlaneStorage::TEvDescribeConnectionResponse,
                                   TEvControlPlaneProxy::TEvDescribeConnectionResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::ModifyConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("ModifyConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
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
            Send(ev->Sender, new TEvControlPlaneProxy::TEvModifyConnectionResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::ModifyConnectionRequest,
                                   TEvControlPlaneStorage::TEvModifyConnectionRequest,
                                   TEvControlPlaneStorage::TEvModifyConnectionResponse,
                                   TEvControlPlaneProxy::TEvModifyConnectionResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::DeleteConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("DeleteConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DELETE_CONNECTION, RTC_DELETE_CONNECTION);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.connections.delete@as"});
        if (issues) {
            CPS_LOG_E("DeleteConnectionRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDeleteConnectionResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::DeleteConnectionRequest,
                                   TEvControlPlaneStorage::TEvDeleteConnectionRequest,
                                   TEvControlPlaneStorage::TEvDeleteConnectionResponse,
                                   TEvControlPlaneProxy::TEvDeleteConnectionResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvTestConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::TestConnectionRequest request = ev->Get()->Request;
        CPP_LOG_T("TestConnectionRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
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
            Send(ev->Sender, new TEvControlPlaneProxy::TEvTestConnectionResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::TestConnectionRequest,
                                   TEvTestConnection::TEvTestConnectionRequest,
                                   TEvTestConnection::TEvTestConnectionResponse,
                                   TEvControlPlaneProxy::TEvTestConnectionResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    TestConnectionActorId(),
                                    requestCounters,
                                    probe, ExtractPermissions(ev, {}), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::CreateBindingRequest request = ev->Get()->Request;
        CPP_LOG_T("CreateBindingRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_CREATE_BINDING, RTC_CREATE_BINDING);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.create@as"});
        if (issues) {
            CPS_LOG_E("CreateBindingRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvCreateBindingResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
        };

        Register(new TRequestActor<YandexQuery::CreateBindingRequest,
                                   TEvControlPlaneStorage::TEvCreateBindingRequest,
                                   TEvControlPlaneStorage::TEvCreateBindingResponse,
                                   TEvControlPlaneProxy::TEvCreateBindingResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe, ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvListBindingsRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::ListBindingsRequest request = ev->Get()->Request;
        CPP_LOG_T("ListBindingsRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_LIST_BINDINGS, RTC_LIST_BINDINGS);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.get@as"});
        if (issues) {
            CPS_LOG_E("ListBindingsRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvListBindingsResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::ListBindingsRequest,
                                   TEvControlPlaneStorage::TEvListBindingsRequest,
                                   TEvControlPlaneStorage::TEvListBindingsResponse,
                                   TEvControlPlaneProxy::TEvListBindingsResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::DescribeBindingRequest request = ev->Get()->Request;
        CPP_LOG_T("DescribeBindingRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DESCRIBE_BINDING, RTC_DESCRIBE_BINDING);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.get@as"});
        if (issues) {
            CPS_LOG_E("DescribeBindingRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDescribeBindingResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::VIEW_PUBLIC
            | TPermissions::TPermission::VIEW_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::DescribeBindingRequest,
                                   TEvControlPlaneStorage::TEvDescribeBindingRequest,
                                   TEvControlPlaneStorage::TEvDescribeBindingResponse,
                                   TEvControlPlaneProxy::TEvDescribeBindingResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::ModifyBindingRequest request = ev->Get()->Request;
        CPP_LOG_T("ModifyBindingRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_MODIFY_BINDING, RTC_MODIFY_BINDING);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.update@as"});
        if (issues) {
            CPS_LOG_E("ModifyBindingRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvModifyBindingResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::ModifyBindingRequest,
                                   TEvControlPlaneStorage::TEvModifyBindingRequest,
                                   TEvControlPlaneStorage::TEvModifyBindingResponse,
                                   TEvControlPlaneProxy::TEvModifyBindingResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::DeleteBindingRequest request = ev->Get()->Request;
        CPP_LOG_T("DeleteBindingRequest: " << request.DebugString());
        const TString cloudId = ev->Get()->CloudId;
        const TString folderId = ev->Get()->FolderId;
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
                                              probe, ev, cookie, GetQuotas));
            return;
        }

        TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DELETE_BINDING, RTC_DELETE_BINDING);
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.delete@as"});
        if (issues) {
            CPS_LOG_E("DeleteBindingRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDeleteBindingResponse(issues), 0, ev->Cookie);
            requestCounters.IncError();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        static const TPermissions availablePermissions {
            TPermissions::TPermission::MANAGE_PUBLIC
            | TPermissions::TPermission::MANAGE_PRIVATE
        };

        Register(new TRequestActor<YandexQuery::DeleteBindingRequest,
                                   TEvControlPlaneStorage::TEvDeleteBindingRequest,
                                   TEvControlPlaneStorage::TEvDeleteBindingResponse,
                                   TEvControlPlaneProxy::TEvDeleteBindingResponse>
                                   (Config, ev->Sender, ev->Cookie, scope, folderId,
                                    std::move(request), std::move(user), std::move(token),
                                    ControlPlaneStorageServiceActorId(),
                                    requestCounters,
                                    probe,
                                    ExtractPermissions(ev, availablePermissions), cloudId));
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        HTML(str) {
            PRE() {
                str << "Current config:" << Endl;
                str << Config.DebugString() << Endl;
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

IActor* CreateControlPlaneProxyActor(const NConfig::TControlPlaneProxyConfig& config, const ::NMonitoring::TDynamicCounterPtr& counters, bool getQuotas) {
    return new TControlPlaneProxyActor(config, counters, getQuotas);
}

}  // namespace NYq
