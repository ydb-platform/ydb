#include "control_plane_proxy.h"
#include "probes.h"
#include "utils.h"

#include <ydb/core/yq/libs/actors/logging/log.h>
#include <ydb/core/yq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <ydb/core/yq/libs/control_plane_storage/util.h>
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

struct TRequestCounters: public virtual TThrRefBase {
    const TString Name;

    NMonitoring::TDynamicCounters::TCounterPtr InFly;
    NMonitoring::TDynamicCounters::TCounterPtr Ok;
    NMonitoring::TDynamicCounters::TCounterPtr Error;
    NMonitoring::TDynamicCounters::TCounterPtr Timeout;
    NMonitoring::THistogramPtr LatencyMs;

    explicit TRequestCounters(const TString& name)
        : Name(name)
    { }

    void Register(const NMonitoring::TDynamicCounterPtr& counters) {
        NMonitoring::TDynamicCounterPtr subgroup = counters->GetSubgroup("request", Name);
        InFly = subgroup->GetCounter("InFly", false);
        Ok = subgroup->GetCounter("Ok", true);
        Error = subgroup->GetCounter("Error", true);
        Timeout = subgroup->GetCounter("Timeout", true);
        LatencyMs = subgroup->GetHistogram("LatencyMs", GetLatencyHistogramBuckets());
    }

private:
    static NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets() {
        return NMonitoring::ExplicitHistogram({0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 50000, 500000});
    }
};

using TRequestCountersPtr = TIntrusivePtr<TRequestCounters>;

template<class TRequestProto, class TRequest, class TResponse, class TResponseProxy, bool ResolveFolder = false>
class TRequestActor : public NActors::TActorBootstrapped<TRequestActor<TRequestProto, TRequest, TResponse, TResponseProxy, ResolveFolder>> {
    using TBase = NActors::TActorBootstrapped<TRequestActor<TRequestProto, TRequest, TResponse, TResponseProxy, ResolveFolder>>;
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
    TRequestCountersPtr Counters;
    TInstant StartTime;
    std::function<void(const TDuration&, bool, bool)> Probe;
    TPermissions Permissions;
    TRequestCountersPtr ResolveFolderCounters;
    TString CloudId;

public:
    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_REQUEST_ACTOR";

    explicit TRequestActor(const NConfig::TControlPlaneProxyConfig& config,
                           TActorId sender, ui32 cookie,
                           const TString& scope, const TString& folderId, TRequestProto&& requestProto,
                           TString&& user, TString&& token, const TActorId& serviceId,
                           const TRequestCountersPtr& counters,
                           const std::function<void(const TDuration&, bool, bool)>& probe,
                           TPermissions permissions,
                           const TRequestCountersPtr& resolveFolderCounters = nullptr)
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
        , ResolveFolderCounters(resolveFolderCounters)
    {
        Counters->InFly->Inc();
        FillDefaultParameters(Config);
    }

public:

    void Bootstrap() {
        CPP_LOG_T("Request actor. Actor id: " << SelfId());
        Become(&TRequestActor::StateFunc, GetDuration(Config.GetRequestTimeout(), TDuration::Seconds(30)), new NActors::TEvents::TEvWakeup());
        if constexpr (ResolveFolder) {
            auto request = std::make_unique<NKikimr::NFolderService::TEvFolderService::TEvGetFolderRequest>();
            request->Request.set_folder_id(FolderId);
            request->Token = Token;
            ResolveFolderCounters->InFly->Inc();
            Send(NKikimr::NFolderService::FolderServiceActorId(), request.release(), 0, 0);
        } else {
            Send(ServiceId, new TRequest(Scope, RequestProto, User, Token, Permissions), 0, Cookie);
        }
    }

    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        hFunc(TResponse, Handle);
        hFunc(NKikimr::NFolderService::TEvFolderService::TEvGetFolderResponse, Handle);
    )

    void HandleTimeout() {
        CPP_LOG_D("Request timeout. " << RequestProto.DebugString());
        NYql::TIssues issues;
        NYql::TIssue issue = MakeErrorIssue(TIssuesIds::TIMEOUT, "Request timeout. Try repeating the request later");
        issues.AddIssue(issue);
        if (ResolveFolder && !CloudId) {
            ResolveFolderCounters->Error->Inc();
            ResolveFolderCounters->Timeout->Inc();
        }
        Counters->Error->Inc();
        Counters->Timeout->Inc();
        const TDuration delta = TInstant::Now() - StartTime;
        Probe(delta, false, true);
        Send(Sender, new TResponseProxy(issues), 0, Cookie);
        PassAway();
    }

    void Handle(NKikimr::NFolderService::TEvFolderService::TEvGetFolderResponse::TPtr& ev) {
        ResolveFolderCounters->InFly->Dec();
        ResolveFolderCounters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());

        const auto& response = ev->Get()->Response;
        TString errorMessage;

        const auto& status = ev->Get()->Status;
        if (!status.Ok() || !ev->Get()->Response.has_folder()) {
            ResolveFolderCounters->Error->Inc();
            errorMessage = "Msg: " + status.Msg + " Details: " + status.Details + " Code: " + ToString(status.GRpcStatusCode) + " InternalError: " + ToString(status.InternalError);
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

        ResolveFolderCounters->Ok->Inc();
        CloudId = response.folder().cloud_id();
        CPP_LOG_T("Cloud id: " << CloudId << " Folder id: " << FolderId);
        if constexpr (ResolveFolder) {
            Send(ServiceId, new TRequest(Scope, RequestProto, User, Token, CloudId, Permissions), 0, Cookie);
        }
    }

    void Handle(typename TResponse::TPtr& ev) {
        const TDuration delta = TInstant::Now() - StartTime;
        auto& response = *ev->Get();
        ProcessResponse(delta, response);
    }

    template<typename T>
    void ProcessResponse(const TDuration& delta, const T& response) {
        if (response.Issues) {
            Counters->Error->Inc();
            Probe(delta, false, false);
            Send(Sender, new TResponseProxy(response.Issues), 0, Cookie);
        } else {
            Counters->Ok->Inc();
            Probe(delta, true, false);
            Send(Sender, new TResponseProxy(response.Result), 0, Cookie);
        }
        PassAway();
    }

    template<typename T> requires requires (T t) { t.AuditDetails; }
    void ProcessResponse(const TDuration& delta, const T& response) {
        if (response.Issues) {
            Counters->Error->Inc();
            Probe(delta, false, false);
            Send(Sender, new TResponseProxy(response.Issues), 0, Cookie);
        } else {
            Counters->Ok->Inc();
            Probe(delta, true, false);
            Send(Sender, new TResponseProxy(response.Result, response.AuditDetails), 0, Cookie);
        }
        PassAway();
    }

    virtual ~TRequestActor() {
        Counters->InFly->Dec();
        Counters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
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
    enum ERequestType {
        RT_CREATE_QUERY,
        RT_LIST_QUERIES,
        RT_DESCRIBE_QUERY,
        RT_GET_QUERY_STATUS, 
        RT_MODIFY_QUERY,
        RT_DELETE_QUERY,
        RT_CONTROL_QUERY,
        RT_GET_RESULT_DATA,
        RT_LIST_JOBS,
        RT_DESCRIBE_JOB, 
        RT_CREATE_CONNECTION,
        RT_LIST_CONNECTIONS,
        RT_DESCRIBE_CONNECTION,
        RT_MODIFY_CONNECTION,
        RT_DELETE_CONNECTION,
        RT_TEST_CONNECTION,
        RT_CREATE_BINDING,
        RT_LIST_BINDINGS,
        RT_DESCRIBE_BINDING,
        RT_MODIFY_BINDING,
        RT_DELETE_BINDING,
        RT_RESOLVE_FOLDER,
        RT_MAX,
    };

    struct TCounters: public virtual TThrRefBase {
        std::array<TRequestCountersPtr, RT_MAX> Requests = CreateArray<RT_MAX, TRequestCountersPtr>({
            { MakeIntrusive<TRequestCounters>("CreateQuery") },
            { MakeIntrusive<TRequestCounters>("ListQueries") },
            { MakeIntrusive<TRequestCounters>("DescribeQuery") },
            { MakeIntrusive<TRequestCounters>("GetQueryStatus") }, 
            { MakeIntrusive<TRequestCounters>("ModifyQuery") },
            { MakeIntrusive<TRequestCounters>("DeleteQuery") },
            { MakeIntrusive<TRequestCounters>("ControlQuery") },
            { MakeIntrusive<TRequestCounters>("GetResultData") },
            { MakeIntrusive<TRequestCounters>("ListJobs") },
            { MakeIntrusive<TRequestCounters>("DescribeJob") }, 
            { MakeIntrusive<TRequestCounters>("CreateConnection") },
            { MakeIntrusive<TRequestCounters>("ListConnections") },
            { MakeIntrusive<TRequestCounters>("DescribeConnection") },
            { MakeIntrusive<TRequestCounters>("ModifyConnection") },
            { MakeIntrusive<TRequestCounters>("DeleteConnection") },
            { MakeIntrusive<TRequestCounters>("TestConnection") },
            { MakeIntrusive<TRequestCounters>("CreateBinding") },
            { MakeIntrusive<TRequestCounters>("ListBindings") },
            { MakeIntrusive<TRequestCounters>("DescribeBinding") },
            { MakeIntrusive<TRequestCounters>("ModifyBinding") },
            { MakeIntrusive<TRequestCounters>("DeleteBinding") },
            { MakeIntrusive<TRequestCounters>("ResolveFolder") },
        });

        NMonitoring::TDynamicCounterPtr Counters;

        explicit TCounters(const NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
        {
            for (auto& request: Requests) {
                request->Register(Counters);
            }
        }
    };

    TCounters Counters;
    NConfig::TControlPlaneProxyConfig Config;

public:
    TControlPlaneProxyActor(const NConfig::TControlPlaneProxyConfig& config, const NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters)
        , Config(config)
    {
    }

    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY";

    void Bootstrap() {
        CPP_LOG_D("Starting yandex query control plane proxy. Actor id: " << SelfId());

        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(YQ_CONTROL_PLANE_PROXY_PROVIDER));

        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
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
    TPermissions ExtractPermissions(T& ev) {
        TPermissions permissions;
        for (const auto& permission: ev->Get()->Permissions) {
            if (auto it = PermissionsItems.find(permission); it != PermissionsItems.end()) {
                permissions.Set(it->second);
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
        YandexQuery::CreateQueryRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("CreateQueryRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_CREATE_QUERY];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(CreateQueryRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.create@as"});
        if (issues) {
            CPS_LOG_E("CreateQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvCreateQueryResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::CreateQueryRequest,
                                   TEvControlPlaneStorage::TEvCreateQueryRequest,
                                   TEvControlPlaneStorage::TEvCreateQueryResponse,
                                   TEvControlPlaneProxy::TEvCreateQueryResponse,
                                   true>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                         std::move(request), std::move(user), std::move(token),
                                         ControlPlaneStorageServiceActorId(),
                                         requestCounters,
                                         probe, ExtractPermissions(ev), Counters.Requests[RT_RESOLVE_FOLDER]));
    }

    void Handle(TEvControlPlaneProxy::TEvListQueriesRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::ListQueriesRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("ListQueriesRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_LIST_QUERIES];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ListQueriesRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.get@as"});
        if (issues) {
            CPS_LOG_E("ListQueriesRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvListQueriesResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::ListQueriesRequest,
                                   TEvControlPlaneStorage::TEvListQueriesRequest,
                                   TEvControlPlaneStorage::TEvListQueriesResponse,
                                   TEvControlPlaneProxy::TEvListQueriesResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                 std::move(request), std::move(user), std::move(token),
                                                                                 ControlPlaneStorageServiceActorId(),
                                                                                 requestCounters,
                                                                                 probe,
                                                                                 ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::DescribeQueryRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("DescribeQueryRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_DESCRIBE_QUERY];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const TString queryId = request.query_id();
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(DescribeQueryRequest, scope, user, queryId, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.get@as"});
        if (issues) {
            CPS_LOG_E("DescribeQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDescribeQueryResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::DescribeQueryRequest,
                                   TEvControlPlaneStorage::TEvDescribeQueryRequest,
                                   TEvControlPlaneStorage::TEvDescribeQueryResponse,
                                   TEvControlPlaneProxy::TEvDescribeQueryResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                   std::move(request), std::move(user), std::move(token),
                                                                                   ControlPlaneStorageServiceActorId(),
                                                                                   requestCounters,
                                                                                   probe,
                                                                                   ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvGetQueryStatusRequest::TPtr& ev) { 
        TInstant startTime = TInstant::Now(); 
        YandexQuery::GetQueryStatusRequest request = std::move(ev->Get()->Request); 
        CPP_LOG_T("GetStatusRequest: " << request.DebugString()); 
        TRequestCountersPtr requestCounters = Counters.Requests[RT_GET_QUERY_STATUS]; 
 
        const TString folderId = ev->Get()->FolderId; 
        const TString scope = "yandexcloud://" + folderId; 
        TString user = std::move(ev->Get()->User); 
        TString token = std::move(ev->Get()->Token); 
        const TString queryId = request.query_id(); 
        const int byteSize = request.ByteSize(); 
 
        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) { 
            LWPROBE(GetQueryStatusRequest, scope, user, queryId, delta, byteSize, isSuccess, isTimeout); 
        }; 
 
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.getStatus@as"}); 
        if (issues) { 
            CPS_LOG_E("GetQueryStatusRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString()); 
            Send(ev->Sender, new TEvControlPlaneProxy::TEvGetQueryStatusResponse(issues), 0, ev->Cookie); 
            requestCounters->Error->Inc(); 
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds()); 
            probe(delta, false, false); 
            return; 
        } 
 
        Register(new TRequestActor<YandexQuery::GetQueryStatusRequest, 
                                   TEvControlPlaneStorage::TEvGetQueryStatusRequest, 
                                   TEvControlPlaneStorage::TEvGetQueryStatusResponse, 
                                   TEvControlPlaneProxy::TEvGetQueryStatusResponse>(Config, ev->Sender, ev->Cookie, scope, folderId, 
                                                                                   std::move(request), std::move(user), std::move(token), 
                                                                                   ControlPlaneStorageServiceActorId(), 
                                                                                   requestCounters, 
                                                                                   probe, 
                                                                                   ExtractPermissions(ev))); 
    } 
 
    void Handle(TEvControlPlaneProxy::TEvModifyQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::ModifyQueryRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("ModifyQueryRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_MODIFY_QUERY];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const TString queryId = request.query_id();
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ModifyQueryRequest, scope, user, queryId, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.update@as"});
        if (issues) {
            CPS_LOG_E("ModifyQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvModifyQueryResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::ModifyQueryRequest,
                                   TEvControlPlaneStorage::TEvModifyQueryRequest,
                                   TEvControlPlaneStorage::TEvModifyQueryResponse,
                                   TEvControlPlaneProxy::TEvModifyQueryResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                 std::move(request), std::move(user), std::move(token),
                                                                                 ControlPlaneStorageServiceActorId(),
                                                                                 requestCounters,
                                                                                 probe,
                                                                                 ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvDeleteQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::DeleteQueryRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("DeleteQueryRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_DELETE_QUERY];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const TString queryId = request.query_id();
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(DeleteQueryRequest, scope, user, queryId, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.delete@as"});
        if (issues) {
            CPS_LOG_E("DeleteQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDeleteQueryResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::DeleteQueryRequest,
                                   TEvControlPlaneStorage::TEvDeleteQueryRequest,
                                   TEvControlPlaneStorage::TEvDeleteQueryResponse,
                                   TEvControlPlaneProxy::TEvDeleteQueryResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                 std::move(request), std::move(user), std::move(token),
                                                                                 ControlPlaneStorageServiceActorId(),
                                                                                 requestCounters,
                                                                                 probe,
                                                                                 ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvControlQueryRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::ControlQueryRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("ControlQueryRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_CONTROL_QUERY];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const TString queryId = request.query_id();
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ControlQueryRequest, scope, user, queryId, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.control@as"});
        if (issues) {
            CPS_LOG_E("ControlQueryRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvControlQueryResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::ControlQueryRequest,
                                   TEvControlPlaneStorage::TEvControlQueryRequest,
                                   TEvControlPlaneStorage::TEvControlQueryResponse,
                                   TEvControlPlaneProxy::TEvControlQueryResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                  std::move(request), std::move(user), std::move(token),
                                                                                  ControlPlaneStorageServiceActorId(),
                                                                                  requestCounters,
                                                                                  probe,
                                                                                  ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvGetResultDataRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::GetResultDataRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("GetResultDataRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_GET_RESULT_DATA];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const TString queryId = request.query_id();
        const int32_t resultSetIndex = request.result_set_index();
        const int64_t limit = request.limit();
        const int64_t offset = request.offset();
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(GetResultDataRequest, scope, user, queryId, resultSetIndex, offset, limit, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.queries.getData@as"});
        if (issues) {
            CPS_LOG_E("GetResultDataRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvGetResultDataResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::GetResultDataRequest,
                                   TEvControlPlaneStorage::TEvGetResultDataRequest,
                                   TEvControlPlaneStorage::TEvGetResultDataResponse,
                                   TEvControlPlaneProxy::TEvGetResultDataResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                      std::move(request), std::move(user), std::move(token),
                                                                                      ControlPlaneStorageServiceActorId(),
                                                                                      requestCounters,
                                                                                      probe,
                                                                                      ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvListJobsRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::ListJobsRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("ListJobsRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_LIST_JOBS];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const TString queryId = request.query_id();
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ListJobsRequest, scope, user, queryId, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.jobs.get@as"});
        if (issues) {
            CPS_LOG_E("ListJobsRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvListJobsResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::ListJobsRequest,
                                   TEvControlPlaneStorage::TEvListJobsRequest,
                                   TEvControlPlaneStorage::TEvListJobsResponse,
                                   TEvControlPlaneProxy::TEvListJobsResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                              std::move(request), std::move(user), std::move(token),
                                                                              ControlPlaneStorageServiceActorId(),
                                                                              requestCounters,
                                                                              probe,
                                                                              ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeJobRequest::TPtr& ev) { 
        TInstant startTime = TInstant::Now(); 
        YandexQuery::DescribeJobRequest request = std::move(ev->Get()->Request); 
        CPP_LOG_T("DescribeJobRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_DESCRIBE_JOB]; 
 
        const TString folderId = ev->Get()->FolderId; 
        const TString scope = "yandexcloud://" + folderId; 
        TString user = std::move(ev->Get()->User); 
        TString token = std::move(ev->Get()->Token); 
        const TString jobId = request.job_id(); 
        const int byteSize = request.ByteSize(); 
 
        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) { 
            LWPROBE(DescribeJobRequest, scope, user, jobId, delta, byteSize, isSuccess, isTimeout); 
        }; 
 
        NYql::TIssues issues = ValidatePermissions(ev, {"yq.jobs.get@as"});
        if (issues) { 
            CPS_LOG_E("DescribeJobRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString()); 
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDescribeJobResponse(issues), 0, ev->Cookie); 
            requestCounters->Error->Inc(); 
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds()); 
            probe(delta, false, false); 
            return; 
        } 
 
        Register(new TRequestActor<YandexQuery::DescribeJobRequest, 
                                   TEvControlPlaneStorage::TEvDescribeJobRequest, 
                                   TEvControlPlaneStorage::TEvDescribeJobResponse, 
                                   TEvControlPlaneProxy::TEvDescribeJobResponse>(Config, ev->Sender, ev->Cookie, scope, folderId, 
                                                                                     std::move(request), std::move(user), std::move(token), 
                                                                                     ControlPlaneStorageServiceActorId(), 
                                                                                     requestCounters, 
                                                                                     probe,
                                                                                     ExtractPermissions(ev)));
    } 
 
    void Handle(TEvControlPlaneProxy::TEvCreateConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::CreateConnectionRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("CreateConnectionRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_CREATE_CONNECTION];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(CreateConnectionRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        TVector<TString> requiredPermissions = {"yq.connections.create@as"};
        if (ExtractServiceAccountId(request)) {
            requiredPermissions.push_back("iam.serviceAccounts.use@as");
        }

        NYql::TIssues issues = ValidatePermissions(ev, requiredPermissions);
        if (issues) {
            CPS_LOG_E("CreateConnectionRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvCreateConnectionResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::CreateConnectionRequest,
                                   TEvControlPlaneStorage::TEvCreateConnectionRequest,
                                   TEvControlPlaneStorage::TEvCreateConnectionResponse,
                                   TEvControlPlaneProxy::TEvCreateConnectionResponse,
                                   true>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                         std::move(request), std::move(user), std::move(token),
                                         ControlPlaneStorageServiceActorId(),
                                         requestCounters,
                                         probe, ExtractPermissions(ev), Counters.Requests[RT_RESOLVE_FOLDER]));
    }

    void Handle(TEvControlPlaneProxy::TEvListConnectionsRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::ListConnectionsRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("ListConnectionsRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_LIST_CONNECTIONS];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ListConnectionsRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.connections.get@as"});
        if (issues) {
            CPS_LOG_E("ListConnectionsRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvListConnectionsResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::ListConnectionsRequest,
                                   TEvControlPlaneStorage::TEvListConnectionsRequest,
                                   TEvControlPlaneStorage::TEvListConnectionsResponse,
                                   TEvControlPlaneProxy::TEvListConnectionsResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                     std::move(request), std::move(user), std::move(token),
                                                                                     ControlPlaneStorageServiceActorId(),
                                                                                     requestCounters,
                                                                                     probe,
                                                                                     ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::DescribeConnectionRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("DescribeConnectionRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_DESCRIBE_CONNECTION];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const TString connectionId = request.connection_id();
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(DescribeConnectionRequest, scope, user, connectionId, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.connections.get@as"});
        if (issues) {
            CPS_LOG_E("DescribeConnectionRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDescribeConnectionResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::DescribeConnectionRequest,
                                   TEvControlPlaneStorage::TEvDescribeConnectionRequest,
                                   TEvControlPlaneStorage::TEvDescribeConnectionResponse,
                                   TEvControlPlaneProxy::TEvDescribeConnectionResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                      std::move(request), std::move(user), std::move(token),
                                                                                      ControlPlaneStorageServiceActorId(),
                                                                                      requestCounters,
                                                                                      probe,
                                                                                      ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvModifyConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::ModifyConnectionRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("ModifyConnectionRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_MODIFY_CONNECTION];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const TString connectionId = request.connection_id();
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ModifyConnectionRequest, scope, user, connectionId, delta, byteSize, isSuccess, isTimeout);
        };

        TVector<TString> requiredPermissions = {"yq.connections.update@as"};
        if (ExtractServiceAccountId(request)) {
            requiredPermissions.push_back("iam.serviceAccounts.use@as");
        }

        NYql::TIssues issues = ValidatePermissions(ev, requiredPermissions);
        if (issues) {
            CPS_LOG_E("ModifyConnectionRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvModifyConnectionResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::ModifyConnectionRequest,
                                   TEvControlPlaneStorage::TEvModifyConnectionRequest,
                                   TEvControlPlaneStorage::TEvModifyConnectionResponse,
                                   TEvControlPlaneProxy::TEvModifyConnectionResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                      std::move(request), std::move(user), std::move(token),
                                                                                      ControlPlaneStorageServiceActorId(),
                                                                                      requestCounters,
                                                                                      probe,
                                                                                      ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvDeleteConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::DeleteConnectionRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("DeleteConnectionRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_DELETE_CONNECTION];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const TString connectionId = request.connection_id();
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(DeleteConnectionRequest, scope, user, connectionId, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.connections.delete@as"});
        if (issues) {
            CPS_LOG_E("DeleteConnectionRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDeleteConnectionResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::DeleteConnectionRequest,
                                   TEvControlPlaneStorage::TEvDeleteConnectionRequest,
                                   TEvControlPlaneStorage::TEvDeleteConnectionResponse,
                                   TEvControlPlaneProxy::TEvDeleteConnectionResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                      std::move(request), std::move(user), std::move(token),
                                                                                      ControlPlaneStorageServiceActorId(),
                                                                                      requestCounters,
                                                                                      probe,
                                                                                      ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvTestConnectionRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now();
        YandexQuery::TestConnectionRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("TestConnectionRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_TEST_CONNECTION];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(TestConnectionRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        TVector<TString> requiredPermissions = {"yq.connections.create@as"};
        if (ExtractServiceAccountId(request)) {
            requiredPermissions.push_back("iam.serviceAccounts.use@as");
        }

        NYql::TIssues issues = ValidatePermissions(ev, requiredPermissions);
        if (issues) {
            CPS_LOG_E("TestConnectionRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvTestConnectionResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime;
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
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
                                     probe, ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvCreateBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::CreateBindingRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("CreateBindingRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_CREATE_BINDING];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(CreateBindingRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.create@as"});
        if (issues) {
            CPS_LOG_E("CreateBindingRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvCreateBindingResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::CreateBindingRequest,
                                   TEvControlPlaneStorage::TEvCreateBindingRequest,
                                   TEvControlPlaneStorage::TEvCreateBindingResponse,
                                   TEvControlPlaneProxy::TEvCreateBindingResponse,
                                   true>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                         std::move(request), std::move(user), std::move(token),
                                         ControlPlaneStorageServiceActorId(),
                                         requestCounters,
                                         probe, ExtractPermissions(ev), Counters.Requests[RT_RESOLVE_FOLDER]));
    }

    void Handle(TEvControlPlaneProxy::TEvListBindingsRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::ListBindingsRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("ListBindingsRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_LIST_BINDINGS];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ListBindingsRequest, scope, user, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.get@as"});
        if (issues) {
            CPS_LOG_E("ListBindingsRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvListBindingsResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::ListBindingsRequest,
                                   TEvControlPlaneStorage::TEvListBindingsRequest,
                                   TEvControlPlaneStorage::TEvListBindingsResponse,
                                   TEvControlPlaneProxy::TEvListBindingsResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                  std::move(request), std::move(user), std::move(token),
                                                                                  ControlPlaneStorageServiceActorId(),
                                                                                  requestCounters,
                                                                                  probe,
                                                                                  ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvDescribeBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::DescribeBindingRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("DescribeBindingRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_DESCRIBE_BINDING];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const TString bindingId = request.binding_id();
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(DescribeBindingRequest, scope, user, bindingId, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.get@as"});
        if (issues) {
            CPS_LOG_E("DescribeBindingRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDescribeBindingResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::DescribeBindingRequest,
                                   TEvControlPlaneStorage::TEvDescribeBindingRequest,
                                   TEvControlPlaneStorage::TEvDescribeBindingResponse,
                                   TEvControlPlaneProxy::TEvDescribeBindingResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                     std::move(request), std::move(user), std::move(token),
                                                                                     ControlPlaneStorageServiceActorId(),
                                                                                     requestCounters,
                                                                                     probe,
                                                                                     ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvModifyBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::ModifyBindingRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("ModifyBindingRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_MODIFY_BINDING];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const TString bindingId = request.binding_id();
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(ModifyBindingRequest, scope, user, bindingId, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.update@as"});
        if (issues) {
            CPS_LOG_E("ModifyBindingRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvModifyBindingResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::ModifyBindingRequest,
                                   TEvControlPlaneStorage::TEvModifyBindingRequest,
                                   TEvControlPlaneStorage::TEvModifyBindingResponse,
                                   TEvControlPlaneProxy::TEvModifyBindingResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                      std::move(request), std::move(user), std::move(token),
                                                                                      ControlPlaneStorageServiceActorId(),
                                                                                      requestCounters,
                                                                                      probe,
                                                                                      ExtractPermissions(ev)));
    }

    void Handle(TEvControlPlaneProxy::TEvDeleteBindingRequest::TPtr& ev) {
        TInstant startTime = TInstant::Now(); 
        YandexQuery::DeleteBindingRequest request = std::move(ev->Get()->Request);
        CPP_LOG_T("DeleteBindingRequest: " << request.DebugString());
        TRequestCountersPtr requestCounters = Counters.Requests[RT_DELETE_BINDING];

        const TString folderId = ev->Get()->FolderId;
        const TString scope = "yandexcloud://" + folderId;
        TString user = std::move(ev->Get()->User);
        TString token = std::move(ev->Get()->Token);
        const TString bindingId = request.binding_id();
        const int byteSize = request.ByteSize();

        auto probe = [=](const TDuration& delta, bool isSuccess, bool isTimeout) {
            LWPROBE(DeleteBindingRequest, scope, user, bindingId, delta, byteSize, isSuccess, isTimeout);
        };

        NYql::TIssues issues = ValidatePermissions(ev, {"yq.bindings.delete@as"});
        if (issues) {
            CPS_LOG_E("DeleteBindingRequest, validation failed: " << scope << " " << user << " " << NKikimr::MaskTicket(token) << " " << request.DebugString() << " error: " << issues.ToString());
            Send(ev->Sender, new TEvControlPlaneProxy::TEvDeleteBindingResponse(issues), 0, ev->Cookie);
            requestCounters->Error->Inc();
            TDuration delta = TInstant::Now() - startTime; 
            requestCounters->LatencyMs->Collect(delta.MilliSeconds());
            probe(delta, false, false);
            return;
        }

        Register(new TRequestActor<YandexQuery::DeleteBindingRequest,
                                   TEvControlPlaneStorage::TEvDeleteBindingRequest,
                                   TEvControlPlaneStorage::TEvDeleteBindingResponse,
                                   TEvControlPlaneProxy::TEvDeleteBindingResponse>(Config, ev->Sender, ev->Cookie, scope, folderId,
                                                                                   std::move(request), std::move(user), std::move(token),
                                                                                   ControlPlaneStorageServiceActorId(),
                                                                                   requestCounters,
                                                                                   probe,
                                                                                   ExtractPermissions(ev)));
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

IActor* CreateControlPlaneProxyActor(const NConfig::TControlPlaneProxyConfig& config, const NMonitoring::TDynamicCounterPtr& counters) {
    return new TControlPlaneProxyActor(config, counters);
}

}  // namespace NYq
