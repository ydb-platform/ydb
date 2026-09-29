#include <ydb/core/base/tablet.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/proxy_service/kqp_proxy_service.h>
#include <ydb/core/kqp/proxy_service/kqp_proxy_service_impl.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/services/workload_manager/ut/common/workload_service_ut_common.h>
#include <ydb/services/workload_manager/actors/actors.h>
#include <ydb/services/workload_manager/service/service.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/services/ydb/ydb_common_ut.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/base/counters.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>

#include <util/generic/vector.h>
#include <util/system/hp_timer.h>

#include <atomic>
#include <memory>
#include <thread>

namespace NKikimr::NKqp {

using namespace Tests;
using namespace NSchemeShard;

namespace  {

struct TSimpleResource {
    ui32 Cnt;
    ui32 NodeId;
    TString DataCenterId;

    TSimpleResource(ui32 cnt, ui32 nodeId, TString dataCenterId)
        : Cnt(cnt)
        , NodeId(nodeId)
        , DataCenterId(std::move(dataCenterId))
    {}
};


TVector<NKikimrKqp::TKqpProxyNodeResources> Transform(TVector<TSimpleResource> data) {
    TVector<NKikimrKqp::TKqpProxyNodeResources> result;
    result.resize(data.size());
    for(auto& item: data) {
        NKikimrKqp::TKqpProxyNodeResources payload;
        payload.SetNodeId(item.NodeId);
        payload.SetDataCenterId(item.DataCenterId);
        payload.SetActiveWorkersCount(item.Cnt);
        result.emplace_back(payload);
    }

    return result;
}

TString CreateSession(TTestActorRuntime* runtime, const TActorId& kqpProxy, const TActorId& sender,
                      const TString& database = {}) {
    auto request = MakeHolder<TEvKqp::TEvCreateSessionRequest>();
    request->Record.MutableRequest()->SetDatabase(database);
    runtime->Send(new IEventHandle(kqpProxy, sender, request.Release()));
    auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvCreateSessionResponse>(sender);
    auto record = reply->Get()->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
    TString sessionId = record.GetResponse().GetSessionId();
    return sessionId;
}

class TWmStateReporter : public TActorBootstrapped<TWmStateReporter> {
public:
    TWmStateReporter(std::shared_ptr<NWorkloadManager::ISessionUpdater> updater,
                     NWorkloadManager::ISessionUpdater::EState state, TActorId edge)
        : Updater(std::move(updater))
        , State(state)
        , Edge(edge)
    {}

    void Bootstrap() {
        Updater->SetRequestState(State, TActivationContext::Now());
        Send(Edge, new TEvents::TEvWakeup());
        PassAway();
    }

private:
    std::shared_ptr<NWorkloadManager::ISessionUpdater> Updater;
    NWorkloadManager::ISessionUpdater::EState State;
    TActorId Edge;
};

void ReportWmState(TTestActorRuntime& runtime,
                   const std::shared_ptr<NWorkloadManager::ISessionUpdater>& updater,
                   NWorkloadManager::ISessionUpdater::EState state, ui32 nodeIndex = 0) {
    const auto edge = runtime.AllocateEdgeActor(nodeIndex);
    runtime.Register(new TWmStateReporter(updater, state, edge), nodeIndex);
    UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge));
}

void CheckNoWmNotifications(TTestActorRuntime& runtime, const TActorId& recipient) {
    // The runtime observer does not see deliveries to edge actors. Inspect the
    // recipient queue up to a marker after the operation under test completes.
    runtime.Send(new IEventHandle(recipient, {}, new TEvents::TEvWakeup()));
    runtime.WaitForEdgeEvents([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
        UNIT_ASSERT_C(event->GetTypeRewrite() != NWorkloadManager::TEvWmStateChanged::EventType,
                      "Unexpected WM notification");
        return event->GetTypeRewrite() == TEvents::TEvWakeup::EventType;
    }, {recipient}, TDuration::Seconds(5));
}

THolder<TEvKqp::TEvQueryRequest> MakeWmQuery(const TString& sessionId, const TString& text,
                                          ui64 timeoutMs = 10000) {
    auto event = MakeHolder<TEvKqp::TEvQueryRequest>();
    auto* request = event->Record.MutableRequest();
    request->SetSessionId(sessionId);
    request->SetDatabase("/Root");
    request->SetPoolId("pool");
    request->SetQuery(text);
    request->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
    request->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    request->SetKeepSession(true);
    request->SetReportWmStateChanges(true);
    request->SetTimeoutMs(timeoutMs);
    request->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    request->MutableTxControl()->set_commit_tx(true);
    return event;
}

void CheckWmAdmissionResult(ui32 issueCode, bool cancel, bool delayed = false) {
    namespace WM = NWorkloadManager;
    using EState = WM::ISessionUpdater::EState;
    TPortManager ports;
    auto settings = Tests::TServerSettings(ports.GetPort());
    settings.SetDomainName("Root");
    settings.SetUseRealThreads(false);
    Tests::TServer server(settings);
    auto& runtime = *server.GetRuntime();
    const auto service = runtime.AllocateEdgeActor();
    const auto worker = runtime.AllocateEdgeActor();
    const auto observer = runtime.AllocateEdgeActor();
    runtime.RegisterService(WM::MakeServiceId(runtime.GetNodeId()), service);

    NResourcePool::TPoolSettings pool;
    pool.ConcurrentQueryLimit = 1;
    pool.QueueSize = 10;
    auto updater = std::make_shared<TWmSessionUpdater>();
    updater->SetPoolContext({"pool", "USER"});
    updater->SetStateObserver(observer, 101);
    const auto handler = runtime.Register(WM::CreatePoolHandlerActor(
        "/Root", "pool", pool, MakeIntrusive<NMonitoring::TDynamicCounters>()));
    runtime.Send(new IEventHandle(service, worker,
        new WM::TEvPlaceRequestIntoPool(1, "/Root", "session", "pool", nullptr, "SELECT 1;", updater)));
    auto placement = runtime.GrabEdgeEvent<WM::TEvPlaceRequestIntoPool>(service);
    runtime.Send(new IEventHandle(handler, service, new WM::TEvPrivate::TEvResolvePoolResponse(
        Ydb::StatusIds::SUCCESS, pool, {}, false, std::move(placement))));
    auto pending = runtime.GrabEdgeEvent<WM::TEvWmStateChanged>(observer);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(pending->Get()->State), static_cast<ui32>(EState::PENDING));

    bool cancelHandled = false;
    runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TEvKqp::TEvQueryRequest::EventType) {
            // This test supplies table-operation responses explicitly.
            return TTestActorRuntime::EEventAction::DROP;
        }
        if (event->GetTypeRewrite() == WM::TEvPrivate::TEvCancelRequest::EventType &&
            event->GetRecipientRewrite() == handler) {
            cancelHandled = true;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });
    if (delayed) {
        UNIT_ASSERT(cancel);
        // Occupy the available slot to force a real FIFO placement. This sets
        // CleanupRequired, so cancellation must wait for the cleanup response.
        WM::TPoolStateDescription poolState;
        poolState.RunningRequests = 1;
        runtime.Send(new IEventHandle(handler, service, new WM::TEvPrivate::TEvRefreshPoolStateResponse(
            Ydb::StatusIds::SUCCESS, poolState, {})));
        runtime.Send(new IEventHandle(handler, service, new WM::TEvPrivate::TEvDelayRequestResponse(
            Ydb::StatusIds::SUCCESS, "session", {})));
        auto queued = runtime.GrabEdgeEvent<WM::TEvWmStateChanged>(observer);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(queued->Get()->State), static_cast<ui32>(EState::DELAYED));
    }

    if (cancel) {
        runtime.Send(new IEventHandle(handler, service, new WM::TEvPrivate::TEvCancelRequest("session")), 0, true);
    } else {
        NYql::TIssues issues;
        if (issueCode) {
            NYql::TIssue issue("Disk space quota exceeded");
            issue.SetCode(issueCode, NYql::TSeverityIds::S_ERROR);
            issues.AddIssue(issue);
        }
        runtime.Send(new IEventHandle(handler, service, new WM::TEvPrivate::TEvDelayRequestResponse(
            issueCode ? Ydb::StatusIds::PRECONDITION_FAILED : Ydb::StatusIds::OVERLOADED,
            "session", issues)));
    }
    if (delayed) {
        runtime.WaitFor("cancellation waits for FIFO cleanup", [&] { return cancelHandled; });
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(updater->GetState()), static_cast<ui32>(EState::DELAYED));
        CheckNoWmNotifications(runtime, observer);
        runtime.Send(new IEventHandle(handler, service, new WM::TEvPrivate::TEvCleanupRequestsResponse(
            Ydb::StatusIds::SUCCESS, std::vector<TString>{"session"}, {})));
    }
    auto notification = runtime.GrabEdgeEvent<WM::TEvWmStateChanged>(observer);
    UNIT_ASSERT_VALUES_EQUAL(notification->Cookie, 101);
    UNIT_ASSERT_VALUES_EQUAL(notification->Get()->PoolId, "pool");
    UNIT_ASSERT_VALUES_EQUAL(notification->Get()->ClassifiedBy, "USER");
    const auto expected = issueCode ? EState::EXITED : EState::NONE;
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(notification->Get()->State), static_cast<ui32>(expected));
    auto response = runtime.GrabEdgeEvent<WM::TEvContinueRequest>(worker);
    const auto expectedAdmission = issueCode ? WM::TEvContinueRequest::EAdmissionResult::ContinueInPool
                                            : WM::TEvContinueRequest::EAdmissionResult::Reject;
    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(response->Get()->GetAdmissionResult()), static_cast<ui32>(expectedAdmission));
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, cancel ? Ydb::StatusIds::CANCELLED :
        (issueCode ? Ydb::StatusIds::PRECONDITION_FAILED : Ydb::StatusIds::OVERLOADED));
    CheckNoWmNotifications(runtime, observer);
    runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
}

class TDatabaseCacheTestActor : public TActorBootstrapped<TDatabaseCacheTestActor> {
public:
    TDatabaseCacheTestActor(const TString& database, const TString& expectedDatabaseId, TDuration idleTimeout, NThreading::TPromise<void> promise)
        : IdleTimeout(idleTimeout)
        , Database(database)
        , ExpectedDatabaseId(expectedDatabaseId)
        , Cache(idleTimeout)
        , Promise(promise)
    {}

    void Bootstrap() {
        Become(&TDatabaseCacheTestActor::StateFunc);

        auto event = MakeHolder<TEvKqp::TEvQueryRequest>();
        event->Record.MutableRequest()->SetDatabase(Database);
        Send(SelfId(), event.Release());

        Schedule(3 * IdleTimeout, new TEvents::TEvWakeup());
    }

    void Handle(TEvKqp::TEvUpdateDatabaseInfo::TPtr& ev) {
        if (!CacheUpdated) {
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, Ydb::StatusIds::SUCCESS, TStringBuilder() << GetErrorString() << ev->Get()->Issues.ToString());
            Cache.UpdateDatabaseInfo(ev, ActorContext());
            CacheUpdated = true;
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Status, Ydb::StatusIds::ABORTED, TStringBuilder() << GetErrorString() << ev->Get()->Issues.ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(ev->Get()->Issues.ToString(), "Database subscription was dropped by idle timeout", GetErrorString());
            Finish();
        }
    }

    void Handle(TEvKqp::TEvDelayedRequestError::TPtr& ev) {
        UNIT_ASSERT_C(false, TStringBuilder() << "Unexpected fail, status: " << ev->Get()->Status << ", " << GetErrorString() << ev->Get()->Issues.ToString());
    }

    void Handle(TEvKqp::TEvQueryRequest::TPtr& ev) {
        auto success = Cache.SetDatabaseIdOrDefer(ev, 0, ActorContext());

        bool dedicated = Database == ExpectedDatabaseId;
        if (CacheUpdated || dedicated) {
            UNIT_ASSERT_C(success, TStringBuilder() << "Expected database id from cache, " << GetErrorString());
            UNIT_ASSERT_STRING_CONTAINS_C(ev->Get()->GetDatabaseId(), ExpectedDatabaseId, GetErrorString());
            if (dedicated) {
                Finish();
            }
        } else {
            UNIT_ASSERT_C(!success, TStringBuilder() << "Unexpected database id from cache, " << GetErrorString());
        }
    }

    void HandleWakeup() {
        UNIT_ASSERT_C(false, TStringBuilder() << "Test cache timeout, " << GetErrorString());
        Finish();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvKqp::TEvUpdateDatabaseInfo, Handle);
        hFunc(TEvKqp::TEvDelayedRequestError, Handle);
        hFunc(TEvKqp::TEvQueryRequest, Handle);
        sFunc(TEvents::TEvWakeup, HandleWakeup);
    )

private:
    TString GetErrorString() const {
        return TStringBuilder() << "cache updated: " << CacheUpdated << ", database: " << Database << "\n";
    }

    void Finish() {
        Promise.SetValue();
        PassAway();
    }

private:
    const TDuration IdleTimeout;
    const TString Database;
    const TString ExpectedDatabaseId;
    TDatabasesCache Cache;
    NThreading::TPromise<void> Promise;

    bool CacheUpdated = false;
};

}

Y_UNIT_TEST_SUITE(KqpProxy) {
    Y_UNIT_TEST(WmNotificationsAfterTimeoutCleanup) {
        namespace WM = NWorkloadManager;
        using EState = WM::ISessionUpdater::EState;
        TPortManager ports;
        auto settings = Tests::TServerSettings(ports.GetPort());
        settings.SetDomainName("Root");
        settings.SetUseRealThreads(false);
        settings.AppConfig->MutableFeatureFlags()->SetEnableResourcePools(true);
        Tests::TServer server(settings);
        auto& runtime = *server.GetRuntime();
        const auto proxy = MakeKqpProxyID(runtime.GetNodeId());
        const auto sender = runtime.AllocateEdgeActor();
        const auto replacementSender = runtime.AllocateEdgeActor();
        const auto workload = runtime.AllocateEdgeActor();
        runtime.RegisterService(WM::MakeServiceId(runtime.GetNodeId()), workload);
        const auto sessionId = CreateSession(&runtime, proxy, sender, "/Root");

        runtime.Send(new IEventHandle(proxy, sender, MakeWmQuery(sessionId, "SELECT 1;").Release(), 0, 101));
        auto placement = runtime.GrabEdgeEvent<WM::TEvPlaceRequestIntoPool>(workload);
        auto oldUpdater = placement->Get()->WmSessionUpdater;
        UNIT_ASSERT(oldUpdater);
        ReportWmState(runtime, oldUpdater, EState::PENDING);
        auto pending = runtime.GrabEdgeEvent<WM::TEvWmStateChanged>(sender);
        UNIT_ASSERT_VALUES_EQUAL(pending->Cookie, 101);

        // Withhold the WM cleanup response: the session actor remains busy,
        // while the proxy's timeout fallback marks its session IDLE.
        auto cleanup = runtime.GrabEdgeEvent<WM::TEvCleanupRequest>(workload);
        auto timeout = runtime.GrabEdgeEvent<TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(timeout->Cookie, 101);
        UNIT_ASSERT_VALUES_EQUAL(timeout->Get()->Record.GetYdbStatus(), Ydb::StatusIds::TIMEOUT);

        TAutoPtr<IEventHandle> heldBusy;
        std::shared_ptr<WM::ISessionUpdater> replacementUpdater;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvKqp::TEvQueryRequest::EventType &&
                event->GetRecipientRewrite() == placement->Sender) {
                replacementUpdater = event->Get<TEvKqp::TEvQueryRequest>()->GetWmSessionUpdater();
            }
            if (event->GetTypeRewrite() == TEvKqp::TEvQueryResponse::EventType &&
                event->Get<TEvKqp::TEvQueryResponse>()->Record.GetYdbStatus() == Ydb::StatusIds::SESSION_BUSY) {
                heldBusy.Reset(event.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        runtime.Send(new IEventHandle(proxy, replacementSender,
            MakeWmQuery(sessionId, "SELECT 2;").Release(), 0, 202));
        runtime.WaitFor("replacement request rejected while cleanup is pending", [&] { return bool(heldBusy); });
        UNIT_ASSERT(replacementUpdater);
        UNIT_ASSERT(oldUpdater != replacementUpdater);
        // Exercise delayed callbacks while Q2's SESSION_BUSY reply is in flight.
        for (auto state : {EState::PENDING, EState::DELAYED, EState::EXITED}) {
            ReportWmState(runtime, oldUpdater, state);
        }
        CheckNoWmNotifications(runtime, sender);
        CheckNoWmNotifications(runtime, replacementSender);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(replacementUpdater->GetState()), static_cast<ui32>(EState::NONE));
        UNIT_ASSERT_VALUES_EQUAL(oldUpdater->GetClassifiedBy(), "USER");
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        runtime.Send(heldBusy.Release());
        auto busy = runtime.GrabEdgeEvent<TEvKqp::TEvQueryResponse>(replacementSender);
        UNIT_ASSERT_VALUES_EQUAL(busy->Cookie, 202);
        UNIT_ASSERT_VALUES_EQUAL(busy->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SESSION_BUSY);
        runtime.Send(new IEventHandle(cleanup->Sender, workload, new WM::TEvCleanupResponse(Ydb::StatusIds::SUCCESS)));
    }

    Y_UNIT_TEST(WmNotificationsDisabledForRemoteSession) {
        namespace WM = NWorkloadManager;
        using EState = WM::ISessionUpdater::EState;
        TPortManager ports;
        auto settings = Tests::TServerSettings(ports.GetPort());
        settings.SetDomainName("Root");
        settings.SetNodeCount(2);
        settings.SetUseRealThreads(false);
        settings.AppConfig->MutableFeatureFlags()->SetEnableResourcePools(true);
        Tests::TServer server(settings);
        auto& runtime = *server.GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        const auto workload = runtime.AllocateEdgeActor(1);
        runtime.RegisterService(WM::MakeServiceId(runtime.GetNodeId(1)), workload, 1);
        const auto sessionId = CreateSession(&runtime, MakeKqpProxyID(runtime.GetNodeId(1)), sender, "/Root");
        runtime.Send(new IEventHandle(MakeKqpProxyID(runtime.GetNodeId()), sender,
            MakeWmQuery(sessionId, "SELECT 1;").Release(), 0, 101));
        auto placement = runtime.GrabEdgeEvent<WM::TEvPlaceRequestIntoPool>(workload);
        for (auto state : {EState::PENDING, EState::DELAYED, EState::NONE}) {
            ReportWmState(runtime, placement->Get()->WmSessionUpdater, state, 1);
        }
        runtime.Send(new IEventHandle(placement->Sender, workload, new WM::TEvContinueRequest(
            placement->Get()->QueryId, Ydb::StatusIds::OVERLOADED, "pool", {})), 1);
        auto cleanup = runtime.GrabEdgeEvent<WM::TEvCleanupRequest>(workload);
        runtime.Send(new IEventHandle(cleanup->Sender, workload, new WM::TEvCleanupResponse(Ydb::StatusIds::SUCCESS)), 1);
        auto response = runtime.GrabEdgeEvent<TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, 101);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetYdbStatus(), Ydb::StatusIds::OVERLOADED);
        CheckNoWmNotifications(runtime, sender);
    }

    Y_UNIT_TEST(WmAdmissionDiskQuotaNotification) {
        CheckWmAdmissionResult(NYql::TIssuesIds::KIKIMR_DATABASE_DISK_SPACE_QUOTA_EXCEEDED, false);
        CheckWmAdmissionResult(NYql::TIssuesIds::KIKIMR_DISK_GROUP_OUT_OF_SPACE, false);
    }

    Y_UNIT_TEST(WmAdmissionRejectedNotification) {
        CheckWmAdmissionResult(0, false);
    }

    Y_UNIT_TEST(WmAdmissionCancelledNotification) {
        CheckWmAdmissionResult(0, true);
    }

    Y_UNIT_TEST(WmAdmissionDelayedCancelledNotification) {
        CheckWmAdmissionResult(0, true, true);
    }

    Y_UNIT_TEST(WmAdmissionResult) {
        using TResponse = NWorkloadManager::TEvContinueRequest;
        using EResult = TResponse::EAdmissionResult;
        const auto check = [](Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues, EResult expected) {
            const TResponse response(1, status, "pool", {}, issues);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(response.GetAdmissionResult()), static_cast<ui32>(expected));
        };
        check(Ydb::StatusIds::SUCCESS, {}, EResult::ContinueInPool);
        check(Ydb::StatusIds::UNSUPPORTED, {}, EResult::ContinueWithoutPool);
        for (auto status : {Ydb::StatusIds::OVERLOADED, Ydb::StatusIds::CANCELLED,
                            Ydb::StatusIds::INTERNAL_ERROR, Ydb::StatusIds::PRECONDITION_FAILED}) {
            check(status, {}, EResult::Reject);
        }
        for (auto code : {NYql::TIssuesIds::KIKIMR_DATABASE_DISK_SPACE_QUOTA_EXCEEDED,
                          NYql::TIssuesIds::KIKIMR_DISK_GROUP_OUT_OF_SPACE}) {
            NYql::TIssue issue("Disk space quota exceeded");
            issue.SetCode(code, NYql::TSeverityIds::S_ERROR);
            NYql::TIssues issues;
            issues.AddIssue(issue);
            check(Ydb::StatusIds::PRECONDITION_FAILED, issues, EResult::ContinueInPool);
            check(Ydb::StatusIds::UNSUPPORTED, issues, EResult::ContinueWithoutPool);
            issues.AddIssue(NYql::TIssue("Another error"));
            check(Ydb::StatusIds::PRECONDITION_FAILED, issues, EResult::Reject);
        }
    }

    Y_UNIT_TEST(CalcPeerStats) {
        auto getActiveWorkers = [](const NKikimrKqp::TKqpProxyNodeResources& entry) {
            return entry.GetActiveWorkersCount();
        };

        UNIT_ASSERT_VALUES_EQUAL(
            CalcPeerStats(Transform(TVector<TSimpleResource>{TSimpleResource(100, 1, "1"), TSimpleResource(50, 2, "1")}), "1", true, getActiveWorkers).CV,
            47);

        UNIT_ASSERT_VALUES_EQUAL(
            CalcPeerStats(Transform(TVector<TSimpleResource>{TSimpleResource(100, 1, "1"), TSimpleResource(50, 2, "2")}), "1", true, getActiveWorkers).CV,
            0);
    }


    Y_UNIT_TEST(InvalidSessionID) {
        TPortManager tp;

        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport);

        Tests::TServer server(settings);
        Tests::TClient client(settings);

        server.GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
        client.InitRootScheme();
        auto runtime = server.GetRuntime();

        TActorId kqpProxy = MakeKqpProxyID(runtime->GetNodeId(0));
        TActorId sender = runtime->AllocateEdgeActor();

        auto SendBadRequestToSession = [&](const TString& sessionId) {
            auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
            ev->Record.MutableRequest()->SetSessionId(sessionId);
            ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
            ev->Record.MutableRequest()->SetQuery("SELECT 1; COMMIT;");
            ev->Record.MutableRequest()->SetKeepSession(true);
            ev->Record.MutableRequest()->SetTimeoutMs(10);

            runtime->Send(new IEventHandle(kqpProxy, sender, ev.Release()));
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::BAD_REQUEST);
        };

        SendBadRequestToSession("ydb://session/1?id=ZjY5NWRlM2EtYWMyYjA5YWEtNzQ0MTVlYTMtM2Q4ZDgzOWQ=&node_id=1234&node_id=12345");
        SendBadRequestToSession("unknown://session/1?id=ZjY5NWRlM2EtYWMyYjA5YWEtNzQ0MTVlYTMtM2Q4ZDgzOWQ=&node_id=1234&node_id=12345");
        SendBadRequestToSession("ydb://session/1?id=ZjY5NWRlM2EtYWMyYjA5YWEtNzQ0MTVlYTMtM2Q4ZDgzOWQ=&node_id=eqweq");
    }

    Y_UNIT_TEST(PassErrroViaSessionActor) {
        TPortManager tp;

        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport);

        Tests::TServer server(settings);
        Tests::TClient client(settings);

        server.GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
        client.InitRootScheme();
        auto runtime = server.GetRuntime();

        TActorId kqpProxy = MakeKqpProxyID(runtime->GetNodeId(0));
        TActorId sender = runtime->AllocateEdgeActor();

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        //ev->Record.MutableRequest()->SetSessionId(sessionId);
        ev->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "SomeUniqTextForUt");

        NYql::TIssues issues;
        issues.AddIssue(issue);
        NYql::IssuesToMessage(issues, ev->Record.MutableQueryIssues());

        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
        ev->Record.MutableRequest()->SetQuery("SELECT 1; COMMIT;");
        ev->Record.MutableRequest()->SetKeepSession(true);
        ev->Record.MutableRequest()->SetTimeoutMs(10);

        runtime->Send(new IEventHandle(kqpProxy, sender, ev.Release()));
        TAutoPtr<IEventHandle> handle;
        auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetResponse().GetQueryIssues().at(0).message(), "<main>: Error: SomeUniqTextForUt\n");
    }

    Y_UNIT_TEST(LoadedMetadataAfterCompilationTimeout) {

        TPortManager tp;

        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport).SetDomainName("Root").SetUseRealThreads(false);
        // set small compilation timeout to avoid long timer creation
        settings.AppConfig->MutableTableServiceConfig()->SetCompileTimeoutMs(400);

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
        server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_WORKER, NActors::NLog::PRI_DEBUG);
        server->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE,  NActors::NLog::PRI_DEBUG);
        server->GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_ACTOR, NActors::NLog::PRI_DEBUG);

        auto runtime = server->GetRuntime();

        TActorId kqpProxy = MakeKqpProxyID(runtime->GetNodeId(0));
        TActorId sender = runtime->AllocateEdgeActor();
        InitRoot(server, sender);

        Cerr << "Allocated edge actor" << Endl;
        std::vector<TAutoPtr<IEventHandle>> captured;
        std::vector<TAutoPtr<IEventHandle>> scheduled;

        auto scheduledEvs = [&](TTestActorRuntimeBase& run, TAutoPtr<IEventHandle> &event, TDuration delay, TInstant &deadline) {
            if (event->GetTypeRewrite() == TEvents::TSystem::Wakeup) {
                Cerr << "Captured TEvents::TSystem::Wakeup to " << runtime->FindActorName(event->GetRecipientRewrite()) << Endl;
                if (runtime->FindActorName(event->GetRecipientRewrite()) == "KQP_COMPILE_ACTOR") {
                    Cerr << "Captured scheduled event for compile actor " << event->Recipient << Endl;
                    scheduled.push_back(event.Release());
                    return true;
                }
            }

            return TTestActorRuntime::DefaultScheduledFilterFunc(run, event, delay, deadline);
        };

        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTxProxySchemeCache::TEvNavigateKeySetResult::EventType) {
                Cerr << "Captured Event" << Endl;
                captured.push_back(ev.Release());
                return true;
            }
            return false;
        };

        auto CreateTable = [&](const TString& sessionId, const TString& queryText) {
            auto ev = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
            ev->Record.MutableRequest()->SetSessionId(sessionId);
            ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DDL);
            ev->Record.MutableRequest()->SetQuery(queryText);
            runtime->Send(new IEventHandle(kqpProxy, sender, ev.release()));
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        };

        auto SendQuery = [&](const TString& sessionId, const TString& queryText) {
            auto ev = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
            ev->Record.MutableRequest()->SetSessionId(sessionId);
            ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_PREPARE);
            ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
            ev->Record.MutableRequest()->SetQuery(queryText);
            ev->Record.MutableRequest()->SetKeepSession(true);
            ev->Record.MutableRequest()->SetTimeoutMs(5000);

            runtime->Send(new IEventHandle(kqpProxy, sender, ev.release()));
            TAutoPtr<IEventHandle> handle;
            auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::TIMEOUT);
        };

        TString sessionId = CreateSession(runtime, kqpProxy, sender);
        CreateTable(sessionId, "--!syntax_v1\nCREATE TABLE `/Root/Table` (A int32, PRIMARY KEY(A));");
        CreateTable(sessionId, "--!syntax_v1\nCREATE TABLE `/Root/TableWithIndex` (A int32, B int32, PRIMARY KEY(A), INDEX TestIndex GLOBAL ON(B));");

        server->GetRuntime()->SetEventFilter(captureEvents);
        server->GetRuntime()->SetScheduledEventFilter(scheduledEvs);
        std::vector<TString> queries{"SELECT * FROM `/Root/Table`;", "SELECT * FROM `/Root/TableWithIndex`;", "SELECT * FROM `/Root/Table`;", "SELECT * FROM `/Root/Table`;"};
        for (auto query: queries) {
            for(size_t iter = 0; iter < 2; ++iter) {
                SendQuery(CreateSession(runtime, kqpProxy, sender), query);
                for(auto ev: scheduled) {
                    Cerr << "Send scheduled evet back" << Endl;
                    runtime->Send(ev.Release());
                }

                for(auto ev: captured) {
                    Cerr << "Send captured event back" << Endl;
                    runtime->Send(ev.Release());
                }

                scheduled.clear();
                captured.clear();
            }
        }
    }

    Y_UNIT_TEST(NoLocalSessionExecution) {
        TPortManager tp;

        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport);
        // Setup two to nodes with 2 KQP_RPOXY_ACTOR instances.
        settings.SetNodeCount(2);

        Tests::TServer server(settings);
        Tests::TClient client(settings);

        server.GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
        auto runtime = server.GetRuntime();

        TActorId kqpProxy1 = MakeKqpProxyID(runtime->GetNodeId(0));
        TActorId kqpProxy2 = MakeKqpProxyID(runtime->GetNodeId(1));
        TActorId sender = runtime->AllocateEdgeActor();

        {
            TString sessionId = CreateSession(runtime, kqpProxy2, sender);
            auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
            ev->Record.MutableRequest()->SetSessionId(sessionId);
            ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
            ev->Record.MutableRequest()->SetQuery("SELECT 1; COMMIT;");
            ev->Record.MutableRequest()->SetKeepSession(true);

            runtime->Send(new IEventHandle(kqpProxy1, sender, ev.Release()));

            TAutoPtr<IEventHandle> handle;
            auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(NodeDisconnectedTest) {
        TPortManager tp;

        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport);
        // Setup two to nodes with 2 KQP_RPOXY_ACTOR instances.
        settings.SetNodeCount(2);
        // Don't use real threads so we can capture all events
        settings.SetUseRealThreads(false);

        Tests::TServer server(settings);
        Tests::TClient client(settings);

        server.GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
        auto runtime = server.GetRuntime();

        TActorId kqpProxy1 = MakeKqpProxyID(runtime->GetNodeId(0));
        TActorId kqpProxy2 = MakeKqpProxyID(runtime->GetNodeId(1));
        Cerr << "KQP PROXY1 " << kqpProxy1 << Endl;
        Cerr << "KQP PROXY2 " << kqpProxy2 << Endl;
        TActorId sender = runtime->AllocateEdgeActor();

        Cerr << "SENDER " << sender << Endl;

        size_t NegativeStories = 0;
        size_t SuccessStories = 0;

        size_t capturedQueries = 0;
        size_t capturedPings = 0;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            // Drop every second event for KQP_PROXY_ACTOR on second node.
            if (ev->Recipient == kqpProxy2 && ev->GetTypeRewrite() == NKqp::TEvKqp::TEvQueryRequest::EventType) {
                ++capturedQueries;
                if (capturedQueries % 2 == 0) {
                    return true;
                } else {
                    return false;
                }
            }

            if (ev->Recipient == kqpProxy2 && ev->GetTypeRewrite() == NKqp::TEvKqp::TEvPingSessionRequest::EventType) {
                ++capturedPings;
                if (capturedPings % 2 == 0) {
                    return true;
                } else {
                    return false;
                }
            }
            return false;
        };

        server.GetRuntime()->SetEventFilter(captureEvents);

        for (ui32 rep = 0; rep < 30; rep++) {

            {
                TString sessionId = CreateSession(runtime, kqpProxy2, sender);
                Cerr << "Created  session " << sessionId << Endl;
                auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
                ev->Record.MutableRequest()->SetSessionId(sessionId);
                ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
                ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
                ev->Record.MutableRequest()->SetQuery("SELECT 1; COMMIT;");
                ev->Record.MutableRequest()->SetKeepSession(true);
                ev->Record.MutableRequest()->SetTimeoutMs(1);

                runtime->Send(new IEventHandle(kqpProxy1, sender, ev.Release()));

                TAutoPtr<IEventHandle> handle;
                auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(handle);
                auto status = reply->Record.GetYdbStatus();
                UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS || status == Ydb::StatusIds::TIMEOUT);

                if (status == Ydb::StatusIds::SUCCESS) {
                    ++SuccessStories;
                } else if (status == Ydb::StatusIds::TIMEOUT) {
                    ++NegativeStories;
                }
            }

            {
                TString sessionId = CreateSession(runtime, kqpProxy2, sender);
                auto ev = MakeHolder<NKqp::TEvKqp::TEvPingSessionRequest>();
                ev->Record.MutableRequest()->SetSessionId(sessionId);
                ev->Record.MutableRequest()->SetTimeoutMs(1);
                runtime->Send(new IEventHandle(kqpProxy1, sender, ev.Release()));

                TAutoPtr<IEventHandle> handle;
                auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvPingSessionResponse>(handle);
                auto status = reply->Record.GetStatus();
                UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS || status == Ydb::StatusIds::TIMEOUT);
                if (status == Ydb::StatusIds::SUCCESS) {
                    ++SuccessStories;
                } else if (status == Ydb::StatusIds::TIMEOUT) {
                    ++NegativeStories;
                }
            }
        }

        UNIT_ASSERT_C(SuccessStories > 0, "Proxy has success responses");
        UNIT_ASSERT_C(NegativeStories > 0, "Proxy has no negative responses");
    }

    Y_UNIT_TEST(CreatesScriptExecutionsTable) {
        TPortManager tp;
        constexpr ui32 nodesCount = 5;

        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport);
        settings.SetEnableScriptExecutionOperations(true);
        settings.SetNodeCount(nodesCount); // Test that all nodes will create table with race

        Tests::TServer server(settings);
        Tests::TClient client(settings);

        server.GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
        //server.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        //server.GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_DEBUG);
        //server.GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NActors::NLog::PRI_DEBUG);
        //server.GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NActors::NLog::PRI_DEBUG);
        //server.GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
        client.InitRootScheme();
        auto runtime = server.GetRuntime();

        TActorId edgeActors[nodesCount];
        for (ui32 node = 0; node < nodesCount; ++node) {
            edgeActors[node] = runtime->AllocateEdgeActor(node);
        }

        // Make sure that KQP proxy will answer with SUCCESS after a period of time
        bool allSuccess = false;
        do {
            allSuccess = true;
            for (ui32 node = 0; node < nodesCount; ++node) {
                TActorId kqpProxy = MakeKqpProxyID(runtime->GetNodeId(node));

                auto ev = MakeHolder<TEvKqp::TEvScriptRequest>();
                auto& req = *ev->Record.MutableRequest();
                req.SetQuery("SELECT 42");
                req.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT);
                req.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
                req.SetDatabase(settings.DomainName);

                runtime->Send(new IEventHandle(kqpProxy, edgeActors[node], ev.Release()), node);
            }
            for (ui32 node = 0; node < nodesCount; ++node) {
                auto reply = runtime->GrabEdgeEvent<TEvKqp::TEvScriptResponse>(edgeActors[node]);
                Ydb::StatusIds::StatusCode status = reply->Get()->Status;
                UNIT_ASSERT_C(status == Ydb::StatusIds::SUCCESS || status == Ydb::StatusIds::UNAVAILABLE, reply->Get()->Issues.ToString());
                UNIT_ASSERT_C(status == Ydb::StatusIds::UNAVAILABLE || reply->Get()->ExecutionId, reply->Get()->Issues.ToString());
                if (status != Ydb::StatusIds::SUCCESS) {
                    allSuccess = false;
                }
            }
            Sleep(TDuration::MilliSeconds(10));
        } while (!allSuccess);
    }

    Y_UNIT_TEST(NoUserAccessToScriptExecutionsTable) {
        // Test that checks that we can create operations table without internal token (=nullptr)
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
        appConfig.MutableFeatureFlags()->SetEnableScriptExecutionOperations(true);
        NYdb::TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);

        // Grant `connect` to user
        {
            ui16 grpc = server.GetPort();
            auto connection = NYdb::TDriver(NYdb::TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << grpc)
            .SetDatabase("/Root")
            .SetAuthToken("root@builtin"));

            NYdb::NTable::TTableClient tableClient(connection);
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery("GRANT CONNECT ON `/Root` TO `user@builtin`").ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            ui16 grpc = server.GetPort();
            auto connection = NYdb::TDriver(NYdb::TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << grpc)
            .SetDatabase("/Root")
            .SetAuthToken("user@builtin"));

            // Wait until KQP proxy is set up
            NYdb::EStatus scriptStatus;
            NYdb::NQuery::TQueryClient client(connection);
            do {
                auto executeScrptsResult = client.ExecuteScript("SELECT 42").ExtractValueSync();
                scriptStatus = executeScrptsResult.Status().GetStatus();
                UNIT_ASSERT_C(scriptStatus == NYdb::EStatus::UNAVAILABLE || scriptStatus == NYdb::EStatus::SUCCESS, executeScrptsResult.Status().GetIssues().ToString());
                UNIT_ASSERT(scriptStatus == NYdb::EStatus::UNAVAILABLE || !executeScrptsResult.Metadata().ExecutionId.empty());
                Sleep(TDuration::MilliSeconds(10));
            } while (scriptStatus == NYdb::EStatus::UNAVAILABLE);

            // Check access to `.metadata/script_executions`
            NYdb::NTable::TTableClient tableClient(connection);
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery("SELECT * FROM `.metadata/script_executions`", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(ExecuteScriptFailsWithoutFeatureFlag) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableScriptExecutionOperations(false);
        NYdb::TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();
        auto connection = NYdb::TDriver(NYdb::TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << grpc)
            .SetDatabase("/Root"));
        NYdb::NQuery::TQueryClient client(connection);

        auto executeScrptsResult = client.ExecuteScript("SELECT 42").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(executeScrptsResult.Status().GetStatus(), NYdb::EStatus::UNSUPPORTED, executeScrptsResult.Status().GetIssues().ToString());

        // Check that there is no .metadata folder
        NYdb::NScheme::TSchemeClient schemeClient(connection);
        auto listResult = schemeClient.ListDirectory("/Root").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(listResult.GetStatus(), NYdb::EStatus::SUCCESS, listResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(listResult.GetChildren().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listResult.GetChildren()[0].Name, ".sys");
    }

    Y_UNIT_TEST(PingNotExistedSession) {
        NKikimrConfig::TAppConfig appConfig;
        NYdb::TKikimrWithGrpcAndRootSchema server(appConfig);

        ui16 grpc = server.GetPort();
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);

        TString location = TStringBuilder() << "localhost:" << grpc;
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);
        bool allDoneOk = false;

        {
            NYdbGrpc::TGRpcClientLow clientLow;
            auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Table::V1::TableService>(clientConfig);

            Ydb::Table::KeepAliveRequest request;
            request.set_session_id("ydb://session/3?node_id=2&id=YDB0NDRhNjItYWQwZmIzMTktMWUyOTE4ZWYtYzE0NzJjNg==");

            NYdbGrpc::TResponseCallback<Ydb::Table::KeepAliveResponse> responseCb =
                [&allDoneOk](NYdbGrpc::TGrpcStatus&& grpcStatus, Ydb::Table::KeepAliveResponse&& response) -> void {
                    UNIT_ASSERT(grpcStatus.GRpcStatusCode == 0);
                    UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::BAD_SESSION);
                    allDoneOk = true;
            };

            connection->DoRequest(request, std::move(responseCb), &Ydb::Table::V1::TableService::Stub::AsyncKeepAlive);
        }

        UNIT_ASSERT(allDoneOk);
    }

    Y_UNIT_TEST(DatabasesCacheForServerless) {
        auto ydb = NWorkloadManager::TYdbSetupSettings()
            .CreateSampleTenants(true)
            .Create();

        auto& runtime = *ydb->GetRuntime();
        TDuration idleTimeout = TDuration::Seconds(5);

        auto checkCache = [&](const TString& database, const TString& expectedDatabaseId, ui32 nodeIndex) {
            auto promise = NThreading::NewPromise();
            runtime.Register(new TDatabaseCacheTestActor(database, expectedDatabaseId, idleTimeout, promise), nodeIndex);
            promise.GetFuture().GetValueSync();
        };

        const auto& dedicatedTenant = ydb->GetSettings().GetDedicatedTenantName();
        checkCache(dedicatedTenant, dedicatedTenant, ydb->GetDedicatedTenantInfo().NodeIdx);

        const auto& sharedTenant = ydb->GetSettings().GetSharedTenantName();
        checkCache(sharedTenant, sharedTenant, ydb->GetSharedTenantInfo().NodeIdx);

        const auto& serverlessTenant = ydb->GetSettings().GetServerlessTenantName();
        const auto& serverlessInfo = ydb->GetServerlessTenantInfo();
        checkCache(serverlessTenant, TStringBuilder() << ":" << serverlessInfo.PathId << ":" << serverlessTenant, serverlessInfo.NodeIdx);
    }

    Y_UNIT_TEST(CreateDeleteSessionsSequential) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport);

        Tests::TServer server(settings);
        Tests::TClient client(settings);

        server.GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_ERROR);
        client.InitRootScheme();
        auto runtime = server.GetRuntime();

        TActorId kqpProxy = MakeKqpProxyID(runtime->GetNodeId(0));
        TActorId sender = runtime->AllocateEdgeActor();

        const ui32 SessionsCount = 1000;

        THPTimer timer;

        for (ui32 i = 0; i < SessionsCount; ++i) {
            TString sessionId = CreateSession(runtime, kqpProxy, sender);
            UNIT_ASSERT(!sessionId.empty());

            auto closeEv = MakeHolder<TEvKqp::TEvCloseSessionRequest>();
            closeEv->Record.MutableRequest()->SetSessionId(sessionId);
            runtime->Send(new IEventHandle(kqpProxy, sender, closeEv.Release()));
        }

        double elapsed = timer.Passed();
        Cerr << "Sequential create+close: " << SessionsCount << " sessions in " << elapsed << " seconds" << Endl;
        Cerr << "Throughput: " << (SessionsCount / elapsed) << " create+close ops/sec" << Endl;

        auto counters = GetServiceCounters(runtime->GetAppData(0).Counters, "ydb");
        for (ui32 attempt = 0; attempt < 100; ++attempt) {
            ui64 activeSessions = counters->GetNamedCounter("name", "table.session.active_count", false)->Val();
            if (activeSessions == 0) {
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }

        ui64 activeSessions = counters->GetNamedCounter("name", "table.session.active_count", false)->Val();
        UNIT_ASSERT_VALUES_EQUAL_C(activeSessions, 0, "All sessions should be closed after cleanup");
    }

    Y_UNIT_TEST(CreateDeleteSessionsStress) {
        NKikimrConfig::TAppConfig appConfig;
        NYdb::TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_ERROR);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(location);

        const ui32 ThreadCount = 10;
        const ui32 SessionsPerThread = 100;

        std::atomic<ui32> totalCreated{0};
        std::atomic<ui32> totalDeleted{0};
        std::atomic<bool> hasErrors{false};

        THPTimer timer;

        TVector<std::thread> threads;
        threads.reserve(ThreadCount);
        for (ui32 t = 0; t < ThreadCount; ++t) {
            threads.emplace_back([&clientConfig, &totalCreated, &totalDeleted, &hasErrors, sessionsPerThread = SessionsPerThread]() {
                for (ui32 i = 0; i < sessionsPerThread; ++i) {
                    TString sessionId = NTestHelpers::CreateQuerySession(clientConfig);
                    if (sessionId.empty()) {
                        Cerr << "Failed to create session" << Endl;
                        hasErrors.store(true);
                        return;
                    }
                    totalCreated.fetch_add(1);

                    bool deleteOk = true;
                    NTestHelpers::CheckDelete(clientConfig, sessionId, Ydb::StatusIds::SUCCESS, deleteOk);
                    if (!deleteOk) {
                        Cerr << "Failed to delete session: " << sessionId << Endl;
                        hasErrors.store(true);
                        return;
                    }
                    totalDeleted.fetch_add(1);
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        double elapsed = timer.Passed();
        ui32 totalOps = totalCreated.load() + totalDeleted.load();

        Cerr << "Concurrent stress test: " << ThreadCount << " threads, "
             << SessionsPerThread << " sessions/thread" << Endl;
        Cerr << "Created: " << totalCreated.load() << ", Deleted: " << totalDeleted.load() << Endl;
        Cerr << "Total time: " << elapsed << " seconds" << Endl;
        Cerr << "Throughput: " << (totalOps / elapsed) << " ops/sec" << Endl;

        UNIT_ASSERT_C(!hasErrors.load(), "No errors during stress test");
        UNIT_ASSERT_VALUES_EQUAL(totalCreated.load(), ThreadCount * SessionsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(totalDeleted.load(), ThreadCount * SessionsPerThread);

        auto counters = GetServiceCounters(server.Server_->GetRuntime()->GetAppData(0).Counters, "ydb");
        for (ui32 attempt = 0; attempt < 100; ++attempt) {
            ui64 activeSessions = counters->GetNamedCounter("name", "table.session.active_count", false)->Val();
            if (activeSessions == 0) {
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }

        ui64 activeSessions = counters->GetNamedCounter("name", "table.session.active_count", false)->Val();
        UNIT_ASSERT_VALUES_EQUAL_C(activeSessions, 0, "All sessions should be closed after stress test");
    }

} // Y_UNIT_TEST_SUITE(KqpProxy)
} // namespace NKikimr
