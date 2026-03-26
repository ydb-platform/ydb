#include <fmt/format.h>

#include <ydb/core/kqp/common/events/workload_service.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/proxy_service/kqp_session_state.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace NKikimr::NKqp {

namespace {

using namespace NWorkload;
using namespace NYdb;
using namespace NActors;

///
/// Wraps the real IWmSessionUpdater and suppresses state transitions beyond
/// FinalState. This lets a test freeze the WM state visible in
/// .sys/query_sessions at a chosen level.
///
class TWmSessionUpdaterWrapper : public IWmSessionUpdater {
public:
    TWmSessionUpdaterWrapper(EWmState finalState, std::shared_ptr<IWmSessionUpdater> inner)
        : Inner(inner)
        , FinalState(finalState)
    {}

    void SetRequestState(EWmState state, TInstant timestamp) override {
        if (state > FinalState) {
            return;
        }
        Inner->SetRequestState(state, timestamp);
    }

    void SetPoolId(TString poolId) override {
        Inner->SetPoolId(poolId);
    }

private:
    std::shared_ptr<IWmSessionUpdater> Inner;
    EWmState FinalState;
};

///
/// A proxy actor assigned to a specific KQP session to intercept its workload events.
/// It acts as a middleman between the Workload Service and the session actor,
/// allowing the test to 'freeze' the request flow at the Edge Actor level.
/// This enables thread-safe inspection of system tables before the session
/// actually starts its SQL execution.
///
class TSessionProxyActor : public TActorBootstrapped<TSessionProxyActor> {
public:
    TSessionProxyActor(TActorId sessionActorId, TActorId edgeActorId)
        : SessionActorId(sessionActorId)
        , EdgeActorId(edgeActorId)
    {}

    void Bootstrap(const TActorContext&) {
        Become(&TSessionProxyActor::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NWorkload::TEvContinueRequest, HandleContinueRequest);
            default:
                Send(ev->Forward(SessionActorId));
        }
    }

    void HandleContinueRequest(NWorkload::TEvContinueRequest::TPtr& ev) {
        senderId = ev->Sender;
        Send(new IEventHandle(EdgeActorId, SelfId(), ev->Release().Release()));
        Become(&TSessionProxyActor::StateWait);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NWorkload::TEvContinueRequest, HandleRelease);
            default:
                Send(ev->Forward(SessionActorId));
        }
    }

    void HandleRelease(NWorkload::TEvContinueRequest::TPtr& ev) {
        Send(new IEventHandle(SessionActorId, senderId, ev->Release().Release()));
    }

private:
    TActorId SessionActorId;
    TActorId EdgeActorId;
    TActorId senderId;
};

///
/// Thread-safe mapping of a query text to an edge actor for interception
/// Used by the proxy to decide which requests should be 'parked'
///
struct TInterceptorRules {
    // Query Text -> EdgeActor
    std::unordered_map<TString, TActorId> ActiveRules;
    std::mutex Lock;

    void Add(TString query, TActorId edge) {
        std::lock_guard<std::mutex> g(Lock);
        ActiveRules[query] = edge;
    }

    std::optional<TActorId> GetEdge(const TString& query) {
        std::lock_guard<std::mutex> g(Lock);
        if (auto it = ActiveRules.find(query); it != ActiveRules.end()) {
            return it->second;
        }
        return std::nullopt;
    }
};

///
/// Replaces the real KQP workload service in the actor system.
///
class TKqpWorkloadProxyActor : public TActorBootstrapped<TKqpWorkloadProxyActor> {
public:
    TKqpWorkloadProxyActor(
        IWmSessionUpdater::EWmState finalState,
        TActorId realWorkloadServiceId,
        std::shared_ptr<TInterceptorRules> rules
    )
        : FinalState(finalState)
        , WorkloadServiceId(realWorkloadServiceId)
        , Rules(rules)
    {}

    void Bootstrap(const TActorContext&) {
        Become(&TKqpWorkloadProxyActor::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NWorkload::TEvPlaceRequestIntoPool, HandlePlaceRequest);
            default:
                Send(ev->Forward(WorkloadServiceId));
        }
    }

    void HandlePlaceRequest(NWorkload::TEvPlaceRequestIntoPool::TPtr& ev) {
        auto* msg = ev->Get();
        auto wrapper = std::make_shared<TWmSessionUpdaterWrapper>(FinalState, msg->WmSessionUpdater);

        auto* proxyMsg = new NWorkload::TEvPlaceRequestIntoPool(
            msg->DatabaseId,
            msg->SessionId,
            msg->PoolId,
            msg->UserToken,
            msg->RequestText,
            wrapper
        );

        TActorId senderForWorkload = ev->Sender;

        if (auto edge = Rules->GetEdge(msg->RequestText)) {
            auto* interceptor = new TSessionProxyActor(ev->Sender, *edge);
            senderForWorkload = Register(interceptor);
        }

        Send(new IEventHandle(WorkloadServiceId, senderForWorkload, proxyMsg));
    }

private:
    IWmSessionUpdater::EWmState FinalState;
    TActorId WorkloadServiceId;
    std::shared_ptr<TInterceptorRules> Rules;
};

///
/// Reads .sys/query_sessions for EXECUTING sessions.
///
class TQuerySessionReader {
public:
    struct Row {
        std::optional<std::string> WmPoolId;
        std::optional<std::string> WmState;
        std::optional<std::string> SessionId;
        std::optional<TInstant> WmEnterTime;
        std::optional<TInstant> WmExitTime;
    };

public:
    TQuerySessionReader(TIntrusivePtr<IYdbSetup> ydb)
        : Ydb(ydb)
    {}

    void FetchAll(const TString& query) {
        using namespace fmt::literals;

        Results.clear();
        TString q = fmt::format(R"(
            SELECT SessionId, WmPoolId, WmState, WmEnterTime, WmExitTime
            FROM `.sys/query_sessions`
            WHERE State = 'EXECUTING' and Query = '{query}'
            ORDER By WmState
        )",
            "query"_a = query
        );

        auto result = Ydb->ExecuteQuery(q, TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto rs = result.GetResultSet(0);
        auto parser = std::make_unique<NYdb::TResultSetParser>(rs);

        while (parser->TryNextRow()) {
            auto wmState = parser->ColumnParser("WmState").GetOptionalUtf8();
            auto wmPoolId = parser->ColumnParser("WmPoolId").GetOptionalUtf8();
            auto sessionId = parser->ColumnParser("SessionId").GetOptionalUtf8();
            auto wmEnterTime = parser->ColumnParser("WmEnterTime").GetOptionalTimestamp();
            auto wmExitTime = parser->ColumnParser("WmExitTime").GetOptionalTimestamp();

            Results.push_back(Row{
                .WmPoolId = wmPoolId,
                .WmState = wmState,
                .SessionId = sessionId,
                .WmEnterTime = wmEnterTime,
                .WmExitTime = wmExitTime
            });
        }
    }

    Row operator[](size_t index) const {
        Y_ENSURE(index < Results.size());
        return Results[index];
    }

    size_t Size() const {
        return Results.size();
    }

private:
    TIntrusivePtr<IYdbSetup> Ydb;
    std::vector<Row> Results;
};

class TQuerySessionTestFixture {
public:
    TQuerySessionTestFixture(const TString myPoolId, IWmSessionUpdater::EWmState state, size_t limit = 10)
        : State(state)
        , Rules(std::make_shared<TInterceptorRules>())
    {
        Ydb = TYdbSetupSettings()
            .NodeCount(1)
            .EnableResourcePools(true)
            // turn off to reduce "noise" in a log
            .EnableStreamingQueries(false)
            .ConcurrentQueryLimit(limit)
            .CreateSamplePool(true)
            .PoolId(myPoolId)
            .Create();

        auto& runtime = *Ydb->GetRuntime();
        auto workloadServiceId = MakeKqpWorkloadServiceId(runtime.GetNodeId(0));
        auto realWorkloadServiceId = runtime.GetLocalServiceId(workloadServiceId);
        auto proxyActor = new TKqpWorkloadProxyActor(State, realWorkloadServiceId, Rules);

        ProxyActorId = runtime.Register(proxyActor);

        runtime.RegisterService(workloadServiceId, ProxyActorId);
    }

    ~TQuerySessionTestFixture() {
        if (ProxyActorId && Ydb) {
            Ydb->GetRuntime()->Send(new IEventHandle(ProxyActorId, TActorId(), new TEvents::TEvPoisonPill()));
        }
    }

    TActorId GetProxyId() const { return ProxyActorId; }

    TIntrusivePtr<IYdbSetup> GetYdb() {
        return Ydb;
    }

    TActorId SetupInterceptor(const TString& query) {
        auto& runtime = *Ydb->GetRuntime();
        TActorId edgeActor = runtime.AllocateEdgeActor();
        Rules->Add(query, edgeActor);
        return edgeActor;
    }

private:
    IWmSessionUpdater::EWmState State;
    TIntrusivePtr<IYdbSetup> Ydb;
    TActorId ProxyActorId;
    std::shared_ptr<TInterceptorRules> Rules;
};

}  // anonymous namespace

Y_UNIT_TEST_SUITE(KqpWorkloadServiceQuerySessions) {
    ///
    /// Executes a query and processes all WM states up to the specified final state.
    /// It captures session data from .sys/query_sessions by 'parking' the request
    /// in the interceptor actor, ensuring a race-free read before actual SQL execution starts.
    ///
    TQuerySessionReader ReadQuerySessionAfterState(IWmSessionUpdater::EWmState state) {
        TQuerySessionTestFixture f("my_pool", state);
        auto myPool = TQueryRunnerSettings().PoolId("my_pool");
        const TString& query = TSampleQueries::TSelect42::Query;
        TActorId edge = f.SetupInterceptor(query);
        auto future = f.GetYdb()->ExecuteQueryAsync(query, myPool);
        auto runtime = f.GetYdb()->GetRuntime();

        // Stop a request before a real execution to prevent read races from a .sys/query_sessions
        // The request is now 'parked' in the interceptor actor
        auto ev = runtime->GrabEdgeEvent<NWorkload::TEvContinueRequest>(edge);

        TQuerySessionReader reader(f.GetYdb());
        reader.FetchAll(query);

        // Continue a request execution by sending the event back to the interceptor
        runtime->Send(new IEventHandle(ev->Sender, edge, ev->Release().Release()));

        auto result = future.GetResult();
        TSampleQueries::TSelect42::CheckResult(result);

        return reader;
    }

    Y_UNIT_TEST(TestWmStateNone) {  
        auto reader = ReadQuerySessionAfterState(IWmSessionUpdater::NONE);

        UNIT_ASSERT_VALUES_EQUAL(reader.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmState, "NONE");
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmPoolId, "my_pool");
        UNIT_ASSERT(!reader[0].WmEnterTime);
        UNIT_ASSERT(!reader[0].WmExitTime);
    }

    Y_UNIT_TEST(TestWmStatePending) {
        auto reader = ReadQuerySessionAfterState(IWmSessionUpdater::PENDING);

        UNIT_ASSERT_VALUES_EQUAL(reader.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmState, "PENDING");
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmPoolId, "my_pool");
        UNIT_ASSERT(reader[0].WmEnterTime);
        UNIT_ASSERT(!reader[0].WmExitTime);
    }

    Y_UNIT_TEST(TestWmStateExited) {
        auto reader = ReadQuerySessionAfterState(IWmSessionUpdater::EXITED);

        UNIT_ASSERT_VALUES_EQUAL(reader.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmState, "EXITED");
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmPoolId, "my_pool");
        UNIT_ASSERT(reader[0].WmEnterTime);
        UNIT_ASSERT(reader[0].WmExitTime);
        UNIT_ASSERT(*reader[0].WmEnterTime < *reader[0].WmExitTime);
    }

    ///
    /// Verifies the full lifecycle of a queued request within a Resource Pool.
    /// The test simulates a pool limit exhaustion (limit=1), forcing the second
    /// query into a 'DELAYED' state. It then ensures that once the first query
    /// is released, the second one correctly transitions to 'EXITED' state,
    /// preserving its original 'WmEnterTime' and recording a valid 'WmExitTime'.
    ///
    Y_UNIT_TEST(TestWmStateDelayedToExited) {
        TQuerySessionTestFixture f("my_pool", IWmSessionUpdater::EXITED, /*limit=*/1);
        auto myPool = TQueryRunnerSettings().PoolId("my_pool");
        auto& runtime = *f.GetYdb()->GetRuntime();

        const TString qHanging = "SELECT 11;";
        const TString qDelayed = TSampleQueries::TSelect42::Query;

        TActorId edgeHanging = f.SetupInterceptor(qHanging);
        TActorId edgeDelayed = f.SetupInterceptor(qDelayed);

        // Stop a first request before a real execution to fill limits
        auto hangingRequest = f.GetYdb()->ExecuteQueryAsync(qHanging, myPool);
        auto evHanging = runtime.GrabEdgeEvent<NWorkload::TEvContinueRequest>(edgeHanging);
        
        // Run a second request which has to be placed in a Delayed Queue
        auto delayedRequest = f.GetYdb()->ExecuteQueryAsync(qDelayed, myPool);

        // Wait for condition
        f.GetYdb()->WaitPoolState({.DelayedRequests = 1, .RunningRequests = 1});

        TQuerySessionReader reader(f.GetYdb());
        reader.FetchAll(qDelayed);
        UNIT_ASSERT_VALUES_EQUAL(reader.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmState, "DELAYED");
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmPoolId, "my_pool");
        UNIT_ASSERT(reader[0].WmEnterTime);
        UNIT_ASSERT(!reader[0].WmExitTime);

        // Continue the first request and wait for the second request to be about to execute
        runtime.Send(new IEventHandle(evHanging->Sender, edgeHanging, evHanging->Release().Release()));
        auto evDelayed = runtime.GrabEdgeEvent<NWorkload::TEvContinueRequest>(edgeDelayed);
 
        TQuerySessionReader reader2(f.GetYdb());
        reader2.FetchAll(qDelayed);
        UNIT_ASSERT_VALUES_EQUAL(reader2.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reader2[0].WmState, "EXITED");
        UNIT_ASSERT_VALUES_EQUAL(reader2[0].WmPoolId, "my_pool");
        UNIT_ASSERT(reader2[0].WmEnterTime);
        UNIT_ASSERT(reader2[0].WmExitTime);
        UNIT_ASSERT(*reader2[0].WmEnterTime < *reader2[0].WmExitTime);
        UNIT_ASSERT_VALUES_EQUAL(*reader[0].WmEnterTime, *reader2[0].WmEnterTime);

        // Continue the second request
        runtime.Send(new IEventHandle(evDelayed->Sender, edgeDelayed, evDelayed->Release().Release()));

        hangingRequest.GetResult();
        TSampleQueries::TSelect42::CheckResult(delayedRequest.GetResult());
    }

    ///
    /// Verifies that session metadata and Workload Manager (WM) state are correctly
    /// cleaned up and reset when a KQP session is reused for a new query.
    /// The test ensures that reusing a session via TableClient properly invokes
    /// WmState->Clean(), resetting timestamps and states in the .sys/query_sessions table.
    ///
    Y_UNIT_TEST(TestWmStateCleanupOnSessionReuse) {
        using namespace NYdb::NTable;

        // We use PENDING state because AttachQueryText calls Clean() before placing request into pool
        TQuerySessionTestFixture f(NResourcePool::DEFAULT_POOL_ID, IWmSessionUpdater::PENDING);
        auto& runtime = *f.GetYdb()->GetRuntime();

        auto db = f.GetYdb()->GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto sessionId = session.GetId();

        // Execute the first query
        const TString qFirst = "SELECT 11;";
        TActorId edge1 = f.SetupInterceptor(qFirst);
        auto future1 = session.ExecuteDataQuery(qFirst, TTxControl::BeginTx().CommitTx());
        auto ev1 = runtime.GrabEdgeEvent<NWorkload::TEvContinueRequest>(edge1);
        
        TQuerySessionReader reader(f.GetYdb());
        reader.FetchAll(qFirst);
        UNIT_ASSERT_VALUES_EQUAL(reader.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reader[0].SessionId, sessionId);
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmState, "PENDING");
        UNIT_ASSERT(reader[0].WmEnterTime);
        UNIT_ASSERT(!reader[0].WmExitTime);

        // Resume and wait for the first query to finish
        runtime.Send(new IEventHandle(ev1->Sender, edge1, ev1->Release().Release()));
        UNIT_ASSERT(future1.GetValueSync().IsSuccess());

        // Execute the second query in the same session
        const TString qSecond = "SELECT 12;";
        TActorId edge2 = f.SetupInterceptor(qSecond);
        auto future2 = session.ExecuteDataQuery(qSecond, TTxControl::BeginTx().CommitTx());
        auto ev2 = runtime.GrabEdgeEvent<NWorkload::TEvContinueRequest>(edge2);

        TQuerySessionReader reader2(f.GetYdb());
        reader2.FetchAll(qSecond);
        UNIT_ASSERT_VALUES_EQUAL(reader2.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reader2[0].SessionId, sessionId);
        UNIT_ASSERT_VALUES_EQUAL(reader2[0].WmState, "PENDING");
        UNIT_ASSERT(reader2[0].WmEnterTime);
        UNIT_ASSERT(!reader2[0].WmExitTime);
        UNIT_ASSERT(*reader[0].WmEnterTime < *reader2[0].WmEnterTime);
        
        // Resume and wait for the second query to finish
        runtime.Send(new IEventHandle(ev2->Sender, edge2, ev2->Release().Release()));
        UNIT_ASSERT(future2.GetValueSync().IsSuccess());
    }
}

}  // namespace NKikimr::NKqp
