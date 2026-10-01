#include <fmt/format.h>

#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/service/service.h>
#include <ydb/services/workload_manager/session_updater.h>
#include <ydb/services/workload_manager/ut/common/workload_service_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace NKikimr::NWorkloadManager {

namespace {

using namespace NWorkloadManager;
using namespace NYdb;
using namespace NActors;

///
/// Wraps the real ISessionUpdater and suppresses state transitions beyond
/// FinalState. This lets a test freeze the WM state visible in
/// .sys/query_sessions at a chosen level.
///
class TWmSessionUpdaterWrapper : public ISessionUpdater {
public:
    TWmSessionUpdaterWrapper(EState finalState, std::shared_ptr<ISessionUpdater> inner)
        : Inner(inner)
        , FinalState(finalState)
    {}

    void SetRequestState(EState state, TInstant timestamp) override {
        if (state > FinalState) {
            return;
        }
        Inner->SetRequestState(state, timestamp);
    }

    void SetPoolContext(TString poolId, TString classifiedBy) override {
        Inner->SetPoolContext(std::move(poolId), std::move(classifiedBy));
    }

private:
    std::shared_ptr<ISessionUpdater> Inner;
    EState FinalState;
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
            hFunc(NWorkloadManager::TEvContinueRequest, HandleContinueRequest);
            default:
                Send(ev->Forward(SessionActorId));
        }
    }

    void HandleContinueRequest(NWorkloadManager::TEvContinueRequest::TPtr& ev) {
        senderId = ev->Sender;
        Send(new IEventHandle(EdgeActorId, SelfId(), ev->Release().Release()));
        Become(&TSessionProxyActor::StateWait);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NWorkloadManager::TEvContinueRequest, HandleRelease);
            default:
                Send(ev->Forward(SessionActorId));
        }
    }

    void HandleRelease(NWorkloadManager::TEvContinueRequest::TPtr& ev) {
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
        ISessionUpdater::EState finalState,
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
            hFunc(NWorkloadManager::TEvPlaceRequestIntoPool, HandlePlaceRequest);
            default:
                Send(ev->Forward(WorkloadServiceId));
        }
    }

    void HandlePlaceRequest(NWorkloadManager::TEvPlaceRequestIntoPool::TPtr& ev) {
        auto* msg = ev->Get();
        auto wrapper = std::make_shared<TWmSessionUpdaterWrapper>(FinalState, msg->WmSessionUpdater);

        auto* proxyMsg = new NWorkloadManager::TEvPlaceRequestIntoPool(
            msg->QueryId,
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

        Send(new IEventHandle(WorkloadServiceId, senderForWorkload, proxyMsg, 0, ev->Cookie));
    }

private:
    ISessionUpdater::EState FinalState;
    TActorId WorkloadServiceId;
    std::shared_ptr<TInterceptorRules> Rules;
};

///
/// Reads .sys/query_sessions for EXECUTING sessions.
///
class TQuerySessionReader {
public:
    struct Row {
        std::optional<std::string> SessionId;
        std::optional<std::string> State;
        std::optional<std::string> WmPoolId;
        std::optional<std::string> WmClassifiedBy;
        std::optional<TInstant> StateChangeAt;
        std::optional<TInstant> QueryStartAt;
        // Deprecated columns kept in the read for regression assertions.
        std::optional<std::string> WmState;
        std::optional<TInstant> WmEnterTime;
        std::optional<TInstant> WmExitTime;
    };

public:
    TQuerySessionReader(TIntrusivePtr<IYdbSetup> ydb)
        : Ydb(ydb)
    {}

    void FetchAll(TStringBuf query) {
        Fetch(TStringBuilder() << "Query = '" << query << "'");
    }

    void FetchBySessionId(TStringBuf sessionId) {
        Fetch(TStringBuilder() << "SessionId = '" << sessionId << "'");
    }

private:
    void Fetch(TStringBuf predicate) {
        using namespace fmt::literals;

        Results.clear();
        TString q = fmt::format(R"(
            SELECT SessionId, State, WmPoolId, WmClassifiedBy, StateChangeAt, QueryStartAt,
                   WmState, WmEnterTime, WmExitTime
            FROM `.sys/query_sessions`
            WHERE {predicate}
            ORDER BY SessionId
        )",
            "predicate"_a = predicate
        );

        auto result = Ydb->ExecuteQuery(q, TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto rs = result.GetResultSet(0);
        auto parser = std::make_unique<NYdb::TResultSetParser>(rs);

        while (parser->TryNextRow()) {
            Results.push_back(Row{
                .SessionId       = parser->ColumnParser("SessionId").GetOptionalUtf8(),
                .State           = parser->ColumnParser("State").GetOptionalUtf8(),
                .WmPoolId        = parser->ColumnParser("WmPoolId").GetOptionalUtf8(),
                .WmClassifiedBy  = parser->ColumnParser("WmClassifiedBy").GetOptionalUtf8(),
                .StateChangeAt   = parser->ColumnParser("StateChangeAt").GetOptionalTimestamp(),
                .QueryStartAt    = parser->ColumnParser("QueryStartAt").GetOptionalTimestamp(),
                .WmState         = parser->ColumnParser("WmState").GetOptionalUtf8(),
                .WmEnterTime     = parser->ColumnParser("WmEnterTime").GetOptionalTimestamp(),
                .WmExitTime      = parser->ColumnParser("WmExitTime").GetOptionalTimestamp(),
            });
        }
    }

public:
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
    TQuerySessionTestFixture(const TString myPoolId, ISessionUpdater::EState state, size_t limit = 10)
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
        auto workloadServiceId = MakeServiceId(runtime.GetNodeId(0));
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
    ISessionUpdater::EState State;
    TIntrusivePtr<IYdbSetup> Ydb;
    TActorId ProxyActorId;
    std::shared_ptr<TInterceptorRules> Rules;
};

}  // anonymous namespace

Y_UNIT_TEST_SUITE(KqpWorkloadServiceQuerySessions) {
    ///
    /// Executes a query in a user-supplied pool, freezing WM at `state` so the
    /// session row can be observed at that step of the queue lifecycle.
    ///
    TQuerySessionReader ReadQuerySessionAfterState(ISessionUpdater::EState state) {
        TQuerySessionTestFixture f("my_pool", state);
        auto myPool = TQueryRunnerSettings().PoolId("my_pool");
        const TString& query = TSampleQueries::TSelect42::Query;
        TActorId edge = f.SetupInterceptor(query);
        auto future = f.GetYdb()->ExecuteQueryAsync(query, myPool);
        auto runtime = f.GetYdb()->GetRuntime();

        auto ev = runtime->GrabEdgeEvent<NWorkloadManager::TEvContinueRequest>(edge);

        TQuerySessionReader reader(f.GetYdb());
        reader.FetchAll(query);

        runtime->Send(new IEventHandle(ev->Sender, edge, ev->Release().Release()));

        auto result = future.GetResult();
        TSampleQueries::TSelect42::CheckResult(result);

        return reader;
    }

    ///
    /// RFC C2: request parked in the local pending queue.
    /// Displayed State must be QUEUED; QueryStartAt is NULL until execution starts.
    ///
    Y_UNIT_TEST(TestStateQueuedPending) {
        auto reader = ReadQuerySessionAfterState(ISessionUpdater::PENDING);

        UNIT_ASSERT_VALUES_EQUAL(reader.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reader[0].State, "QUEUED");
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmPoolId, "my_pool");
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmClassifiedBy, "USER");
        UNIT_ASSERT(reader[0].StateChangeAt);
        UNIT_ASSERT(!reader[0].QueryStartAt);
    }

    ///
    /// RFC C2: request parked in the delayed_requests table.
    /// Collapses to the same displayed State = QUEUED as PENDING.
    ///
    Y_UNIT_TEST(TestStateQueuedDelayed) {
        TQuerySessionTestFixture f("my_pool", ISessionUpdater::DELAYED, /*limit=*/1);
        auto myPool = TQueryRunnerSettings().PoolId("my_pool");
        auto& runtime = *f.GetYdb()->GetRuntime();

        const TString qHanging = "SELECT 11;";
        const TString qDelayed = TSampleQueries::TSelect42::Query;

        TActorId edgeHanging = f.SetupInterceptor(qHanging);
        TActorId edgeDelayed = f.SetupInterceptor(qDelayed);

        auto hangingRequest = f.GetYdb()->ExecuteQueryAsync(qHanging, myPool);
        auto evHanging = runtime.GrabEdgeEvent<NWorkloadManager::TEvContinueRequest>(edgeHanging);

        auto delayedRequest = f.GetYdb()->ExecuteQueryAsync(qDelayed, myPool);
        f.GetYdb()->WaitPoolState({.DelayedRequests = 1, .RunningRequests = 1});

        TQuerySessionReader reader(f.GetYdb());
        reader.FetchAll(qDelayed);
        UNIT_ASSERT_VALUES_EQUAL(reader.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reader[0].State, "QUEUED");
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmPoolId, "my_pool");
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmClassifiedBy, "USER");
        UNIT_ASSERT(reader[0].StateChangeAt);
        UNIT_ASSERT(!reader[0].QueryStartAt);
        // Deprecated columns must stay NULL even while the request is DELAYED.
        UNIT_ASSERT(!reader[0].WmState);
        UNIT_ASSERT(!reader[0].WmEnterTime);
        UNIT_ASSERT(!reader[0].WmExitTime);

        runtime.Send(new IEventHandle(evHanging->Sender, edgeHanging, evHanging->Release().Release()));
        auto evDelayed = runtime.GrabEdgeEvent<NWorkloadManager::TEvContinueRequest>(edgeDelayed);
        runtime.Send(new IEventHandle(evDelayed->Sender, edgeDelayed, evDelayed->Release().Release()));

        hangingRequest.GetResult();
        TSampleQueries::TSelect42::CheckResult(delayedRequest.GetResult());
    }

    ///
    /// RFC C3: after the request leaves the queue, displayed State is EXECUTING
    /// and StateChangeAt / QueryStartAt both equal the queue-exit time.
    ///
    Y_UNIT_TEST(TestStateExecutingAfterQueue) {
        auto reader = ReadQuerySessionAfterState(ISessionUpdater::EXITED);

        UNIT_ASSERT_VALUES_EQUAL(reader.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reader[0].State, "EXECUTING");
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmPoolId, "my_pool");
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmClassifiedBy, "USER");
        UNIT_ASSERT(reader[0].StateChangeAt);
        UNIT_ASSERT(reader[0].QueryStartAt);
        UNIT_ASSERT_VALUES_EQUAL(*reader[0].StateChangeAt, *reader[0].QueryStartAt);
    }

    ///
    /// RFC C3.1: request that hits the default pool through the classifier fallback
    /// must show WmClassifiedBy = "NONE". Uses TableClient because IYdbSetup's
    /// runner always attaches a PoolId to the request; TableClient sends no
    /// PoolId, letting the classifier reach its Default branch.
    ///
    Y_UNIT_TEST(TestStateExecutingDefaultPool) {
        using namespace NYdb::NTable;
        TQuerySessionTestFixture f(NResourcePool::DEFAULT_POOL_ID, ISessionUpdater::EXITED);
        const TString& query = TSampleQueries::TSelect42::Query;
        TActorId edge = f.SetupInterceptor(query);
        auto& runtime = *f.GetYdb()->GetRuntime();

        auto session = f.GetYdb()->GetTableClient().CreateSession().GetValueSync().GetSession();
        auto future = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx());
        auto ev = runtime.GrabEdgeEvent<NWorkloadManager::TEvContinueRequest>(edge);

        TQuerySessionReader reader(f.GetYdb());
        reader.FetchAll(query);
        UNIT_ASSERT_VALUES_EQUAL(reader.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reader[0].State, "EXECUTING");
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmPoolId, NResourcePool::DEFAULT_POOL_ID);
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmClassifiedBy, "NONE");

        runtime.Send(new IEventHandle(ev->Sender, edge, ev->Release().Release()));
        UNIT_ASSERT(future.GetValueSync().IsSuccess());
    }

    ///
    /// RFC C3.2: request routed by a classifier match must show
    /// WmClassifiedBy = "CLASSIFIER: <name>".
    ///
    Y_UNIT_TEST(TestStateExecutingClassifiedByClassifier) {
        using namespace NYdb::NTable;
        TQuerySessionTestFixture f("my_pool", ISessionUpdater::EXITED);

        const TString classifierId = "my_pool_classifier";
        auto ddl = f.GetYdb()->ExecuteQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL CLASSIFIER )" << classifierId << R"( WITH (
                RESOURCE_POOL="my_pool",
                RANK=20
            );
        )", TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID));
        UNIT_ASSERT_VALUES_EQUAL_C(ddl.GetStatus(), NYdb::EStatus::SUCCESS, ddl.GetIssues().ToString());
        f.GetYdb()->WaitForClassifierPropagation();

        const TString& query = TSampleQueries::TSelect42::Query;
        TActorId edge = f.SetupInterceptor(query);
        auto& runtime = *f.GetYdb()->GetRuntime();

        auto session = f.GetYdb()->GetTableClient().CreateSession().GetValueSync().GetSession();
        auto future = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx());
        auto ev = runtime.GrabEdgeEvent<NWorkloadManager::TEvContinueRequest>(edge);

        TQuerySessionReader reader(f.GetYdb());
        reader.FetchAll(query);
        UNIT_ASSERT_VALUES_EQUAL(reader.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reader[0].State, "EXECUTING");
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmPoolId, "my_pool");
        UNIT_ASSERT_VALUES_EQUAL(reader[0].WmClassifiedBy, TString("CLASSIFIER: ") + classifierId);

        runtime.Send(new IEventHandle(ev->Sender, edge, ev->Release().Release()));
        UNIT_ASSERT(future.GetValueSync().IsSuccess());
    }

    ///
    /// RFC C1: after the query finishes the session goes IDLE — Query is cleared,
    /// WmPoolId / WmClassifiedBy / QueryStartAt must all be NULL.
    ///
    Y_UNIT_TEST(TestStateIdle) {
        using namespace NYdb::NTable;
        TQuerySessionTestFixture f("my_pool", ISessionUpdater::EXITED);
        auto db = f.GetYdb()->GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        const auto sessionId = session.GetId();

        auto res = session.ExecuteDataQuery(TSampleQueries::TSelect42::Query,
                                            TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        TQuerySessionReader reader(f.GetYdb());
        reader.FetchBySessionId(sessionId);
        UNIT_ASSERT_VALUES_EQUAL(reader.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reader[0].State, "IDLE");
        UNIT_ASSERT(!reader[0].WmPoolId);
        UNIT_ASSERT(!reader[0].WmClassifiedBy);
        UNIT_ASSERT(!reader[0].QueryStartAt);
    }

    ///
    /// RFC C4: with ResourcePools disabled the classifier is never invoked, so
    /// SetPoolContext is never called — WmPoolId / WmClassifiedBy stay NULL for the
    /// whole session lifecycle. State can still be IDLE or EXECUTING.
    ///
    Y_UNIT_TEST(TestStateWmDisabled) {
        using namespace NYdb::NTable;
        auto ydb = TYdbSetupSettings()
            .NodeCount(1)
            .EnableResourcePools(false)
            .EnableStreamingQueries(false)
            .Create();

        auto session = ydb->GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto sessionId = session.GetId();

        auto res = session.ExecuteDataQuery(TSampleQueries::TSelect42::Query,
                                            TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        TQuerySessionReader reader(ydb);
        reader.FetchBySessionId(sessionId);
        UNIT_ASSERT_VALUES_EQUAL(reader.Size(), 1);
        UNIT_ASSERT(!reader[0].WmPoolId);
        UNIT_ASSERT(!reader[0].WmClassifiedBy);
    }

    ///
    /// RFC: WmState / WmEnterTime / WmExitTime are deprecated and MUST always be NULL,
    /// regardless of the actual queue state.
    ///
    Y_UNIT_TEST(TestDeprecatedColumnsAlwaysNull) {
        for (auto state : {ISessionUpdater::NONE, ISessionUpdater::PENDING, ISessionUpdater::EXITED}) {
            auto reader = ReadQuerySessionAfterState(state);
            UNIT_ASSERT_VALUES_EQUAL(reader.Size(), 1);
            UNIT_ASSERT_C(!reader[0].WmState,     "WmState must be NULL for state=" << ui32(state));
            UNIT_ASSERT_C(!reader[0].WmEnterTime, "WmEnterTime must be NULL for state=" << ui32(state));
            UNIT_ASSERT_C(!reader[0].WmExitTime,  "WmExitTime must be NULL for state=" << ui32(state));
        }
    }
}

}  // namespace NKikimr::NWorkloadManager
