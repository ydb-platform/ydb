#include "kqp_workload_service.h"

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/resource_pools/resource_pool_settings.h>


namespace NKikimr::NKqp {

using namespace NActors;
using namespace NKikimrConfig;
using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NWorkload;
using namespace Tests;


namespace  {

constexpr TDuration FUTURE_WAIT_TIMEOUT = TDuration::Minutes(2);


// Query runner

struct TQueryRunnerSettings {
    using TSelf = TQueryRunnerSettings;

    // Query settings
    FLUENT_FIELD_DEFAULT(TString, PoolId, "");
    FLUENT_FIELD(std::optional<TString>, UserSID);

    // Runner settings
    FLUENT_FIELD_DEFAULT(bool, HangUpDuringExecution, false);
    FLUENT_FIELD(std::optional<TActorId>, OwnerActorId);
    FLUENT_FIELD(std::optional<TActorId>, InFlightCoordinatorActorId);

    // Runner validations
    FLUENT_FIELD_DEFAULT(bool, ExecutionExpected, true);
};

struct TQueryRunnerResult {
    NKikimrKqp::TEvQueryResponse Response;
    std::vector<Ydb::ResultSet> ResultSets;

    EStatus GetStatus() const {
        return static_cast<EStatus>(Response.GetYdbStatus());
    }

    NYql::TIssues GetIssues() const {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(Response.GetResponse().GetQueryIssues(), issues);
        return issues;
    }

    Ydb::ResultSet GetResultSet(size_t resultIndex) const {
        UNIT_ASSERT_C(resultIndex < ResultSets.size(), "Invalid result set index");
        return ResultSets[resultIndex];
    }

    const std::vector<Ydb::ResultSet>& GetResultSets() const {
        return ResultSets;
    }
};

struct TQueryRunnerResultAsync {
    TFuture<TQueryRunnerResult> AsyncResult;
    TActorId QueryRunnerActor;
    TActorId EdgeActor;  // Receives events about progress from QueryRunnerActor

    TQueryRunnerResult GetResult() const {
        return AsyncResult.GetValue(FUTURE_WAIT_TIMEOUT);
    }

    TFuture<void> GetFuture() const {
        return AsyncResult.IgnoreResult();
    }

    bool HasValue() const {
        return AsyncResult.HasValue();
    }
};

struct TEvQueryRunner {
    // Event ids
    enum EEv : ui32 {
        EvExecutionStarted = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvContinueExecution,
        EvContinueExecutionResponse,
        EvExecutionFinished,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events
    struct TEvExecutionStarted : public TEventLocal<TEvExecutionStarted, EvExecutionStarted> {
    };

    struct TEvContinueExecution : public TEventLocal<TEvContinueExecution, EvContinueExecution> {
    };

    struct TEvContinueExecutionResponse : public TEventLocal<TEvContinueExecutionResponse, EvContinueExecutionResponse> {
    };

    struct TEvExecutionFinished : public TEventLocal<TEvExecutionFinished, EvExecutionFinished> {
    };
};

class TQueryRunnerActor : public TActorBootstrapped<TQueryRunnerActor> {
public:
    TQueryRunnerActor(std::unique_ptr<TEvKqp::TEvQueryRequest> request, TPromise<TQueryRunnerResult> promise, const TQueryRunnerSettings& settings)
        : Request_(std::move(request))
        , Promise_(promise)
        , Settings_(settings)
    {}

    void Bootstrap() {
        ActorIdToProto(SelfId(), Request_->Record.MutableRequestActorId());
        Send(MakeKqpProxyID(SelfId().NodeId()), std::move(Request_));

        Become(&TQueryRunnerActor::StateFunc);
    }

    void Handle(TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        UNIT_ASSERT_C(Settings_.ExecutionExpected_ || ExecutionContinued, "Execution is not expected and was not continued");
        if (!ExecutionStartReported) {
            ExecutionStartReported = true;
            if (Settings_.OwnerActorId_) {
                Send(*Settings_.OwnerActorId_, new TEvQueryRunner::TEvExecutionStarted());
            }
            if (Settings_.InFlightCoordinatorActorId_) {
                Send(*Settings_.InFlightCoordinatorActorId_, new TEvQueryRunner::TEvExecutionStarted());
            }
        }

        auto response = std::make_unique<TEvKqpExecuter::TEvStreamDataAck>();
        response->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        response->Record.SetFreeSpace(std::numeric_limits<i64>::max());

        auto resultSetIndex = ev->Get()->Record.GetQueryResultIndex();
        if (resultSetIndex >= Result_.ResultSets.size()) {
            Result_.ResultSets.resize(resultSetIndex + 1);
        }

        for (auto& row : *ev->Get()->Record.MutableResultSet()->mutable_rows()) {
            *Result_.ResultSets[resultSetIndex].add_rows() = std::move(row);
        }
        *Result_.ResultSets[resultSetIndex].mutable_columns() = ev->Get()->Record.GetResultSet().columns();

        if (!Settings_.HangUpDuringExecution_ || ExecutionContinued) {
            Send(ev->Sender, response.release());
        } else {
            DelayedAckQueue.emplace(new IEventHandle(ev->Sender, SelfId(), response.release()));
        }
    }

    void Handle(TEvKqp::TEvQueryResponse::TPtr& ev) {
        if (Settings_.InFlightCoordinatorActorId_) {
            Send(*Settings_.InFlightCoordinatorActorId_, new TEvQueryRunner::TEvExecutionFinished());
        }

        Result_.Response = ev->Get()->Record.GetRef();
        Promise_.SetValue(Result_);
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvKqpExecuter::TEvStreamData, Handle);
        hFunc(TEvKqp::TEvQueryResponse, Handle);
        hFunc(TEvQueryRunner::TEvContinueExecution, Handle);
    )

private:
    void Handle(TEvQueryRunner::TEvContinueExecution::TPtr& ev) {
        Y_ENSURE(!ExecutionContinued, "Got second continue execution event");

        ExecutionContinued = true;
        while (!DelayedAckQueue.empty()) {
            Send(DelayedAckQueue.front().release());
            DelayedAckQueue.pop();
        }

        Send(ev->Sender, new TEvQueryRunner::TEvContinueExecutionResponse());
    }

private:
    std::unique_ptr<TEvKqp::TEvQueryRequest> Request_;
    TPromise<TQueryRunnerResult> Promise_;

    TQueryRunnerResult Result_;
    std::queue<std::unique_ptr<IEventHandle>> DelayedAckQueue;
    bool ExecutionStartReported = false;
    bool ExecutionContinued = false;

    const TQueryRunnerSettings Settings_;
};


// In flight coordinator

class TInFlightCoordinatorActor : public TActor<TInFlightCoordinatorActor> {
    using TBase = TActor<TInFlightCoordinatorActor>;

public:
    TInFlightCoordinatorActor(ui32 numberRequests, ui32 expectedInFlight)
        : TBase(&TInFlightCoordinatorActor::StateFunc)
        , ExpectedInFlight(expectedInFlight)
        , RequestsRemains(numberRequests)
    {
        Y_ENSURE(RequestsRemains > 0, "At least one request should be started");
        PendingRequests.reserve(expectedInFlight);
        RunningRequests.reserve(expectedInFlight);
    }

    void Handle(TEvQueryRunner::TEvExecutionStarted::TPtr& ev) {
        const auto& runnerId = ev->Sender;
        Y_ENSURE(!PendingRequests.contains(runnerId) && !RunningRequests.contains(runnerId), "Unexpected InFlightCoordinator state");

        PendingRequests.insert(runnerId);
        UNIT_ASSERT_LE_C(PendingRequests.size(), ExpectedInFlight, "Too many in flight requests");
        TryStartNextChunk();
    }

    void Handle(TEvQueryRunner::TEvExecutionFinished::TPtr& ev) {
        const auto& runnerId = ev->Sender;
        Y_ENSURE(RunningRequests.contains(runnerId), "Unexpected InFlightCoordinator state");

        RunningRequests.erase(runnerId);
        TryStartNextChunk();

        --RequestsRemains;
        if (!RequestsRemains) {
            Y_ENSURE(PendingRequests.size() + RunningRequests.size() == 0, "To many requests started");
            PassAway();
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvQueryRunner::TEvExecutionStarted, Handle);
        hFunc(TEvQueryRunner::TEvExecutionFinished, Handle);
        IgnoreFunc(TEvQueryRunner::TEvContinueExecutionResponse);
    )

private:
    void TryStartNextChunk() {
        if (!RunningRequests.empty() || PendingRequests.size() != std::min(ExpectedInFlight, RequestsRemains)) {
            return;
        }

        while (!PendingRequests.empty()) {
            const auto runnerId = *PendingRequests.begin();
            PendingRequests.erase(runnerId);

            Send(runnerId, new TEvQueryRunner::TEvContinueExecution());
            RunningRequests.insert(runnerId);
        }
    }

private:
    const ui32 ExpectedInFlight;

    ui32 RequestsRemains = 0;
    std::unordered_set<TActorId> PendingRequests;
    std::unordered_set<TActorId> RunningRequests;
};


// Ydb setup

struct TYdbSetupSettings {
    using TSelf = TYdbSetupSettings;

    // Cluster settings
    FLUENT_FIELD_DEFAULT(ui32, NodeCount, 1);
    FLUENT_FIELD_DEFAULT(TString, DomainName, "Root");

    // Cluster configuration
    FLUENT_FIELD_DEFAULT(bool, EnableResourcePools, true);

    // Default queue settings
    FLUENT_FIELD_DEFAULT(TString, PoolId, "sample_pool_id");
    FLUENT_FIELD_DEFAULT(i32, ConcurrentQueryLimit, -1);
    FLUENT_FIELD_DEFAULT(i32, QueueSize, -1);
    FLUENT_FIELD_DEFAULT(TDuration, QueryCancelAfter, TDuration::Zero());
};

class TWorkloadServiceYdbSetup {
private:
    TAppConfig GetAppConfig() const {
        NResourcePool::TPoolSettings defaultPoolConfig;
        defaultPoolConfig.ConcurrentQueryLimit = Settings_.ConcurrentQueryLimit_;
        defaultPoolConfig.QueueSize = Settings_.QueueSize_;
        defaultPoolConfig.QueryCancelAfter = Settings_.QueryCancelAfter_;

        TWorkloadManagerConfig workloadManagerConfig;
        workloadManagerConfig.Pools.insert({Settings_.PoolId_, defaultPoolConfig});
        SetWorkloadManagerConfig(workloadManagerConfig);

        TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableResourcePools(Settings_.EnableResourcePools_);

        return appConfig;
    }

    void SetLoggerSettings(TServerSettings& serverSettings) const {
        auto loggerInitializer = [](TTestActorRuntime& runtime) {
            runtime.SetLogPriority(NKikimrServices::KQP_WORKLOAD_SERVICE, NLog::EPriority::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::KQP_SESSION, NLog::EPriority::PRI_DEBUG);
        };

        serverSettings.SetLoggerInitializer(loggerInitializer);
    }

    TServerSettings GetServerSettings(ui32 grpcPort) {
        ui32 msgBusPort = PortManager_.GetPort();
        TAppConfig appConfig = GetAppConfig();

        auto serverSettings = TServerSettings(msgBusPort)
            .SetGrpcPort(grpcPort)
            .SetNodeCount(Settings_.NodeCount_)
            .SetDomainName(Settings_.DomainName_)
            .SetAppConfig(appConfig)
            .SetFeatureFlags(appConfig.GetFeatureFlags());

        SetLoggerSettings(serverSettings);

        return serverSettings;
    }

    void InitializeServer() {
        ui32 grpcPort = PortManager_.GetPort();
        TServerSettings serverSettings = GetServerSettings(grpcPort);

        Server_ = std::make_unique<TServer>(serverSettings);
        Server_->EnableGRpc(grpcPort);
        GetRuntime()->SetDispatchTimeout(FUTURE_WAIT_TIMEOUT);

        Client_ = std::make_unique<TClient>(serverSettings);
        Client_->InitRootScheme();

        YdbDriver_ = std::make_unique<TDriver>(TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << grpcPort)
            .SetDatabase(TStringBuilder() << "/" << Settings_.DomainName_));
    }

public:
    explicit TWorkloadServiceYdbSetup(const TYdbSetupSettings& settings)
        : Settings_(settings)
    {
        InitializeServer();

        QueryClient_ = std::make_unique<TQueryClient>(*YdbDriver_);
    }

    TExecuteQueryResult ExecuteQueryGrpc(const TString& query, const TString& poolId = "") const {
        TExecuteQuerySettings settings;
        settings.PoolId(poolId ? poolId : Settings_.PoolId_);

        return QueryClient_->ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
    }

    TQueryRunnerResult ExecuteQuery(const TString& query, TQueryRunnerSettings settings = TQueryRunnerSettings()) const {
        SetupDefaultSettings(settings, false);

        auto event = GetQueryRequest(query, settings);
        auto promise = NewPromise<TQueryRunnerResult>();
        GetRuntime()->Register(new TQueryRunnerActor(std::move(event), promise, settings));

        return promise.GetFuture().GetValue(FUTURE_WAIT_TIMEOUT);
    }

    TQueryRunnerResultAsync ExecuteQueryAsync(const TString& query, TQueryRunnerSettings settings) const {
        SetupDefaultSettings(settings, true);
        Y_ENSURE(settings.OwnerActorId_, "Owner actor is required for async execution");

        auto event = GetQueryRequest(query, settings);
        auto promise = NewPromise<TQueryRunnerResult>();
        auto runerActor = GetRuntime()->Register(new TQueryRunnerActor(std::move(event), promise, settings));

        return {.AsyncResult = promise.GetFuture(), .QueryRunnerActor = runerActor, .EdgeActor = *settings.OwnerActorId_};
    }

    TActorId CreateInFlightCoordinator(ui32 numberRequests, ui32 expectedInFlight) const {
        return GetRuntime()->Register(new TInFlightCoordinatorActor(numberRequests, expectedInFlight));
    }

    TTestActorRuntime* GetRuntime() const {
        return Server_->GetRuntime();
    }

    // Async query execution actions
    TEvQueryRunner::TEvExecutionStarted::TPtr WaitQueryExecution(const TQueryRunnerResultAsync& query) const {
        return GetRuntime()->GrabEdgeEvent<TEvQueryRunner::TEvExecutionStarted>(query.EdgeActor, FUTURE_WAIT_TIMEOUT);
    }

    TEvQueryRunner::TEvContinueExecutionResponse::TPtr ContinueQueryExecution(const TQueryRunnerResultAsync& query) const {
        GetRuntime()->Send(query.QueryRunnerActor, query.EdgeActor, new TEvQueryRunner::TEvContinueExecution());
        return GetRuntime()->GrabEdgeEvent<TEvQueryRunner::TEvContinueExecutionResponse>(query.EdgeActor, FUTURE_WAIT_TIMEOUT);
    }

private:
    void SetupDefaultSettings(TQueryRunnerSettings& settings, bool asyncExecution) const {
        UNIT_ASSERT_C(!settings.HangUpDuringExecution_ || asyncExecution, "Hang up during execution is not supported for sync execution");

        if (!settings.PoolId_) {
            settings.PoolId(Settings_.PoolId_);
        }
        if (!settings.OwnerActorId_ && asyncExecution) {
            settings.OwnerActorId(GetRuntime()->AllocateEdgeActor());
        }
    }

    std::unique_ptr<TEvKqp::TEvQueryRequest> GetQueryRequest(const TString& query, const TQueryRunnerSettings& settings) const {
        auto event = std::make_unique<TEvKqp::TEvQueryRequest>();
        if (settings.UserSID_) {
            event->Record.SetUserToken(NACLib::TUserToken("", *settings.UserSID_, {}).SerializeAsString());
        }

        auto request = event->Record.MutableRequest();
        request->SetQuery(query);
        request->SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
        request->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->SetDatabase(Settings_.DomainName_);
        request->SetPoolId(settings.PoolId_);

        return event;
    }

private:
    const TYdbSetupSettings Settings_;

    TPortManager PortManager_;
    std::unique_ptr<TServer> Server_;
    std::unique_ptr<TClient> Client_;
    std::unique_ptr<TDriver> YdbDriver_;
    std::unique_ptr<TQueryClient> QueryClient_;
};


// Test queries

struct TSampleQueries {
    template <typename TResult>
    static void CheckSuccess(const TResult& result) {
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    struct TSelect42 {
        static constexpr char Query[] = "SELECT 42;";

        template <typename TResult>
        static void CheckResult(const TResult& result) {
            CheckSuccess(result);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            CompareYson("[[42]]", FormatResultSetYson(result.GetResultSet(0)));
        }
    };
};

}  // anonymous namespace

Y_UNIT_TEST_SUITE(KqpWorkloadService) {
    Y_UNIT_TEST(WorkloadServiceDisabledByFeatureFlag) {
        TWorkloadServiceYdbSetup ydb(TYdbSetupSettings().EnableResourcePools(false));
        TSampleQueries::TSelect42::CheckResult(ydb.ExecuteQueryGrpc(TSampleQueries::TSelect42::Query, "another_pool_id"));
    }

    Y_UNIT_TEST(ValidationOfQueueSize) {
        auto settings = TYdbSetupSettings()
            .ConcurrentQueryLimit(1)
            .QueueSize(1);
        TWorkloadServiceYdbSetup ydb(settings);

        // Query will be executed after hanging
        auto hangingRequest = ydb.ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().HangUpDuringExecution(true));
        ydb.WaitQueryExecution(hangingRequest);

        // One of these requests should be rejected by QueueSize
        auto firstRequest = ydb.ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().ExecutionExpected(false));
        auto secondRequest = ydb.ExecuteQueryAsync(TSampleQueries::TSelect42::Query, TQueryRunnerSettings().ExecutionExpected(false));
        WaitAny(firstRequest.GetFuture(), secondRequest.GetFuture()).GetValue(FUTURE_WAIT_TIMEOUT);

        if (secondRequest.HasValue()) {
            std::swap(firstRequest, secondRequest);
        }
        UNIT_ASSERT_C(firstRequest.HasValue(), "One of two requests shoud be rejected");
        UNIT_ASSERT_C(!secondRequest.HasValue(), "One of two requests shoud be placed in pool");

        auto firstResult = firstRequest.GetResult();
        UNIT_ASSERT_VALUES_EQUAL_C(firstResult.GetStatus(), EStatus::OVERLOADED, firstResult.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(firstResult.GetIssues().ToString(), TStringBuilder() << "Too many pending requests for pool " << settings.PoolId_);

        ydb.ContinueQueryExecution(secondRequest);
        ydb.ContinueQueryExecution(hangingRequest);
        TSampleQueries::TSelect42::CheckResult(hangingRequest.GetResult());
        TSampleQueries::TSelect42::CheckResult(secondRequest.GetResult());
    }

    Y_UNIT_TEST(ValidationOfQueryCancelAfter) {
        TWorkloadServiceYdbSetup ydb(TYdbSetupSettings()
            .QueryCancelAfter(TDuration::Seconds(10)));

        auto result = ydb.ExecuteQueryAsync(
            TSampleQueries::TSelect42::Query,
            TQueryRunnerSettings().HangUpDuringExecution(true)
        ).GetResult();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::CANCELLED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Request timeout exceeded, cancelling after");
    }

    Y_UNIT_TEST(ValidationOfConcurrentQueryLimit) {
        const ui64 activeCountLimit = 5;
        const ui64 overallCountLimit = 50;
        TWorkloadServiceYdbSetup ydb(TYdbSetupSettings()
            .ConcurrentQueryLimit(activeCountLimit)
            .QueueSize(overallCountLimit));

        const auto& inFlightCoordinator = ydb.CreateInFlightCoordinator(overallCountLimit, activeCountLimit);

        std::vector<TQueryRunnerResultAsync> asyncResults;
        asyncResults.reserve(overallCountLimit);
        for (size_t i = 0; i < overallCountLimit; ++i) {
            asyncResults.emplace_back(ydb.ExecuteQueryAsync(TSampleQueries::TSelect42::Query,
                TQueryRunnerSettings()
                    .HangUpDuringExecution(true)
                    .InFlightCoordinatorActorId(inFlightCoordinator)
            ));
        }

        for (const auto& asyncResult : asyncResults) {
            TSampleQueries::TSelect42::CheckResult(asyncResult.GetResult());
        }
    }
}

}  // namespace NKikimr::NKqp
