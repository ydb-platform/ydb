#include <ydb/core/kqp/executer_actor/kqp_planner.h>
#include <ydb/core/kqp/node_service/kqp_query_control_plane.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {
namespace {

constexpr ui64 TxId = 42;
constexpr ui64 InitialMemory = 1_MB;
constexpr TStringBuf CreationError = "injected compute actor creation failure";

struct TCreationState {
    ui32 Attempts = 0;
    ui32 Aborted = 0;
    std::weak_ptr<IQueryQuotaManager> QueryQuota;
    TIntrusivePtr<NRm::TTxState> Tx;
};

class TTestComputeActor : public TActor<TTestComputeActor> {
public:
    TTestComputeActor(const NComputeActor::IKqpNodeComputeActorFactory::TCreateArgs& args,
        std::shared_ptr<TCreationState> creationState)
        : TActor(&TThis::StateFunc)
        , Executer(args.ExecuterId)
        , TaskId(args.Task->GetId())
        , InitialMemoryLimit(args.InitialMemoryLimit)
        , QueryQuota(args.QueryQuotaManager)
        , NodeState(args.State)
        , CreationState(std::move(creationState))
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqp::TEvAbortExecution, Handle);
        }
    }

    void Handle(TEvKqp::TEvAbortExecution::TPtr&) {
        QueryQuota->FreeTasks(1, InitialMemoryLimit);
        if (NodeState) {
            NodeState->OnTaskFinished(TxId, Executer, TaskId, false);
        }
        ++CreationState->Aborted;
        PassAway();
    }

private:
    const TActorId Executer;
    const ui64 TaskId;
    const ui64 InitialMemoryLimit;
    const TQueryQuotaManagerPtr QueryQuota;
    const std::shared_ptr<TNodeState> NodeState;
    const std::shared_ptr<TCreationState> CreationState;
};

class TThrowingComputeActorFactory : public NComputeActor::IKqpNodeComputeActorFactory {
public:
    TThrowingComputeActorFactory(ui32 failOnAttempt, bool unknownException)
        : FailOnAttempt(failOnAttempt)
        , UnknownException(unknownException)
    {
        MkqlLightProgramMemoryLimit = InitialMemory;
        MkqlHeavyProgramMemoryLimit = InitialMemory;
    }

    TActorId CreateKqpComputeActor(TCreateArgs&& args) override {
        CreationState->QueryQuota = args.QueryQuotaManager;
        CreationState->Tx = args.TxInfo;
        if (++CreationState->Attempts == FailOnAttempt) {
            if (UnknownException) {
                throw 42;
            }
            ythrow yexception() << CreationError;
        }
        return TlsActivationContext->Register(new TTestComputeActor(args, CreationState));
    }

    void ApplyConfig(const NKikimrConfig::TTableServiceConfig::TResourceManager&) override {}
    bool GetVerboseMemoryLimitException() override { return false; }
    TShardsScanningPolicy GetShardsScanningPolicy() override { return {}; }

    const std::shared_ptr<TCreationState> CreationState = std::make_shared<TCreationState>();

private:
    const ui32 FailOnAttempt;
    const bool UnknownException;
};

void CheckResourcesReleased(TTestActorRuntime& runtime, const TCreationState& state, ui32 startedTasks) {
    runtime.WaitFor("compute actor creation cleanup", [&] {
        return state.Aborted == startedTasks && state.QueryQuota.expired();
    });
    UNIT_ASSERT(state.Tx);
    UNIT_ASSERT_VALUES_EQUAL(state.Tx->TxExecutionUnits.load(), 0);
    UNIT_ASSERT_VALUES_EQUAL(state.Tx->TxExternalDataQueryMemory.load(), 0);
    UNIT_ASSERT_VALUES_EQUAL(state.Tx->TxScanQueryMemory.load(), 0);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpComputeActorCreation) {
    Y_UNIT_TEST_TWIN(NodeServiceFailure, UnknownException) {
        for (ui32 failOnAttempt : {1u, 2u, 3u}) {
            TKikimrRunner kikimr(TKikimrSettings().SetUseRealThreads(false).SetWithSampleTables(false));
            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            const auto executer = runtime.AllocateEdgeActor();
            auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
            auto nodeState = std::make_shared<TNodeState>();
            auto resourceManager = GetKqpResourceManager(runtime.GetNodeId());
            auto factory = std::make_shared<TThrowingComputeActorFactory>(failOnAttempt, UnknownException);
            std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> caFactory = factory;
            const auto manager = runtime.Register(CreateKqpQueryManager(counters, nodeState, resourceManager, caFactory, true, true));
            bool cancelled = false;
            TActorId registeredManager;
            UNIT_ASSERT(nodeState->AddRequest(executer, manager, cancelled, registeredManager));

            // Also cover aborting tasks started by an earlier request for the same executer.
            if (failOnAttempt == 3) {
                auto earlierStart = MakeHolder<TEvKqpNode::TEvStartKqpTasksRequest>();
                earlierStart->Record.SetTxId(TxId);
                earlierStart->Record.SetStartAllOrFail(true);
                earlierStart->Record.AddTasks()->SetId(1);
                earlierStart->Record.AddTasks()->SetId(2);
                runtime.Send(new IEventHandle(manager, executer, earlierStart.Release()));
                auto earlierReply = runtime.GrabEdgeEventRethrow<TEvKqpNode::TEvStartKqpTasksResponse>(executer);
                UNIT_ASSERT_VALUES_EQUAL(earlierReply->Get()->Record.StartedTasksSize(), 2);
            }
            const ui64 firstTaskId = failOnAttempt == 3 ? 3 : 1;
            auto start = MakeHolder<TEvKqpNode::TEvStartKqpTasksRequest>();
            start->Record.SetTxId(TxId);
            start->Record.SetStartAllOrFail(true);
            for (ui64 taskId = firstTaskId; taskId < firstTaskId + 3; ++taskId) {
                start->Record.AddTasks()->SetId(taskId);
            }
            constexpr ui64 requestId = 123;
            runtime.Send(new IEventHandle(manager, executer, start.Release(), 0, requestId));
            auto reply = runtime.GrabEdgeEventRethrow<TEvKqpNode::TEvStartKqpTasksResponse>(executer);
            const auto& record = reply->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), TxId);
            UNIT_ASSERT_VALUES_EQUAL(record.StartedTasksSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(record.NotStartedTasksSize(), 3);
            for (ui32 i = 0; i < 3; ++i) {
                const auto& task = record.GetNotStartedTasks(i);
                UNIT_ASSERT_VALUES_EQUAL(task.GetTaskId(), firstTaskId + i);
                UNIT_ASSERT_VALUES_EQUAL(task.GetRequestId(), requestId);
                UNIT_ASSERT_EQUAL(task.GetReason(), NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR);
                UNIT_ASSERT_STRING_CONTAINS(task.GetMessage(), "Failed to create compute actor");
                if (!UnknownException) {
                    UNIT_ASSERT_STRING_CONTAINS(task.GetMessage(), CreationError);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(factory->CreationState->Attempts, failOnAttempt);
            CheckResourcesReleased(runtime, *factory->CreationState, failOnAttempt - 1);
            TActorId foundExecuter;
            UNIT_ASSERT(!nodeState->ValidateKqpExecuterId(ToString(executer), foundExecuter));
        }
    }

    Y_UNIT_TEST_TWIN(PlannerFailure, UnknownException) {
        for (ui32 failOnAttempt : {1u, 2u}) {
            TKikimrRunner kikimr(TKikimrSettings().SetUseRealThreads(false).SetWithSampleTables(false));
            auto& runtime = *kikimr.GetTestServer().GetRuntime();
            const auto executer = runtime.AllocateEdgeActor();
            auto resourceManager = GetKqpResourceManager(runtime.GetNodeId());
            auto factory = std::make_shared<TThrowingComputeActorFactory>(failOnAttempt, UnknownException);
            std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> caFactory = factory;

            runtime.RunCall([&] {
                NKqpProto::TKqpPhyTx txProto;
                txProto.AddStages();
                auto txHolder = std::make_shared<TKqpPhyTxHolder>(nullptr, &txProto, nullptr, nullptr);
                const IKqpGateway::TPhysicalTxData tx(txHolder, nullptr);
                const TVector<IKqpGateway::TPhysicalTxData> transactions;
                const NKikimrConfig::TTableServiceConfig::TAggregationConfig aggregation;
                TKqpTasksGraph graph("/Root", transactions, nullptr, {}, aggregation, nullptr, {}, nullptr, false);
                graph.GetMeta().UserRequestContext = MakeIntrusive<TUserRequestContext>();
                graph.GetMeta().MayRunTasksLocally = true;
                const NYql::NDq::TStageId stageId(0, 0);
                graph.AddStageInfo(TStageInfo(stageId, 0, 0, TStageInfoMeta(tx)));
                auto& stage = graph.GetStageInfo(stageId);
                for (ui32 i = 0; i < 3; ++i) {
                    auto& task = graph.AddTask(stage);
                    task.Meta.ExpectedNodeId = executer.NodeId();
                }

                NWilson::TSpan span;
                NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig retries;
                TKqpPlanner planner({
                    .TasksGraph = graph,
                    .TxId = TxId,
                    .Executer = executer,
                    .Database = "/Root",
                    .UserToken = {},
                    .Deadline = {},
                    .StatsMode = Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE,
                    .StatsReportingSettings = {},
                    .RlPath = {},
                    .ExecuterSpan = span,
                    .ExecuterRetriesConfig = retries,
                    .MkqlMemoryLimit = 0,
                    .AsyncIoFactory = nullptr,
                    .AllowSinglePartitionOpt = false,
                    .FederatedQuerySetup = std::nullopt,
                    .ResourceManager_ = resourceManager,
                    .CaFactory_ = caFactory,
                    .BlockTrackingMode = {},
                    .ArrayBufferMinFillPercentage = {},
                    .BufferPageAllocSize = {},
                    .CheckpointCoordinator = {},
                    .EnableWatermarks = false,
                });
                auto error = planner.PlanExecution();
                UNIT_ASSERT(error);
                UNIT_ASSERT(error->Recipient == executer);
                auto* abort = error->Get<TEvKqp::TEvAbortExecution>();
                UNIT_ASSERT_EQUAL(abort->Record.GetStatusCode(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
                if (!UnknownException) {
                    UNIT_ASSERT_STRING_CONTAINS(abort->GetIssues().ToString(), CreationError);
                }
                UNIT_ASSERT_VALUES_EQUAL(planner.GetAllComputeActors().size(), failOnAttempt - 1);
                // The executer uses the planner's acknowledged actors to abort tasks after receiving this error.
                for (const auto& actorId : planner.GetAllComputeActors()) {
                    runtime.Send(new IEventHandle(actorId, executer,
                        new TEvKqp::TEvAbortExecution(abort->Record.GetStatusCode(), abort->GetIssues())));
                }
                return true;
            });
            UNIT_ASSERT_VALUES_EQUAL(factory->CreationState->Attempts, failOnAttempt);
            CheckResourcesReleased(runtime, *factory->CreationState, failOnAttempt - 1);
        }
    }
}

} // namespace NKikimr::NKqp
