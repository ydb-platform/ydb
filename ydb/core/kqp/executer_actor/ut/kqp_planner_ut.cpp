#include <ydb/core/fq/libs/checkpointing/events/events.h>
#include <ydb/core/kqp/executer_actor/kqp_planner.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpPlanner) {
    Y_UNIT_TEST_TWIN(NoStreamingReadinessDuringShutdown, ComputeStateFirst) {
        TKikimrRunner kikimr(TKikimrSettings().SetUseRealThreads(false).SetWithSampleTables(false));
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto executer = runtime.AllocateEdgeActor();
        const auto firstActor = runtime.AllocateEdgeActor();
        const auto lastActor = runtime.AllocateEdgeActor();
        const TString database = "/Root";
        const NKikimrConfig::TTableServiceConfig config;

        for (const bool shuttingDown : {false, true}) {
            const auto nodesManager = runtime.AllocateEdgeActor();
            runtime.RunCall([&] {
                NKqpProto::TKqpPhyTx txProto;
                txProto.AddStages();
                auto txHolder = std::make_shared<TKqpPhyTxHolder>(nullptr, &txProto, nullptr, nullptr);
                const IKqpGateway::TPhysicalTxData tx(txHolder, nullptr);
                const TVector<IKqpGateway::TPhysicalTxData> transactions;
                TKqpTasksGraph graph(database, transactions, nullptr, config.GetResourceManager(),
                    config.GetAggregationConfig(), nullptr, {}, nullptr, false);
                graph.GetMeta().UserRequestContext = MakeIntrusive<TUserRequestContext>();
                graph.GetMeta().UserRequestContext->IsStreamingQuery = true;
                const NYql::NDq::TStageId stageId(0, 0);
                graph.AddStageInfo(TStageInfo(stageId, 0, 0, TStageInfoMeta(tx)));
                auto& stage = graph.GetStageInfo(stageId);
                auto addTask = [&] {
                    auto& task = graph.AddTask(stage);
                    task.Meta.ExpectedNodeId = executer.NodeId();
                    return task.Id;
                };
                const auto firstTaskId = addTask();
                const auto lastTaskId = addTask();

                NWilson::TSpan span;
                TKqpPlanner planner({
                    .TasksGraph = graph,
                    .TxId = 1,
                    .Executer = executer,
                    .Database = database,
                    .UserToken = {},
                    .Deadline = {},
                    .StatsMode = Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE,
                    .StatsReportingSettings = {},
                    .RlPath = {},
                    .ExecuterSpan = span,
                    .ExecuterRetriesConfig = config.GetExecuterRetriesConfig(),
                    .MkqlMemoryLimit = 0,
                    .AsyncIoFactory = nullptr,
                    .AllowSinglePartitionOpt = false,
                    .FederatedQuerySetup = std::nullopt,
                    .ResourceManager_ = {},
                    .CaFactory_ = {},
                    .BlockTrackingMode = {},
                    .ArrayBufferMinFillPercentage = {},
                    .BufferPageAllocSize = {},
                    .CheckpointCoordinator = {},
                    .EnableWatermarks = false,
                    .StreamingQueryNodesManager = nodesManager,
                });
                UNIT_ASSERT(!planner.PlanExecution());
                UNIT_ASSERT_VALUES_EQUAL(planner.GetPendingComputeTasks().size(), 2);
                UNIT_ASSERT(planner.AcknowledgeCA(firstTaskId, firstActor, nullptr));

                if (shuttingDown) {
                    planner.BeginShutdown();
                }
                NYql::NDqProto::TEvComputeActorState state;
                state.SetTaskId(lastTaskId);
                state.SetState(NYql::NDqProto::COMPUTE_STATE_EXECUTING);
                UNIT_ASSERT(planner.AcknowledgeCA(lastTaskId, lastActor, ComputeStateFirst ? &state : nullptr));
                UNIT_ASSERT(!planner.AcknowledgeCA(lastTaskId, lastActor, ComputeStateFirst ? nullptr : &state));

                UNIT_ASSERT(planner.GetPendingComputeTasks().empty());
                UNIT_ASSERT(planner.GetPendingComputeActors().contains(lastActor));
                UNIT_ASSERT(planner.CompletedCA(lastTaskId, lastActor));
                UNIT_ASSERT(planner.CompletedCA(firstTaskId, firstActor));
                UNIT_ASSERT(planner.GetPendingComputeActors().empty());
                return true;
            });

            using TEvReadyState = NFq::TEvCheckpointCoordinator::TEvReadyState;
            if (!shuttingDown) {
                auto ready = runtime.GrabEdgeEventRethrow<TEvReadyState>(nodesManager, TDuration::Seconds(1));
                UNIT_ASSERT(ready);
                UNIT_ASSERT_VALUES_EQUAL(ready->Get()->Tasks.size(), 2);
            }
            UNIT_ASSERT(!runtime.GrabEdgeEventRethrow<TEvReadyState>(nodesManager, TDuration::MilliSeconds(100)));
        }
    }
}

} // namespace NKikimr::NKqp
