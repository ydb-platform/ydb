#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/testlib/helpers.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

namespace NKikimr {
namespace NStat {

Y_UNIT_TEST_SUITE(AnalyzeStatistics) {

    Y_UNIT_TEST_TWIN(Analyze, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(AnalyzeMultiColumnStatistics, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareMultiColumnTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        CheckMultiColumnStatisticsProbes(env, runtime, tableInfo.PathId, {2, 3});
    }

    Y_UNIT_TEST_TWIN(AnalyzeTwoTables, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table1 = PrepareTable(env, "Database", "Table1", ColumnShard);
        const auto table2 = PrepareTable(env, "Database", "Table2", ColumnShard);

        Analyze(runtime, table1.SaTabletId, {table1.PathId, table2.PathId});

        ValidateStatistics(runtime, table1.PathId);
        ValidateStatistics(runtime, table2.PathId);
    }

    Y_UNIT_TEST_TWIN(AnalyzeEmptyTable, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = CreateEmptyTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        // An empty table produces no statistics.
        std::vector<TCountMinSketchProbes> expected = {
            { .Tag = 1, .Probes = std::nullopt },
            { .Tag = 2, .Probes = std::nullopt },
        };
        CheckCountMinSketch(runtime, tableInfo.PathId, expected);
    }

    Y_UNIT_TEST_TWIN(AnalyzeServerless, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Database", "/Root/Shared");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId}, "operationId", "/Root/Database");

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(AnalyzeUsesBatchPool, ColumnShard) {
        TTestEnv env(1, 1, ColumnShard, [](Tests::TServerSettings& settings) {
            if constexpr (ColumnShard) {
                using TExecutor = NKikimrConfig::TActorSystemConfig::TExecutor;
                auto& actorSystemConfig = *settings.AppConfig->MutableActorSystemConfig();
                actorSystemConfig.ClearExecutor();

                const auto addPool = [&](const TString& name, const TExecutor::EType type) {
                    auto& executor = *actorSystemConfig.AddExecutor();
                    executor.SetType(type);
                    executor.SetName(name);
                    executor.SetThreads(1);
                    if (type == TExecutor::BASIC) {
                        executor.SetSpinThreshold(1);
                    }
                };

                addPool("System", TExecutor::BASIC);
                actorSystemConfig.SetSysExecutor(0);
                addPool("User", TExecutor::BASIC);
                actorSystemConfig.SetUserExecutor(1);
                addPool("Batch", TExecutor::BASIC);
                actorSystemConfig.SetBatchExecutor(2);
                addPool("IO", TExecutor::IO);
                actorSystemConfig.SetIoExecutor(3);
            }
        });
        auto& runtime = *env.GetServer().GetRuntime();
        if (ColumnShard) {
            for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
                UNIT_ASSERT_VALUES_UNEQUAL(
                    runtime.GetAppData(nodeIndex).UserPoolId, runtime.GetAppData(nodeIndex).BatchPoolId);
            }
        }
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        size_t taskRequests = 0;
        auto tasksObserver = runtime.AddObserver<NKqp::TEvKqpNode::TEvStartKqpTasksRequest>([&](auto& ev) {
            UNIT_ASSERT(ev->Get()->Record.GetUseBatchPool());
            ++taskRequests;
        });
        size_t scans = 0;
        auto scansObserver = runtime.AddObserver<TEvDataShard::TEvKqpScan>([&](auto& ev) {
            UNIT_ASSERT(ev->Get()->Record.GetUseBatchPool());
            ++scans;
        });

        const auto getReceivedConveyorTasks = [&](const ui32 poolId) {
            ui64 result = 0;
            for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
                auto counters = runtime.GetAppData(nodeIndex).Counters
                    ->GetSubgroup("actor_system_pool_id", ::ToString(poolId))
                    ->GetSubgroup("module_id", "COMPOSITE_CONVEYOR");
                if (const auto histogram = counters->FindHistogram("Histogram/ReceiveTask/Duration/Us")) {
                    const auto snapshot = histogram->Snapshot();
                    for (size_t i = 0; i < snapshot->Count(); ++i) {
                        result += snapshot->Value(i);
                    }
                }
            }
            return result;
        };
        const ui32 batchPoolId = runtime.GetAppData().BatchPoolId;
        const ui64 batchTasksBeforeAnalyze = ColumnShard ? getReceivedConveyorTasks(batchPoolId) : 0;

        Analyze(runtime, tableInfo.SaTabletId, {tableInfo.PathId});

        if (ColumnShard) {
            UNIT_ASSERT_GT(getReceivedConveyorTasks(batchPoolId), batchTasksBeforeAnalyze);
        } else {
            UNIT_ASSERT_GT(taskRequests, 0);
            UNIT_ASSERT_GT(scans, 0);
        }
    }

    Y_UNIT_TEST_TWIN(QueryDoesNotUseBatchPool, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        PrepareTable(env, "Database", "Table", ColumnShard);

        size_t scans = 0;
        auto scansObserver = runtime.AddObserver<TEvDataShard::TEvKqpScan>([&](auto& ev) {
            UNIT_ASSERT(!ev->Get()->Record.GetUseBatchPool());
            ++scans;
        });
        THashSet<TActorId> userConveyorServices;
        THashSet<TActorId> batchConveyorServices;
        for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
            const auto nodeId = runtime.GetNodeId(nodeIndex);
            userConveyorServices.emplace(NConveyorComposite::TServiceOperator::MakeServiceId(nodeId, false));
            batchConveyorServices.emplace(NConveyorComposite::TServiceOperator::MakeServiceId(nodeId, true));
        }
        THashSet<ui64> userScanProcesses;
        THashSet<ui64> batchScanProcesses;
        auto conveyorProcessesObserver = runtime.AddObserver<NConveyorComposite::TEvExecution::TEvRegisterProcess>([&](auto& ev) {
            if (ev->Get()->GetCategory() != NConveyorComposite::ESpecialTaskCategory::Scan) {
                return;
            }
            if (userConveyorServices.contains(ev->Recipient)) {
                userScanProcesses.emplace(ev->Get()->GetInternalProcessId());
            } else if (batchConveyorServices.contains(ev->Recipient)) {
                batchScanProcesses.emplace(ev->Get()->GetInternalProcessId());
            }
        });
        size_t userScanTasks = 0;
        size_t batchScanTasks = 0;
        auto conveyorTasksObserver = runtime.AddObserver<NConveyorComposite::TEvExecution::TEvNewTask>([&](auto& ev) {
            if (ev->Get()->GetCategory() != NConveyorComposite::ESpecialTaskCategory::Scan) {
                return;
            }
            if (userScanProcesses.contains(ev->Get()->GetInternalProcessId())) {
                UNIT_ASSERT(userConveyorServices.contains(ev->Recipient));
                ++userScanTasks;
            } else if (batchScanProcesses.contains(ev->Get()->GetInternalProcessId())) {
                UNIT_ASSERT(batchConveyorServices.contains(ev->Recipient));
                ++batchScanTasks;
            }
        });

        ExecuteYqlScript(env, "SELECT COUNT(*) FROM `Root/Database/Table`;");

        UNIT_ASSERT_GT(scans, 0);
        if (ColumnShard) {
            UNIT_ASSERT_GT(userScanProcesses.size(), 0);
            UNIT_ASSERT_GT(userScanTasks, 0);
            UNIT_ASSERT_VALUES_EQUAL(batchScanProcesses.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(batchScanTasks, 0);
        }
    }

    Y_UNIT_TEST_TWIN(AnalyzeSpecificColumns, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        Analyze(runtime, tableInfo.SaTabletId, {{tableInfo.PathId, {1, 2}}});

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(AnalyzeStatus, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        const TString operationId = "operationId";
        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION);

        auto analyzeRequest = MakeAnalyzeRequest({{tableInfo.PathId, {1, 2}}}, operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size(); });

        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_IN_PROGRESS);

        // Check EvRemoteHttpInfo
        {
            auto httpRequest = std::make_unique<NActors::NMon::TEvRemoteHttpInfo>("/app?");
            runtime.SendToPipe(tableInfo.SaTabletId, sender, httpRequest.release(), 0, {});
            auto httpResponse = runtime.GrabEdgeEventRethrow<NActors::NMon::TEvRemoteHttpInfoRes>(sender);
            TString body = httpResponse->Get()->Html;
            Cerr << body << Endl;
            UNIT_ASSERT(body.size() > 500);
            UNIT_ASSERT(body.Contains("ForceTraversals: 1"));
        }

        block.Unblock();
        block.Stop();

        auto analyzeResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(analyzeResponse->Get()->Record.GetOperationId(), operationId);

        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId, NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION);
    }

    Y_UNIT_TEST_TWIN(AnalyzeSameOperationId, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();
        const TString operationId = "operationId";

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        auto tabletPipe = runtime.ConnectToPipe(tableInfo.SaTabletId, sender, 0, {});

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tabletPipe, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size(); });

        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tabletPipe, sender, analyzeRequest2.release());

        block.Unblock();
        block.Stop();

        auto response1 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT(response1);
        UNIT_ASSERT_VALUES_EQUAL(response1->Get()->Record.GetOperationId(), operationId);

        auto response2 = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender, TDuration::Seconds(5));
        UNIT_ASSERT(!response2);
    }

    Y_UNIT_TEST_TWIN(AnalyzeMultiOperationId, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();

        auto GetOperationId = [] (size_t i) { return TStringBuilder() << "operationId" << i; };

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        const size_t numEvents = 10;

        auto tabletPipe = runtime.ConnectToPipe(tableInfo.SaTabletId, sender, 0, {});

        for (size_t i = 0; i < numEvents; ++i) {
            auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId}, GetOperationId(i));
            runtime.SendToPipe(tabletPipe, sender, analyzeRequest.release());
        }

        block.Unblock();
        block.Stop();

        for (size_t i = 0; i < numEvents; ++i) {
            auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetOperationId(), GetOperationId(i));
        }
    }

    Y_UNIT_TEST_TWIN(AnalyzeDeadline, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size(); });
        runtime.AdvanceCurrentTime(TDuration::Days(2));

        auto analyzeResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        const auto& record = analyzeResponse->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetOperationId(), "operationId");
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);
        UNIT_ASSERT(!record.GetIssues().empty());
    }

    Y_UNIT_TEST_TWIN(AnalyzeCancel, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();

        size_t finalResultsCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAnalyzeActorResult>([&](auto& ev) {
            if (ev->Get()->Final) {
                ++finalResultsCount;
            }
        });

        TBlockEvents<TEvDataShard::TEvKqpScan> block(runtime);

        auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId});
        auto operationId = analyzeRequest->Record.GetOperationId();
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvKqpScan", [&]{ return !block.empty(); });

        auto cancelRequest = MakeHolder<TEvStatistics::TEvAnalyzeCancel>();
        cancelRequest->Record.SetOperationId(operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, cancelRequest.Release());

        auto analyzeResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        const auto& record = analyzeResponse->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetOperationId(), "operationId");
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_CANCELLED);
        block.Unblock();
        block.Stop();

        // Do another ANALYZE
        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId}, "operationId2");
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest2.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

        // Make sure that only 1 AnalyzeActor successfully finished.
        UNIT_ASSERT_VALUES_EQUAL(finalResultsCount, 1);
    }

    Y_UNIT_TEST_TWIN(AnalyzeRebootSa, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();
        const TString operationId = "operationId";

        size_t finalResultsCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAnalyzeActorResult>([&](auto& ev) {
            if (ev->Get()->Final) {
                ++finalResultsCount;
            }
        });

        TBlockEvents<TEvDataShard::TEvKqpScan> block(runtime);

        auto analyzeRequest1 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest1.release());

        runtime.WaitFor("TEvKqpScan", [&]{ return !block.empty(); });
        RebootTablet(runtime, tableInfo.SaTabletId, sender);

        // After restart, the operation must still appear as IN_PROGRESS, not ENQUEUED.
        AnalyzeStatus(runtime, sender, tableInfo.SaTabletId, operationId,
            NKikimrStat::TEvAnalyzeStatusResponse::STATUS_IN_PROGRESS);

        block.Unblock();
        block.Stop();

        // Make sure that new operations can be performed
        auto analyzeRequest2 = MakeAnalyzeRequest({tableInfo.PathId}, "operationId2");
        auto sender2 = runtime.AllocateEdgeActor();
        runtime.SendToPipe(tableInfo.SaTabletId, sender2, analyzeRequest2.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender2);

        // Make sure that the old operation is performed after the reattach request
        auto analyzeRequest3 = MakeAnalyzeRequest({tableInfo.PathId}, operationId);
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest3.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

        // Check that AnalyzeActor on the initial tablet instance got cancelled and
        // only 2 AnalyzeActors successfully finished.
        UNIT_ASSERT_VALUES_EQUAL(finalResultsCount, 2);

        ValidateStatistics(runtime, tableInfo.PathId);
    }

    Y_UNIT_TEST_TWIN(DropTableNavigateError, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);

        DropTable(env, "Database", "Table");

        auto result = Analyze(
            runtime, tableInfo.SaTabletId, {tableInfo.PathId},
            "operationId", {}, NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR);

        NYql::TIssues issues;
        NYql::IssuesFromMessage(result.GetIssues(), issues);
        UNIT_ASSERT_C(issues.ToString().Contains("Could not find table"), issues.ToString());

        std::vector<TCountMinSketchProbes> expected = {
            { .Tag = 1, .Probes = std::nullopt },
            { .Tag = 2, .Probes = std::nullopt },
        };
        CheckCountMinSketch(runtime, tableInfo.PathId, expected);
    }

    Y_UNIT_TEST_TWIN(TrickyTableAndColumnNames, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");

        if constexpr (ColumnShard) {
            ExecuteYqlScript(env, R"(
                CREATE TABLE `Root/Database/test\\Test\`test`(
                    key Uint64 NOT NULL,
                    `val-Val` Uint32,
                    PRIMARY KEY (key)
                )
                PARTITION BY HASH(key)
                WITH (STORE = COLUMN);
            )");
        } else {
            ExecuteYqlScript(env, R"(
                CREATE TABLE `Root/Database/test\\Test\`test`(
                    key Uint32,
                    `val-Val` Uint32,
                    PRIMARY KEY (key)
                );
            )");
        }

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, R"(/Root/Database/test\Test`test)", nullptr, &saTabletId);
        // Check that ANALYZE succeeds with tricky table and column names.
        auto result = Analyze(runtime, saTabletId, {pathId}, "operationId");
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
    }

    Y_UNIT_TEST_TWIN(DeleteForceTraversalUsesCorrectKey, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table1 = PrepareTable(env, "Database", "Table1", ColumnShard);
        const auto table2 = PrepareTable(env, "Database", "Table2", ColumnShard);

        auto sender1 = runtime.AllocateEdgeActor();
        auto sender2 = runtime.AllocateEdgeActor();
        auto sender3 = runtime.AllocateEdgeActor();

        TBlockEvents<TEvStatistics::TEvSaveStatisticsQueryResponse> block(runtime);

        auto req1 = MakeAnalyzeRequest({table1.PathId}, "op1");
        runtime.SendToPipe(table1.SaTabletId, sender1, req1.release());
        runtime.WaitFor("TEvSaveStatisticsQueryResponse", [&]{ return block.size() > 0; });

        // Re-send from different sender triggers delete of the queued operation
        auto req2 = MakeAnalyzeRequest({table2.PathId}, "op2");
        runtime.SendToPipe(table1.SaTabletId, sender2, req2.release());
        auto req3 = MakeAnalyzeRequest({table2.PathId}, "op2");
        runtime.SendToPipe(table1.SaTabletId, sender3, req3.release());

        runtime.SimulateSleep(TDuration::MilliSeconds(10));
        RebootTablet(runtime, table1.SaTabletId, sender1);

        block.Unblock();
        block.Stop();

        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender3);

        // op1 must still be enqueued after reboot
        AnalyzeStatus(runtime, sender1, table1.SaTabletId, "op1",
            NKikimrStat::TEvAnalyzeStatusResponse::STATUS_ENQUEUED);
    }

    Y_UNIT_TEST_TWIN(AnalyzeRebootShard, ColumnShard) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto tableInfo = PrepareTable(env, "Database", "Table", ColumnShard);
        auto sender = runtime.AllocateEdgeActor();

        TBlockEvents<TEvDataShard::TEvKqpScan> block(runtime);

        auto analyzeRequest = MakeAnalyzeRequest({tableInfo.PathId});
        runtime.SendToPipe(tableInfo.SaTabletId, sender, analyzeRequest.release());

        runtime.WaitFor("TEvKqpScan", [&]{ return !block.empty(); });
        RebootTablet(runtime, tableInfo.ShardIds[0], sender);
        block.Unblock();
        block.Stop();

        auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);

        ValidateStatistics(runtime, tableInfo.PathId);
    }
}

} // NStat
} // NKikimr
