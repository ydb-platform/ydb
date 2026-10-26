#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/control.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/executer_actor/kqp_planner.h>
#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpExecuter) {

    /* Scenario:
        - Start query execution and receive TEvTxRequest.
        - When sending TEvAddQuery from executer to scheduler, immediately receive TEvAbortExecution.
        - Imitate receiving TEvQueryResponse before receiving self TEvPoison by executer.
        - Check that scheduler got TEvRemoveQuery.
        - Do not crash or get undefined behavior.
     */
    Y_UNIT_TEST(TestSuddenAbortAfterReady) {
        TKikimrSettings settings = TKikimrSettings().SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->MutableComputeSchedulerSettings()->SetAccountDefaultPool(true);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); } );
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); } );

        TActorId executerId, targetId;
        ui8 queries = 0;
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            Cerr << (TStringBuilder() << "Got " << ev->GetTypeName() << " " << ev->Recipient << " " << ev->Sender << Endl);

            if (ev->GetTypeRewrite() == TEvKqpExecuter::TEvTxRequest::EventType) {
                targetId = ActorIdFromProto(ev->Get<TEvKqpExecuter::TEvTxRequest>()->Record.GetTarget());
            }

            if (ev->GetTypeRewrite() == NScheduler::TEvAddQuery::EventType) {
                ++queries;
                executerId = ev->Sender;
                auto* abortExecution = new TEvKqp::TEvAbortExecution(NYql::NDqProto::StatusIds::UNSPECIFIED, NYql::TIssues());
                runtime.Send(new IEventHandle(ev->Sender, targetId, abortExecution));
            }

            if (ev->GetTypeRewrite() == NActors::TEvents::TEvPoison::EventType && ev->Sender == executerId && ev->Recipient == executerId) {
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto future = kikimr.RunInThreadPool([&] {
            return session.ExecuteDataQuery("SELECT COUNT(*) FROM `/Root/TwoShard`;", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        });

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&](IEventHandle& ev) {
            if (ev.GetTypeRewrite() == NScheduler::TEvRemoveQuery::EventType) {
                --queries;
            }
            return (ev.GetTypeRewrite() == TEvKqpExecuter::TEvTxResponse::EventType || ev.GetTypeRewrite() == NScheduler::TEvRemoveQuery::EventType) && !queries;
        });
        runtime.DispatchEvents(opts);

        auto result = runtime.WaitFuture(future);
        UNIT_ASSERT(!result.IsSuccess());
    }

    Y_UNIT_TEST(ResultChannelFlowControlSmoke) {
        TKikimrSettings settings = TKikimrSettings().SetUseRealThreads(false);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });

        auto result = kikimr.RunCall([&] {
            return db.ExecuteQuery("SELECT * FROM `/Root/EightShard`;",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        });

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 24);
    }

    Y_UNIT_TEST_TWIN(ResultChannelFlowControlPauseResume, ClientStats) {
        TKikimrSettings settings = TKikimrSettings().SetUseRealThreads(false);
        settings.AppConfig.MutableFeatureFlags()->SetEnableKqpRuntimeStats(true);

        TKikimrRunner kikimr(settings);
        const ui32 totalRows = 2000;
        kikimr.RunCall([&] { CreateManyShardsTable(kikimr, totalRows, 50, 20); return true; });

        auto client = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return client.CreateSession().GetValueSync().GetSession(); });
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        TActorId executerId;
        THashSet<ui32> pausedChannels;
        bool resuming = false;
        ui64 rowsWhilePaused = 0;
        ui64 rowsAfterResume = 0;
        auto streamSender = runtime.AllocateEdgeActor();
        ui64 queryStatsReports = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvKqp::TEvCurrentQueryStats::EventType) {
                const auto& msg = *ev->Get<TEvKqp::TEvCurrentQueryStats>();
                if (msg.SessionId == session.GetId().c_str()) {
                    ++queryStatsReports;
                }
            }
            if (ev->GetTypeRewrite() == TEvKqpExecuter::TEvStreamData::EventType && ev->Recipient == streamSender) {
                auto& record = ev->Get<TEvKqpExecuter::TEvStreamData>()->Record;
                auto resp = MakeHolder<TEvKqpExecuter::TEvStreamDataAck>(record.GetSeqNo(), record.GetChannelId());
                resp->Record.SetEnough(false);

                executerId = ev->Sender;
                if (!resuming) {
                    pausedChannels.insert(record.GetChannelId());
                    rowsWhilePaused += record.GetResultSet().rows().size();
                    resp->Record.SetFreeSpace(-1);
                } else {
                    rowsAfterResume += record.GetResultSet().rows().size();
                    resp->Record.SetFreeSpace(100_MB);
                }

                runtime.Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto request = NDataShard::NKqpHelpers::MakeStreamRequest(
            streamSender, "SELECT * FROM `/Root/ManyShardsTable`;", false);
        request->Record.MutableRequest()->SetSessionId(session.GetId().c_str());
        request->Record.MutableRequest()->SetDatabase("/Root");
        request->Record.MutableRequest()->SetKeepSession(true);
        if (ClientStats) {
            request->Record.MutableRequest()->SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC);
            request->SetProgressStatsPeriod(TDuration::MilliSeconds(1));
        }
        NDataShard::NKqpHelpers::SendRequest(runtime, streamSender, std::move(request));

        runtime.WaitFor("paused result channel", [&] { return !pausedChannels.empty(); });
        runtime.SimulateSleep(TDuration::Seconds(2));
        UNIT_ASSERT_GE(queryStatsReports, 1);
        runtime.SimulateSleep(TDuration::Seconds(33));
        UNIT_ASSERT_GE(queryStatsReports, 2);
        UNIT_ASSERT(!pausedChannels.empty());
        UNIT_ASSERT_LT_C(rowsWhilePaused, totalRows,
            "not all rows should be delivered while every result channel is paused");

        resuming = true;
        // StreamExecuteScanQuery historically resumes with ChannelId=0 while result channel ids start from 1.
        auto resumeAck = MakeHolder<TEvKqpExecuter::TEvStreamDataAck>(0, 0);
        resumeAck->Record.SetEnough(false);
        resumeAck->Record.SetFreeSpace(100_MB);
        runtime.Send(new IEventHandle(executerId, sender, resumeAck.Release()));

        auto reply = runtime.GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(streamSender);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
            reply->Get()->Record.GetResponse().DebugString());

        UNIT_ASSERT_GT(rowsAfterResume, 0);
        UNIT_ASSERT_VALUES_EQUAL(rowsWhilePaused + rowsAfterResume, totalRows);
    }

    // TODO: Test shard write shuffle.
    /*
    Y_UNIT_TEST(BlindWriteDistributed) {
        TKikimrRunner kikimr;
        auto gateway = MakeIcGateway(kikimr);

        TExprContext ctx;
        auto tx = BuildTxPlan(R"(
            DECLARE $items AS 'List<Struct<Key:Uint64?, Text:String?>>';

            $itemsSource = (
                SELECT Item.Key AS Key, Item.Text AS Text
                FROM (SELECT $items AS List) FLATTEN BY List AS Item
            );

            UPSERT INTO [Root/EightShard]
            SELECT * FROM $itemsSource;
        )", gateway, ctx, kikimr.GetTestServer().GetRuntime()->GetAnyNodeActorSystem());

        LogTxPlan(kikimr, tx);

        auto db = kikimr.GetTableClient();
        auto params = db.GetParamsBuilder()
            .AddParam("$items")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint64(205)
                    .AddMember("Text")
                        .OptionalString("New")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint64(505)
                    .AddMember("Text")
                        .OptionalString("New")
                    .EndStruct()
                .EndList()
                .Build()
            .Build();

        auto paramsMap = GetParamsMap(std::move(params));

        IKqpGateway::TExecPhysicalRequest request;
        request.Transactions.emplace_back(tx.Ref(), GetParamRefsMap(paramsMap));

        auto txResult = gateway->ExecutePhysical(std::move(request)).GetValueSync();
        UNIT_ASSERT(txResult.Success());

        UNIT_ASSERT_VALUES_EQUAL(txResult.ExecuterResult.GetStats().GetAffectedShards(), 2);

        auto session = db.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM [Root/EightShard] WHERE Text = "New" ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"(
            [
                [#;[205u];["New"]];
                [#;[505u];["New"]]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    */
    Y_UNIT_TEST(OversizedTaskReturnsLimitExceeded) {
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings().SetKqpSettings({setting});
        serverSettings.SetNodeCount(2);
        TKikimrRunner kikimr(serverSettings);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString tableName = "/Root/TestLimitSize";
        const int column_count = 49;
        auto partitions =
            TExplicitPartitions().AppendSplitPoints(TValueBuilder()
                                                        .BeginTuple()
                                                        .AddElement()
                                                        .BeginOptional()
                                                        .Uint64(24)
                                                        .EndOptional()
                                                        .EndTuple()
                                                        .Build());

        { /* create table */
            auto tableBuilder = db.GetTableBuilder();
            tableBuilder.AddNonNullableColumn("Key", EPrimitiveType::Uint64);
            for (int i = 0; i < column_count; i++) {
                tableBuilder.AddNonNullableColumn(
                    TStringBuilder() << "Value" << i, EPrimitiveType::Uint64);
            }
            tableBuilder.SetPrimaryKeyColumns({"Key"}).SetPartitionAtKeys(
                partitions);
            auto result = session.CreateTable(tableName, tableBuilder.Build())
                              .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        { /* fill in the table */
            TValueBuilder rows;
            rows.BeginList();
            for (int i = 0; i < 50; ++i) {
                auto &rbuilder =
                    rows.AddListItem().BeginStruct().AddMember("Key").Uint64(i);
                for (int c = 0; c < column_count; c++) {
                    rbuilder.AddMember(TStringBuilder() << "Value" << c)
                        .Uint64(i + c);
                }
                rbuilder.EndStruct();
            }
            rows.EndList();

            auto res = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
            Cerr << res.GetIssues().ToString();
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);
        }

        TStringBuilder query;
        query << "UPDATE `/Root/TestLimitSize` ON SELECT `Key`, ";
        for (int i = 0; i < column_count; i++) {
            if (i)
                query << ", ";
            query << "($s)->(CASE\n";
            for (int j = 0; j < column_count; j++) {
                query << "\tWHEN $s > " << j * 10 << " THEN " << j + 1 << "\n";
            }
            query << "\tELSE " << i << "\n";
            query << "END)(`Value" << i << "`) as `Value" << i << "`\n";
        }
        query << "FROM `/Root/TestLimitSize`;";

        ui64 oldSize = GetMaxTaskSize();
        SetMaxTaskSize(200_KB);
        Y_DEFER { SetMaxTaskSize(oldSize); };
        auto result =
            session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx())
                .ExtractValueSync();

        UNIT_ASSERT(!result.IsSuccess());
        // After the fix: NDqProto::LIMIT_EXCEEDED ->
        // Ydb::PRECONDITION_FAILED. Before the fix this would have been
        // EStatus::ABORTED.
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(),
                                   EStatus::PRECONDITION_FAILED,
                                   result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                                    "Datashard program size limit exceeded");
    }

    enum class ERemoteStart {
        Late,               // the start request is delivered after all
        LateWithoutReply,   // the same, but the executer learns about the compute actor from its first state only
        NotStarted,         // the node replies that the tasks are not started
        Undelivered,        // the start request bounces
    };

    /* Scenario: the tasks of a read-only data query on the second node cannot delay its error response.
        - The physical tx reads a table on the second node. With failLocally it also reads another table on the
          executer node, where Ensure() fails on the first row.
        - TEvStartKqpTasksRequest to the second node is held, so the executer does not know the remote compute actor.
        - With failLocally TerminateComputeActors() skips the remote task (no CA id yet) and the executer waits for it
          in WaitShutdownState. Otherwise the executer is still executing.
        - Then the remote start ends as remoteStart says.
        - The response must come without waiting for WaitCAStatsTimeoutMs, a late compute actor must be aborted
          (with channels v1 it also fails by itself sending to its aborted consumer).
     */
    void CheckRemoteStartDoesNotDelayError(bool channelsV2, bool failLocally, ERemoteStart remoteStart,
        Ydb::StatusIds::StatusCode expectedStatus, const TString& expectedIssue)
    {
        // Simulated time: the response must come much earlier than the timeout
        const TDuration waitCAStatsTimeout = TDuration::Seconds(60);
        const TDuration responseBudget = TDuration::Seconds(10);
        const bool lateStart = remoteStart == ERemoteStart::Late || remoteStart == ERemoteStart::LateWithoutReply;

        TKikimrSettings settings = TKikimrSettings()
            .SetNodeCount(2)
            .SetUseRealThreads(false)
            .SetWithSampleTables(false);
        auto& tableServiceConfig = *settings.AppConfig.MutableTableServiceConfig();
        tableServiceConfig.SetDqChannelVersion(channelsV2 ? 2 : 1);
        tableServiceConfig.MutableQueryLimits()->SetWaitCAStatsTimeoutMs(waitCAStatsTimeout.MilliSeconds());
        // Pin the defaults: with the single partition optimization a read stage runs on the executer node
        tableServiceConfig.SetEnableSimpleProgramsSinglePartitionOptimization(false);
        tableServiceConfig.SetEnableSimpleProgramsSinglePartitionOptimizationBroadPrograms(false);

        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const ui32 executerNodeId = runtime.GetNodeId(0);
        const ui32 remoteNodeId = runtime.GetNodeId(1);

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        for (const char* table : {"LocalTable", "RemoteTable"}) {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteSchemeQuery(TStringBuilder() << "CREATE TABLE `/Root/" << table
                    << "` (Key Uint64, Value Uint64, PRIMARY KEY (Key));").GetValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery("UPSERT INTO `/Root/LocalTable` (Key, Value) VALUES (1ul, 1ul);",
                    TTxControl::BeginTx().CommitTx()).GetValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const auto edge = runtime.AllocateEdgeActor();
        const auto localShards = GetTableShards(&kikimr.GetTestServer(), edge, "/Root/LocalTable");
        const auto remoteShards = GetTableShards(&kikimr.GetTestServer(), edge, "/Root/RemoteTable");
        UNIT_ASSERT_VALUES_EQUAL(localShards.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(remoteShards.size(), 1);

        const TString sessionId = NDataShard::NKqpHelpers::CreateSessionRPC(runtime, "/Root");

        TActorId executerId;
        bool localFailure = false;
        TActorId lateComputeActorId;

        // Whatever nodes the shards are on, read LocalTable on the executer node and RemoteTable on the other one
        auto placeShards = runtime.AddObserver<NShardResolver::TEvShardsResolveStatus>([&](auto& ev) {
            auto& shardsToNodes = ev->Get()->ShardsToNodes;
            if (auto it = shardsToNodes.find(localShards[0]); it != shardsToNodes.end()) {
                it->second = executerNodeId;
            }
            if (auto it = shardsToNodes.find(remoteShards[0]); it != shardsToNodes.end() && !executerId) {
                it->second = remoteNodeId;
                executerId = ev->GetRecipientRewrite();
            }
        });

        // Recipient keeps the service id, the node service forwards the request to its query manager
        NActors::TBlockEvents<TEvKqpNode::TEvStartKqpTasksRequest> remoteStartRequests(runtime, [&](const auto& ev) {
            return executerId && ev->Sender == executerId && ev->Recipient == MakeKqpNodeServiceID(remoteNodeId);
        });

        // The executer handles the event right after the observers
        auto watchFailure = runtime.AddObserver<NYql::NDq::TEvDqCompute::TEvState>([&](auto& ev) {
            if (executerId && ev->GetRecipientRewrite() == executerId && ev->Sender.NodeId() == executerNodeId
                && ev->Get()->Record.GetState() == NYql::NDqProto::COMPUTE_STATE_FAILURE)
            {
                localFailure = true;
            }
        });

        auto watchAbort = runtime.AddObserver<TEvKqp::TEvAbortExecution>([&](auto& ev) {
            if (executerId && ev->Sender == executerId && ev->GetRecipientRewrite().NodeId() == remoteNodeId) {
                lateComputeActorId = ev->GetRecipientRewrite();
            }
        });

        // Unlike runtime.WaitFor() does not abort on timeout
        auto dispatchUntil = [&](std::function<bool()> condition, TDuration simTimeout) {
            if (!condition()) {
                TDispatchOptions opts;
                opts.CustomFinalCondition = condition;
                runtime.DispatchEvents(opts, simTimeout);
            }
            return condition();
        };

        TStringBuilder query;
        if (failLocally) {
            query << R"(SELECT Ensure(Key, Key > 100, "expected_local_failure") AS Key FROM `/Root/LocalTable`;)";
        }
        query << "SELECT Key, Value FROM `/Root/RemoteTable`;";

        auto future = NDataShard::NKqpHelpers::SendRequest(runtime,
            NDataShard::NKqpHelpers::MakeSimpleRequestRPC(query, sessionId, /* txId */ "", /* commitTx */ true), "/Root");

        const bool remoteStartHeld = dispatchUntil([&] {
            return (localFailure || !failLocally) && !remoteStartRequests.empty();
        }, TDuration::Seconds(30));
        UNIT_ASSERT_C(remoteStartHeld, "local failure: " << localFailure
            << ", remote start requests: " << remoteStartRequests.size() << ", executer: " << executerId
            << ", response: " << (future.HasValue() ? future.GetValue().DebugString() : "none"));
        UNIT_ASSERT_VALUES_EQUAL(remoteStartRequests.size(), 1);
        // The aborted local compute actors report meanwhile, only the remote task can keep the executer waiting
        UNIT_ASSERT_C(!dispatchUntil([&] { return future.HasValue(); }, TDuration::Seconds(1)),
            "The executer has replied before the remote start ended: " << future.GetValue().DebugString());

        NActors::TBlockEvents<TEvKqpNode::TEvStartKqpTasksResponse> startReplies(runtime, [&](const auto& ev) {
            return remoteStart == ERemoteStart::LateWithoutReply && ev->GetRecipientRewrite() == executerId;
        });

        const TInstant released = runtime.GetCurrentTime();
        remoteStartRequests.Stop();
        if (lateStart) {
            remoteStartRequests.Unblock();
        } else {
            auto request = std::move(remoteStartRequests.front());
            remoteStartRequests.pop_front();

            IEventBase* reply = nullptr;
            if (remoteStart == ERemoteStart::NotStarted) {
                auto response = MakeHolder<TEvKqpNode::TEvStartKqpTasksResponse>();
                response->Record.SetTxId(request->Get()->Record.GetTxId());
                for (const auto& task : request->Get()->Record.GetTasks()) {
                    auto* notStartedTask = response->Record.AddNotStartedTasks();
                    notStartedTask->SetTaskId(task.GetId());
                    notStartedTask->SetReason(NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY);
                    notStartedTask->SetMessage("remote_node_out_of_memory");
                    notStartedTask->SetRequestId(request->Cookie);
                }
                reply = response.Release();
            } else {
                reply = new NActors::TEvents::TEvUndelivered(TEvKqpNode::TEvStartKqpTasksRequest::EventType,
                    NActors::TEvents::TEvUndelivered::Disconnected);
            }
            runtime.Send(new IEventHandle(executerId, request->GetRecipientRewrite(), reply, 0, request->Cookie),
                /* senderNodeIndex */ 1, /* viaActorSystem */ true);
        }

        UNIT_ASSERT_C(dispatchUntil([&] { return future.HasValue(); }, responseBudget),
            "No response within " << responseBudget << " (WaitCAStatsTimeoutMs " << waitCAStatsTimeout.MilliSeconds() << ")");
        UNIT_ASSERT_LT(runtime.GetCurrentTime() - released, responseBudget);
        const auto response = future.ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), expectedStatus, response.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(response.DebugString(), expectedIssue);

        if (lateStart) {
            UNIT_ASSERT_C(lateComputeActorId, "The executer has not aborted the compute actor started late");
            UNIT_ASSERT_C(dispatchUntil([&] { return !runtime.FindActor(lateComputeActorId); }, responseBudget),
                "Compute actor " << lateComputeActorId << " is still alive");
        }
        if (remoteStart == ERemoteStart::LateWithoutReply) {
            UNIT_ASSERT_VALUES_EQUAL_C(startReplies.size(), 1, "The executer is expected to learn about the compute actor from its state");
        }
    }

    Y_UNIT_TEST_TWIN(LateRemoteStartAfterLocalFailure, ChannelsV2) {
        CheckRemoteStartDoesNotDelayError(ChannelsV2, /* failLocally */ true, ERemoteStart::Late,
            Ydb::StatusIds::PRECONDITION_FAILED, "expected_local_failure");
    }

    Y_UNIT_TEST(LateRemoteStateAfterLocalFailure) {
        CheckRemoteStartDoesNotDelayError(/* channelsV2 */ true, /* failLocally */ true, ERemoteStart::LateWithoutReply,
            Ydb::StatusIds::PRECONDITION_FAILED, "expected_local_failure");
    }

    Y_UNIT_TEST(RemoteTasksNotStartedAfterLocalFailure) {
        CheckRemoteStartDoesNotDelayError(/* channelsV2 */ true, /* failLocally */ true, ERemoteStart::NotStarted,
            Ydb::StatusIds::PRECONDITION_FAILED, "expected_local_failure");
    }

    Y_UNIT_TEST(RemoteStartUndeliveredAfterLocalFailure) {
        CheckRemoteStartDoesNotDelayError(/* channelsV2 */ true, /* failLocally */ true, ERemoteStart::Undelivered,
            Ydb::StatusIds::PRECONDITION_FAILED, "expected_local_failure");
    }

    Y_UNIT_TEST(RemoteTasksNotStarted) {
        CheckRemoteStartDoesNotDelayError(/* channelsV2 */ true, /* failLocally */ false, ERemoteStart::NotStarted,
            Ydb::StatusIds::OVERLOADED, "remote_node_out_of_memory");
    }

    Y_UNIT_TEST(RemoteStartUndelivered) {
        CheckRemoteStartDoesNotDelayError(/* channelsV2 */ true, /* failLocally */ false, ERemoteStart::Undelivered,
            Ydb::StatusIds::UNAVAILABLE, "node is unavailable");
    }
}

Y_UNIT_TEST_SUITE(KqpCurrentExecutionStats) {

using namespace NYql::NDqProto;

    Y_UNIT_TEST(RuntimeStatsPreserveExistingReporting) {
        for (bool enabled : {false, true}) {
            for (bool streaming : {false, true}) {
                for (auto progressPeriod : {TDuration::Zero(), TDuration::MilliSeconds(1), TDuration::Seconds(1)}) {
                    TUserRequestContext context;
                    context.CurrentQueryStatsInterval = enabled ? TDuration::Seconds(30) : TDuration::Zero();
                    context.IsStreamingQuery = streaming;

                    const auto settings = MakeStatsReportingSettings(context, progressPeriod);
                    UNIT_ASSERT_VALUES_EQUAL(settings.CollectCurrentQueryStats, enabled);
                    UNIT_ASSERT_VALUES_EQUAL(settings.WithProgressStats, progressPeriod != TDuration::Zero());
                    UNIT_ASSERT_VALUES_EQUAL(settings.LocalReportStatsSettings.Defined(), enabled);
                    if (enabled) {
                        UNIT_ASSERT_VALUES_EQUAL(settings.LocalReportStatsSettings->MinInterval,
                            progressPeriod ? progressPeriod : TDuration::Seconds(30));
                        UNIT_ASSERT_VALUES_EQUAL(settings.LocalReportStatsSettings->MaxInterval, TDuration::Seconds(30));
                    }
                    UNIT_ASSERT_VALUES_EQUAL(settings.RemoteReportStatsSettings.Defined(), streaming);
                    if (streaming) {
                        UNIT_ASSERT_VALUES_EQUAL(settings.RemoteReportStatsSettings->MinInterval, TDuration::Seconds(1));
                        UNIT_ASSERT_VALUES_EQUAL(settings.RemoteReportStatsSettings->MaxInterval, TDuration::Seconds(5));
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(CurrentQueryStatsPublisherReportsRateAndStaleness) {
        TCurrentQueryStatsPublisher publisher(TMonotonic::Seconds(10), TDuration::Seconds(30));
        TCurrentExecStatsReport report;
        report.Stats.ReadIngressBytes = 600;
        report.SequenceNo = 1;

        UNIT_ASSERT(publisher.Update(report));
        auto first = publisher.Publish(TMonotonic::Seconds(30));
        UNIT_ASSERT(first);
        UNIT_ASSERT(!first->Stats.ReadIngressBytesRate);
        UNIT_ASSERT(first->ScheduleNextPublish);

        auto stale = publisher.Publish(TMonotonic::Seconds(60));
        UNIT_ASSERT(stale);
        UNIT_ASSERT(!stale->Stats.ReadIngressBytesRate);
        UNIT_ASSERT(!stale->ScheduleNextPublish);

        report.SequenceNo = 2;
        UNIT_ASSERT(publisher.Update(report));
        auto zero = publisher.Publish(TMonotonic::Seconds(90));
        UNIT_ASSERT(zero);
        UNIT_ASSERT(zero->Stats.ReadIngressBytesRate);
        UNIT_ASSERT_VALUES_EQUAL(*zero->Stats.ReadIngressBytesRate, 0);

        report.Stats.ReadIngressBytes = 1200;
        report.SequenceNo = 3;
        UNIT_ASSERT(publisher.Update(report));
        auto rate = publisher.Publish(TMonotonic::Seconds(120));
        UNIT_ASSERT(rate);
        UNIT_ASSERT(rate->Stats.ReadIngressBytesRate);
        UNIT_ASSERT_VALUES_EQUAL(*rate->Stats.ReadIngressBytesRate, 20);
    }

TDqComputeActorStats MakeReport(ui64 taskId, ui64 cpu, ui64 memory, ui64 tableBytes, ui64 sourceBytes) {
    TDqComputeActorStats report;
    report.SetMemoryUsage(memory);
    auto& task = *report.AddTasks();
    task.SetTaskId(taskId);
    task.SetCpuTimeUs(cpu);
    task.SetIngressBytes(sourceBytes);
    auto& table = *task.AddTables();
    table.SetTablePath("/Root/Table");
    table.SetReadBytes(tableBytes);
    return report;
}

    void Init(TQueryExecutionStats& stats) {
    stats.TaskCount = 2;
    stats.TaskCount4 = 4;
    stats.ComputeCpuTimeUs.Resize(4);
    stats.StartTs = TInstant::Seconds(10);
    }

    Y_UNIT_TEST(RuntimeStatsAreDisabledByDefault) {
        TQueryExecutionStats stats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE, nullptr, nullptr, 0);
        Init(stats);
        stats.UpdateTaskStats(1, 1, MakeReport(1, 100, 4096, 1000, 700), nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());

        const auto snapshot = stats.GetCurrentQueryResources();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.CpuTimeUs, 100);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.ComputeMemoryBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.ReadIngressBytes, 0);
    }

    Y_UNIT_TEST(CollectWithoutFullProfile) {
        for (auto mode : {Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE,
                          Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC}) {
            TQueryExecutionStats stats(mode, nullptr, nullptr, 0, true);
            Init(stats);
            auto first = MakeReport(1, 100, 4096, 1000, 700);
            auto second = MakeReport(2, 200, 8192, 2000, 1400);
            stats.UpdateTaskStats(1, 1, first, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
            stats.UpdateTaskStats(2, 2, second, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
            stats.UpdateTaskStats(1, 1, first, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
            stats.StorageCpuTimeUs = 50;
            auto snapshot = stats.GetCurrentQueryResources();
            UNIT_ASSERT_VALUES_EQUAL(snapshot.CpuTimeUs, 350);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.ComputeMemoryBytes, 12288);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.ReadIngressBytes, 2100);
            UNIT_ASSERT(stats.StageStats.empty());

            first.SetMemoryUsage(0);
            first.MutableTasks(0)->SetCpuTimeUs(150);
            stats.UpdateTaskStats(1, 1, first, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
            snapshot = stats.GetCurrentQueryResources();
            UNIT_ASSERT_VALUES_EQUAL(snapshot.ComputeMemoryBytes, 8192);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.CpuTimeUs, 400);

            stats.UpdateTaskStats(2, 2, second, nullptr, COMPUTE_STATE_FINISHED, TDuration::Max());
            snapshot = stats.GetCurrentQueryResources();
            UNIT_ASSERT_VALUES_EQUAL(snapshot.ComputeMemoryBytes, 0);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.ReadIngressBytes, 2100);
            UNIT_ASSERT_VALUES_EQUAL(snapshot.CpuTimeUs, 400);
        }
    }

    Y_UNIT_TEST(ScanIngressUsesExistingStats) {
        const TVector<IKqpGateway::TPhysicalTxData> transactions;
        const IKqpGateway::TPhysicalTxData tx(nullptr, nullptr);
        const NKikimrConfig::TTableServiceConfig::TAggregationConfig aggregation;
        TKqpTasksGraph graph("/Root", transactions, nullptr, {}, aggregation, nullptr, {}, nullptr, false);
        const NYql::NDq::TStageId stageId(0, 1);
        graph.AddStageInfo(TStageInfo(stageId, 0, 0, TStageInfoMeta(tx)));
        auto& stage = graph.GetStageInfo(stageId);
        stage.Meta.TablePath = "/Root/Table";
        auto& task = graph.AddTask(stage);

        for (auto kind : {ETableKind::Datashard, ETableKind::Olap, ETableKind::SysView}) {
            stage.Meta.TableKind = kind;
            for (bool scan : {false, true}) {
                task.Meta.ScanTask = scan;
                for (auto mode : {Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE,
                                  Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC,
                                  Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL,
                                  Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE}) {
                    TQueryExecutionStats stats(mode, &graph, nullptr, 0, true);
                    Init(stats);
                    const bool scanActor = scan && kind != ETableKind::SysView;
                    const ui64 sourceBytes = 700;
                    const ui64 scanBytes = scanActor ? 1000 : 0;
                    const auto ingressBytes = sourceBytes + (CollectFullStats(mode) ? scanBytes : 0);
                    auto report = MakeReport(task.Id, 100, 4096, 1000, ingressBytes);
                    auto& otherTable = *report.MutableTasks(0)->AddTables();
                    otherTable.SetTablePath("/Root/Other");
                    otherTable.SetReadBytes(sourceBytes);
                    stats.UpdateTaskStats(1, task.Id, report, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
                    stats.UpdateTaskStats(1, task.Id, report, nullptr, COMPUTE_STATE_FINISHED, TDuration::Max());
                    UNIT_ASSERT_VALUES_EQUAL(stats.GetCurrentQueryResources().ReadIngressBytes, sourceBytes + scanBytes);
                    UNIT_ASSERT_VALUES_EQUAL(stats.GetCurrentQueryResources().ComputeMemoryBytes, 0);
                    UNIT_ASSERT_VALUES_EQUAL(stats.Tables.at("/Root/Table").ReadBytes.Sum, 1000);
                    UNIT_ASSERT_VALUES_EQUAL(stats.Tables.at("/Root/Other").ReadBytes.Sum, sourceBytes);
                }
            }
        }
    }

    Y_UNIT_TEST(UpdateTotalsFromPartialReports) {
        TQueryExecutionStats stats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE, nullptr, nullptr, 0, true);
        Init(stats);
        auto first = MakeReport(1, 100, 4096, 1000, 700);
        auto* extra = first.MutableTasks(0)->AddTables();
        extra->SetTablePath("/Root/Other");
        extra->SetReadBytes(500);
        auto second = MakeReport(2, 200, 8192, 2000, 1400);
        stats.UpdateTaskStats(1, 1, first, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
        stats.UpdateTaskStats(2, 2, second, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());

        first.MutableTasks(0)->MutableTables()->RemoveLast();
        first.MutableTasks(0)->MutableTables(0)->SetReadBytes(1500);
        first.MutableTasks(0)->SetIngressBytes(900);
        first.SetMemoryUsage(2048);
        stats.UpdateTaskStats(1, 1, first, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
        stats.UpdateTaskStats(1, 1, first, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
        auto snapshot = stats.GetCurrentQueryResources();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.ReadIngressBytes, 2300);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.ComputeMemoryBytes, 10240);

        first.MutableTasks(0)->MutableTables(0)->SetReadBytes(0);
        first.MutableTasks(0)->SetIngressBytes(0);
        first.SetMemoryUsage(0);
        stats.UpdateTaskStats(1, 1, first, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
        snapshot = stats.GetCurrentQueryResources();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.ReadIngressBytes, 2300);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.ComputeMemoryBytes, 8192);

        auto storage = MakeReport(0, 0, 0, 300, 0);
        stats.UpdateTaskStats(1, 0, storage, nullptr, COMPUTE_STATE_FINISHED, TDuration::Max());
        stats.UpdateTaskStats(1, 0, storage, nullptr, COMPUTE_STATE_FINISHED, TDuration::Max());
        stats.UpdateTaskStats(2, 2, TDqComputeActorStats{}, nullptr, COMPUTE_STATE_FAILURE, TDuration::Max());
        snapshot = stats.GetCurrentQueryResources();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.ReadIngressBytes, 2300);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.ComputeMemoryBytes, 0);
    }

    Y_UNIT_TEST(FinalReportReleasesMemoryWithoutProgressDelivery) {
        TQueryExecutionStats stats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE, nullptr, nullptr, 0, true);
        Init(stats);
        auto report = MakeReport(1, 100, 4096, 1000, 700);
        stats.UpdateTaskStats(1, 1, report, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
        const auto progress = stats.TakeCurrentStats();
        stats.UpdateTaskStats(1, 1, report, nullptr, COMPUTE_STATE_FINISHED, TDuration::Max());
        const auto final = stats.TakeCurrentStats(true);
        UNIT_ASSERT_GT(final.SequenceNo, progress.SequenceNo);
        TCurrentQueryStats query;
        TCurrentQueryStats::TSourceState source;
        query.Update(source, final);
        UNIT_ASSERT_VALUES_EQUAL(query.Get()->ComputeMemoryBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(query.Get()->CpuTimeUs, 100);
    }

    Y_UNIT_TEST(AggregatePhysicalExecutions) {
        TCurrentQueryStats query;
        UNIT_ASSERT(!query.Get());
        TQueryExecutionStats first(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE, nullptr, nullptr, 0, true);
        TQueryExecutionStats second(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE, nullptr, nullptr, 0, true);
        Init(first);
        Init(second);
        auto report = MakeReport(1, 100, 4096, 1000, 700);
        first.UpdateTaskStats(1, 1, report, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
        TCurrentQueryStats::TSourceState firstSource, secondSource, thirdSource;
        query.Update(firstSource, first.TakeCurrentStats());
        query.Update(firstSource, first.TakeCurrentStats());
        second.UpdateTaskStats(1, 1, report, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
        query.Update(secondSource, second.TakeCurrentStats());
        UNIT_ASSERT_VALUES_EQUAL(query.Get()->CpuTimeUs, 200);
        UNIT_ASSERT_VALUES_EQUAL(query.Get()->ComputeMemoryBytes, 8192);
        UNIT_ASSERT_VALUES_EQUAL(query.Get()->ReadIngressBytes, 1400);
        query.Update(firstSource, first.TakeCurrentStats(true));
        UNIT_ASSERT_VALUES_EQUAL(query.Get()->ComputeMemoryBytes, 4096);
        query.Update(secondSource, second.TakeCurrentStats(true));
        UNIT_ASSERT_VALUES_EQUAL(query.Get()->ComputeMemoryBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(query.Get()->CpuTimeUs, 200);
        TQueryExecutionStats third(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE, nullptr, nullptr, 0, true);
        Init(third);
        third.UpdateTaskStats(1, 1, report, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
        query.Update(thirdSource, third.TakeCurrentStats());
        UNIT_ASSERT_VALUES_EQUAL(query.Get()->CpuTimeUs, 300);
        UNIT_ASSERT_VALUES_EQUAL(query.Get()->ComputeMemoryBytes, 4096);
    }

    Y_UNIT_TEST(FinishedSourceAcceptsNewExecutionSequence) {
        TCurrentQueryStats query;
        TCurrentQueryStats::TSourceState source;

        TQueryExecutionStats first(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE, nullptr, nullptr, 0, true);
        Init(first);
        auto report = MakeReport(1, 100, 4096, 1000, 700);
        first.UpdateTaskStats(1, 1, report, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
        UNIT_ASSERT(query.Update(source, first.TakeCurrentStats()));
        UNIT_ASSERT(query.Finish(source));

        TQueryExecutionStats second(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE, nullptr, nullptr, 0, true);
        Init(second);
        second.UpdateTaskStats(1, 1, report, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
        const auto next = second.TakeCurrentStats();
        UNIT_ASSERT_VALUES_EQUAL(next.SequenceNo, 1);
        UNIT_ASSERT(query.Update(source, next));
        UNIT_ASSERT_VALUES_EQUAL(query.Get()->CpuTimeUs, 200);
        UNIT_ASSERT_VALUES_EQUAL(query.Get()->ComputeMemoryBytes, 4096);
    }

    Y_UNIT_TEST(FailureWithoutTaskStatsReleasesMemory) {
        TQueryExecutionStats stats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC, nullptr, nullptr, 0, true);
        Init(stats);
        auto report = MakeReport(1, 100, 4096, 1000, 700);
        stats.UpdateTaskStats(1, 1, report, nullptr, COMPUTE_STATE_EXECUTING, TDuration::Max());
        stats.UpdateTaskStats(1, 1, TDqComputeActorStats{}, nullptr, COMPUTE_STATE_FAILURE, TDuration::Max());
        const auto snapshot = stats.GetCurrentQueryResources();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.ComputeMemoryBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.CpuTimeUs, 100);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.ReadIngressBytes, 700);
    }
}

} // namespace NKikimr::NKqp
