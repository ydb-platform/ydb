#include "defs.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

using namespace NSchemeShard;
using namespace Tests;

TAutoPtr<IEventHandle> EjectDataPropose(TServer::TPtr server, ui64 dataShard)
{
    auto &runtime = *server->GetRuntime();
    ui64 schemeShard = ChangeStateStorage(SchemeRoot, server->GetSettings().Domain);

    bool hasFound = false;
    TAutoPtr<IEventHandle> proposeEvent = nullptr;

    auto captureProposes = [dataShard, schemeShard, &hasFound, &proposeEvent] (TAutoPtr<IEventHandle> &event) -> auto
    {
        if (event->GetTypeRewrite() == TEvTxProxy::EvProposeTransaction) {
            auto &rec = event->Get<TEvTxProxy::TEvProposeTransaction>()->Record;
            bool noSchemeShard = true;
            bool hasDataShard = false;

            for (auto& affected: rec.GetTransaction().GetAffectedSet()) {
                if (schemeShard == affected.GetTabletId()) {
                    noSchemeShard = false;
                }
                if (dataShard == affected.GetTabletId()) {
                    hasDataShard = true;
                }
            }

            if (noSchemeShard && hasDataShard) {
                Cerr << "\n\n\n got data transaction propose \n\n\n";
                Cerr << rec.DebugString();
                hasFound = true;
                proposeEvent = std::move(event);
                return TTestActorRuntime::EEventAction::DROP;
            }
        }

        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserver = runtime.SetObserverFunc(captureProposes);

    auto stopCondition = [&hasFound] () -> auto
    {
        return hasFound;
    };

    TDispatchOptions options;
    options.CustomFinalCondition = stopCondition;
    server->GetRuntime()->DispatchEvents(options);
    runtime.SetObserverFunc(prevObserver);

    Y_ABORT_UNLESS(proposeEvent);
    return proposeEvent;
}

Y_UNIT_TEST_SUITE(TDataShardMinStepTest) {
    void TestDropTablePlanComesNotTooEarly(const TString& query, Ydb::StatusIds::StatusCode expectedStatus, bool volatileTxs) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardVolatileTransactions(volatileTxs)
            .SetAppConfig(app);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_NOTICE);
        runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NLog::PRI_TRACE);
        //runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TABLETQUEUE, NLog::PRI_TRACE);

        //runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_TRACE);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ui64 shard1 = GetTableShards(server, sender, "/Root/table-1")[0];

        CreateShardedTable(server, sender, "/Root", "table-2", 1);
        ui64 shard2 = GetTableShards(server, sender, "/Root/table-2")[0];

        // propose data tx for datashards but not to coordinator
        // eject propose message to coordinator
        SendSQL(server, sender, query);
        auto proposeEvent = EjectDataPropose(server, shard2);

        // drop one table while proposes are active
        auto senderScheme = runtime.AllocateEdgeActor();
        const TInstant dropStart = runtime.GetCurrentTime();
        ExecSQL(server, senderScheme, "DROP TABLE `/Root/table-1`", false);
        WaitTabletBecomesOffline(server, shard1);
        const TInstant dropEnd = runtime.GetCurrentTime();

        UNIT_ASSERT_C((dropEnd - dropStart) < TDuration::Seconds(35),
            "Drop has taken " << (dropEnd - dropStart) << " of simulated time");

        { // make sure that the ejeceted propose has become outdated
            auto request = MakeHolder<TEvTxProxy::TEvProposeTransaction>();
            request->Record.CopyFrom(proposeEvent->Get<TEvTxProxy::TEvProposeTransaction>()->Record);
            runtime.SendToPipe(request->Record.GetCoordinatorID(), sender, request.Release());

            auto ev = runtime.GrabEdgeEventRethrow<TEvTxProxy::TEvProposeTransactionStatus>(sender);
            auto expectedPlanStatus = volatileTxs
                // Volatile transactions abort eagerly, so plan will be accepted (but eventually fail)
                ? TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAccepted
                : TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated;
            UNIT_ASSERT_VALUES_EQUAL((TEvTxProxy::TEvProposeTransactionStatus::EStatus)ev->Get()->Record.GetStatus(),
                expectedPlanStatus);
        }

        { // handle respond from unplanned data transaction because plan ejection
            auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
            auto* reply = ev->Get();
            NYql::TIssues issues;
            NYql::IssuesFromMessage(reply->Record.GetRef().GetResponse().GetQueryIssues(), issues);
            UNIT_ASSERT_VALUES_EQUAL_C(reply->Record.GetRef().GetYdbStatus(), expectedStatus,
                issues.ToString());
        }

        // make sure that second table is still operationable
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2);");
        ExecSQL(server, sender, "DROP TABLE `/Root/table-2`", false);
        WaitTabletBecomesOffline(server, shard2);
    }

    Y_UNIT_TEST_TWIN(TestDropTablePlanComesNotTooEarlyRW, VolatileTxs) {
        TestDropTablePlanComesNotTooEarly(
            "UPSERT INTO `/Root/table-2` (key, value) SELECT key, value FROM `/Root/table-1`;",
            Ydb::StatusIds::ABORTED,
            VolatileTxs
        );
    }

    enum class ERebootOnPropose {
        DataShard,
        SchemeShard,
    };

/*
    void TestAlterProposeRebootMinStep(ERebootOnPropose rebootOnPropose) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        NKikimrConfig::TAppConfig app;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        ui64 schemeShard = ChangeStateStorage(SchemeRoot, server->GetSettings().Domain);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_NOTICE);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2);");
        ui64 table1shard = GetTableShards(server, sender, "/Root/table-1").at(0);

        bool capturePlan = false;
        bool captureProposes = false;
        bool captureProposeResults = false;
        TDeque<THolder<IEventHandle>> plans;
        TDeque<THolder<IEventHandle>> proposes;
        TDeque<THolder<IEventHandle>> proposeResults;
        size_t schemaChangedCount = 0;
        TTestActorRuntimeBase::TEventObserver prevObserver;
        auto captureObserver = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProxy::TEvProposeTransaction::EventType:
                    if (capturePlan) {
                        Cerr << "Captured TEvTxProxy::TEvProposeTransaction" << Endl;
                        plans.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                case TEvDataShard::TEvProposeTransaction::EventType:
                    if (captureProposes) {
                        Cerr << "Captured TEvDataShard::TEvProposeTransaction" << Endl;
                        proposes.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                case TEvDataShard::TEvProposeTransactionResult::EventType:
                    if (captureProposeResults) {
                        Cerr << "Captured TEvDataShard::TEvProposeTransactionResult" << Endl;
                        proposeResults.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                case TEvDataShard::TEvSchemaChangedResult::EventType:
                    Cerr << "Observed TEvDataShard::TEvSchemaChangedResult" << Endl;
                    ++schemaChangedCount;
                    break;
            }
            return prevObserver(ev);
        };
        prevObserver = runtime.SetObserverFunc(captureObserver);

        // We want select to prepare on all shards, but not planned yet
        captureProposeResults = true;
        Cerr << "Sending initial SELECT query..." << Endl;
        SendSQL(server, sender, "SELECT * FROM `/Root/table-1` UNION ALL SELECT * FROM `/Root/table-2`");
        if (proposeResults.size() < 2) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() -> bool {
                return proposeResults.size() >= 2;
            };
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(proposeResults.size(), 2u);
        auto selectProposeResults = std::move(proposeResults);

        // Start alter that drops the value column and wait
        // until it's stuck waiting for the propose result
        Cerr << "Sending DROP COLUMN propose to schemeshard..." << Endl;
        ui64 txId = AsyncAlterDropColumn(server, "/Root", "table-1", "value");
        if (proposeResults.size() < 1) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() -> bool {
                return proposeResults.size() >= 1;
            };
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(proposeResults.size(), 1u);
        auto alterProposeInitialResults = std::move(proposeResults);

        const auto* initialResult = alterProposeInitialResults[0]->Get<TEvDataShard::TEvProposeTransactionResult>();
        UNIT_ASSERT_VALUES_EQUAL(
            initialResult->Record.GetStatus(),
            NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED);
        auto initialMinStep = initialResult->Record.GetMinStep();
        auto initialMaxStep = initialResult->Record.GetMaxStep();

        // Now restart datashard/schemeshard and wait for retry
        captureProposes = true;
        switch (rebootOnPropose) {
            case ERebootOnPropose::DataShard:
                Cerr << "Rebooting datashard and waiting for new propose..." << Endl;
                RebootTablet(runtime, table1shard, sender);
                break;
            case ERebootOnPropose::SchemeShard:
                Cerr << "Rebooting schemeshard and waiting for datashard propose..." << Endl;
                RebootTablet(runtime, schemeShard, sender);
                break;
        }
        if (proposes.size() < 1) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() -> bool {
                return proposes.size() >= 1;
            };
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(proposes.size(), 1u);
        captureProposes = false;

        // Make a copy of the last propose message, it would be needed later
        auto proposeRecord = proposes[0]->Get<TEvDataShard::TEvProposeTransaction>()->Record;

        // Resend to destinations
        Cerr << "Resending propose from schemeshard..." << Endl;
        for (auto& ev : proposes) {
            runtime.Send(ev.Release(), 0, true);
        }

        // Capture the propose result
        if (proposeResults.size() < 1) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() -> bool {
                return proposeResults.size() >= 1;
            };
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(proposeResults.size(), 1u);
        auto alterProposeRetriedResults = std::move(proposeResults);
        captureProposeResults = false;

        // Check the retried message has expected status and min/max step
        const auto* retriedResult = alterProposeRetriedResults[0]->Get<TEvDataShard::TEvProposeTransactionResult>();
        UNIT_ASSERT_VALUES_EQUAL(
            retriedResult->Record.GetStatus(),
            NKikimrTxDataShard::TEvProposeTransactionResult::PREPARED);
        UNIT_ASSERT_VALUES_EQUAL(retriedResult->Record.GetMinStep(), initialMinStep);
        UNIT_ASSERT_VALUES_EQUAL(retriedResult->Record.GetMaxStep(), initialMaxStep);
        auto shardActorId = alterProposeRetriedResults[0]->Sender;

        // Resend captured messages and wait for plan
        capturePlan = true;
        Cerr << "Waiting for schemeshard to plan its transaction..." << Endl;
        for (auto& ev : alterProposeRetriedResults) {
            runtime.Send(ev.Release(), 0, true);
        }
        if (plans.size() < 1) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() -> bool {
                return plans.size() >= 1;
            };
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(plans.size(), 1u);
        capturePlan = false;

        // Unblock both plan and select propose results
        schemaChangedCount = 0;
        Cerr << "Unblocking schemeshard plan..." << Endl;
        for (auto& ev : plans) {
            runtime.Send(ev.Release(), 0, true);
        }
        plans.clear();

        // Sleep a little and unblock select propose results
        SimulateSleep(server, TDuration::Seconds(2));
        Cerr << "Unblocking initial select results..." << Endl;
        for (auto& ev : selectProposeResults) {
            runtime.Send(ev.Release(), 0, true);
        }

        // Wait for alter to complete
        Cerr << "Waiting for alter to finish..." << Endl;
        WaitTxNotification(server, txId);

        // Wait for schema changed to be handled
        if (schemaChangedCount < 1) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() -> bool {
                return schemaChangedCount >= 1;
            };
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(schemaChangedCount, 1u);

        // Wait for query result
        Cerr << "Waiting for kqp query result..." << Endl;
        auto evKqpResult = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);

        // Send a copy of the last proposal
        Cerr << "Simulating a propose echo..." << Endl;
        {
            auto event = new TEvDataShard::TEvProposeTransaction;
            event->Record = proposeRecord;
            runtime.Send(new IEventHandle(shardActorId, sender, event), 0, true);
        }

        Cerr << "Waiting for propose echo reply..." << Endl;
        auto evResult = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(
            evResult->Get()->Record.GetStatus(),
            NKikimrTxDataShard::TEvProposeTransactionResult::ERROR);
    }
    Y_UNIT_TEST(TestAlterProposeRebootDataShardMinStep) {
        TestAlterProposeRebootMinStep(ERebootOnPropose::DataShard);
    }

    Y_UNIT_TEST(TestAlterProposeRebootSchemeShardMinStep) {
        TestAlterProposeRebootMinStep(ERebootOnPropose::SchemeShard);
    }
*/
    void TestDropTableCompletesQuickly(const TString& query, Ydb::StatusIds::StatusCode expectedStatus, bool volatileTxs) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardVolatileTransactions(volatileTxs)
            .SetAppConfig(app);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_NOTICE);
        runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NLog::PRI_TRACE);
        //runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TABLETQUEUE, NLog::PRI_TRACE);

        //runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_TRACE);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ui64 shard1 = GetTableShards(server, sender, "/Root/table-1")[0];

        CreateShardedTable(server, sender, "/Root", "table-2", 1);
        ui64 shard2 = GetTableShards(server, sender, "/Root/table-2")[0];

        // propose data tx for datashards but not to coordinator
        // eject propose message to coordinator
        SendSQL(server, sender, query);
        auto proposeEvent = EjectDataPropose(server, shard1);

        // capture scheme proposals
        size_t seenProposeScheme = 0;
        auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvProposeTransaction::EventType: {
                    const auto* msg = ev->Get<TEvDataShard::TEvProposeTransaction>();
                    if (msg->GetTxKind() == NKikimrTxDataShard::TX_KIND_SCHEME) {
                        ++seenProposeScheme;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(captureEvents);

        // drop one table while proposes are waiting
        // we also capture scheme propose at datashard
        auto senderScheme = runtime.AllocateEdgeActor();
        auto dropStart = runtime.GetCurrentTime();
        SendSQL(server, senderScheme, "DROP TABLE `/Root/table-1`", false);
        if (!seenProposeScheme) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() -> bool {
                return seenProposeScheme > 0;
            };
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(seenProposeScheme, 1u);

        // Unblock coordinator proposal for the data transaction
        runtime.Send(proposeEvent.Release(), 0, /* viaActorSystem */ true);

        // Wait for tablet to become offline
        Cerr << "--- Waiting for table-1 (" << shard1 << ") to go offline ---" << Endl;
        WaitTabletBecomesOffline(server, shard1);
        auto dropEnd = runtime.GetCurrentTime();

        UNIT_ASSERT_C((dropEnd - dropStart) < TDuration::Seconds(5),
            "Drop has taken " << (dropEnd - dropStart) << " of simulated time");

        { // handle response from data transaction
            auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->Record.GetRef().GetResponse().GetQueryIssues(), issues);
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Record.GetRef().GetYdbStatus(), expectedStatus, issues.ToString());
        }

        { // handle response from scheme transaction
            auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(senderScheme);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->Record.GetRef().GetResponse().GetQueryIssues(), issues);
            UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::SUCCESS, issues.ToString());
        }

        // make sure that second table is still operationable
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2);");
        ExecSQL(server, sender, "DROP TABLE `/Root/table-2`", false);
        WaitTabletBecomesOffline(server, shard2);
    }

    Y_UNIT_TEST_TWIN(TestDropTableCompletesQuicklyRW, VolatileTxs) {
        TestDropTableCompletesQuickly(
            "UPSERT INTO `/Root/table-2` (key, value) SELECT key, value FROM `/Root/table-1`;",
            VolatileTxs ? Ydb::StatusIds::ABORTED : Ydb::StatusIds::SUCCESS,
            VolatileTxs
        );
    }

}

} // namespace NKikimr
