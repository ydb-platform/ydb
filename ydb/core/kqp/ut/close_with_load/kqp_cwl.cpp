#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/shutdown/state.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/shutdown/controller.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/base/counters.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <library/cpp/threading/local_executor/local_executor.h>
#include <ydb/core/tx/datashard/datashard_failpoints.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpService) {

    Y_UNIT_TEST(CloseSessionsWithLoad) {
        auto kikimr = std::make_shared<TKikimrRunner>();
        kikimr->GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);
        kikimr->GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_DEBUG);
        kikimr->GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_ACTOR, NLog::PRI_DEBUG);
        kikimr->GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NLog::PRI_DEBUG);

        auto db = kikimr->GetTableClient();

        const ui32 SessionsCount = 50;
        const TDuration WaitDuration = TDuration::Seconds(1);

        TVector<TSession> sessions;
        for (ui32 i = 0; i < SessionsCount; ++i) {
            auto sessionResult = db.CreateSession().GetValueSync();
            UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());

            sessions.push_back(sessionResult.GetSession());
        }

        NPar::LocalExecutor().RunAdditionalThreads(SessionsCount + 1);
        NPar::LocalExecutor().ExecRange([&kikimr, sessions, WaitDuration](int id) mutable {
            if (id == (i32)sessions.size()) {
                Sleep(WaitDuration);
                Cerr << "start sessions close....." << Endl;
                for (ui32 i = 0; i < sessions.size(); ++i) {
                    sessions[i].Close();
                }

                Cerr << "finished sessions close....." << Endl;
                auto counters = GetServiceCounters(kikimr->GetTestServer().GetRuntime()->GetAppData(0).Counters,  "ydb");

                ui64 pendingCompilations = 0;
                do {
                    Sleep(WaitDuration);
                    pendingCompilations = counters->GetNamedCounter("name", "table.query.compilation.active_count", false)->Val();
                    Cerr << "still compiling... " << pendingCompilations << Endl;
                } while (pendingCompilations != 0);

                ui64 pendingSessions = 0;
                do {
                    Sleep(WaitDuration);
                    pendingSessions = counters->GetNamedCounter("name", "table.session.active_count", false)->Val();
                    Cerr << "still active sessions ... " << pendingSessions << Endl;
                } while (pendingSessions != 0);

                Sleep(TDuration::Seconds(5));

                return;
            }

            auto session = sessions[id];
            std::optional<NYdb::NTable::TTransaction> tx;

            while (true) {
                if (tx) {
                    auto result = tx->Commit().GetValueSync();
                    if (!result.IsSuccess()) {
                        return;
                    }

                    tx = {};
                    continue;
                }

                auto query = Sprintf(R"(
                    SELECT Key, Text, Data FROM `/Root/EightShard` WHERE Key=%1$d + 0;
                    SELECT Key, Data, Text FROM `/Root/EightShard` WHERE Key=%1$d + 1;
                    SELECT Text, Key, Data FROM `/Root/EightShard` WHERE Key=%1$d + 2;
                    SELECT Text, Data, Key FROM `/Root/EightShard` WHERE Key=%1$d + 3;
                    SELECT Data, Key, Text FROM `/Root/EightShard` WHERE Key=%1$d + 4;
                    SELECT Data, Text, Key FROM `/Root/EightShard` WHERE Key=%1$d + 5;

                    UPSERT INTO `/Root/EightShard` (Key, Text) VALUES
                        (%2$dul, "New");
                )", RandomNumber<ui32>(), RandomNumber<ui32>());

                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx()).GetValueSync();
                if (!result.IsSuccess()) {
                    Sleep(TDuration::Seconds(5));
                    Cerr << "received non-success status for session " << id << Endl;
                    return;
                }

                tx = result.GetTransaction();
            }
        }, 0, SessionsCount + 1, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
        WaitForZeroReadIterators(kikimr->GetTestServer(), "/Root/EightShard");
    }

    // Regression test: closing a session while a non-final cleanup is in progress
    // used to clobber the ExecuterId that was just set by the final-cleanup rollback,
    // causing the session to get stuck in CleanupState forever.
    Y_UNIT_TEST(CloseSessionDuringNonFinalCleanup) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        auto kikimr = TKikimrRunner(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        runtime->SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

        NKqp::TKqpCounters counters(runtime->GetAppData().Counters);

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        // Open TX1 with a write so it definitely stays in the Active transactions map
        // and has effects that require rollback.
        // Later, FinalCleanup() will move it to ToBeAborted and trigger a rollback.
        auto result1 = kikimr.RunCall([&] {
            return session.ExecuteDataQuery(
                "UPSERT INTO `/Root/EightShard` (Key, Text) VALUES (100500u, \"tx1\");",
                TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        });
        UNIT_ASSERT_C(result1.IsSuccess(), result1.GetIssues().ToString());
        UNIT_ASSERT(result1.GetTransaction());

        // Stall reads so the next query hangs mid-execution.
        NDataShard::gSkipReadIteratorResultFailPoint.Enable(-1);
        Y_DEFER { NDataShard::gSkipReadIteratorResultFailPoint.Disable(); };

        // Observer: (a) replace the first CA→Executer TEvState with TEvAbortExecution
        //           so the query fails with CANCELLED (KeepSession stays true),
        //           (b) hold the SECOND TEvTxResponse (the rollback-during-cleanup one)
        //           so we can inject TEvCloseSessionRequest while in CleanupState.
        bool abortSent = false;
        int txResponseCount = 0;
        THolder<IEventHandle> heldRollbackResponse;
        TActorId sessionActorId;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (!abortSent &&
                ev->GetTypeRewrite() == NYql::NDq::TEvDqCompute::TEvState::EventType) {
                abortSent = true;
                ev = new IEventHandle(ev->Recipient, ev->Sender,
                    new TEvKqp::TEvAbortExecution(
                        NYql::NDqProto::StatusIds::CANCELLED, NYql::TIssues()));
            }
            if (ev &&
                ev->GetTypeRewrite() == TEvKqpExecuter::TEvTxResponse::EventType) {
                ++txResponseCount;
                if (txResponseCount == 1) {
                    // First TEvTxResponse = failed-query result → remember session actor.
                    sessionActorId = ev->GetRecipientRewrite();
                } else if (txResponseCount == 2) {
                    // Second TEvTxResponse = rollback result during non-final cleanup → hold it.
                    heldRollbackResponse.Reset(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // Start a second query (new BeginTx → TX2). It will hang, be aborted,
        // and its invalidated TxCtx will cause a rollback in non-final cleanup.
        auto future = kikimr.RunInThreadPool([&] {
            return session.ExecuteDataQuery(
                "SELECT Key FROM `/Root/EightShard` WHERE Key = 2u;",
                TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        });

        // Wait until the rollback response is captured by the observer.
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(
                [&](IEventHandle&) { return heldRollbackResponse != nullptr; });
            runtime->DispatchEvents(opts);
        }

        // The session is now in non-final CleanupState.
        // Inject TEvCloseSessionRequest → sets KeepSession=false.
        UNIT_ASSERT(sessionActorId);
        {
            auto close = std::make_unique<TEvKqp::TEvCloseSessionRequest>();
            close->Record.MutableRequest()->SetSessionId(TString(session.GetId()));
            runtime->Send(new IEventHandle(sessionActorId, TActorId(), close.release()));
        }

        // Now release the held rollback response.
        // EndCleanup(false) will see doNotKeepSession==true and call CleanupAndPassAway(),
        // which triggers FinalCleanup → rollback of TX1.
        // Before the fix ExecuterId was clobbered and the session got stuck.
        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        runtime->Send(heldRollbackResponse.Release());

        // The query future must complete (session sends the reply in EndCleanup).
        auto result = runtime->WaitFuture(future);
        UNIT_ASSERT_C(!result.IsSuccess(), "Expected the aborted query to fail");

        // Final cleanup must finish: the session actor dies and the active-sessions
        // counter drops to zero.  Without the fix the session gets stuck in
        // CleanupState because the rollback response for TX1 is silently discarded
        // (ExecuterId was clobbered) and the counter never reaches zero.
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(
                [&](IEventHandle&) { return counters.GetActiveSessionActors()->Val() == 0; });
            UNIT_ASSERT_C(
                runtime->DispatchEvents(opts, TDuration::Seconds(10)),
                "Session is stuck in CleanupState — active session actor count never reached zero");
        }
    }
}
}
}