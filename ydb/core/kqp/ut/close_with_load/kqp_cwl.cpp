#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/shutdown/state.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/buffer/events.h>
#include <ydb/core/kqp/common/shutdown/controller.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/base/counters.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <library/cpp/threading/local_executor/local_executor.h>
#include <ydb/core/tx/datashard/datashard_failpoints.h>
#include <ydb/core/tx/data_events/events.h>

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

    // A final cleanup may have to close the legacy worker used by Table
    // ExecuteSchemeQuery. The final intent must survive that asynchronous wait.
    Y_UNIT_TEST(FinalCleanupIntentIsPreservedWhileClosingLegacyWorker) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        auto kikimr = TKikimrRunner(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        NKqp::TKqpCounters counters(runtime->GetAppData().Counters);

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto createResult = kikimr.RunCall([&] { return db.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(createResult.IsSuccess(), createResult.GetIssues().ToString());
        auto session = createResult.GetSession();

        TActorId proxyId;
        TActorId sessionActorId;
        bool workerRequestDropped = false;
        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvKqp::TEvQueryRequest::EventType) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }
            if (!proxyId) {
                // The first hop is RPC actor -> the dynamically registered KQP proxy.
                proxyId = ev->GetRecipientRewrite();
            } else if (ev->Sender == proxyId) {
                // The second hop is KQP proxy -> this Table session actor.
                sessionActorId = ev->GetRecipientRewrite();
            } else if (sessionActorId && ev->Sender == sessionActorId) {
                // The third hop is session actor -> legacy DDL worker.
                workerRequestDropped = true;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto future = kikimr.RunInThreadPool([&] {
            return session.ExecuteSchemeQuery(
                "CREATE TABLE `/Root/LegacyWorkerCleanup` (Key Uint64, PRIMARY KEY (Key));")
                .GetValueSync();
        });

        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(
                [&](IEventHandle&) { return workerRequestDropped; });
            UNIT_ASSERT_C(runtime->DispatchEvents(opts, TDuration::Seconds(10)),
                "Table scheme request was not forwarded to the legacy worker");
        }
        UNIT_ASSERT(sessionActorId);

        // Force the session's fatal-error cleanup while its legacy worker is
        // alive. Without propagating isFinal to TKqpCleanupCtx, the worker close
        // response incorrectly returns the session actor to ReadyState.
        runtime->Send(new IEventHandle(
            sessionActorId, TActorId(), new TEvents::TEvWakeup()));

        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(
                [&](IEventHandle&) { return counters.GetActiveSessionActors()->Val() == 0; });
            UNIT_ASSERT_C(runtime->DispatchEvents(opts, TDuration::Seconds(10)),
                "Final cleanup lost its final flag while waiting for the legacy worker");
        }
        UNIT_ASSERT_VALUES_EQUAL(counters.GetActiveSessionActors()->Val(), 0);

        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        auto result = runtime->WaitFuture(future);
        UNIT_ASSERT_C(!result.IsSuccess(), "Fault-injected scheme query unexpectedly succeeded");
    }

    Y_UNIT_TEST(UndeliveredIdleCloseReleasesSessionQuota) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetSessionsLimitPerNode(1);
        settings.AppConfig.MutableTableServiceConfig()->SetSessionIdleDurationSeconds(1);

        auto kikimr = TKikimrRunner(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto createResult = kikimr.RunCall([&] { return db.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(createResult.IsSuccess(), createResult.GetIssues().ToString());

        TActorId proxyId;
        TActorId sessionActorId;
        bool idleCloseDropped = false;
        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvKqp::TEvCloseSessionRequest::EventType) {
                UNIT_ASSERT_C(ev->Flags & IEventHandle::FlagTrackDelivery,
                    "Idle close request must track delivery");
                proxyId = ev->Sender;
                sessionActorId = ev->GetRecipientRewrite();
                idleCloseDropped = true;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // Let the proxy remove the session from IdleSessions and attempt the
        // tracked close, but emulate an actor-system delivery failure.
        runtime->SimulateSleep(TDuration::Seconds(3));
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(
                [&](IEventHandle&) { return idleCloseDropped; });
            UNIT_ASSERT_C(runtime->DispatchEvents(opts, TDuration::Seconds(10)),
                "Idle close request was not sent");
        }
        UNIT_ASSERT(proxyId);
        UNIT_ASSERT(sessionActorId);

        // Until nondelivery is handled, the orphan registry entry still owns
        // the only per-database quota slot.
        auto atLimit = kikimr.RunCall([&] { return db.CreateSession().GetValueSync(); });
        UNIT_ASSERT_VALUES_EQUAL(atLimit.GetStatus(), EStatus::OVERLOADED);

        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        runtime->Send(new IEventHandle(
            proxyId,
            sessionActorId,
            new TEvents::TEvUndelivered(
                TEvKqp::TEvCloseSessionRequest::EventType,
                TEvents::TEvUndelivered::ReasonActorUnknown)));

        // The undelivered handler removes the entry by target actor id, so a
        // new Table session can be admitted without any limit-time eviction.
        auto afterUndelivered = kikimr.RunCall([&] { return db.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(afterUndelivered.IsSuccess(), afterUndelivered.GetIssues().ToString());
    }

    // A proxy-side operation timeout makes the session visible as IDLE before
    // the session actor has necessarily stopped its executer. If the executer
    // also fails to acknowledge cancellation, the later idle close must still
    // have a bounded completion time.
    Y_UNIT_TEST(IdleCloseOfStuckExecutingTableSessionReleasesSlot) {
        TStringStream logs;
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.SetLogStream(&logs);
        settings.AppConfig.MutableTableServiceConfig()->SetSessionsLimitPerNode(1);
        settings.AppConfig.MutableTableServiceConfig()->SetSessionIdleDurationSeconds(1);

        auto kikimr = TKikimrRunner(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        runtime->SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_DEBUG);

        NKqp::TKqpCounters counters(runtime->GetAppData().Counters);

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto createResult = kikimr.RunCall([&] { return db.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(createResult.IsSuccess(), createResult.GetIssues().ToString());
        auto session = createResult.GetSession();

        NDataShard::gSkipReadIteratorResultFailPoint.Enable(-1);
        Y_DEFER { NDataShard::gSkipReadIteratorResultFailPoint.Disable(); };

        TActorId executerId;
        TActorId sessionActorId;
        bool cancellationDropped = false;
        bool idleCloseSeen = false;
        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            const auto eventType = ev->GetTypeRewrite();
            if (eventType == NYql::NDq::TEvDqCompute::TEvState::EventType && !executerId) {
                executerId = ev->GetRecipientRewrite();
            } else if (eventType == TEvKqp::TEvAbortExecution::EventType
                    && executerId && ev->GetRecipientRewrite() == executerId) {
                // Emulate the executor never acknowledging either the proxy
                // timeout cancellation or the later session-close cancellation.
                cancellationDropped = true;
                return TTestActorRuntime::EEventAction::DROP;
            } else if (eventType == TEvKqp::TEvCloseSessionRequest::EventType && !idleCloseSeen) {
                idleCloseSeen = true;
                sessionActorId = ev->GetRecipientRewrite();
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto queryFuture = kikimr.RunInThreadPool([&] {
            return session.ExecuteDataQuery(
                "SELECT Key FROM `/Root/EightShard` WHERE Key = 2u;",
                TTxControl::BeginTx().CommitTx(),
                TExecDataQuerySettings().OperationTimeout(TDuration::Seconds(1)))
                .GetValueSync();
        });

        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(
                [&](IEventHandle&) { return bool{executerId}; });
            UNIT_ASSERT_C(runtime->DispatchEvents(opts, TDuration::Seconds(10)),
                "Table query did not reach the executer");
        }

        // The first timer round asks the session to abort. The second one
        // returns a synthetic TIMEOUT through ForwardEvent, which re-arms the
        // proxy idle timer and clears QueryStartAt even though ExecuteState is
        // still waiting for the executer.
        runtime->SimulateSleep(TDuration::Seconds(3));
        auto queryResult = runtime->WaitFuture(queryFuture);
        UNIT_ASSERT_VALUES_EQUAL_C(queryResult.GetStatus(), EStatus::TIMEOUT,
            queryResult.GetIssues().ToString());
        UNIT_ASSERT_C(cancellationDropped,
            "Executor cancellation was not fault-injected");

        // After the one-second idle TTL, proxy removes the session from its
        // IdleSessions queue and sends close to the still-executing actor.
        runtime->SimulateSleep(TDuration::Seconds(3));
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(
                [&](IEventHandle&) { return idleCloseSeen; });
            UNIT_ASSERT_C(runtime->DispatchEvents(opts, TDuration::Seconds(10)),
                "Proxy did not close the timed-out IDLE Table session");
        }
        UNIT_ASSERT(sessionActorId);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetActiveSessionActors()->Val(), 1);

        // No executer response arrives. The close watchdog starts directly in
        // ExecuteState and first exposes the stuck phase at WARN.
        runtime->SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_WARN);
        runtime->SimulateSleep(TDuration::Seconds(61));
        const TString warningLogs = logs.Str();
        UNIT_ASSERT_C(warningLogs.Contains("Session cleanup is taking too long"), warningLogs);
        UNIT_ASSERT_C(warningLogs.Contains("ActorState: ExecuteState"), warningLogs);
        UNIT_ASSERT_C(warningLogs.Contains(TString(session.GetId())), warningLogs);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetActiveSessionActors()->Val(), 1);

        runtime->SimulateSleep(TDuration::Minutes(4) + TDuration::Seconds(1));
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(
                [&](IEventHandle&) { return counters.GetActiveSessionActors()->Val() == 0; });
            UNIT_ASSERT_C(runtime->DispatchEvents(opts, TDuration::Seconds(10)),
                "ExecuteState close timeout did not release the session actor");
        }
        UNIT_ASSERT_VALUES_EQUAL(counters.GetActiveSessionActors()->Val(), 0);

        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        auto secondCreate = kikimr.RunCall([&] { return db.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(secondCreate.IsSuccess(), secondCreate.GetIssues().ToString());
    }

    // A commit can finish in the buffer actor just before the executer's timeout.
    // Delay its result to exercise that ordering without losing any messages or
    // killing actors: cleanup must handle rollback to the already finished buffer.
    Y_UNIT_TEST(TableCommitTimeoutAfterBufferCompletionReleasesSession) {
        TStringStream logs;
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.SetLogStream(&logs);
        settings.AppConfig.MutableTableServiceConfig()->SetSessionsLimitPerNode(1);

        auto kikimr = TKikimrRunner(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        runtime->SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);
        TKqpCounters counters(runtime->GetAppData().Counters);

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto createResult = kikimr.RunCall([&] { return db.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(createResult.IsSuccess(), createResult.GetIssues().ToString());
        auto session = createResult.GetSession();

        TActorId bufferActorId;
        TActorId commitExecuterId;
        THolder<IEventHandle> heldCommitResult;
        bool rollbackUndelivered = false;
        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvKqpBuffer::TEvCommit::EventType) {
                bufferActorId = ev->GetRecipientRewrite();
                commitExecuterId = ev->Sender;
            } else if (ev->GetTypeRewrite() == TEvKqpBuffer::TEvResult::EventType
                    && ev->Sender == bufferActorId && ev->GetRecipientRewrite() == commitExecuterId) {
                heldCommitResult.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            } else if (ev->GetTypeRewrite() == TEvents::TEvUndelivered::EventType
                    && ev->Get<TEvents::TEvUndelivered>()->SourceType == TEvKqpBuffer::TEvRollback::EventType) {
                rollbackUndelivered = true;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc); };

        auto queryFuture = kikimr.RunInThreadPool([&] {
            return session.ExecuteDataQuery(
                "UPSERT INTO `/Root/EightShard` (Key, Text) VALUES (100502u, \"commit-timeout\");",
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                TExecDataQuerySettings().OperationTimeout(TDuration::Seconds(1))).GetValueSync();
        });
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) { return bool{heldCommitResult}; });
            UNIT_ASSERT_C(runtime->DispatchEvents(opts, TDuration::Seconds(10)),
                "The buffer actor did not finish the Table commit");
        }

        runtime->SimulateSleep(TDuration::Seconds(3));
        auto result = runtime->WaitFuture(queryFuture);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::TIMEOUT, result.GetIssues().ToString());
        UNIT_ASSERT_C(rollbackUndelivered, logs.Str());

        // The delayed result is delivered too, after the timeout has won.
        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        runtime->Send(heldCommitResult.Release());
        kikimr.RunCall([&] { return session.Close().GetValueSync(); });

        // A local nondelivery must finish cleanup promptly, before the closing
        // session watchdog (one-minute warning, five-minute forced cleanup).
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) {
                return counters.GetActiveSessionActors()->Val() == 0;
            });
            UNIT_ASSERT_C(runtime->DispatchEvents(opts, TDuration::Seconds(10)), logs.Str());
        }
        UNIT_ASSERT_VALUES_EQUAL(counters.GetActiveSessionActors()->Val(), 0);
        auto nextCreate = kikimr.RunCall([&] { return db.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(nextCreate.IsSuccess(), nextCreate.GetIssues().ToString());
    }

    // A write query with no matching rows finishes by rolling back its read
    // locks. If that overlaps a timeout, cleanup sends another rollback to the
    // same live buffer actor, with a new executer waiting for its result.
    Y_UNIT_TEST(TableNoOpWriteTimeoutDuringRollbackReleasesSession) {
        TStringStream logs;
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.SetLogStream(&logs);
        settings.AppConfig.MutableTableServiceConfig()->SetSessionsLimitPerNode(1);

        auto kikimr = TKikimrRunner(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        runtime->SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_COMPUTE, NLog::PRI_DEBUG);
        TKqpCounters counters(runtime->GetAppData().Counters);

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto createResult = kikimr.RunCall([&] { return db.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(createResult.IsSuccess(), createResult.GetIssues().ToString());
        auto session = createResult.GetSession();

        TActorId bufferActorId;
        TActorId commitExecuterId;
        TActorId rollbackExecuterId;
        TVector<THolder<IEventHandle>> heldShardResults;
        bool holdShardResults = true;
        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvKqpBuffer::TEvCommit::EventType) {
                bufferActorId = ev->GetRecipientRewrite();
                commitExecuterId = ev->Sender;
            } else if (ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWriteResult::EventType
                    && bufferActorId && ev->GetRecipientRewrite() == bufferActorId && holdShardResults) {
                heldShardResults.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            } else if (ev->GetTypeRewrite() == TEvKqpBuffer::TEvRollback::EventType
                    && ev->GetRecipientRewrite() == bufferActorId) {
                rollbackExecuterId = ev->Sender;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc); };

        auto queryFuture = kikimr.RunInThreadPool([&] {
            return session.ExecuteDataQuery(
                "SELECT Key FROM `/Root/EightShard` WHERE Key = 2u; "
                "UPDATE `/Root/EightShard` SET Text = \"unused\" "
                "WHERE Key = 2u AND Text = \"no-such-value\";",
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                TExecDataQuerySettings().OperationTimeout(TDuration::Seconds(1))).GetValueSync();
        });
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) { return !heldShardResults.empty(); });
            UNIT_ASSERT_C(runtime->DispatchEvents(opts, TDuration::Seconds(10)),
                "The no-op write did not reach buffer rollback");
        }
        runtime->SimulateSleep(TDuration::Seconds(3));
        auto result = runtime->WaitFuture(queryFuture);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::TIMEOUT, result.GetIssues().ToString());
        UNIT_ASSERT_C(rollbackExecuterId && rollbackExecuterId != commitExecuterId, logs.Str());

        // Deliver every delayed shard response. The buffer must now reply to
        // the cleanup executer, even though rollback began for the old commit.
        holdShardResults = false;
        for (auto& ev : heldShardResults) {
            runtime->Send(ev.Release());
        }
        kikimr.RunCall([&] { return session.Close().GetValueSync(); });
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) {
                return counters.GetActiveSessionActors()->Val() == 0;
            });
            UNIT_ASSERT_C(runtime->DispatchEvents(opts, TDuration::Seconds(10)), logs.Str());
        }
        UNIT_ASSERT_VALUES_EQUAL(counters.GetActiveSessionActors()->Val(), 0);
        auto nextCreate = kikimr.RunCall([&] { return db.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(nextCreate.IsSuccess(), nextCreate.GetIssues().ToString());
    }

    // Regression test for YDBREQUESTS-8110: even if a final rollback response
    // is lost, an idle Table session must not own its quota slot forever.
    Y_UNIT_TEST(IdleSessionCleanupTimeoutReleasesSessionSlot) {
        TStringStream logs;
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.SetLogStream(&logs);
        settings.AppConfig.MutableTableServiceConfig()->SetSessionsLimitPerNode(1);
        settings.AppConfig.MutableTableServiceConfig()->SetSessionIdleDurationSeconds(1);

        auto kikimr = TKikimrRunner(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        runtime->SetLogPriority(NKikimrServices::KQP_PROXY, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

        NKqp::TKqpCounters counters(runtime->GetAppData().Counters);

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto createResult = kikimr.RunCall([&] { return db.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(createResult.IsSuccess(), createResult.GetIssues().ToString());
        auto session = createResult.GetSession();

        // Leave a read-write transaction open in an otherwise IDLE Table session.
        // Final idle cleanup must roll it back before the session actor can die.
        auto queryResult = kikimr.RunCall([&] {
            return session.ExecuteDataQuery(
                "UPSERT INTO `/Root/EightShard` (Key, Text) VALUES (100501u, \"idle-tx\");",
                TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();
        });
        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        UNIT_ASSERT(queryResult.GetTransaction());
        UNIT_ASSERT_VALUES_EQUAL(counters.GetActiveSessionActors()->Val(), 1);

        TActorId sessionActorId;
        bool idleCloseSeen = false;
        bool rollbackResponseDropped = false;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvKqp::TEvCloseSessionRequest::EventType && !idleCloseSeen) {
                idleCloseSeen = true;
                sessionActorId = ev->GetRecipientRewrite();
            } else if (ev->GetTypeRewrite() == TEvKqpExecuter::TEvTxResponse::EventType
                    && sessionActorId && ev->GetRecipientRewrite() == sessionActorId) {
                rollbackResponseDropped = true;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // The KQP proxy checks idle sessions every two seconds. Wait until it
        // starts final cleanup and the rollback executer replies to the session.
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(
                [&](IEventHandle&) { return rollbackResponseDropped; });
            UNIT_ASSERT_C(runtime->DispatchEvents(opts, TDuration::Seconds(10)),
                "Idle cleanup did not start a rollback for the open transaction");
        }
        UNIT_ASSERT(idleCloseSeen);
        UNIT_ASSERT(sessionActorId);

        // Until the cleanup watchdog fires, losing the response leaves the
        // session actor in CleanupState and keeps the session slot occupied.
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(
                [&](IEventHandle&) { return counters.GetActiveSessionActors()->Val() == 0; });
            UNIT_ASSERT_C(!runtime->DispatchEvents(opts, TDuration::Seconds(3)),
                "Session unexpectedly finished without its rollback response");
        }
        UNIT_ASSERT_VALUES_EQUAL(counters.GetActiveSessionActors()->Val(), 1);

        // The soft watchdog makes the otherwise silent phase observable at WARN.
        runtime->SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_WARN);
        runtime->SimulateSleep(TDuration::Seconds(61));
        const TString capturedLogs = logs.Str();
        UNIT_ASSERT_C(capturedLogs.Contains("Session cleanup is taking too long"), capturedLogs);
        UNIT_ASSERT_C(capturedLogs.Contains("ActorState: CleanupState"), capturedLogs);
        UNIT_ASSERT_C(capturedLogs.Contains(TString(session.GetId())), capturedLogs);
        UNIT_ASSERT_VALUES_EQUAL(counters.GetActiveSessionActors()->Val(), 1);

        // The hard watchdog must finish the session locally and notify the KQP
        // proxy, even though the expected rollback response never arrives.
        runtime->SimulateSleep(TDuration::Minutes(4) + TDuration::Seconds(1));
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back(
                [&](IEventHandle&) { return counters.GetActiveSessionActors()->Val() == 0; });
            UNIT_ASSERT_C(runtime->DispatchEvents(opts, TDuration::Seconds(10)),
                "Cleanup timeout did not release the active session actor");
        }
        UNIT_ASSERT_VALUES_EQUAL(counters.GetActiveSessionActors()->Val(), 0);

        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        auto secondCreate = kikimr.RunCall([&] { return db.CreateSession().GetValueSync(); });
        UNIT_ASSERT_C(secondCreate.IsSuccess(), secondCreate.GetIssues().ToString());

        const TString finalLogs = logs.Str();
        UNIT_ASSERT_C(finalLogs.Contains("Session cleanup timed out; forcing session close"), finalLogs);
        UNIT_ASSERT_C(finalLogs.Contains(TString(session.GetId())), finalLogs);
    }
}
}
}
