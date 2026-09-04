#include "common/simple/services.h"

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/shutdown/state.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/shutdown/controller.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/session_actor/kqp_query_state.h>
#include <ydb/services/workload_manager/events.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/util/ulid.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <library/cpp/iterator/functools.h>

#include <util/system/sanitizers.h>

#include <ydb/core/tx/datashard/datashard_failpoints.h>
#include <ydb/core/grpc_services/cancelation/cancelation_event.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;


namespace {
    void TestShutdownNodeAndExecuteQuery(TKikimrRunner& kikimr, const TString& query, ui32 nodeIndexToShutdown, ui32 expectedMinShutdownEvents,
        NYdb::EStatus expectedStatus, const TString& stageDescription)
    {
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto queryClient = kikimr.RunCall([&] { return kikimr.GetQueryClient(); } );

        auto nodeId = runtime.GetNodeId(nodeIndexToShutdown);
        ui32 nodeShuttingDownCount = 0;

        auto grab = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            if (ev->GetTypeRewrite() == TEvKqpNode::TEvStartKqpTasksResponse::EventType) {
                auto& msg = ev->Get<TEvKqpNode::TEvStartKqpTasksResponse>()->Record;
                if (msg.NotStartedTasksSize() > 0) {
                    for (auto& task : msg.GetNotStartedTasks()) {
                        if (task.GetReason() == NKikimrKqp::TEvStartKqpTasksResponse::NODE_SHUTTING_DOWN) {
                            ++nodeShuttingDownCount;
                        }
                    }
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime.SetObserverFunc(grab);

        auto shutdownState = new TKqpShutdownState();
        runtime.Send(new IEventHandle(NKqp::MakeKqpNodeServiceID(nodeId), {},
                     new TEvKqp::TEvInitiateShutdownRequest(shutdownState)), nodeIndexToShutdown);

        auto future = kikimr.RunInThreadPool([&queryClient, &query](){
            return queryClient.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });

        if (expectedMinShutdownEvents > 0) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&nodeShuttingDownCount, expectedMinShutdownEvents](IEventHandle&) {
                return nodeShuttingDownCount >= expectedMinShutdownEvents;
            });
            runtime.DispatchEvents(opts);
        }

        auto result = runtime.WaitFuture(future);

        UNIT_ASSERT_C(nodeShuttingDownCount >= expectedMinShutdownEvents,
            stageDescription << ": Expected at least " << expectedMinShutdownEvents
            << " NODE_SHUTTING_DOWN responses, got: " << nodeShuttingDownCount);

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus,
            stageDescription << ": Unexpected result status. Got issues: " << result.GetIssues().ToString());
    }
} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpService) {
    Y_UNIT_TEST(QueryTxIdResetUnsetsValue) {
        TULIDGenerator ulidGen;
        TKqpQueryState::TQueryTxId txId;

        UNIT_ASSERT(!txId.HasValue());

        const auto firstId = ulidGen.Next();
        txId.SetValue(firstId);
        UNIT_ASSERT(txId.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(txId.GetValue().GetHumanStr(), firstId.ToString());

        // Reset() must return the id to the unset state. Storing a default constructed
        // TTxId instead leaves the underlying TMaybe engaged, and then every later
        // SetValue() fails with "SetValue(): requirement !Id failed".
        txId.Reset();
        UNIT_ASSERT(!txId.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(txId.GetValue().GetHumanStr(), "");

        const auto secondId = ulidGen.Next();
        txId.SetValue(secondId);
        UNIT_ASSERT(txId.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(txId.GetValue().GetHumanStr(), secondId.ToString());
    }

    Y_UNIT_TEST(DuplicatedWorkloadManagerContinueRequest) {
        NKikimrConfig::TAppConfig app;
        app.MutableFeatureFlags()->SetEnableResourcePools(true);

        TKikimrRunner kikimr(TKikimrSettings(app)
            .SetWithSampleTables(false)
            .SetUseRealThreads(false));

        auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });

        {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteQuery(R"(
                    CREATE RESOURCE POOL test_pool WITH (concurrent_query_limit = 1);
                )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        ui32 continueRequests = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NWorkloadManager::TEvContinueRequest::EventType && ++continueRequests == 1) {
                const auto* msg = ev->Get<NWorkloadManager::TEvContinueRequest>();
                auto copy = std::make_unique<NWorkloadManager::TEvContinueRequest>(
                    msg->QueryId, msg->Status, msg->PoolId, msg->PoolConfig, msg->Issues);
                runtime.Send(new IEventHandle(ev->Recipient, ev->Sender, copy.release(), ev->Flags, ev->Cookie));
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto result = kikimr.RunCall([&] {
            return session.ExecuteQuery("SELECT 42;",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
                NYdb::NQuery::TExecuteQuerySettings().ResourcePool("test_pool")).GetValueSync();
        });

        UNIT_ASSERT_C(continueRequests > 0, "Query did not go through workload manager admission");
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(Shutdown) {
        const ui32 Inflight = 50;
        const TDuration WaitDuration = TDuration::Seconds(1);

        auto kikimr = MakeHolder<TKikimrRunner>();

        NPar::LocalExecutor().RunAdditionalThreads(Inflight);
        auto driverConfig = kikimr->GetDriverConfig();

        NYdb::TDriver driver(driverConfig);
        NPar::LocalExecutor().ExecRange([driver](int id) {
            NYdb::NTable::TTableClient db(driver);

            auto sessionResult = db.CreateSession().GetValueSync();
            if (!sessionResult.IsSuccess()) {
                if (!sessionResult.IsTransportError()) {
                    sessionResult.GetIssues().PrintTo(Cerr);
                }

                return;
            }

            auto session = sessionResult.GetSession();

            while (true) {
                auto params = session.GetParamsBuilder()
                    .AddParam("$key").Uint32(id).Build()
                    .AddParam("$value").Int32(id).Build()
                    .Build();

                auto result = session.ExecuteDataQuery(R"(
                    DECLARE $key AS Uint32;
                    DECLARE $value AS Int32;

                    SELECT * FROM `/Root/EightShard`;

                    UPSERT INTO `/Root/TwoShard` (Key, Value2) VALUES
                        ($key, $value);
                )", TTxControl::BeginTx().CommitTx(), params).GetValueSync();

                if (result.IsTransportError()) {
                    return;
                }

                result.GetIssues().PrintTo(Cerr);
            }

        }, 0, Inflight, NPar::TLocalExecutor::MED_PRIORITY);

        Sleep(WaitDuration);
        kikimr.Reset();
        Sleep(WaitDuration);
        driver.Stop(true);
    }

    Y_UNIT_TEST(CloseSessionAbortQueryExecution) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        auto kikimr = TKikimrRunner(settings);

        auto runtime = kikimr.GetTestServer().GetRuntime();

        runtime->SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_COMPILE_ACTOR, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NLog::PRI_DEBUG);

        auto db = kikimr.GetQueryClient();

        {
            auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); } );
            auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); } );
            kikimr.RunCall([&]() {CreateLargeTable(kikimr, 100, 2, 2, 10, 2);});
        }

        ui32 stateEvents = 0;
        auto grab = [&stateEvents](TAutoPtr<IEventHandle>& ev) -> auto {
            if (ev->GetTypeRewrite() == NYql::NDq::TEvDqCompute::TEvState::EventType) {
                ++stateEvents;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime->SetObserverFunc(grab);
        Y_DEFER {
            runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        };

        NDataShard::gSkipReadIteratorResultFailPoint.Enable(-1);
        Y_DEFER {
            NDataShard::gSkipReadIteratorResultFailPoint.Disable();
        };

        auto session = kikimr.RunCall([&] { return db.GetSession().ExtractValueSync().GetSession(); } );

        auto future = kikimr.RunInThreadPool([&] { return session.ExecuteQuery("select * from `/Root/LargeTable`", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync(); });

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&stateEvents](IEventHandle&) {
            return stateEvents > 0;
        });
        runtime->DispatchEvents(opts);

        Cerr << "OK! Passed the test. Compute Actors are started" << Endl;

        auto close = std::make_unique<TEvKqp::TEvCloseSessionRequest>();
        close->Record.MutableRequest()->SetSessionId(TString(session.GetId()));
        auto sender = kikimr.GetTestServer().GetRuntime()->AllocateEdgeActor();
        runtime->Send(new IEventHandle(MakeKqpProxyID(kikimr.GetTestServer().GetRuntime()->GetNodeId(0)), sender, close.release()));

        auto result = kikimr.GetTestServer().GetRuntime()->WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::CANCELLED, result.GetIssues().ToString());
    }

    // Verifies issue #39166: the executer must terminate every still-running compute actor
    // when the query is aborted (e.g. session close, client lost), via TEvAbortExecution.
    // Compute actors no longer arm their own timeout timer (the planner now passes an empty
    // Deadline to the CA factory, matching kqp_query_control_plane.cpp), so the executer's
    // broadcast is the sole mechanism that stops them on cancellation. This test asserts:
    //   (a) every CA that registered with the executer received TEvAbortExecution, and
    //   (b) no CA ever called Schedule(... TEvWakeup(TimeoutTag) ...) on itself.
    // (b) is checked at schedule time via SetScheduledEventFilter, not at delivery time:
    // the cancel arrives within seconds, while any plausible CA self-timeout would be far
    // longer, so delivery-time observation would silently pass even on the old code.
    Y_UNIT_TEST(CancelTerminatesAllComputeActors) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        auto kikimr = TKikimrRunner(settings);

        auto runtime = kikimr.GetTestServer().GetRuntime();

        runtime->SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_DEBUG);

        auto db = kikimr.GetQueryClient();

        {
            auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
            auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
            kikimr.RunCall([&]() { CreateLargeTable(kikimr, 100, 2, 2, 10, 2); });
        }

        // Origin tracking: a KQP compute actor is uniquely characterised by being the sender
        // of TEvDqCompute::TEvState whose recipient is the query's executer. We capture the
        // executer from the first TEvState we see (only one query runs in this test), then
        // accept further events only if they tie back to that exact executer:
        //   - TEvState        : recipient must be `executerActor` (else: foreign CA, rejected)
        //   - TEvAbortExecution: sender must be `executerActor` AND recipient a tracked CA
        // The scheduled-event filter records every TimeoutTag wakeup as it is armed; we
        // intersect that set with the CA set at the end of the test (Bootstrap schedules
        // the timer before the "Hello" TEvState, so recipients are not yet known when the
        // schedule happens). EEvWakeupTag::TimeoutTag = 1 in dq_compute_actor_impl.h.
        constexpr ui64 timeoutWakeupTag = 1;
        TActorId executerActor;
        THashSet<TActorId> computeActors;
        THashSet<TActorId> abortedComputeActors;
        THashSet<TActorId> timeoutTagScheduleRecipients;
        ui32 stateEvents = 0;
        ui32 foreignStateEvents = 0;
        ui32 foreignAbortEvents = 0;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            const auto type = ev->GetTypeRewrite();
            if (type == NYql::NDq::TEvDqCompute::TEvState::EventType) {
                const auto recipient = ev->GetRecipientRewrite();
                if (!executerActor) {
                    executerActor = recipient;
                }
                if (recipient == executerActor) {
                    computeActors.insert(ev->Sender);
                    ++stateEvents;
                } else {
                    ++foreignStateEvents;
                }
            } else if (type == TEvKqp::TEvAbortExecution::EventType) {
                if (computeActors.contains(ev->GetRecipientRewrite())) {
                    if (ev->Sender == executerActor) {
                        abortedComputeActors.insert(ev->GetRecipientRewrite());
                    } else {
                        ++foreignAbortEvents;
                    }
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc); };

        // Filter fires inside Schedule(...) before the event enters the queue. Returning
        // false keeps the event scheduled (we observe, we don't drop). We can't yet tell
        // whether the recipient is a CA at this point, so record all candidates and
        // resolve against `computeActors` after the test completes.
        auto prevScheduledFilter = runtime->SetScheduledEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev, TDuration, TInstant&) {
                if (ev->GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType) {
                    if (auto* msg = ev->Get<NActors::TEvents::TEvWakeup>(); msg && msg->Tag == timeoutWakeupTag) {
                        timeoutTagScheduleRecipients.insert(ev->GetRecipientRewrite());
                    }
                }
                return false;
            });
        Y_DEFER { runtime->SetScheduledEventFilter(prevScheduledFilter); };

        // Stall reads so the query hangs with all its compute actors alive.
        NDataShard::gSkipReadIteratorResultFailPoint.Enable(-1);
        Y_DEFER { NDataShard::gSkipReadIteratorResultFailPoint.Disable(); };

        auto session = kikimr.RunCall([&] { return db.GetSession().ExtractValueSync().GetSession(); });

        auto future = kikimr.RunInThreadPool([&] {
            return session.ExecuteQuery("select * from `/Root/LargeTable`",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        });

        // Wait until at least a few compute actors have come up.
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) { return stateEvents >= 3; });
            runtime->DispatchEvents(opts);
        }

        const auto computeActorsBeforeCancel = computeActors;
        UNIT_ASSERT_C(!computeActorsBeforeCancel.empty(),
            "Expected at least one compute actor to register before cancel");
        UNIT_ASSERT_C(executerActor, "Failed to capture executer actor id from TEvState recipient");
        UNIT_ASSERT_VALUES_EQUAL_C(foreignStateEvents, 0u,
            "Observed TEvDqCompute::TEvState events with recipient != executerActor "
            "(unexpected second executer in test). executer=" << executerActor);

        // Trigger cancellation: session close -> session actor -> executer ->
        //   TerminateComputeActors -> TEvAbortExecution to every alive CA.
        auto close = std::make_unique<TEvKqp::TEvCloseSessionRequest>();
        close->Record.MutableRequest()->SetSessionId(TString(session.GetId()));
        auto sender = runtime->AllocateEdgeActor();
        runtime->Send(new IEventHandle(MakeKqpProxyID(runtime->GetNodeId(0)), sender, close.release()));

        auto result = runtime->WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::CANCELLED, result.GetIssues().ToString());

        // Every CA that registered before cancel must have received TEvAbortExecution.
        TVector<TActorId> notAborted;
        for (const auto& ca : computeActorsBeforeCancel) {
            if (!abortedComputeActors.contains(ca)) {
                notAborted.push_back(ca);
            }
        }
        UNIT_ASSERT_C(notAborted.empty(),
            "Compute actors not terminated by executer broadcast: "
                << JoinSeq(", ", notAborted)
                << " (registered=" << computeActorsBeforeCancel.size()
                << ", aborted=" << abortedComputeActors.size()
                << ", executer=" << executerActor << ")");
        UNIT_ASSERT_VALUES_EQUAL_C(foreignAbortEvents, 0u,
            "Observed TEvAbortExecution to a tracked CA from a sender other than executerActor "
            "(" << executerActor << "); CAs must be terminated by their own executer.");

        // Fix verification: no compute actor armed its own TimeoutTag timer. Checked at
        // schedule time, not delivery time — see header comment.
        TVector<TActorId> selfTimedCAs;
        for (const auto& ca : computeActors) {
            if (timeoutTagScheduleRecipients.contains(ca)) {
                selfTimedCAs.push_back(ca);
            }
        }
        UNIT_ASSERT_C(selfTimedCAs.empty(),
            "Compute actors armed their own TimeoutTag timer (see issue #39166): "
                << JoinSeq(", ", selfTimedCAs));
    }

    // Repro for the cross-node client-cancel "subscription race" (the Proxy/Forwarded
    // path). When a query-service request is served by a session on a different node than
    // the gRPC request actor, the session learns about client cancel only by subscribing
    // (TEvSubscribeGrpcCancel) to that remote gRPC actor. If the gRPC actor already died on
    // its own client-lost before the subscription is delivered, the subscription is
    // silently dropped and the session never tears down -> the executer and all its compute
    // actors leak until the query deadline.
    //
    // Reproduced deterministically on a single node: drive KqpProxy with a TEvQueryRequest
    // that has no RequestCtx (so the session takes the remote CancelationActor cancel path,
    // exactly as a forwarded request does) and a CancelationActor that is dead by the time
    // the subscription is delivered. We hold the subscription until the compute actors are
    // up (emulating cross-node latency), then deliver it to the dead actor. With the fix the
    // FlagTrackDelivery bounce (TEvUndelivered) makes the session treat it as client-lost
    // and abort every compute actor; without the fix nothing happens and they leak (this
    // assertion fails).
    Y_UNIT_TEST(RemoteClientLostSubscriptionRaceTerminatesComputeActors) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        auto kikimr = TKikimrRunner(settings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        runtime->SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_SESSION, NLog::PRI_DEBUG);

        {
            auto tdb = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
            kikimr.RunCall([&]() { CreateLargeTable(kikimr, 100, 2, 2, 10, 2); });
        }

        // The "remote gRPC request actor" the session subscribes to for client-cancel.
        // It is never registered, so by the time the held subscription is delivered the
        // actor is dead -> FlagTrackDelivery bounces TEvUndelivered back to the session.
        const TActorId deadRpcActor(runtime->GetNodeId(0), 0, 0xDEAD, 0);

        TActorId executerActor;
        THashSet<TActorId> computeActors;
        THashSet<TActorId> abortedComputeActors;
        ui32 stateEvents = 0;
        THolder<IEventHandle> heldSubscribe;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            const auto type = ev->GetTypeRewrite();
            if (type == NGRpcService::TEvSubscribeGrpcCancel::EventType &&
                ev->GetRecipientRewrite() == deadRpcActor && !heldSubscribe) {
                // Emulate cross-node latency: hold the subscription until the compute
                // actors are up, so any teardown happens with CAs still alive.
                heldSubscribe.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (type == NYql::NDq::TEvDqCompute::TEvState::EventType) {
                const auto recipient = ev->GetRecipientRewrite();
                if (!executerActor) {
                    executerActor = recipient;
                }
                if (recipient == executerActor) {
                    computeActors.insert(ev->Sender);
                    ++stateEvents;
                }
            } else if (type == TEvKqp::TEvAbortExecution::EventType) {
                if (computeActors.contains(ev->GetRecipientRewrite()) && ev->Sender == executerActor) {
                    abortedComputeActors.insert(ev->GetRecipientRewrite());
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Y_DEFER { runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc); };

        // Stall reads so the query hangs with all its compute actors alive.
        NDataShard::gSkipReadIteratorResultFailPoint.Enable(-1);
        Y_DEFER { NDataShard::gSkipReadIteratorResultFailPoint.Disable(); };

        // Drive KqpProxy directly with a request that looks forwarded from another node:
        // no RequestCtx (so the session uses the remote CancelationActor cancel path) and a
        // CancelationActor pointing at the already-dead gRPC request actor.
        auto edge = runtime->AllocateEdgeActor();
        auto ev = std::make_unique<TEvKqp::TEvQueryRequest>();
        ActorIdToProto(edge, ev->Record.MutableRequestActorId());
        ActorIdToProto(deadRpcActor, ev->Record.MutableCancelationActor());
        auto& req = *ev->Record.MutableRequest();
        req.SetDatabase("/Root");
        req.SetQuery("SELECT * FROM `/Root/LargeTable`");
        req.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        req.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
        auto* txControl = req.MutableTxControl();
        txControl->mutable_begin_tx()->mutable_serializable_read_write();
        txControl->set_commit_tx(true);
        runtime->Send(new IEventHandle(MakeKqpProxyID(runtime->GetNodeId(0)), edge, ev.release()));

        // Wait until the compute actors are up AND the session has issued its cancel
        // subscription (which we are holding).
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) { return stateEvents >= 3 && heldSubscribe; });
            runtime->DispatchEvents(opts);
        }
        UNIT_ASSERT_C(!computeActors.empty(), "Expected compute actors to start");
        UNIT_ASSERT_C(heldSubscribe, "Session did not issue a remote cancel subscription");
        UNIT_ASSERT_C(abortedComputeActors.empty(), "Compute actors aborted before client-lost");

        const auto computeActorsBeforeCancel = computeActors;

        // The client "cancels": deliver the subscription to the now-dead gRPC actor.
        runtime->Send(heldSubscribe.Release());

        // The session must learn the client is gone and terminate every compute actor.
        {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&](IEventHandle&) {
                return abortedComputeActors.size() >= computeActorsBeforeCancel.size();
            });
            runtime->DispatchEvents(opts, TDuration::Seconds(10));
        }

        TVector<TActorId> notAborted;
        for (const auto& ca : computeActorsBeforeCancel) {
            if (!abortedComputeActors.contains(ca)) {
                notAborted.push_back(ca);
            }
        }
        UNIT_ASSERT_C(notAborted.empty(),
            "Compute actors leaked after client-lost (subscription race): "
                << JoinSeq(", ", notAborted)
                << " (registered=" << computeActorsBeforeCancel.size()
                << ", aborted=" << abortedComputeActors.size() << ")");
    }

    TVector<TAsyncDataQueryResult> simulateSessionBusy(ui32 count, TSession& session) {
        TVector<TAsyncDataQueryResult> futures;
        for (ui32 i = 0; i < count; ++i) {
            auto query = Sprintf(R"(
                SELECT * FROM `/Root/EightShard` WHERE Key=%1$d;
            )", i);

            auto future = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx());
            futures.push_back(future);
        }
        return futures;
    }

    Y_UNIT_TEST(SessionBusy) {
        NKikimrConfig::TAppConfig appConfig;

        auto kikimr = DefaultKikimrRunner({}, appConfig);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto futures = simulateSessionBusy(10, session);

        NThreading::WaitExceptionOrAll(futures).GetValueSync();

        for (auto& future : futures) {
            auto result = future.GetValue();
            if (!result.IsSuccess()) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SESSION_BUSY, result.GetIssues().ToString());
            }
        }
    }

    Y_UNIT_TEST(SessionBusyRetryOperation) {
        NKikimrConfig::TAppConfig appConfig;

        auto kikimr = DefaultKikimrRunner({}, appConfig);
        auto db = kikimr.GetTableClient();

        ui32 queriesCount = 10;
        ui32 busyResultCount = 0;
        auto status = db.RetryOperation([&queriesCount, &busyResultCount](TSession session) {
            UNIT_ASSERT(queriesCount);
            UNIT_ASSERT(!session.GetId().empty());

            auto futures = simulateSessionBusy(queriesCount, session);

            NThreading::WaitExceptionOrAll(futures).GetValueSync();

            for (auto& future : futures) {
                auto result = future.GetValue();
                if (!result.IsSuccess()) {
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SESSION_BUSY, result.GetIssues().ToString());
                    queriesCount--;
                    busyResultCount++;
                    return NThreading::MakeFuture<TStatus>(result);
                }
            }
            return NThreading::MakeFuture<TStatus>(TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues()));
         }).GetValueSync();
         // Result should be SUCCESS in case of SESSION_BUSY
         UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
    }

    Y_UNIT_TEST(SessionBusyRetryOperationSync) {
        NKikimrConfig::TAppConfig appConfig;

        auto kikimr = DefaultKikimrRunner({}, appConfig);
        auto db = kikimr.GetTableClient();

        ui32 queriesCount = 10;
        ui32 busyResultCount = 0;
        auto status = db.RetryOperationSync([&queriesCount, &busyResultCount](TSession session) {
            UNIT_ASSERT(queriesCount);
            UNIT_ASSERT(!session.GetId().empty());

            auto futures = simulateSessionBusy(queriesCount, session);

            NThreading::WaitExceptionOrAll(futures).GetValueSync();

            for (auto& future : futures) {
                auto result = future.GetValue();
                if (!result.IsSuccess()) {
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SESSION_BUSY, result.GetIssues().ToString());
                    queriesCount--;
                    busyResultCount++;
                    return (TStatus)result;
                }
            }
            return TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues());
         });
         // Result should be SUCCESS in case of SESSION_BUSY
         UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
    }

    void ConfigureSettings(TKikimrSettings & settings, bool useCache, bool useAsyncPatternCompilation, bool useCompiledCapacityBytesLimit) {
        size_t cacheSize = 0;
        if (useCache) {
            cacheSize = useAsyncPatternCompilation ? 10_MB : 1_MB;
        }

        auto * tableServiceConfig = settings.AppConfig.MutableTableServiceConfig();
        tableServiceConfig->MutableResourceManager()->SetKqpPatternCacheCapacityBytes(cacheSize);
        if (useCompiledCapacityBytesLimit) {
            tableServiceConfig->MutableResourceManager()->SetKqpPatternCacheCompiledCapacityBytes(1_MB * 0.1);
        }

        tableServiceConfig->SetEnableAsyncComputationPatternCompilation(useAsyncPatternCompilation);

        if (useAsyncPatternCompilation) {
            tableServiceConfig->MutableCompileComputationPatternServiceConfig()->SetWakeupIntervalMs(1);
            tableServiceConfig->MutableResourceManager()->SetKqpPatternCachePatternAccessTimesBeforeTryToCompile(0);
        }
    }

    enum AsyncPatternCompilationStrategy {
        Off,
        On,
        OnWithLimit,
    };

    void PatternCacheImpl(bool useCache, AsyncPatternCompilationStrategy asyncPatternCompilationStrategy) {
        bool useAsyncPatternCompilation = asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::On ||
            asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::OnWithLimit;
        bool useCompiledCapacityBytesLimit = asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::OnWithLimit;

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        ConfigureSettings(settings, useCache, useAsyncPatternCompilation, useCompiledCapacityBytesLimit);

        auto kikimr = TKikimrRunner{settings};
        auto driver = kikimr.GetDriver();

        NKqp::TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

        static constexpr i64 AsyncPatternCompilationUniqueRequestsSize = 5;

        auto async_compilation_condition = [&]() {
            if (useCache) {
                if (asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::On) {
                    return *counters.CompiledComputationPatterns != AsyncPatternCompilationUniqueRequestsSize;
                } else if (asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::OnWithLimit) {
                    return *counters.CompiledComputationPatterns < AsyncPatternCompilationUniqueRequestsSize * 4;
                }
            }

            return false;
        };

        size_t InFlight = NSan::PlainOrUnderSanitizer(10, 4);
        const ui32 IterationCount = NSan::PlainOrUnderSanitizer(500u, 50u);
        NPar::LocalExecutor().RunAdditionalThreads(InFlight);
        NPar::LocalExecutor().ExecRange([&](int /*id*/) {
            NYdb::NTable::TTableClient db(driver);
            auto session = db.CreateSession().GetValueSync().GetSession();

            for (ui32 i = 0; i < IterationCount || async_compilation_condition(); ++i) {
                ui32 value = useCache && useAsyncPatternCompilation ? i % AsyncPatternCompilationUniqueRequestsSize : i / 5;
                ui64 total = 100500;
                TString request = (TStringBuilder() << R"_(
                    $data = AsList(
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),

                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),

                        AsStruct("aaa" AS Key,)_" << total - 10 * value << R"_(u AS Value),

                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),

                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value)
                    );

                    SELECT * FROM (
                        SELECT Key, SUM(Value) as Sum FROM (
                            SELECT * FROM AS_TABLE($data)
                        ) GROUP BY Key
                    ) WHERE Key == "aaa";
                )_");

                NYdb::NTable::TExecDataQuerySettings execSettings;
                execSettings.KeepInQueryCache(true);

                auto result = session.ExecuteDataQuery(request,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), execSettings).ExtractValueSync();
                AssertSuccessResult(result);

                CompareYson(R"( [ ["aaa";100500u] ])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }, 0, InFlight, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);

        if (useCache) {
            if (asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::On) {
                UNIT_ASSERT(*counters.CompiledComputationPatterns == AsyncPatternCompilationUniqueRequestsSize);
            } else if (asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::OnWithLimit) {
                UNIT_ASSERT(*counters.CompiledComputationPatterns >= AsyncPatternCompilationUniqueRequestsSize);
            }
        }
    }

    Y_UNIT_TEST(PatternCache) {
        PatternCacheImpl(false, AsyncPatternCompilationStrategy::Off);
        PatternCacheImpl(false, AsyncPatternCompilationStrategy::On);
        PatternCacheImpl(false, AsyncPatternCompilationStrategy::OnWithLimit);
        PatternCacheImpl(true, AsyncPatternCompilationStrategy::Off);
        PatternCacheImpl(true, AsyncPatternCompilationStrategy::On);
        PatternCacheImpl(true, AsyncPatternCompilationStrategy::OnWithLimit);
    }

    // YQL-15582
    Y_UNIT_TEST_TWIN(RangeCache, UseCache) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(true);
        size_t cacheSize = UseCache ? 1_MB : 0;
        settings.AppConfig.MutableTableServiceConfig()->MutableResourceManager()->SetKqpPatternCacheCapacityBytes(cacheSize);
        auto kikimr = TKikimrRunner{settings};
        auto driver = kikimr.GetDriver();

        size_t InFlight = 10;
        NPar::LocalExecutor().RunAdditionalThreads(InFlight);
        NPar::LocalExecutor().ExecRange([&driver](int /*id*/) {
            TTimer t;
            NYdb::NTable::TTableClient db(driver);
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto query = TStringBuilder()
                << Q_(R"(
                    DECLARE $in AS List<Uint64>;
                    SELECT Key, Value FROM `/Root/KeyValue`
                    WHERE Value = "One" AND Key IN $in
                )");
            for (ui32 i = 0; i < 20; ++i) {
                auto params = TParamsBuilder();
                auto& pl = params.AddParam("$in").BeginList();
                for (auto v : {1, 2, 3, 42, 50, 100}) {
                    pl.AddListItem().Uint64(v);
                }
                pl.EndList().Build();


                auto result = session.ExecuteDataQuery(query,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params.Build()).ExtractValueSync();
                AssertSuccessResult(result);

                CompareYson(
                    R"([[[1u];["One"]]])",
                    FormatResultSetYson(result.GetResultSet(0)));
            }
        }, 0, InFlight, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
    }

    Y_UNIT_TEST_TWIN(SwitchCache, UseCache) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(true);
        size_t cacheSize = UseCache ? 1_MB : 0;
        settings.AppConfig.MutableTableServiceConfig()->MutableResourceManager()->SetKqpPatternCacheCapacityBytes(cacheSize);
        auto kikimr = TKikimrRunner{settings};
        auto driver = kikimr.GetDriver();

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TwoKeys` (
                Key1 Int32,
                Key2 Int32,
                Value Int32,
                PRIMARY KEY (Key1, Key2)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        auto result = session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/TwoKeys` (Key1, Key2, Value) VALUES
                (1, 1, 1),
                (2, 1, 2),
                (3, 2, 3),
                (4, 2, 4),
                (5, 3, 5),
                (6, 3, 6),
                (7, 4, 7),
                (8, 4, 8);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        size_t InFlight = 10;
        NPar::LocalExecutor().RunAdditionalThreads(InFlight);
        NPar::LocalExecutor().ExecRange([&driver](int /*id*/) {
            TTimer t;
            NYdb::NTable::TTableClient db(driver);
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto query = TStringBuilder()
                << Q_(R"(
                $values = SELECT Key1, Key2, Value FROM `/Root/TwoKeys` WHERE Value > 4;
                $cnt = SELECT count(*) FROM $values;
                $sum = SELECT sum(Key1) FROM $values WHERE Key1 > 4;

                SELECT $cnt + $sum;
            )");

            for (ui32 i = 0; i < 20; ++i) {
                auto result = session.ExecuteDataQuery(query,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
                AssertSuccessResult(result);

                CompareYson(
                    R"([[[30]]])",
                    FormatResultSetYson(result.GetResultSet(0)));
            }
        }, 0, InFlight, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
    }

struct TDictCase {
    const std::vector<TString> DictSet = {"($i.1)", "(Yql::Void)"};
    const std::vector<TString> Compact = {"", "AsAtom('Compact')"};
    const std::vector<TString> OneMany = {"One", "Many"};
    const std::vector<TString> SortedHashed = {"Sorted", "Hashed"};

    TString Expected;

    ui32 MaxCase = DictSet.size() * Compact.size() * OneMany.size() * SortedHashed.size();
    ui32 Case = 0;

    bool InTheEnd() const {
        return Case == MaxCase;
    }

    bool GetCase(size_t shift) {
        return (Case >> shift) & 1;
    }

    TString GetExpected() const {
        return Expected;
    }

    TString Get() {
        if (InTheEnd()) {
            Expected = "";
            return {};
        }

        {
            TStringStream expected;
            expected << "[[[[1;";
            if (OneMany[GetCase(1)] == "Many") {
                expected << "[";
            }
            if (DictSet[GetCase(3)] == "(Yql::Void)") {
                expected << "\"Void\"";
            } else {
                expected << "2";
            }
            if (OneMany[GetCase(1)] == "Many") {
                expected << "]";
            }
            expected << "]]]]";
            Expected = expected.Str();
        }

        TStringStream res;
        res << "SELECT Yql::ToDict([(1, 2)], ($i) -> ($i.0), ($i) -> " << DictSet[GetCase(3)] << ", AsTuple(";
        if (auto c = Compact[GetCase(2)]) {
            res << c << ", ";
        }
        res << "AsAtom('" << OneMany[GetCase(1)] << "'), AsAtom('" << SortedHashed[GetCase(0)] << "')));";
        ++Case;
        return res.Str();
    }
};

    // KIKIMR-18169
    Y_UNIT_TEST_TWIN(ToDictCache, UseCache) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        size_t cacheSize = UseCache ? 1_MB : 0;
        settings.AppConfig.MutableTableServiceConfig()->MutableResourceManager()->SetKqpPatternCacheCapacityBytes(cacheSize);
        auto kikimr = TKikimrRunner{settings};
        auto driver = kikimr.GetDriver();

        size_t InFlight = 4;
        NPar::LocalExecutor().RunAdditionalThreads(InFlight);

        TDictCase gen;
        while (!gen.InTheEnd()) {
            auto query = gen.Get();
            Cout << query << Endl;
            NPar::LocalExecutor().ExecRange([&driver, &query, &gen](int /*id*/) {
                TTimer t;
                NYdb::NTable::TTableClient db(driver);
                auto session = db.CreateSession().GetValueSync().GetSession();
                for (ui32 i = 0; i < 10; ++i) {
                    auto params = TParamsBuilder();

                    auto result = session.ExecuteDataQuery(query,
                        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params.Build()).ExtractValueSync();
                    AssertSuccessResult(result);

                    CompareYson(gen.GetExpected(), FormatResultSetYson(result.GetResultSet(0)));
                }
            }, 0, InFlight, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);
        }
    }

    Y_UNIT_TEST(ThreeNodesGradualShutdown) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableShuttingDownNodeState(true);

        TKikimrRunner kikimr(TKikimrSettings()
                                        .SetFeatureFlags(featureFlags)
                                        .SetNodeCount(3)
                                        .SetUseRealThreads(false));
        kikimr.RunCall([&]() {CreateLargeTable(kikimr, 100, 2, 2, 10, 2);});

        auto queries = std::vector<TString>({
            R"(
                SELECT Key, COUNT(*) AS cnt, SUM(Data) AS sum_data, MAX(DataText) AS max_text
                FROM `/Root/LargeTable`
                WHERE Data > 0
                GROUP BY Key
                ORDER BY cnt DESC
                LIMIT 100
            )",
            R"(
                SELECT Key, COUNT(*) AS cnt, MIN(Data) AS min_data, MAX(Data) AS max_data
                FROM `/Root/LargeTable`
                GROUP BY Key
                ORDER BY cnt DESC
                LIMIT 100
            )",
            R"(
                SELECT Key, COUNT(*) AS cnt, SUM(Data) AS sum_data
                FROM `/Root/EightShard`
                WHERE Data > 0
                GROUP BY Key
                ORDER BY cnt DESC
                LIMIT 100
            )"
        });
        for (size_t i = 0; i < queries.size(); ++i) {
            i32 nodeIndexToShutdown = queries.size() - (i + 1);
            TestShutdownNodeAndExecuteQuery(kikimr, queries[i], nodeIndexToShutdown, i + 1, NYdb::EStatus::SUCCESS, "Stage " + ToString(i + 1));
        }
    }

    Y_UNIT_TEST(RetryAfterShutdownThenDisconnect) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableShuttingDownNodeState(true);

        TKikimrRunner kikimr(TKikimrSettings()
                                    .SetFeatureFlags(featureFlags)
                                    .SetNodeCount(2)
                                    .SetUseRealThreads(false));
        kikimr.RunCall([&]() { CreateLargeTable(kikimr, 100, 2, 2, 10, 2); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto queryClient = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });

        ui32 nodeToShutdown = 1;
        auto shuttingDownNodeId = runtime.GetNodeId(nodeToShutdown);

        bool nodeShuttingDownReceived = false;
        bool retryStarted = false;
        TActorId executerActorId;

        auto observer = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            if (ev->GetTypeRewrite() == TEvKqpNode::TEvStartKqpTasksResponse::EventType) {
                auto& msg = ev->Get<TEvKqpNode::TEvStartKqpTasksResponse>()->Record;
                if (msg.NotStartedTasksSize() > 0) {
                    for (auto& task : msg.GetNotStartedTasks()) {
                        if (task.GetReason() == NKikimrKqp::TEvStartKqpTasksResponse::NODE_SHUTTING_DOWN) {
                            nodeShuttingDownReceived = true;
                            executerActorId = ev->Recipient;
                        }
                    }
                }
            }

            if (ev->GetTypeRewrite() == TEvKqpNode::TEvStartKqpTasksRequest::EventType) {
                if (nodeShuttingDownReceived && ev->Recipient.NodeId() != shuttingDownNodeId) {
                    retryStarted = true;
                    auto disconnectEv = new TEvInterconnect::TEvNodeDisconnected(shuttingDownNodeId);
                    runtime.Send(new IEventHandle(executerActorId, TActorId(), disconnectEv), 0, true);
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime.SetObserverFunc(observer);

        auto shutdownState = new TKqpShutdownState();
        runtime.Send(new IEventHandle(NKqp::MakeKqpNodeServiceID(shuttingDownNodeId), {},
                     new TEvKqp::TEvInitiateShutdownRequest(shutdownState)), nodeToShutdown);

        auto result = kikimr.RunCall([&queryClient]() {
            return queryClient.ExecuteQuery(R"(
                SELECT COUNT(*) AS cnt, SUM(Data) AS sum_data
                FROM `/Root/LargeTable`
                LIMIT 100
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS,
            "Expected SUCCESS because retry to another node was in progress, but got: " << result.GetIssues().ToString());
    }

    /* Scenario (rolling upgrade / node drain):
        - Every TEvStartKqpTasksRequest bounces back as undelivered with ReasonActorUnknown,
          so the executer keeps rescheduling internal retries until the budget is exhausted.
        - A node going away is a transient condition, so the query must terminate with the
          retriable UNAVAILABLE, just like the Disconnected and NODE_SHUTTING_DOWN paths do,
          and not with the non-retriable INTERNAL_ERROR.
     */
    Y_UNIT_TEST(RetriesExhaustedByActorUnknownIsUnavailable) {
        TKikimrSettings settings = TKikimrSettings()
                                    .SetNodeCount(2)
                                    .SetUseRealThreads(false);
        auto* retriesConfig = settings.AppConfig.MutableTableServiceConfig()->MutableExecuterRetriesConfig();
        retriesConfig->SetMaxRetryNumber(2);
        retriesConfig->SetMinDelayToRetryMs(1);
        retriesConfig->SetMaxDelayToRetryMs(5);

        TKikimrRunner kikimr(settings);
        kikimr.RunCall([&]() { CreateLargeTable(kikimr, 100, 2, 2, 10, 2); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto queryClient = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });

        ui32 bouncedRequests = 0;

        // Imitate a draining node: the KQP node-service actor is already gone, so every
        // attempt to start tasks there comes back with ReasonActorUnknown. Bounce the
        // requests to all the nodes, otherwise the planner may hand the tasks over to a
        // node that is not tracked yet and the outcome becomes timing dependent.
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvKqpNode::TEvStartKqpTasksRequest::EventType) {
                ++bouncedRequests;
                auto undeliveredEv = new TEvents::TEvUndelivered(
                    TEvKqpNode::TEvStartKqpTasksRequest::EventType,
                    TEvents::TEvUndelivered::ReasonActorUnknown);
                auto senderNodeIndex = ev->Recipient.NodeId() - runtime.GetNodeId(0);
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, undeliveredEv, 0, ev->Cookie),
                    senderNodeIndex, true);
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        auto result = kikimr.RunCall([&queryClient]() {
            return queryClient.ExecuteQuery(R"(
                SELECT COUNT(*) AS cnt, SUM(Data) AS sum_data
                FROM `/Root/LargeTable`
                LIMIT 100
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });

        UNIT_ASSERT_C(bouncedRequests > 0, "The query did not send a single TEvStartKqpTasksRequest, "
            "so the ActorUnknown retry path was never reached");

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::UNAVAILABLE,
            "Expected the retriable UNAVAILABLE after the ActorUnknown retries are exhausted, but got: "
                << result.GetStatus() << ", " << result.GetIssues().ToString());
    }

}

} // namespace NKqp
} // namespace NKikimr
