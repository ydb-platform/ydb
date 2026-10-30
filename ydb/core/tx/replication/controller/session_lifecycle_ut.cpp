#include "controller_impl.h"

#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/testlib/tx_helpers.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>

#include <memory>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(SessionLifecycle) {
    using namespace NTestHelpers;

    // Exercise the real resolver and its sender/cookie checks while controlling
    // the external secret lookup and the order of delivery to the controller.
    class TSecretGate {
        TTestActorRuntime& Runtime;
        TTestActorRuntime::TEventObserverHolder Observer;

    public:
        bool Hold = false;
        TAutoPtr<IEventHandle> Pending;

        explicit TSecretGate(TTestActorRuntime& runtime)
            : Runtime(runtime)
            , Observer(runtime.AddObserver([this](IEventHandle::TPtr& ev) {
                if (ev->GetTypeRewrite() == TEvTxProxySchemeCache::EvNavigateKeySet
                    && Runtime.FindActor(ev->Sender)->GetActivityType()
                        == NKikimrServices::TActivity::REPLICATION_CONTROLLER_SECRET_RESOLVER)
                {
                    auto request = ev->Get<TEvTxProxySchemeCache::TEvNavigateKeySet>()->Request;
                    request->ResultSet.front().Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
                    request->ResultSet.front().SecurityObject = nullptr;
                    Runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(),
                        new TEvTxProxySchemeCache::TEvNavigateKeySetResult(request)));
                    ev.Reset();
                } else if (ev->GetTypeRewrite() == TEvPrivate::EvResolveSecretResult) {
                    auto result = MakeHolder<TEvPrivate::TEvResolveSecretResult>(
                        ev->Get<TEvPrivate::TEvResolveSecretResult>()->ReplicationId, "root@builtin");
                    ev.Reset(new IEventHandle(ev->GetRecipientRewrite(), ev->Sender,
                        result.Release(), 0, ev->Cookie));
                    if (Hold) {
                        UNIT_ASSERT(!Pending);
                        Pending = std::move(ev);
                    }
                }
            }))
        {}

        void Release() {
            UNIT_ASSERT(Pending);
            Hold = false;
            Runtime.Send(Pending.Release());
        }
    };

    struct TSetup {
        TEnv<> Env;
        std::unique_ptr<TSecretGate> Secret;
        TActorId Edges[2];
        TActorId Controller;
        ui64 ControllerId = 0;
        TPathId PathId;
        TWorkerId Worker{0, 0, 0};
        const bool Transfer;

        explicit TSetup(ui32 nodeCount = 2, bool transfer = false, bool secret = false)
            : Env(true, ERuntimeMode::Simulated, nodeCount)
            , Transfer(transfer)
        {
            UNIT_ASSERT(nodeCount >= 1 && nodeCount <= 2);
            auto& runtime = Env.GetRuntime();
            runtime.SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);
            if (secret) {
                Secret = std::make_unique<TSecretGate>(runtime);
            }

            for (ui32 node = 0; node < nodeCount; ++node) {
                Edges[node] = runtime.AllocateEdgeActor(node);
                runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(node)),
                    runtime.Register(CreateReplicationMockService(Edges[node]), node), node);
            }

            Env.RunCall([&] {
                NYdb::NTable::TTableClient client(Env.GetDriver());
                auto session = client.CreateSession().GetValueSync().GetSession();

                if (Transfer) {
                    auto result = session.ExecuteSchemeQuery(R"(
                        CREATE TABLE `/Root/table` (Key Uint64, Value String, PRIMARY KEY (Key));
                    )").GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                    result = session.ExecuteSchemeQuery(R"(
                        CREATE TOPIC `/Root/topic`;
                    )").GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                    result = session.ExecuteSchemeQuery(Sprintf(R"(
                        CREATE TRANSFER `/Root/transfer`
                            FROM `/Root/topic` TO `/Root/table`
                            USING ($x) -> { RETURN <| Key:$x._offset, Value:$x._data |> }
                        WITH (CONNECTION_STRING = "grpc://%s/?database=/Root");
                    )", Env.GetEndpoint().c_str())).GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                    return;
                }

                auto result = session.ExecuteSchemeQuery(R"(
                    CREATE TABLE `/Root/table` (key Uint32, PRIMARY KEY (key));
                )").GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                result = session.ExecuteSchemeQuery(Sprintf(R"(
                    CREATE ASYNC REPLICATION `replication` FOR
                        `/Root/table` AS `/Root/replica`
                    WITH (CONNECTION_STRING = "grpc://%s/?database=/Root"%s);
                )", Env.GetEndpoint().c_str(), secret ? ", TOKEN_SECRET_PATH = \"/Root/source-token\"" : "")).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            });

            PathId = Env.GetPathId(Transfer ? "/Root/transfer" : "/Root/replication");

            for (ui32 node = 0; node < nodeCount; ++node) {
                const auto handshake = runtime.GrabEdgeEvent<TEvService::TEvHandshake>(Edges[node]);
                UNIT_ASSERT(handshake);

                Controller = handshake->Sender;
                ControllerId = handshake->Get()->Record.GetController().GetTabletId();
            }

            // Start on node 0 only, so placement is deterministic.
            Status(0, false);
            const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(Edges[0]);
            UNIT_ASSERT(run);
            Worker = TWorkerId::Parse(run->Get()->Record.GetWorker());

            if (nodeCount > 1) {
                Status(1, false);
            }

            // Queue processing is driven explicitly below, without advancing timers.
            runtime.SetScheduledEventFilter([](TTestActorRuntimeBase& runtime,
                    TAutoPtr<IEventHandle>&, TDuration delay, TInstant& deadline) {
                deadline = runtime.GetCurrentTime() + delay;
                return false;
            });
        }

        void Reboot() {
            Env.SendAsync(ControllerId, new TEvents::TEvPoisonPill());
            const auto handshake = Env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(Edges[0]);
            UNIT_ASSERT(handshake);
            Controller = handshake->Sender;
            Sync();
        }

        NKikimrReplication::TEvDescribeReplicationResult Describe() {
            auto request = MakeHolder<TEvController::TEvDescribeReplication>();
            PathId.ToProto(request->Record.MutablePathId());

            const auto response = Env.Send<TEvController::TEvDescribeReplicationResult>(Controller, std::move(request));
            UNIT_ASSERT_C(response->Get()->Record.GetStatus()
                == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS,
                response->Get()->Record.ShortDebugString());
            return response->Get()->Record;
        }

        void Sync() {
            Describe();
        }

        void ReportLag(ui32 node, TDuration lag) {
            Env.GetRuntime().Send(new IEventHandle(Controller, Edges[node],
                new TEvService::TEvWorkerStatus(Worker, lag)));
            Sync();
        }

        void ExpectLag(std::optional<TDuration> lag) {
            const auto desc = Describe();
            UNIT_ASSERT_VALUES_EQUAL(desc.TargetsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTargets(0).HasLagMilliSeconds(), lag.has_value());
            UNIT_ASSERT(desc.GetState().HasStandBy());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetState().GetStandBy().HasLagMilliSeconds(), lag.has_value());
            if (lag) {
                UNIT_ASSERT_VALUES_EQUAL(desc.GetTargets(0).GetLagMilliSeconds(), lag->MilliSeconds());
                UNIT_ASSERT_VALUES_EQUAL(desc.GetState().GetStandBy().GetLagMilliSeconds(), lag->MilliSeconds());
            }
        }

        void Status(ui32 node, bool hasWorker = true) {
            auto status = MakeHolder<TEvService::TEvStatus>();
            if (hasWorker) {
                Worker.Serialize(*status->Record.AddWorkers());
            }

            Env.GetRuntime().Send(new IEventHandle(Controller, Edges[node], status.Release()));
            Sync();
        }

        void ProcessQueues(bool syncService = false) {
            Env.SendAsync(Controller, new TEvPrivate::TEvProcessQueues());
            Sync();
            if (syncService) {
                // Drain the service mailbox before asserting that no run was sent.
                Env.GetRuntime().Send(new IEventHandle(MakeReplicationServiceId(Edges[0].NodeId()),
                    Env.GetSender(), new TEvService::TEvHandshake(0, 0)), 0, true);
                UNIT_ASSERT(Env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(Edges[0]));
            }
        }

        void Disconnect(ui32 node) {
            Env.SendAsync(Controller, new TEvInterconnect::TEvNodeDisconnected(Edges[node].NodeId()));
            Sync();
        }

        void ExpectStop(ui32 node) {
            const auto stop = Env.GetRuntime().GrabEdgeEvent<TEvService::TEvStopWorker>(Edges[node]);
            UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), Worker);
        }

        void ExpectRun(ui32 node) {
            const auto run = Env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(Edges[node]);
            UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(run->Get()->Record.GetWorker()), Worker);
        }

        void AlterState(const TString& state) {
            Env.RunCall([&] {
                NYdb::NTable::TTableClient client(Env.GetDriver());
                auto session = client.CreateSession().GetValueSync().GetSession();
                const auto result = session.ExecuteSchemeQuery(Sprintf(R"(
                    ALTER %s SET (STATE = "%s");
                )", Transfer ? "TRANSFER `/Root/transfer`" : "ASYNC REPLICATION `/Root/replication`",
                    state.c_str())).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            });
        }

        bool HasWorkerRow() {
            NKikimrMiniKQL::TResult result;
            const auto query = Sprintf(R"((
                (let key '('('ReplicationId (Uint64 '%lu)) '('TargetId (Uint64 '%lu)) '('WorkerId (Uint64 '%lu))))
                (return (AsList (SetResult 'Worker (SelectRow 'Workers key '('WorkerId)))))
            ))", Worker.ReplicationId(), Worker.TargetId(), Worker.WorkerId());
            UNIT_ASSERT_VALUES_EQUAL(LocalQuery(Env.GetRuntime(), ControllerId, query, result), NKikimrProto::OK);
            return result.GetValue().GetStruct(0).GetOptional().HasOptional();
        }
    };

    Y_UNIT_TEST(LagUnavailableAfterStopWhileWaitingInBootQueue) {
        TSetup s(1);
        auto& runtime = s.Env.GetRuntime();
        s.ReportLag(0, TDuration::Seconds(5));
        s.ExpectLag(TDuration::Seconds(5));
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
        s.Sync();
        s.ExpectLag(std::nullopt);

        s.ProcessQueues();
        s.ExpectRun(0);
        s.ExpectLag(std::nullopt); // Starting alone cannot revive the previous sample.
        s.ReportLag(0, TDuration::Seconds(1));
        s.ExpectLag(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(LagCannotBeRestoredByReportAfterWorkerError) {
        TSetup s(1);
        auto& runtime = s.Env.GetRuntime();
        s.ReportLag(0, TDuration::Seconds(5));
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
                NKikimrReplication::TEvWorkerStatus::REASON_ERROR, "reader failed")));
        s.Sync();
        s.ReportLag(0, TDuration::Zero());
        const auto desc = s.Describe();
        UNIT_ASSERT(desc.GetState().HasError());
        UNIT_ASSERT_VALUES_EQUAL(desc.TargetsSize(), 1);
        UNIT_ASSERT(!desc.GetTargets(0).HasLagMilliSeconds());
    }

    Y_UNIT_TEST(LagStaysUnavailableWhileBootQueueHasNoReadySession) {
        TSetup s(1);
        s.ReportLag(0, TDuration::Seconds(5));
        s.Disconnect(0);
        s.ProcessQueues();
        s.ExpectLag(std::nullopt);
        // The mock service does not reply to the restored handshake. Queue
        // processing keeps retrying, but cannot attach the worker for 70 seconds.
        s.Env.GetRuntime().SimulateSleep(TDuration::Seconds(70));
        s.ExpectLag(std::nullopt);
        s.Status(0, false);
        s.ProcessQueues();
        s.ExpectRun(0);
        s.ExpectLag(std::nullopt);
        s.ReportLag(0, TDuration::Zero());
        s.ExpectLag(TDuration::Zero());
    }

    Y_UNIT_TEST(LagCannotBeRestoredByReportWhileStopping) {
        TSetup s(1);
        s.ReportLag(0, TDuration::Seconds(5));
        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRemoveWorker(
            s.Worker.ReplicationId(), s.Worker.TargetId(), s.Worker.WorkerId()));
        s.Sync();
        s.ProcessQueues();
        s.ExpectStop(0);
        s.ExpectLag(std::nullopt);
        s.ReportLag(0, TDuration::Zero());
        s.Status(0); // Same-owner status does not undo a pending stop.
        s.ReportLag(0, TDuration::Zero());
        s.ExpectLag(std::nullopt);
    }

    Y_UNIT_TEST(LagUnavailableAfterSessionLossAndReattachment) {
        TSetup s(2);
        s.ReportLag(0, TDuration::Seconds(5));
        s.Disconnect(0);
        s.ExpectLag(std::nullopt);
        // Adopt the worker on another service without processing BootQueue.
        s.Status(1);
        s.ExpectLag(std::nullopt);
        s.ReportLag(0, TDuration::Seconds(9));
        s.ExpectLag(std::nullopt);
        s.ReportLag(1, TDuration::Seconds(2));
        s.ExpectLag(TDuration::Seconds(2));
        s.Status(1); // Repeating status for the current owner preserves freshness.
        s.ExpectLag(TDuration::Seconds(2));
    }

    Y_UNIT_TEST(LagRejectsFormerSessionReportsAndStops) {
        TSetup s(2);
        auto& runtime = s.Env.GetRuntime();
        s.ReportLag(0, TDuration::Seconds(5));
        s.Status(1); // Ownership moves while the former service session still exists.
        s.ExpectLag(std::nullopt);
        s.ReportLag(0, TDuration::Seconds(9));
        s.ExpectLag(std::nullopt);
        s.ReportLag(1, TDuration::Seconds(2));
        s.ProcessQueues();
        s.ExpectStop(0); // Stopping the former copy cannot invalidate the current owner.
        s.ExpectLag(TDuration::Seconds(2));
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
        s.Sync();
        s.ExpectLag(TDuration::Seconds(2));
    }

    Y_UNIT_TEST(LagExpiresWithoutReportsOrQueueProgress) {
        TSetup s(1);
        auto& runtime = s.Env.GetRuntime();
        ui32 expirations = 0;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->GetTypeRewrite() == TEvPrivate::EvProcessQueues) {
                ev.Reset();
            } else if (ev->GetTypeRewrite() == TEvPrivate::EvExpireWorkerLag) {
                ++expirations;
            }
        });
        s.ReportLag(0, TDuration::Seconds(5));
        runtime.SimulateSleep(TDuration::Seconds(59));
        s.ExpectLag(TDuration::Seconds(5));
        runtime.SimulateSleep(TDuration::Seconds(2));
        // These are real scheduled expiry events, not manually delivered checks.
        UNIT_ASSERT_GT(expirations, 0);
        s.ExpectLag(std::nullopt);
        s.ReportLag(0, TDuration::Seconds(3));
        s.ExpectLag(TDuration::Seconds(3));
    }

    Y_UNIT_TEST(LagPeriodicIdleReportsRemainAvailable) {
        TSetup s(1);
        auto& runtime = s.Env.GetRuntime();
        s.ReportLag(0, TDuration::Zero());
        // No topic records are produced. The slowest healthy report interval is 14s.
        for (ui32 i = 0; i < 7; ++i) {
            runtime.SimulateSleep(TDuration::Seconds(14));
            s.ExpectLag(TDuration::Zero());
            s.ReportLag(0, TDuration::Zero());
        }
    }

    Y_UNIT_TEST(LagMissingValueAndAckDoNotRefreshReport) {
        TSetup s(1);
        auto& runtime = s.Env.GetRuntime();
        s.ReportLag(0, TDuration::Seconds(5));
        runtime.SimulateSleep(TDuration::Seconds(50));
        auto info = MakeHolder<TEvService::TEvWorkerStatus>(s.Worker, TDuration::Zero());
        info->Record.ClearLagMilliSeconds();
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0], info.Release()));
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING)));
        auto stats = MakeHolder<TEvService::TEvWorkerStatus>(s.Worker, TDuration::Zero());
        stats->Record.SetReason(NKikimrReplication::TEvWorkerStatus::REASON_STATS);
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0], stats.Release()));
        s.Sync();
        s.ExpectLag(TDuration::Seconds(5));
        runtime.SimulateSleep(TDuration::Seconds(11));
        s.ExpectLag(std::nullopt);
    }

    class TLagExpiryProbe {
        TTestActorRuntime& Runtime;
        TTestActorRuntime::TScheduledEventFilter PreviousFilter;
        TTestActorRuntime::TEventObserverHolder Observer;

    public:
        ui32 Scheduled = 0;
        ui32 Delivered = 0;

        explicit TLagExpiryProbe(TTestActorRuntime& runtime)
            : Runtime(runtime)
            , PreviousFilter(runtime.SetScheduledEventFilter([this](TTestActorRuntimeBase& runtime,
                    TAutoPtr<IEventHandle>& ev, TDuration delay, TInstant& deadline) {
                deadline = runtime.GetCurrentTime() + delay;
                if (ev->GetTypeRewrite() == TEvPrivate::EvExpireWorkerLag) {
                    ++Scheduled;
                }
                return false;
            }))
            , Observer(runtime.AddObserver<TEvPrivate::TEvExpireWorkerLag>(
                [this](TEvPrivate::TEvExpireWorkerLag::TPtr&) { ++Delivered; }))
        {}

        ~TLagExpiryProbe() {
            Runtime.SetScheduledEventFilter(PreviousFilter);
        }
    };

    Y_UNIT_TEST(LagRefreshKeepsEarlierWakeupAndReschedulesAtNewDeadline) {
        TSetup s(1);
        auto& runtime = s.Env.GetRuntime();
        TLagExpiryProbe probe(runtime);
        s.ReportLag(0, TDuration::Seconds(5));
        runtime.SimulateSleep(TDuration::Seconds(30));
        for (ui32 i = 0; i < 10; ++i) {
            s.ReportLag(0, TDuration::Zero());
        }
        UNIT_ASSERT_VALUES_EQUAL(probe.Scheduled, 1);

        runtime.SimulateSleep(TDuration::Seconds(31));
        s.ExpectLag(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(probe.Delivered, 1);
        UNIT_ASSERT_VALUES_EQUAL(probe.Scheduled, 2);
        runtime.SimulateSleep(TDuration::Seconds(28));
        s.ExpectLag(TDuration::Zero());
        runtime.SimulateSleep(TDuration::Seconds(2));
        s.ExpectLag(std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(probe.Delivered, 2);
        runtime.SimulateSleep(TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL(probe.Scheduled, 2);
        UNIT_ASSERT_VALUES_EQUAL(probe.Delivered, 2);
    }

    Y_UNIT_TEST(LagRemovalAndReuseKeepOnePendingWakeup) {
        TSetup s(1);
        auto& runtime = s.Env.GetRuntime();
        TLagExpiryProbe probe(runtime);
        s.ReportLag(0, TDuration::Seconds(5));
        runtime.SimulateSleep(TDuration::Seconds(20));
        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRemoveWorker(
            s.Worker.ReplicationId(), s.Worker.TargetId(), s.Worker.WorkerId()));
        s.Sync();
        s.ProcessQueues();
        s.ExpectStop(0);
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
        s.Sync();
        UNIT_ASSERT(!s.HasWorkerRow());

        // Removal emptied the expiry queue, but its timer is still pending.
        // Re-adopting the same identity must replace every old aggregation entry.
        s.Status(0);
        s.ReportLag(0, TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(probe.Scheduled, 1);
        runtime.SimulateSleep(TDuration::Seconds(41));
        s.ExpectLag(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(probe.Delivered, 1);
        UNIT_ASSERT_VALUES_EQUAL(probe.Scheduled, 2);
        runtime.SimulateSleep(TDuration::Seconds(20));
        s.ExpectLag(std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(probe.Delivered, 2);
    }

    Y_UNIT_TEST(LagEqualDeadlinesExpireInBoundedMailboxBatches) {
        TSetup s(1);
        auto& runtime = s.Env.GetRuntime();
        TLagExpiryProbe probe(runtime);
        constexpr ui32 WorkerCount = 251;
        TVector<TWorkerId> workers;
        auto status = MakeHolder<TEvService::TEvStatus>();
        for (ui32 i = 0; i < WorkerCount; ++i) {
            workers.emplace_back(s.Worker.ReplicationId(), s.Worker.TargetId(), s.Worker.WorkerId() + i);
            workers.back().Serialize(*status->Record.AddWorkers());
        }
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0], status.Release()));
        s.Sync();
        const auto reportedAt = runtime.GetCurrentMonotonicTime();
        for (const auto& worker : workers) {
            runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
                new TEvService::TEvWorkerStatus(worker, TDuration::Zero())));
        }
        s.Sync();
        UNIT_ASSERT_VALUES_EQUAL(runtime.GetCurrentMonotonicTime(), reportedAt);
        s.ExpectLag(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(probe.Scheduled, 1);

        const auto edge = runtime.AllocateEdgeActor();
        ui32 batches = 0;
        bool describeBetweenBatches = false;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->GetTypeRewrite() == TEvPrivate::EvExpireWorkerLag) {
                if (++batches == 1) {
                    auto request = MakeHolder<TEvController::TEvDescribeReplication>();
                    s.PathId.ToProto(request->Record.MutablePathId());
                    runtime.Send(new IEventHandle(s.Controller, edge, request.Release()), 0, true);
                }
            } else if (ev->GetTypeRewrite() == TEvController::EvDescribeReplication && ev->Sender == edge) {
                describeBetweenBatches = batches == 1;
            }
        });
        runtime.SimulateSleep(TDuration::Seconds(61));
        s.ExpectLag(std::nullopt);
        UNIT_ASSERT_C(describeBetweenBatches, "expiry continuation did not yield the mailbox");
        UNIT_ASSERT_VALUES_EQUAL(batches, 3); // 100, 100, 51 workers.
        UNIT_ASSERT_VALUES_EQUAL(probe.Delivered, 3);
        UNIT_ASSERT_VALUES_EQUAL(probe.Scheduled, 1); // Continuations are sent, not timed.
        runtime.SimulateSleep(TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL(probe.Delivered, 3);
        UNIT_ASSERT_VALUES_EQUAL(probe.Scheduled, 1);
    }

    void RecoverWithSecret(bool adopt, bool secretFirst, bool disconnect, bool readSource = false) {
        TSetup s(1, false, true);
        auto& runtime = s.Env.GetRuntime();
        UNIT_ASSERT(s.Secret);
        s.Secret->Hold = true;

        // Recovery must succeed independently of the registrar. In particular,
        // it cannot replace an adopted worker's stale command with a fresh one.
        TVector<TEvService::TEvRunWorker::TPtr> registrations;
        ui32 runs = 0;
        auto observer = runtime.AddObserver<TEvService::TEvRunWorker>([&](TEvService::TEvRunWorker::TPtr& ev) {
            if (ev->Get()->Record.HasController() && ev->GetRecipientRewrite() != s.Edges[0]) {
                ++runs;
            } else if (ev->GetRecipientRewrite() == s.Controller) {
                registrations.push_back(std::move(ev));
            }
        });

        if (adopt) {
            // An older controller may not have persisted service workers. Make
            // adoption the only way to reconstruct this worker after recovery.
            NKikimrMiniKQL::TResult result;
            const auto query = Sprintf(R"((
                (let key '('('ReplicationId (Uint64 '%lu)) '('TargetId (Uint64 '%lu)) '('WorkerId (Uint64 '%lu))))
                (return (AsList (EraseRow 'Workers key)))
            ))", s.Worker.ReplicationId(), s.Worker.TargetId(), s.Worker.WorkerId());
            UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, s.ControllerId, query, result), NKikimrProto::OK);
            UNIT_ASSERT(!s.HasWorkerRow());
        }

        s.Reboot();
        runtime.WaitFor("held secret result", [&] { return bool(s.Secret->Pending); }, TDuration::Seconds(30));
        if (secretFirst) {
            s.Secret->Release();
            s.Sync();
        }
        s.Status(0, adopt);

        s.ProcessQueues(true);
        UNIT_ASSERT_VALUES_EQUAL_C(runs, 0, "persisted worker booted before the recovery grace period");

        // Explicitly expire the startup grace period. Existing workers remain
        // adopted, while missing persisted workers become eligible for boot.
        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvBootPersistedWorkers());
        s.Sync();
        s.ProcessQueues(true);
        UNIT_ASSERT_VALUES_EQUAL(runs, !adopt && secretFirst ? 1 : 0);

        // Reproduce the incident's stale-cache ordering on disconnect; the
        // STOPPED case below also covers a restart requested while unresolved.
        const bool resolveBeforeRestart = !secretFirst && adopt && disconnect;
        if (resolveBeforeRestart) {
            s.Secret->Release();
            s.Sync();
            s.ProcessQueues(true);
            UNIT_ASSERT_VALUES_EQUAL(runs, 0);
        }

        if (adopt) {
            if (disconnect) {
                s.Disconnect(0);
                s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRestoreSession(s.Edges[0].NodeId()));
                s.Sync();
                UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvHandshake>(s.Edges[0]));
                s.Status(0, false);
            } else {
                runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
                    new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
                s.Sync();
            }
        }
        s.ProcessQueues(true);
        if (!secretFirst && !resolveBeforeRestart) {
            UNIT_ASSERT_VALUES_EQUAL_C(runs, 0, "worker booted before the secret resolved");
            s.Secret->Release();
            s.Sync();
            s.ProcessQueues(true);
        }

        auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(s.Edges[0]);
        UNIT_ASSERT(run);
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(run->Get()->Record.GetWorker()), s.Worker);
        UNIT_ASSERT_VALUES_EQUAL(run->Get()->Record.GetCommand().GetRemoteTopicReader()
            .GetConnectionParams().GetOAuthToken().GetToken(), "root@builtin");
        UNIT_ASSERT_VALUES_EQUAL(runs, 1);

        if (readSource) {
            // Run the recovered command through the real service and SDK reader,
            // verifying the token on the source RPC as well as row delivery.
            ui32 authenticatedReads = 0;
            ui32 finishedReads = 0;
            auto readObserver = runtime.AddObserver<NGRpcService::TEvStreamTopicReadRequest>(
                [&](NGRpcService::TEvStreamTopicReadRequest::TPtr& ev) {
                    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetYdbToken().GetOrElse(""), "root@builtin");
                    ev->Get()->SetAuditLogHook([&](auto, const auto&) { ++finishedReads; });
                    ++authenticatedReads;
                });
            const auto controller = run->Get()->Record.GetController();
            const auto service = runtime.Register(CreateReplicationService());
            runtime.Send(new IEventHandle(service, s.Controller, new TEvService::TEvHandshake(
                s.ControllerId, controller.GetGeneration())), 0, true);
            runtime.Send(run->Forward(service), 0, true);

            s.Env.RunCall([&] {
                NYdb::NTable::TTableClient client(s.Env.GetDriver());
                auto session = client.CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery("UPSERT INTO `/Root/table` (key) VALUES (42u)",
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                for (const auto deadline = TInstant::Now() + TDuration::Seconds(30); TInstant::Now() < deadline;) {
                    result = session.ExecuteDataQuery("SELECT key FROM `/Root/replica` WHERE key = 42u",
                        NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                    if (result.GetResultSet(0).RowsCount()) {
                        return;
                    }
                    Sleep(TDuration::MilliSeconds(50));
                }
                UNIT_FAIL("recovered worker did not consume the source row");
            });
            UNIT_ASSERT_GT(authenticatedReads, 0);

            // Close the live SDK stream while its source GRpc server is alive.
            auto stop = MakeHolder<TEvService::TEvStopWorker>();
            stop->Record.MutableController()->CopyFrom(controller);
            s.Worker.Serialize(*stop->Record.MutableWorker());
            runtime.Send(new IEventHandle(service, s.Controller, stop.Release()), 0, true);
            runtime.WaitFor("source read stream closed", [&] { return finishedReads > 0; }, TDuration::Seconds(30));
            runtime.Send(new IEventHandle(service, s.Controller, new TEvents::TEvPoison()), 0, true);
            runtime.WaitFor("replication service stopped", [&] { return !runtime.FindActor(service); }, TDuration::Seconds(30));
        }
    }

    Y_UNIT_TEST_TWIN(AdoptedWorkerWaitsForSecretAfterStop, SecretFirst) {
        RecoverWithSecret(true, SecretFirst, false);
    }

    Y_UNIT_TEST_TWIN(AdoptedWorkerWaitsForSecretAfterDisconnect, SecretFirst) {
        RecoverWithSecret(true, SecretFirst, true);
    }

    Y_UNIT_TEST_TWIN(PersistedWorkerWaitsForSecret, SecretFirst) {
        RecoverWithSecret(false, SecretFirst, false);
    }

    Y_UNIT_TEST(ChildWorkerWaitsForSecretAfterDataEnd) {
        TSetup s(1, false, true);
        auto& runtime = s.Env.GetRuntime();
        s.Secret->Hold = true;
        s.Reboot();
        runtime.WaitFor("held secret result", [&] { return bool(s.Secret->Pending); }, TDuration::Seconds(30));
        s.Status(0);

        ui32 runs = 0;
        auto observer = runtime.AddObserver<TEvService::TEvRunWorker>([&](TEvService::TEvRunWorker::TPtr& ev) {
            if (ev->Get()->Record.HasController() && ev->GetRecipientRewrite() != s.Edges[0]) {
                ++runs;
            }
        });
        const TWorkerId child(s.Worker.ReplicationId(), s.Worker.TargetId(), s.Worker.WorkerId() + 1);
        auto dataEnd = MakeHolder<TEvService::TEvWorkerDataEnd>();
        s.Worker.Serialize(*dataEnd->Record.MutableWorker());
        dataEnd->Record.AddChildPartitionsIds(child.WorkerId());
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0], dataEnd.Release()));
        s.Sync();
        s.ProcessQueues(true);
        UNIT_ASSERT_VALUES_EQUAL_C(runs, 0, "child worker booted before the secret resolved");

        s.Secret->Release();
        s.Sync();
        s.ProcessQueues(true);
        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(s.Edges[0]);
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(run->Get()->Record.GetWorker()), child);
        UNIT_ASSERT_VALUES_EQUAL(run->Get()->Record.GetCommand().GetRemoteTopicReader()
            .GetConnectionParams().GetOAuthToken().GetToken(), "root@builtin");
        UNIT_ASSERT_VALUES_EQUAL(runs, 1);
    }

    Y_UNIT_TEST(RecoveredWorkerAuthenticatesAndConsumesAfterDelayedSecret) {
        RecoverWithSecret(true, false, false, true);
    }

    Y_UNIT_TEST(DisconnectBeforeStopAcknowledgement) {
        TSetup s;

        // Both nodes report the same worker before the queued stops are sent.
        s.Status(1);
        s.Status(0);

        s.ProcessQueues();
        s.ExpectStop(0);
        s.ExpectStop(1);

        // The stop acknowledgement is lost with the session. The worker must
        // lose its owner so another node can adopt it, without a stale session ID.
        s.Disconnect(0);
        s.Status(1);

        s.Disconnect(1);

        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRestoreSession(s.Edges[0].NodeId()));
        s.Sync();
        UNIT_ASSERT(s.Env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(s.Edges[0]));

        s.Status(0, false);
        s.ProcessQueues();
        s.ExpectRun(0);
    }

    Y_UNIT_TEST(OldNodeDisconnectKeepsNewOwner) {
        TSetup s;

        s.Status(1);
        s.ProcessQueues();
        s.ExpectStop(0);

        // An acknowledgement from the old node must not detach the new owner.
        ui32 duplicateRuns = 0;
        auto observer = s.Env.GetRuntime().AddObserver<TEvService::TEvRunWorker>(
            [&](TEvService::TEvRunWorker::TPtr&) { ++duplicateRuns; });

        s.Env.GetRuntime().Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
        s.Sync();
        s.ProcessQueues();
        UNIT_ASSERT_VALUES_EQUAL(duplicateRuns, 0);
        observer.Remove();

        s.Disconnect(0);
        s.Status(1);

        s.Disconnect(1);

        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRestoreSession(s.Edges[0].NodeId()));
        s.Sync();
        UNIT_ASSERT(s.Env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(s.Edges[0]));

        s.Status(0, false);
        s.ProcessQueues();
        s.ExpectRun(0);
    }

    Y_UNIT_TEST(RemovalDetachesWorkerFromSession) {
        TSetup s;

        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRemoveWorker(
            s.Worker.ReplicationId(), s.Worker.TargetId(), s.Worker.WorkerId()));
        s.Sync();
        s.ProcessQueues();
        s.ExpectStop(0);

        s.Env.GetRuntime().Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
        s.Sync();

        // Wait for the removal transaction before reusing the same worker ID.
        NKikimrMiniKQL::TResult result;
        const auto query = Sprintf(R"((
            (let key '('('ReplicationId (Uint64 '%lu)) '('TargetId (Uint64 '%lu)) '('WorkerId (Uint64 '%lu))))
            (return (AsList (SetResult 'Worker (SelectRow 'Workers key '('WorkerId)))))
        ))", s.Worker.ReplicationId(), s.Worker.TargetId(), s.Worker.WorkerId());
        UNIT_ASSERT_VALUES_EQUAL(LocalQuery(s.Env.GetRuntime(), s.ControllerId, query, result), NKikimrProto::OK);
        UNIT_ASSERT_C(!result.GetValue().GetStruct(0).GetOptional().HasOptional(), result.ShortDebugString());

        s.Sync();
        s.Status(1);

        // A stale entry in node 0's session would clear node 1's new owner
        // and incorrectly boot a second copy when node 0 disconnects.
        ui32 duplicateRuns = 0;
        auto observer = s.Env.GetRuntime().AddObserver<TEvService::TEvRunWorker>(
            [&](TEvService::TEvRunWorker::TPtr&) { ++duplicateRuns; });

        s.Disconnect(0);
        s.ProcessQueues();
        UNIT_ASSERT_VALUES_EQUAL(duplicateRuns, 0);
    }

    // The node is still in discovery, so the session is restored after a disconnect.
    // A failed restore handshake is reported the way interconnect does for an event
    // with FlagSubscribeOnSession: TEvNodeDisconnected to the sender.
    Y_UNIT_TEST(WorkerBootsAfterFailedRestoreHandshake) {
        TSetup s(1);
        auto& runtime = s.Env.GetRuntime();
        const auto nodeId = s.Edges[0].NodeId();

        ui32 dropped = 0;
        bool dropHandshake = true;
        auto observer = runtime.AddObserver<TEvService::TEvHandshake>([&](TEvService::TEvHandshake::TPtr& ev) {
            if (!dropHandshake || ev->GetRecipientRewrite().NodeId() != nodeId) {
                return;
            }
            ++dropped;
            runtime.Send(new IEventHandle(ev->Sender, ev->Sender, new TEvInterconnect::TEvNodeDisconnected(nodeId)));
            ev.Reset();
        });

        s.Disconnect(0);
        runtime.DispatchEvents({}, TDuration::Seconds(5));
        UNIT_ASSERT_C(dropped > 1, "restore is not retried after a failed handshake, dropped: " << dropped);
        dropHandshake = false;
        observer.Remove();

        const auto handshake = runtime.GrabEdgeEvent<TEvService::TEvHandshake>(s.Edges[0], TDuration::Seconds(15));
        UNIT_ASSERT_C(handshake, "no handshake after the node returns");

        s.Status(0, false);
        s.ProcessQueues();
        s.ExpectRun(0);
    }

    // The stop is sent, then the node disconnects before STATUS_STOPPED arrives.
    // DeleteSession moves the worker back to BootQueue regardless of RemoveQueue,
    // so the removal never completes.
    Y_UNIT_TEST(TransferRemovalCompletesWhenStopAcknowledgementIsLost) {
        TSetup s(2, true);
        auto& runtime = s.Env.GetRuntime();

        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRemoveWorker(
            s.Worker.ReplicationId(), s.Worker.TargetId(), s.Worker.WorkerId()));
        s.Sync();
        s.ProcessQueues();
        s.ExpectStop(0);

        ui32 runs = 0;
        auto observer = runtime.AddObserver<TEvService::TEvRunWorker>(
            [&](TEvService::TEvRunWorker::TPtr&) { ++runs; });

        s.Disconnect(0);
        for (ui32 i = 0; i < 10; ++i) {
            s.ProcessQueues();
        }
        UNIT_ASSERT_VALUES_EQUAL_C(runs, 0, "worker pending removal was booted again");

        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_C(!s.HasWorkerRow(), "worker pending removal is never removed");
    }

    // Incident scenario: a transfer is paused while nodes restart. The node with
    // the worker restarts after the stop is sent, the acknowledgement is lost.
    // The worker stays in RemoveQueue forever, the target never leaves Alter,
    // and neither pause nor resume can recover it; only a controller restart can.
    void ResumeAfterPauseWithLostStopAcknowledgement(bool transfer) {
        TSetup s(1, transfer);
        auto& runtime = s.Env.GetRuntime();
        const auto nodeId = s.Edges[0].NodeId();

        s.AlterState("Paused");
        s.ProcessQueues();
        s.ExpectStop(0);

        // The node restarts: the acknowledgement is lost, the new process has no workers.
        s.Disconnect(0);
        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRestoreSession(nodeId));
        s.Sync();
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvHandshake>(s.Edges[0]));
        s.Status(0, false);
        s.ProcessQueues();

        s.AlterState("StandBy");
        for (ui32 i = 0; i < 50; ++i) {
            s.ProcessQueues();
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
        }

        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(s.Edges[0], TDuration::Seconds(10));
        UNIT_ASSERT_C(run, "worker does not start after resume");
    }

    Y_UNIT_TEST(TransferResumeAfterPauseWithLostStopAcknowledgement) {
        ResumeAfterPauseWithLostStopAcknowledgement(true);
    }

    Y_UNIT_TEST(ReplicationResumeAfterPauseWithLostStopAcknowledgement) {
        ResumeAfterPauseWithLostStopAcknowledgement(false);
    }

    // The same pause and resume with a delivered acknowledgement.
    Y_UNIT_TEST(TransferResumeAfterPause) {
        TSetup s(1, true);
        auto& runtime = s.Env.GetRuntime();

        s.AlterState("Paused");
        s.ProcessQueues();
        s.ExpectStop(0);
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
        s.Sync();

        s.AlterState("StandBy");
        for (ui32 i = 0; i < 50; ++i) {
            s.ProcessQueues();
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
        }

        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(s.Edges[0], TDuration::Seconds(10));
        UNIT_ASSERT_C(run, "transfer worker does not start after resume");
    }
}

}
