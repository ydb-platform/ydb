#include "controller_impl.h"
#include "target_table.h"

#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/testlib/tx_helpers.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <util/generic/scope.h>
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
                if (!ev->HasEvent()) {
                    return;
                }

                if (ev->GetTypeRewrite() == TEvTxProxySchemeCache::EvNavigateKeySet) {
                    const auto* sender = Runtime.FindActor(ev->Sender);
                    if (!sender || sender->GetActivityType()
                        != NKikimrServices::TActivity::REPLICATION_CONTROLLER_SECRET_RESOLVER)
                    {
                        return;
                    }

                    auto request = ev->Get<TEvTxProxySchemeCache::TEvNavigateKeySet>()->Request;
                    request->ResultSet.front().Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
                    request->ResultSet.front().SecurityObject = nullptr;
                    Runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(),
                        new TEvTxProxySchemeCache::TEvNavigateKeySetResult(request)));
                    ev.Reset();
                } else if (const auto* original = dynamic_cast<TEvPrivate::TEvResolveSecretResult*>(ev->GetBase())) {
                    // Private event IDs are shared with unrelated actors, including Hive.
                    auto result = MakeHolder<TEvPrivate::TEvResolveSecretResult>(
                        original->ReplicationId, "root@builtin");
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

        void ExpectLagCounters(TDuration lag, ui64 unavailable) {
            const auto response = Env.Send<TEvTablet::TEvGetCountersResponse>(
                ControllerId, new TEvTablet::TEvGetCounters());
            THashMap<TString, ui64> counters;
            for (const auto& counter : response->Get()->Record.GetTabletCounters().GetAppCounters().GetSimpleCounters()) {
                counters.emplace(counter.GetName(), counter.GetValue());
            }

            UNIT_ASSERT(counters.contains("ReplicationController/DataLag"));
            UNIT_ASSERT_VALUES_EQUAL(counters.at("ReplicationController/DataLag"), lag.MilliSeconds());
            UNIT_ASSERT(counters.contains("ReplicationController/DataLagUnavailableReplications"));
            UNIT_ASSERT_VALUES_EQUAL(counters.at("ReplicationController/DataLagUnavailableReplications"), unavailable);
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

            ExpectLagCounters(lag.value_or(TDuration::Zero()), lag ? 0 : 1);
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

        TString Monitoring(const TVector<std::pair<TString, TString>>& parameters = {}) {
            NActorsProto::TRemoteHttpInfo request;
            request.SetMethod(HTTP_METHOD_GET);
            request.SetPath("/app");
            for (const auto& [key, value] : parameters) {
                auto* parameter = request.AddQueryParams();
                parameter->SetKey(key);
                parameter->SetValue(value);
            }
            return Env.Send<NMon::TEvRemoteHttpInfoRes>(Controller,
                new NMon::TEvRemoteHttpInfo(std::move(request)))->Get()->Html;
        }
    };

    class TRegistrarPolicyTarget: public TTargetTable {
    public:
        using TTargetTable::TTargetTable;
        using TTargetTable::CreateWorkerRegistar;
    };

    class TRegistrarPolicyOwner: public TActorBootstrapped<TRegistrarPolicyOwner> {
        const TActorId Edge;
        TReplication::TPtr Replication;
        TRegistrarPolicyTarget Target;
        const NKikimrReplication::TConnectionParams Params;
        TActorId Registrar;

        template <typename TEventPtr>
        void Forward(TEventPtr& ev) {
            Send(ev->Forward(Edge));
        }

        void PassAway() override {
            Send(Registrar, new TEvents::TEvPoison());
            Replication->Shutdown(TActivationContext::AsActorContext());
            TActorBootstrapped<TRegistrarPolicyOwner>::PassAway();
        }

    public:
        TRegistrarPolicyOwner(TActorId edge, const NKikimrReplication::TConnectionParams& params)
            : Edge(edge)
            , Replication(MakeIntrusive<TReplication>(ui64(1), TPathId(1, 2), NKikimrReplication::TReplicationConfig(), TString("/Root")))
            , Target(Replication.Get(), 1, std::make_shared<TTargetTable::TTableConfig>("/Root/source", "/Root/destination"))
            , Params(params)
        {}

        void Bootstrap(const TActorContext& ctx) {
            // Create a benign local proxy without starting target discovery.
            // The registrar captures the tested connection policy separately;
            // its requests are intercepted before this proxy receives them.
            Replication->SetState(TReplication::EState::Error);
            Replication->Progress(ctx);
            UNIT_ASSERT(Replication->GetYdbProxy());
            NKikimrReplication::TReplicationConfig config;
            config.MutableSrcConnectionParams()->CopyFrom(Params);
            config.MutableConsistencySettings()->MutableRow();
            Replication->SetConfig(std::move(config));
            Target.SetStreamName("stream");
            Target.SetStreamConsumerName("consumer");
            Target.SetDstPathId(TPathId(1, 3));
            Registrar = Register(Target.CreateWorkerRegistar(ctx));
            Become(&TThis::StateWork);
        }

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvWorkerRegistrationError, Forward);
                hFunc(TEvPrivate::TEvCompleteWorkerSet, Forward);
                sFunc(TEvents::TEvPoison, PassAway);
            }
        }
    };

    Y_UNIT_TEST(WorkerRegistrarStaticAndIamAuthenticationPolicy) {
        enum class EExpectedResult { Error, RetryThenSuccess };
        struct TCase {
            NKikimrReplication::TConnectionParams::CredentialsCase Credentials;
            NYdb::EStatus Status;
            EExpectedResult Expected;
        };
        const TCase cases[] = {
            {NKikimrReplication::TConnectionParams::kStaticCredentials, NYdb::EStatus::CLIENT_UNAUTHENTICATED, EExpectedResult::Error},
            {NKikimrReplication::TConnectionParams::kStaticCredentials, NYdb::EStatus::UNAUTHORIZED, EExpectedResult::Error},
            {NKikimrReplication::TConnectionParams::kIamCredentials, NYdb::EStatus::CLIENT_UNAUTHENTICATED, EExpectedResult::RetryThenSuccess},
            {NKikimrReplication::TConnectionParams::kIamCredentials, NYdb::EStatus::UNAUTHORIZED, EExpectedResult::Error},
        };
        for (const auto& test : cases) {
            TTestActorRuntime runtime;
            runtime.Initialize(TAppPrepare().Unwrap());
            const auto edge = runtime.AllocateEdgeActor();
            NKikimrReplication::TConnectionParams params;
            params.SetEndpoint("not-contacted:1");
            params.SetDatabase("/Root");
            if (test.Credentials == NKikimrReplication::TConnectionParams::kStaticCredentials) {
                params.MutableStaticCredentials()->SetUser("policy-user");
                params.MutableStaticCredentials()->SetPassword("policy-password");
            } else {
                params.MutableIamCredentials()->SetServiceAccountId("policy-service-account");
                params.MutableIamCredentials()->SetResourceId("policy-resource");
            }
            ui32 requests = 0;
            auto intercept = runtime.AddObserver<TEvYdbProxy::TEvDescribeTopicRequest>(
                [&](TEvYdbProxy::TEvDescribeTopicRequest::TPtr& ev) {
                    runtime.EnableScheduleForActor(ev->Sender);
                    const auto status = ++requests == 1 ? test.Status : NYdb::EStatus::SUCCESS;
                    runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(),
                        new TEvYdbProxy::TEvDescribeTopicResponse(NYdb::TStatus(status, {}), Ydb::Topic::DescribeTopicResult()),
                        0, ev->Cookie));
                    ev.Reset();
                });
            const auto owner = runtime.Register(new TRegistrarPolicyOwner(edge, params));
            Y_DEFER {
                runtime.Send(new IEventHandle(owner, edge, new TEvents::TEvPoison()), 0, true);
                runtime.WaitFor("policy owner stopped", [&] { return !runtime.FindActor(owner); }, TDuration::Seconds(5));
            };
            if (test.Expected == EExpectedResult::Error) {
                const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvWorkerRegistrationError>(edge, TDuration::Seconds(5));
                UNIT_ASSERT(result);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, test.Status);
                UNIT_ASSERT_VALUES_EQUAL(requests, 1);
            } else {
                runtime.WaitFor("first IAM registrar attempt", [&] { return requests == 1; }, TDuration::Seconds(5));
                runtime.SimulateSleep(TDuration::Seconds(10));
                UNIT_ASSERT(runtime.GrabEdgeEvent<TEvPrivate::TEvCompleteWorkerSet>(edge, TDuration::Seconds(5)));
                UNIT_ASSERT_VALUES_EQUAL(requests, 2);
            }
        }
    }

    void CompleteRegistrarPause(TSetup& s) {
        const bool hadWorker = s.HasWorkerRow();
        s.AlterState("Paused");
        s.ProcessQueues();
        if (hadWorker) {
            s.ExpectStop(0);
            s.Env.GetRuntime().Send(new IEventHandle(s.Controller, s.Edges[0],
                new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
            s.Sync();
        }
        s.Env.GetRuntime().WaitFor("completed pause without worker rows", [&] {
            return s.Describe().GetState().HasPaused() && !s.HasWorkerRow();
        }, TDuration::Seconds(30));
    }

    class TRegistrarFault {
        TTestActorRuntime& Runtime;
        TTestActorRuntime::TEventObserverHolder Observer;
    public:
        ui32 Requests = 0;
        TActorId Registrar;

        TRegistrarFault(TTestActorRuntime& runtime, NYdb::EStatus status)
            : Runtime(runtime)
            , Observer(runtime.AddObserver<TEvYdbProxy::TEvDescribeTopicRequest>(
                [this, status](TEvYdbProxy::TEvDescribeTopicRequest::TPtr& ev) {
                    const auto* sender = Runtime.FindActor(ev->Sender);
                    if (!sender || sender->GetActivityType()
                        != NKikimrServices::TActivity::REPLICATION_CONTROLLER_TABLE_WORKER_REGISTAR)
                    {
                        return;
                    }
                    ++Requests;
                    if (Requests != 1) {
                        return;
                    }
                    Registrar = ev->Sender;
                    NYdb::NIssue::TIssues issues;
                    if (status == NYdb::EStatus::UNAUTHORIZED || status == NYdb::EStatus::CLIENT_UNAUTHENTICATED) {
                        NYdb::NIssue::TIssue issue("RegistrarPasswordMarker27516");
                        issue.AddSubIssue(MakeIntrusive<NYdb::NIssue::TIssue>("RegistrarTokenMarker27516"));
                        issues.AddIssue(issue);
                    }
                    Runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(),
                        new TEvYdbProxy::TEvDescribeTopicResponse(NYdb::TStatus(status, std::move(issues)),
                            Ydb::Topic::DescribeTopicResult()), 0, ev->Cookie));
                    ev.Reset();
                }))
        {}
    };

    auto WaitRegistrarWorker(TSetup& s) {
        for (ui32 i = 0; i < 50; ++i) {
            s.ProcessQueues();
            s.Env.GetRuntime().SimulateSleep(TDuration::MilliSeconds(200));
        }
        auto run = s.Env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(s.Edges[0], TDuration::Seconds(10));
        UNIT_ASSERT_C(run, "registration did not produce a worker");
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(run->Get()->Record.GetWorker()), s.Worker);
        UNIT_ASSERT(s.HasWorkerRow());
        return run;
    }

    Y_UNIT_TEST(WorkerRegistrarRetriesUnavailableAndNonStaticInitialization) {
        for (const auto status : {NYdb::EStatus::UNAVAILABLE, NYdb::EStatus::TRANSPORT_UNAVAILABLE,
                NYdb::EStatus::CLIENT_UNAUTHENTICATED}) {
            TSetup s(1);
            auto& runtime = s.Env.GetRuntime();
            CompleteRegistrarPause(s);
            TRegistrarFault fault(runtime, status);
            ui32 retries = 0;
            auto wakeups = runtime.AddObserver<TEvents::TEvWakeup>([&](TEvents::TEvWakeup::TPtr& ev) {
                if (ev->GetRecipientRewrite() == fault.Registrar) {
                    ++retries;
                }
            });
            s.AlterState("StandBy");
            runtime.WaitFor("first registrar request rejected", [&] { return fault.Requests >= 1; }, TDuration::Seconds(30));
            UNIT_ASSERT(s.Describe().GetState().HasStandBy());
            runtime.SimulateSleep(TDuration::Seconds(10));
            runtime.WaitFor("registrar scheduled retry request", [&] { return fault.Requests >= 2; }, TDuration::Seconds(30));
            UNIT_ASSERT_GT(retries, 0);
            WaitRegistrarWorker(s);
            s.ReportLag(0, TDuration::Seconds(2));
            s.ExpectLag(TDuration::Seconds(2));
        }
    }

    Y_UNIT_TEST(ObsoleteRegistrarAuthenticationErrorKeepsReplacement) {
        TSetup s(1);
        auto& runtime = s.Env.GetRuntime();
        CompleteRegistrarPause(s);
        TAutoPtr<IEventHandle> delayed;
        auto hold = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->HasEvent() && dynamic_cast<TEvPrivate::TEvWorkerRegistrationError*>(ev->GetBase())) {
                UNIT_ASSERT(!delayed);
                delayed = std::move(ev);
            }
        });
        TRegistrarFault fault(runtime, NYdb::EStatus::UNAUTHORIZED);
        s.AlterState("StandBy");
        runtime.WaitFor("actual registrar authentication result", [&] { return bool(delayed); }, TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(delayed->Sender, fault.Registrar);
        UNIT_ASSERT_STRING_CONTAINS(delayed->Get<TEvPrivate::TEvWorkerRegistrationError>()->GetIssue(), "UNAUTHORIZED");
        hold.Remove();
        CompleteRegistrarPause(s);
        s.AlterState("StandBy");
        WaitRegistrarWorker(s);
        UNIT_ASSERT_GT(fault.Requests, 1);
        s.ReportLag(0, TDuration::Seconds(2));
        s.ExpectLag(TDuration::Seconds(2));
        ui32 stops = 0;
        ui32 starts = 0;
        auto stopObserver = runtime.AddObserver<TEvService::TEvStopWorker>([&](TEvService::TEvStopWorker::TPtr&) { ++stops; });
        auto runObserver = runtime.AddObserver<TEvService::TEvRunWorker>([&](TEvService::TEvRunWorker::TPtr&) { ++starts; });
        runtime.Send(delayed.Release());
        s.Sync();
        s.ProcessQueues(true);
        UNIT_ASSERT(s.HasWorkerRow());
        s.ReportLag(0, TDuration::Seconds(1));
        s.ExpectLag(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(stops, 0);
        UNIT_ASSERT_VALUES_EQUAL(starts, 0);

    }

    Y_UNIT_TEST(WorkerRegistrarStopsAdoptedTransferAfterAuthenticationError) {
        TSetup s(1, true);
        auto& runtime = s.Env.GetRuntime();
        TString logs;
        TTestActorRuntimeBase::TEventFilter previousFilter;
        previousFilter = runtime.SetEventFilter([&](TTestActorRuntimeBase& actorRuntime, TAutoPtr<IEventHandle>& ev) {
            // The simulated executor delivers logs directly, before observers.
            // This filter sees the event sent to the real logger fast path.
            if (ev->HasEvent() && ev->GetTypeRewrite() == NLog::TEvLog::EventType) {
                const auto* log = ev->Get<NLog::TEvLog>();
                if (log->Component == NKikimrServices::REPLICATION_CONTROLLER) {
                    logs += log->Line;
                    if (log->StructuredMessage) {
                        TStringBuilder fields;
                        NActors::NStructuredLog::TTextWriter writer;
                        writer.Write(fields, log->StructuredMessage.GetRef());
                        logs += fields;
                    }
                    logs += '\n';
                }
            }
            return previousFilter(actorRuntime, ev);
        });
        Y_DEFER { runtime.SetEventFilter(std::move(previousFilter)); };
        NActors::NStructuredLog::TStructuredMessage probe;
        probe.AppendFixedValue({"probe"}, "RegistrarCaptureMarker27516");
        runtime.Send(new IEventHandle(runtime.GetActorSystem(0)->LoggerSettings()->LoggerActorId, s.Env.GetSender(),
            new NLog::TEvLog(NLog::PRI_TRACE, NKikimrServices::REPLICATION_CONTROLLER,
                __FILE__, __LINE__, "Registrar log capture calibration", std::move(probe))), 0, true);
        TRegistrarFault fault(runtime, NYdb::EStatus::UNAUTHORIZED);
        TAutoPtr<IEventHandle> delayed;
        auto hold = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->HasEvent() && dynamic_cast<TEvPrivate::TEvWorkerRegistrationError*>(ev->GetBase())) {
                UNIT_ASSERT(!delayed);
                delayed = std::move(ev);
            }
        });
        auto expectDurableError = [&] {
            const auto desc = s.Describe();
            UNIT_ASSERT(desc.GetState().HasError());
            const auto issue = desc.GetState().GetError().issues(0).message();
            UNIT_ASSERT_STRING_CONTAINS(issue, "authentication");
            UNIT_ASSERT_STRING_CONTAINS(issue, "UNAUTHORIZED");
            UNIT_ASSERT(!issue.Contains("RegistrarPasswordMarker27516"));
            UNIT_ASSERT(!issue.Contains("RegistrarTokenMarker27516"));
            NKikimrMiniKQL::TResult result;
            const auto query = Sprintf(R"((
                (let replicationKey '('('Id (Uint64 '%lu))))
                (let targetKey '('('ReplicationId (Uint64 '%lu)) '('Id (Uint64 '%lu))))
                (return (AsList
                    (SetResult 'Replication (SelectRow 'Replications replicationKey '('State 'Issue)))
                    (SetResult 'Target (SelectRow 'Targets targetKey '('DstState 'Issue)))))
            ))", s.Worker.ReplicationId(), s.Worker.ReplicationId(), s.Worker.TargetId());
            UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, s.ControllerId, query, result), NKikimrProto::OK);
            const auto& replication = result.GetValue().GetStruct(0).GetOptional().GetOptional();
            const auto& target = result.GetValue().GetStruct(1).GetOptional().GetOptional();
            UNIT_ASSERT_VALUES_EQUAL(replication.GetStruct(1).GetOptional().GetUint32(), static_cast<ui32>(TReplication::EState::Error));
            UNIT_ASSERT_VALUES_EQUAL(target.GetStruct(0).GetOptional().GetUint32(), static_cast<ui32>(TReplication::EDstState::Error));
            UNIT_ASSERT_VALUES_EQUAL(replication.GetStruct(0).GetOptional().GetText(), issue);
            UNIT_ASSERT_STRING_CONTAINS(target.GetStruct(1).GetOptional().GetText(), "UNAUTHORIZED");
            UNIT_ASSERT(!target.GetStruct(1).GetOptional().GetText().Contains("RegistrarPasswordMarker27516"));
            UNIT_ASSERT(!target.GetStruct(1).GetOptional().GetText().Contains("RegistrarTokenMarker27516"));
        };
        s.Reboot();
        runtime.WaitFor("restarted transfer registrar rejected", [&] { return bool(delayed); }, TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(delayed->Sender, fault.Registrar);
        s.Status(0, true);
        UNIT_ASSERT(s.HasWorkerRow());
        hold.Remove();
        runtime.Send(delayed.Release());
        s.Sync();
        expectDurableError();
        s.ProcessQueues();
        // Consume the exact stop event. Edge observers may revisit a retained
        // event while another mailbox fence is awaited, so do not count visits.
        s.ExpectStop(0);
        ui32 starts = 0;
        auto noRestart = runtime.AddObserver<TEvService::TEvRunWorker>([&](TEvService::TEvRunWorker::TPtr&) { ++starts; });
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
        s.Sync();
        s.ProcessQueues(true);
        UNIT_ASSERT_VALUES_EQUAL(starts, 0);
        expectDurableError();
        s.Reboot();
        expectDurableError();
        runtime.Send(runtime.GetActorSystem(0)->LoggerSettings()->LoggerActorId, s.Env.GetSender(),
            new TLogComponentLevelRequest(NLog::PRI_TRACE, NKikimrServices::REPLICATION_CONTROLLER));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TLogComponentLevelResponse>(s.Env.GetSender(), TDuration::Seconds(5)));
        UNIT_ASSERT_STRING_CONTAINS(logs, "RegistrarCaptureMarker27516");
        UNIT_ASSERT_STRING_CONTAINS(logs, "Source authentication failed during worker registration");
        UNIT_ASSERT(!logs.Contains("RegistrarPasswordMarker27516"));
        UNIT_ASSERT(!logs.Contains("RegistrarTokenMarker27516"));
    }

    Y_UNIT_TEST(LagUnavailableAfterStopWhileWaitingInBootQueue) {
        TSetup s(1);
        auto& runtime = s.Env.GetRuntime();
        s.ExpectLag(std::nullopt);
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

    Y_UNIT_TEST(LagCountersAggregateReplicationsAndTrackRemoval) {
        TSetup s(1);
        auto& runtime = s.Env.GetRuntime();
        s.ReportLag(0, TDuration::Seconds(5));

        // SchemeShard normally allocates separate tablets. Create another
        // replication directly to exercise the controller's shared counters.
        const TPathId otherPath(s.PathId.OwnerId, s.PathId.LocalPathId + 1);
        auto create = MakeHolder<TEvController::TEvCreateReplication>();
        otherPath.ToProto(create->Record.MutablePathId());
        create->Record.SetDatabase("/Root");
        auto* config = create->Record.MutableConfig();
        config->MutableSrcConnectionParams()->SetEndpoint(s.Env.GetEndpoint());
        config->MutableSrcConnectionParams()->SetDatabase("/Root");
        config->MutableSrcConnectionParams()->MutableOAuthToken()->SetToken("");
        config->MutableConsistencySettings()->MutableRow();
        auto* target = config->MutableSpecific()->AddTargets();
        target->SetSrcPath("/Root/table");
        target->SetDstPath("/Root/other_replica");
        const auto created = s.Env.Send<TEvController::TEvCreateReplicationResult>(s.Controller, std::move(create));
        UNIT_ASSERT_C(created->Get()->Record.GetStatus() == NKikimrReplication::TEvCreateReplicationResult::SUCCESS,
            created->Get()->Record.ShortDebugString());
        s.ExpectLagCounters(TDuration::Seconds(5), 1);

        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(s.Edges[0]);
        const auto otherWorker = TWorkerId::Parse(run->Get()->Record.GetWorker());
        UNIT_ASSERT(otherWorker.ReplicationId() != s.Worker.ReplicationId());
        const auto report = [&](TDuration lag) {
            runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
                new TEvService::TEvWorkerStatus(otherWorker, lag)));
            s.Sync();
        };
        report(TDuration::Seconds(2));
        s.ExpectLagCounters(TDuration::Seconds(5), 0);
        report(TDuration::Seconds(9));
        s.ReportLag(0, TDuration::Seconds(1));
        s.ExpectLagCounters(TDuration::Seconds(9), 0);
        report(TDuration::Seconds(1)); // Equal lag values still represent two replications.
        s.ExpectLagCounters(TDuration::Seconds(1), 0);

        runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(otherWorker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
        s.Sync();
        s.ExpectLagCounters(TDuration::Seconds(1), 1);
        s.ProcessQueues();
        const auto restarted = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(s.Edges[0]);
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(restarted->Get()->Record.GetWorker()), otherWorker);
        report(TDuration::Seconds(9));
        s.ExpectLagCounters(TDuration::Seconds(9), 0);

        auto drop = MakeHolder<TEvController::TEvDropReplication>();
        otherPath.ToProto(drop->Record.MutablePathId());
        s.Env.SendAsync(s.Controller, std::move(drop));
        s.Sync();
        s.ProcessQueues();
        const auto stop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(s.Edges[0]);
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), otherWorker);
        s.ExpectLagCounters(TDuration::Seconds(1), 1);
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(otherWorker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
        const auto dropped = runtime.GrabEdgeEvent<TEvController::TEvDropReplicationResult>(s.Env.GetSender());
        UNIT_ASSERT_C(dropped->Get()->Record.GetStatus() == NKikimrReplication::TEvDropReplicationResult::SUCCESS,
            dropped->Get()->Record.ShortDebugString());
        s.ExpectLagCounters(TDuration::Seconds(1), 0);

        auto dropLast = MakeHolder<TEvController::TEvDropReplication>();
        s.PathId.ToProto(dropLast->Record.MutablePathId());
        s.Env.SendAsync(s.Controller, std::move(dropLast));
        s.ProcessQueues();
        s.ExpectStop(0);
        s.ExpectLagCounters(TDuration::Zero(), 1);
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED)));
        const auto lastDropped = runtime.GrabEdgeEvent<TEvController::TEvDropReplicationResult>(s.Env.GetSender());
        UNIT_ASSERT_C(lastDropped->Get()->Record.GetStatus() == NKikimrReplication::TEvDropReplicationResult::SUCCESS,
            lastDropped->Get()->Record.ShortDebugString());
        s.ExpectLagCounters(TDuration::Zero(), 0);
    }

    Y_UNIT_TEST(LagCountersUnavailableAfterControllerRestart) {
        TSetup s(1);
        s.ReportLag(0, TDuration::Seconds(5));
        s.ExpectLag(TDuration::Seconds(5));
        s.Reboot();
        s.ExpectLag(std::nullopt);
        s.Status(0);
        s.ExpectLag(std::nullopt);
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
        s.ExpectLagCounters(TDuration::Zero(), 1);
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
        ui32 duplicateRuns = 0;
        ui32 stopsToNewOwner = 0;
        auto observeRun = runtime.AddObserver<TEvService::TEvRunWorker>(
            [&](TEvService::TEvRunWorker::TPtr&) { ++duplicateRuns; });
        auto observeStop = runtime.AddObserver<TEvService::TEvStopWorker>(
            [&](TEvService::TEvStopWorker::TPtr& ev) {
                if (ev->GetRecipientRewrite().NodeId() == s.Edges[1].NodeId()) {
                    ++stopsToNewOwner;
                }
            });
        runtime.Send(new IEventHandle(s.Controller, s.Edges[0],
            new TEvService::TEvWorkerStatus(s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
                NKikimrReplication::TEvWorkerStatus::REASON_ERROR, "Source authentication failed")));
        s.Sync();
        s.ProcessQueues();
        s.ExpectLag(TDuration::Seconds(2));
        s.ReportLag(1, TDuration::Seconds(3));
        s.ExpectLag(TDuration::Seconds(3));
        UNIT_ASSERT_VALUES_EQUAL(duplicateRuns, 0);
        UNIT_ASSERT_VALUES_EQUAL(stopsToNewOwner, 0);
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

        s.ReportLag(0, TDuration::Seconds(5));
        s.ExpectLagCounters(TDuration::Seconds(5), 0);
        s.AlterState("Paused");
        s.ProcessQueues();
        s.ExpectStop(0);
        s.ExpectLagCounters(TDuration::Zero(), 1);

        // The node restarts: the acknowledgement is lost, the new process has no workers.
        s.Disconnect(0);
        s.Env.SendAsync(s.Controller, new TEvPrivate::TEvRestoreSession(nodeId));
        s.Sync();
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvHandshake>(s.Edges[0]));
        s.Status(0, false);
        s.ProcessQueues();
        s.ExpectLagCounters(TDuration::Zero(), 1);

        s.AlterState("StandBy");
        for (ui32 i = 0; i < 50; ++i) {
            s.ProcessQueues();
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
        }

        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(s.Edges[0], TDuration::Seconds(10));
        UNIT_ASSERT_C(run, "worker does not start after resume");
        s.ExpectLagCounters(TDuration::Zero(), 1);
        s.ReportLag(0, TDuration::Seconds(1));
        s.ExpectLagCounters(TDuration::Seconds(1), 0);
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

    Y_UNIT_TEST(MonitoringMemoryAndPersistedTables) {
        TSetup s;
        const auto memory = s.Monitoring();
        for (TStringBuf expected : {"Replication controller", "In memory", "Persisted tables",
                "/Root/replica", "Workers", "Sessions", "Pipeline", "Schema barriers", "Index builds"}) {
            UNIT_ASSERT_C(memory.Contains(expected), expected);
        }
        for (const TString table : {"Replications", "Targets", "SrcStreams", "Workers", "TxIds",
                "SysParams", "SchemaBarrierWorkers", "IndexBuildWorkers"}) {
            const auto storage = s.Monitoring({{"view", "storage"}, {"table", table}});
            UNIT_ASSERT_C(storage.Contains("<h3>" + table + "</h3>"), storage);
            UNIT_ASSERT_C(storage.Contains("Read-only snapshot"), storage);
            UNIT_ASSERT_C(!storage.Contains("Cannot decode"), storage);
        }
        const auto storedTargets = s.Monitoring({{"view", "storage"}, {"table", "Targets"}});
        UNIT_ASSERT_C(storedTargets.Contains("/Root/replica"), storedTargets);

        const auto filtered = s.Monitoring({{"rid", ToString(Max<ui64>())}});
        UNIT_ASSERT_C(filtered.Contains("is not present in memory"), filtered);
        UNIT_ASSERT_C(!filtered.Contains("/Root/replica"), filtered);
        const auto secondPage = s.Monitoring({{"page", "1"}});
        UNIT_ASSERT_C(secondPage.Contains("Previous page"), secondPage);
        UNIT_ASSERT_C(secondPage.Contains("No entries on this page"), secondPage);
        UNIT_ASSERT_C(!secondPage.Contains("/Root/replica"), secondPage);
    }

    Y_UNIT_TEST(MonitoringEscapesErrorsAndRejectsInvalidParameters) {
        TSetup s;
        const TString error = "source <script>alert('error')</script> & destination";
        s.Env.GetRuntime().Send(new IEventHandle(s.Controller, s.Edges[0], new TEvService::TEvWorkerStatus(
            s.Worker, NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
            NKikimrReplication::TEvWorkerStatus::REASON_ERROR, error)));
        s.Sync();

        for (const auto& parameters : TVector<TVector<std::pair<TString, TString>>>{
                {}, {{"view", "storage"}, {"table", "Replications"}}, {{"view", "storage"}, {"table", "Targets"}}}) {
            const auto page = s.Monitoring(parameters);
            UNIT_ASSERT_C(!page.Contains(error), page);
            UNIT_ASSERT_C(page.Contains("&lt;script&gt;alert(&#39;error&#39;)&lt;/script&gt; &amp; destination"), page);
        }
        const auto invalidId = s.Monitoring({{"rid", "<script>"}});
        UNIT_ASSERT_C(invalidId.Contains("Invalid replication ID"), invalidId);
        const auto invalidPage = s.Monitoring({{"page", "-1"}});
        UNIT_ASSERT_C(invalidPage.Contains("Invalid page number"), invalidPage);
        const auto invalidTable = s.Monitoring({{"view", "storage"}, {"table", "<script>untrusted-table</script>"}});
        UNIT_ASSERT_C(invalidTable.Contains("Unknown persisted table"), invalidTable);
        UNIT_ASSERT_C(!invalidTable.Contains("untrusted-table"), invalidTable);
        for (const TString cursor : {"1", "1,2,3,4", "-1,2,3", "1,2,<script>"}) {
            const auto invalidCursor = s.Monitoring({{"view", "storage"}, {"table", "Workers"}, {"from", cursor}});
            UNIT_ASSERT_C(invalidCursor.Contains("Invalid table cursor"), invalidCursor);
        }
    }

    Y_UNIT_TEST(MonitoringReadsStorageIndependentlyAndPaginates) {
        TSetup s;
        NKikimrMiniKQL::TResult result;
        const auto query = Sprintf(R"((
            (let key '('('ReplicationId (Uint64 '%lu)) '('Id (Uint64 '%lu))))
            (return (AsList (UpdateRow 'Targets key '('('DstPath (Utf8 '/Root/persisted-only))))))
        ))", s.Worker.ReplicationId(), s.Worker.TargetId());
        UNIT_ASSERT_VALUES_EQUAL(LocalQuery(s.Env.GetRuntime(), s.ControllerId, query, result), NKikimrProto::OK);
        UNIT_ASSERT(s.Monitoring().Contains("/Root/replica"));
        UNIT_ASSERT(!s.Monitoring().Contains("/Root/persisted-only"));
        const auto targets = s.Monitoring({{"view", "storage"}, {"table", "Targets"}});
        UNIT_ASSERT_C(targets.Contains("/Root/persisted-only"), targets);

        TStringBuilder updates;
        updates << "((return (AsList ";
        for (ui32 id = 100; id <= 200; ++id) {
            updates << "(UpdateRow 'SysParams '('('Id (Uint32 '" << id << "))) "
                << "'('('IntValue (Uint64 '" << id << ")) "
                << "'('TextValue (Utf8 'monitoring-secret)) '('BinaryValue (String 'monitoring-secret)))) ";
        }
        updates << ")))";
        UNIT_ASSERT_VALUES_EQUAL(LocalQuery(s.Env.GetRuntime(), s.ControllerId, updates, result), NKikimrProto::OK);
        const auto first = s.Monitoring({{"view", "storage"}, {"table", "SysParams"}});
        UNIT_ASSERT_C(first.Contains("Next page"), first);
        UNIT_ASSERT_C(first.Contains("from=199"), first);
        UNIT_ASSERT_C(!first.Contains("<td>200</td>"), first);
        UNIT_ASSERT_C(!first.Contains("monitoring-secret"), first);
        const auto next = s.Monitoring({{"view", "storage"}, {"table", "SysParams"}, {"from", "199"}});
        UNIT_ASSERT_C(next.Contains("<td>199</td>"), next);
        UNIT_ASSERT_C(next.Contains("<td>200</td>"), next);
        UNIT_ASSERT_C(!next.Contains("Next page"), next);
        UNIT_ASSERT_C(!next.Contains("monitoring-secret"), next);
    }
}

}
