#include "controller_impl.h"
#include "target_table.h"
#include <ydb/core/base/hive.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/printf.h>

namespace NKikimr::NReplication::NController {
using namespace NTestHelpers;
using TProgress = Ydb::Replication::ReplicationApplyProgress;

// A gate runs in the real actor system, so tests can delay an exact protocol
// operation without relying on timing or test-runtime delivery observers.
class TEventGate: public TActorBootstrapped<TEventGate> {
public:
    TEventGate(TActorId destination, TActorId edge, std::function<bool(IEventHandle&)> predicate)
        : Destination(destination), Edge(edge), Predicate(std::move(predicate)) {}
    void Bootstrap() { Become(&TThis::StateWork); }
    STATEFN(StateWork) {
        if (ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
            Holding = false;
            for (auto& held : Held) {
                Send(held->Forward(Destination));
            }
            Held.clear();
        } else if (Holding && Predicate(*ev)) {
            Held.emplace_back(ev.Release());
            Send(Edge, new TEvents::TEvWakeup());
        } else {
            Send(ev->Forward(Destination));
        }
    }
private:
    const TActorId Destination;
    const TActorId Edge;
    const std::function<bool(IEventHandle&)> Predicate;
    bool Holding = true;
    TVector<THolder<IEventHandle>> Held;
};

class TDescribeReplyCatcher: public TActorBootstrapped<TDescribeReplyCatcher> {
public:
    explicit TDescribeReplyCatcher(NThreading::TPromise<NKikimrReplication::TEvDescribeReplicationResult> promise)
        : Promise(std::move(promise)) {}
    void Bootstrap() { Become(&TThis::StateWork); }
    STATEFN(StateWork) {
        if (ev->GetTypeRewrite() == TEvController::TEvDescribeReplicationResult::EventType) {
            Promise.SetValue(ev->Get<TEvController::TEvDescribeReplicationResult>()->Record);
            PassAway();
        }
    }
private:
    NThreading::TPromise<NKikimrReplication::TEvDescribeReplicationResult> Promise;
};

Y_UNIT_TEST_SUITE(ReplicationApplyProgress) {
    struct TApplyEnv: TEnv<> {
        ui64 TabletId = 0;
        NKikimrReplication::TWorkerIdentity Worker;
        TVector<NKikimrReplication::TWorkerIdentity> Workers;
        TString TopicPath;
        TActorId Controller;

        explicit TApplyEnv(ui32 tables = 1) {
            GetRuntime().RegisterService(MakeReplicationServiceId(GetRuntime().GetNodeId(0)),
                GetRuntime().Register(CreateReplicationMockService(GetSender())));
            TString targets;
            for (ui32 i = 0; i < tables; ++i) {
                const TString name = i ? TStringBuilder() << "table" << i : TString("table");
                if (i) { targets += ", "; }
                targets += TStringBuilder() << "`/Root/" << name << "` AS `/Root/replica" << i << "`";
                CreateTable("/Root", *MakeTableDescription(TTestTableDescription{
                .Name = name, .KeyColumns = {"key"},
                .Columns = {{.Name = "key", .Type = "Uint32"}, {.Name = "value", .Type = "Utf8"}},
                .ReplicationConfig = Nothing(),
            }));
            }
            NYdb::NTable::TTableClient client(GetDriver());
            auto session = client.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(Sprintf(R"(
                CREATE ASYNC REPLICATION `replication` FOR %s
                WITH (CONNECTION_STRING = "grpc://%s/?database=/Root",
                    CONSISTENCY_LEVEL = "GLOBAL", COMMIT_INTERVAL = Interval("PT10S"));
            )", targets.c_str(), GetEndpoint().c_str())).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            TabletId = GetDescription("/Root/replication").GetPathDescription().GetReplicationDescription().GetControllerId();
            auto handshake = GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(GetSender());
            Controller = handshake->Sender;
            SendAsync(Controller, new TEvService::TEvStatus());
            for (ui32 i = 0; i < tables; ++i) {
                auto run = GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(GetSender());
                Workers.push_back(run->Get()->Record.GetWorker());
                TopicPath = run->Get()->Record.GetCommand().GetRemoteTopicReader().GetTopicPath();
            }
            Worker = Workers.front();
        }

        NKikimrReplication::TEvDescribeReplicationResult Describe(std::optional<TRowVersion> boundary = TRowVersion(10000, 0), bool stats = false) {
            auto request = new TEvController::TEvDescribeReplication();
            GetPathId("/Root/replication").ToProto(request->Record.MutablePathId());
            request->Record.SetIncludeStats(stats);
            if (boundary) {
                request->Record.MutableRequestedAppliedVersion()->set_step(boundary->Step);
                request->Record.MutableRequestedAppliedVersion()->set_tx_id(boundary->TxId);
            }
            return Send<TEvController::TEvDescribeReplicationResult>(TabletId, request)->Get()->Record;
        }

        void Heartbeat(TRowVersion version, ui32 worker = 0) {
            auto event = new TEvService::TEvHeartbeat(version);
            event->Record.MutableWorker()->CopyFrom(Workers.at(worker));
            SendAsync(Controller, event);
        }

        TProgress WaitState(TProgress::State state, TRowVersion boundary = TRowVersion(10000, 0)) {
            const auto deadline = TInstant::Now() + TDuration::Seconds(30);
            TProgress progress;
            do {
                progress = Describe(boundary).GetApplyProgress();
                if (progress.state() == state) {
                    return progress;
                }
                Sleep(TDuration::MilliSeconds(10));
            } while (TInstant::Now() < deadline);
            UNIT_FAIL(progress.DebugString());
            return progress;
        }
    };

    Y_UNIT_TEST(EnumerationWaitsForWorkerRegistration) {
        NKikimrReplication::TReplicationConfig config;
        TReplication replication(1, TPathId(1, 1), config, "/Root");
        TTargetTable target(&replication, 0,
            std::make_shared<TTargetTable::TTableConfig>("/Root/source", "/Root/replica"));
        target.SetDstState(TReplication::EDstState::Ready);
        target.SetStreamState(TReplication::EStreamState::Ready);
        target.AddWorker(0);

        // CompleteWorkerSet may finish while another RunWorker transaction is
        // waiting for a page fault. A partial registered set is not a quorum.
        UNIT_ASSERT(target.CompleteWorkerSet(TActorId(), THashSet<ui64>{0, 1}, false));
        UNIT_ASSERT(!target.IsWorkerSetEnumerated());
        UNIT_ASSERT(!target.IsWorkerSetComplete());
        target.AddWorker(1);
        UNIT_ASSERT(target.IsWorkerSetEnumerated());
        UNIT_ASSERT(target.IsWorkerSetComplete());

        // Later topology changes invalidate APPLIED, while ordinary replication
        // continues to use its existing dynamic worker/heartbeat quorum.
        target.AddWorker(2);
        UNIT_ASSERT(target.IsWorkerSetEnumerated());
        UNIT_ASSERT(!target.IsWorkerSetComplete());
    }

    Y_UNIT_TEST(EmptyIntervalNeedsEveryHeartbeatAndNoWriteTransaction) {
        TApplyEnv env;
        UNIT_ASSERT(!env.Describe({}).HasApplyProgress());
        env.WaitState(TProgress::WAITING_HEARTBEATS);
        env.Heartbeat(TRowVersion(9999, Max<ui64>()));
        env.WaitState(TProgress::WAITING_HEARTBEATS);
        env.Heartbeat(TRowVersion(10000, 0));
        const auto progress = env.WaitState(TProgress::APPLIED);
        UNIT_ASSERT(progress.worker_set_complete());
        UNIT_ASSERT_VALUES_EQUAL(progress.expected_workers(), 1);
        UNIT_ASSERT_VALUES_EQUAL(progress.workers_at_boundary(), 1);
        UNIT_ASSERT_VALUES_EQUAL(progress.assigned_transactions(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ui32(env.Describe(TRowVersion(10000, 0), true).GetApplyProgress().state()), ui32(TProgress::APPLIED));
        UNIT_ASSERT_VALUES_EQUAL(ui32(env.Describe(TRowVersion(10001, 0)).GetApplyProgress().state()), ui32(TProgress::BLOCKED));
        UNIT_ASSERT_VALUES_EQUAL(ui32(env.Describe(TRowVersion(10000, 1)).GetApplyProgress().state()), ui32(TProgress::BLOCKED));
        UNIT_ASSERT_VALUES_EQUAL(ui32(env.Describe(TRowVersion(0, 0)).GetApplyProgress().state()), ui32(TProgress::BLOCKED));

        NYdb::NReplication::TReplicationClient client(env.GetDriver());
        auto result = client.DescribeReplication("/Root/replication", NYdb::NReplication::TDescribeReplicationSettings()
            .RequestedAppliedVersion(NYdb::NReplication::TSourceVersion{10000, 0}).IncludeStats(true)).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(result.GetApplyProgress());
        UNIT_ASSERT_VALUES_EQUAL(result.GetApplyProgress()->State, NYdb::NReplication::TReplicationApplyProgress::EState::Applied);
    }

    Y_UNIT_TEST(WaitsForCommitCompletion) {
        TApplyEnv env;
        env.WaitState(TProgress::WAITING_HEARTBEATS);
        // Save the path before intercepting TxProxy. Only the controller's commit
        // protocol is mocked here; source discovery and worker registration are real.
        const auto pathId = env.GetPathId("/Root/replication");
        auto ids = env.Send<TEvService::TEvTxIdResult>(env.TabletId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(1, 0)}));
        const auto writeTxId = ids->Get()->Record.GetVersionTxIds(0).GetTxId();
        const auto proxy = env.GetRuntime().GetLocalServiceId(MakeTxProxyID());
        env.GetRuntime().RegisterService(MakeTxProxyID(), env.GetSender());
        env.Heartbeat(TRowVersion(10000, 0));
        auto commit = env.GetRuntime().GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(commit->Get()->Record.GetTransaction().GetCommitWrites().GetWriteTxId(), writeTxId);
        const auto describe = [&] {
            auto request = new TEvController::TEvDescribeReplication();
            pathId.ToProto(request->Record.MutablePathId());
            request->Record.MutableRequestedAppliedVersion()->set_step(10000);
            return env.Send<TEvController::TEvDescribeReplicationResult>(env.TabletId, request)->Get()->Record.GetApplyProgress();
        };
        auto progress = describe();
        UNIT_ASSERT_VALUES_EQUAL(ui32(progress.state()), ui32(TProgress::WAITING_COMMITS));
        UNIT_ASSERT_VALUES_EQUAL(progress.assigned_transactions(), 1);
        UNIT_ASSERT(progress.commit_in_progress());
        auto status = new TEvTxUserProxy::TEvProposeTransactionStatus();
        status->Record.SetStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete);
        env.GetRuntime().Send(new IEventHandle(env.Controller, env.GetSender(), status, 0, writeTxId));
        // Description is a tablet transaction; its Complete waits for TxIds deletion.
        progress = describe();
        UNIT_ASSERT_VALUES_EQUAL(ui32(progress.state()), ui32(TProgress::APPLIED));
        env.GetRuntime().RegisterService(MakeTxProxyID(), proxy);
    }

    void CheckPendingCommitRestart(bool loseDeletion) {
        TApplyEnv env;
        env.WaitState(TProgress::WAITING_HEARTBEATS);
        auto& runtime = env.GetRuntime();
        const auto pathId = env.GetPathId("/Root/replication");
        auto ids = env.Send<TEvService::TEvTxIdResult>(env.TabletId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(1, 0)}));
        const auto writeTxId = ids->Get()->Record.GetVersionTxIds(0).GetTxId();
        const auto proxy = runtime.GetLocalServiceId(MakeTxProxyID());
        const auto commitGate = runtime.Register(new TEventGate(proxy, env.GetSender(), [](IEventHandle& ev) {
            return ev.GetTypeRewrite() == TEvTxUserProxy::TEvProposeTransaction::EventType
                && ev.Get<TEvTxUserProxy::TEvProposeTransaction>()->Record.GetTransaction().HasCommitWrites();
        }));
        runtime.RegisterService(MakeTxProxyID(), commitGate);
        const auto describe = [&] {
            auto request = new TEvController::TEvDescribeReplication();
            pathId.ToProto(request->Record.MutablePathId());
            request->Record.MutableRequestedAppliedVersion()->set_step(10000);
            return env.Send<TEvController::TEvDescribeReplicationResult>(env.TabletId, request)->Get()->Record.GetApplyProgress();
        };
        const auto complete = [&] {
            auto status = new TEvTxUserProxy::TEvProposeTransactionStatus();
            status->Record.SetStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete);
            runtime.Send(new IEventHandle(env.Controller, env.GetSender(), status, 0, writeTxId));
        };
        env.Heartbeat(TRowVersion(10000, 0));
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(ui32(describe().state()), ui32(TProgress::WAITING_COMMITS));
        if (loseDeletion) {
            auto info = env.Send<TEvHive::TEvGetTabletStorageInfoResult>(Tests::Hive,
                new TEvHive::TEvGetTabletStorageInfo(env.TabletId));
            UNIT_ASSERT_VALUES_EQUAL(info->Get()->Record.GetStatus(), NKikimrProto::OK);
            const auto& channel = info->Get()->Record.GetInfo().GetChannels(0);
            const auto service = MakeBlobStorageProxyID(channel.GetHistory(channel.HistorySize() - 1).GetGroupID());
            const auto original = runtime.GetLocalServiceId(service);
            const auto tabletId = env.TabletId;
            const auto gate = runtime.Register(new TEventGate(original, env.GetSender(), [tabletId](IEventHandle& ev) {
                return ev.GetTypeRewrite() == TEvBlobStorage::TEvPut::EventType
                    && ev.Get<TEvBlobStorage::TEvPut>()->Id.TabletID() == tabletId;
            }));
            runtime.RegisterService(service, gate);
            complete();
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
            auto request = new TEvController::TEvDescribeReplication();
            pathId.ToProto(request->Record.MutablePathId());
            request->Record.MutableRequestedAppliedVersion()->set_step(10000);
            auto promise = NThreading::NewPromise<NKikimrReplication::TEvDescribeReplicationResult>();
            auto future = promise.GetFuture();
            runtime.Send(new IEventHandle(env.Controller, runtime.Register(new TDescribeReplyCatcher(promise)), request));
            UNIT_ASSERT_C(!future.Wait(TDuration::MilliSeconds(100)), "APPLIED replied before TxIds deletion was durable");
            // Simulate losing the old generation's unpersisted redo on crash.
            // Keep its Put held forever; the new generation uses the real proxy.
            runtime.RegisterService(service, original);
        }
        env.SendAsync(env.TabletId, new TEvents::TEvPoisonPill());
        auto handshake = runtime.GrabEdgeEvent<TEvService::TEvHandshake>(env.GetSender());
        UNIT_ASSERT(handshake->Sender != env.Controller);
        env.Controller = handshake->Sender;
        env.SendAsync(env.Controller, new TEvService::TEvStatus());
        // Fresh enumeration must trigger retry of the persisted assigned TxId,
        // using the persisted heartbeat and no new writer heartbeat.
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        TProgress progress;
        do {
            progress = describe();
            UNIT_ASSERT_C(progress.state() != TProgress::APPLIED,
                "APPLIED before persisted assigned transaction was recommitted");
            if (progress.state() == TProgress::WAITING_COMMITS && progress.commit_in_progress()) {
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        } while (TInstant::Now() < deadline);
        UNIT_ASSERT_VALUES_EQUAL_C(ui32(progress.state()), ui32(TProgress::WAITING_COMMITS), progress.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(progress.assigned_transactions(), 1);
        UNIT_ASSERT(progress.commit_in_progress());
        complete();
        env.WaitState(TProgress::APPLIED);
        runtime.RegisterService(MakeTxProxyID(), proxy);
    }

    Y_UNIT_TEST(PendingCommitSurvivesRestart) { CheckPendingCommitRestart(false); }
    Y_UNIT_TEST(UnpersistedCommitDeletionSurvivesRestart) { CheckPendingCommitRestart(true); }

    Y_UNIT_TEST(WaitsForAllTables) {
        TApplyEnv env(2);
        env.WaitState(TProgress::WAITING_HEARTBEATS);
        env.Heartbeat(TRowVersion(10000, 0), 0);
        auto progress = env.WaitState(TProgress::WAITING_HEARTBEATS);
        UNIT_ASSERT_VALUES_EQUAL(progress.expected_workers(), 2);
        env.Heartbeat(TRowVersion(10000, 0), 1);
        progress = env.WaitState(TProgress::APPLIED);
        UNIT_ASSERT_VALUES_EQUAL(progress.workers_at_boundary(), 2);
    }

    Y_UNIT_TEST(RevalidatesMembershipAfterRestart) {
        TApplyEnv env;
        env.Heartbeat(TRowVersion(10000, 0));
        env.WaitState(TProgress::APPLIED);
        auto& runtime = env.GetRuntime();
        const auto service = MakeSchemeCacheID();
        const auto original = runtime.GetLocalServiceId(service);
        UNIT_ASSERT(original);
        const auto path = SplitPath(env.TopicPath);
        const auto gate = runtime.Register(new TEventGate(original, env.GetSender(), [path](IEventHandle& ev) {
            if (ev.GetTypeRewrite() != TEvTxProxySchemeCache::TEvNavigateKeySet::EventType) { return false; }
            for (const auto& entry : ev.Get<TEvTxProxySchemeCache::TEvNavigateKeySet>()->Request->ResultSet) {
                if (entry.Path == path) { return true; }
            }
            return false;
        }));
        runtime.RegisterService(service, gate);
        env.SendAsync(env.TabletId, new TEvents::TEvPoisonPill());
        auto handshake = runtime.GrabEdgeEvent<TEvService::TEvHandshake>(env.GetSender());
        UNIT_ASSERT(handshake->Sender != env.Controller);
        env.Controller = handshake->Sender;
        env.SendAsync(env.Controller, new TEvService::TEvStatus());
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        auto progress = env.Describe().GetApplyProgress();
        UNIT_ASSERT_VALUES_EQUAL(ui32(progress.state()), ui32(TProgress::INITIALIZING));
        UNIT_ASSERT(!progress.worker_set_complete());
        runtime.RegisterService(service, original);
        env.SendAsync(gate, new TEvents::TEvWakeup());
        // The persisted heartbeat is sufficient only after fresh enumeration.
        env.WaitState(TProgress::APPLIED);
    }

    Y_UNIT_TEST(WaitsForHeartbeatDurability) {
        TApplyEnv env;
        env.WaitState(TProgress::WAITING_HEARTBEATS);
        auto& runtime = env.GetRuntime();
        const auto pathId = env.GetPathId("/Root/replication");
        auto info = env.Send<TEvHive::TEvGetTabletStorageInfoResult>(Tests::Hive,
            new TEvHive::TEvGetTabletStorageInfo(env.TabletId));
        UNIT_ASSERT_VALUES_EQUAL(info->Get()->Record.GetStatus(), NKikimrProto::OK);
        const auto& channel = info->Get()->Record.GetInfo().GetChannels(0);
        const auto group = channel.GetHistory(channel.HistorySize() - 1).GetGroupID();
        const auto service = MakeBlobStorageProxyID(group);
        const auto original = runtime.GetLocalServiceId(service);
        UNIT_ASSERT(original);
        const auto tabletId = env.TabletId;
        const auto gate = runtime.Register(new TEventGate(original, env.GetSender(), [tabletId](IEventHandle& ev) {
            return ev.GetTypeRewrite() == TEvBlobStorage::TEvPut::EventType
                && ev.Get<TEvBlobStorage::TEvPut>()->Id.TabletID() == tabletId;
        }));
        runtime.RegisterService(service, gate);
        env.Heartbeat(TRowVersion(10000, 0));
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        auto request = new TEvController::TEvDescribeReplication();
        pathId.ToProto(request->Record.MutablePathId());
        request->Record.MutableRequestedAppliedVersion()->set_step(10000);
        auto promise = NThreading::NewPromise<NKikimrReplication::TEvDescribeReplicationResult>();
        auto future = promise.GetFuture();
        const auto catcher = runtime.Register(new TDescribeReplyCatcher(promise));
        runtime.Send(new IEventHandle(env.Controller, catcher, request));
        UNIT_ASSERT(!future.Wait(TDuration::MilliSeconds(100)));
        runtime.RegisterService(service, original);
        env.SendAsync(gate, new TEvents::TEvWakeup());
        UNIT_ASSERT(future.Wait(TDuration::Seconds(30)));
        UNIT_ASSERT_VALUES_EQUAL(ui32(future.GetValueSync().GetApplyProgress().state()), ui32(TProgress::APPLIED));
    }

    Y_UNIT_TEST(TopologyChangeBlocksApplied) {
        TApplyEnv env;
        env.Heartbeat(TRowVersion(10000, 0));
        env.WaitState(TProgress::APPLIED);
        auto event = new TEvService::TEvWorkerDataEnd();
        event->Record.MutableWorker()->CopyFrom(env.Worker);
        env.SendAsync(env.Controller, event);
        env.WaitState(TProgress::BLOCKED);
    }

    void CheckAppliedData(bool directoryScope) {
        // Exercise real reader/writer actors and CommitWrites. The source is
        // quiescent after the two writes; Frozen-heartbeat behavior is tested
        // separately in DataShard tests, so this receiver patch is independent.
        TEnv<> env;
        env.GetRuntime().RegisterService(MakeReplicationServiceId(env.GetRuntime().GetNodeId(0)),
            env.GetRuntime().Register(CreateReplicationService()));
        const TString sourceDir = directoryScope ? "/Root/source" : "/Root";
        const TString destinationDir = "/Root/destination";
        if (directoryScope) {
            env.MkDir("/Root", "source");
            env.MkDir("/Root", "destination");
        }
        const TString sourcePath = sourceDir + "/table";
        const TString destinationPath = directoryScope ? destinationDir + "/table" : TString("/Root/replica");
        env.CreateTable(sourceDir, *MakeTableDescription(TTestTableDescription{
            .Name = "table", .KeyColumns = {"key"},
            .Columns = {{.Name = "key", .Type = "Uint32"}, {.Name = "value", .Type = "Utf8"}},
            .ReplicationConfig = Nothing(),
        }));
        NYdb::NTable::TTableClient tables(env.GetDriver());
        auto session = tables.CreateSession().GetValueSync().GetSession();
        const auto execute = [&](const TString& query) {
            auto result = session.ExecuteDataQuery(query,
                NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result;
        };
        execute(Sprintf("UPSERT INTO `%s` (key, value) VALUES (1, 'before');", sourcePath.c_str()));
        auto created = session.ExecuteSchemeQuery(Sprintf(R"(
            CREATE ASYNC REPLICATION `replication` FOR `%s` AS `%s`
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root",
                CONSISTENCY_LEVEL = "GLOBAL", COMMIT_INTERVAL = Interval("PT1S"));
        )", (directoryScope ? sourceDir : sourcePath).c_str(),
            (directoryScope ? destinationDir : destinationPath).c_str(), env.GetEndpoint().c_str())).GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        execute(Sprintf("UPSERT INTO `%s` (key, value) VALUES (2, 'after');", sourcePath.c_str()));

        using TLongTxEvents = NLongTxService::TEvLongTxService;
        auto snapshot = env.Send<TLongTxEvents::TEvAcquireReadSnapshotResult>(
            NLongTxService::MakeLongTxServiceID(env.GetRuntime().GetNodeId(0)),
            new TLongTxEvents::TEvAcquireReadSnapshot(env.GetDatabase()));
        UNIT_ASSERT_VALUES_EQUAL(snapshot->Get()->Status, Ydb::StatusIds::SUCCESS);
        const NYdb::NReplication::TSourceVersion boundary{(snapshot->Get()->Snapshot.Step / 1000 + 1) * 1000, 0};
        NYdb::NReplication::TReplicationClient replication(env.GetDriver());
        const auto deadline = TInstant::Now() + TDuration::Seconds(60);
        bool applied = false;
        TString lastProgress;
        do {
            auto result = replication.DescribeReplication("/Root/replication",
                NYdb::NReplication::TDescribeReplicationSettings().RequestedAppliedVersion(boundary)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetApplyProgress());
            lastProgress = TStringBuilder() << "state=" << ui32(result.GetApplyProgress()->State)
                << " complete=" << result.GetApplyProgress()->WorkerSetComplete
                << " workers=" << result.GetApplyProgress()->ExpectedWorkers
                << " atBoundary=" << result.GetApplyProgress()->WorkersAtBoundary
                << " assigned=" << result.GetApplyProgress()->AssignedTransactions;
            applied = result.GetApplyProgress()->State == NYdb::NReplication::TReplicationApplyProgress::EState::Applied;
            if (applied) { break; }
            UNIT_ASSERT_C(result.GetApplyProgress()->State != NYdb::NReplication::TReplicationApplyProgress::EState::Blocked,
                result.GetApplyProgress()->Issues.ToString());
            Sleep(TDuration::MilliSeconds(20));
        } while (TInstant::Now() < deadline);
        UNIT_ASSERT_C(applied, "Replication did not apply the source snapshot: " << lastProgress);
        auto rows = execute(Sprintf("SELECT key, value FROM `%s` ORDER BY key;", destinationPath.c_str()));
        NYdb::TResultSetParser parser(rows.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("value").GetOptionalUtf8().value(), "before");
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("value").GetOptionalUtf8().value(), "after");
        UNIT_ASSERT(!parser.TryNextRow());
    }

    Y_UNIT_TEST(AppliesInitialScanAndLiveChangesThroughBoundary) { CheckAppliedData(false); }
    Y_UNIT_TEST(DirectoryScopeStillCommits) { CheckAppliedData(true); }

    Y_UNIT_TEST(OldServerResponseDoesNotImplyApplied) {
        Ydb::Replication::DescribeReplicationResult proto;
        proto.mutable_row_consistency();
        proto.mutable_running();
        NYdb::NReplication::TDescribeReplicationResult result(
            NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues()), std::move(proto));
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT(!result.GetApplyProgress());
    }
}
} // namespace NKikimr::NReplication::NController
