#include "controller_impl.h"
#include "dst_schema_changer.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/datetime/base.h>

namespace NKikimr::NReplication::NController {

namespace {

NKikimrReplication::TSchemaChange MakeSchemaChange(
        ui64 step = 100, ui64 txId = 10, ui64 sourceSchemaVersion = 2,
        bool withExtraColumn = true)
{
    NKikimrReplication::TSchemaChange schema;
    schema.MutableVersion()->SetStep(step);
    schema.MutableVersion()->SetTxId(txId);
    schema.SetSourceSchemaVersion(sourceSchemaVersion);

    auto* key = schema.AddColumns();
    key->SetName("key");
    key->SetType("Uint32");

    auto* value = schema.AddColumns();
    value->SetName("value");
    value->SetType("Utf8");

    if (withExtraColumn) {
        auto* extra = schema.AddColumns();
        extra->SetName("extra");
        extra->SetType("Uint64");
    }

    schema.AddPrimaryKeyColumnNames("key");
    return schema;
}

TEvService::TEvSchemaChangeReport* MakeSchemaChangeReport(
        const TWorkerId& id, const NKikimrReplication::TSchemaChange& schema,
        bool applied = false, bool completed = false)
{
    auto* event = new TEvService::TEvSchemaChangeReport();
    id.Serialize(*event->Record.MutableWorker());
    event->Record.MutableSchema()->CopyFrom(schema);
    event->Record.SetApplied(applied);
    event->Record.SetCompleted(completed);
    return event;
}

using TTestEnv = NTestHelpers::TEnv<>;

// TTestEnv uses real actor threads, so runtime observers cannot reliably
// intercept actor-to-actor events. TBlockEvents is observer-based as well;
// the synchronization gates below therefore participate in actor routing.
class TDescribeTopicRequestGate: public TActorBootstrapped<TDescribeTopicRequestGate> {
    static constexpr ui64 RequestBlocked = 1;
    static constexpr ui64 ReleaseRequest = 2;

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
        for (const auto& entry : ev->Get()->Request->ResultSet) {
            if (CanonizePath(JoinPath(entry.Path)) == TopicPath) {
                UNIT_ASSERT(!PendingRequest);
                PendingRequest = std::move(ev);
                Send(Notify, new TEvents::TEvWakeup(RequestBlocked));
                return;
            }
        }

        Send(ev->Forward(SchemeCache));
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Tag, ReleaseRequest);
        UNIT_ASSERT(PendingRequest);
        Send(PendingRequest->Forward(SchemeCache));
    }

public:
    TDescribeTopicRequestGate(const TActorId& schemeCache, const TActorId& notify, TString topicPath)
        : SchemeCache(schemeCache)
        , Notify(notify)
        , TopicPath(CanonizePath(topicPath))
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
            default:
                Send(ev->Forward(SchemeCache));
        }
    }

    static ui64 RequestBlockedTag() {
        return RequestBlocked;
    }

    static ui64 ReleaseRequestTag() {
        return ReleaseRequest;
    }

private:
    const TActorId SchemeCache;
    const TActorId Notify;
    const TString TopicPath;
    TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr PendingRequest;
};

class TCommitWritesRequestGate: public TActorBootstrapped<TCommitWritesRequestGate> {
    static constexpr ui64 RequestBlocked = 3;
    static constexpr ui64 ReleaseRequests = 4;
    static constexpr ui64 Ready = 5;

    void Handle(TEvTxUserProxy::TEvProposeTransaction::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.HasTransaction() && record.GetTransaction().HasCommitWrites()) {
            const auto& commit = record.GetTransaction().GetCommitWrites();
            if (commit.TablesSize() == 1 && commit.GetTables(0).GetTablePath() == TargetPath) {
                PendingRequests.push_back(std::move(ev));
                Send(Notify, new TEvents::TEvWakeup(RequestBlocked), 0, commit.GetWriteTxId());
                return;
            }
        }

        Send(ev->Forward(TxProxy));
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Tag, ReleaseRequests);
        UNIT_ASSERT(!PendingRequests.empty());
        for (auto& request : PendingRequests) {
            Send(request->Forward(TxProxy));
        }
        PendingRequests.clear();
    }

public:
    TCommitWritesRequestGate(const TActorId& txProxy, const TActorId& notify, TString targetPath)
        : TxProxy(txProxy)
        , Notify(notify)
        , TargetPath(std::move(targetPath))
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Send(Notify, new TEvents::TEvWakeup(Ready));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvProposeTransaction, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
            default:
                Send(ev->Forward(TxProxy));
        }
    }

    static ui64 RequestBlockedTag() {
        return RequestBlocked;
    }

    static ui64 ReleaseRequestsTag() {
        return ReleaseRequests;
    }

    static ui64 ReadyTag() {
        return Ready;
    }

private:
    const TActorId TxProxy;
    const TActorId Notify;
    const TString TargetPath;
    TVector<TEvTxUserProxy::TEvProposeTransaction::TPtr> PendingRequests;
};

struct TReplicationTestInfo {
    ui64 ControllerId = 0;
    TPathId PathId;
    NKikimrReplication::TReplicationConfig Config;
    ui32 Generation = 0;
};

void CreateSourceTable(TTestEnv& env, const TString& name) {
    env.CreateTable("/Root", *NTestHelpers::MakeTableDescription(NTestHelpers::TTestTableDescription{
        .Name = name,
        .KeyColumns = {"key"},
        .Columns = {
            {.Name = "key", .Type = "Uint32"},
            {.Name = "value", .Type = "Utf8"},
        },
        .ReplicationConfig = Nothing(),
    }));
}

TReplicationTestInfo StartReplication(
        TTestEnv& env,
        int targetCount = 1,
        const TString& token = "root@builtin",
        bool globalConsistency = false)
{
    for (int i = 1; i <= targetCount; ++i) {
        CreateSourceTable(env, Sprintf("table%i", i));
    }

    const auto service = env.GetRuntime().Register(NTestHelpers::CreateReplicationMockService(env.GetSender()));
    env.GetRuntime().RegisterService(MakeReplicationServiceId(env.GetRuntime().GetNodeId(0)), service);

    TVector<TString> targets(::Reserve(targetCount));
    for (int i = 1; i <= targetCount; ++i) {
        targets.push_back(Sprintf("`/Root/table%i` AS `/Root/replica%i`", i, i));
    }

    TVector<TString> params = {Sprintf(R"(CONNECTION_STRING = "grpc://%s/?database=/Root")", env.GetEndpoint().c_str())};
    if (token) {
        params.push_back(Sprintf(R"(TOKEN = "%s")", token.c_str()));
    }
    if (globalConsistency) {
        params.push_back(R"(CONSISTENCY_LEVEL = "GLOBAL")");
        params.push_back(R"(COMMIT_INTERVAL = Interval("PT10S"))");
    }

    NYdb::NTable::TTableClient client(env.GetDriver(), NYdb::NTable::TClientSettings()
        .DiscoveryEndpoint(env.GetEndpoint())
        .Database(env.GetDatabase()));
    auto session = client.CreateSession().GetValueSync().GetSession();
    const auto status = session.ExecuteSchemeQuery(Sprintf(R"(
        CREATE ASYNC REPLICATION `replication` FOR %s WITH (%s);
    )", JoinSeq(", ", targets).c_str(), JoinSeq(", ", params).c_str())).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), NYdb::EStatus::SUCCESS, status.GetIssues().ToString());

    const auto desc = env.GetDescription("/Root/replication").GetPathDescription().GetReplicationDescription();
    TReplicationTestInfo info;
    info.ControllerId = desc.GetControllerId();
    info.PathId = env.GetPathId("/Root/replication");
    info.Config.CopyFrom(desc.GetConfig());

    const auto handshake = env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(env.GetSender());
    info.Generation = handshake->Get()->Record.GetController().GetGeneration();
    env.SendAsync(info.ControllerId, new TEvService::TEvStatus());
    return info;
}

void SendHeartbeat(TTestEnv& env, ui64 controllerId, const TWorkerId& worker, const TRowVersion& version) {
    auto heartbeat = MakeHolder<TEvService::TEvHeartbeat>();
    worker.Serialize(*heartbeat->Record.MutableWorker());
    version.ToProto(heartbeat->Record.MutableVersion());
    env.SendAsync(controllerId, heartbeat.Release());
}

void AttachWorkers(TTestEnv& env, ui64 controllerId, std::initializer_list<TWorkerId> workers) {
    auto status = MakeHolder<TEvService::TEvStatus>();
    for (const auto& id : workers) {
        id.Serialize(*status->Record.AddWorkers());
    }

    env.SendAsync(controllerId, status.Release());
}

TWorkerId RegisterSecondWorkerAndCompleteSet(TTestEnv& env, ui64 controllerId, const TWorkerId& first) {
    const TWorkerId second(first.ReplicationId(), first.TargetId(), first.WorkerId() + 1);
    auto run = MakeHolder<TEvService::TEvRunWorker>();
    second.Serialize(*run->Record.MutableWorker());
    env.SendAsync(controllerId, run.Release());
    env.SendAsync(controllerId, new TEvPrivate::TEvCompleteWorkerSet(first.ReplicationId(), first.TargetId()));
    AttachWorkers(env, controllerId, {first, second});
    return second;
}

auto DescribeReplication(TTestEnv& env, const TReplicationTestInfo& info) {
    auto request = MakeHolder<TEvController::TEvDescribeReplication>();
    info.PathId.ToProto(request->Record.MutablePathId());
    return env.Send<TEvController::TEvDescribeReplicationResult>(info.ControllerId, std::move(request));
}

void RequestPause(TTestEnv& env, const TReplicationTestInfo& info, ui64 txId) {
    auto request = MakeHolder<TEvController::TEvAlterReplication>();
    info.PathId.ToProto(request->Record.MutablePathId());
    request->Record.MutableConfig()->CopyFrom(info.Config);
    request->Record.MutableConfig()->MutableSrcConnectionParams()->MutableOAuthToken()->SetToken("root@builtin");
    request->Record.MutableSwitchState()->MutablePaused();
    request->Record.MutableOperationId()->SetTxId(txId);

    const auto result = env.Send<TEvController::TEvAlterReplicationResult>(info.ControllerId, std::move(request));
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
}

void RequestConfigUpdate(TTestEnv& env, const TReplicationTestInfo& info, ui64 txId) {
    auto request = MakeHolder<TEvController::TEvAlterReplication>();
    info.PathId.ToProto(request->Record.MutablePathId());
    request->Record.MutableConfig()->CopyFrom(info.Config);
    request->Record.MutableConfig()->MutableSrcConnectionParams()->MutableOAuthToken()->SetToken("root@builtin");
    request->Record.MutableConfig()->MutableMetricsConfig()->SetLevel(
        NKikimrProto::NMetricsConfig::TMetricsConfig::LEVEL_DETAILED);
    request->Record.MutableOperationId()->SetTxId(txId);

    const auto result = env.Send<TEvController::TEvAlterReplicationResult>(info.ControllerId, std::move(request));
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
}

void WaitForPaused(TTestEnv& env, const TReplicationTestInfo& info) {
    for (ui32 attempt = 0; attempt < 50; ++attempt) {
        if (DescribeReplication(env, info)->Get()->Record.GetState().HasPaused()) {
            return;
        }
        Sleep(TDuration::MilliSeconds(100));
    }

    UNIT_FAIL("Replication did not Paused: " << DescribeReplication(env, info)->Get()->Record.GetState());
}

void WaitForColumnCount(TTestEnv& env, const TString& path, ui32 expected) {
    for (ui32 attempt = 0; attempt < 50; ++attempt) {
        if (env.GetDescription(path).GetPathDescription().GetTable().ColumnsSize() == expected) {
            return;
        }
        Sleep(TDuration::MilliSeconds(20));
    }

    UNIT_ASSERT_VALUES_EQUAL(
        env.GetDescription(path).GetPathDescription().GetTable().ColumnsSize(), expected);
}

ui32 RestartController(TTestEnv& env, ui64 controllerId) {
    env.SendAsync(controllerId, new TEvents::TEvPoisonPill());
    const auto handshake = env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(env.GetSender());
    return handshake->Get()->Record.GetController().GetGeneration();
}

struct TSchemaAltererTestEnv {
    TTestActorRuntime Runtime;
    TActorId Parent;
    TActorId Allocator;
    TActorId PipeCache;
    TActorId Alterer;
    TPathId DstPathId = TPathId(1, 1);
    NKikimrReplication::TSchemaChange Schema;

    TSchemaAltererTestEnv(const NKikimrReplication::TSchemaChange& schema, ui64 dstAlterTxId)
        : Schema(schema)
    {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Parent = Runtime.AllocateEdgeActor();
        Allocator = Runtime.AllocateEdgeActor();
        PipeCache = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(MakeTxProxyID(), Allocator);
        Alterer = Runtime.Register(CreateSchemaChangeDstAlterer(Parent, DstPathId.OwnerId,
            1, 1, TReplication::ETargetKind::Table, DstPathId, Schema, dstAlterTxId));

        Runtime.GrabEdgeEvent<TEvTxUserProxy::TEvAllocateTxId>(Allocator);
        NTxProxy::TTxProxyServices services;
        services.LeaderPipeCache = PipeCache;
        Runtime.Send(Alterer, Allocator,
            new TEvTxUserProxy::TEvAllocateTxIdResult(dstAlterTxId + 1, services, {}));
    }

    void ReplyMatchingDescription() {
        const auto request = Runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(PipeCache);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->Ev->Type(),
            NSchemeShard::TEvSchemeShard::TEvDescribeScheme::EventType);

        auto description = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
        description->Record.SetStatus(NKikimrScheme::StatusSuccess);
        auto* table = description->Record.MutablePathDescription()->MutableTable();
        for (const auto& column : Schema.GetColumns()) {
            auto* current = table->AddColumns();
            current->SetName(column.GetName());
            current->SetType(column.GetType());
        }
        for (const auto& key : Schema.GetPrimaryKeyColumnNames()) {
            table->AddKeyColumnNames(key);
        }
        Runtime.Send(Alterer, PipeCache, description.Release());
    }

    void ExpectUnlink() {
        const auto unlink = Runtime.GrabEdgeEvent<TEvPipeCache::TEvUnlink>(PipeCache);
        UNIT_ASSERT_VALUES_EQUAL(unlink->Get()->TabletId, DstPathId.OwnerId);
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(SchemaChangeBarrier) {
    using namespace NTestHelpers;

    Y_UNIT_TEST(RecoveryWaitsForDestinationShardCompletion) {
        constexpr ui64 dstAlterTxId = 100;
        TSchemaAltererTestEnv env(MakeSchemaChange(), dstAlterTxId);

        // This is a replacement actor with the persisted DDL TxId. The
        // published destination description already matches, but the shard's
        // completion remains delayed at SchemeShard.
        env.ReplyMatchingDescription();
        const auto subscription = env.Runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(env.PipeCache);
        UNIT_ASSERT_VALUES_EQUAL(subscription->Get()->Ev->Type(),
            NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion::EventType);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion*>(
            subscription->Get()->Ev.Get())->Record.GetTxId(), dstAlterTxId);

        env.Runtime.Send(env.Alterer, env.PipeCache,
            new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered(dstAlterTxId));
        // Only the completion notification permits another description and
        // the final release. A matching description alone produced no result.
        env.Runtime.Send(env.Alterer, env.PipeCache,
            new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult(dstAlterTxId));
        env.ReplyMatchingDescription();

        const auto result = env.Runtime.GrabEdgeEvent<TEvPrivate::TEvSchemaChangeDstAlterResult>(env.Parent);
        UNIT_ASSERT(result->Get()->IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->DstAlterTxId, dstAlterTxId);
        env.ExpectUnlink();
    }

    Y_UNIT_TEST(PoisonUnlinksDestinationSchemaPipeCache) {
        TSchemaAltererTestEnv env(MakeSchemaChange(), 100);
        env.Runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(env.PipeCache);

        env.Runtime.Send(env.Alterer, env.Parent, new TEvents::TEvPoison());
        env.ExpectUnlink();
    }

    Y_UNIT_TEST(PauseCancelsStrandedCollectingBarrierAfterWorkerError) {
        TEnv env;
        const auto info = StartReplication(env);
        const auto controllerId = info.ControllerId;

        const auto run = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto first = TWorkerId::Parse(run->Get()->Record.GetWorker());
        const auto second = RegisterSecondWorkerAndCompleteSet(env, controllerId, first);

        env.SendAsync(controllerId, MakeSchemaChangeReport(first, MakeSchemaChange()));
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(second,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
            NKikimrReplication::TEvWorkerStatus::REASON_ERROR, "reader failed"));

        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());

        RequestPause(env, info, 1000);
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(first,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(second,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        WaitForPaused(env, info);

        RestartController(env, controllerId);
        env.SendAsync(controllerId, new TEvService::TEvStatus());
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasPaused());
    }

    Y_UNIT_TEST(PauseRecoversUnfinishedAppliedBarrierAfterWorkerError) {
        TEnv env;
        const auto info = StartReplication(env);
        const auto controllerId = info.ControllerId;

        const auto run = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto first = TWorkerId::Parse(run->Get()->Record.GetWorker());
        const auto second = RegisterSecondWorkerAndCompleteSet(env, controllerId, first);

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(first, schema));
        env.SendAsync(controllerId, MakeSchemaChangeReport(second, schema));
        for (ui32 i = 0; i < 2; ++i) {
            env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        }
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1")
            .GetPathDescription().GetTable().ColumnsSize(), 3);

        for (const auto& id : {first, second}) {
            env.SendAsync(controllerId, MakeSchemaChangeReport(id, schema, true));
            env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        }
        env.SendAsync(controllerId, MakeSchemaChangeReport(first, schema, false, true));
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(second,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
            NKikimrReplication::TEvWorkerStatus::REASON_ERROR, "reader failed"));

        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());

        RequestPause(env, info, 1001);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        // The durable Applied barrier is retained. Only its unfinished
        // worker is restarted, so its completion can release the deferred
        // lifecycle change even after the controller restarts.
        const auto stop = env.GetRuntime().GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), second);
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(second,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        RestartController(env, controllerId);
        AttachWorkers(env, controllerId, {first, second});
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
        const auto restored = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(restored->Get()->Record.GetApplied());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(restored->Get()->Record.GetWorker()), second);

        env.SendAsync(controllerId, MakeSchemaChangeReport(second, schema, false, true));
        const auto completed = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(completed->Get()->Record.GetCompleted());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(completed->Get()->Record.GetWorker()), second);

        for (ui32 i = 0; i < 2; ++i) {
            env.GetRuntime().GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        }
        for (const auto& id : {first, second}) {
            env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(id,
                NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        }
        WaitForPaused(env, info);
    }

    Y_UNIT_TEST(PauseRecoversHealthySiblingBarrierAfterErrorRestart) {
        TEnv env;
        const auto info = StartReplication(env, 2);
        const auto controllerId = info.ControllerId;

        const auto firstRun = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto secondRun = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto barrierWorker = TWorkerId::Parse(firstRun->Get()->Record.GetWorker());
        const auto failedWorker = TWorkerId::Parse(secondRun->Get()->Record.GetWorker());
        UNIT_ASSERT_VALUES_UNEQUAL(barrierWorker.TargetId(), failedWorker.TargetId());

        env.SendAsync(controllerId, new TEvPrivate::TEvCompleteWorkerSet(
            barrierWorker.ReplicationId(), barrierWorker.TargetId()));
        AttachWorkers(env, controllerId, {barrierWorker, failedWorker});

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema));
        const auto release = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(release->Get()->Record.GetWorker()), barrierWorker);
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema, true));
        const auto applied = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(applied->Get()->Record.GetApplied());

        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(failedWorker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
            NKikimrReplication::TEvWorkerStatus::REASON_ERROR, "reader failed"));

        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());

        // Restart while Error, before requesting pause. The new controller
        // has no worker commands and stops both workers reported by service,
        // including the healthy target's barrier worker.
        RestartController(env, controllerId);
        AttachWorkers(env, controllerId, {barrierWorker, failedWorker});
        THashSet<TWorkerId> stopped;
        for (ui32 i = 0; i < 2; ++i) {
            const auto stop = env.GetRuntime().GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
            stopped.insert(TWorkerId::Parse(stop->Get()->Record.GetWorker()));
        }
        UNIT_ASSERT(stopped.contains(barrierWorker));
        UNIT_ASSERT(stopped.contains(failedWorker));
        for (const auto& id : {barrierWorker, failedWorker}) {
            env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(id,
                NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        }
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());

        RequestPause(env, info, 1002);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        // Progress must re-register the healthy sibling's durable worker;
        // the old target-error-only recovery would never emit this run.
        const auto replacement = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(replacement->Get()->Record.GetWorker()), barrierWorker);
        AttachWorkers(env, controllerId, {barrierWorker});
        const auto replay = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(replay->Get()->Record.GetApplied());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(replay->Get()->Record.GetWorker()), barrierWorker);

        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema, false, true));
        const auto completed = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(completed->Get()->Record.GetCompleted());

        const auto finalStop = env.GetRuntime().GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(finalStop->Get()->Record.GetWorker()), barrierWorker);
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(barrierWorker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        WaitForPaused(env, info);
    }

    Y_UNIT_TEST(PauseRecoversDetachedSiblingWithoutControllerRestart) {
        TEnv env;
        const auto info = StartReplication(env, 2);
        const auto controllerId = info.ControllerId;

        const auto firstRun = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto secondRun = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto barrierWorker = TWorkerId::Parse(firstRun->Get()->Record.GetWorker());
        const auto failedWorker = TWorkerId::Parse(secondRun->Get()->Record.GetWorker());
        UNIT_ASSERT_VALUES_UNEQUAL(barrierWorker.TargetId(), failedWorker.TargetId());
        env.SendAsync(controllerId, new TEvPrivate::TEvCompleteWorkerSet(
            barrierWorker.ReplicationId(), barrierWorker.TargetId()));
        AttachWorkers(env, controllerId, {barrierWorker, failedWorker});

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema));
        const auto release = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(release->Get()->Record.GetWorker()), barrierWorker);
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema, true));
        const auto applied = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(applied->Get()->Record.GetApplied());

        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(failedWorker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
            NKikimrReplication::TEvWorkerStatus::REASON_ERROR, "reader failed"));
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());

        // The healthy target's worker exhausts transient retries while the
        // replication is Error. It is detached from TSessionInfo without a
        // controller restart; its TWorkerInfo must not retain that session.
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(barrierWorker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
            NKikimrReplication::TEvWorkerStatus::REASON_UNSPECIFIED, ""));
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());

        RequestPause(env, info, 1003);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        // A stale session ID would suppress recovery and never emit this run.
        const auto replacement = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(replacement->Get()->Record.GetWorker()), barrierWorker);
        AttachWorkers(env, controllerId, {barrierWorker});
        const auto replay = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(replay->Get()->Record.GetApplied());
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema, false, true));
        const auto completed = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(completed->Get()->Record.GetCompleted());

        for (ui32 i = 0; i < 2; ++i) {
            env.GetRuntime().GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        }
        for (const auto& id : {barrierWorker, failedWorker}) {
            env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(id,
                NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        }
        WaitForPaused(env, info);
    }

    Y_UNIT_TEST(PauseKeepsWorkerRegistrationAliveAfterRestart) {
        TEnv env;
        const auto info = StartReplication(env);
        const auto controllerId = info.ControllerId;

        const auto run = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());
        const TString topicPath = run->Get()->Record.GetCommand().GetRemoteTopicReader().GetTopicPath();
        AttachWorkers(env, controllerId, {worker});

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema));
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema, true));
        const auto applied = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(applied->Get()->Record.GetApplied());

        auto& runtime = env.GetRuntime();
        const auto schemeCache = runtime.GetLocalServiceId(MakeSchemeCacheID());
        const auto describeTopicGate = runtime.Register(
            new TDescribeTopicRequestGate(schemeCache, env.GetSender(), topicPath));
        runtime.RegisterService(MakeSchemeCacheID(), describeTopicGate);

        RestartController(env, controllerId);
        env.SendAsync(controllerId, new TEvService::TEvStatus());

        const auto blocked = runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(blocked->Get()->Tag, TDescribeTopicRequestGate::RequestBlockedTag());
        RequestPause(env, info, 1004);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        runtime.RegisterService(MakeSchemeCacheID(), schemeCache);
        env.SendAsync(describeTopicGate,
            new TEvents::TEvWakeup(TDescribeTopicRequestGate::ReleaseRequestTag()));

        const auto replacement = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(replacement->Get()->Record.GetWorker()), worker);
        AttachWorkers(env, controllerId, {worker});
        const auto replay = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(replay->Get()->Record.GetApplied());

        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema, false, true));
        const auto completed = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(completed->Get()->Record.GetCompleted());

        runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(worker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        WaitForPaused(env, info);
    }

    Y_UNIT_TEST(ConfigurationUpdatePreservesDeferredPauseAcrossRestart) {
        TEnv env;
        const auto info = StartReplication(env);
        const auto controllerId = info.ControllerId;

        const auto run = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());
        AttachWorkers(env, controllerId, {worker});

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema));
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema, true));
        const auto applied = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(applied->Get()->Record.GetApplied());

        RequestPause(env, info, 1005);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
        RequestConfigUpdate(env, info, 1006);

        RestartController(env, controllerId);
        AttachWorkers(env, controllerId, {worker});
        const auto replay = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(replay->Get()->Record.GetApplied());

        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema, false, true));
        const auto completed = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(completed->Get()->Record.GetCompleted());

        env.GetRuntime().GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(worker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        WaitForPaused(env, info);
    }

    Y_UNIT_TEST(GlobalTargetFlushReplaysAfterControllerRestart) {
        TEnv env;
        const auto info = StartReplication(env, 2, "root@builtin", true);
        const auto controllerId = info.ControllerId;
        auto& runtime = env.GetRuntime();

        const auto firstRun = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto secondRun = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto first = TWorkerId::Parse(firstRun->Get()->Record.GetWorker());
        const auto second = TWorkerId::Parse(secondRun->Get()->Record.GetWorker());
        const auto replica1 = env.GetPathId("/Root/replica1");
        const bool firstWritesReplica1 = TPathId::FromProto(firstRun->Get()->Record
            .GetCommand().GetLocalTableWriter().GetPathId()) == replica1;
        const auto barrierWorker = firstWritesReplica1 ? first : second;
        const auto siblingWorker = firstWritesReplica1 ? second : first;
        AttachWorkers(env, controllerId, {barrierWorker, siblingWorker});
        env.SendAsync(controllerId, new TEvPrivate::TEvCompleteWorkerSet(
            barrierWorker.ReplicationId(), barrierWorker.TargetId()));
        DescribeReplication(env, info);

        // Allocate the later source interval first, making its write ID
        // numerically smaller than the earlier interval's ID.
        const auto laterResult = env.Send<TEvService::TEvTxIdResult>(controllerId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(19000, 0)}));
        const auto laterTxId = laterResult->Get()->Record.GetVersionTxIds(0).GetTxId();
        const auto earlierResult = env.Send<TEvService::TEvTxIdResult>(controllerId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(5000, 0)}));
        const auto earlierTxId = earlierResult->Get()->Record.GetVersionTxIds(0).GetTxId();
        UNIT_ASSERT(laterTxId);
        UNIT_ASSERT(earlierTxId);
        UNIT_ASSERT_VALUES_UNEQUAL(laterTxId, earlierTxId);
        const auto assignedVersion = TRowVersion(20000, 0);

        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        const auto commitGate = runtime.Register(
            new TCommitWritesRequestGate(txProxy, env.GetSender(), "/Root/replica1"));
        runtime.RegisterService(MakeTxProxyID(), commitGate);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())
            ->Get()->Tag, TCommitWritesRequestGate::ReadyTag());

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema));
        auto blocked = runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(blocked->Get()->Tag, TCommitWritesRequestGate::RequestBlockedTag());
        UNIT_ASSERT_VALUES_EQUAL(blocked->Cookie, earlierTxId);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1")
            .GetPathDescription().GetTable().ColumnsSize(), 2);

        RestartController(env, controllerId);
        blocked = runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(blocked->Get()->Tag, TCommitWritesRequestGate::RequestBlockedTag());
        UNIT_ASSERT_VALUES_EQUAL(blocked->Cookie, earlierTxId);
        AttachWorkers(env, controllerId, {barrierWorker, siblingWorker});

        env.SendAsync(commitGate,
            new TEvents::TEvWakeup(TCommitWritesRequestGate::ReleaseRequestsTag()));
        blocked = runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(blocked->Get()->Tag, TCommitWritesRequestGate::RequestBlockedTag());
        UNIT_ASSERT_VALUES_EQUAL(blocked->Cookie, laterTxId);
        runtime.RegisterService(MakeTxProxyID(), txProxy);
        env.SendAsync(commitGate,
            new TEvents::TEvWakeup(TCommitWritesRequestGate::ReleaseRequestsTag()));

        const auto release = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(release->Get()->Record.GetWorker()), barrierWorker);
        WaitForColumnCount(env, "/Root/replica1", 3);

        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema, true));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender())
            ->Get()->Record.GetApplied());
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema, false, true));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender())
            ->Get()->Record.GetCompleted());

        // The captured IDs must survive recovery in Verifying as well as
        // recovery while the target-only flush is still in progress.
        RestartController(env, controllerId);
        AttachWorkers(env, controllerId, {barrierWorker, siblingWorker});
        DescribeReplication(env, info);

        RequestPause(env, info, 2000);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        SendHeartbeat(env, controllerId, barrierWorker, assignedVersion);
        SendHeartbeat(env, controllerId, siblingWorker, assignedVersion);

        for (ui32 i = 0; i < 2; ++i) {
            runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        }
        for (const auto& worker : {barrierWorker, siblingWorker}) {
            env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(worker,
                NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        }
        WaitForPaused(env, info);
    }

    Y_UNIT_TEST(GlobalBarrierWithoutPendingWritesWaitsForFreshHeartbeat) {
        TEnv env;
        const auto info = StartReplication(env, 1, "root@builtin", true);
        const auto controllerId = info.ControllerId;

        const auto run = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());
        AttachWorkers(env, controllerId, {worker});

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema));
        const auto release = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(release->Get()->Record.GetWorker()), worker);

        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema, true));
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema, false, true));
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());

        RequestPause(env, info, 2001);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        SendHeartbeat(env, controllerId, worker, TRowVersion(101, 0));
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(worker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        WaitForPaused(env, info);
    }

    Y_UNIT_TEST(PauseRestartsCompletedWorkerNeededForVerification) {
        TEnv env;
        const auto info = StartReplication(env, 1, "root@builtin", true);
        const auto controllerId = info.ControllerId;
        auto& runtime = env.GetRuntime();

        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());
        AttachWorkers(env, controllerId, {worker});

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema));
        runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema, true));
        runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema, false, true));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender())
            ->Get()->Record.GetCompleted());

        // The handshake is complete, but no post-DDL heartbeat was reported.
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(worker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
            NKikimrReplication::TEvWorkerStatus::REASON_ERROR, "reader failed"));
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());

        RequestPause(env, info, 2002);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
        const auto stop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), worker);
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(worker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));

        const auto replacement = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(replacement->Get()->Record.GetWorker()), worker);
        AttachWorkers(env, controllerId, {worker});
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        SendHeartbeat(env, controllerId, worker, TRowVersion(101, 0));
        const auto finalStop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(finalStop->Get()->Record.GetWorker()), worker);
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(worker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        WaitForPaused(env, info);
    }

    Y_UNIT_TEST(PauseRecoversFailedSiblingNeededForVerification) {
        TEnv env;
        const auto info = StartReplication(env, 2, "root@builtin", true);
        const auto controllerId = info.ControllerId;
        auto& runtime = env.GetRuntime();

        const auto firstRun = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto secondRun = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto barrierWorker = TWorkerId::Parse(firstRun->Get()->Record.GetWorker());
        const auto siblingWorker = TWorkerId::Parse(secondRun->Get()->Record.GetWorker());
        UNIT_ASSERT_VALUES_UNEQUAL(barrierWorker.TargetId(), siblingWorker.TargetId());
        AttachWorkers(env, controllerId, {barrierWorker, siblingWorker});
        env.SendAsync(controllerId, new TEvPrivate::TEvCompleteWorkerSet(
            barrierWorker.ReplicationId(), barrierWorker.TargetId()));
        DescribeReplication(env, info);

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema));
        const auto release = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(release->Get()->Record.GetWorker()), barrierWorker);
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema, true));
        runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema, false, true));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender())
            ->Get()->Record.GetCompleted());

        // The sibling has no schema barrier, but its heartbeat is still
        // required to move the changed target out of Verifying.
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(siblingWorker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
            NKikimrReplication::TEvWorkerStatus::REASON_ERROR, "reader failed"));
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());

        RequestPause(env, info, 2003);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
        const auto stop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), siblingWorker);
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(siblingWorker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));

        const auto replacement = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(replacement->Get()->Record.GetWorker()), siblingWorker);
        AttachWorkers(env, controllerId, {barrierWorker, siblingWorker});
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        SendHeartbeat(env, controllerId, barrierWorker, TRowVersion(101, 0));
        SendHeartbeat(env, controllerId, siblingWorker, TRowVersion(101, 0));
        THashSet<TWorkerId> stopped;
        for (ui32 i = 0; i < 2; ++i) {
            const auto finalStop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
            stopped.insert(TWorkerId::Parse(finalStop->Get()->Record.GetWorker()));
        }
        UNIT_ASSERT(stopped.contains(barrierWorker));
        UNIT_ASSERT(stopped.contains(siblingWorker));
        for (const auto& worker : {barrierWorker, siblingWorker}) {
            env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(worker,
                NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        }
        WaitForPaused(env, info);
    }

    Y_UNIT_TEST(PauseRecoversFailedSiblingWhileCollectingSchemaReports) {
        TEnv env;
        const auto info = StartReplication(env, 2, "root@builtin", true);
        const auto controllerId = info.ControllerId;
        auto& runtime = env.GetRuntime();

        const auto firstRun = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto secondRun = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto replica1 = env.GetPathId("/Root/replica1");
        const bool firstWritesReplica1 = TPathId::FromProto(firstRun->Get()->Record
            .GetCommand().GetLocalTableWriter().GetPathId()) == replica1;
        const auto first = TWorkerId::Parse((firstWritesReplica1 ? firstRun : secondRun)
            ->Get()->Record.GetWorker());
        const auto other = TWorkerId::Parse((firstWritesReplica1 ? secondRun : firstRun)
            ->Get()->Record.GetWorker());
        UNIT_ASSERT_VALUES_UNEQUAL(first.TargetId(), other.TargetId());
        const auto second = RegisterSecondWorkerAndCompleteSet(env, controllerId, first);
        AttachWorkers(env, controllerId, {first, second, other});
        DescribeReplication(env, info);

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(first, schema));
        DescribeReplication(env, info);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1")
            .GetPathDescription().GetTable().ColumnsSize(), 2);

        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(other,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
            NKikimrReplication::TEvWorkerStatus::REASON_ERROR, "reader failed"));
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());

        RequestPause(env, info, 2006);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
        const auto stop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), other);
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(other,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        const auto replacement = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(replacement->Get()->Record.GetWorker()), other);
        AttachWorkers(env, controllerId, {first, second, other});

        // The barrier is still Collecting; the second partition now starts DDL.
        env.SendAsync(controllerId, MakeSchemaChangeReport(second, schema));
        THashSet<TWorkerId> released;
        for (ui32 i = 0; i < 2; ++i) {
            const auto result = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
            released.insert(TWorkerId::Parse(result->Get()->Record.GetWorker()));
        }
        UNIT_ASSERT(released.contains(first));
        UNIT_ASSERT(released.contains(second));
        WaitForColumnCount(env, "/Root/replica1", 3);

        for (const auto& worker : {first, second}) {
            env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema, true));
            runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
            env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema, false, true));
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender())
                ->Get()->Record.GetCompleted());
        }

        for (const auto& worker : {first, second, other}) {
            SendHeartbeat(env, controllerId, worker, TRowVersion(101, 0));
        }
        THashSet<TWorkerId> stopped;
        for (ui32 i = 0; i < 3; ++i) {
            const auto finalStop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
            stopped.insert(TWorkerId::Parse(finalStop->Get()->Record.GetWorker()));
        }
        UNIT_ASSERT(stopped.contains(first));
        UNIT_ASSERT(stopped.contains(second));
        UNIT_ASSERT(stopped.contains(other));
        for (const auto& worker : {first, second, other}) {
            env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(worker,
                NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        }
        WaitForPaused(env, info);
    }

    Y_UNIT_TEST(PauseRecoversFailedSiblingBeforeDestinationDdl) {
        TEnv env;
        const auto info = StartReplication(env, 2, "root@builtin", true);
        const auto controllerId = info.ControllerId;
        auto& runtime = env.GetRuntime();

        const auto firstRun = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto secondRun = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto replica1 = env.GetPathId("/Root/replica1");
        const bool firstWritesReplica1 = TPathId::FromProto(firstRun->Get()->Record
            .GetCommand().GetLocalTableWriter().GetPathId()) == replica1;
        const auto barrierWorker = TWorkerId::Parse((firstWritesReplica1 ? firstRun : secondRun)
            ->Get()->Record.GetWorker());
        const auto siblingWorker = TWorkerId::Parse((firstWritesReplica1 ? secondRun : firstRun)
            ->Get()->Record.GetWorker());
        AttachWorkers(env, controllerId, {barrierWorker, siblingWorker});
        env.SendAsync(controllerId, new TEvPrivate::TEvCompleteWorkerSet(
            barrierWorker.ReplicationId(), barrierWorker.TargetId()));
        DescribeReplication(env, info);

        const auto assigned = env.Send<TEvService::TEvTxIdResult>(controllerId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(100, 0)}));
        UNIT_ASSERT(assigned->Get()->Record.GetVersionTxIds(0).GetTxId());
        const auto boundary = TRowVersion::FromProto(assigned->Get()->Record.GetVersionTxIds(0).GetVersion());

        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        const auto commitGate = runtime.Register(
            new TCommitWritesRequestGate(txProxy, env.GetSender(), "/Root/replica1"));
        runtime.RegisterService(MakeTxProxyID(), commitGate);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())
            ->Get()->Tag, TCommitWritesRequestGate::ReadyTag());

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema));
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())
            ->Get()->Tag, TCommitWritesRequestGate::RequestBlockedTag());
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1")
            .GetPathDescription().GetTable().ColumnsSize(), 2);

        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(siblingWorker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
            NKikimrReplication::TEvWorkerStatus::REASON_ERROR, "reader failed"));
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());

        RequestPause(env, info, 2004);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
        const auto stop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), siblingWorker);
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(siblingWorker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        const auto replacement = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(replacement->Get()->Record.GetWorker()), siblingWorker);
        AttachWorkers(env, controllerId, {barrierWorker, siblingWorker});

        // DDL is still blocked; recovery must not wait for Verifying.
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1")
            .GetPathDescription().GetTable().ColumnsSize(), 2);
        runtime.RegisterService(MakeTxProxyID(), txProxy);
        env.SendAsync(commitGate,
            new TEvents::TEvWakeup(TCommitWritesRequestGate::ReleaseRequestsTag()));

        const auto release = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(release->Get()->Record.GetWorker()), barrierWorker);
        WaitForColumnCount(env, "/Root/replica1", 3);
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema, true));
        runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema, false, true));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender())
            ->Get()->Record.GetCompleted());

        SendHeartbeat(env, controllerId, barrierWorker, boundary);
        SendHeartbeat(env, controllerId, siblingWorker, boundary);
        for (ui32 i = 0; i < 2; ++i) {
            runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        }
        for (const auto& worker : {barrierWorker, siblingWorker}) {
            env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(worker,
                NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        }
        WaitForPaused(env, info);
    }

    Y_UNIT_TEST(PauseRecoversFreshSiblingNeededForCapturedTxId) {
        TEnv env;
        const auto info = StartReplication(env, 2, "root@builtin", true);
        const auto controllerId = info.ControllerId;
        auto& runtime = env.GetRuntime();

        const auto firstRun = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto secondRun = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto barrierWorker = TWorkerId::Parse(firstRun->Get()->Record.GetWorker());
        const auto siblingWorker = TWorkerId::Parse(secondRun->Get()->Record.GetWorker());
        UNIT_ASSERT_VALUES_UNEQUAL(barrierWorker.TargetId(), siblingWorker.TargetId());
        AttachWorkers(env, controllerId, {barrierWorker, siblingWorker});
        env.SendAsync(controllerId, new TEvPrivate::TEvCompleteWorkerSet(
            barrierWorker.ReplicationId(), barrierWorker.TargetId()));
        DescribeReplication(env, info);

        const auto assigned = env.Send<TEvService::TEvTxIdResult>(controllerId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(15000, 0)}));
        const auto& captured = assigned->Get()->Record.GetVersionTxIds(0);
        UNIT_ASSERT(captured.GetTxId());
        const auto capturedBoundary = TRowVersion::FromProto(captured.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(capturedBoundary, TRowVersion(20000, 0));

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema));
        const auto release = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(release->Get()->Record.GetWorker()), barrierWorker);
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema, true));
        runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        env.SendAsync(controllerId, MakeSchemaChangeReport(barrierWorker, schema, false, true));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender())
            ->Get()->Record.GetCompleted());

        // This is newer than the schema but too old to commit the captured ID.
        SendHeartbeat(env, controllerId, siblingWorker, TRowVersion(10000, 0));
        DescribeReplication(env, info);
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(siblingWorker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
            NKikimrReplication::TEvWorkerStatus::REASON_ERROR, "reader failed"));
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());

        RequestPause(env, info, 2005);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
        const auto stop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), siblingWorker);
        env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(siblingWorker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));

        const auto replacement = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(replacement->Get()->Record.GetWorker()), siblingWorker);
        AttachWorkers(env, controllerId, {barrierWorker, siblingWorker});
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        SendHeartbeat(env, controllerId, barrierWorker, capturedBoundary);
        SendHeartbeat(env, controllerId, siblingWorker, capturedBoundary);
        THashSet<TWorkerId> stopped;
        for (ui32 i = 0; i < 2; ++i) {
            const auto finalStop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
            stopped.insert(TWorkerId::Parse(finalStop->Get()->Record.GetWorker()));
        }
        UNIT_ASSERT(stopped.contains(barrierWorker));
        UNIT_ASSERT(stopped.contains(siblingWorker));
        for (const auto& worker : {barrierWorker, siblingWorker}) {
            env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(worker,
                NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
        }
        WaitForPaused(env, info);
    }

    Y_UNIT_TEST(GlobalBackToBackSchemaChangesProgressDuringVerification) {
        TEnv env;
        const auto info = StartReplication(env, 1, "root@builtin", true);
        const auto controllerId = info.ControllerId;

        const auto run = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());
        AttachWorkers(env, controllerId, {worker});

        const auto txIdResult = env.Send<TEvService::TEvTxIdResult>(controllerId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(100, 0)}));
        UNIT_ASSERT(txIdResult->Get()->Record.GetVersionTxIds(0).GetTxId());

        const auto addColumn = MakeSchemaChange();
        const auto dropColumn = MakeSchemaChange(200, 20, 3, false);
        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, addColumn));
        auto release = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(release->Get()->Record.GetSchema().SerializeAsString(),
            addColumn.SerializeAsString());

        // A newer record proves a post-DDL boundary but remains parked until
        // the current data-plane barrier is fully completed.
        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, dropColumn));
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1")
            .GetPathDescription().GetTable().ColumnsSize(), 3);

        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, addColumn, true));
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, addColumn, false, true));
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());

        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, dropColumn));
        release = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(release->Get()->Record.GetSchema().SerializeAsString(),
            dropColumn.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1")
            .GetPathDescription().GetTable().ColumnsSize(), 2);
    }

    Y_UNIT_TEST(WaitsForCompleteMembershipAndReleasesDuplicateReport) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NLog::PRI_TRACE);
        const auto info = StartReplication(env, 1, "");
        const auto controllerId = info.ControllerId;
        const auto initialGeneration = info.Generation;

        // The production target registar supplies the first root-partition
        // worker.  Its command is also the authoritative replication/target
        // identity for the test.
        const auto firstRun = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto first = TWorkerId::Parse(firstRun->Get()->Record.GetWorker());
        // Register a second root partition through the controller's normal
        // durable worker-registration transaction before either report.
        const auto second = RegisterSecondWorkerAndCompleteSet(env, controllerId, first);

        const auto schema = MakeSchemaChange();
        env.SendAsync(controllerId, MakeSchemaChangeReport(first, schema));

        // A single report must not begin DDL. The destination still lacks the
        // requested column until every worker in the durable roster reports.
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().ColumnsSize(), 2);

        // Drain the first report transaction, then restart the controller
        // while it is still collecting. The second report must complete the
        // same persisted member set rather than replacing it with only the
        // worker that survived in memory.
        DescribeReplication(env, info);
        const auto recoveredGeneration = RestartController(env, controllerId);
        UNIT_ASSERT(recoveredGeneration > initialGeneration);
        AttachWorkers(env, controllerId, {first, second});

        env.SendAsync(controllerId, MakeSchemaChangeReport(second, schema));

        const auto appliedFirst = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        const auto appliedFirstId = TWorkerId::Parse(appliedFirst->Get()->Record.GetWorker());
        UNIT_ASSERT(appliedFirstId == first || appliedFirstId == second);
        UNIT_ASSERT_VALUES_EQUAL(appliedFirst->Get()->Record.GetSchema().SerializeAsString(), schema.SerializeAsString());

        const auto appliedSecond = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        const auto appliedSecondId = TWorkerId::Parse(appliedSecond->Get()->Record.GetWorker());
        UNIT_ASSERT(appliedSecondId == first || appliedSecondId == second);
        UNIT_ASSERT_VALUES_UNEQUAL(appliedFirstId, appliedSecondId);

        const auto destinationDescription = env.GetDescription("/Root/replica1");
        const auto& destination = destinationDescription.GetPathDescription().GetTable();
        UNIT_ASSERT_VALUES_EQUAL(destination.ColumnsSize(), 3);

        env.SendAsync(controllerId, MakeSchemaChangeReport(first, schema, true));
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());

        const auto appliedGeneration = RestartController(env, controllerId);
        UNIT_ASSERT(appliedGeneration > recoveredGeneration);
        AttachWorkers(env, controllerId, {first, second});
        const auto restored = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(restored->Get()->Record.GetApplied());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(restored->Get()->Record.GetWorker()), first);

        // A service that restarted with no workers subsequently boots this
        // worker and acknowledges STATUS_RUNNING. That acknowledgement must
        // receive the durable recovery release, not only the initial status
        // worker list.
        auto running = MakeHolder<TEvService::TEvWorkerStatus>(
            first, NKikimrReplication::TEvWorkerStatus::STATUS_RUNNING);
        env.SendAsync(controllerId, running.Release());
        const auto replayed = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(replayed->Get()->Record.GetApplied());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(replayed->Get()->Record.GetWorker()), first);
        UNIT_ASSERT_VALUES_EQUAL(replayed->Get()->Record.GetSchema().SerializeAsString(), schema.SerializeAsString());

        env.SendAsync(controllerId, MakeSchemaChangeReport(second, schema, true));
        env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());

        env.SendAsync(controllerId, MakeSchemaChangeReport(first, schema, false, true));
        auto completed = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(completed->Get()->Record.GetCompleted());
        env.SendAsync(controllerId, MakeSchemaChangeReport(second, schema, false, true));
        completed = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(completed->Get()->Record.GetCompleted());

        // A replay after APPLIED is an idempotent release: it must not start a
        // second DDL operation or change the durable schema.
        env.SendAsync(controllerId, MakeSchemaChangeReport(first, schema));
        const auto duplicate = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(duplicate->Get()->Record.GetWorker()), first);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().ColumnsSize(), 3);

        // A later normalized snapshot can remove the previously added
        // column, but only after the earlier barrier was fully completed.
        const auto dropped = MakeSchemaChange(101, 11, 3, false);
        env.SendAsync(controllerId, MakeSchemaChangeReport(first, dropped));
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().ColumnsSize(), 3);
        env.SendAsync(controllerId, MakeSchemaChangeReport(second, dropped));

        const auto dropFirst = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        const auto dropSecond = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_UNEQUAL(
            TWorkerId::Parse(dropFirst->Get()->Record.GetWorker()),
            TWorkerId::Parse(dropSecond->Get()->Record.GetWorker()));
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().ColumnsSize(), 2);

        for (const auto& id : {first, second}) {
            env.SendAsync(controllerId, MakeSchemaChangeReport(id, dropped, true));
            env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
            env.SendAsync(controllerId, MakeSchemaChangeReport(id, dropped, false, true));
            const auto completion = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
            UNIT_ASSERT(completion->Get()->Record.GetCompleted());
        }

        // Different snapshots for one barrier are rejected rather than
        // allowing one worker's schema to determine destination DDL.
        const auto conflicting = MakeSchemaChange(102, 12, 4);
        env.SendAsync(controllerId, MakeSchemaChangeReport(first, conflicting));
        env.SendAsync(controllerId, MakeSchemaChangeReport(second, MakeSchemaChange(102, 12, 4, false)));

        const auto state = DescribeReplication(env, info);
        UNIT_ASSERT(state->Get()->Record.GetState().HasError());
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().ColumnsSize(), 2);
    }

}

} // NKikimr::NReplication::NController
