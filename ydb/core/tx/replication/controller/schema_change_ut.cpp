#include "controller_impl.h"
#include "dst_creator.h"
#include "dst_schema_changer.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/tx_helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/replication/ut_helpers/mock_service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/test_table.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/executor_pool_base.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/datetime/base.h>

#include <functional>

namespace NKikimr::NReplication::NController {

namespace {

using TFamily = NKikimrReplication::TSchemaChange::TFamily;

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

NTestHelpers::TFeatureFlags MakeIndexReplicationFlags() {
    NTestHelpers::TFeatureFlags flags;
    flags.SetEnableChangefeedsOnIndexTables(true);
    flags.FeatureFlags.SetEnableAsyncReplicationSchemaChanges(true);
    return flags;
}

NKikimrReplication::TSchemaChange MakeSyncIndexSchemaChange() {
    auto schema = MakeSchemaChange(100, 10, 2, false);
    auto& index = *schema.MutableIndexes()->AddItems();
    index.SetName("by_value");
    index.SetType("GlobalSync");
    index.AddIndexColumns("value");
    return schema;
}

void AddSyncIndex(NYdb::NTable::TSession& session, const TString& table, const TString& index) {
    const auto result = session.ExecuteSchemeQuery(TStringBuilder()
        << "ALTER TABLE `" << table << "` ADD INDEX " << index << " GLOBAL SYNC ON (value);").GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

NKikimrReplication::TSchemaChange MakeFamilySchemaChange(const TString& media) {
    auto schema = MakeSchemaChange(100, 10, 2, false);
    schema.MutableColumns(0)->SetFamily("default");
    schema.MutableColumns(1)->SetFamily("archive");

    auto* defaultFamily = schema.AddFamilies();
    defaultFamily->SetName("default");
    defaultFamily->SetCompression(TFamily::COMPRESSION_OFF);
    defaultFamily->SetCacheMode(TFamily::CACHE_MODE_REGULAR);

    auto* archive = schema.AddFamilies();
    archive->SetName("archive");
    archive->SetMedia(media);
    archive->SetCompression(TFamily::COMPRESSION_LZ4);
    archive->SetCacheMode(TFamily::CACHE_MODE_REGULAR);
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

bool WaitFor(const std::function<bool()>& predicate, ui32 attempts = 50,
    TDuration delay = TDuration::MilliSeconds(100))
{
    for (ui32 attempt = 0; attempt < attempts; ++attempt) {
        if (predicate()) {
            return true;
        }

        Sleep(delay);
    }

    return false;
}

using TTestEnv = NTestHelpers::TEnv<>;

void CompleteSchemaChange(TTestEnv& env, ui64 controllerId, const TWorkerId& worker,
        const NKikimrReplication::TSchemaChange& schema)
{
    env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema, true));
    env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
    env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema, false, true));
    env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
}

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

class TSchemaDescribeGate: public TActorBootstrapped<TSchemaDescribeGate> {
public:
    TSchemaDescribeGate(const TActorId& pipeCache, const TActorId& notify, TPathId pathId)
        : PipeCache(pipeCache)
        , Notify(notify)
        , PathId(pathId)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    static constexpr ui64 Blocked = 20;
    static constexpr ui64 Release = 21;

private:
    void Handle(TEvPipeCache::TEvForward::TPtr& ev) {
        if (ev->Get()->Ev->Type() == NSchemeShard::TEvSchemeShard::TEvDescribeScheme::EventType) {
            const auto& record = static_cast<NSchemeShard::TEvSchemeShard::TEvDescribeScheme*>(ev->Get()->Ev.Get())->Record;
            if (record.GetSchemeshardId() == PathId.OwnerId && record.GetPathId() == PathId.LocalPathId) {
                Pending.push_back(std::move(ev));
                Send(Notify, new TEvents::TEvWakeup(Blocked));
                return;
            }
        }

        Send(ev->Forward(PipeCache));
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Tag, Release);
        UNIT_ASSERT(!Pending.empty());
        for (auto& request : Pending) {
            Send(request->Forward(PipeCache));
        }
        Pending.clear();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPipeCache::TEvForward, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        default:
            Send(ev->Forward(PipeCache));
        }
    }

    const TActorId PipeCache;
    const TActorId Notify;
    const TPathId PathId;
    TVector<TEvPipeCache::TEvForward::TPtr> Pending;
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

template <bool CreateStream>
class TProposalRecorder: public TActorBootstrapped<TProposalRecorder<CreateStream>> {
    using TBase = TActorBootstrapped<TProposalRecorder<CreateStream>>;

public:
    TProposalRecorder(const TActorId& proxy, const TActorId& notify)
        : Proxy(proxy)
        , Notify(notify)
    {
    }

    void Bootstrap() {
        TBase::Become(&TProposalRecorder::StateWork);
        TBase::Send(Notify, new TEvents::TEvWakeup());
    }

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() != TEvTxUserProxy::TEvProposeTransaction::EventType) {
            TBase::Send(ev->Forward(Proxy));
            return;
        }

        const auto& tx = ev->Get<TEvTxUserProxy::TEvProposeTransaction>()->Record.GetTransaction();
        const bool record = CreateStream ? tx.GetModifyScheme().HasCreateCdcStream() : tx.HasCommitWrites();
        TBase::Send(ev->Forward(record ? Notify : Proxy));
    }

private:
    const TActorId Proxy;
    const TActorId Notify;
};

using TCommitWritesRecorder = TProposalRecorder<false>;
using TCreateCdcStreamRequestGate = TProposalRecorder<true>;

class TAttachAllocationGate: public TActorBootstrapped<TAttachAllocationGate> {
    static constexpr ui64 Ready = 6;
    static constexpr ui64 Blocked = 7;
    static constexpr ui64 Release = 8;

    void Handle(TEvTxUserProxy::TEvAllocateTxId::TPtr& ev) {
        if (++Allocations[ev->Sender] == 2) {
            UNIT_ASSERT(!PendingRequest);
            PendingRequest = std::move(ev);
            Send(Notify, new TEvents::TEvWakeup(Blocked));
        } else {
            Send(ev->Forward(TxProxy));
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Tag, Release);
        UNIT_ASSERT(PendingRequest);
        Send(PendingRequest->Forward(TxProxy));
    }

public:
    TAttachAllocationGate(const TActorId& txProxy, const TActorId& notify)
        : TxProxy(txProxy)
        , Notify(notify)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        Send(Notify, new TEvents::TEvWakeup(Ready));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvAllocateTxId, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        default:
            Send(ev->Forward(TxProxy));
        }
    }

    static ui64 ReadyTag() { return Ready; }
    static ui64 BlockedTag() { return Blocked; }
    static ui64 ReleaseTag() { return Release; }

private:
    const TActorId TxProxy;
    const TActorId Notify;
    THashMap<TActorId, ui32> Allocations;
    TEvTxUserProxy::TEvAllocateTxId::TPtr PendingRequest;
};

class TWorkerRemovalGate: public TDecorator {
public:
    static constexpr ui64 Blocked = 22;
    static constexpr ui64 Release = 23;

    TWorkerRemovalGate(THolder<IActor> actor, TWorkerId worker, TActorId notify)
        : TDecorator(std::move(actor))
        , Worker(worker)
        , Notify(notify)
    {}

    bool DoBeforeReceiving(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) override {
        const bool removingWorker = Blocking && ev->GetTypeRewrite() == TEvPrivate::TEvRemoveWorker::EventType;
        if (removingWorker && ev->Get<TEvPrivate::TEvRemoveWorker>()->Id == Worker) {
            Pending.push_back(std::move(ev));
            ctx.Send(Notify, new TEvents::TEvWakeup(Blocked));
            return false;
        }

        if (ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType && ev->Get<TEvents::TEvWakeup>()->Tag == Release) {
            Blocking = false;
            for (auto& pending : Pending) {
                ctx.Send(pending.Release());
            }
            Pending.clear();
            return false;
        }

        return true;
    }

private:
    const TWorkerId Worker;
    const TActorId Notify;
    bool Blocking = true;
    TVector<TAutoPtr<IEventHandle>> Pending;
};

TActorId InstallWorkerRemovalGate(TTestEnv& env, ui64 controllerId, const TWorkerId& worker) {
    auto& runtime = env.GetRuntime();
    const auto controller = ResolveTablet(runtime, controllerId);
    TMailbox* mailbox = nullptr;
    for (auto* pool : runtime.GetActorSystem(0)->GetBasicExecutorPools()) {
        if (pool->PoolId == controller.PoolID()) {
            mailbox = pool->GetMailboxTable()->Get(controller.Hint());
            break;
        }
    }
    UNIT_ASSERT(mailbox);
    // Install on the controller's own mailbox so no event can concurrently
    // execute on the actor while it is wrapped. Runtime observers cannot hold
    // controller-to-controller events with TTestEnv's real actor threads.
    runtime.Register(new TFunctorActor([=, notify = env.GetSender()] {
        const auto& ctx = TActivationContext::AsActorContext();
        THolder<IActor> actor(mailbox->DetachActor(controller.LocalId()));
        UNIT_ASSERT(actor);
        auto* gate = new TWorkerRemovalGate(std::move(actor), worker, notify);
        DoActorInit(ctx.ActorSystem(), gate, controller, {});
        mailbox->AttachActor(controller.LocalId(), gate);
    }, env.GetSender()), 0, controller.PoolID(), mailbox);
    runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
    return controller;
}

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
        bool globalConsistency = false, bool withIndex = false,
        NKikimrSchemeOp::EIndexType indexType = NKikimrSchemeOp::EIndexTypeGlobal, bool mockService = true)
{
    NYdb::NTable::TTableClient client(env.GetDriver(), NYdb::NTable::TClientSettings()
        .DiscoveryEndpoint(env.GetEndpoint())
        .Database(env.GetDatabase()));
    auto session = client.CreateSession().GetValueSync().GetSession();
    for (int i = 1; i <= targetCount; ++i) {
        if (withIndex) {
            const auto status = session.ExecuteSchemeQuery(Sprintf(R"(
                CREATE TABLE `/Root/table%i` (
                    key Uint32, value Utf8, PRIMARY KEY (key),
                    INDEX by_value GLOBAL %s ON (value)
                );
            )", i, indexType == NKikimrSchemeOp::EIndexTypeGlobalAsync ? "ASYNC" : "SYNC")).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        } else {
            CreateSourceTable(env, Sprintf("table%i", i));
        }
    }

    const auto service = env.GetRuntime().Register(mockService
        ? NTestHelpers::CreateReplicationMockService(env.GetSender())
        : CreateReplicationService());
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

    const auto status = session.ExecuteSchemeQuery(Sprintf(R"(
        CREATE ASYNC REPLICATION `replication` FOR %s WITH (%s);
    )", JoinSeq(", ", targets).c_str(), JoinSeq(", ", params).c_str())).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), NYdb::EStatus::SUCCESS, status.GetIssues().ToString());

    const auto desc = env.GetDescription("/Root/replication").GetPathDescription().GetReplicationDescription();
    TReplicationTestInfo info;
    info.ControllerId = desc.GetControllerId();
    info.PathId = env.GetPathId("/Root/replication");
    info.Config.CopyFrom(desc.GetConfig());

    if (mockService) {
        const auto handshake = env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(env.GetSender());
        info.Generation = handshake->Get()->Record.GetController().GetGeneration();
        env.SendAsync(info.ControllerId, new TEvService::TEvStatus());
    }

    return info;
}

void SendHeartbeat(TTestEnv& env, ui64 controllerId, const TWorkerId& worker, const TRowVersion& version) {
    auto heartbeat = MakeHolder<TEvService::TEvHeartbeat>();
    worker.Serialize(*heartbeat->Record.MutableWorker());
    version.ToProto(heartbeat->Record.MutableVersion());
    env.SendAsync(controllerId, heartbeat.Release());
}

void SendWorkerStopped(TTestEnv& env, ui64 controllerId, const TWorkerId& worker) {
    env.SendAsync(controllerId, new TEvService::TEvWorkerStatus(worker,
        NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED));
}

void AttachWorkers(TTestEnv& env, ui64 controllerId, std::initializer_list<TWorkerId> workers) {
    auto status = MakeHolder<TEvService::TEvStatus>();
    for (const auto& id : workers) {
        id.Serialize(*status->Record.AddWorkers());
    }

    env.SendAsync(controllerId, status.Release());
}

std::pair<TWorkerId, TWorkerId> AttachBaseAndIndexWorkers(TTestEnv& env, const TReplicationTestInfo& info) {
    auto& runtime = env.GetRuntime();
    const auto first = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
    const auto second = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
    const bool firstIsIndex = first->Get()->Record.GetCommand().GetRemoteTopicReader().GetRetryOnSchemeError();
    const auto base = TWorkerId::Parse((firstIsIndex ? second : first)->Get()->Record.GetWorker());
    const auto index = TWorkerId::Parse((firstIsIndex ? first : second)->Get()->Record.GetWorker());
    AttachWorkers(env, info.ControllerId, {base, index});
    return {base, index};
}

void WaitForAppliedSchemaBarrier(TTestEnv& env, ui64 controllerId, const TWorkerId& worker) {
    constexpr ui32 AppliedPhase = 3;
    const bool applied = WaitFor([&] {
        NKikimrMiniKQL::TResult result;
        UNIT_ASSERT_VALUES_EQUAL(LocalQuery(env.GetRuntime(), controllerId, Sprintf(R"((
            (let key '('('ReplicationId (Uint64 '%lu)) '('Id (Uint64 '%lu))))
            (return (AsList (SetResult 'Barrier (SelectRow 'Targets key '('SchemaBarrierPhase)))))
        ))", worker.ReplicationId(), worker.TargetId()), result), NKikimrProto::OK);
        const auto phase = result.GetValue().GetStruct(0).GetOptional().GetOptional()
            .GetStruct(0).GetOptional().GetUint32();
        return phase == AppliedPhase;
    }, 100, TDuration::MilliSeconds(20));
    UNIT_ASSERT_C(applied, "Schema barrier did not reach Applied");
}

NKikimrReplication::TIndexBuildState ReadIndexBuild(TTestEnv& env, ui64 controllerId, const TWorkerId& worker) {
    NKikimrMiniKQL::TResult result;
    const auto status = LocalQuery(env.GetRuntime(), controllerId, Sprintf(R"((
        (let key '('('ReplicationId (Uint64 '%lu)) '('Id (Uint64 '%lu))))
        (return (AsList (SetResult 'Build (SelectRow 'Targets key '('IndexBuild)))))
    ))", worker.ReplicationId(), worker.TargetId()), result);
    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::OK);
    const auto& row = result.GetValue().GetStruct(0).GetOptional();
    UNIT_ASSERT_C(row.HasOptional(), result.DebugString());
    const auto& value = row.GetOptional().GetStruct(0);
    UNIT_ASSERT_C(value.HasOptional(), result.DebugString());
    NKikimrReplication::TIndexBuildState build;
    UNIT_ASSERT(build.ParseFromString(value.GetOptional().GetBytes()));
    return build;
}

struct TIndexBuildTestEnv: public TTestEnv {
    const TReplicationTestInfo Info;
    const TWorkerId Base;
    NYdb::NTable::TTableClient Client;
    NYdb::NTable::TSession Session;

    TIndexBuildTestEnv()
        : TTestEnv(MakeIndexReplicationFlags())
        , Info(StartReplication(*this, 1, "root@builtin", true))
        , Base(TWorkerId::Parse(GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(GetSender())
            ->Get()->Record.GetWorker()))
        , Client(GetDriver(), NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(GetEndpoint())
            .Database(GetDatabase()))
        , Session(Client.CreateSession().GetValueSync().GetSession())
    {
        AttachWorkers(*this, Info.ControllerId, {Base});
    }

    void AddSourceIndex() {
        AddSyncIndex(Session, "/Root/table1", "by_value");
    }

    void ReportSchemaChange() {
        SendAsync(Info.ControllerId, MakeSchemaChangeReport(Base, MakeSyncIndexSchemaChange()));
        GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(GetSender());
    }

    TWorkerId StartIndexBuild() {
        AddSourceIndex();
        ReportSchemaChange();
        const auto run = GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());
        UNIT_ASSERT(worker.TargetId() != Base.TargetId());
        UNIT_ASSERT(run->Get()->Record.GetCommand().GetLocalTableWriter().GetIndexBuild());

        AttachWorkers(*this, Info.ControllerId, {Base, worker});
        return worker;
    }

    void CompleteBarrier() {
        CompleteSchemaChange(*this, Info.ControllerId, Base, MakeSyncIndexSchemaChange());
        SendHeartbeat(*this, Info.ControllerId, Base, TRowVersion(300, 0));
        WaitForAppliedSchemaBarrier(*this, Info.ControllerId, Base);
    }

    void AssertBuildPhase(const TWorkerId& worker, NKikimrReplication::TIndexBuildState::EPhase phase) {
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(ReadIndexBuild(*this, Info.ControllerId, worker).GetPhase()),
            static_cast<int>(phase));
    }

    void AssertIndexWriteOnly() {
        UNIT_ASSERT_VALUES_EQUAL(GetDescription("/Root/replica1").GetPathDescription()
            .GetTable().GetTableIndexes(0).GetState(), NKikimrSchemeOp::EIndexStateWriteOnly);
    }

    void AlterReplication(THolder<TEvController::TEvAlterReplication> request) {
        const auto result = Send<TEvController::TEvAlterReplicationResult>(Info.ControllerId, std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
    }

    void StopWorker(const TWorkerId& expected) {
        const auto stop = GetRuntime().GrabEdgeEvent<TEvService::TEvStopWorker>(GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), expected);
        SendWorkerStopped(*this, Info.ControllerId, expected);
    }

    void ReportProgress(const TWorkerId& worker, ui64 offset, const TRowVersion& heartbeat) {
        auto event = MakeHolder<TEvService::TEvIndexBuildProgress>();
        worker.Serialize(*event->Record.MutableWorker());
        auto& progress = *event->Record.MutableProgress();
        progress.SetOffset(offset);
        progress.SetSwitchOffset(1);
        TRowVersion(200, 9).ToProto(progress.MutableMaxVersion());
        heartbeat.ToProto(progress.MutableHeartbeat());
        Send<TEvService::TEvIndexBuildProgressResult>(Info.ControllerId, std::move(event));
    }
};

TWorkerId RegisterSecondWorkerAndCompleteSet(TTestEnv& env, ui64 controllerId, const TWorkerId& first) {
    const TWorkerId second(first.ReplicationId(), first.TargetId(), first.WorkerId() + 1);
    auto run = MakeHolder<TEvService::TEvRunWorker>();
    second.Serialize(*run->Record.MutableWorker());
    env.SendAsync(controllerId, run.Release());
    env.SendAsync(controllerId, new TEvPrivate::TEvCompleteWorkerSet(first.ReplicationId(), first.TargetId()));
    AttachWorkers(env, controllerId, {first, second});
    return second;
}

TEvController::TEvDescribeReplicationResult::TPtr DescribeReplication(TTestEnv& env, const TReplicationTestInfo& info) {
    auto request = MakeHolder<TEvController::TEvDescribeReplication>();
    info.PathId.ToProto(request->Record.MutablePathId());
    return env.Send<TEvController::TEvDescribeReplicationResult>(info.ControllerId, std::move(request));
}

THolder<TEvController::TEvAlterReplication> MakeAlterReplicationRequest(
        const TReplicationTestInfo& info, ui64 txId, bool sourceUnavailable = false)
{
    auto request = MakeHolder<TEvController::TEvAlterReplication>();
    info.PathId.ToProto(request->Record.MutablePathId());
    request->Record.MutableConfig()->CopyFrom(info.Config);
    auto& connection = *request->Record.MutableConfig()->MutableSrcConnectionParams();
    connection.MutableOAuthToken()->SetToken("root@builtin");
    if (sourceUnavailable) {
        connection.SetEndpoint("localhost:1");
    }

    request->Record.MutableOperationId()->SetTxId(txId);
    return request;
}

THolder<TEvController::TEvAlterReplication> MakeDoneRequest(const TReplicationTestInfo& info, ui64 txId) {
    auto request = MakeAlterReplicationRequest(info, txId, true);
    request->Record.MutableSwitchState()->MutableDone()->SetFailoverMode(
        NKikimrReplication::TReplicationState::TDone::FAILOVER_MODE_FORCE);
    return request;
}

void RequestPause(TTestEnv& env, const TReplicationTestInfo& info, ui64 txId) {
    auto request = MakeAlterReplicationRequest(info, txId);
    request->Record.MutableSwitchState()->MutablePaused();

    const auto result = env.Send<TEvController::TEvAlterReplicationResult>(info.ControllerId, std::move(request));
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
}

void RequestConfigUpdate(TTestEnv& env, const TReplicationTestInfo& info, ui64 txId) {
    auto request = MakeAlterReplicationRequest(info, txId);
    request->Record.MutableConfig()->MutableMetricsConfig()->SetLevel(
        NKikimrProto::NMetricsConfig::TMetricsConfig::LEVEL_DETAILED);

    const auto result = env.Send<TEvController::TEvAlterReplicationResult>(info.ControllerId, std::move(request));
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
}

void WaitForPaused(TTestEnv& env, const TReplicationTestInfo& info) {
    UNIT_ASSERT_C(WaitFor([&] {
        return DescribeReplication(env, info)->Get()->Record.GetState().HasPaused();
    }), "Replication did not pause: " << DescribeReplication(env, info)->Get()->Record.GetState());
}

void WaitForColumnCount(TTestEnv& env, const TString& path, ui32 expected) {
    UNIT_ASSERT_C(WaitFor([&] {
        return env.GetDescription(path).GetPathDescription().GetTable().ColumnsSize() == expected;
    }, 50, TDuration::MilliSeconds(20)), "Destination column count differs: " << path);
}

std::optional<bool> ReadStreamSchemaChanges(TTestEnv& env, ui64 controllerId, const TWorkerId& worker) {
    NKikimrMiniKQL::TResult result;
    const auto status = LocalQuery(env.GetRuntime(), controllerId, Sprintf(R"((
        (let key '('('ReplicationId (Uint64 '%lu)) '('TargetId (Uint64 '%lu))))
        (return (AsList (SetResult 'Capability (SelectRow 'SrcStreams key '('SchemaChanges)))))
    ))", worker.ReplicationId(), worker.TargetId()), result);
    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::OK);
    const auto& row = result.GetValue().GetStruct(0).GetOptional();
    UNIT_ASSERT_C(row.HasOptional(), result.DebugString());
    const auto& capability = row.GetOptional().GetStruct(0);
    return capability.HasOptional() ? std::make_optional(capability.GetOptional().GetBool()) : std::nullopt;
}

void ClearStreamSchemaChanges(TTestEnv& env, ui64 controllerId, const TWorkerId& worker) {
    NKikimrMiniKQL::TResult result;
    const auto status = LocalQuery(env.GetRuntime(), controllerId, Sprintf(R"((
        (let key '('('ReplicationId (Uint64 '%lu)) '('TargetId (Uint64 '%lu))))
        (return (AsList (UpdateRow 'SrcStreams key '('('SchemaChanges (Null))))))
    ))", worker.ReplicationId(), worker.TargetId()), result);
    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::OK);
    UNIT_ASSERT(!ReadStreamSchemaChanges(env, controllerId, worker).has_value());
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

    TSchemaAltererTestEnv(const NKikimrReplication::TSchemaChange& schema, ui64 dstAlterTxId,
            bool requireTargetFlush = false, bool globalConsistency = false)
        : Schema(schema)
    {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Parent = Runtime.AllocateEdgeActor();
        Allocator = Runtime.AllocateEdgeActor();
        PipeCache = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(MakeTxProxyID(), Allocator);

        const TSchemaChangeDstAlterSettings settings{
            .TxId = dstAlterTxId,
            .RequireTargetFlush = requireTargetFlush,
            .GlobalConsistency = globalConsistency,
        };
        Alterer = Runtime.Register(CreateSchemaChangeDstAlterer(Parent, DstPathId.OwnerId,
            1, 1, TReplication::ETargetKind::Table, DstPathId, Schema, settings));

        Runtime.GrabEdgeEvent<TEvTxUserProxy::TEvAllocateTxId>(Allocator);
        NTxProxy::TTxProxyServices services;
        services.LeaderPipeCache = PipeCache;
        Runtime.Send(Alterer, Allocator,
            new TEvTxUserProxy::TEvAllocateTxIdResult(dstAlterTxId + 1, services, {}));
    }

    void ReplyMatchingDescription(bool withExtraFamily = false, bool withIndex = false,
            bool withoutExtraColumn = false)
    {
        const auto request = Runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(PipeCache);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->Ev->Type(),
            NSchemeShard::TEvSchemeShard::TEvDescribeScheme::EventType);

        auto description = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
        description->Record.SetStatus(NKikimrScheme::StatusSuccess);
        description->Record.SetPath("/Root/replica1");
        auto* table = description->Record.MutablePathDescription()->MutableTable();
        for (const auto& column : Schema.GetColumns()) {
            if (withoutExtraColumn && column.GetName() == "extra") {
                continue;
            }

            auto* current = table->AddColumns();
            current->SetName(column.GetName());
            current->SetType(column.GetType());
        }

        for (const auto& key : Schema.GetPrimaryKeyColumnNames()) {
            table->AddKeyColumnNames(key);
        }

        if (withExtraFamily) {
            auto* family = table->MutablePartitionConfig()->AddColumnFamilies();
            family->SetId(1);
            family->SetName("manual");
        }

        if (withIndex) {
            auto* index = table->AddTableIndexes();
            index->SetName("by_value");
            index->SetType(NKikimrSchemeOp::EIndexTypeGlobal);
            index->SetState(NKikimrSchemeOp::EIndexStateReady);
            index->AddKeyColumnNames("value");
        }

        Runtime.Send(Alterer, PipeCache, description.Release());
    }

    NKikimrSchemeOp::TModifyScheme GrabAlter() {
        const auto proposal = Runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(PipeCache);
        UNIT_ASSERT_VALUES_EQUAL(proposal->Get()->Ev->Type(),
            NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction::EventType);
        return static_cast<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction*>(
            proposal->Get()->Ev.Get())->Record.GetTransaction(0);
    }

    void CompleteAlter(ui64 txId) {
        Runtime.Send(Alterer, PipeCache, new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult(txId));
    }

    void ExpectUnlink() {
        const auto unlink = Runtime.GrabEdgeEvent<TEvPipeCache::TEvUnlink>(PipeCache);
        UNIT_ASSERT_VALUES_EQUAL(unlink->Get()->TabletId, DstPathId.OwnerId);
    }
};

struct TPendingAttachment {
    TReplicationTestInfo Info;
    ui64 TargetId = 0;
    TPathId DstPathId;
};

TPendingAttachment StartPendingAttachment(TTestEnv& env) {
    env.GetRuntime().GetAppData().ReplicationConfig.SetSkipInitialScan(true);
    CreateSourceTable(env, "replica1");
    const auto info = StartReplication(env);
    UNIT_ASSERT(info.Config.GetSkipInitialScan());

    ui64 targetId = 0;
    bool foundTarget = false;
    for (ui32 attempt = 0; attempt < 50; ++attempt) {
        const auto result = DescribeReplication(env, info);
        if (result->Get()->Record.TargetsSize()) {
            targetId = result->Get()->Record.GetTargets(0).GetId();
            foundTarget = true;
            break;
        }
        Sleep(TDuration::MilliSeconds(100));
    }
    UNIT_ASSERT(foundTarget);

    // Keep this test focused on attachment: source stream setup is independent
    // and may otherwise delay destination progress in the controller.
    env.SendAsync(info.ControllerId, new TEvPrivate::TEvCreateStreamResult(1, targetId,
        NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues())));
    DescribeReplication(env, info);

    env.GetRuntime().Register(CreateDstCreator(
        env.GetSender(), env.GetSchemeshardId("/Root/table1"), env.GetYdbProxy(),
        "/Root", info.PathId, 1, targetId, TReplication::ETargetKind::Table,
        "/Root/table1", "/Root/replica1", EReplicationMode::ReadOnly,
        EConsistencyLevel::Row, true));

    auto prepare = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvPrepareAttachDst>(env.GetSender());
    const auto dstPathId = prepare->Get()->DstPathId;
    env.SendAsync(info.ControllerId,
        new TEvPrivate::TEvPrepareAttachDst(1, targetId, dstPathId));
    env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvPrepareAttachDstResult>(env.GetSender());
    env.GetRuntime().Send(prepare->Sender, env.GetSender(),
        new TEvPrivate::TEvPrepareAttachDstResult());

    const auto attached = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
    UNIT_ASSERT(attached->Get()->IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription()
        .GetTable().GetReplicationConfig().GetMode(),
        NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);

    return {info, targetId, dstPathId};
}

void AssertEventuallyDone(TTestEnv& env, const TReplicationTestInfo& info) {
    UNIT_ASSERT_C(WaitFor([&] {
        return DescribeReplication(env, info)->Get()->Record.GetState().HasDone();
    }, 150), DescribeReplication(env, info)->Get()->Record.GetState().DebugString());
}

void AssertDestinationWritable(TTestEnv& env) {
    UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription()
        .GetTable().GetReplicationConfig().GetMode(),
        NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE);
}

void AssertIndexCancellationCleanedUp(TIndexBuildTestEnv& env) {
    AssertDestinationWritable(env);
    UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription()
        .GetTable().TableIndexesSize(), 0);

    // A new build needs the same per-table snapshot slot. This fails if
    // cancellation removed only the index and leaked its snapshot.
    AddSyncIndex(env.Session, "/Root/replica1", "after_done");
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(AttachmentLifecycle) {
    Y_UNIT_TEST(DoneAfterAttachAlterBeforeResult) {
        TTestEnv env;
        const auto pending = StartPendingAttachment(env);
        const auto& info = pending.Info;
        const auto targetId = pending.TargetId;
        const auto dstPathId = pending.DstPathId;

        auto request = MakeHolder<TEvController::TEvAlterReplication>();
        info.PathId.ToProto(request->Record.MutablePathId());
        request->Record.MutableConfig()->CopyFrom(info.Config);
        request->Record.MutableConfig()->MutableSrcConnectionParams()->MutableOAuthToken()->SetToken("root@builtin");
        request->Record.MutableSwitchState()->MutableDone()->SetFailoverMode(
            NKikimrReplication::TReplicationState::TDone::FAILOVER_MODE_FORCE);
        request->Record.MutableOperationId()->SetTxId(100);

        env.SendAsync(info.ControllerId, std::move(request));
        const auto result = env.GetRuntime().GrabEdgeEvent<TEvController::TEvAlterReplicationResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        UNIT_ASSERT(!DescribeReplication(env, info)->Get()->Record.GetState().HasDone());

        env.SendAsync(info.ControllerId,
            new TEvPrivate::TEvCreateDstResult(1, targetId, dstPathId));
        AssertEventuallyDone(env, info);
        AssertDestinationWritable(env);
    }

    Y_UNIT_TEST(DoneAfterRestartWithoutSource) {
        TTestEnv env;
        const auto pending = StartPendingAttachment(env);
        const auto& info = pending.Info;

        auto request = MakeHolder<TEvController::TEvAlterReplication>();
        info.PathId.ToProto(request->Record.MutablePathId());
        request->Record.MutableConfig()->CopyFrom(info.Config);
        auto* connection = request->Record.MutableConfig()->MutableSrcConnectionParams();
        connection->SetEndpoint("localhost:1");
        connection->MutableOAuthToken()->SetToken("root@builtin");
        request->Record.MutableSwitchState()->MutableDone()->SetFailoverMode(
            NKikimrReplication::TReplicationState::TDone::FAILOVER_MODE_FORCE);
        request->Record.MutableOperationId()->SetTxId(101);

        env.SendAsync(info.ControllerId, std::move(request));
        const auto result = env.GetRuntime().GrabEdgeEvent<TEvController::TEvAlterReplicationResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        UNIT_ASSERT(!DescribeReplication(env, info)->Get()->Record.GetState().HasDone());

        RestartController(env, info.ControllerId);
        AssertEventuallyDone(env, info);
        AssertDestinationWritable(env);
    }

    Y_UNIT_TEST(DropCascadeDuringAttachment) {
        TTestEnv env;
        const auto pending = StartPendingAttachment(env);
        const auto& info = pending.Info;

        auto request = MakeHolder<TEvController::TEvDropReplication>();
        info.PathId.ToProto(request->Record.MutablePathId());
        request->Record.MutableOperationId()->SetTxId(102);
        request->Record.SetCascade(true);

        env.SendAsync(info.ControllerId, std::move(request));
        DescribeReplication(env, info);
        env.SendAsync(info.ControllerId, new TEvPrivate::TEvDropStreamResult(1, pending.TargetId,
            NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues())));
        env.SendAsync(info.ControllerId,
            new TEvPrivate::TEvCreateDstResult(1, pending.TargetId, pending.DstPathId));

        const auto result = env.GetRuntime().GrabEdgeEvent<TEvController::TEvDropReplicationResult>(env.GetSender());
        UNIT_ASSERT(result->Get()->Record.GetStatus()
            == NKikimrReplication::TEvDropReplicationResult::SUCCESS);
        AssertDestinationWritable(env);
    }

    Y_UNIT_TEST(DropWaitsForDelayedAttachAllocation) {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        runtime.GetAppData().ReplicationConfig.SetSkipInitialScan(true);
        CreateSourceTable(env, "replica1");
        const auto info = StartReplication(env);

        ui64 targetId = 0;
        for (ui32 attempt = 0; attempt < 50; ++attempt) {
            const auto result = DescribeReplication(env, info);
            if (result->Get()->Record.TargetsSize()) {
                targetId = result->Get()->Record.GetTargets(0).GetId();
                break;
            }

            Sleep(TDuration::MilliSeconds(100));
        }

        UNIT_ASSERT(targetId);

        env.SendAsync(info.ControllerId, new TEvPrivate::TEvCreateStreamResult(1, targetId,
            NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues())));
        DescribeReplication(env, info);

        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        const auto gate = runtime.Register(new TAttachAllocationGate(txProxy, env.GetSender()));
        runtime.RegisterService(MakeTxProxyID(), gate);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())
            ->Get()->Tag, TAttachAllocationGate::ReadyTag());

        auto resume = MakeHolder<TEvController::TEvAlterReplication>();
        info.PathId.ToProto(resume->Record.MutablePathId());
        resume->Record.MutableConfig()->CopyFrom(info.Config);
        resume->Record.MutableConfig()->MutableSrcConnectionParams()->MutableOAuthToken()->SetToken("root@builtin");
        resume->Record.MutableSwitchState()->MutableStandBy();
        resume->Record.MutableOperationId()->SetTxId(103);
        const auto resumed = env.Send<TEvController::TEvAlterReplicationResult>(info.ControllerId, std::move(resume));
        UNIT_ASSERT_VALUES_EQUAL(resumed->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())
            ->Get()->Tag, TAttachAllocationGate::BlockedTag());

        runtime.RegisterService(MakeTxProxyID(), txProxy);

        auto drop = MakeHolder<TEvController::TEvDropReplication>();
        info.PathId.ToProto(drop->Record.MutablePathId());
        drop->Record.MutableOperationId()->SetTxId(104);
        drop->Record.SetCascade(true);

        env.SendAsync(info.ControllerId, std::move(drop));
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetStatus()
            == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);

        env.SendAsync(info.ControllerId, new TEvPrivate::TEvDropStreamResult(1, targetId,
            NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues())));
        Sleep(TDuration::MilliSeconds(200));
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetStatus()
            == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
        AssertDestinationWritable(env);

        env.SendAsync(gate, new TEvents::TEvWakeup(TAttachAllocationGate::ReleaseTag()));
        const auto dropped = runtime.GrabEdgeEvent<TEvController::TEvDropReplicationResult>(env.GetSender());
        UNIT_ASSERT(dropped->Get()->Record.GetStatus()
            == NKikimrReplication::TEvDropReplicationResult::SUCCESS);
        AssertDestinationWritable(env);
    }
}

Y_UNIT_TEST_SUITE(SchemaChangeBarrier) {
    using namespace NTestHelpers;

    Y_UNIT_TEST(AddSyncIndexCreatesWriteOnlyReplicaAndReconcilesAfterCompletion) {
        const auto schema = MakeSyncIndexSchemaChange();
        TSchemaAltererTestEnv env(schema, 100, true, true);

        env.ReplyMatchingDescription();
        const auto transaction = env.GrabAlter();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(transaction.GetOperationType()),
            static_cast<int>(NKikimrSchemeOp::ESchemeOpCreateIndexBuild));
        UNIT_ASSERT(transaction.GetInternal());
        UNIT_ASSERT(transaction.GetInitiateIndexBuild().GetForReplication());
        UNIT_ASSERT_VALUES_EQUAL(transaction.GetInitiateIndexBuild().GetTable(), "/Root/replica1");
        UNIT_ASSERT_VALUES_EQUAL(transaction.GetInitiateIndexBuild().GetIndex().GetName(), "by_value");

        env.CompleteAlter(100);
        env.ReplyMatchingDescription(false, true);
        const auto result = env.Runtime.GrabEdgeEvent<TEvPrivate::TEvSchemaChangeDstAlterResult>(env.Parent);
        UNIT_ASSERT(result->Get()->IsSuccess());
        UNIT_ASSERT(!result->Get()->RequiresTargetFlush);
        env.ExpectUnlink();
    }

    Y_UNIT_TEST(DropIndexUsesInternalDdlAndReconcilesAfterCompletion) {
        auto schema = MakeSchemaChange(100, 10, 2, false);
        schema.MutableIndexes();
        TSchemaAltererTestEnv env(schema, 100, true);

        env.ReplyMatchingDescription(false, true);
        const auto transaction = env.GrabAlter();
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(transaction.GetOperationType()),
            static_cast<int>(NKikimrSchemeOp::ESchemeOpDropIndex));
        UNIT_ASSERT(transaction.GetInternal());
        UNIT_ASSERT_VALUES_EQUAL(transaction.GetWorkingDir(), "/Root");
        UNIT_ASSERT_VALUES_EQUAL(transaction.GetDropIndex().GetTableName(), "replica1");
        UNIT_ASSERT_VALUES_EQUAL(transaction.GetDropIndex().GetIndexName(), "by_value");

        env.CompleteAlter(100);
        env.ReplyMatchingDescription();
        const auto result = env.Runtime.GrabEdgeEvent<TEvPrivate::TEvSchemaChangeDstAlterResult>(env.Parent);
        UNIT_ASSERT(result->Get()->IsSuccess());
        UNIT_ASSERT(!result->Get()->RequiresTargetFlush);
        env.ExpectUnlink();
    }

    Y_UNIT_TEST(LegacySchemaSnapshotPreservesIndexes) {
        TSchemaAltererTestEnv env(MakeSchemaChange(100, 10, 2, false), 100);

        env.ReplyMatchingDescription(false, true);
        const auto subscription = env.Runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(env.PipeCache);
        UNIT_ASSERT_VALUES_EQUAL(subscription->Get()->Ev->Type(),
            NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion::EventType);

        env.Runtime.Send(env.Alterer, env.Parent, new TEvents::TEvPoison());
        env.ExpectUnlink();
    }

    Y_UNIT_TEST(IndexMetadataPreservesColumnFlushRequirement) {
        auto schema = MakeFamilySchemaChange("ssd");
        schema.MutableIndexes();
        TSchemaAltererTestEnv env(schema, 100, true);

        env.ReplyMatchingDescription();
        const auto result = env.Runtime.GrabEdgeEvent<TEvPrivate::TEvSchemaChangeDstAlterResult>(env.Parent);
        UNIT_ASSERT(result->Get()->IsSuccess());
        UNIT_ASSERT(result->Get()->RequiresTargetFlush);
        env.ExpectUnlink();
    }

    Y_UNIT_TEST(IndexMetadataPreservesInitialIndexFiltering) {
        for (const TString type : {"GlobalAsync", "GlobalSync"}) {
            auto schema = MakeSchemaChange(100, 10, 2, false);
            auto* index = schema.MutableIndexes()->AddItems();
            index->SetName("by_value");
            index->SetType(type);
            index->AddIndexColumns("value");
            TSchemaAltererTestEnv env(schema, 100);
            env.Runtime.GetAppData().FeatureFlags.SetEnableAsyncIndexReplication(false);

            env.ReplyMatchingDescription();
            if (type == "GlobalSync") {
                const auto result = env.Runtime.GrabEdgeEvent<TEvPrivate::TEvSchemaChangeDstAlterResult>(env.Parent);
                UNIT_ASSERT(!result->Get()->IsSuccess());
            } else {
                const auto subscription = env.Runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(env.PipeCache);
                UNIT_ASSERT_VALUES_EQUAL(subscription->Get()->Ev->Type(),
                    NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion::EventType);

                env.CompleteAlter(100);
                env.ReplyMatchingDescription();
                const auto result = env.Runtime.GrabEdgeEvent<TEvPrivate::TEvSchemaChangeDstAlterResult>(env.Parent);
                UNIT_ASSERT(result->Get()->IsSuccess());
            }

            env.ExpectUnlink();
        }
    }

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
        env.CompleteAlter(dstAlterTxId);
        env.ReplyMatchingDescription();

        const auto result = env.Runtime.GrabEdgeEvent<TEvPrivate::TEvSchemaChangeDstAlterResult>(env.Parent);
        UNIT_ASSERT(result->Get()->IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->DstAlterTxId, dstAlterTxId);
        env.ExpectUnlink();
    }

    Y_UNIT_TEST(FreshAlterWaitsForCompletionAfterPipeRetry) {
        constexpr ui64 dstAlterTxId = 1;
        TSchemaAltererTestEnv env(MakeSchemaChange(), 0);

        env.ReplyMatchingDescription(false, false, true);
        const auto save = env.Runtime.GrabEdgeEvent<TEvPrivate::TEvSchemaChangeDstAlterTxId>(env.Parent);
        UNIT_ASSERT_VALUES_EQUAL(save->Get()->TxId, dstAlterTxId);

        env.Runtime.Send(env.Alterer, env.Parent,
            new TEvPrivate::TEvSchemaChangeDstAlterTxIdSaved(dstAlterTxId));
        const auto proposal = env.Runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(env.PipeCache);
        UNIT_ASSERT_VALUES_EQUAL(proposal->Get()->Ev->Type(),
            NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction::EventType);

        // SchemeShard can publish the desired schema before the DDL completes.
        // Losing the proposal response must not release the worker on retry.
        env.Runtime.Send(env.Alterer, env.PipeCache,
            new TEvPipeCache::TEvDeliveryProblem(env.DstPathId.OwnerId, false));
        env.Runtime.Send(env.Alterer, env.Parent, new TEvents::TEvWakeup());
        env.Runtime.GrabEdgeEvent<TEvTxUserProxy::TEvAllocateTxId>(env.Allocator);
        NTxProxy::TTxProxyServices services;
        services.LeaderPipeCache = env.PipeCache;
        env.Runtime.Send(env.Alterer, env.Allocator,
            new TEvTxUserProxy::TEvAllocateTxIdResult(dstAlterTxId + 1, services, {}));
        env.ReplyMatchingDescription();
        const auto subscription = env.Runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(env.PipeCache);
        UNIT_ASSERT_VALUES_EQUAL(subscription->Get()->Ev->Type(),
            NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion::EventType);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion*>(
            subscription->Get()->Ev.Get())->Record.GetTxId(), dstAlterTxId);

        env.CompleteAlter(dstAlterTxId);
        env.ReplyMatchingDescription();
        const auto result = env.Runtime.GrabEdgeEvent<TEvPrivate::TEvSchemaChangeDstAlterResult>(env.Parent);
        UNIT_ASSERT(result->Get()->IsSuccess());
        env.ExpectUnlink();
    }

    Y_UNIT_TEST(PoisonUnlinksDestinationSchemaPipeCache) {
        TSchemaAltererTestEnv env(MakeSchemaChange(), 100);
        env.Runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(env.PipeCache);

        env.Runtime.Send(env.Alterer, env.Parent, new TEvents::TEvPoison());
        env.ExpectUnlink();
    }

    Y_UNIT_TEST(CombinedFamilyCreationAndColumnReassignmentUsesOneAlter) {
        const auto schema = MakeFamilySchemaChange("ssd");
        TSchemaAltererTestEnv env(schema, 100);

        env.ReplyMatchingDescription();

        const auto transaction = env.GrabAlter();
        const auto& alter = transaction.GetAlterTable();
        UNIT_ASSERT_VALUES_EQUAL(alter.ColumnsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(alter.GetColumns(0).GetName(), "value");
        UNIT_ASSERT_VALUES_EQUAL(alter.GetColumns(0).GetFamilyName(), "archive");
        UNIT_ASSERT_VALUES_EQUAL(alter.GetPartitionConfig().ColumnFamiliesSize(), 2);

        env.Runtime.Send(env.Alterer, env.Parent, new TEvents::TEvPoison());
        env.ExpectUnlink();
    }

    Y_UNIT_TEST(ExtraUnusedDestinationFamilyDoesNotBlockAlter) {
        TSchemaAltererTestEnv env(MakeFamilySchemaChange("ssd"), 100);

        env.ReplyMatchingDescription(true);

        const auto transaction = env.GrabAlter();
        UNIT_ASSERT_VALUES_EQUAL(transaction.GetAlterTable().GetPartitionConfig().ColumnFamiliesSize(), 2);

        env.Runtime.Send(env.Alterer, env.Parent, new TEvents::TEvPoison());
        env.ExpectUnlink();
    }

    Y_UNIT_TEST(CombinedFamilyCreationAndReassignmentCompletes) {
        TEnv env;
        const auto info = StartReplication(env);
        const auto controllerId = info.ControllerId;
        const auto run = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());

        AttachWorkers(env, controllerId, {worker});

        const auto schema = MakeFamilySchemaChange("test");

        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema));
        const auto released = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(released->Get()->Record.GetSchema().SerializeAsString(), schema.SerializeAsString());

        const auto description = env.GetDescription("/Root/replica1");
        const auto& table = description.GetPathDescription().GetTable();
        ui32 archiveId = 0;
        for (const auto& family : table.GetPartitionConfig().GetColumnFamilies()) {
            if (family.GetName() == "archive") {
                archiveId = family.GetId();
                UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(family.GetColumnCodec()),
                    static_cast<ui32>(NKikimrSchemeOp::ColumnCodecLZ4));
            }
        }

        UNIT_ASSERT(archiveId);

        bool reassigned = false;
        for (const auto& column : table.GetColumns()) {
            if (column.GetName() == "value") {
                reassigned = column.GetFamily() == archiveId;
            }
        }

        UNIT_ASSERT(reassigned);

        CompleteSchemaChange(env, controllerId, worker, schema);

        auto next = schema;
        next.MutableVersion()->SetStep(200);
        next.MutableVersion()->SetTxId(20);
        next.SetSourceSchemaVersion(3);
        next.MutableFamilies(1)->SetCompression(TFamily::COMPRESSION_OFF);

        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, next));
        const auto nextRelease = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(nextRelease->Get()->Record.GetSchema().SerializeAsString(), next.SerializeAsString());

        const auto updated = env.GetDescription("/Root/replica1");
        const auto& updatedTable = updated.GetPathDescription().GetTable();
        for (const auto& item : updatedTable.GetPartitionConfig().GetColumnFamilies()) {
            if (item.GetName() == "archive") {
                UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(item.GetColumnCodec()),
                    static_cast<ui32>(NKikimrSchemeOp::ColumnCodecPlain));
            }
        }

        CompleteSchemaChange(env, controllerId, worker, next);

        auto withoutMedia = next;
        withoutMedia.MutableVersion()->SetStep(300);
        withoutMedia.MutableVersion()->SetTxId(30);
        withoutMedia.SetSourceSchemaVersion(4);
        withoutMedia.MutableFamilies(1)->ClearMedia();

        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, withoutMedia));
        const auto resetRelease = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(resetRelease->Get()->Record.GetSchema().SerializeAsString(), withoutMedia.SerializeAsString());

        const auto reset = env.GetDescription("/Root/replica1");
        bool mediaReset = false;
        for (const auto& family : reset.GetPathDescription().GetTable().GetPartitionConfig().GetColumnFamilies()) {
            if (family.GetId() == archiveId) {
                mediaReset = family.GetStorageConfig().GetData().GetAllowOtherKinds();
            }
        }

        UNIT_ASSERT(mediaReset);

        CompleteSchemaChange(env, controllerId, worker, withoutMedia);

        auto withColumn = withoutMedia;
        withColumn.MutableVersion()->SetStep(400);
        withColumn.MutableVersion()->SetTxId(40);
        withColumn.SetSourceSchemaVersion(5);
        auto* extra = withColumn.AddColumns();
        extra->SetName("extra");
        extra->SetType("Uint64");
        extra->SetFamily("archive");

        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, withColumn));
        const auto columnRelease = env.GetRuntime().GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(columnRelease->Get()->Record.GetSchema().SerializeAsString(), withColumn.SerializeAsString());

        const auto withColumnDescription = env.GetDescription("/Root/replica1");
        bool added = false;
        for (const auto& column : withColumnDescription.GetPathDescription().GetTable().GetColumns()) {
            if (column.GetName() == "extra") {
                added = column.GetFamily() == archiveId;
            }
        }

        UNIT_ASSERT(added);
    }

    Y_UNIT_TEST(UnavailableFamilyPoolStopsReplication) {
        TEnv env;
        const auto info = StartReplication(env);
        const auto controllerId = info.ControllerId;
        const auto run = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());

        AttachWorkers(env, controllerId, {worker});

        const auto schema = MakeFamilySchemaChange("unavailable_pool_kind");

        env.SendAsync(controllerId, MakeSchemaChangeReport(worker, schema));
        bool failed = false;
        for (ui32 attempt = 0; attempt < 100; ++attempt) {
            const auto state = DescribeReplication(env, info);
            if (state->Get()->Record.GetState().HasError()) {
                const TString issue = state->Get()->Record.GetState().GetError().DebugString();
                UNIT_ASSERT_C(issue.find("unavailable_pool_kind") != TString::npos, issue);
                failed = true;
                break;
            }

            Sleep(TDuration::MilliSeconds(10));
        }

        UNIT_ASSERT(failed);

        const auto description = env.GetDescription("/Root/replica1");
        for (const auto& family : description.GetPathDescription().GetTable().GetPartitionConfig().GetColumnFamilies()) {
            UNIT_ASSERT_VALUES_UNEQUAL(family.GetName(), "archive");
        }
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
        SendWorkerStopped(env, controllerId, first);
        SendWorkerStopped(env, controllerId, second);
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

        SendWorkerStopped(env, controllerId, second);
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
            SendWorkerStopped(env, controllerId, id);
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
            SendWorkerStopped(env, controllerId, id);
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

        SendWorkerStopped(env, controllerId, barrierWorker);
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
            SendWorkerStopped(env, controllerId, id);
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
        SendWorkerStopped(env, controllerId, worker);
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
        SendWorkerStopped(env, controllerId, worker);
        WaitForPaused(env, info);
    }

    void CheckSourceDropIndex(NKikimrSchemeOp::EIndexType type, bool globalConsistency,
            bool schemaChanges = true, bool enableAfterCreation = false)
    {
        TFeatureFlags flags;
        flags.SetEnableChangefeedsOnIndexTables(true);
        flags.SetEnableAsyncIndexReplication(true);
        flags.FeatureFlags.SetEnableAsyncReplicationSchemaChanges(schemaChanges);
        TEnv env(flags);
        const auto info = StartReplication(env, 1, "root@builtin", globalConsistency, true, type, false);
        NYdb::NTable::TTableClient client(env.GetDriver(), NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(env.GetEndpoint())
            .Database(env.GetDatabase()));
        auto session = client.CreateSession().GetValueSync().GetSession();
        const auto upsert = [&](ui32 key) {
            const auto result = session.ExecuteDataQuery(Sprintf(
                "UPSERT INTO `/Root/table1` (key, value) VALUES (%u, 'value');", key),
                NYdb::NTable::TTxControl::BeginTx()
                        .CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        const auto waitForRow = [&](ui32 key) {
            for (ui32 attempt = 0; attempt < 600; ++attempt) {
                const auto result = session.ExecuteDataQuery(Sprintf(
                    "SELECT key FROM `/Root/replica1` WHERE key = %u;", key),
                    NYdb::NTable::TTxControl::BeginTx()
                        .CommitTx()).GetValueSync();
                if (result.IsSuccess() && result.GetResultSet(0).RowsCount() == 1) {
                    return;
                }

                const auto replication = DescribeReplication(env, info);
                UNIT_ASSERT_C(!replication->Get()->Record.GetState().HasError(),
                    replication->Get()->Record.DebugString());

                Sleep(TDuration::MilliSeconds(100));
            }

            UNIT_FAIL("Timed out waiting for replicated row");
        };
        upsert(1);
        waitForRow(1);
        if (enableAfterCreation) {
            env.GetRuntime().GetAppData().FeatureFlags.SetEnableAsyncReplicationSchemaChanges(true);
        }

        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().TableIndexesSize(), 1);

        const auto manualDrop = session.ExecuteSchemeQuery(
            "ALTER TABLE `/Root/replica1` DROP INDEX by_value;").GetValueSync();
        UNIT_ASSERT(!manualDrop.IsSuccess());

        const auto drop = session.ExecuteSchemeQuery(
            "ALTER TABLE `/Root/table1` DROP INDEX by_value;").GetValueSync();
        UNIT_ASSERT_C(drop.IsSuccess(), drop.GetIssues().ToString());
        if (!schemaChanges) {
            for (ui32 attempt = 0; attempt < 600; ++attempt) {
                if (DescribeReplication(env, info)->Get()->Record.GetState().HasError()) {
                    return;
                }

                Sleep(TDuration::MilliSeconds(100));
            }

            UNIT_FAIL("Missing index stream did not produce a terminal error");
        }

        for (ui32 attempt = 0; attempt < 600; ++attempt) {
            if (!env.GetDescription("/Root/replica1").GetPathDescription().GetTable().TableIndexesSize()) {
                upsert(2);
                waitForRow(2);
                UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
                return;
            }

            const auto replication = DescribeReplication(env, info);
            UNIT_ASSERT_C(!replication->Get()->Record.GetState().HasError(), replication->Get()->Record.DebugString());

            Sleep(TDuration::MilliSeconds(100));
        }

        UNIT_FAIL("Timed out waiting for replicated DROP INDEX");
    }

    Y_UNIT_TEST(SourceDropSyncIndexWithGlobalConsistency) {
        CheckSourceDropIndex(NKikimrSchemeOp::EIndexTypeGlobal, true);
    }

    Y_UNIT_TEST(SourceDropAsyncIndexWithRowConsistency) {
        CheckSourceDropIndex(NKikimrSchemeOp::EIndexTypeGlobalAsync, false);
    }

    Y_UNIT_TEST(SourceDropIndexWithoutSchemaChangesReportsError) {
        CheckSourceDropIndex(NKikimrSchemeOp::EIndexTypeGlobal, true, false);
    }

    Y_UNIT_TEST(SourceDropIndexWithSchemaChangesEnabledAfterCreationReportsError) {
        CheckSourceDropIndex(NKikimrSchemeOp::EIndexTypeGlobal, true, false, true);
    }

    Y_UNIT_TEST(IndexRetryPolicySurvivesControllerRestartAndFlagChange) {
        for (const bool schemaChanges : {false, true}) {
            TFeatureFlags flags;
            flags.SetEnableChangefeedsOnIndexTables(true);
            flags.FeatureFlags.SetEnableAsyncReplicationSchemaChanges(schemaChanges);
            TEnv env(flags);
            const auto info = StartReplication(env, 1, "root@builtin", true, true);
            const auto checkWorkers = [&] {
                bool foundIndex = false;
                for (ui32 i = 0; i < 2; ++i) {
                    const auto run = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
                    const auto& reader = run->Get()->Record.GetCommand().GetRemoteTopicReader();
                    const bool isIndex = TString(reader.GetTopicPath()).Contains("/by_value/");
                    foundIndex |= isIndex;
                    UNIT_ASSERT_VALUES_EQUAL(reader.GetRetryOnSchemeError(), isIndex && schemaChanges);
                }

                UNIT_ASSERT(foundIndex);
            };
            checkWorkers();
            env.GetRuntime().GetAppData().FeatureFlags.SetEnableAsyncReplicationSchemaChanges(!schemaChanges);

            RestartController(env, info.ControllerId);
            env.SendAsync(info.ControllerId, new TEvService::TEvStatus());
            checkWorkers();
        }
    }

    Y_UNIT_TEST(DiscoversLegacyStreamCapabilityBeforeBootingIndexWorkers) {
        for (const bool schemaChanges : {false, true}) {
            TFeatureFlags flags;
            flags.SetEnableChangefeedsOnIndexTables(true);
            flags.FeatureFlags.SetEnableAsyncReplicationSchemaChanges(schemaChanges);
            TEnv env(flags);
            const auto info = StartReplication(env, 1, "root@builtin", true, true);
            const auto checkWorkers = [&] {
                std::optional<TWorkerId> base;
                bool foundIndex = false;
                for (ui32 i = 0; i < 2; ++i) {
                    const auto run = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
                    const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());
                    const auto& reader = run->Get()->Record.GetCommand().GetRemoteTopicReader();
                    const bool isIndex = TString(reader.GetTopicPath()).Contains("/by_value/");
                    foundIndex |= isIndex;
                    if (!isIndex) {
                        base = worker;
                    }

                    UNIT_ASSERT_VALUES_EQUAL(reader.GetRetryOnSchemeError(), isIndex && schemaChanges);
                    if (isIndex && base) {
                        // Discovery must be durable before an index starts.
                        UNIT_ASSERT(ReadStreamSchemaChanges(env, info.ControllerId, *base) == schemaChanges);
                    }
                }

                UNIT_ASSERT(base && foundIndex);
                return *base;
            };
            const auto base = checkWorkers();
            UNIT_ASSERT(ReadStreamSchemaChanges(env, info.ControllerId, base) == schemaChanges);

            ClearStreamSchemaChanges(env, info.ControllerId, base);
            // The current flag deliberately disagrees with the actual stream.
            env.GetRuntime().GetAppData().FeatureFlags.SetEnableAsyncReplicationSchemaChanges(!schemaChanges);

            RestartController(env, info.ControllerId);
            env.SendAsync(info.ControllerId, new TEvService::TEvStatus());
            UNIT_ASSERT_VALUES_EQUAL(checkWorkers(), base);
            UNIT_ASSERT(ReadStreamSchemaChanges(env, info.ControllerId, base) == schemaChanges);

            // The discovered value survives another restart as a known value.
            RestartController(env, info.ControllerId);
            env.SendAsync(info.ControllerId, new TEvService::TEvStatus());
            UNIT_ASSERT_VALUES_EQUAL(checkWorkers(), base);
            UNIT_ASSERT(ReadStreamSchemaChanges(env, info.ControllerId, base) == schemaChanges);
        }
    }

    Y_UNIT_TEST(LegacyStreamCapabilityDiscoveryFailureReportsError) {
        TEnv env(MakeIndexReplicationFlags());
        const auto info = StartReplication(env, 1, "root@builtin", true, true);
        std::optional<TWorkerId> base;
        for (ui32 i = 0; i < 2; ++i) {
            const auto run = env.GetRuntime().GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
            if (!TString(run->Get()->Record.GetCommand().GetRemoteTopicReader().GetTopicPath()).Contains("/by_value/")) {
                base = TWorkerId::Parse(run->Get()->Record.GetWorker());
            }
        }

        UNIT_ASSERT(base);

        ClearStreamSchemaChanges(env, info.ControllerId, *base);
        NYdb::NTable::TTableClient client(env.GetDriver(), NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(env.GetEndpoint())
            .Database(env.GetDatabase()));
        auto session = client.CreateSession().GetValueSync().GetSession();
        const auto drop = session.ExecuteSchemeQuery("DROP TABLE `/Root/table1`;").GetValueSync();
        UNIT_ASSERT_C(drop.IsSuccess(), drop.GetIssues().ToString());

        RestartController(env, info.ControllerId);
        for (ui32 attempt = 0; attempt < 50; ++attempt) {
            if (DescribeReplication(env, info)->Get()->Record.GetState().HasError()) {
                return;
            }

            Sleep(TDuration::MilliSeconds(100));
        }

        UNIT_FAIL("Failed capability discovery did not report an error");
    }

    Y_UNIT_TEST(DropIndexDuringGlobalCommitRetriesOnlyRemainingTargets) {
        TEnv env(MakeIndexReplicationFlags());
        const auto info = StartReplication(env, 1, "root@builtin", true, true);
        auto& runtime = env.GetRuntime();
        const auto [base, index] = AttachBaseAndIndexWorkers(env, info);
        env.SendAsync(info.ControllerId, new TEvPrivate::TEvCompleteWorkerSet(base.ReplicationId(), base.TargetId()));
        DescribeReplication(env, info);

        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        const auto recorder = runtime.Register(new TCommitWritesRecorder(txProxy, env.GetSender()));
        runtime.RegisterService(MakeTxProxyID(), recorder);
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        const auto assigned = env.Send<TEvService::TEvTxIdResult>(info.ControllerId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(5000, 0)}));
        const auto writeTxId = assigned->Get()->Record.GetVersionTxIds(0).GetTxId();

        SendHeartbeat(env, info.ControllerId, base, TRowVersion(10000, 0));
        SendHeartbeat(env, info.ControllerId, index, TRowVersion(10000, 0));
        const auto commit = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(commit->Get()->Record.GetTransaction().GetCommitWrites().TablesSize(), 2);

        auto schema = MakeSchemaChange(11000, 10, 2, false);
        schema.MutableIndexes();

        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(base, schema));
        const auto stop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), index);

        // Leave the index worker registered: exclusion must not depend on its
        // stop acknowledgement, and DDL must not wait for this active commit.
        const auto release = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(release->Get()->Record.GetWorker()), base);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().TableIndexesSize(), 0);

        auto failure = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>();
        failure->Record.SetStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError);

        runtime.Send(new IEventHandle(commit->Sender, env.GetSender(), failure.Release(), 0, writeTxId));
        const auto retry = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender());
        const auto& retried = retry->Get()->Record.GetTransaction().GetCommitWrites();
        UNIT_ASSERT_VALUES_EQUAL(retried.GetWriteTxId(), writeTxId);
        UNIT_ASSERT_VALUES_EQUAL(retried.TablesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(retried.GetTables(0).GetTablePath(), "/Root/replica1");

        auto success = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>();
        success->Record.SetStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete);

        runtime.Send(new IEventHandle(retry->Sender, env.GetSender(), success.Release(), 0, writeTxId));
        CompleteSchemaChange(env, info.ControllerId, base, schema);
        SendHeartbeat(env, info.ControllerId, base, TRowVersion(20000, 0));
        // A late heartbeat from the stopped index must not rejoin the quorum.
        SendHeartbeat(env, info.ControllerId, index, TRowVersion(1, 0));
        DescribeReplication(env, info);
        const auto next = env.Send<TEvService::TEvTxIdResult>(info.ControllerId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(15000, 0)}));
        SendHeartbeat(env, info.ControllerId, base, TRowVersion(30000, 0));
        const auto nextCommit = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(nextCommit->Get()->Record.GetTransaction().GetCommitWrites().GetWriteTxId(),
            next->Get()->Record.GetVersionTxIds(0).GetTxId());
        UNIT_ASSERT_VALUES_EQUAL(nextCommit->Get()->Record.GetTransaction().GetCommitWrites().TablesSize(), 1);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        runtime.RegisterService(MakeTxProxyID(), txProxy);
    }

    void CheckLegacyIndexMetadataBarrier(bool emptyIndexes, bool legacyFirst) {
        TEnv env(MakeIndexReplicationFlags());
        const auto info = StartReplication(env, 1, "root@builtin", true, true);
        auto& runtime = env.GetRuntime();
        const auto [base, index] = AttachBaseAndIndexWorkers(env, info);
        const auto other = RegisterSecondWorkerAndCompleteSet(env, info.ControllerId, base);
        auto schema = MakeSchemaChange(100, 10, 2, !emptyIndexes);
        schema.MutableIndexes();
        if (!emptyIndexes) {
            auto* retained = schema.MutableIndexes()->AddItems();
            retained->SetName("by_value");
            retained->SetType("GlobalSync");
            retained->AddIndexColumns("value");
        }

        auto legacy = schema;
        legacy.ClearIndexes();

        // Persist the same bytes that the previous parser produced, then
        // recover a collecting barrier before the upgraded worker reports.
        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(base, legacyFirst ? legacy : schema));
        if (emptyIndexes && !legacyFirst) {
            runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        }

        DescribeReplication(env, info);
        RestartController(env, info.ControllerId);
        if (legacyFirst || !emptyIndexes) {
            AttachWorkers(env, info.ControllerId, {base, other, index});
        } else {
            AttachWorkers(env, info.ControllerId, {base, other});
        }

        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(other, legacyFirst ? schema : legacy));
        if (emptyIndexes && legacyFirst) {
            runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        }

        for (ui32 i = 0; i < 2; ++i) {
            const auto release = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
            const bool isBase = TWorkerId::Parse(release->Get()->Record.GetWorker()) == base;
            UNIT_ASSERT_VALUES_EQUAL(release->Get()->Record.GetSchema().HasIndexes(), isBase != legacyFirst);
        }

        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().TableIndexesSize(), !emptyIndexes);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().ColumnsSize(), emptyIndexes ? 2 : 3);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(base, legacyFirst ? legacy : schema, true));
        runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        RestartController(env, info.ControllerId);
        AttachWorkers(env, info.ControllerId, {base, other});
        const auto recovered = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(recovered->Get()->Record.GetApplied());
        UNIT_ASSERT_VALUES_EQUAL(recovered->Get()->Record.GetSchema().HasIndexes(), !legacyFirst);

        CompleteSchemaChange(env, info.ControllerId, other, legacyFirst ? schema : legacy);
        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(base, legacyFirst ? legacy : schema, false, true));
        const auto completed = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(completed->Get()->Record.GetCompleted());
    }

    Y_UNIT_TEST(RecoversLegacyBarrierWithEmptyIndexMetadata) {
        CheckLegacyIndexMetadataBarrier(true, true);
    }

    Y_UNIT_TEST(RecoversLegacyBarrierWithRetainedIndexMetadata) {
        CheckLegacyIndexMetadataBarrier(false, true);
    }

    Y_UNIT_TEST(MixedWorkersAcceptLegacyReportAfterIndexMetadata) {
        CheckLegacyIndexMetadataBarrier(true, false);
    }

    Y_UNIT_TEST(RecoversEstablishedLegacyDropIndexBarrier) {
        TEnv env(MakeIndexReplicationFlags());
        const auto info = StartReplication(env, 1, "root@builtin", true, true);
        auto& runtime = env.GetRuntime();
        const auto [base, index] = AttachBaseAndIndexWorkers(env, info);
        env.SendAsync(info.ControllerId, new TEvPrivate::TEvCompleteWorkerSet(base.ReplicationId(), base.TargetId()));
        DescribeReplication(env, info);

        const auto pipeService = MakePipePerNodeCacheID(false);
        const auto pipeCache = runtime.GetLocalServiceId(pipeService);
        const auto gate = runtime.Register(new TSchemaDescribeGate(
            pipeCache, env.GetSender(), env.GetPathId("/Root/replica1")));
        runtime.RegisterService(pipeService, gate);
        auto schema = MakeSchemaChange(100, 10, 2, false);
        schema.MutableIndexes();
        auto legacy = schema;
        legacy.ClearIndexes();

        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(base, legacy));
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TSchemaDescribeGate::Blocked);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().TableIndexesSize(), 1);

        // Recover after collection with a persisted legacy Altering barrier.
        RestartController(env, info.ControllerId);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TSchemaDescribeGate::Blocked);

        AttachWorkers(env, info.ControllerId, {base, index});
        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(base, schema));
        const auto stop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), index);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TSchemaDescribeGate::Blocked);

        runtime.RegisterService(pipeService, pipeCache);
        env.SendAsync(gate, new TEvents::TEvWakeup(TSchemaDescribeGate::Release));
        const auto release = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(release->Get()->Record.GetWorker()), base);
        UNIT_ASSERT(release->Get()->Record.GetSchema().HasIndexes());
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().TableIndexesSize(), 0);

        CompleteSchemaChange(env, info.ControllerId, base, schema);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
    }

    Y_UNIT_TEST(RecoversEstablishedLegacyAsyncIndexDrop) {
        TFeatureFlags flags;
        flags.SetEnableChangefeedsOnIndexTables(true);
        flags.SetEnableAsyncIndexReplication(true);
        flags.FeatureFlags.SetEnableAsyncReplicationSchemaChanges(true);
        TEnv env(flags);
        const auto info = StartReplication(env, 1, "root@builtin", false, true,
            NKikimrSchemeOp::EIndexTypeGlobalAsync);
        auto& runtime = env.GetRuntime();
        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());

        AttachWorkers(env, info.ControllerId, {worker});
        env.SendAsync(info.ControllerId, new TEvPrivate::TEvCompleteWorkerSet(worker.ReplicationId(), worker.TargetId()));
        DescribeReplication(env, info);

        const auto pipeService = MakePipePerNodeCacheID(false);
        const auto pipeCache = runtime.GetLocalServiceId(pipeService);
        const auto gate = runtime.Register(new TSchemaDescribeGate(
            pipeCache, env.GetSender(), env.GetPathId("/Root/replica1")));
        runtime.RegisterService(pipeService, gate);
        auto schema = MakeSchemaChange(100, 10, 2, false);
        schema.MutableIndexes();
        auto legacy = schema;
        legacy.ClearIndexes();

        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(worker, legacy));
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TSchemaDescribeGate::Blocked);

        RestartController(env, info.ControllerId);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TSchemaDescribeGate::Blocked);

        AttachWorkers(env, info.ControllerId, {worker});
        // There is no IndexTable target to remove for an async index.
        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(worker, schema));
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TSchemaDescribeGate::Blocked);

        runtime.RegisterService(pipeService, pipeCache);
        env.SendAsync(gate, new TEvents::TEvWakeup(TSchemaDescribeGate::Release));
        const auto release = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(release->Get()->Record.GetWorker()), worker);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().TableIndexesSize(), 0);

        CompleteSchemaChange(env, info.ControllerId, worker, schema);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
    }

    Y_UNIT_TEST(EstablishedLegacyAsyncIndexMetadataAfterApply) {
        for (const bool dropIndex : {false, true}) {
            TFeatureFlags flags;
            flags.SetEnableChangefeedsOnIndexTables(true);
            flags.SetEnableAsyncIndexReplication(true);
            flags.FeatureFlags.SetEnableAsyncReplicationSchemaChanges(true);
            TEnv env(flags);
            const auto info = StartReplication(env, 1, "root@builtin", false, true,
                NKikimrSchemeOp::EIndexTypeGlobalAsync);
            auto& runtime = env.GetRuntime();
            const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
            const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());

            AttachWorkers(env, info.ControllerId, {worker});
            env.SendAsync(info.ControllerId,
                new TEvPrivate::TEvCompleteWorkerSet(worker.ReplicationId(), worker.TargetId()));
            DescribeReplication(env, info);

            auto schema = MakeSchemaChange();
            schema.MutableIndexes();
            if (!dropIndex) {
                auto* retained = schema.MutableIndexes()->AddItems();
                retained->SetName("by_value");
                retained->SetType("GlobalAsync");
                retained->AddIndexColumns("value");
            }

            auto legacy = schema;
            legacy.ClearIndexes();

            env.SendAsync(info.ControllerId, MakeSchemaChangeReport(worker, legacy));
            runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
            WaitForColumnCount(env, "/Root/replica1", 3);
            UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1")
                .GetPathDescription().GetTable().TableIndexesSize(), 1);

            const auto pipeService = MakePipePerNodeCacheID(false);
            const auto pipeCache = runtime.GetLocalServiceId(pipeService);
            TActorId gate;
            if (!dropIndex) {
                gate = runtime.Register(new TSchemaDescribeGate(pipeCache, env.GetSender(),
                    env.GetPathId("/Root/replica1")));
                runtime.RegisterService(pipeService, gate);
            }

            env.SendAsync(info.ControllerId, MakeSchemaChangeReport(worker, schema));
            if (!dropIndex) {
                UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
                    TSchemaDescribeGate::Blocked);
                NKikimrMiniKQL::TResult result;
                UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, info.ControllerId, Sprintf(R"((
                    (let key '('('ReplicationId (Uint64 '%lu)) '('Id (Uint64 '%lu))))
                    (return (AsList (SetResult 'Barrier (SelectRow 'Targets key '('DstAlterTxId)))))
                ))", worker.ReplicationId(), worker.TargetId()), result), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(result.GetValue().GetStruct(0).GetOptional().GetOptional()
                    .GetStruct(0).GetOptional().GetUint64(), 0);

                RestartController(env, info.ControllerId);
                UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
                    TSchemaDescribeGate::Blocked);

                AttachWorkers(env, info.ControllerId, {worker});
                runtime.RegisterService(pipeService, pipeCache);
                env.SendAsync(gate, new TEvents::TEvWakeup(TSchemaDescribeGate::Release));
            }

            const auto release = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
            UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(release->Get()->Record.GetWorker()), worker);
            UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1")
                .GetPathDescription().GetTable().TableIndexesSize(), dropIndex ? 0 : 1);

            CompleteSchemaChange(env, info.ControllerId, worker, schema);
            UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
        }
    }

    Y_UNIT_TEST(CompletedWorkersSurviveEstablishedDropReconciliation) {
        TEnv env(MakeIndexReplicationFlags());
        const auto info = StartReplication(env, 1, "root@builtin", false, true);
        auto& runtime = env.GetRuntime();
        const auto [base, index] = AttachBaseAndIndexWorkers(env, info);
        const auto other = RegisterSecondWorkerAndCompleteSet(env, info.ControllerId, base);

        auto schema = MakeSchemaChange();
        schema.MutableIndexes();
        auto legacy = schema;
        legacy.ClearIndexes();

        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(base, legacy));
        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(other, legacy));
        for (ui32 i = 0; i < 2; ++i) {
            runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        }

        WaitForColumnCount(env, "/Root/replica1", 3);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().TableIndexesSize(), 1);

        // One partition has discarded the barrier. The other has crossed its
        // durable topic offset and can now replay only Completed, not Applied.
        CompleteSchemaChange(env, info.ControllerId, base, legacy);
        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(other, legacy, true));
        const auto applied = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(applied->Get()->Record.GetApplied());

        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(other, schema, false, true));
        const auto stop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), index);
        for (ui32 i = 0; i < 2; ++i) {
            const auto release = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
            UNIT_ASSERT(!release->Get()->Record.GetApplied());
            UNIT_ASSERT(!release->Get()->Record.GetCompleted());
        }

        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().TableIndexesSize(), 0);

        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(other, schema, false, true));
        const auto completed = runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT(completed->Get()->Record.GetCompleted());

        auto next = MakeSchemaChange(200, 10, 3);
        next.MutableIndexes();

        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(base, next));
        DescribeReplication(env, info);
        NKikimrMiniKQL::TResult result;
        UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, info.ControllerId, Sprintf(R"((
            (let key '('('ReplicationId (Uint64 '%lu)) '('Id (Uint64 '%lu))))
            (return (AsList (SetResult 'Barrier (SelectRow 'Targets key '('SchemaBarrierPhase)))))
        ))", base.ReplicationId(), base.TargetId()), result), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result.GetValue().GetStruct(0).GetOptional().GetOptional()
            .GetStruct(0).GetOptional().GetUint32(), 1); // Collecting a newer barrier
    }

    Y_UNIT_TEST(DropIndexDecisionSurvivesRestartWhileCollectingReports) {
        TEnv env(MakeIndexReplicationFlags());

        const auto info = StartReplication(env, 1, "root@builtin", true, true);
        auto& runtime = env.GetRuntime();
        const auto [base, index] = AttachBaseAndIndexWorkers(env, info);
        const auto other = RegisterSecondWorkerAndCompleteSet(env, info.ControllerId, base);
        auto schema = MakeSchemaChange(100, 10, 2, false);
        schema.MutableIndexes();

        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(base, schema));
        runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        DescribeReplication(env, info);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().TableIndexesSize(), 1);

        RestartController(env, info.ControllerId);
        AttachWorkers(env, info.ControllerId, {base, other});
        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(other, schema));
        runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription().GetTable().TableIndexesSize(), 0);

        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        runtime.RegisterService(MakeTxProxyID(), runtime.Register(new TCommitWritesRecorder(txProxy, env.GetSender())));
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        env.Send<TEvService::TEvTxIdResult>(info.ControllerId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(5000, 0)}));
        SendHeartbeat(env, info.ControllerId, base, TRowVersion(10000, 0));
        SendHeartbeat(env, info.ControllerId, other, TRowVersion(10000, 0));
        const auto commit = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(commit->Get()->Record.GetTransaction().GetCommitWrites().TablesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(commit->Get()->Record.GetTransaction().GetCommitWrites().GetTables(0).GetTablePath(), "/Root/replica1");

        runtime.RegisterService(MakeTxProxyID(), txProxy);
    }

    Y_UNIT_TEST(LastGlobalCommitCompletesBeforeTargetFlushAndRestart) {
        TEnv env;
        const auto info = StartReplication(env, 1, "root@builtin", true);
        auto& runtime = env.GetRuntime();
        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());

        AttachWorkers(env, info.ControllerId, {worker});
        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        runtime.RegisterService(MakeTxProxyID(), runtime.Register(new TCommitWritesRecorder(txProxy, env.GetSender())));
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        const auto assigned = env.Send<TEvService::TEvTxIdResult>(info.ControllerId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(5000, 0)}));
        const auto writeTxId = assigned->Get()->Record.GetVersionTxIds(0).GetTxId();

        SendHeartbeat(env, info.ControllerId, worker, TRowVersion(10000, 0));
        const auto commit = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender());

        const auto pipeService = MakePipePerNodeCacheID(false);
        const auto pipeCache = runtime.GetLocalServiceId(pipeService);
        const auto gate = runtime.Register(new TSchemaDescribeGate(
            pipeCache, env.GetSender(), env.GetPathId("/Root/replica1")));
        runtime.RegisterService(pipeService, gate);
        auto schema = MakeSchemaChange(11000, 10, 2);
        schema.MutableIndexes();

        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(worker, schema));
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TSchemaDescribeGate::Blocked);

        auto success = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>();
        success->Record.SetStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete);

        runtime.Send(new IEventHandle(commit->Sender, env.GetSender(), success.Release(), 0, writeTxId));
        DescribeReplication(env, info);
        env.SendAsync(gate, new TEvents::TEvWakeup(TSchemaDescribeGate::Release));
        // The preflight requested a flush, but the last assignment is gone.
        // Stop its replacement before DDL to inspect and recover that state.
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TSchemaDescribeGate::Blocked);
        NKikimrMiniKQL::TResult result;
        UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, info.ControllerId, Sprintf(R"((
            (let key '('('ReplicationId (Uint64 '%lu)) '('Id (Uint64 '%lu))))
            (return (AsList (SetResult 'Barrier (SelectRow 'Targets key '('SchemaBarrierPhase)))))
        ))", worker.ReplicationId(), worker.TargetId()), result), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result.GetValue().GetStruct(0).GetOptional().GetOptional()
            .GetStruct(0).GetOptional().GetUint32(), 2); // Altering
        RestartController(env, info.ControllerId);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TSchemaDescribeGate::Blocked);

        AttachWorkers(env, info.ControllerId, {worker});
        runtime.RegisterService(pipeService, pipeCache);
        runtime.RegisterService(MakeTxProxyID(), txProxy);
        env.SendAsync(gate, new TEvents::TEvWakeup(TSchemaDescribeGate::Release));
        runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        WaitForColumnCount(env, "/Root/replica1", 3);
        CompleteSchemaChange(env, info.ControllerId, worker, schema);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
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
            SendWorkerStopped(env, controllerId, worker);
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
        SendWorkerStopped(env, controllerId, worker);
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

        SendWorkerStopped(env, controllerId, worker);

        const auto replacement = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(replacement->Get()->Record.GetWorker()), worker);

        AttachWorkers(env, controllerId, {worker});
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        SendHeartbeat(env, controllerId, worker, TRowVersion(101, 0));
        const auto finalStop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(finalStop->Get()->Record.GetWorker()), worker);

        SendWorkerStopped(env, controllerId, worker);
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

        SendWorkerStopped(env, controllerId, siblingWorker);

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
            SendWorkerStopped(env, controllerId, worker);
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

        SendWorkerStopped(env, controllerId, other);
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
            SendWorkerStopped(env, controllerId, worker);
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

        SendWorkerStopped(env, controllerId, siblingWorker);
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
            SendWorkerStopped(env, controllerId, worker);
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

        SendWorkerStopped(env, controllerId, siblingWorker);

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
            SendWorkerStopped(env, controllerId, worker);
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


Y_UNIT_TEST_SUITE(IndexBuild) {
    using namespace NTestHelpers;

    void CheckSyncIndexJoinBoundaryAndScopedAssignmentsSurviveRestart(bool committedBase) {
        TIndexBuildTestEnv env;
        const auto& info = env.Info;
        const auto& base = env.Base;
        auto& runtime = env.GetRuntime();
        const auto global = env.Send<TEvService::TEvTxIdResult>(info.ControllerId,
            new TEvService::TEvGetTxId(TVector<TRowVersion>{TRowVersion(committedBase ? 95000 : 50, 0)}));
        const auto globalId = global->Get()->Record.GetVersionTxIds(0).GetTxId();
        if (committedBase) {
            SendHeartbeat(env, info.ControllerId, base, TRowVersion(100000, 0));
            // Wait for the real commit and durable frontier, then restart
            // while the base is idle and every assignment has been erased.
            const bool committed = WaitFor([&] {
                NKikimrMiniKQL::TResult result;
                UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, info.ControllerId, R"((
                    (return (AsList (SetResult 'Frontier
                        (SelectRow 'Replications '('('Id (Uint64 '1))) '('CommittedStep)))))
                ))", result), NKikimrProto::OK);

                const auto& value = result.GetValue().GetStruct(0).GetOptional().GetOptional().GetStruct(0);
                return value.HasOptional() && value.GetOptional().GetUint64() == 100000;
            }, 100, TDuration::MilliSeconds(20));
            UNIT_ASSERT(committed);

            RestartController(env, info.ControllerId);
            AttachWorkers(env, info.ControllerId, {base});
        }

        const auto worker = env.StartIndexBuild();
        const auto request = [&](TRowVersion version) {
            auto event = MakeHolder<TEvService::TEvGetTxId>(TVector<TRowVersion>{version});
            worker.Serialize(*event->Record.MutableWorker());
            return env.Send<TEvService::TEvTxIdResult>(info.ControllerId, std::move(event));
        };

        env.ReportProgress(worker, 1, TRowVersion(200, 8));
        env.AssertBuildPhase(worker, NKikimrReplication::TIndexBuildState::FILLING);

        const auto local = request(TRowVersion(201, 0));
        const auto localId = local->Get()->Record.GetVersionTxIds(0).GetTxId();
        UNIT_ASSERT(localId != globalId);
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(local->Get()->Record.GetVersionTxIds(0).GetVersion()),
            TRowVersion(10000, 0));

        // Step equality is insufficient: Hjoin must be strictly above M,
        // including its TxId component.
        env.ReportProgress(worker, 2, TRowVersion(200, 9));
        env.AssertBuildPhase(worker, NKikimrReplication::TIndexBuildState::FILLING);

        env.ReportProgress(worker, 3, TRowVersion(200, 10));
        const auto before = ReadIndexBuild(env, info.ControllerId, worker);
        UNIT_ASSERT(before.GetPhase() == NKikimrReplication::TIndexBuildState::JOINING);

        const auto join = TRowVersion::FromProto(before.GetJoinVersion());
        UNIT_ASSERT(join > TRowVersion(committedBase ? 100000 : 10000, 0));

        const auto shared = request(join);
        const auto sharedId = shared->Get()->Record.GetVersionTxIds(0).GetTxId();
        UNIT_ASSERT(sharedId != localId);
        UNIT_ASSERT_VALUES_EQUAL(TRowVersion::FromProto(shared->Get()->Record.GetVersionTxIds(0).GetBegin()), join);

        RestartController(env, info.ControllerId);
        AttachWorkers(env, info.ControllerId, {base, worker});
        const auto after = ReadIndexBuild(env, info.ControllerId, worker);
        UNIT_ASSERT_VALUES_EQUAL(after.SerializeAsString(), before.SerializeAsString());

        const auto recoveredLocal = request(TRowVersion(201, 0));
        UNIT_ASSERT_VALUES_EQUAL(recoveredLocal->Get()->Record.GetVersionTxIds(0).GetTxId(), localId);

        const auto recoveredShared = request(join);
        UNIT_ASSERT_VALUES_EQUAL(recoveredShared->Get()->Record.GetVersionTxIds(0).GetTxId(), sharedId);
        env.AssertIndexWriteOnly();
    }

    Y_UNIT_TEST(SyncIndexJoinBoundaryAndScopedAssignmentsSurviveRestart) {
        CheckSyncIndexJoinBoundaryAndScopedAssignmentsSurviveRestart(false);
    }

    Y_UNIT_TEST(SyncIndexJoinBeyondCompletedGlobalFrontierAfterRestart) {
        CheckSyncIndexJoinBoundaryAndScopedAssignmentsSurviveRestart(true);
    }

    enum class EIndexCancellationScenario {
        Normal,
        LateWorkerError,
        Interrupted,
    };

    struct TIndexCancellationSettings {
        bool Joining = false;
        bool Restart = false;
        bool Paused = false;
        EIndexCancellationScenario Scenario = EIndexCancellationScenario::Normal;
    };

    void CheckDoneWithUnfinishedSyncIndex(const TIndexCancellationSettings& settings) {
        TIndexBuildTestEnv env;
        const auto& info = env.Info;
        const auto& base = env.Base;
        auto& runtime = env.GetRuntime();
        const auto worker = env.StartIndexBuild();
        if (settings.Joining) {
            env.ReportProgress(worker, 1, TRowVersion(200, 10));
            env.AssertBuildPhase(worker, NKikimrReplication::TIndexBuildState::JOINING);
        }

        // Verification completes without an index-build heartbeat quorum.
        env.CompleteBarrier();
        env.AssertIndexWriteOnly();
        if (settings.Paused) {
            RequestPause(env, info, 1004);
            for (ui32 i = 0; i < 2; ++i) {
                const auto stop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
                SendWorkerStopped(env, info.ControllerId,
                    TWorkerId::Parse(stop->Get()->Record.GetWorker()));
            }

            WaitForPaused(env, info);
        }

        TActorId removalGate;
        if (settings.Scenario != EIndexCancellationScenario::Normal) {
            removalGate = InstallWorkerRemovalGate(env, info.ControllerId, worker);
        }

        auto request = MakeDoneRequest(info, 1005);

        env.SendAsync(info.ControllerId, std::move(request));
        const auto result = runtime.GrabEdgeEvent<TEvController::TEvAlterReplicationResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        if (!settings.Paused) {
            if (removalGate) {
                UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
                    TWorkerRemovalGate::Blocked);
            } else {
                const auto stopIndex = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
                UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stopIndex->Get()->Record.GetWorker()), worker);
            }

            UNIT_ASSERT(!DescribeReplication(env, info)->Get()->Record.GetState().HasDone());
            env.AssertBuildPhase(worker, NKikimrReplication::TIndexBuildState::CANCELLING);
            if (settings.Scenario == EIndexCancellationScenario::LateWorkerError) {
                env.SendAsync(info.ControllerId, new TEvService::TEvWorkerStatus(worker,
                    NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
                    NKikimrReplication::TEvWorkerStatus::REASON_ERROR, "cancelled reader failed"));
                const auto state = DescribeReplication(env, info)->Get()->Record.GetState();
                UNIT_ASSERT_C(state.HasStandBy(), state.DebugString());
                // The error itself confirms worker shutdown. Keep removal
                // requests blocked until the base is allowed to detach.
            } else if (settings.Scenario == EIndexCancellationScenario::Interrupted) {
                // Emulate the durable state left by the old error handler.
                // Recovery must repair it after restart and an explicit DONE retry.
                NKikimrMiniKQL::TResult result;
                UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, info.ControllerId, Sprintf(R"((
                    (let targetKey '('('ReplicationId (Uint64 '%lu)) '('Id (Uint64 '%lu))))
                    (let replicationKey '('('Id (Uint64 '%lu))))
                    (return (AsList
                        (UpdateRow 'Targets targetKey '('('DstState (Uint8 '255))))
                        (UpdateRow 'Replications replicationKey '('('State (Uint8 '255))))))
                ))", worker.ReplicationId(), worker.TargetId(), worker.ReplicationId()), result), NKikimrProto::OK);
            }

            if (settings.Restart) {
                RestartController(env, info.ControllerId);
                // The detached index worker can now be removed; the base must
                // remain in replica mode until cancellation is confirmed.
                AttachWorkers(env, info.ControllerId, {base});
                if (settings.Scenario == EIndexCancellationScenario::Interrupted) {
                    UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());

                    env.AlterReplication(MakeDoneRequest(info, 1006));
                }
            } else if (!removalGate) {
                SendWorkerStopped(env, info.ControllerId, worker);
            }

            const auto stopBase = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
            UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stopBase->Get()->Record.GetWorker()), base);
            UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription()
                .GetTable().TableIndexesSize(), 0);
            if (removalGate && !settings.Restart) {
                env.SendAsync(removalGate, new TEvents::TEvWakeup(TWorkerRemovalGate::Release));
            }

            SendWorkerStopped(env, info.ControllerId, base);
        }

        AssertEventuallyDone(env, info);
        AssertIndexCancellationCleanedUp(env);
    }

    Y_UNIT_TEST(DoneCancelsFillingSyncIndex) {
        CheckDoneWithUnfinishedSyncIndex({});
    }

    Y_UNIT_TEST(DoneCancelsJoiningSyncIndexAfterRestart) {
        CheckDoneWithUnfinishedSyncIndex({.Joining = true, .Restart = true});
    }

    Y_UNIT_TEST(DoneCancelsFillingSyncIndexFromPaused) {
        CheckDoneWithUnfinishedSyncIndex({.Paused = true});
    }

    Y_UNIT_TEST(DoneCancelsSyncIndexAfterLateWorkerError) {
        CheckDoneWithUnfinishedSyncIndex({.Scenario = EIndexCancellationScenario::LateWorkerError});
    }

    Y_UNIT_TEST(DoneCancelsSyncIndexAfterLateWorkerErrorAndRestart) {
        CheckDoneWithUnfinishedSyncIndex({.Restart = true, .Scenario = EIndexCancellationScenario::LateWorkerError});
    }

    Y_UNIT_TEST(DoneRetriesInterruptedSyncIndexCancellationAfterRestart) {
        CheckDoneWithUnfinishedSyncIndex({.Restart = true, .Scenario = EIndexCancellationScenario::Interrupted});
    }

    void CheckDoneWithCreatingIndexStream(bool restart) {
        TIndexBuildTestEnv env;
        const auto& info = env.Info;
        const auto& base = env.Base;
        auto& runtime = env.GetRuntime();

        env.AddSourceIndex();

        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        runtime.RegisterService(MakeTxProxyID(), runtime.Register(
            new TCreateCdcStreamRequestGate(txProxy, env.GetSender())));
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        env.ReportSchemaChange();
        const auto createStream = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(createStream->Get()->Record.GetTransaction().GetModifyScheme()
            .GetWorkingDir(), "/Root/table1/by_value");

        runtime.RegisterService(MakeTxProxyID(), txProxy);

        ui64 indexTargetId = 0;
        const auto description = DescribeReplication(env, info);
        for (const auto& target : description->Get()->Record.GetTargets()) {
            if (target.GetId() != base.TargetId()) {
                UNIT_ASSERT(!indexTargetId);
                indexTargetId = target.GetId();
            }
        }

        UNIT_ASSERT(indexTargetId);

        const TWorkerId indexWorker(base.ReplicationId(), indexTargetId, 0);
        env.AssertBuildPhase(indexWorker, NKikimrReplication::TIndexBuildState::FILLING);

        const auto readStream = [&] {
            NKikimrMiniKQL::TResult result;
            UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, info.ControllerId, Sprintf(R"((
                (let key '('('ReplicationId (Uint64 '%lu)) '('TargetId (Uint64 '%lu))))
                (return (AsList (SetResult 'Stream (SelectRow 'SrcStreams key '('Name 'ConsumerName 'State)))))
            ))", base.ReplicationId(), indexTargetId), result), NKikimrProto::OK);
            return result.SerializeAsString();
        };
        const auto streamBefore = readStream();
        UNIT_ASSERT(streamBefore.Contains(createStream->Get()->Record.GetTransaction().GetModifyScheme()
            .GetCreateCdcStream().GetStreamDescription().GetName()));
        UNIT_ASSERT(streamBefore.Contains("replicationConsumer"));

        env.CompleteBarrier();

        // Make the source unavailable before requesting forced DONE.
        env.AlterReplication(MakeAlterReplicationRequest(info, 1004, true));

        const auto pipeService = MakePipePerNodeCacheID(false);
        const auto pipeCache = runtime.GetLocalServiceId(pipeService);
        const auto gate = runtime.Register(new TSchemaDescribeGate(pipeCache, env.GetSender(),
            env.GetPathId("/Root/replica1")));
        runtime.RegisterService(pipeService, gate);
        env.AlterReplication(MakeDoneRequest(info, 1005));
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TSchemaDescribeGate::Blocked);
        env.AssertBuildPhase(indexWorker, NKikimrReplication::TIndexBuildState::CANCELLING);

        const auto checkLateResults = [&](bool done) {
            for (const auto status : {NYdb::EStatus::SUCCESS, NYdb::EStatus::SCHEME_ERROR}) {
                env.SendAsync(info.ControllerId, new TEvPrivate::TEvCreateStreamResult(
                    base.ReplicationId(), indexTargetId, NYdb::TStatus(status, NYdb::NIssue::TIssues())));
                const auto state = DescribeReplication(env, info)->Get()->Record.GetState();
                UNIT_ASSERT_C(done ? state.HasDone() : state.HasStandBy(), state.DebugString());
                UNIT_ASSERT_VALUES_EQUAL(readStream(), streamBefore);
            }
        };
        checkLateResults(false);
        if (restart) {
            RestartController(env, info.ControllerId);
            UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
                TSchemaDescribeGate::Blocked);

            AttachWorkers(env, info.ControllerId, {base});
            checkLateResults(false);
        }

        runtime.RegisterService(pipeService, pipeCache);
        env.SendAsync(gate, new TEvents::TEvWakeup(TSchemaDescribeGate::Release));
        env.StopWorker(base);
        AssertEventuallyDone(env, info);
        env.AssertBuildPhase(indexWorker, NKikimrReplication::TIndexBuildState::CANCELLED);
        checkLateResults(true);
        AssertIndexCancellationCleanedUp(env);
    }

    Y_UNIT_TEST(DoneCancelsSyncIndexWithCreatingStream) {
        CheckDoneWithCreatingIndexStream(false);
    }

    Y_UNIT_TEST(DoneCancelsSyncIndexWithCreatingStreamAfterRestart) {
        CheckDoneWithCreatingIndexStream(true);
    }

    Y_UNIT_TEST(SourceAddSyncIndexWithGlobalConsistency) {
        TEnv env(MakeIndexReplicationFlags());
        const auto info = StartReplication(env, 1, "root@builtin", true, false,
            NKikimrSchemeOp::EIndexTypeGlobal, false);
        NYdb::NTable::TTableClient client(env.GetDriver(), NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(env.GetEndpoint())
            .Database(env.GetDatabase()));
        auto session = client.CreateSession().GetValueSync().GetSession();
        const auto execute = [&](const TString& query) {
            std::optional<NYdb::NTable::TDataQueryResult> result;
            const auto status = client.RetryOperationSync([&](NYdb::NTable::TSession retrySession) {
                result.emplace(retrySession.ExecuteDataQuery(query,
                    NYdb::NTable::TTxControl::BeginTx()
                        .CommitTx()).GetValueSync());
                return NYdb::TStatus(*result);
            });
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
            return std::move(*result);
        };
        execute("UPSERT INTO `/Root/table1` (key, value) VALUES (1, 'old'), (2, 'deleted');");
        AddSyncIndex(session, "/Root/table1", "by_value");
        execute("UPSERT INTO `/Root/table1` (key, value) VALUES (1, 'new'), (3, 'inserted');");
        execute("DELETE FROM `/Root/table1` WHERE key = 2;");

        const auto assertCompactionPolicy = [&] {
            auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeScheme>(
                "/Root/replica1/by_value/indexImplTable");
            request->Record.MutableOptions()->SetShowPrivateTable(true);
            const auto response = env.Send<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(
                env.GetSchemeshardId("/Root/replica1"), std::move(request));
            const auto& record = response->Get()->GetRecord();
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrScheme::StatusSuccess);

            const auto& config = record.GetPathDescription().GetTable().GetPartitionConfig();
            UNIT_ASSERT(config.HasCompactionPolicy());
            UNIT_ASSERT(!config.GetCompactionPolicy().GetKeepEraseMarkers());
        };
        bool restartedDuringBuild = false;
        const bool ready = WaitFor([&] {
            const auto description = env.GetDescription("/Root/replica1");
            const auto& table = description.GetPathDescription().GetTable();
            const auto* index = table.TableIndexesSize() == 1 ? &table.GetTableIndexes(0) : nullptr;
            const bool filling = index && index->GetState() == NKikimrSchemeOp::EIndexStateWriteOnly;
            if (!restartedDuringBuild && filling) {
                assertCompactionPolicy();
                env.SendAsync(info.ControllerId, new TEvents::TEvPoisonPill());
                InvalidateTabletResolverCache(env.GetRuntime(), info.ControllerId);
                restartedDuringBuild = true;
            }

            if (index && index->GetState() == NKikimrSchemeOp::EIndexStateReady) {
                return true;
            }

            if (!restartedDuringBuild) {
                const auto replication = DescribeReplication(env, info);
                UNIT_ASSERT_C(!replication->Get()->Record.GetState().HasError(), replication->Get()->Record.DebugString());
            }

            return false;
        }, 1200);
        UNIT_ASSERT_C(ready, "Timed out waiting for replicated ADD SYNC INDEX");
        UNIT_ASSERT(restartedDuringBuild);
        assertCompactionPolicy();

        InvalidateTabletResolverCache(env.GetRuntime(), info.ControllerId);
        const auto replication = DescribeReplication(env, info);
        UNIT_ASSERT_C(!replication->Get()->Record.GetState().HasError(), replication->Get()->Record.DebugString());

        // Query through the index: backfill, update, insertion and delete must
        // all be visible once the first shared commit has made it Ready.
        const auto rows = execute("SELECT key, value FROM `/Root/replica1` VIEW by_value ORDER BY key;");
        UNIT_ASSERT_VALUES_EQUAL(rows.GetResultSet(0).RowsCount(), 2);
        NYdb::TResultSetParser parser(rows.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("key").GetOptionalUint32().value(), 1);
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("value").GetOptionalUtf8().value(), "new");
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("key").GetOptionalUint32().value(), 3);
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("value").GetOptionalUtf8().value(), "inserted");

        // A controller restart must retain global membership after Ready.
        env.SendAsync(info.ControllerId, new TEvents::TEvPoisonPill());
        execute("UPSERT INTO `/Root/table1` (key, value) VALUES (4, 'after-restart');");
        UNIT_ASSERT_C(WaitFor([&] {
            const auto result = execute("SELECT key FROM `/Root/replica1` VIEW by_value WHERE value = 'after-restart';");
            return result.GetResultSet(0).RowsCount() == 1;
        }, 600), "Index did not resume after controller restart");
    }

}
} // NKikimr::NReplication::NController
