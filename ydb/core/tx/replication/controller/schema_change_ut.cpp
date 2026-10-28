#include "controller_impl.h"
#include "controller.h"
#include "dst_alterer.h"
#include "dst_creator.h"
#include "dst_schema_changer.h"
#include "stream_creator.h"
#include "stream_remover.h"
#include "target_table.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/basics/appdata.h>
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
#include <ydb/library/testlib/helpers.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/datetime/base.h>

#include <atomic>
#include <functional>
#include <memory>

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
class TDescribePathRequestGate: public TActorBootstrapped<TDescribePathRequestGate> {
    static constexpr ui64 RequestBlocked = 1;
    static constexpr ui64 ReleaseRequest = 2;

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
        for (const auto& entry : ev->Get()->Request->ResultSet) {
            if (CanonizePath(JoinPath(entry.Path)) == Path) {
                PendingRequests.push_back(std::move(ev));
                Send(Notify, new TEvents::TEvWakeup(RequestBlocked));
                return;
            }
        }

        Send(ev->Forward(SchemeCache));
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Tag, ReleaseRequest);
        UNIT_ASSERT(!PendingRequests.empty());
        for (auto& request : PendingRequests) {
            Send(request->Forward(SchemeCache));
        }
        PendingRequests.clear();
    }

public:
    TDescribePathRequestGate(const TActorId& schemeCache, const TActorId& notify, TString path)
        : SchemeCache(schemeCache)
        , Notify(notify)
        , Path(CanonizePath(path))
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
    const TString Path;
    TVector<TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr> PendingRequests;
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

template <bool CreateStream, bool DropStream = false>
class TProposalRecorder: public TActorBootstrapped<TProposalRecorder<CreateStream, DropStream>> {
    using TBase = TActorBootstrapped<TProposalRecorder<CreateStream, DropStream>>;

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
        const bool record = CreateStream ? tx.GetModifyScheme().HasCreateCdcStream()
            : DropStream ? tx.GetModifyScheme().HasDropCdcStream() : tx.HasCommitWrites();
        TBase::Send(ev->Forward(record ? Notify : Proxy));
    }

private:
    const TActorId Proxy;
    const TActorId Notify;
};

using TCommitWritesRecorder = TProposalRecorder<false>;
using TCreateCdcStreamRequestGate = TProposalRecorder<true>;
using TDropCdcStreamRequestGate = TProposalRecorder<false, true>;

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

class TDstAlterResultGate: public TDecorator {
public:
    static constexpr ui64 Blocked = 24;
    static constexpr ui64 Release = 25;

    TDstAlterResultGate(THolder<IActor> actor, TActorId notify)
        : TDecorator(std::move(actor))
        , Notify(notify)
    {}

    bool DoBeforeReceiving(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) override {
        if (Blocking && ev->GetTypeRewrite() == TEvPrivate::TEvAlterDstResult::EventType) {
            Pending.push_back(std::move(ev));
            ctx.Send(new IEventHandle(Notify, Pending.back()->Sender, new TEvents::TEvWakeup(Blocked)));
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
    const TActorId Notify;
    bool Blocking = true;
    TVector<TAutoPtr<IEventHandle>> Pending;
};

class TAttachmentResultGate: public TDecorator {
    const bool HoldOnce;

public:
    TAttachmentResultGate(THolder<IActor> actor, TActorId notify, bool holdOnce = false)
        : TDecorator(std::move(actor))
        , HoldOnce(holdOnce)
        , Notify(notify)
    {}

    bool DoBeforeReceiving(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) override {
        if (ev->GetTypeRewrite() == TEvPrivate::TEvCreateDstResult::EventType) {
            const auto* result = ev->Get<TEvPrivate::TEvCreateDstResult>();
            if (!result->IsSuccess() || Released.contains(result->TargetId)) {
                return true;
            }

            auto reply = MakeHolder<TEvPrivate::TEvCreateDstResult>(
                result->ReplicationId,
                result->TargetId,
                result->DstPathId);
            ctx.Send(new IEventHandle(Notify, ev->Sender, reply.Release()));
            if (HoldOnce) {
                Released.insert(result->TargetId);
            }
            Pending[result->TargetId] = std::move(ev);
            return false;
        }
        if (ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
            const auto it = Pending.find(ev->Get<TEvents::TEvWakeup>()->Tag);
            UNIT_ASSERT(it != Pending.end());
            Released.insert(it->first);
            ctx.Send(it->second.Release());
            Pending.erase(it);
            return false;
        }
        return true;
    }

private:
    const TActorId Notify;
    THashMap<ui64, TAutoPtr<IEventHandle>> Pending;
    THashSet<ui64> Released;
};

class TDstCreatorResultGate: public TDecorator {
public:
    static constexpr ui64 ReleasePrepare = 30;
    static constexpr ui64 ReleaseResult = 31;

    TDstCreatorResultGate(THolder<IActor> actor, TActorId notify)
        : TDecorator(std::move(actor))
        , Notify(notify)
    {}

    bool DoBeforeReceiving(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) override {
        if (ev->GetTypeRewrite() == TEvPrivate::TEvPrepareAttachDst::EventType
            && !Released.contains(ev->Sender))
        {
            const auto* prepare = ev->Get<TEvPrivate::TEvPrepareAttachDst>();
            ctx.Send(new IEventHandle(
                Notify,
                ev->Sender,
                new TEvPrivate::TEvPrepareAttachDst(
                    prepare->ReplicationId,
                    prepare->TargetId,
                    prepare->DstPathId)));
            Creator = ev->Sender;
            Preparations.push_back(std::move(ev));
            return false;
        }
        if (ev->GetTypeRewrite() == TEvPrivate::TEvCreateDstResult::EventType
            && ev->Sender == Creator
            && HoldResult
            && ev->Get<TEvPrivate::TEvCreateDstResult>()->IsSuccess())
        {
            const auto* result = ev->Get<TEvPrivate::TEvCreateDstResult>();
            ctx.Send(new IEventHandle(
                Notify,
                ev->Sender,
                new TEvPrivate::TEvCreateDstResult(
                    result->ReplicationId,
                    result->TargetId,
                    result->DstPathId)));
            Result = std::move(ev);
            return false;
        }
        if (ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
            switch (ev->Get<TEvents::TEvWakeup>()->Tag) {
            case ReleasePrepare: {
                UNIT_ASSERT(!Preparations.empty());
                auto prepare = std::move(Preparations.front());
                Preparations.pop_front();
                Released.insert(prepare->Sender);
                ctx.Send(prepare.Release());
                return false;
            }
            case ReleaseResult:
                UNIT_ASSERT(Result);
                HoldResult = false;
                ctx.Send(Result.Release());
                return false;
            }
        }
        return true;
    }

private:
    const TActorId Notify;
    TActorId Creator;
    THashSet<TActorId> Released;
    TDeque<TAutoPtr<IEventHandle>> Preparations;
    TAutoPtr<IEventHandle> Result;
    bool HoldResult = true;
};

class TWorkerStartCounter: public TDecorator {
public:
    TWorkerStartCounter(THolder<IActor> actor, TActorId notify, std::shared_ptr<std::atomic<ui32>> starts)
        : TDecorator(std::move(actor))
        , Notify(notify)
        , Starts(std::move(starts))
    {}

    bool DoBeforeReceiving(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) override {
        if (ev->GetTypeRewrite() == TEvService::TEvRunWorker::EventType) {
            ++*Starts;
        } else if (ev->GetTypeRewrite() == TEvService::TEvHandshake::EventType) {
            auto copy = MakeHolder<TEvService::TEvHandshake>();
            copy->Record.CopyFrom(ev->Get<TEvService::TEvHandshake>()->Record);
            ctx.Send(Notify, copy.Release());
        }
        return true;
    }

private:
    const TActorId Notify;
    const std::shared_ptr<std::atomic<ui32>> Starts;
};

template <typename TMakeGate>
TActorId InstallControllerGate(TTestEnv& env, ui64 controllerId, TMakeGate makeGate) {
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
        auto* gate = makeGate(std::move(actor), notify);
        DoActorInit(ctx.ActorSystem(), gate, controller, {});
        mailbox->AttachActor(controller.LocalId(), gate);
    }, env.GetSender()), 0, controller.PoolID(), mailbox);
    runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
    return controller;
}

TActorId InstallWorkerRemovalGate(TTestEnv& env, ui64 controllerId, const TWorkerId& worker) {
    return InstallControllerGate(
        env,
        controllerId,
        [worker](THolder<IActor> actor, TActorId notify) {
            return new TWorkerRemovalGate(std::move(actor), worker, notify);
        });
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

void AssertDestinationMode(
        TTestEnv& env,
        const TString& path,
        NKikimrSchemeOp::TTableReplicationConfig::EReplicationMode expected)
{
    const auto actual = env.GetDescription(path).GetPathDescription().GetTable().GetReplicationConfig().GetMode();
    UNIT_ASSERT_VALUES_EQUAL_C(actual, expected, "Destination replication mode differs: " << path);
}

NYdb::NTable::TTableClient CreateTableClient(TTestEnv& env) {
    auto settings = NYdb::NTable::TClientSettings()
        .DiscoveryEndpoint(env.GetEndpoint())
        .Database(env.GetDatabase());
    return NYdb::NTable::TTableClient(env.GetDriver(), settings);
}

NKikimrMiniKQL::TResult ReadControllerQuery(
        TTestActorRuntime& runtime,
        ui64 controllerId,
        const TString& query)
{
    NKikimrMiniKQL::TResult result;
    UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, controllerId, query, result), NKikimrProto::OK);
    return result;
}

ui32 ReadPersistedStreamState(TTestActorRuntime& runtime, ui64 controllerId, const TWorkerId& worker) {
    const auto query = Sprintf(
        R"((
            (let key '('('ReplicationId (Uint64 '%lu)) '('TargetId (Uint64 '%lu))))
            (return (AsList (SetResult 'Stream (SelectRow 'SrcStreams key '('State)))))
        ))",
        worker.ReplicationId(),
        worker.TargetId());
    const auto result = ReadControllerQuery(runtime, controllerId, query);
    return result.GetValue().GetStruct(0).GetOptional().GetOptional().GetStruct(0).GetOptional().GetUint32();
}

TReplication::EDstState ReadPersistedDstState(
        TTestActorRuntime& runtime,
        ui64 controllerId,
        const TWorkerId& worker)
{
    const auto query = Sprintf(
        R"((
            (let key '('('ReplicationId (Uint64 '%lu)) '('Id (Uint64 '%lu))))
            (return (AsList (SetResult 'State (SelectRow 'Targets key '('DstState)))))
        ))",
        worker.ReplicationId(),
        worker.TargetId());
    const auto result = ReadControllerQuery(runtime, controllerId, query);
    const auto& row = result.GetValue().GetStruct(0).GetOptional().GetOptional();
    return static_cast<TReplication::EDstState>(row.GetStruct(0).GetOptional().GetUint32());
}

NYdb::NTable::TSession CreateTableSession(TTestEnv& env, NYdb::NTable::TTableClient& client) {
    std::optional<NYdb::NTable::TSession> session;
    const auto status = env.RunCall([&] {
        return client.RetryOperationSync([&](NYdb::NTable::TTableClient& tableClient) {
            auto result = tableClient.CreateSession().GetValueSync();
            if (!result.IsSuccess()) {
                return result;
            }

            session = result.GetSession();
            return result;
        });
    });
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    return *session;
}

TReplicationTestInfo StartReplication(
        TTestEnv& env,
        int targetCount = 1,
        const TString& token = "root@builtin",
        bool globalConsistency = false, bool withIndex = false,
        NKikimrSchemeOp::EIndexType indexType = NKikimrSchemeOp::EIndexTypeGlobal, bool mockService = true,
        TMaybe<bool> initialScan = Nothing(),
        std::shared_ptr<std::atomic<ui32>> workerStarts = {})
{
    NYdb::NTable::TTableClient client(env.GetDriver(), NYdb::NTable::TClientSettings()
        .DiscoveryEndpoint(env.GetEndpoint())
        .Database(env.GetDatabase()));
    auto session = CreateTableSession(env, client);
    for (int i = 1; i <= targetCount; ++i) {
        if (withIndex) {
            const auto status = env.RunCall([&] {
                return session.ExecuteSchemeQuery(Sprintf(R"(
                    CREATE TABLE `/Root/table%i` (
                        key Uint32, value Utf8, PRIMARY KEY (key),
                        INDEX by_value GLOBAL %s ON (value)
                    );
                )", i, indexType == NKikimrSchemeOp::EIndexTypeGlobalAsync ? "ASYNC" : "SYNC")).GetValueSync();
            });
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        } else {
            CreateSourceTable(env, Sprintf("table%i", i));
        }
    }

    THolder<IActor> serviceActor(mockService
        ? NTestHelpers::CreateReplicationMockService(env.GetSender())
        : CreateReplicationService());
    if (workerStarts) {
        serviceActor.Reset(new TWorkerStartCounter(std::move(serviceActor), env.GetSender(), workerStarts));
    }
    const auto service = env.GetRuntime().Register(serviceActor.Release());
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
    if (initialScan) {
        params.push_back(TStringBuilder() << "INITIAL_SCAN = " << (*initialScan ? "TRUE" : "FALSE"));
    }

    const auto status = env.RunCall([&] {
        return session.ExecuteSchemeQuery(Sprintf(R"(
            CREATE ASYNC REPLICATION `replication` FOR %s WITH (%s);
        )", JoinSeq(", ", targets).c_str(), JoinSeq(", ", params).c_str())).GetValueSync();
    });
    UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), NYdb::EStatus::SUCCESS, status.GetIssues().ToString());

    const auto desc = env.GetDescription("/Root/replication").GetPathDescription().GetReplicationDescription();
    TReplicationTestInfo info;
    info.ControllerId = desc.GetControllerId();
    info.PathId = env.GetPathId("/Root/replication");
    info.Config.CopyFrom(desc.GetConfig());

    if (mockService || workerStarts) {
        const auto handshake = env.GetRuntime().GrabEdgeEvent<TEvService::TEvHandshake>(env.GetSender());
        info.Generation = handshake->Get()->Record.GetController().GetGeneration();
        if (mockService) {
            env.SendAsync(info.ControllerId, new TEvService::TEvStatus());
        }
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

bool SourceStreamsReady(TTestEnv& env, const TReplicationTestInfo& info, ui32 expectedTargets) {
    const auto description = DescribeReplication(env, info);
    const auto& record = description->Get()->Record;
    if (static_cast<ui32>(record.TargetsSize()) != expectedTargets) {
        return false;
    }

    for (const auto& target : record.GetTargets()) {
        const TWorkerId worker(1, target.GetId(), 0);
        if (!ReadStreamSchemaChanges(env, info.ControllerId, worker).has_value()) {
            return false;
        }
    }
    return true;
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
    TActorId Creator;
};

void SendAttachmentResult(TTestEnv& env, const TPendingAttachment& pending, TEvPrivate::TEvCreateDstResult* result) {
    env.GetRuntime().Send(new IEventHandle(
        ResolveTablet(env.GetRuntime(), pending.Info.ControllerId),
        pending.Creator,
        result));
}

TPendingAttachment StartPendingAttachment(TTestEnv& env, bool withIndex = false) {
    env.GetRuntime().GetAppData().ReplicationConfig.SetInitialScan(false);
    if (withIndex) {
        auto client = CreateTableClient(env);
        auto session = client.CreateSession().GetValueSync().GetSession();
        const auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/replica1` (
                key Uint32, value Utf8, PRIMARY KEY (key),
                INDEX by_value GLOBAL SYNC ON (value)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    } else {
        CreateSourceTable(env, "replica1");
    }
    const auto info = StartReplication(env, 1, "root@builtin", false, withIndex);
    UNIT_ASSERT(!info.Config.GetInitialScan());

    ui64 targetId = 0;
    bool foundTarget = false;
    for (ui32 attempt = 0; attempt < 50; ++attempt) {
        const auto result = DescribeReplication(env, info);
        if (result->Get()->Record.TargetsSize() == (withIndex ? 2 : 1)) {
            for (const auto& target : result->Get()->Record.GetTargets()) {
                if (target.GetDstPath() == "/Root/replica1") {
                    targetId = target.GetId();
                    foundTarget = true;
                }
            }
            if (foundTarget) {
                break;
            }
        }
        Sleep(TDuration::MilliSeconds(100));
    }
    UNIT_ASSERT(foundTarget);

    // Finish real source setup before starting the attachment so its late result
    // cannot progress a destination before the test installs its gate.
    UNIT_ASSERT_C(
        WaitFor([&] {
            return SourceStreamsReady(env, info, withIndex ? 2 : 1);
        }),
        "Source stream setup did not finish before attachment");

    // Hold the real controller-owned creator's final result after its local DDL
    // commits. Later replies must carry this actor's identity.
    const auto controller = InstallControllerGate(
        env,
        info.ControllerId,
        [](THolder<IActor> actor, TActorId notify) {
            return new TAttachmentResultGate(std::move(actor), notify, true);
        });
    auto resume = MakeAlterReplicationRequest(info, 90);
    resume->Record.MutableSwitchState()->MutableStandBy();
    const auto resumed = env.Send<TEvController::TEvAlterReplicationResult>(info.ControllerId, std::move(resume));
    UNIT_ASSERT_VALUES_EQUAL(
        resumed->Get()->Record.GetStatus(),
        NKikimrReplication::TEvAlterReplicationResult::SUCCESS);

    TPendingAttachment pending{info, targetId, {}, {}};
    for (ui32 i = 0; i < (withIndex ? 2 : 1); ++i) {
        const auto attached = env.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT(attached->Get()->IsSuccess());
        if (attached->Get()->TargetId == targetId) {
            pending.DstPathId = attached->Get()->DstPathId;
            pending.Creator = attached->Sender;
        } else {
            env.SendAsync(controller, new TEvents::TEvWakeup(attached->Get()->TargetId));
        }
    }
    UNIT_ASSERT(pending.Creator);
    UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetPathDescription()
        .GetTable().GetReplicationConfig().GetMode(),
        NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
    return pending;
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

void AssertAttachmentPaths(
        TTestEnv& env,
        const TPendingAttachment& attachment,
        const TPathId& pendingPathId,
        const TPathId& dstPathId = {})
{
    NKikimrMiniKQL::TResult result;
    const auto status = LocalQuery(env.GetRuntime(), attachment.Info.ControllerId, Sprintf(R"((
        (let key '('('ReplicationId (Uint64 '1)) '('Id (Uint64 '%lu))))
        (return (AsList (SetResult 'Paths (SelectRow 'Targets key
            '('DstPathLocalId 'DstPathOwnerId 'PendingDstPathLocalId 'PendingDstPathOwnerId)))))
    ))", attachment.TargetId), result);
    UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::OK);
    const auto& row = result.GetValue().GetStruct(0).GetOptional();
    UNIT_ASSERT_C(row.HasOptional(), result.DebugString());
    const auto& paths = row.GetOptional();
    const auto readPath = [&paths](ui32 localIndex, ui32 ownerIndex) {
        const auto& local = paths.GetStruct(localIndex);
        const auto& owner = paths.GetStruct(ownerIndex);
        UNIT_ASSERT_VALUES_EQUAL(local.HasOptional(), owner.HasOptional());
        // Before the first destination result, an empty path is stored as NULL.
        return local.HasOptional()
            ? TPathId(owner.GetOptional().GetUint64(), local.GetOptional().GetUint64())
            : TPathId();
    };
    UNIT_ASSERT_VALUES_EQUAL(readPath(0, 1), dstPathId);
    UNIT_ASSERT_VALUES_EQUAL(readPath(2, 3), pendingPathId);
}

void CheckDoneAfterAttachmentError(bool restart, bool doneBeforeError = false) {
    TTestEnv env;
    const auto pending = StartPendingAttachment(env);
    const auto& info = pending.Info;
    const auto requestDone = [&] {
        const auto result = env.Send<TEvController::TEvAlterReplicationResult>(
            info.ControllerId, MakeDoneRequest(info, 105));
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
    };
    if (doneBeforeError) {
        requestDone();
        UNIT_ASSERT(!DescribeReplication(env, info)->Get()->Record.GetState().HasDone());
    }

    // The attachment ALTER has committed, but its post-ALTER description failed
    // validation. The controller has only the persisted pending path to detach.
    SendAttachmentResult(
        env,
        pending,
        new TEvPrivate::TEvCreateDstResult(
            1,
            pending.TargetId,
            NKikimrScheme::StatusSchemeError,
            "Replication consistency level mismatch"));
    if (!doneBeforeError) {
        UNIT_ASSERT(WaitFor([&] {
            return DescribeReplication(env, info)->Get()->Record.GetState().HasError();
        }));
        AssertAttachmentPaths(env, pending, pending.DstPathId);

        if (restart) {
            RestartController(env, info.ControllerId);
            UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());
            AssertAttachmentPaths(env, pending, pending.DstPathId);
        }
        requestDone();
    }

    AssertEventuallyDone(env, info);
    AssertDestinationWritable(env);
    AssertAttachmentPaths(env, pending, {});
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

Y_UNIT_TEST_SUITE(ReplicationCreation) {
    Y_UNIT_TEST(InitialScanDisabledDoesNotTouchMissingDestination) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        CreateSourceTable(env, "bootstrap");
        const TString destination = "/Root/replica1";
        const auto missingDestinationStatus = static_cast<int>(NKikimrScheme::StatusPathDoesNotExist);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription(destination).GetSchemeStatus(), missingDestinationStatus);

        ui32 destinationCreators = 0;
        TTestActorRuntimeBase::TRegistrationObserver previousRegistrationObserver;
        previousRegistrationObserver = runtime.SetRegistrationObserverFunc(
            [&](TTestActorRuntimeBase& actorRuntime, const TActorId& parent, const TActorId& actorId) {
                previousRegistrationObserver(actorRuntime, parent, actorId);
                if (actorRuntime.FindActor(actorId)->GetActivityType()
                    == NKikimrServices::TActivity::REPLICATION_CONTROLLER_DST_CREATOR)
                {
                    ++destinationCreators;
                }
            });
        Y_DEFER {
            runtime.SetRegistrationObserverFunc(std::move(previousRegistrationObserver));
        };

        bool streamCreated = false;
        auto streamObserver = runtime.AddObserver(
            [&](IEventHandle::TPtr& ev) {
                if (ev->GetTypeRewrite() != TEvPrivate::EvCreateStreamResult || !ev->HasEvent()) {
                    return;
                }
                // Private event numbers are shared by unrelated actors.
                if (const auto* result = dynamic_cast<TEvPrivate::TEvCreateStreamResult*>(ev->GetBase())) {
                    UNIT_ASSERT_C(result->IsSuccess(), result->Status.GetIssues().ToString());
                    streamCreated = true;
                }
            });

        const auto info = StartReplication(env, 1, "root@builtin", false, false,
            NKikimrSchemeOp::EIndexTypeGlobal, true, false);
        UNIT_ASSERT(!info.Config.GetInitialScan());
        runtime.WaitFor("source stream creation", [&] { return streamCreated; }, TDuration::Seconds(60));

        const auto state = DescribeReplication(env, info);
        UNIT_ASSERT_C(state->Get()->Record.GetStatus() == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS,
            state->Get()->Record.DebugString());
        UNIT_ASSERT_C(state->Get()->Record.GetState().HasPaused(), state->Get()->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(state->Get()->Record.TargetsSize(), 1);
        const auto targetId = state->Get()->Record.GetTargets(0).GetId();
        // The committed stream capability proves source setup has completed.
        UNIT_ASSERT(ReadStreamSchemaChanges(env, info.ControllerId, TWorkerId(1, targetId, 0)).has_value());
        UNIT_ASSERT_VALUES_EQUAL(destinationCreators, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription(destination).GetSchemeStatus(), missingDestinationStatus);

        // Reloading the ready source stream also exercises target progress during startup.
        RestartController(env, info.ControllerId);
        const auto restarted = DescribeReplication(env, info);
        UNIT_ASSERT_C(restarted->Get()->Record.GetState().HasPaused(), restarted->Get()->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(restarted->Get()->Record.TargetsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(destinationCreators, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription(destination).GetSchemeStatus(), missingDestinationStatus);
    }

    Y_UNIT_TEST_TWIN(InitialScan, ConfigInitialScan) {
        for (const auto initialScan : TVector<TMaybe<bool>>{Nothing(), false, true}) {
            TTestEnv env;
            env.GetRuntime().GetAppData().ReplicationConfig.SetInitialScan(ConfigInitialScan);
            const bool expectedInitialScan = initialScan.GetOrElse(ConfigInitialScan);
            if (!expectedInitialScan) {
                CreateSourceTable(env, "replica1");
            }

            const auto info = StartReplication(
                env,
                1,
                "root@builtin",
                false,
                false,
                NKikimrSchemeOp::EIndexTypeGlobal,
                true,
                initialScan);
            UNIT_ASSERT_VALUES_EQUAL(info.Config.GetInitialScan(), expectedInitialScan);

            const auto desc = env.GetDescription("/Root/replication").GetPathDescription().GetReplicationDescription();
            UNIT_ASSERT_VALUES_EQUAL(desc.GetState().HasPaused(), !expectedInitialScan);
            UNIT_ASSERT_VALUES_EQUAL(desc.GetState().HasStandBy(), expectedInitialScan);
        }
    }
}

Y_UNIT_TEST_SUITE(StatsDescription) {
    TReplicationTestInfo StartPausedReplicationWithSourceStreams(TTestEnv& env, ui32 targetCount = 1) {
        auto& runtime = env.GetRuntime();
        UNIT_ASSERT(!runtime.IsRealThreads());
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        for (ui32 i = 1; i <= targetCount; ++i) {
            CreateSourceTable(env, Sprintf("replica%u", i));
        }
        THashSet<ui64> streams;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->GetTypeRewrite() == TEvPrivate::EvCreateStreamResult && ev->HasEvent()) {
                // Private event numbers are shared with unrelated actors.
                if (const auto* result = dynamic_cast<TEvPrivate::TEvCreateStreamResult*>(ev->GetBase())) {
                    UNIT_ASSERT_C(result->IsSuccess(), result->Status.GetIssues().ToString());
                    streams.insert(result->TargetId);
                }
            }
        });
        const auto info = StartReplication(env, targetCount);
        runtime.WaitFor("source stream setup", [&] { return streams.size() == targetCount; }, TDuration::Seconds(60));
        return info;
    }

    THolder<TEvController::TEvDescribeReplication> MakeStatsRequest(const TReplicationTestInfo& info) {
        auto request = MakeHolder<TEvController::TEvDescribeReplication>();
        info.PathId.ToProto(request->Record.MutablePathId());
        request->Record.SetIncludeStats(true);
        return request;
    }

    TEvController::TEvDescribeReplicationResult::TPtr DescribeStats(
            TTestEnv& env,
            const TReplicationTestInfo& info,
            TDuration timeout = TDuration::Seconds(1))
    {
        env.SendAsync(info.ControllerId, MakeStatsRequest(info));
        auto result = env.GetRuntime().GrabEdgeEvent<TEvController::TEvDescribeReplicationResult>(
            env.GetSender(),
            timeout);
        UNIT_ASSERT_C(result, "Stats describe did not finish within " << timeout);
        UNIT_ASSERT(result->Get()->Record.GetStatus() == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.TargetsSize(), 1);
        const auto& target = result->Get()->Record.GetTargets(0);
        UNIT_ASSERT_VALUES_EQUAL(target.GetSrcPath(), "/Root/table1");
        UNIT_ASSERT_VALUES_EQUAL(target.GetDstPath(), "/Root/replica1");
        return result;
    }

    Y_UNIT_TEST(DoneWithoutSourceAndAfterRestart) {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        CreateSourceTable(env, "replica1");
        const auto info = StartReplication(env);
        UNIT_ASSERT(WaitFor([&] {
            return SourceStreamsReady(env, info, 1);
        }));
        const auto before = DescribeStats(env, info, TDuration::Seconds(10));
        const auto stream = before->Get()->Record.GetTargets(0).GetSrcStreamName();
        UNIT_ASSERT(stream);

        const auto gate = InstallControllerGate(
            env,
            info.ControllerId,
            [](THolder<IActor> actor, TActorId notify) {
                return new TDstAlterResultGate(std::move(actor), notify);
            });
        auto done = MakeDoneRequest(info, 117);
        auto& credentials = *done->Record.MutableConfig()->MutableSrcConnectionParams()->MutableOAuthToken();
        credentials.ClearToken();
        credentials.SetTokenSecretName("unavailable-source-secret");
        const auto altered = env.Send<TEvController::TEvAlterReplicationResult>(info.ControllerId, std::move(done));
        UNIT_ASSERT_VALUES_EQUAL(
            altered->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TDstAlterResultGate::Blocked);
        // Desired DONE must also stay independent of source statistics while
        // destination cleanup is still pending.
        const auto pending = DescribeStats(env, info);
        UNIT_ASSERT(!pending->Get()->Record.GetState().HasDone());
        UNIT_ASSERT_VALUES_EQUAL(pending->Get()->Record.GetTargets(0).GetSrcStreamName(), stream);
        env.SendAsync(gate, new TEvents::TEvWakeup(TDstAlterResultGate::Release));
        AssertEventuallyDone(env, info);

        for (const bool restart : {false, true}) {
            if (restart) {
                RestartController(env, info.ControllerId);
            }
            const auto result = DescribeStats(env, info);
            UNIT_ASSERT(result->Get()->Record.GetState().HasDone());
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTargets(0).GetSrcStreamName(), stream);
            UNIT_ASSERT(!result->Get()->Record.GetTargets(0).HasInitialScanProgress());
        }
    }

    Y_UNIT_TEST(MissingProxy) {
        TTestEnv env;
        env.GetRuntime().GetAppData().ReplicationConfig.SetInitialScan(false);
        CreateSourceTable(env, "replica1");
        const auto info = StartReplication(env);
        UNIT_ASSERT(WaitFor([&] { return DescribeReplication(env, info)->Get()->Record.TargetsSize() == 1; }));
        auto update = MakeAlterReplicationRequest(info, 118);
        auto& credentials = *update->Record.MutableConfig()->MutableSrcConnectionParams()->MutableOAuthToken();
        credentials.ClearToken();
        credentials.SetTokenSecretName("unavailable-source-secret");
        const auto altered = env.Send<TEvController::TEvAlterReplicationResult>(info.ControllerId, std::move(update));
        UNIT_ASSERT_VALUES_EQUAL(
            altered->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        DescribeStats(env, info);
    }

    Y_UNIT_TEST_TWIN(CredentialsDuringStatsRequest, ReplyPending) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        const auto info = StartPausedReplicationWithSourceStreams(env);
        TActorId describer;
        TAutoPtr<IEventHandle> delayed;
        bool block = true;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (!block) {
                return;
            }
            if (ev->GetTypeRewrite() == TEvYdbProxy::EvDescribeTableRequest) {
                const auto activity = runtime.FindActor(ev->Sender)->GetActivityType();
                if (activity != NKikimrServices::TActivity::REPLICATION_CONTROLLER_TARGET_DESCRIBER) {
                    return;
                }

                describer = ev->Sender;
                if (!ReplyPending) {
                    delayed = std::move(ev);
                }
                return;
            }
            if (!ReplyPending || !describer) {
                return;
            }
            if (ev->GetTypeRewrite() == TEvYdbProxy::EvDescribeTableResponse
                && ev->GetRecipientRewrite() == describer)
            {
                delayed = std::move(ev);
            }
        });
        const auto statsSender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, info.ControllerId, statsSender, MakeStatsRequest(info).Release());
        runtime.WaitFor("pending source stats", [&] { return bool(delayed); }, TDuration::Seconds(30));
        // Both an unsent request and an accepted request with a lost reply must
        // finish promptly when their connection is replaced, before the timer.
        auto update = MakeAlterReplicationRequest(info, 119);
        update->Record.MutableConfig()->MutableSrcConnectionParams()->MutableOAuthToken()->SetToken("");
        const auto altered = env.Send<TEvController::TEvAlterReplicationResult>(info.ControllerId, std::move(update));
        UNIT_ASSERT_VALUES_EQUAL(
            altered->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        const auto result = runtime.GrabEdgeEvent<TEvController::TEvDescribeReplicationResult>(
            statsSender,
            TDuration::Seconds(1));
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Get()->Record.GetStatus() == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.TargetsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTargets(0).GetSrcPath(), "/Root/table1");

        block = false;
        runtime.Send(delayed.Release());
        // The old response cannot reply twice or affect a new stats request.
        DescribeStats(env, info, TDuration::Seconds(10));
        UNIT_ASSERT(!runtime.GrabEdgeEvent<TEvController::TEvDescribeReplicationResult>(
            statsSender,
            TDuration::MilliSeconds(100)));
    }

    Y_UNIT_TEST(UnresponsiveProxyReturnsPartialStats) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        const auto info = StartPausedReplicationWithSourceStreams(env, 2);
        const auto description = DescribeReplication(env, info);
        UNIT_ASSERT_VALUES_EQUAL(description->Get()->Record.TargetsSize(), 2);
        const auto first = description->Get()->Record.GetTargets(0);
        ui32 requests = 0;
        auto observer = runtime.AddObserver<TEvYdbProxy::TEvDescribeTableRequest>(
            [&](TEvYdbProxy::TEvDescribeTableRequest::TPtr& ev) {
                const auto activity = runtime.FindActor(ev->Sender)->GetActivityType();
                if (activity != NKikimrServices::TActivity::REPLICATION_CONTROLLER_TARGET_DESCRIBER) {
                    return;
                }
                ++requests;
                if (ev->Cookie == first.GetId()) {
                    Ydb::Table::DescribeTableResult table;
                    auto& stream = *table.add_changefeeds();
                    stream.set_name(first.GetSrcStreamName());
                    stream.mutable_initial_scan_progress()->set_parts_total(4);
                    stream.mutable_initial_scan_progress()->set_parts_completed(1);
                    runtime.Send(new IEventHandle(
                        ev->Sender,
                        ev->GetRecipientRewrite(),
                        new TEvYdbProxy::TEvDescribeTableResponse(
                            NYdb::TStatus(NYdb::EStatus::SUCCESS, {}),
                            std::move(table),
                            NYdb::NTable::TDescribeTableSettings()),
                        0,
                        ev->Cookie));
                }
                // The other request is accepted but never answered.
                ev.Reset();
            });
        env.SendAsync(info.ControllerId, MakeStatsRequest(info));
        const auto result = runtime.GrabEdgeEvent<TEvController::TEvDescribeReplicationResult>(
            env.GetSender(),
            TDuration::Seconds(6));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(requests, 2);
        UNIT_ASSERT(result->Get()->Record.GetStatus() == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.TargetsSize(), 2);
        for (const auto& target : result->Get()->Record.GetTargets()) {
            UNIT_ASSERT_VALUES_EQUAL(target.HasInitialScanProgress(), target.GetId() == first.GetId());
            if (target.GetId() == first.GetId()) {
                UNIT_ASSERT_VALUES_EQUAL(target.GetInitialScanProgress(), 25);
            }
        }
        UNIT_ASSERT(!result->Get()->Record.GetState().GetStandBy().HasInitialScanProgress());
    }
}

Y_UNIT_TEST_SUITE(TargetDiscoveryResults) {
    class TDiscoveryResultGate {
        TTestEnv& Env;
        bool Capture = true;
        TTestActorRuntime::TEventObserverHolder Observer;

    public:
        TVector<TAutoPtr<IEventHandle>> Pending;
        THashSet<ui64> ReadyStreams;

        explicit TDiscoveryResultGate(TTestEnv& env)
            : Env(env)
            , Observer(env.GetRuntime().AddObserver([this](IEventHandle::TPtr& ev) {
                if (!ev->HasEvent()) {
                    return;
                }
                if (Capture && dynamic_cast<TEvPrivate::TEvDiscoveryTargetsResult*>(ev->GetBase())) {
                    // Hold replies produced by actual controller-owned actors.
                    Pending.push_back(std::move(ev));
                    return;
                }

                const auto* result = dynamic_cast<TEvPrivate::TEvCreateStreamResult*>(ev->GetBase());
                if (!result) {
                    return;
                }
                UNIT_ASSERT_C(result->IsSuccess(), result->Status.GetIssues().ToString());
                ReadyStreams.insert(result->TargetId);
            }))
        {}

        void Wait(ui32 count) {
            Env.GetRuntime().WaitFor(
                "target discoverer reply",
                [&] { return Pending.size() >= count; },
                TDuration::Seconds(30));
        }

        auto Deliver(const TReplicationTestInfo& info, ui32 index, bool success = true, bool empty = false) {
            const auto& original = Pending.at(index);
            const auto* result = original->Get<TEvPrivate::TEvDiscoveryTargetsResult>();
            THolder<TEvPrivate::TEvDiscoveryTargetsResult> reply;
            if (success) {
                auto toAdd = empty ? TVector<TEvPrivate::TEvDiscoveryTargetsResult::TAddEntry>() : result->ToAdd;
                reply = MakeHolder<TEvPrivate::TEvDiscoveryTargetsResult>(
                    result->ReplicationId,
                    std::move(toAdd),
                    TVector<ui64>());
            } else {
                TVector<TEvPrivate::TEvDiscoveryTargetsResult::TFailedEntry> failed;
                failed.emplace_back("/Root/table1", NYdb::TStatus(NYdb::EStatus::SCHEME_ERROR, {}));
                reply = MakeHolder<TEvPrivate::TEvDiscoveryTargetsResult>(result->ReplicationId, std::move(failed));
            }
            Capture = false;
            auto& runtime = Env.GetRuntime();
            runtime.Send(new IEventHandle(ResolveTablet(runtime, info.ControllerId), original->Sender, reply.Release()));
            auto description = DescribeReplication(Env, info);
            Capture = true;
            return description;
        }
    };

    void AssertTargetCount(TTestEnv& env, const TReplicationTestInfo& info, ui32 expected) {
        const auto description = DescribeReplication(env, info);
        UNIT_ASSERT_VALUES_EQUAL(description->Get()->Record.TargetsSize(), expected);
        UNIT_ASSERT(!description->Get()->Record.GetState().HasError());
    }

    void AssertPersistedCounts(TTestEnv& env, const TReplicationTestInfo& info, ui32 expected) {
        for (const auto* table : {"Targets", "SrcStreams"}) {
            const auto* targetId = TStringBuf(table) == "Targets" ? "Id" : "TargetId";
            const auto query = Sprintf(
                R"((
                (let range '('('ReplicationId (Uint64 '1) (Uint64 '1)) '('%s (Null) (Void))))
                (return (AsList (SetResult 'Rows (SelectRange '%s range '('ReplicationId) '())))))
            )",
                targetId,
                table);
            const auto result = ReadControllerQuery(env.GetRuntime(), info.ControllerId, query);
            UNIT_ASSERT_VALUES_EQUAL_C(
                result.GetValue().GetStruct(0).GetOptional().GetStruct(0).ListSize(),
                expected,
                table << ": " << result.DebugString());
        }
    }

    Y_UNIT_TEST_TWIN(ReplacedDiscovery, NewFirst) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        CreateSourceTable(env, "replica1");
        TDiscoveryResultGate gate(env);
        const auto info = StartReplication(env);
        gate.Wait(1);
        AssertTargetCount(env, info, 0);

        auto update = MakeAlterReplicationRequest(info, 150);
        update->Record.MutableConfig()->MutableSrcConnectionParams()->MutableOAuthToken()->SetToken("");
        update->Record.MutableSwitchState()->MutablePaused();
        UNIT_ASSERT_VALUES_EQUAL(
            env.Send<TEvController::TEvAlterReplicationResult>(
                info.ControllerId,
                std::move(update))->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        gate.Wait(2);
        UNIT_ASSERT(gate.Pending[0]->Sender != gate.Pending[1]->Sender);

        if constexpr (NewFirst) {
            gate.Deliver(info, 1);
        }
        // Obsolete success/failure and repeated successes must never append
        // another target or fail the replacement, whichever reply arrives first.
        gate.Deliver(info, 0);
        gate.Deliver(info, 0, false);
        gate.Deliver(info, 0);
        AssertTargetCount(env, info, NewFirst ? 1 : 0);
        if constexpr (!NewFirst) {
            gate.Deliver(info, 1);
        }
        gate.Deliver(info, 1);
        gate.Deliver(info, 1, false);
        AssertTargetCount(env, info, 1);
        runtime.WaitFor("source stream setup", [&] { return gate.ReadyStreams.size() == 1; }, TDuration::Seconds(60));
        AssertPersistedCounts(env, info, 1);
        UNIT_ASSERT(ReadStreamSchemaChanges(env, info.ControllerId, TWorkerId(1, 1, 0)).has_value());

        auto client = CreateTableClient(env);
        auto session = env.RunCall([&] { return client.CreateSession().GetValueSync().GetSession(); });
        const auto table = env.RunCall([&] { return session.DescribeTable("/Root/table1").GetValueSync(); });
        UNIT_ASSERT_C(table.IsSuccess(), table.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(table.GetTableDescription().GetChangefeedDescriptions().size(), 1);

        RestartController(env, info.ControllerId);
        gate.Deliver(info, 0);
        gate.Deliver(info, 1);
        gate.Deliver(info, 1, false);
        AssertTargetCount(env, info, 1);
        AssertPersistedCounts(env, info, 1);
        UNIT_ASSERT_VALUES_EQUAL(DescribeReplication(env, info)->Get()->Record.GetTargets(0).GetId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(gate.Pending.size(), 2);
    }

    Y_UNIT_TEST(CurrentFailureAndRetry) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        env.GetRuntime().GetAppData().ReplicationConfig.SetInitialScan(false);
        CreateSourceTable(env, "replica1");
        TDiscoveryResultGate gate(env);
        const auto info = StartReplication(env);
        gate.Wait(1);
        UNIT_ASSERT(gate.Deliver(info, 0, false)->Get()->Record.GetState().HasError());
        gate.Deliver(info, 0);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());
        RequestPause(env, info, 151);
        gate.Wait(2);
        gate.Deliver(info, 0, false);
        gate.Deliver(info, 1);
        AssertTargetCount(env, info, 1);
        WaitForPaused(env, info);
    }

    Y_UNIT_TEST(EmptyDiscoveryIsConsumed) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        env.GetRuntime().GetAppData().ReplicationConfig.SetInitialScan(false);
        CreateSourceTable(env, "replica1");
        TDiscoveryResultGate gate(env);
        const auto info = StartReplication(env);
        gate.Wait(1);
        gate.Deliver(info, 0, true, true);
        gate.Deliver(info, 0);
        gate.Deliver(info, 0, false);
        AssertTargetCount(env, info, 0);
        AssertPersistedCounts(env, info, 0);
        env.GetRuntime().SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(gate.Pending.size(), 1);
        // An explicit retry can still start a fresh discovery after an empty result.
        RequestPause(env, info, 152);
        gate.Wait(2);
        gate.Deliver(info, 1);
        AssertTargetCount(env, info, 1);
    }

    Y_UNIT_TEST_TWIN(TerminalStateIgnoresDiscovery, Drop) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        CreateSourceTable(env, "replica1");
        TDiscoveryResultGate gate(env);
        const auto info = StartReplication(env);
        gate.Wait(1);
        TAutoPtr<IEventHandle> dropCompletion;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->HasEvent() && dynamic_cast<TEvPrivate::TEvDropReplication*>(ev->GetBase())) {
                dropCompletion = std::move(ev);
            }
        });
        if constexpr (Drop) {
            auto request = MakeHolder<TEvController::TEvDropReplication>();
            info.PathId.ToProto(request->Record.MutablePathId());
            request->Record.MutableOperationId()->SetTxId(153);
            env.SendAsync(info.ControllerId, std::move(request));
            runtime.WaitFor("pending replication removal", [&] { return bool(dropCompletion); }, TDuration::Seconds(30));
        } else {
            UNIT_ASSERT_VALUES_EQUAL(
                env.Send<TEvController::TEvAlterReplicationResult>(
                    info.ControllerId,
                    MakeDoneRequest(info, 153))->Get()->Record.GetStatus(),
                NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        }
        gate.Deliver(info, 0);
        gate.Deliver(info, 0, false);
        AssertTargetCount(env, info, 0);
        AssertPersistedCounts(env, info, 0);
        if constexpr (Drop) {
            observer.Remove();
            runtime.Send(dropCompletion.Release());
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvController::TEvDropReplicationResult>(
                env.GetSender())->Get()->Record.GetStatus() == NKikimrReplication::TEvDropReplicationResult::SUCCESS);
            gate.Deliver(info, 0);
            gate.Deliver(info, 0, false);
            AssertPersistedCounts(env, info, 0);
        } else {
            UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasDone());
            RestartController(env, info.ControllerId);
            gate.Deliver(info, 0);
            gate.Deliver(info, 0, false);
            UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasDone());
            AssertPersistedCounts(env, info, 0);
        }
    }
}

Y_UNIT_TEST_SUITE(CredentialResolution) {
    using NTestSuiteStatsDescription::StartPausedReplicationWithSourceStreams;

    class TCredentialResultGate {
        TTestEnv& Env;
        bool Capture = true;
        TTestActorRuntime::TEventObserverHolder Observer;

        bool CompleteSecretLookup(IEventHandle::TPtr& ev) {
            if (ev->GetTypeRewrite() != TEvTxProxySchemeCache::EvNavigateKeySet) {
                return false;
            }
            auto& runtime = Env.GetRuntime();
            const auto activity = runtime.FindActor(ev->Sender)->GetActivityType();
            if (activity != NKikimrServices::TActivity::REPLICATION_CONTROLLER_SECRET_RESOLVER) {
                return false;
            }

            auto request = ev->Get<TEvTxProxySchemeCache::TEvNavigateKeySet>()->Request;
            request->ResultSet.front().Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
            request->ResultSet.front().SecurityObject = nullptr;
            auto* response = new TEvTxProxySchemeCache::TEvNavigateKeySetResult(request);
            runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response));
            ev.Reset();
            return true;
        }

        bool CompleteResourceIdLookup(IEventHandle::TPtr& ev) {
            if (ev->GetTypeRewrite() != TEvYdbProxy::EvDescribeTableRequest) {
                return false;
            }
            auto& runtime = Env.GetRuntime();
            const auto activity = runtime.FindActor(ev->Sender)->GetActivityType();
            if (activity != NKikimrServices::TActivity::REPLICATION_CONTROLLER_RESOURCE_ID_RESOLVER) {
                return false;
            }

            auto* response = new TEvYdbProxy::TEvDescribeTableResponse(
                NYdb::TStatus(NYdb::EStatus::BAD_REQUEST, {}),
                Ydb::Table::DescribeTableResult(),
                NYdb::NTable::TDescribeTableSettings());
            runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), response));
            ev.Reset();
            return true;
        }

    public:
        TVector<TAutoPtr<IEventHandle>> Pending;

        explicit TCredentialResultGate(TTestEnv& env)
            : Env(env)
            , Observer(env.GetRuntime().AddObserver([this](IEventHandle::TPtr& ev) {
                // Keep the resolver actors real, but complete their external
                // lookups deterministically and hold the actual replies.
                if (CompleteSecretLookup(ev) || CompleteResourceIdLookup(ev)) {
                    return;
                }
                if (!Capture || !ev->HasEvent()) {
                    return;
                }

                if (dynamic_cast<TEvPrivate::TEvResolveSecretResult*>(ev->GetBase())
                    || dynamic_cast<TEvPrivate::TEvResolveResourceIdResult*>(ev->GetBase()))
                {
                    Pending.push_back(std::move(ev));
                }
            }))
        {}

        void Wait(ui32 count) {
            Env.GetRuntime().WaitFor(
                "credential resolver reply",
                [&] { return Pending.size() >= count; },
                TDuration::Seconds(30));
        }

        auto Deliver(const TReplicationTestInfo& info, ui32 index, bool success, const TString& value = "root@builtin") {
            const auto& original = Pending.at(index);
            THolder<IEventBase> result;
            if (dynamic_cast<TEvPrivate::TEvResolveSecretResult*>(original->GetBase())) {
                result.Reset(success
                    ? new TEvPrivate::TEvResolveSecretResult(1, value)
                    : new TEvPrivate::TEvResolveSecretResult(1, false, "late secret failure"));
            } else {
                result.Reset(success
                    ? new TEvPrivate::TEvResolveResourceIdResult(1, value)
                    : new TEvPrivate::TEvResolveResourceIdResult(1, false, "late resource id failure"));
            }
            Capture = false;
            auto& runtime = Env.GetRuntime();
            runtime.Send(new IEventHandle(
                ResolveTablet(runtime, info.ControllerId),
                original->Sender,
                result.Release(),
                0,
                original->Cookie));
            auto description = DescribeReplication(Env, info);
            Capture = true;
            return description;
        }
    };

    auto MakeResolutionRequest(
            const TReplicationTestInfo& info,
            ui64 txId,
            bool resourceId,
            const TString& name = "first-secret")
    {
        auto request = MakeAlterReplicationRequest(info, txId);
        auto& params = *request->Record.MutableConfig()->MutableSrcConnectionParams();
        if (resourceId) {
            params.MutableIamCredentials()->MutableInitialToken()->SetToken(name);
        } else {
            params.MutableOAuthToken()->ClearToken();
            params.MutableOAuthToken()->SetTokenSecretName(name);
        }
        return request;
    }

    void Alter(
            TTestEnv& env,
            const TReplicationTestInfo& info,
            THolder<TEvController::TEvAlterReplication> request)
    {
        const auto result = env.Send<TEvController::TEvAlterReplicationResult>(info.ControllerId, std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
    }

    Y_UNIT_TEST_TWIN(LateResultsDuringAndAfterDone, ResourceId) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        const auto info = StartPausedReplicationWithSourceStreams(env);
        TCredentialResultGate credentials(env);
        auto update = MakeResolutionRequest(info, 130, ResourceId);
        const auto config = update->Record.GetConfig();
        Alter(env, info, std::move(update));
        credentials.Wait(1);

        TAutoPtr<IEventHandle> destinationResult;
        bool holdDestination = true;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (holdDestination && ev->HasEvent() && dynamic_cast<TEvPrivate::TEvAlterDstResult*>(ev->GetBase())) {
                destinationResult = std::move(ev);
            }
        });
        auto done = MakeDoneRequest(info, 131);
        done->Record.MutableConfig()->CopyFrom(config);
        Alter(env, info, std::move(done));
        runtime.WaitFor("pending destination cleanup", [&] { return bool(destinationResult); }, TDuration::Seconds(30));

        const auto check = [&](bool done) {
            for (const bool success : {false, true}) {
                const auto description = credentials.Deliver(info, 0, success);
                UNIT_ASSERT(!description->Get()->Record.GetState().HasError());
                UNIT_ASSERT_VALUES_EQUAL(description->Get()->Record.GetState().HasDone(), done);
                UNIT_ASSERT_VALUES_EQUAL(
                    description->Get()->Record.GetConnectionParams().SerializeAsString(),
                    config.GetSrcConnectionParams().SerializeAsString());
            }
        };
        check(false);
        holdDestination = false;
        runtime.Send(destinationResult.Release());
        AssertEventuallyDone(env, info);
        check(true);
        RestartController(env, info.ControllerId);
        check(true);
    }

    Y_UNIT_TEST_TWIN(ReplacementRejectsOldResultsAndAcceptsNew, ResourceId) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        const auto info = StartPausedReplicationWithSourceStreams(env);
        TCredentialResultGate credentials(env);
        Alter(env, info, MakeResolutionRequest(info, 132, ResourceId));
        credentials.Wait(1);
        auto replacement = MakeResolutionRequest(info, 133, ResourceId, "replacement-secret");
        const auto config = replacement->Record.GetConfig();
        Alter(env, info, std::move(replacement));
        credentials.Wait(2);
        for (const bool success : {false, true}) {
            const auto description = credentials.Deliver(info, 0, success);
            UNIT_ASSERT(!description->Get()->Record.GetState().HasError());
            UNIT_ASSERT_VALUES_EQUAL(
                description->Get()->Record.GetConnectionParams().SerializeAsString(),
                config.GetSrcConnectionParams().SerializeAsString());
        }
        const auto resolved = credentials.Deliver(info, 1, true);
        UNIT_ASSERT(!resolved->Get()->Record.GetState().HasError());
        if constexpr (ResourceId) {
            UNIT_ASSERT_VALUES_EQUAL(resolved->Get()->Record.GetConnectionParams().GetIamCredentials().GetResourceId(), "root@builtin");
        } else {
            UNIT_ASSERT_VALUES_EQUAL(resolved->Get()->Record.GetConnectionParams().GetOAuthToken().GetToken(), "root@builtin");
        }
        // A duplicate error after successful resolution must not undo it.
        UNIT_ASSERT(!credentials.Deliver(info, 1, false)->Get()->Record.GetState().HasError());
    }

    void CheckDeferredAlter(bool resourceId, bool done, bool drop = false) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        const auto info = StartPausedReplicationWithSourceStreams(env);
        auto resume = MakeAlterReplicationRequest(info, 138);
        resume->Record.MutableSwitchState()->MutableStandBy();
        Alter(env, info, std::move(resume));
        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());
        AttachWorkers(env, info.ControllerId, {worker});
        const auto schema = MakeSchemaChange();
        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(worker, schema));
        runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(worker, schema, true));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender())->Get()->Record.GetApplied());

        TCredentialResultGate credentials(env);
        auto update = MakeResolutionRequest(info, 136, resourceId);
        const auto config = update->Record.GetConfig();
        Alter(env, info, std::move(update));
        credentials.Wait(1);
        auto deferred = MakeAlterReplicationRequest(info, 137);
        if (done) {
            // Same config, same resolver identity: only DesiredState changes.
            deferred->Record.MutableConfig()->CopyFrom(config);
            deferred->Record.MutableSwitchState()->MutableDone()->SetFailoverMode(
                NKikimrReplication::TReplicationState::TDone::FAILOVER_MODE_FORCE);
        } else {
            deferred = MakeResolutionRequest(info, 137, resourceId, "replacement-secret");
            deferred->Record.MutableSwitchState()->MutablePaused();
        }
        const auto expected = deferred->Record.GetConfig().GetSrcConnectionParams().SerializeAsString();
        Alter(env, info, std::move(deferred));
        const auto checkOldResults = [&](const TString& expected) {
            for (const bool success : {false, true}) {
                const auto description = credentials.Deliver(info, 0, success);
                UNIT_ASSERT(description->Get()->Record.GetState().HasStandBy());
                UNIT_ASSERT_VALUES_EQUAL(description->Get()->Record.GetConnectionParams().SerializeAsString(), expected);
            }
        };
        checkOldResults(expected);
        if (!done) {
            // Returning to the original config must not revive its resolver.
            auto restore = MakeAlterReplicationRequest(info, 139);
            restore->Record.MutableConfig()->CopyFrom(config);
            Alter(env, info, std::move(restore));
            checkOldResults(config.GetSrcConnectionParams().SerializeAsString());
            auto changeVariant = MakeAlterReplicationRequest(info, 140);
            auto& auth = *changeVariant->Record.MutableConfig()->MutableSrcConnectionParams()->MutableStaticCredentials();
            auth.SetUser("replacement-user");
            auth.SetPassword("replacement-password");
            const auto params = changeVariant->Record.GetConfig().GetSrcConnectionParams().SerializeAsString();
            Alter(env, info, std::move(changeVariant));
            checkOldResults(params);
        }
        if (drop) {
            auto request = MakeHolder<TEvController::TEvDropReplication>();
            info.PathId.ToProto(request->Record.MutablePathId());
            request->Record.MutableOperationId()->SetTxId(141);
            env.SendAsync(info.ControllerId, std::move(request));
            credentials.Wait(2);
            credentials.Deliver(info, 0, false);
            credentials.Deliver(info, 1, true);
            runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
            SendWorkerStopped(env, info.ControllerId, worker);
            const auto result = runtime.GrabEdgeEvent<TEvController::TEvDropReplicationResult>(env.GetSender(), TDuration::Seconds(30));
            UNIT_ASSERT(result);
            UNIT_ASSERT(result->Get()->Record.GetStatus() == NKikimrReplication::TEvDropReplicationResult::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/table1").GetPathDescription().GetTable().CdcStreamsSize(), 0);
            return;
        }

        bool destinationAltered = false;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (ev->HasEvent()) {
                if (const auto* result = dynamic_cast<TEvPrivate::TEvAlterDstResult*>(ev->GetBase())) {
                    UNIT_ASSERT(result->IsSuccess());
                    destinationAltered = true;
                }
            }
        });
        env.SendAsync(info.ControllerId, MakeSchemaChangeReport(worker, schema, false, true));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender())->Get()->Record.GetCompleted());
        runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        SendWorkerStopped(env, info.ControllerId, worker);
        if (done) {
            // Advance the simulated scheduler while the local DDL completes;
            // wall-clock polling alone cannot deliver its scheduled events.
            runtime.WaitFor("deferred DONE destination cleanup", [&] { return destinationAltered; }, TDuration::Seconds(30));
            AssertEventuallyDone(env, info);
        } else {
            WaitForPaused(env, info);
        }
    }

    Y_UNIT_TEST_TWIN(ChangedCredentialsBeforeDeferredReset, ResourceId) {
        CheckDeferredAlter(ResourceId, false);
    }

    Y_UNIT_TEST_TWIN(SameCredentialsDuringDeferredDone, ResourceId) {
        CheckDeferredAlter(ResourceId, true);
    }

    Y_UNIT_TEST(DropDuringDeferredDoneResolvesFreshSecret) {
        CheckDeferredAlter(false, true, true);
    }

    Y_UNIT_TEST(DropAfterDoneResolvesFreshSecret) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        const auto info = StartPausedReplicationWithSourceStreams(env);
        TCredentialResultGate credentials(env);
        auto done = MakeResolutionRequest(info, 134, false);
        done->Record.MutableSwitchState()->MutableDone()->SetFailoverMode(
            NKikimrReplication::TReplicationState::TDone::FAILOVER_MODE_FORCE);
        Alter(env, info, std::move(done));
        AssertEventuallyDone(env, info);
        UNIT_ASSERT(credentials.Pending.empty());
        RestartController(env, info.ControllerId);
        auto drop = MakeHolder<TEvController::TEvDropReplication>();
        info.PathId.ToProto(drop->Record.MutablePathId());
        drop->Record.MutableOperationId()->SetTxId(135);
        env.SendAsync(info.ControllerId, std::move(drop));
        credentials.Wait(1);
        credentials.Deliver(info, 0, true);
        const auto result = runtime.GrabEdgeEvent<TEvController::TEvDropReplicationResult>(env.GetSender(), TDuration::Seconds(30));
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Get()->Record.GetStatus() == NKikimrReplication::TEvDropReplicationResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/table1").GetPathDescription().GetTable().CdcStreamsSize(), 0);
        AssertDestinationWritable(env);
    }
}

Y_UNIT_TEST_SUITE(StreamResultFreshness) {
    void CheckReplacement(bool describeOnly, bool currentFailure) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        CreateSourceTable(env, "bootstrap");
        runtime.GetAppData().ReplicationConfig.MutableSchemeOperationLimits()->SetInflightCreateStreamLimit(1);

        TVector<TAutoPtr<IEventHandle>> pending;
        bool holdResults = !describeOnly;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (!holdResults || ev->Cookie || !ev->HasEvent()) {
                return;
            }

            const auto* result = dynamic_cast<TEvPrivate::TEvCreateStreamResult*>(ev->GetBase());
            if (!result) {
                return;
            }

            UNIT_ASSERT_C(result->IsSuccess(), result->Status.GetIssues().ToString());
            pending.push_back(std::move(ev));
        });
        const auto info = StartReplication(env);
        if (describeOnly) {
            const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
            const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());
            ClearStreamSchemaChanges(env, info.ControllerId, worker);
            holdResults = true;
            RestartController(env, info.ControllerId);
        }
        runtime.WaitFor("obsolete source setup result", [&] { return pending.size() == 1; }, TDuration::Seconds(30));
        const auto* obsolete = pending[0]->Get<TEvPrivate::TEvCreateStreamResult>();
        const TWorkerId worker(obsolete->ReplicationId, obsolete->TargetId, 0);
        const auto obsoleteActor = pending[0]->Sender;
        const auto readStreamState = [&] {
            return ReadPersistedStreamState(runtime, info.ControllerId, worker);
        };
        const auto originalState = readStreamState();
        UNIT_ASSERT_VALUES_EQUAL(
            originalState,
            static_cast<ui32>(describeOnly
                ? TReplication::EStreamState::Ready : TReplication::EStreamState::Creating));

        // PAUSE shuts down the creator while leaving the same stream state.
        // Hold its replacement's real success so an old callback can race it.
        RequestPause(env, info, 160);
        const auto sendResult = [&](const TActorId& actor, NYdb::EStatus status) {
            runtime.Send(new IEventHandle(
                ResolveTablet(runtime, info.ControllerId),
                actor,
                new TEvPrivate::TEvCreateStreamResult(
                    worker.ReplicationId(),
                    worker.TargetId(),
                    NYdb::TStatus(status, {})),
                0,
                1));
        };
        const auto assertUnchanged = [&] {
            UNIT_ASSERT(!DescribeReplication(env, info)->Get()->Record.GetState().HasError());
            UNIT_ASSERT_VALUES_EQUAL(readStreamState(), originalState);
            UNIT_ASSERT(!ReadStreamSchemaChanges(env, info.ControllerId, worker).has_value());
        };
        // Even an obsolete result must release its own limiter permit. With a
        // limit of one, the replacement cannot finish before this callback.
        sendResult(obsoleteActor, NYdb::EStatus::SCHEME_ERROR);
        assertUnchanged();
        runtime.WaitFor("replacement source setup result", [&] { return pending.size() == 2; }, TDuration::Seconds(30));
        const auto currentActor = pending[1]->Sender;
        UNIT_ASSERT(currentActor != obsoleteActor);
        holdResults = false;
        runtime.Send(pending[0].Release());
        assertUnchanged();
        sendResult(obsoleteActor, NYdb::EStatus::SUCCESS);
        assertUnchanged();

        if (currentFailure) {
            sendResult(currentActor, NYdb::EStatus::SCHEME_ERROR);
            UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());
            UNIT_ASSERT_VALUES_EQUAL(readStreamState(), static_cast<ui32>(TReplication::EStreamState::Error));
            // A duplicate success must not overwrite the current failure.
            runtime.Send(pending[1].Release());
        } else {
            runtime.Send(pending[1].Release());
            runtime.WaitFor(
                "PAUSE after current source setup",
                [&] {
                    return DescribeReplication(env, info)->Get()->Record.GetState().HasPaused();
                },
                TDuration::Seconds(30));
            UNIT_ASSERT(ReadStreamSchemaChanges(env, info.ControllerId, worker).has_value());
            // A duplicate failure must not undo an accepted success.
            sendResult(currentActor, NYdb::EStatus::SCHEME_ERROR);
        }
        const auto assertFinal = [&] {
            const auto state = DescribeReplication(env, info)->Get()->Record.GetState();
            UNIT_ASSERT(currentFailure ? state.HasError() : state.HasPaused());
            UNIT_ASSERT_VALUES_EQUAL(
                readStreamState(),
                static_cast<ui32>(currentFailure
                    ? TReplication::EStreamState::Error : TReplication::EStreamState::Ready));
            const auto capability = ReadStreamSchemaChanges(env, info.ControllerId, worker);
            UNIT_ASSERT(capability.has_value());
            if (currentFailure) {
                UNIT_ASSERT(!*capability);
            }
        };
        assertFinal();
        sendResult(obsoleteActor, NYdb::EStatus::SCHEME_ERROR);
        sendResult(obsoleteActor, NYdb::EStatus::SUCCESS);
        assertFinal();
        RestartController(env, info.ControllerId);
        assertFinal();
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/table1").GetPathDescription().GetTable().CdcStreamsSize(), 1);
    }

    Y_UNIT_TEST_TWIN(CurrentSuccessAfterReplacement, DescribeOnly) {
        CheckReplacement(DescribeOnly, false);
    }

    Y_UNIT_TEST_TWIN(CurrentFailureAfterReplacement, DescribeOnly) {
        CheckReplacement(DescribeOnly, true);
    }
}

Y_UNIT_TEST_SUITE(StreamRetry) {
    void RetryPausedBootstrapWithPublicConfig(bool failConsumer, bool changeCredentials) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        CreateSourceTable(env, "bootstrap");
        ui32 workerStarts = 0;
        ui32 destinationCreators = 0;
        TTestActorRuntimeBase::TRegistrationObserver previousRegistrationObserver;
        previousRegistrationObserver = runtime.SetRegistrationObserverFunc(
            [&](
                TTestActorRuntimeBase& actorRuntime,
                const TActorId& parent,
                const TActorId& actorId)
            {
                previousRegistrationObserver(actorRuntime, parent, actorId);
                const auto activity = actorRuntime.FindActor(actorId)->GetActivityType();
                if (activity != NKikimrServices::TActivity::REPLICATION_CONTROLLER_DST_CREATOR) {
                    return;
                }

                ++destinationCreators;
            });
        Y_DEFER {
            runtime.SetRegistrationObserverFunc(std::move(previousRegistrationObserver));
        };

        bool rejectSource = true;
        bool holdResults = true;
        TVector<TAutoPtr<IEventHandle>> pending;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (ev->GetTypeRewrite() == TEvService::EvRunWorker) {
                ++workerStarts;
            }
            // Reject the real creator's source operation before it commits.
            // Consumer failure leaves the CDC stream itself already created.
            const auto rejectedEvent = failConsumer
                ? TEvYdbProxy::EvAlterTopicRequest : TEvYdbProxy::EvAlterTableRequest;
            if (rejectSource && ev->GetTypeRewrite() == rejectedEvent) {
                auto status = NYdb::TStatus(NYdb::EStatus::UNAUTHORIZED, {});
                IEventBase* response = failConsumer
                    ? static_cast<IEventBase*>(new TEvYdbProxy::TEvAlterTopicResponse(std::move(status)))
                    : static_cast<IEventBase*>(new TEvYdbProxy::TEvAlterTableResponse(std::move(status)));
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response, 0, ev->Cookie));
                ev.Reset();
                rejectSource = false;
                return;
            }
            if (!holdResults) {
                return;
            }

            const auto* result = dynamic_cast<TEvPrivate::TEvCreateStreamResult*>(ev->GetBase());
            if (!result || !result->IsSuccess()) {
                return;
            }

            pending.push_back(std::move(ev));
        });
        const auto info = StartReplication(
            env,
            1,
            changeCredentials ? "" : "root@builtin",
            false,
            false,
            NKikimrSchemeOp::EIndexTypeGlobal,
            true,
            false);
        runtime.WaitFor(
            "failed PAUSED source setup",
            [&] {
                return DescribeReplication(env, info)->Get()->Record.GetState().HasError();
            },
            TDuration::Seconds(30));
        const auto failed = DescribeReplication(env, info);
        UNIT_ASSERT_VALUES_EQUAL(failed->Get()->Record.TargetsSize(), 1);
        const TWorkerId worker(1, failed->Get()->Record.GetTargets(0).GetId(), 0);
        const auto readStream = [&] (TReplication::EStreamState expected) {
            NKikimrMiniKQL::TResult result;
            const auto query = Sprintf(
                R"((
                (let key '('('ReplicationId (Uint64 '1)) '('TargetId (Uint64 '%lu))))
                (return (AsList (SetResult 'Stream (SelectRow 'SrcStreams key '('Name 'ConsumerName 'State)))))
                ))",
                worker.TargetId());
            UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, info.ControllerId, query, result), NKikimrProto::OK);
            const auto& row = result.GetValue().GetStruct(0).GetOptional().GetOptional();
            UNIT_ASSERT_VALUES_EQUAL(row.GetStruct(2).GetOptional().GetUint32(), static_cast<ui32>(expected));
            // MiniKQL struct members are ordered by name: ConsumerName, Name, State.
            return std::make_pair(row.GetStruct(1).GetOptional().GetText(), row.GetStruct(0).GetOptional().GetText());
        };
        const auto stream = readStream(TReplication::EStreamState::Error);
        UNIT_ASSERT_VALUES_EQUAL(stream.second, "replicationConsumer");
        UNIT_ASSERT_VALUES_EQUAL(
            env.GetDescription("/Root/table1").GetPathDescription().GetTable().CdcStreamsSize(),
            failConsumer ? 1 : 0);

        auto client = CreateTableClient(env);
        auto session = CreateTableSession(env, client);
        const auto altered = env.RunCall([&] {
            return session.ExecuteSchemeQuery(R"(
                ALTER ASYNC REPLICATION `/Root/replication` SET (TOKEN = "root@builtin");
            )").GetValueSync();
        });
        UNIT_ASSERT_C(altered.IsSuccess(), altered.GetIssues().ToString());
        runtime.WaitFor(
            "retried source setup result",
            [&] { return pending.size() == 1; },
            TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(readStream(TReplication::EStreamState::Creating), stream);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasPaused());

        // The retry must survive a crash between its durable reset and success.
        RestartController(env, info.ControllerId);
        runtime.WaitFor(
            "retried source setup after restart",
            [&] { return pending.size() == 2; },
            TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(readStream(TReplication::EStreamState::Creating), stream);
        holdResults = false;
        runtime.Send(pending.back().Release());

        const auto assertPaused = [&] {
            const auto description = DescribeReplication(env, info);
            UNIT_ASSERT_C(description->Get()->Record.GetState().HasPaused(), description->Get()->Record.DebugString());
            NKikimrMiniKQL::TResult target;
            const auto query = Sprintf(
                R"((
                (let key '('('ReplicationId (Uint64 '1)) '('Id (Uint64 '%lu))))
                (return (AsList (SetResult 'Target (SelectRow 'Targets key '('DstState 'Issue)))))
                ))",
                worker.TargetId());
            UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, info.ControllerId, query, target), NKikimrProto::OK);
            const auto& row = target.GetValue().GetStruct(0).GetOptional().GetOptional();
            UNIT_ASSERT_VALUES_EQUAL(
                row.GetStruct(0).GetOptional().GetUint32(),
                static_cast<ui32>(TReplication::EDstState::Creating));
            UNIT_ASSERT_VALUES_EQUAL(row.GetStruct(1).GetOptional().GetText(), "");
            UNIT_ASSERT_VALUES_EQUAL(readStream(TReplication::EStreamState::Ready), stream);
            UNIT_ASSERT_VALUES_EQUAL(destinationCreators, 0);
            UNIT_ASSERT_VALUES_EQUAL(workerStarts, 0);
            UNIT_ASSERT_VALUES_EQUAL(
                env.GetDescription("/Root/replica1").GetSchemeStatus(),
                static_cast<int>(NKikimrScheme::StatusPathDoesNotExist));
            UNIT_ASSERT(env.GetDescription("/Root/replication").GetPathDescription()
                .GetReplicationDescription().GetState().HasPaused());
        };
        assertPaused();
        RestartController(env, info.ControllerId);
        assertPaused();
        const auto streams = env.GetDescription("/Root/table1").GetPathDescription().GetTable();
        UNIT_ASSERT_VALUES_EQUAL(streams.CdcStreamsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(streams.GetCdcStreams(0).GetName(), stream.first);
        auto request = MakeHolder<TEvYdbProxy::TEvDescribeTopicRequest>(
            TStringBuilder() << "/Root/table1/" << stream.first,
            NYdb::NTopic::TDescribeTopicSettings());
        const auto topic = env.Send<TEvYdbProxy::TEvDescribeTopicResponse>(env.GetYdbProxy(), std::move(request));
        UNIT_ASSERT_C(topic->Get()->Result.IsSuccess(), topic->Get()->Result.GetIssues().ToString());
        const auto& consumers = topic->Get()->Result.GetTopicDescription().GetConsumers();
        UNIT_ASSERT_VALUES_EQUAL(consumers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(consumers[0].GetConsumerName(), stream.second);
    }

    Y_UNIT_TEST(PausedBootstrapCredentialRetry) {
        RetryPausedBootstrapWithPublicConfig(false, true);
    }

    Y_UNIT_TEST(PausedBootstrapUnchangedCredentialRetryAfterConsumerFailure) {
        RetryPausedBootstrapWithPublicConfig(true, false);
    }

    Y_UNIT_TEST(PausedConfigPreservesDestinationError) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        CreateSourceTable(env, "replica1");
        const auto info = StartReplication(
            env,
            1,
            "root@builtin",
            false,
            false,
            NKikimrSchemeOp::EIndexTypeGlobal,
            true,
            false);
        auto client = CreateTableClient(env);
        auto session = CreateTableSession(env, client);
        const auto alter = [&](const TString& settings) {
            return env.RunCall([&] {
                const TString query = TStringBuilder()
                    << "ALTER ASYNC REPLICATION `/Root/replication` SET (" << settings << ");";
                return session.ExecuteSchemeQuery(query).GetValueSync();
            });
        };
        UNIT_ASSERT(alter("STATE = \"STANDBY\"").IsSuccess());
        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());
        AttachWorkers(env, info.ControllerId, {worker});
        UNIT_ASSERT(alter("STATE = \"PAUSED\"").IsSuccess());
        SendWorkerStopped(env, info.ControllerId, worker);
        runtime.WaitFor(
            "ordinary PAUSE",
            [&] {
                return DescribeReplication(env, info)->Get()->Record.GetState().HasPaused();
            },
            TDuration::Seconds(30));
        // A late failed writer report is a destination error, even though this
        // replication originally bootstrapped with INITIAL_SCAN=false.
        auto error = MakeHolder<TEvService::TEvWorkerStatus>(
            worker,
            NKikimrReplication::TEvWorkerStatus::STATUS_STOPPED,
            NKikimrReplication::TEvWorkerStatus::REASON_ERROR,
            "writer failed");
        env.SendAsync(info.ControllerId, std::move(error));
        runtime.WaitFor(
            "destination error",
            [&] {
                return DescribeReplication(env, info)->Get()->Record.GetState().HasError();
            },
            TDuration::Seconds(30));
        const auto assertError = [&] {
            const auto state = DescribeReplication(env, info)->Get()->Record.GetState();
            UNIT_ASSERT_C(state.HasError(), state.DebugString());
            UNIT_ASSERT_C(state.DebugString().Contains("writer failed"), state.DebugString());
            AssertDestinationMode(
                env,
                "/Root/replica1",
                NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
        };
        UNIT_ASSERT(alter("TOKEN = \"root@builtin\"").IsSuccess());
        assertError();
        RestartController(env, info.ControllerId);
        assertError();
    }

    Y_UNIT_TEST_TWIN(ResetSurvivesRestartBeforeSourceCompletion, Deferred) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        // Initialize local table services before the SDK creates its session.
        CreateSourceTable(env, "bootstrap");
        const auto info = StartReplication(env, Deferred ? 2 : 1);
        auto initialRun = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto barrierWorker = TWorkerId::Parse(initialRun->Get()->Record.GetWorker());
        if constexpr (Deferred) {
            initialRun = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        }
        const auto worker = TWorkerId::Parse(initialRun->Get()->Record.GetWorker());
        const auto schema = MakeSchemaChange();
        if constexpr (Deferred) {
            AttachWorkers(env, info.ControllerId, {barrierWorker, worker});
            env.SendAsync(info.ControllerId, MakeSchemaChangeReport(barrierWorker, schema));
            runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender());
            env.SendAsync(info.ControllerId, MakeSchemaChangeReport(barrierWorker, schema, true));
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender())
                ->Get()->Record.GetApplied());
        } else {
            AttachWorkers(env, info.ControllerId, {worker});
        }

        TVector<TAutoPtr<IEventHandle>> pending;
        bool holdResults = true;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (!holdResults || !ev->HasEvent()) {
                return;
            }

            const auto* result = dynamic_cast<TEvPrivate::TEvCreateStreamResult*>(ev->GetBase());
            if (!result) {
                return;
            }

            UNIT_ASSERT_C(result->IsSuccess(), result->Status.GetIssues().ToString());
            pending.push_back(std::move(ev));
        });
        const auto waitForResults = [&](ui32 count) {
            runtime.WaitFor("source setup reply", [&] { return pending.size() >= count; }, TDuration::Seconds(30));
        };
        const auto readStreamState = [&] {
            return ReadPersistedStreamState(runtime, info.ControllerId, worker);
        };
        const auto readDeferred = [&] {
            const auto query = Sprintf(
                R"((
                (let key '('('Id (Uint64 '%lu))))
                (return (AsList (SetResult 'Replication (SelectRow 'Replications key '('DeferredAlter)))))
            ))",
                worker.ReplicationId());
            const auto result = ReadControllerQuery(runtime, info.ControllerId, query);
            return result.GetValue().GetStruct(0).GetOptional().GetOptional().GetStruct(0).GetOptional().GetBool();
        };

        // A legacy stream requires capability discovery on restart. Fail its
        // real creator's reply so the controller durably records stream Error.
        ClearStreamSchemaChanges(env, info.ControllerId, worker);
        RestartController(env, info.ControllerId);
        waitForResults(1);
        if constexpr (Deferred) {
            AttachWorkers(env, info.ControllerId, {barrierWorker});
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender())
                ->Get()->Record.GetApplied());
        }
        holdResults = false;
        runtime.Send(new IEventHandle(
            ResolveTablet(runtime, info.ControllerId),
            pending[0]->Sender,
            new TEvPrivate::TEvCreateStreamResult(
                worker.ReplicationId(),
                worker.TargetId(),
                NYdb::TStatus(NYdb::EStatus::SCHEME_ERROR, {}))));
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasError());
        holdResults = true;
        UNIT_ASSERT_VALUES_EQUAL(readStreamState(), static_cast<ui32>(TReplication::EStreamState::Error));

        if constexpr (Deferred) {
            // PAUSE recovers the replication but retains the unfinished Applied
            // barrier and failed stream. STANDBY must wait for that barrier.
            RequestPause(env, info, 150);
            UNIT_ASSERT(readDeferred());
            UNIT_ASSERT_VALUES_EQUAL(readStreamState(), static_cast<ui32>(TReplication::EStreamState::Error));
        }
        auto resume = MakeAlterReplicationRequest(info, 151);
        resume->Record.MutableSwitchState()->MutableStandBy();
        const auto altered = env.Send<TEvController::TEvAlterReplicationResult>(info.ControllerId, std::move(resume));
        UNIT_ASSERT_VALUES_EQUAL(altered->Get()->Record.GetStatus(), NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        if constexpr (Deferred) {
            UNIT_ASSERT(readDeferred());
            UNIT_ASSERT_VALUES_EQUAL(readStreamState(), static_cast<ui32>(TReplication::EStreamState::Error));
            env.SendAsync(info.ControllerId, MakeSchemaChangeReport(barrierWorker, schema, false, true));
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvService::TEvSchemaChangeResult>(env.GetSender())
                ->Get()->Record.GetCompleted());
        }
        waitForResults(2);
        UNIT_ASSERT(!readDeferred());
        UNIT_ASSERT_VALUES_EQUAL(readStreamState(), static_cast<ui32>(TReplication::EStreamState::Creating));

        // Crash after the reset commits, before the source result can commit.
        // Recovery must start a new creator, rather than reload stream Error.
        RestartController(env, info.ControllerId);
        env.SendAsync(info.ControllerId, new TEvService::TEvStatus());
        waitForResults(3);
        UNIT_ASSERT_VALUES_EQUAL(readStreamState(), static_cast<ui32>(TReplication::EStreamState::Creating));
        holdResults = false;
        runtime.Send(pending.back().Release());
        auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        if constexpr (Deferred) {
            if (TWorkerId::Parse(run->Get()->Record.GetWorker()) == barrierWorker) {
                run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(run->Get()->Record.GetWorker()), worker);
        UNIT_ASSERT_VALUES_EQUAL(readStreamState(), static_cast<ui32>(TReplication::EStreamState::Ready));
        UNIT_ASSERT(ReadStreamSchemaChanges(env, info.ControllerId, worker).has_value());
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
        for (ui32 i = 1; i <= (Deferred ? 2 : 1); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                env.GetDescription(Sprintf("/Root/table%u", i))
                    .GetPathDescription().GetTable().CdcStreamsSize(),
                1);
        }
        UNIT_ASSERT_VALUES_EQUAL(
            run->Get()->Record.GetCommand().GetRemoteTopicReader().SerializeAsString(),
            initialRun->Get()->Record.GetCommand().GetRemoteTopicReader().SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(
            run->Get()->Record.GetCommand().GetLocalTableWriter().SerializeAsString(),
            initialRun->Get()->Record.GetCommand().GetLocalTableWriter().SerializeAsString());
    }
}

Y_UNIT_TEST_SUITE(AttachmentLifecycle) {
    Y_UNIT_TEST(PublicDoneRetriesResourceExhaustionAcrossRestart) {
        TTestEnv env(true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        CreateSourceTable(env, "bootstrap");
        const auto info = StartReplication(env);
        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());
        AttachWorkers(env, info.ControllerId, {worker});
        const auto destination = env.GetPathId("/Root/replica1");
        AssertDestinationMode(
            env,
            "/Root/replica1",
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);

        bool exhausted = true;
        ui32 rejected = 0;
        auto observer = runtime.AddObserver<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>(
            [&](NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction::TPtr& ev) {
                if (!exhausted) {
                    return;
                }
                for (const auto& tx : ev->Get()->Record.GetTransaction()) {
                    if (tx.GetOperationType() != NKikimrSchemeOp::ESchemeOpAlterTable) {
                        continue;
                    }

                    const auto& alter = tx.GetAlterTable();
                    const auto mode = alter.GetReplicationConfig().GetMode();
                    if (!alter.HasReplicationConfig()
                        || mode != NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE
                        || alter.GetPathId().GetLocalId() != destination.LocalPathId)
                    {
                        continue;
                    }

                    // Reject before the DDL is submitted, as SchemeShard's
                    // in-flight limit does in ProcessOperationParts.
                    runtime.EnableScheduleForActor(ev->Sender);
                    auto* response = new NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult(
                        NKikimrScheme::StatusResourceExhausted,
                        ev->Get()->Record.GetTxId(),
                        destination.OwnerId,
                        "injected in-flight operation limit");
                    runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response));
                    ++rejected;
                    ev.Reset();
                    return;
                }
            });

        auto client = CreateTableClient(env);
        auto session = CreateTableSession(env, client);
        const auto done = [&] {
            return env.RunCall([&] {
                return session.ExecuteSchemeQuery(R"(
                    ALTER ASYNC REPLICATION `/Root/replication`
                    SET (STATE = "DONE", FAILOVER_MODE = "FORCE");
                )").GetValueSync();
            });
        };
        const auto accepted = done();
        UNIT_ASSERT_C(accepted.IsSuccess(), accepted.GetIssues().ToString());
        SendWorkerStopped(env, info.ControllerId, worker);
        runtime.WaitFor(
            "destination detach retry",
            [&] { return rejected >= 2; },
            TDuration::Seconds(60));
        const auto state = DescribeReplication(env, info)->Get()->Record.GetState();
        UNIT_ASSERT_C(!state.HasError() && !state.HasDone(), state.DebugString());
        // Public SchemeShard state is already terminal: another DONE cannot
        // recover cleanup. The accepted operation must make progress itself.
        UNIT_ASSERT(env.GetDescription("/Root/replication").GetPathDescription()
            .GetReplicationDescription().GetState().HasDone());
        const auto repeated = done();
        UNIT_ASSERT(!repeated.IsSuccess());

        RestartController(env, info.ControllerId);
        const auto beforeRestart = rejected;
        runtime.WaitFor(
            "destination detach retry after restart",
            [&] {
                return rejected > beforeRestart;
            },
            TDuration::Seconds(60));
        exhausted = false;
        runtime.WaitFor(
            "completed destination detach",
            [&] {
                return DescribeReplication(env, info)->Get()->Record.GetState().HasDone();
            },
            TDuration::Seconds(60));
        AssertDestinationWritable(env);
        UNIT_ASSERT_VALUES_EQUAL(env.GetPathId("/Root/replica1"), destination);
        const auto write = env.RunCall([&] {
            return session.ExecuteDataQuery(
                "UPSERT INTO `/Root/replica1` (key, value) VALUES (1, 'writable');",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        RestartController(env, info.ControllerId);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasDone());
        AssertDestinationWritable(env);
    }

    Y_UNIT_TEST_TWIN(ObsoletePauseResultDuringDone, Restart) {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        const auto pending = StartPendingAttachment(env);
        const auto& info = pending.Info;
        // Accept the real attachment completion for the restored table before
        // testing PAUSE followed by DONE on that same destination.
        env.SendAsync(ResolveTablet(runtime, info.ControllerId), new TEvents::TEvWakeup(pending.TargetId));
        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        const auto worker = TWorkerId::Parse(run->Get()->Record.GetWorker());
        AttachWorkers(env, info.ControllerId, {worker});
        const auto controller = InstallControllerGate(
            env,
            info.ControllerId,
            [](THolder<IActor> actor, TActorId notify) {
                return new TDstAlterResultGate(std::move(actor), notify);
            });
        RequestPause(env, info, 130);
        UNIT_ASSERT_C(
            runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender(), TDuration::Seconds(30)),
            "PAUSE did not stop the worker");
        SendWorkerStopped(env, info.ControllerId, worker);
        const auto paused = runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender(), TDuration::Seconds(30));
        UNIT_ASSERT_C(paused, "PAUSE destination result was not captured");
        UNIT_ASSERT_VALUES_EQUAL(paused->Get()->Tag, TDstAlterResultGate::Blocked);
        const auto obsolete = paused->Sender;

        // Hold the replacement before it can submit detach DDL.
        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        runtime.RegisterService(MakeTxProxyID(), env.GetSender());
        Y_DEFER { runtime.RegisterService(MakeTxProxyID(), txProxy); };
        const auto altered = env.Send<TEvController::TEvAlterReplicationResult>(
            info.ControllerId,
            MakeDoneRequest(info, 131));
        UNIT_ASSERT_VALUES_EQUAL(
            altered->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        const auto allocation = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvAllocateTxId>(env.GetSender(), TDuration::Seconds(30));
        UNIT_ASSERT_C(allocation, "Replacement DONE actor did not request an allocation");
        // Keep only this request delayed; table descriptions also use TxProxy.
        runtime.RegisterService(MakeTxProxyID(), txProxy);
        UNIT_ASSERT(allocation->Sender != obsolete);
        env.SendAsync(controller, new TEvents::TEvWakeup(TDstAlterResultGate::Release));
        UNIT_ASSERT(!DescribeReplication(env, info)->Get()->Record.GetState().HasDone());
        for (const bool success : {false, true}) {
            runtime.Send(new IEventHandle(
                controller,
                obsolete,
                success
                    ? new TEvPrivate::TEvAlterDstResult(1, worker.TargetId())
                    : new TEvPrivate::TEvAlterDstResult(
                        1,
                        worker.TargetId(),
                        NKikimrScheme::StatusSchemeError,
                        "obsolete PAUSE failure")));
            const auto state = DescribeReplication(env, info)->Get()->Record.GetState();
            UNIT_ASSERT(!state.HasDone());
            UNIT_ASSERT(!state.HasError());
        }
        AssertDestinationMode(
            env,
            "/Root/replica1",
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);

        if constexpr (Restart) {
            RestartController(env, info.ControllerId);
        } else {
            runtime.Send(allocation->Forward(txProxy));
        }
        AssertEventuallyDone(env, info);
        AssertDestinationWritable(env);
        UNIT_ASSERT_VALUES_EQUAL(env.GetPathId("/Root/replica1"), pending.DstPathId);
        auto client = CreateTableClient(env);
        const auto write = client.RetryOperationSync([](NYdb::NTable::TSession session) {
            return session.ExecuteDataQuery(
                "UPSERT INTO `/Root/replica1` (key, value) VALUES (1, 'writable');",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        runtime.Send(new IEventHandle(
            ResolveTablet(runtime, info.ControllerId),
            obsolete,
            new TEvPrivate::TEvAlterDstResult(
                1,
                worker.TargetId(),
                NKikimrScheme::StatusSchemeError,
                "duplicate obsolete failure")));
        AssertEventuallyDone(env, info);
        RestartController(env, info.ControllerId);
        AssertEventuallyDone(env, info);
        AssertDestinationWritable(env);
    }

    Y_UNIT_TEST(ObsoleteDestinationCreatorResults) {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        CreateSourceTable(env, "replica1");
        const auto info = StartReplication(env);
        UNIT_ASSERT(WaitFor([&] {
            return DescribeReplication(env, info)->Get()->Record.TargetsSize() == 1;
        }));
        const auto description = DescribeReplication(env, info);
        UNIT_ASSERT_VALUES_EQUAL(description->Get()->Record.TargetsSize(), 1);
        const auto targetId = description->Get()->Record.GetTargets(0).GetId();
        UNIT_ASSERT(WaitFor([&] {
            return ReadStreamSchemaChanges(env, info.ControllerId, TWorkerId(1, targetId, 0)).has_value();
        }));
        const auto controller = InstallControllerGate(
            env,
            info.ControllerId,
            [](THolder<IActor> actor, TActorId notify) {
                return new TDstCreatorResultGate(std::move(actor), notify);
            });
        const auto resume = [&](ui64 txId) {
            auto request = MakeAlterReplicationRequest(info, txId);
            request->Record.MutableSwitchState()->MutableStandBy();
            UNIT_ASSERT_VALUES_EQUAL(
                env.Send<TEvController::TEvAlterReplicationResult>(
                    info.ControllerId,
                    std::move(request))->Get()->Record.GetStatus(),
                NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
            return runtime.GrabEdgeEvent<TEvPrivate::TEvPrepareAttachDst>(env.GetSender());
        };
        const auto obsolete = resume(140);
        const auto current = resume(141);
        UNIT_ASSERT(obsolete->Sender != current->Sender);
        const TPendingAttachment pending{info, targetId, current->Get()->DstPathId, current->Sender};

        // The real preparation from the canceled creator must not persist a path
        // or acknowledge DDL. Its late result must not finish replacement setup.
        env.SendAsync(controller, new TEvents::TEvWakeup(TDstCreatorResultGate::ReleasePrepare));
        DescribeReplication(env, info);
        AssertAttachmentPaths(env, pending, {});
        const auto deliverObsolete = [&] {
            for (const bool success : {false, true}) {
                runtime.Send(new IEventHandle(
                    controller,
                    obsolete->Sender,
                    success
                        ? new TEvPrivate::TEvCreateDstResult(1, targetId, pending.DstPathId)
                        : new TEvPrivate::TEvCreateDstResult(
                            1,
                            targetId,
                            NKikimrScheme::StatusSchemeError,
                            "obsolete creator failure")));
                UNIT_ASSERT(!DescribeReplication(env, info)->Get()->Record.GetState().HasError());
            }
        };
        deliverObsolete();
        AssertAttachmentPaths(env, pending, {});

        env.SendAsync(controller, new TEvents::TEvWakeup(TDstCreatorResultGate::ReleasePrepare));
        const auto attached = runtime.GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(attached->Sender, current->Sender);
        AssertAttachmentPaths(env, pending, pending.DstPathId);
        deliverObsolete();
        AssertAttachmentPaths(env, pending, pending.DstPathId);
        env.SendAsync(controller, new TEvents::TEvWakeup(TDstCreatorResultGate::ReleaseResult));
        runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        AssertAttachmentPaths(env, pending, {}, pending.DstPathId);
        SendAttachmentResult(
            env,
            pending,
            new TEvPrivate::TEvCreateDstResult(
                1,
                targetId,
                NKikimrScheme::StatusSchemeError,
                "duplicate current creator failure"));
        UNIT_ASSERT(!DescribeReplication(env, info)->Get()->Record.GetState().HasError());
        RestartController(env, info.ControllerId);
        AssertAttachmentPaths(env, pending, {}, pending.DstPathId);
        UNIT_ASSERT(!DescribeReplication(env, info)->Get()->Record.GetState().HasError());
    }

    Y_UNIT_TEST_TWIN(StreamLimiterCancellation, Create) {
        using TCancel = std::conditional_t<
            Create,
            TEvPrivate::TEvCancelCreateStream,
            TEvPrivate::TEvCancelDropStream>;
        using TAllow = std::conditional_t<
            Create,
            TEvPrivate::TEvAllowCreateStream,
            TEvPrivate::TEvAllowDropStream>;
        using TRequest = std::conditional_t<
            Create,
            TEvPrivate::TEvRequestCreateStream,
            TEvPrivate::TEvRequestDropStream>;
        using TResult = std::conditional_t<
            Create,
            TEvPrivate::TEvCreateStreamResult,
            TEvPrivate::TEvDropStreamResult>;
        TTestBasicRuntime runtime;
        TAppPrepare app;
        app.AddDomain(TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
            "Root",
            TTestTxConfig::DomainUid,
            TTestTxConfig::SchemeShard,
            50,
            TVector<ui64>{},
            TVector<ui64>{},
            TVector<ui64>{TTestTxConfig::TxAllocator}).Release());
        app.AddHive(0);
        SetupTabletServices(runtime, &app);
        const auto sender = runtime.AllocateEdgeActor();
        auto& limits = *runtime.GetAppData().ReplicationConfig.MutableSchemeOperationLimits();
        limits.SetInflightCreateStreamLimit(1);
        limits.SetInflightDropStreamLimit(1);
        const ui64 controllerId = TTestTxConfig::TxTablet0;
        const auto instance = StartTestTablet(
            runtime,
            CreateTestTabletInfo(controllerId, TTabletTypes::ReplicationController),
            &CreateController);
        runtime.EnableScheduleForActor(instance);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvRestored, 1);
        runtime.DispatchEvents(options);
        const auto controller = ResolveTablet(runtime, controllerId);
        const auto proxy = runtime.AllocateEdgeActor();
        THashSet<TActorId> requested;
        THashSet<TActorId> cancelled;
        THashMap<TActorId, ui32> grants;
        auto observer = runtime.AddObserver([&](IEventHandle::TPtr& ev) {
            if (!ev->HasEvent()) {
                return;
            }
            if (ev->GetRecipientRewrite() == controller) {
                if (dynamic_cast<TRequest*>(ev->GetBase())) {
                    requested.insert(ev->Sender);
                }
                if (dynamic_cast<TCancel*>(ev->GetBase())) {
                    cancelled.insert(ev->Sender);
                }
            }
            if (ev->Sender == controller && dynamic_cast<TAllow*>(ev->GetBase())) {
                ++grants[ev->GetRecipientRewrite()];
            }
        });
        const auto start = [&](ui64 tid) {
            if constexpr (Create) {
                return runtime.Register(CreateStreamCreator(
                    controller,
                    proxy,
                    999,
                    tid,
                    std::make_shared<TTargetTable::TTableConfig>("/Root/table1", "/Root/replica1"),
                    "stream",
                    "consumer",
                    TDuration::Hours(1)));
            } else {
                return runtime.Register(CreateStreamRemover(
                    controller,
                    proxy,
                    999,
                    tid,
                    TReplication::ETargetKind::Table,
                    "/Root/table1",
                    "stream"));
            }
        };
        const auto cancel = [&](const TActorId& actor) {
            runtime.Send(actor, sender, new TEvents::TEvPoison());
            runtime.WaitFor(
                "stream actor cancellation",
                [&] { return cancelled.contains(actor); },
                TDuration::Seconds(30));
        };
        const auto first = start(1);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTableRequest>(proxy)->Sender, first);
        const auto queued = start(2);
        runtime.WaitFor(
            "queued stream permission request",
            [&] { return requested.contains(queued); },
            TDuration::Seconds(30));
        // Poison while waiting for permission, before the first actor releases it.
        cancel(queued);
        cancel(first);
        const auto replacement = start(3);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTableRequest>(proxy)->Sender, replacement);
        UNIT_ASSERT_VALUES_EQUAL(grants[queued], 0);

        const auto next = start(4);
        // A duplicate cancellation or a late result belongs only to the old
        // actor. Neither may release the replacement's active permit.
        runtime.Send(controller, first, new TCancel());
        runtime.Send(
            controller,
            first,
            new TResult(
                999,
                1,
                NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues())));
        UNIT_ASSERT(!runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTableRequest>(proxy, TDuration::MilliSeconds(100)));
        UNIT_ASSERT_VALUES_EQUAL(grants[next], 0);
        cancel(replacement);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvYdbProxy::TEvAlterTableRequest>(proxy)->Sender, next);
        cancel(next);
    }

    Y_UNIT_TEST_TWIN(StateAlterDuringPausedStreamCreation, Resume) {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        runtime.GetAppData().ReplicationConfig.MutableSchemeOperationLimits()->SetInflightCreateStreamLimit(1);
        for (ui32 i = 1; i <= 3; ++i) {
            CreateSourceTable(env, Sprintf("replica%u", i));
        }
        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        runtime.RegisterService(
            MakeTxProxyID(),
            runtime.Register(
                new TCreateCdcStreamRequestGate(txProxy, env.GetSender())));
        Y_DEFER {
            runtime.RegisterService(MakeTxProxyID(), txProxy);
        };
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        const auto info = StartReplication(env, 3);
        auto oldRequest = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender());
        runtime.RegisterService(MakeTxProxyID(), txProxy);

        auto request = MakeAlterReplicationRequest(info, 123);
        if constexpr (Resume) {
            request->Record.MutableSwitchState()->MutableStandBy();
        } else {
            request->Record.MutableSwitchState()->MutablePaused();
        }
        const auto altered = env.Send<TEvController::TEvAlterReplicationResult>(info.ControllerId, std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(altered->Get()->Record.GetStatus(), NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        const auto description = DescribeReplication(env, info);
        UNIT_ASSERT_VALUES_EQUAL(description->Get()->Record.TargetsSize(), 3);
        for (const auto& target : description->Get()->Record.GetTargets()) {
            UNIT_ASSERT_C(
                WaitFor([&] {
                    return ReadStreamSchemaChanges(env, info.ControllerId, TWorkerId(1, target.GetId(), 0)).has_value();
                }),
                "State ALTER stranded source setup behind a cancelled stream creator");
        }
        if constexpr (Resume) {
            THashSet<ui64> started;
            while (started.size() != 3) {
                const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
                started.insert(TWorkerId::Parse(run->Get()->Record.GetWorker()).TargetId());
            }
        } else {
            WaitForPaused(env, info);
        }
        // The original DDL can commit after cancellation. Its response goes to
        // the dead creator and cannot consume another actor's permission.
        runtime.Send(oldRequest->Forward(txProxy));
        const auto final = DescribeReplication(env, info);
        UNIT_ASSERT_VALUES_EQUAL(final->Get()->Record.GetState().HasStandBy(), Resume);
        UNIT_ASSERT_VALUES_EQUAL(final->Get()->Record.GetState().HasPaused(), !Resume);
    }

    void PauseAfterMultiTargetAttachmentError(bool restart, bool reverseCompletion) {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        for (ui32 i = 1; i <= 3; ++i) {
            CreateSourceTable(env, Sprintf("replica%u", i));
        }
        const auto starts = std::make_shared<std::atomic<ui32>>(0);
        const auto info = StartReplication(
            env,
            3,
            "root@builtin",
            false,
            false,
            NKikimrSchemeOp::EIndexTypeGlobal,
            false,
            false,
            starts);
        WaitForPaused(env, info);
        // Let real source setup finish before resuming. Cancelling an inflight
        // stream creator exercises a separate limiter lifecycle problem.
        UNIT_ASSERT(WaitFor([&] {
            return SourceStreamsReady(env, info, 3);
        }));
        const auto gate = InstallControllerGate(
            env,
            info.ControllerId,
            [](THolder<IActor> actor, TActorId notify) {
                return new TAttachmentResultGate(std::move(actor), notify);
            });

        auto resume = MakeAlterReplicationRequest(info, 120);
        resume->Record.MutableSwitchState()->MutableStandBy();
        UNIT_ASSERT_VALUES_EQUAL(
            env.Send<TEvController::TEvAlterReplicationResult>(
                info.ControllerId,
                std::move(resume))->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);

        TVector<ui64> targets;
        THashMap<ui64, TActorId> creators;
        for (ui32 i = 0; i < 3; ++i) {
            const auto attached = runtime.GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
            targets.push_back(attached->Get()->TargetId);
            creators[attached->Get()->TargetId] = attached->Sender;
        }
        Sort(targets);
        const auto failed = targets[0];
        const auto first = targets[reverseCompletion ? 2 : 1];
        const auto last = targets[reverseCompletion ? 1 : 2];
        // A creator may fail its final source validation after the local ALTER.
        // The other two real attachment results are still pending at the controller.
        runtime.Send(new IEventHandle(
            ResolveTablet(runtime, info.ControllerId),
            creators.at(failed),
            new TEvPrivate::TEvCreateDstResult(
                1,
                failed,
                NKikimrScheme::StatusSchemeError,
                "injected final validation failure")));
        UNIT_ASSERT(WaitFor([&] {
            return DescribeReplication(env, info)->Get()->Record.GetState().HasError();
        }));
        RequestPause(env, info, 121);

        const auto state = [&](ui64 target) {
            return ReadPersistedDstState(runtime, info.ControllerId, TWorkerId(1, target, 0));
        };
        UNIT_ASSERT(WaitFor([&] { return state(failed) == TReplication::EDstState::Paused; }));
        env.SendAsync(gate, new TEvents::TEvWakeup(first));
        UNIT_ASSERT(WaitFor([&] { return state(first) != TReplication::EDstState::Attaching; }));
        // Keep the last target pending long enough for a mistakenly Ready sibling
        // to register and start a real worker, including after Error -> PAUSE.
        Sleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(starts->load(), 0);
        UNIT_ASSERT(state(first) == TReplication::EDstState::Paused);
        UNIT_ASSERT(state(last) == TReplication::EDstState::Attaching);

        if (restart) {
            RestartController(env, info.ControllerId);
        } else {
            env.SendAsync(gate, new TEvents::TEvWakeup(last));
        }
        WaitForPaused(env, info);
        UNIT_ASSERT(state(last) == TReplication::EDstState::Paused);

        NYdb::NTable::TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        for (ui32 i = 1; i <= 3; ++i) {
            const auto write = session.ExecuteDataQuery(
                Sprintf(
                    "UPSERT INTO `/Root/table%u` (key, value) VALUES (1, 'after pause');",
                    i),
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        }
        // Exercise the real service/writer path, then verify that PAUSED stays
        // quiescent through a second controller recovery as well.
        for (ui32 attempt = 0; attempt < 10; ++attempt) {
            for (ui32 i = 1; i <= 3; ++i) {
                const auto read = session.ExecuteDataQuery(
                    Sprintf(
                        "SELECT key FROM `/Root/replica%u`;",
                        i),
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(read.GetResultSet(0).RowsCount(), 0);
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        RestartController(env, info.ControllerId);
        WaitForPaused(env, info);
        UNIT_ASSERT_VALUES_EQUAL(starts->load(), 0);

        auto standby = MakeAlterReplicationRequest(info, 122);
        standby->Record.MutableSwitchState()->MutableStandBy();
        UNIT_ASSERT_VALUES_EQUAL(
            env.Send<TEvController::TEvAlterReplicationResult>(
                info.ControllerId,
                std::move(standby))->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        // Prove that the real workers can start and replay the rows written
        // during PAUSE, including recovery of the failed target's pending path.
        for (ui32 i = 1; i <= 3; ++i) {
            UNIT_ASSERT_C(
                WaitFor(
                    [&] {
                        const auto read = session.ExecuteDataQuery(
                            Sprintf(
                                "SELECT key FROM `/Root/replica%u`;",
                                i),
                            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                        // Recovery serializes another attachment ALTER. Its new schema
                        // version can invalidate the SELECT cached while paused.
                        if (read.GetStatus() == NYdb::EStatus::SCHEME_ERROR || read.GetStatus() == NYdb::EStatus::ABORTED) {
                            return false;
                        }
                        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
                        return read.GetResultSet(0).RowsCount() == 1;
                    },
                    100),
                "Replication did not resume for target " << i);
        }
        UNIT_ASSERT(starts->load() >= 3);
    }

    Y_UNIT_TEST_TWIN(PauseAfterMultiTargetAttachmentError, ReverseCompletion) {
        PauseAfterMultiTargetAttachmentError(false, ReverseCompletion);
    }

    Y_UNIT_TEST_TWIN(PauseAfterMultiTargetAttachmentErrorAndRestart, ReverseCompletion) {
        PauseAfterMultiTargetAttachmentError(true, ReverseCompletion);
    }

    enum class EAttachmentAlter {
        Pause,
        StandBy,
        Credentials,
    };

    void AlterDuringAttachmentRevalidation(EAttachmentAlter alter) {
        TTestEnv env;
        const auto pending = StartPendingAttachment(env);
        const auto& info = pending.Info;
        auto& runtime = env.GetRuntime();

        const auto schemeCache = runtime.GetLocalServiceId(MakeSchemeCacheID());
        const auto gate = runtime.Register(new TDescribePathRequestGate(
            schemeCache,
            env.GetSender(),
            "/Root/table1"));
        runtime.RegisterService(MakeSchemeCacheID(), gate);
        Y_DEFER {
            runtime.RegisterService(MakeSchemeCacheID(), schemeCache);
        };

        RestartController(env, info.ControllerId);
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TDescribePathRequestGate::RequestBlockedTag());

        auto resume = MakeAlterReplicationRequest(info, 109);
        resume->Record.MutableSwitchState()->MutableStandBy();
        const auto resumed = env.Send<TEvController::TEvAlterReplicationResult>(
            info.ControllerId,
            std::move(resume));
        UNIT_ASSERT_VALUES_EQUAL(
            resumed->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TDescribePathRequestGate::RequestBlockedTag());

        RestartController(env, info.ControllerId);
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TDescribePathRequestGate::RequestBlockedTag());
        AssertAttachmentPaths(env, pending, pending.DstPathId);

        auto request = MakeAlterReplicationRequest(info, 110);
        switch (alter) {
        case EAttachmentAlter::Pause:
            request->Record.MutableSwitchState()->MutablePaused();
            break;
        case EAttachmentAlter::StandBy:
            request->Record.MutableSwitchState()->MutableStandBy();
            break;
        case EAttachmentAlter::Credentials:
            // A credential-only update takes the !alter branch in the controller.
            request->Record.MutableConfig()->MutableSrcConnectionParams()->MutableOAuthToken()->SetToken("");
            break;
        }
        const auto result = env.Send<TEvController::TEvAlterReplicationResult>(
            info.ControllerId,
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(
            result->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        // The retained creator must issue a fresh request through the new proxy.
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TDescribePathRequestGate::RequestBlockedTag());
        AssertAttachmentPaths(env, pending, pending.DstPathId);

        runtime.RegisterService(MakeSchemeCacheID(), schemeCache);
        env.SendAsync(gate, new TEvents::TEvWakeup(TDescribePathRequestGate::ReleaseRequestTag()));
        UNIT_ASSERT_C(
            WaitFor([&] {
                const auto state = DescribeReplication(env, info)->Get()->Record.GetState();
                return alter == EAttachmentAlter::Pause ? state.HasPaused() : state.HasStandBy();
            }),
            "Attachment did not finish after replacing the source proxy");
        // Ready can already be the replication state while the target is attaching.
        UNIT_ASSERT_C(
            WaitFor([&] {
                const auto expectedState = alter == EAttachmentAlter::Pause
                    ? TReplication::EDstState::Paused : TReplication::EDstState::Ready;
                const auto state = ReadPersistedDstState(runtime, info.ControllerId, TWorkerId(1, pending.TargetId, 0));
                return state == expectedState;
            }),
            "Destination attachment did not complete");
        AssertAttachmentPaths(env, pending, {}, pending.DstPathId);
        AssertDestinationMode(
            env,
            "/Root/replica1",
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);

        if (alter == EAttachmentAlter::Pause) {
            auto standby = MakeAlterReplicationRequest(info, 111);
            standby->Record.MutableSwitchState()->MutableStandBy();
            const auto result = env.Send<TEvController::TEvAlterReplicationResult>(
                info.ControllerId,
                std::move(standby));
            UNIT_ASSERT_VALUES_EQUAL(
                result->Get()->Record.GetStatus(),
                NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
            UNIT_ASSERT(WaitFor([&] {
                return DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy();
            }));
            AssertAttachmentPaths(env, pending, {}, pending.DstPathId);
        }
    }

    Y_UNIT_TEST(PauseDuringAttachmentRevalidationAfterRestart) {
        AlterDuringAttachmentRevalidation(EAttachmentAlter::Pause);
    }

    Y_UNIT_TEST(StandByDuringAttachmentRevalidationAfterRestart) {
        AlterDuringAttachmentRevalidation(EAttachmentAlter::StandBy);
    }

    Y_UNIT_TEST(CredentialsDuringAttachmentRevalidationAfterRestart) {
        AlterDuringAttachmentRevalidation(EAttachmentAlter::Credentials);
    }

    void CredentialsDuringPausedStreamSetup(bool describeCapability) {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        runtime.GetAppData().ReplicationConfig.MutableSchemeOperationLimits()->SetInflightCreateStreamLimit(1);
        CreateSourceTable(env, "replica1");
        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        const auto createGate = runtime.Register(new TCreateCdcStreamRequestGate(txProxy, env.GetSender()));
        runtime.RegisterService(MakeTxProxyID(), createGate);
        const auto schemeCache = runtime.GetLocalServiceId(MakeSchemeCacheID());
        Y_DEFER {
            runtime.RegisterService(MakeTxProxyID(), txProxy);
            runtime.RegisterService(MakeSchemeCacheID(), schemeCache);
        };
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        const auto info = StartReplication(env);
        auto createStream = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender());
        runtime.RegisterService(MakeTxProxyID(), txProxy);

        TActorId describeGate;
        if (describeCapability) {
            describeGate = runtime.Register(new TDescribePathRequestGate(
                schemeCache,
                env.GetSender(),
                "/Root/table1"));
            runtime.RegisterService(MakeSchemeCacheID(), describeGate);
            // Commit source DDL, then hold the capability read that keeps the
            // target in Creating and returns before destination progress.
            runtime.Send(createStream->Forward(txProxy));
            UNIT_ASSERT_VALUES_EQUAL(
                runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
                TDescribePathRequestGate::RequestBlockedTag());
        }

        auto request = MakeAlterReplicationRequest(info, 112);
        request->Record.MutableConfig()->MutableSrcConnectionParams()->MutableOAuthToken()->SetToken("");
        const auto result = env.Send<TEvController::TEvAlterReplicationResult>(
            info.ControllerId,
            std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(
            result->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        if (describeCapability) {
            UNIT_ASSERT_VALUES_EQUAL(
                runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
                TDescribePathRequestGate::RequestBlockedTag());
            runtime.RegisterService(MakeSchemeCacheID(), schemeCache);
            env.SendAsync(
                describeGate,
                new TEvents::TEvWakeup(TDescribePathRequestGate::ReleaseRequestTag()));
        }

        const auto description = DescribeReplication(env, info);
        UNIT_ASSERT_VALUES_EQUAL(description->Get()->Record.TargetsSize(), 1);
        const TWorkerId worker(1, description->Get()->Record.GetTargets(0).GetId(), 0);
        UNIT_ASSERT_C(
            WaitFor([&] {
                return ReadStreamSchemaChanges(env, info.ControllerId, worker).has_value();
            }),
            "Credential update stranded initial PAUSED source setup");
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasPaused());
        AssertDestinationWritable(env);

        if (!describeCapability) {
            // The old source request can still reach SchemeShard after the
            // refreshed actor has completed. It must not undo that completion.
            runtime.Send(createStream->Forward(txProxy));
        }

        auto resume = MakeAlterReplicationRequest(info, 113);
        resume->Record.MutableSwitchState()->MutableStandBy();
        const auto resumed = env.Send<TEvController::TEvAlterReplicationResult>(
            info.ControllerId,
            std::move(resume));
        UNIT_ASSERT_VALUES_EQUAL(
            resumed->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        const auto run = runtime.GrabEdgeEvent<TEvService::TEvRunWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(run->Get()->Record.GetWorker()), worker);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());
        AssertDestinationMode(
            env,
            "/Root/replica1",
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
    }

    Y_UNIT_TEST(CredentialsDuringPausedStreamCreation) {
        CredentialsDuringPausedStreamSetup(false);
    }

    Y_UNIT_TEST(CredentialsDuringPausedStreamCapabilityRead) {
        CredentialsDuringPausedStreamSetup(true);
    }

    void DoneDuringSourceSetup(bool restart, bool missingSecret) {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        runtime.GetAppData().ReplicationConfig.MutableSchemeOperationLimits()->SetInflightCreateStreamLimit(1);
        CreateSourceTable(env, "replica1");

        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        runtime.RegisterService(
            MakeTxProxyID(),
            runtime.Register(
                new TCreateCdcStreamRequestGate(txProxy, env.GetSender())));
        Y_DEFER {
            runtime.RegisterService(MakeTxProxyID(), txProxy);
        };
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        const auto info = StartReplication(env);
        const auto createStream = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender());
        runtime.RegisterService(MakeTxProxyID(), txProxy);

        const auto description = DescribeReplication(env, info);
        UNIT_ASSERT_VALUES_EQUAL(description->Get()->Record.TargetsSize(), 1);
        const auto targetId = description->Get()->Record.GetTargets(0).GetId();
        const TString streamName = createStream->Get()->Record.GetTransaction().GetModifyScheme()
            .GetCreateCdcStream().GetStreamDescription().GetName();
        const auto readStream = [&] {
            const auto query = Sprintf(
                R"((
                (let key '('('ReplicationId (Uint64 '1)) '('TargetId (Uint64 '%lu))))
                (return (AsList (SetResult 'Stream (SelectRow 'SrcStreams key '('Name 'ConsumerName 'State)))))
            ))",
                targetId);
            const auto result = ReadControllerQuery(runtime, info.ControllerId, query);
            const auto& row = result.GetValue().GetStruct(0).GetOptional().GetOptional();
            UNIT_ASSERT_VALUES_EQUAL(
                row.GetStruct(2).GetOptional().GetUint32(),
                static_cast<ui32>(TReplication::EStreamState::Creating));
            return result.SerializeAsString();
        };
        const auto streamBefore = readStream();
        UNIT_ASSERT(streamBefore.Contains(streamName));
        UNIT_ASSERT(streamBefore.Contains("replicationConsumer"));

        const auto controllerGate = InstallControllerGate(
            env,
            info.ControllerId,
            [](THolder<IActor> actor, TActorId notify) {
                return new TDstAlterResultGate(std::move(actor), notify);
            });
        auto done = MakeDoneRequest(info, 114);
        if (missingSecret) {
            auto& credentials = *done->Record.MutableConfig()->MutableSrcConnectionParams()->MutableOAuthToken();
            credentials.ClearToken();
            credentials.SetTokenSecretName("unavailable-source-secret");
        }
        const auto altered = env.Send<TEvController::TEvAlterReplicationResult>(info.ControllerId, std::move(done));
        UNIT_ASSERT_VALUES_EQUAL(
            altered->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TDstAlterResultGate::Blocked);
        AssertDestinationWritable(env);

        const auto checkLateResults = [&](bool isDone) {
            for (const auto status : {NYdb::EStatus::SCHEME_ERROR, NYdb::EStatus::SUCCESS}) {
                env.SendAsync(
                    info.ControllerId,
                    new TEvPrivate::TEvCreateStreamResult(
                        1,
                        targetId,
                        NYdb::TStatus(status, NYdb::NIssue::TIssues())));
                UNIT_ASSERT_VALUES_EQUAL(DescribeReplication(env, info)->Get()->Record.GetState().HasDone(), isDone);
                UNIT_ASSERT_VALUES_EQUAL(readStream(), streamBefore);
            }
        };
        // Late failures must not interrupt pending DONE, and late successes must
        // not replace the unfinished stream metadata with Ready.
        checkLateResults(false);
        if (restart) {
            // Crash after DONE was persisted but before local cleanup completed.
            RestartController(env, info.ControllerId);
        } else {
            env.SendAsync(controllerGate, new TEvents::TEvWakeup(TDstAlterResultGate::Release));
        }
        AssertEventuallyDone(env, info);
        checkLateResults(true);
        RestartController(env, info.ControllerId);
        checkLateResults(true);

        // The submitted source DDL can still commit after DONE and restart.
        // A later DROP must use the retained name to remove that stream.
        runtime.Send(createStream->Forward(txProxy));
        UNIT_ASSERT_C(
            WaitFor([&] {
                return env.GetDescription("/Root/table1").GetPathDescription().GetTable().CdcStreamsSize() == 1;
            }),
            "Delayed source stream creation did not commit");
        const auto restored = env.Send<TEvController::TEvAlterReplicationResult>(
            info.ControllerId,
            MakeAlterReplicationRequest(info, 115));
        UNIT_ASSERT_VALUES_EQUAL(
            restored->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        auto drop = MakeHolder<TEvController::TEvDropReplication>();
        info.PathId.ToProto(drop->Record.MutablePathId());
        drop->Record.MutableOperationId()->SetTxId(116);
        const auto dropped = env.Send<TEvController::TEvDropReplicationResult>(info.ControllerId, std::move(drop));
        UNIT_ASSERT(dropped->Get()->Record.GetStatus()
            == NKikimrReplication::TEvDropReplicationResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/table1").GetPathDescription().GetTable().CdcStreamsSize(), 0);
        AssertDestinationWritable(env);
    }

    Y_UNIT_TEST(DoneDuringSourceCreation) {
        DoneDuringSourceSetup(false, false);
    }

    Y_UNIT_TEST(DoneDuringSourceCreationAfterRestart) {
        DoneDuringSourceSetup(true, false);
    }

    Y_UNIT_TEST(DoneDuringSourceCreationWithMissingSecretAfterRestart) {
        DoneDuringSourceSetup(true, true);
    }

    void RefreshSourceDuringAttachment(bool retry) {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        CreateSourceTable(env, "table1");
        CreateSourceTable(env, "replica1");
        const auto oldProxy = runtime.AllocateEdgeActor();
        const auto newProxy = runtime.AllocateEdgeActor();
        const auto dstPathId = env.GetPathId("/Root/replica1");
        const auto creator = runtime.Register(CreateDstCreator(
            env.GetSender(),
            dstPathId.OwnerId,
            oldProxy,
            "/Root",
            env.GetPathId("/Root"),
            1,
            1,
            TReplication::ETargetKind::Table,
            "/Root/table1",
            "/Root/replica1",
            EReplicationMode::ReadOnly,
            EConsistencyLevel::Row,
            false,
            dstPathId));
        const auto oldRequest = runtime.GrabEdgeEvent<TEvYdbProxy::TEvDescribeTableRequest>(oldProxy);
        if (retry) {
            runtime.Send(new IEventHandle(
                creator,
                oldProxy,
                new TEvYdbProxy::TEvDescribeTableResponse(
                    NYdb::TStatus(NYdb::EStatus::OVERLOADED, {}),
                    Ydb::Table::DescribeTableResult{},
                    NYdb::NTable::TDescribeTableSettings()),
                0,
                oldRequest->Cookie));
        }

        env.SendAsync(creator, new TEvPrivate::TEvUpdateYdbProxy(newProxy));
        auto newRequest = runtime.GrabEdgeEvent<TEvYdbProxy::TEvDescribeTableRequest>(newProxy);
        UNIT_ASSERT_VALUES_UNEQUAL(newRequest->Cookie, oldRequest->Cookie);
        // Old retries and terminal failures must not affect the new validation.
        env.SendAsync(creator, new TEvents::TEvWakeup(oldRequest->Cookie));
        runtime.Send(new IEventHandle(
            creator,
            oldProxy,
            new TEvYdbProxy::TEvDescribeTableResponse(
                NYdb::TStatus(NYdb::EStatus::UNAUTHORIZED, {}),
                Ydb::Table::DescribeTableResult{},
                NYdb::NTable::TDescribeTableSettings()),
            0,
            oldRequest->Cookie));
        runtime.Send(newRequest->Forward(env.GetYdbProxy()));
        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_C(result->Get()->IsSuccess(), result->Get()->Error);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->DstPathId, dstPathId);
    }

    Y_UNIT_TEST(RefreshAttachmentSourceWithDelayedResponse) {
        RefreshSourceDuringAttachment(false);
    }

    Y_UNIT_TEST(RefreshAttachmentSourceWithPendingRetry) {
        RefreshSourceDuringAttachment(true);
    }

    Y_UNIT_TEST(RefreshAttachmentSourceDuringLocalDdl) {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        CreateSourceTable(env, "table1");
        CreateSourceTable(env, "replica1");
        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        const auto gate = runtime.Register(new TAttachAllocationGate(txProxy, env.GetSender()));
        runtime.RegisterService(MakeTxProxyID(), gate);
        Y_DEFER {
            runtime.RegisterService(MakeTxProxyID(), txProxy);
        };
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TAttachAllocationGate::ReadyTag());
        const auto dstPathId = env.GetPathId("/Root/replica1");
        const auto creator = runtime.Register(CreateDstCreator(
            env.GetSender(),
            dstPathId.OwnerId,
            env.GetYdbProxy(),
            "/Root",
            env.GetPathId("/Root"),
            1,
            1,
            TReplication::ETargetKind::Table,
            "/Root/table1",
            "/Root/replica1",
            EReplicationMode::ReadOnly,
            EConsistencyLevel::Row,
            false));
        runtime.GrabEdgeEvent<TEvPrivate::TEvPrepareAttachDst>(env.GetSender());
        env.SendAsync(creator, new TEvPrivate::TEvPrepareAttachDstResult());
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TAttachAllocationGate::BlockedTag());

        // Once local DDL starts, refreshing an unavailable source must leave the
        // allocation/result reconciliation alive rather than restart validation.
        env.SendAsync(creator, new TEvPrivate::TEvUpdateYdbProxy(runtime.AllocateEdgeActor()));
        runtime.RegisterService(MakeTxProxyID(), txProxy);
        env.SendAsync(gate, new TEvents::TEvWakeup(TAttachAllocationGate::ReleaseTag()));
        const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(env.GetSender());
        UNIT_ASSERT_C(result->Get()->IsSuccess(), result->Get()->Error);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->DstPathId, dstPathId);
    }

    Y_UNIT_TEST(DoneAfterDestinationNameConflict) {
        for (const auto status : {NKikimrScheme::StatusPathDoesNotExist,
                NKikimrScheme::StatusSuccess, NKikimrScheme::StatusSchemeError}) {
            TTestActorRuntime runtime;
            runtime.Initialize(TAppPrepare().Unwrap());
            const auto parent = runtime.AllocateEdgeActor();
            const auto allocator = runtime.AllocateEdgeActor();
            const auto pipeCache = runtime.AllocateEdgeActor();
            runtime.RegisterService(MakeTxProxyID(), allocator);
            const TPathId pathId(1, 2);
            const auto alterer = runtime.Register(CreateDstAlterer(parent, pathId.OwnerId,
                1, 1, TReplication::ETargetKind::Table, pathId, TReplication::EState::Done));

            runtime.GrabEdgeEvent<TEvTxUserProxy::TEvAllocateTxId>(allocator);
            NTxProxy::TTxProxyServices services;
            services.LeaderPipeCache = pipeCache;
            runtime.Send(alterer, allocator,
                new TEvTxUserProxy::TEvAllocateTxIdResult(100, services, {}));
            const auto alter = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
            UNIT_ASSERT_VALUES_EQUAL(alter->Get()->Ev->Type(),
                NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction::EventType);
            const TString error = "path is empty";
            runtime.Send(alterer, pipeCache,
                new NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult(
                    NKikimrScheme::StatusNameConflict, 100, pathId.OwnerId, error));

            const auto describe = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
            UNIT_ASSERT_VALUES_EQUAL(describe->Get()->Ev->Type(),
                NSchemeShard::TEvSchemeShard::TEvDescribeScheme::EventType);
            const auto& request = static_cast<NSchemeShard::TEvSchemeShard::TEvDescribeScheme*>(
                describe->Get()->Ev.Get())->Record;
            UNIT_ASSERT_VALUES_EQUAL(request.GetSchemeshardId(), pathId.OwnerId);
            UNIT_ASSERT_VALUES_EQUAL(request.GetPathId(), pathId.LocalPathId);
            auto description = MakeHolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
            description->Record.SetStatus(status);
            runtime.Send(alterer, pipeCache, description.Release());

            const auto result = runtime.GrabEdgeEvent<TEvPrivate::TEvAlterDstResult>(parent);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->IsSuccess(),
                status == NKikimrScheme::StatusPathDoesNotExist);
            if (!result->Get()->IsSuccess()) {
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrScheme::StatusNameConflict);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Error, error);
            }
        }
    }

    Y_UNIT_TEST(DoneAfterAttachmentValidationError) {
        CheckDoneAfterAttachmentError(false);
    }

    Y_UNIT_TEST(DoneAfterAttachmentValidationErrorAndRestartWithoutSource) {
        CheckDoneAfterAttachmentError(true);
    }

    Y_UNIT_TEST(DoneBeforeAttachmentValidationError) {
        CheckDoneAfterAttachmentError(false, true);
    }

    Y_UNIT_TEST(DoneAfterPendingDestinationDeleted) {
        TTestEnv env;
        const auto pending = StartPendingAttachment(env);
        const auto& info = pending.Info;

        SendAttachmentResult(
            env,
            pending,
            new TEvPrivate::TEvCreateDstResult(
                1,
                pending.TargetId,
                NKikimrScheme::StatusSchemeError,
                "Cannot validate attached destination"));
        UNIT_ASSERT(WaitFor([&] {
            return DescribeReplication(env, info)->Get()->Record.GetState().HasError();
        }));
        AssertAttachmentPaths(env, pending, pending.DstPathId);

        NYdb::NTable::TTableClient client(env.GetDriver(), NYdb::NTable::TClientSettings()
            .DiscoveryEndpoint(env.GetEndpoint())
            .Database(env.GetDatabase()));
        auto session = client.CreateSession().GetValueSync().GetSession();
        const auto drop = session.ExecuteSchemeQuery("DROP TABLE `/Root/replica1`;").GetValueSync();
        UNIT_ASSERT_C(drop.IsSuccess(), drop.GetIssues().ToString());

        const auto result = env.Send<TEvController::TEvAlterReplicationResult>(
            info.ControllerId, MakeDoneRequest(info, 106));
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        AssertEventuallyDone(env, info);
        AssertAttachmentPaths(env, pending, {});
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica1").GetSchemeStatus(),
            NKikimrScheme::StatusPathDoesNotExist);
    }

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

        SendAttachmentResult(
            env,
            pending,
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

    Y_UNIT_TEST(DoneDuringAttachmentRevalidationAfterRestart) {
        TTestEnv env;
        const auto pending = StartPendingAttachment(env);
        const auto& info = pending.Info;
        auto& runtime = env.GetRuntime();

        const auto schemeCache = runtime.GetLocalServiceId(MakeSchemeCacheID());
        const auto gate = runtime.Register(new TDescribePathRequestGate(
            schemeCache,
            env.GetSender(),
            "/Root/table1"));
        runtime.RegisterService(MakeSchemeCacheID(), gate);
        Y_DEFER {
            runtime.RegisterService(MakeSchemeCacheID(), schemeCache);
        };

        // Persist desired Ready while keeping the attachment and source Describe
        // unfinished. Restart must resume source validation before DONE arrives.
        RestartController(env, info.ControllerId);
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TDescribePathRequestGate::RequestBlockedTag());

        auto resume = MakeAlterReplicationRequest(info, 107);
        resume->Record.MutableSwitchState()->MutableStandBy();
        const auto resumed = env.Send<TEvController::TEvAlterReplicationResult>(
            info.ControllerId,
            std::move(resume));
        UNIT_ASSERT_VALUES_EQUAL(
            resumed->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TDescribePathRequestGate::RequestBlockedTag());
        AssertAttachmentPaths(env, pending, pending.DstPathId);

        RestartController(env, info.ControllerId);
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TDescribePathRequestGate::RequestBlockedTag());
        AssertAttachmentPaths(env, pending, pending.DstPathId);

        // DONE also replaces the source endpoint: no source response or another
        // controller restart may be needed to finish local reconciliation/detach.
        const auto result = env.Send<TEvController::TEvAlterReplicationResult>(
            info.ControllerId,
            MakeDoneRequest(info, 108));
        UNIT_ASSERT_VALUES_EQUAL(
            result->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        AssertEventuallyDone(env, info);
        AssertDestinationWritable(env);

        env.SendAsync(gate, new TEvents::TEvWakeup(TDescribePathRequestGate::ReleaseRequestTag()));
    }

    Y_UNIT_TEST_TWIN(DropDuringAttachmentRevalidationAfterRestart, Cascade) {
        TTestEnv env;
        const auto pending = StartPendingAttachment(env);
        const auto& info = pending.Info;
        auto& runtime = env.GetRuntime();
        // Hold stream cleanup independently of the blocked source validation.
        // Its completion below must be what allows local attachment cleanup.
        runtime.GetAppData().ReplicationConfig.MutableSchemeOperationLimits()->SetInflightDropStreamLimit(0);

        const auto schemeCache = runtime.GetLocalServiceId(MakeSchemeCacheID());
        const auto gate = runtime.Register(new TDescribePathRequestGate(
            schemeCache,
            env.GetSender(),
            "/Root/table1"));
        runtime.RegisterService(MakeSchemeCacheID(), gate);
        Y_DEFER {
            runtime.RegisterService(MakeSchemeCacheID(), schemeCache);
        };

        RestartController(env, info.ControllerId);
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TDescribePathRequestGate::RequestBlockedTag());

        auto resume = MakeAlterReplicationRequest(info, 113);
        resume->Record.MutableSwitchState()->MutableStandBy();
        const auto resumed = env.Send<TEvController::TEvAlterReplicationResult>(
            info.ControllerId,
            std::move(resume));
        UNIT_ASSERT_VALUES_EQUAL(
            resumed->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TDescribePathRequestGate::RequestBlockedTag());

        RestartController(env, info.ControllerId);
        UNIT_ASSERT_VALUES_EQUAL(
            runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender())->Get()->Tag,
            TDescribePathRequestGate::RequestBlockedTag());
        AssertAttachmentPaths(env, pending, pending.DstPathId);

        auto request = MakeHolder<TEvController::TEvDropReplication>();
        info.PathId.ToProto(request->Record.MutablePathId());
        request->Record.MutableOperationId()->SetTxId(114);
        request->Record.SetCascade(Cascade);
        env.SendAsync(info.ControllerId, std::move(request));
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetStatus()
            == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
        AssertAttachmentPaths(env, pending, pending.DstPathId);
        AssertDestinationMode(
            env,
            "/Root/replica1",
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);

        env.SendAsync(
            info.ControllerId,
            new TEvPrivate::TEvDropStreamResult(
                1,
                pending.TargetId,
                NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues())));
        // No source response, synthetic attachment result, or second restart is
        // needed after stream cleanup: the retained creator must reconcile locally.
        UNIT_ASSERT_C(
            WaitFor([&] {
                return DescribeReplication(env, info)->Get()->Record.GetStatus()
                    == NKikimrReplication::TEvDescribeReplicationResult::NOT_FOUND;
            }),
            "DROP did not finish while attachment source validation was blocked");
        const auto dropped = runtime.GrabEdgeEvent<TEvController::TEvDropReplicationResult>(env.GetSender());
        UNIT_ASSERT(dropped->Get()->Record.GetStatus()
            == NKikimrReplication::TEvDropReplicationResult::SUCCESS);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetStatus()
            == NKikimrReplication::TEvDescribeReplicationResult::NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(env.GetPathId("/Root/replica1"), pending.DstPathId);
        AssertDestinationWritable(env);

        runtime.RegisterService(MakeSchemeCacheID(), schemeCache);
        env.SendAsync(gate, new TEvents::TEvWakeup(TDescribePathRequestGate::ReleaseRequestTag()));
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
        SendAttachmentResult(
            env,
            pending,
            new TEvPrivate::TEvCreateDstResult(1, pending.TargetId, pending.DstPathId));

        const auto result = env.GetRuntime().GrabEdgeEvent<TEvController::TEvDropReplicationResult>(env.GetSender());
        UNIT_ASSERT(result->Get()->Record.GetStatus()
            == NKikimrReplication::TEvDropReplicationResult::SUCCESS);
        AssertDestinationWritable(env);
    }

    void DropWithAttachedIndex(TReplication::EDstState baseState, bool cascade, bool indexFirst = false) {
        NTestHelpers::TFeatureFlags flags;
        flags.SetEnableChangefeedsOnIndexTables(true);
        TTestEnv env(flags);
        const auto pending = StartPendingAttachment(env, true);
        const auto& info = pending.Info;
        auto& runtime = env.GetRuntime();
        runtime.GetAppData().ReplicationConfig.MutableSchemeOperationLimits()->SetInflightDropStreamLimit(0);

        const TString indexPath = "/Root/replica1/by_value/indexImplTable";
        ui64 indexTargetId = 0;
        const auto description = DescribeReplication(env, info);
        for (const auto& target : description->Get()->Record.GetTargets()) {
            if (target.GetDstPath() == indexPath) {
                indexTargetId = target.GetId();
            }
        }
        UNIT_ASSERT(indexTargetId);
        const auto indexPathId = env.GetPathId(indexPath);
        UNIT_ASSERT(WaitFor([&] {
            const auto state = ReadPersistedDstState(runtime, info.ControllerId, TWorkerId(1, indexTargetId, 0));
            return state == TReplication::EDstState::Ready;
        }));
        const TPendingAttachment index{info, indexTargetId, indexPathId, {}};
        AssertAttachmentPaths(env, index, {}, indexPathId);
        AssertDestinationMode(
            env,
            indexPath,
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);

        if (baseState == TReplication::EDstState::Error) {
            SendAttachmentResult(
                env,
                pending,
                new TEvPrivate::TEvCreateDstResult(
                    1,
                    pending.TargetId,
                    NKikimrScheme::StatusSchemeError,
                    "Cannot validate attached destination"));
            UNIT_ASSERT(WaitFor([&] {
                return DescribeReplication(env, info)->Get()->Record.GetState().HasError();
            }));
        } else if (baseState == TReplication::EDstState::Ready) {
            SendAttachmentResult(
                env,
                pending,
                new TEvPrivate::TEvCreateDstResult(1, pending.TargetId, pending.DstPathId));
            DescribeReplication(env, info);
        }
        const bool pendingBase = baseState != TReplication::EDstState::Ready;
        const bool detach = baseState == TReplication::EDstState::Attaching || (cascade && pendingBase);
        const bool deleted = cascade && !pendingBase;
        AssertAttachmentPaths(
            env,
            pending,
            pendingBase ? pending.DstPathId : TPathId(),
            pendingBase ? TPathId() : pending.DstPathId);

        auto request = MakeHolder<TEvController::TEvDropReplication>();
        info.PathId.ToProto(request->Record.MutablePathId());
        request->Record.MutableOperationId()->SetTxId(114);
        request->Record.SetCascade(cascade);
        env.SendAsync(info.ControllerId, std::move(request));
        DescribeReplication(env, info);
        if (baseState == TReplication::EDstState::Attaching) {
            SendAttachmentResult(
                env,
                pending,
                new TEvPrivate::TEvCreateDstResult(1, pending.TargetId, pending.DstPathId));
        }
        env.SendAsync(
            info.ControllerId,
            new TEvPrivate::TEvDropStreamResult(
                1,
                indexFirst ? indexTargetId : pending.TargetId,
                NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues())));
        UNIT_ASSERT_C(
            WaitFor([&] {
                const auto description = DescribeReplication(env, info);
                return description->Get()->Record.TargetsSize() == 1
                    && description->Get()->Record.GetTargets(0).GetId()
                        == (indexFirst ? pending.TargetId : indexTargetId);
            }),
            "First target was not removed before the second target's cleanup");

        // In the base-first case its metadata is now gone. The decision to
        // detach the index must survive independently across controller restart.
        RestartController(env, info.ControllerId);
        if (!indexFirst) {
            AssertAttachmentPaths(env, index, detach ? indexPathId : TPathId(), indexPathId);
        }
        env.SendAsync(
            info.ControllerId,
            new TEvPrivate::TEvDropStreamResult(
                1,
                indexFirst ? pending.TargetId : indexTargetId,
                NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues())));
        UNIT_ASSERT_C(
            WaitFor([&] {
                return DescribeReplication(env, info)->Get()->Record.GetStatus()
                    == NKikimrReplication::TEvDescribeReplicationResult::NOT_FOUND;
            }),
            "DROP did not finish after index cleanup");

        if (deleted) {
            UNIT_ASSERT_VALUES_EQUAL(
                env.GetDescription("/Root/replica1").GetSchemeStatus(),
                NKikimrScheme::StatusPathDoesNotExist);
            UNIT_ASSERT_VALUES_EQUAL(
                env.GetDescription(indexPath).GetSchemeStatus(),
                NKikimrScheme::StatusPathDoesNotExist);
            return;
        }

        UNIT_ASSERT_VALUES_EQUAL(env.GetPathId("/Root/replica1"), pending.DstPathId);
        UNIT_ASSERT_VALUES_EQUAL(env.GetPathId(indexPath), indexPathId);
        if (!detach) {
            // Ordinary plain DROP retains fully attached destinations, and an
            // Error base is not reconciled merely because it has a pending path.
            for (const TString& path : {TString("/Root/replica1"), indexPath}) {
                AssertDestinationMode(
                    env,
                    path,
                    NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
            }
            return;
        }
        AssertDestinationWritable(env);
        AssertDestinationMode(
            env,
            indexPath,
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE);

        auto client = CreateTableClient(env);
        const auto write = client.RetryOperationSync([](NYdb::NTable::TSession session) {
            return session.ExecuteDataQuery(
                "UPSERT INTO `/Root/replica1` (key, value) VALUES (1, 'writable');",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        auto session = client.CreateSession().GetValueSync().GetSession();
        const auto read = session.ExecuteDataQuery(
            "SELECT key FROM `/Root/replica1` VIEW by_value WHERE value = 'writable';",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(read.GetResultSet(0).RowsCount(), 1);
    }

    void DropAfterEarlyBaseValidationError(bool cascade) {
        NTestHelpers::TFeatureFlags flags;
        flags.SetEnableChangefeedsOnIndexTables(true);
        TTestEnv env(flags);
        auto& runtime = env.GetRuntime();
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        CreateSourceTable(env, "bootstrap");
        auto client = CreateTableClient(env);
        auto session = client.CreateSession().GetValueSync().GetSession();
        // The extra non-indexed column rejects the base before attachment
        // preparation, while the restored index still matches its source.
        const auto create = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/replica1` (
                key Uint32, value Utf8, extra Uint64, PRIMARY KEY (key),
                INDEX by_value GLOBAL SYNC ON (value)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());
        const TString indexPath = "/Root/replica1/by_value/indexImplTable";
        const auto basePathId = env.GetPathId("/Root/replica1");
        const auto indexPathId = env.GetPathId(indexPath);
        const auto info = StartReplication(env, 1, "root@builtin", false, true);
        UNIT_ASSERT(WaitFor([&] {
            return SourceStreamsReady(env, info, 2);
        }));
        auto resume = MakeAlterReplicationRequest(info, 115);
        resume->Record.MutableSwitchState()->MutableStandBy();
        const auto resumed = env.Send<TEvController::TEvAlterReplicationResult>(
            info.ControllerId,
            std::move(resume));
        UNIT_ASSERT_VALUES_EQUAL(
            resumed->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        UNIT_ASSERT(WaitFor([&] {
            return DescribeReplication(env, info)->Get()->Record.GetState().HasError();
        }));
        const auto description = DescribeReplication(env, info);
        UNIT_ASSERT_STRING_CONTAINS(
            description->Get()->Record.GetState().GetError().DebugString(),
            "Columns size mismatch");
        ui64 baseTargetId = 0, indexTargetId = 0;
        for (const auto& target : description->Get()->Record.GetTargets()) {
            (target.GetDstPath() == indexPath ? indexTargetId : baseTargetId) = target.GetId();
        }
        UNIT_ASSERT(baseTargetId && indexTargetId);
        const TPendingAttachment base{info, baseTargetId, basePathId, {}};
        const TPendingAttachment index{info, indexTargetId, indexPathId, {}};
        // No pending or confirmed base path exists; the index attached through
        // the real creator and persisted its confirmed path independently.
        AssertAttachmentPaths(env, base, {});
        UNIT_ASSERT(WaitFor([&] {
            const auto state = ReadPersistedDstState(runtime, info.ControllerId, TWorkerId(1, indexTargetId, 0));
            return state == TReplication::EDstState::Ready;
        }));
        AssertAttachmentPaths(env, index, {}, indexPathId);
        AssertDestinationMode(
            env,
            indexPath,
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);

        // Remove the base first, then restart without its metadata to check
        // that the index retains its own durable cleanup ownership.
        runtime.GetAppData().ReplicationConfig.MutableSchemeOperationLimits()->SetInflightDropStreamLimit(2);
        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        const auto gate = runtime.Register(new TDropCdcStreamRequestGate(txProxy, env.GetSender()));
        runtime.RegisterService(MakeTxProxyID(), gate);
        Y_DEFER {
            runtime.RegisterService(MakeTxProxyID(), txProxy);
        };
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        auto request = MakeHolder<TEvController::TEvDropReplication>();
        info.PathId.ToProto(request->Record.MutablePathId());
        request->Record.MutableOperationId()->SetTxId(116);
        request->Record.SetCascade(cascade);
        env.SendAsync(info.ControllerId, std::move(request));
        auto first = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender());
        auto second = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender());
        const auto& tx = first->Get()->Record.GetTransaction().GetModifyScheme();
        const bool firstIsIndex = tx.GetDropCdcStream().GetTableName() == "indexImplTable";
        if (firstIsIndex) {
            std::swap(first, second);
        }
        runtime.Send(first->Forward(txProxy));
        const auto baseRemoved = [&] {
            const auto description = DescribeReplication(env, info);
            return description->Get()->Record.TargetsSize() == 1
                && description->Get()->Record.GetTargets(0).GetId() == indexTargetId;
        };
        UNIT_ASSERT_C(WaitFor(baseRemoved), "First target was not removed before restart");
        auto& limits = *runtime.GetAppData().ReplicationConfig.MutableSchemeOperationLimits();
        limits.SetInflightDropStreamLimit(0);
        runtime.RegisterService(MakeTxProxyID(), txProxy);
        RestartController(env, info.ControllerId);
        AssertAttachmentPaths(env, index, cascade ? indexPathId : TPathId(), indexPathId);
        limits.SetInflightDropStreamLimit(2);
        env.SendAsync(info.ControllerId, new TEvPrivate::TEvCancelDropStream());
        const auto replicationRemoved = [&] {
            return DescribeReplication(env, info)->Get()->Record.GetStatus()
                == NKikimrReplication::TEvDescribeReplicationResult::NOT_FOUND;
        };
        UNIT_ASSERT_C(WaitFor(replicationRemoved, 150), "DROP did not finish after restart");
        for (const TString& path : {TString("/Root/table1"), TString("/Root/table1/by_value/indexImplTable")}) {
            UNIT_ASSERT_VALUES_EQUAL(env.GetDescription(path).GetPathDescription().GetTable().CdcStreamsSize(), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(env.GetPathId("/Root/replica1"), basePathId);
        UNIT_ASSERT_VALUES_EQUAL(env.GetPathId(indexPath), indexPathId);
        AssertDestinationWritable(env);
        const auto expectedMode = cascade
            ? NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE
            : NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY;
        AssertDestinationMode(env, indexPath, expectedMode);
        if (!cascade) {
            return;
        }

        const auto write = client.RetryOperationSync([](NYdb::NTable::TSession session) {
            return session.ExecuteDataQuery(
                "UPSERT INTO `/Root/replica1` (key, value) VALUES (1, 'writable');",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        const auto read = session.ExecuteDataQuery(
            "SELECT key FROM `/Root/replica1` VIEW by_value WHERE value = 'writable';",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(read.GetResultSet(0).RowsCount(), 1);
    }

    Y_UNIT_TEST(DropCascadeAfterEarlyBaseValidationErrorAndRestart) {
        DropAfterEarlyBaseValidationError(true);
    }

    Y_UNIT_TEST(DropAfterEarlyBaseValidationErrorPreservesReadonlyIndex) {
        DropAfterEarlyBaseValidationError(false);
    }

    Y_UNIT_TEST_TWIN(DropCascadeWithPartiallyAttachedIndex, AttachmentError) {
        DropWithAttachedIndex(
            AttachmentError
                ? TReplication::EDstState::Error : TReplication::EDstState::Attaching,
            true);
    }

    Y_UNIT_TEST(DropCascadeWithFullyAttachedIndex) {
        DropWithAttachedIndex(TReplication::EDstState::Ready, true);
    }

    Y_UNIT_TEST_TWIN(DropWithPartiallyAttachedIndex, IndexFirst) {
        DropWithAttachedIndex(TReplication::EDstState::Attaching, false, IndexFirst);
    }

    Y_UNIT_TEST_TWIN(DropWithAttachedIndexPreservesReadonlyDestinations, AttachmentError) {
        DropWithAttachedIndex(
            AttachmentError
                ? TReplication::EDstState::Error : TReplication::EDstState::Ready,
            false);
    }

    Y_UNIT_TEST(DropCascadeAfterPendingDestinationDeleted) {
        TTestEnv env;
        const auto pending = StartPendingAttachment(env);
        const auto& info = pending.Info;

        SendAttachmentResult(
            env,
            pending,
            new TEvPrivate::TEvCreateDstResult(
                1,
                pending.TargetId,
                NKikimrScheme::StatusSchemeError,
                "Cannot validate attached destination"));
        UNIT_ASSERT(WaitFor([&] {
            return DescribeReplication(env, info)->Get()->Record.GetState().HasError();
        }));
        AssertAttachmentPaths(env, pending, pending.DstPathId);

        auto client = CreateTableClient(env);
        auto session = client.CreateSession().GetValueSync().GetSession();
        const auto drop = session.ExecuteSchemeQuery("DROP TABLE `/Root/replica1`;").GetValueSync();
        UNIT_ASSERT_C(drop.IsSuccess(), drop.GetIssues().ToString());

        auto request = MakeHolder<TEvController::TEvDropReplication>();
        info.PathId.ToProto(request->Record.MutablePathId());
        request->Record.MutableOperationId()->SetTxId(107);
        request->Record.SetCascade(true);
        env.SendAsync(info.ControllerId, std::move(request));
        DescribeReplication(env, info);
        env.SendAsync(
            info.ControllerId,
            new TEvPrivate::TEvDropStreamResult(
                1,
                pending.TargetId,
                NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues())));

        const auto result = env.GetRuntime().GrabEdgeEvent<TEvController::TEvDropReplicationResult>(env.GetSender());
        UNIT_ASSERT(result->Get()->Record.GetStatus()
            == NKikimrReplication::TEvDropReplicationResult::SUCCESS);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetStatus()
            == NKikimrReplication::TEvDescribeReplicationResult::NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(
            env.GetDescription("/Root/replica1").GetSchemeStatus(),
            NKikimrScheme::StatusPathDoesNotExist);
    }

    Y_UNIT_TEST(DropCascadeAfterRestartWithoutSource) {
        TTestEnv env;
        const auto pending = StartPendingAttachment(env);
        const auto& info = pending.Info;
        env.GetRuntime().GetAppData().ReplicationConfig.MutableSchemeOperationLimits()->SetInflightDropStreamLimit(0);

        // Keep DesiredState Ready while making source revalidation unavailable.
        auto resume = MakeAlterReplicationRequest(info, 107, true);
        resume->Record.MutableSwitchState()->MutableStandBy();
        const auto altered = env.Send<TEvController::TEvAlterReplicationResult>(
            info.ControllerId,
            std::move(resume));
        UNIT_ASSERT_VALUES_EQUAL(
            altered->Get()->Record.GetStatus(),
            NKikimrReplication::TEvAlterReplicationResult::SUCCESS);

        auto request = MakeHolder<TEvController::TEvDropReplication>();
        info.PathId.ToProto(request->Record.MutablePathId());
        request->Record.MutableOperationId()->SetTxId(108);
        request->Record.SetCascade(true);
        env.SendAsync(info.ControllerId, std::move(request));
        DescribeReplication(env, info);
        // Restart before stream cleanup completes. Once it does, recovery must
        // reconcile the local ALTER without a source response before detaching.
        NKikimrMiniKQL::TResult persisted;
        const auto status = LocalQuery(
            env.GetRuntime(),
            info.ControllerId,
            Sprintf(
                R"((
            (let replicationKey '('('Id (Uint64 '1))))
            (let targetKey '('('ReplicationId (Uint64 '1)) '('Id (Uint64 '%lu))))
            (let streamKey '('('ReplicationId (Uint64 '1)) '('TargetId (Uint64 '%lu))))
            (return (AsList
                (SetResult 'Replication (SelectRow 'Replications replicationKey '('State 'DesiredState)))
                (SetResult 'Target (SelectRow 'Targets targetKey
                    '('DstState 'PendingDstPathOwnerId 'PendingDstPathLocalId)))
                (SetResult 'Stream (SelectRow 'SrcStreams streamKey '('State)))))
        ))",
                pending.TargetId,
                pending.TargetId),
            persisted);
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::OK);
        const auto readColumn = [&](TStringBuf resultName, TStringBuf columnName) {
            const auto& results = persisted.GetType().GetStruct();
            for (ui32 i = 0; i < results.MemberSize(); ++i) {
                const auto& result = results.GetMember(i);
                if (result.GetName() != resultName) {
                    continue;
                }
                const auto& row = persisted.GetValue().GetStruct(i).GetOptional();
                UNIT_ASSERT_C(row.HasOptional(), persisted.DebugString());
                const auto& columns = result.GetType().GetOptional().GetItem().GetOptional().GetItem().GetStruct();
                for (ui32 j = 0; j < columns.MemberSize(); ++j) {
                    if (columns.GetMember(j).GetName() == columnName) {
                        const auto& column = row.GetOptional().GetStruct(j);
                        UNIT_ASSERT_C(column.HasOptional(), persisted.DebugString());
                        return column.GetOptional();
                    }
                }
            }
            UNIT_FAIL("Missing persisted column: " << resultName << "." << columnName);
            return NKikimrMiniKQL::TValue{};
        };
        UNIT_ASSERT_VALUES_EQUAL(readColumn("Replication", "State").GetUint32(), static_cast<ui32>(TReplication::EState::Removing));
        UNIT_ASSERT_VALUES_EQUAL(readColumn("Replication", "DesiredState").GetUint32(), static_cast<ui32>(TReplication::EState::Ready));
        UNIT_ASSERT_VALUES_EQUAL(readColumn("Target", "DstState").GetUint32(), static_cast<ui32>(TReplication::EDstState::Attaching));
        UNIT_ASSERT_VALUES_EQUAL(readColumn("Stream", "State").GetUint32(), static_cast<ui32>(TReplication::EStreamState::Removing));
        UNIT_ASSERT_VALUES_EQUAL(
            TPathId(
                readColumn("Target", "PendingDstPathOwnerId").GetUint64(),
                readColumn("Target", "PendingDstPathLocalId").GetUint64()),
            pending.DstPathId);

        RestartController(env, info.ControllerId);
        env.SendAsync(
            info.ControllerId,
            new TEvPrivate::TEvDropStreamResult(
                1,
                pending.TargetId,
                NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues())));
        UNIT_ASSERT_C(
            WaitFor([&] {
                return DescribeReplication(env, info)->Get()->Record.GetStatus()
                    == NKikimrReplication::TEvDescribeReplicationResult::NOT_FOUND;
            }),
            "DROP did not finish after restarting without the source");
        UNIT_ASSERT_VALUES_EQUAL(env.GetPathId("/Root/replica1"), pending.DstPathId);
        AssertDestinationWritable(env);
    }

    Y_UNIT_TEST(DropWaitsForDelayedAttachAllocation) {
        TTestEnv env;
        auto& runtime = env.GetRuntime();
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
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

        UNIT_ASSERT_C(
            WaitFor([&] {
                return ReadStreamSchemaChanges(env, info.ControllerId, TWorkerId(1, targetId, 0)).has_value();
            }),
            "Source setup did not finish before delaying attachment allocation");

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
            new TDescribePathRequestGate(schemeCache, env.GetSender(), topicPath));
        runtime.RegisterService(MakeSchemeCacheID(), describeTopicGate);

        RestartController(env, controllerId);
        env.SendAsync(controllerId, new TEvService::TEvStatus());

        const auto blocked = runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(blocked->Get()->Tag, TDescribePathRequestGate::RequestBlockedTag());

        RequestPause(env, info, 1004);
        UNIT_ASSERT(DescribeReplication(env, info)->Get()->Record.GetState().HasStandBy());

        runtime.RegisterService(MakeSchemeCacheID(), schemeCache);
        env.SendAsync(describeTopicGate,
            new TEvents::TEvWakeup(TDescribePathRequestGate::ReleaseRequestTag()));

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
        auto session = CreateTableSession(env, client);
        const auto upsert = [&](ui32 key) {
            constexpr ui32 MaxUpsertAttempts = 50;
            const auto retryDelay = TDuration::MilliSeconds(100);

            for (ui32 attempt = 0; attempt < MaxUpsertAttempts; ++attempt) {
                const auto result = session.ExecuteDataQuery(Sprintf(
                    "UPSERT INTO `/Root/table1` (key, value) VALUES (%u, 'value');", key),
                    NYdb::NTable::TTxControl::BeginTx()
                            .CommitTx()).GetValueSync();
                if (result.IsSuccess()) {
                    return;
                }

                const auto status = result.GetStatus();
                // Source stream setup and index changes can temporarily block writes.
                const bool retryable = status == NYdb::EStatus::ABORTED
                    || status == NYdb::EStatus::SCHEME_ERROR
                    || status == NYdb::EStatus::OVERLOADED;
                UNIT_ASSERT_C(retryable, status << ": " << result.GetIssues().ToString());
                UNIT_ASSERT_C(attempt + 1 < MaxUpsertAttempts,
                    "Upsert retry limit reached: " << status << ": " << result.GetIssues().ToString());

                Sleep(retryDelay);
            }
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

    Y_UNIT_TEST(DropOwnsStreamRemovalAfterIndexCancellationRestart) {
        TIndexBuildTestEnv env;
        const auto& info = env.Info;
        auto& runtime = env.GetRuntime();
        const auto worker = env.StartIndexBuild();
        env.CompleteBarrier();
        // Keep the source reachable; this test completes real stream removal.
        auto done = MakeAlterReplicationRequest(info, 1010);
        done->Record.MutableSwitchState()->MutableDone()->SetFailoverMode(
            NKikimrReplication::TReplicationState::TDone::FAILOVER_MODE_FORCE);
        env.AlterReplication(std::move(done));
        const auto stop = runtime.GrabEdgeEvent<TEvService::TEvStopWorker>(env.GetSender());
        UNIT_ASSERT_VALUES_EQUAL(TWorkerId::Parse(stop->Get()->Record.GetWorker()), worker);
        env.AssertBuildPhase(worker, NKikimrReplication::TIndexBuildState::CANCELLING);

        auto& limits = *runtime.GetAppData().ReplicationConfig.MutableSchemeOperationLimits();
        limits.SetInflightDropStreamLimit(0);
        const auto drop = [&] {
            auto request = MakeHolder<TEvController::TEvDropReplication>();
            info.PathId.ToProto(request->Record.MutablePathId());
            request->Record.MutableOperationId()->SetTxId(1011);
            request->Record.SetCascade(true);
            env.SendAsync(info.ControllerId, std::move(request));
        };
        drop();
        DescribeReplication(env, info);
        RestartController(env, info.ControllerId);
        drop();
        // Recovery removes unadopted workers before source cleanup can start.
        // Keep the limiter closed until those removals and another queued index
        // transaction complete, exposing an erroneous cancellation Shutdown.
        UNIT_ASSERT_C(
            WaitFor([&] {
                const auto result = ReadControllerQuery(
                    runtime,
                    info.ControllerId,
                    R"((
                (let range '('('ReplicationId (Uint64 '1) (Uint64 '1))
                    '('TargetId (Null) (Void)) '('WorkerId (Null) (Void))))
                (return (AsList (SetResult 'Rows (SelectRange 'Workers range '('WorkerId) '())))))
            )");
                return result.GetValue().GetStruct(0).GetOptional().GetStruct(0).ListSize() == 0;
            }),
            "DROP did not remove recovered workers");
        auto progress = MakeHolder<TEvService::TEvIndexBuildProgress>();
        worker.Serialize(*progress->Record.MutableWorker());
        env.SendAsync(info.ControllerId, std::move(progress));
        env.AssertBuildPhase(worker, NKikimrReplication::TIndexBuildState::CANCELLING);
        DescribeReplication(env, info);

        // Hold source DDL so one target's completion cannot restart a remover
        // incorrectly retired by cancellation of another target.
        const auto txProxy = runtime.GetLocalServiceId(MakeTxProxyID());
        runtime.RegisterService(
            MakeTxProxyID(),
            runtime.Register(
                new TDropCdcStreamRequestGate(txProxy, env.GetSender())));
        Y_DEFER {
            runtime.RegisterService(MakeTxProxyID(), txProxy);
        };
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(env.GetSender());
        limits.SetInflightDropStreamLimit(2);
        env.SendAsync(info.ControllerId, new TEvPrivate::TEvCancelDropStream());
        const auto first = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender(), TDuration::Seconds(5));
        const auto second = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransaction>(env.GetSender(), TDuration::Seconds(5));
        UNIT_ASSERT_C(first && second, "Index cancellation retired a queued DROP stream remover");
        runtime.RegisterService(MakeTxProxyID(), txProxy);
        runtime.Send(first->Forward(txProxy));
        runtime.Send(second->Forward(txProxy));
        UNIT_ASSERT_C(
            WaitFor(
                [&] {
                    return DescribeReplication(env, info)->Get()->Record.GetStatus()
                        == NKikimrReplication::TEvDescribeReplicationResult::NOT_FOUND;
                },
                150),
            "Index cancellation retired DROP's stream remover after restart");
        const auto dropped = runtime.GrabEdgeEvent<TEvController::TEvDropReplicationResult>(env.GetSender());
        UNIT_ASSERT(dropped->Get()->Record.GetStatus() == NKikimrReplication::TEvDropReplicationResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/table1").GetPathDescription().GetTable().CdcStreamsSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            env.GetDescription("/Root/replica1").GetSchemeStatus(),
            NKikimrScheme::StatusPathDoesNotExist);
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
