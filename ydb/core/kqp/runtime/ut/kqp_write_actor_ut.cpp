#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/buffer/buffer.h>
#include <ydb/core/kqp/common/kqp_shard_meta.h>
#include <ydb/core/kqp/runtime/kqp_write_actor.h>
#include <ydb/core/kqp/runtime/kqp_write_actor_settings.h>
#include <ydb/core/kqp/runtime/kqp_write_table.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/mon.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <yql/essentials/minikql/mkql_string_util.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <functional>

namespace NKikimr::NKqp {
namespace {

using namespace NActors;
using namespace NMiniKQL;
using namespace NYql::NDq;

constexpr ui64 ShardId = 1001001;
const TTableId TableId(1, 2, 3);
constexpr ui64 ReplacementShardId = ShardId + 1;
constexpr ui64 OtherShardId = ShardId + 2;

enum class ETableKind { Column, Row };
// Each partition has an exclusive upper key bound; Nothing() denotes +infinity.
using TPartitions = TVector<std::pair<ui64, TMaybe<ui64>>>;

NKikimrKqp::TKqpTableSinkSettings MakeSettings(ETableKind kind = ETableKind::Column) {
    NKikimrKqp::TKqpTableSinkSettings settings;
    settings.SetDatabase("/Root");
    settings.MutableTable()->SetOwnerId(TableId.PathId.OwnerId);
    settings.MutableTable()->SetTableId(TableId.PathId.LocalPathId);
    settings.MutableTable()->SetVersion(TableId.SchemaVersion);
    settings.MutableTable()->SetPath("/Root/Table");
    settings.SetType(NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT);
    settings.SetInconsistentTx(true);
    settings.SetIsOlap(kind == ETableKind::Column);
    settings.SetEnableStreamWrite(true);

    auto* key = settings.AddColumns();
    key->SetId(1);
    key->SetName("key");
    key->SetTypeId(NScheme::NTypeIds::Uint64);
    *settings.AddKeyColumns() = *key;
    auto* value = settings.AddColumns();
    value->SetId(2);
    value->SetName("value");
    value->SetTypeId(NScheme::NTypeIds::String);
    settings.AddWriteIndexes(0);
    settings.AddWriteIndexes(1);
    return settings;
}

NYql::NDqProto::TCheckpoint MakeCheckpoint(ui64 id) {
    NYql::NDqProto::TCheckpoint checkpoint;
    checkpoint.SetGeneration(1);
    checkpoint.SetId(id);
    return checkpoint;
}

TUnboxedValueBatch MakeData(THolderFactory& holderFactory, const TVector<ui64>& keys) {
    TUnboxedValueBatch batch;
    for (const auto key : keys) {
        NUdf::TUnboxedValue* items = nullptr;
        auto row = holderFactory.CreateDirectArrayHolder(2, items);
        items[0] = NUdf::TUnboxedValuePod(key);
        items[1] = MakeString("value");
        batch.emplace_back(std::move(row));
    }
    return batch;
}

struct TCallbacks : IDqComputeActorAsyncOutput::ICallbacks {
    ui64 Resumes = 0;
    TVector<ui64> SavedCheckpoints;
    bool Finished = false;
    NYql::TIssues Errors;

    void ResumeExecution(EResumeSource) override {
        ++Resumes;
    }

    void OnAsyncOutputError(ui64, const NYql::TIssues& issues, NYql::NDqProto::StatusIds::StatusCode) override {
        Errors.AddIssues(issues);
    }

    void OnAsyncOutputStateSaved(TSinkState&&, ui64, const NYql::NDqProto::TCheckpoint& checkpoint) override {
        SavedCheckpoints.push_back(checkpoint.GetId());
    }

    void OnAsyncOutputStateCommitted(ui64, const NYql::NDqProto::TCheckpoint&) override {}

    void OnAsyncOutputFinished(ui64) override {
        Finished = true;
    }
};

// Invoke the public sink interface from its owner's mailbox, as sync CA does.
class TSinkOwner : public TActorBootstrapped<TSinkOwner> {
public:
    using TAction = std::function<void(IDqComputeActorAsyncOutput&, THolderFactory&)>;

    struct TEvExecute : TEventLocal<TEvExecute, EventSpaceBegin(TEvents::ES_PRIVATE)> {
        TAction Action;

        explicit TEvExecute(TAction action)
            : Action(std::move(action))
        {}
    };

    TSinkOwner(TCallbacks& callbacks, ETableKind kind)
        : TSinkOwner(callbacks, MakeSettings(kind))
    {}

    TSinkOwner(TCallbacks& callbacks, NKikimrKqp::TKqpTableSinkSettings settings)
        : Callbacks(callbacks)
        , Settings(std::move(settings))
        , Alloc(std::make_shared<TScopedAlloc>(__LOCATION__))
        , MemoryInfo("KqpDirectWriteActorTest")
        , HolderFactory(Alloc->Ref(), MemoryInfo)
        , TypeEnv(*Alloc)
    {
        Alloc->Release();
    }

    ~TSinkOwner() {
        Alloc->Acquire();
    }

    void Bootstrap() {
        TGuard guard(*Alloc);
        auto factory = MakeIntrusive<TDqAsyncIoFactory>();
        RegisterKqpWriteActor(*factory, MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>()));
        NYql::NDqProto::TTaskOutput output;
        output.MutableSink()->SetType(TString(NYql::KqpTableSinkName));
        output.MutableSink()->MutableSettings()->PackFrom(Settings);
        const THashMap<TString, TString> params;
        auto [sink, actor] = factory->CreateDqSink({
            .OutputDesc = output,
            .OutputIndex = 0,
            .StatsLevel = TCollectStatsLevel::None,
            .TxId = ui64(1),
            .TaskId = 1,
            .Callback = &Callbacks,
            .SecureParams = params,
            .TaskParams = params,
            .TypeEnv = TypeEnv,
            .HolderFactory = HolderFactory,
            .Alloc = Alloc,
            .RandomProvider = nullptr,
            .TraceId = {},
            .TaskCounters = {},
            .HasCheckpoints = true,
        });
        Sink = sink;
        RegisterWithSameMailbox(actor);
        Become(&TSinkOwner::StateWork);
    }

private:
    STRICT_STFUNC(StateWork,
        hFunc(TEvExecute, Handle);
        hFunc(TEvents::TEvPoison, Handle);
    )

    void Handle(TEvExecute::TPtr& ev) {
        TGuard guard(*Alloc);
        ev->Get()->Action(*Sink, HolderFactory);
        Send(ev->Sender, new TEvents::TEvWakeup());
    }

    void Handle(TEvents::TEvPoison::TPtr& ev) {
        {
            TGuard guard(*Alloc);
            Sink->PassAway();
        }
        Send(ev->Sender, new TEvents::TEvWakeup());
        PassAway();
    }

    TCallbacks& Callbacks;
    const NKikimrKqp::TKqpTableSinkSettings Settings;
    std::shared_ptr<TScopedAlloc> Alloc;
    TMemoryUsageInfo MemoryInfo;
    THolderFactory HolderFactory;
    TTypeEnvironment TypeEnv;
    IDqComputeActorAsyncOutput* Sink = nullptr;
};

class TSinkFixture {
public:
    using TWrite = TEvPipeCache::TEvForward::TPtr;

    explicit TSinkFixture(ETableKind kind = ETableKind::Column, i64 memoryLimit = 64_MB,
            TPartitions partitions = {{ShardId, Nothing()}}, ui64 maxRetryResolvesPerShard = 5)
        : PreviousSettings(GetWriteActorSettings())
        , Kind(kind)
        , RowPartitions(std::move(partitions))
    {
        auto settings = MakeIntrusive<TWriteActorSettings>(PreviousSettings);
        settings->InFlightMemoryLimitPerActorBytes = memoryLimit;
        settings->MaxWriteAttempts = 1;
        settings->MaxRetryResolvesPerShard = maxRetryResolvesPerShard;
        SetWriteActorSettings(settings);

        Runtime.Initialize(TAppPrepare().Unwrap());
        Runtime.SetDispatchTimeout(TDuration::Seconds(5));
        Edge = Runtime.AllocateEdgeActor();
        SchemeCache = Runtime.AllocateEdgeActor();
        PipeCache = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(MakeSchemeCacheID(), SchemeCache);
        Runtime.RegisterService(MakePipePerNodeCacheID(false), PipeCache);
        Owner = Runtime.Register(new TSinkOwner(Callbacks, Kind));
        Resolve();
        UNIT_ASSERT_VALUES_EQUAL(Callbacks.Resumes, 1);
    }

    ~TSinkFixture() {
        Runtime.Send(Owner, Edge, new TEvents::TEvPoison());
        Runtime.GrabEdgeEvent<TEvents::TEvWakeup>(Edge);
        SetWriteActorSettings(MakeIntrusive<TWriteActorSettings>(PreviousSettings));
    }

    void Execute(TSinkOwner::TAction action = [](auto&, auto&) {}) {
        Runtime.Send(Owner, Edge, new TSinkOwner::TEvExecute(std::move(action)));
        UNIT_ASSERT(Runtime.GrabEdgeEvent<TEvents::TEvWakeup>(Edge, TDuration::Seconds(1)));
        UNIT_ASSERT_C(Callbacks.Errors.Empty(), Callbacks.Errors.ToOneLineString());
    }

    void Write(ui64 key, TMaybe<NYql::NDqProto::TCheckpoint> checkpoint = Nothing(), bool finished = false) {
        Write(TVector<ui64>{key}, checkpoint, finished);
    }

    void Write(TVector<ui64> keys, TMaybe<NYql::NDqProto::TCheckpoint> checkpoint = Nothing(), bool finished = false) {
        Execute([=](auto& sink, auto& holderFactory) {
            sink.SendData(MakeData(holderFactory, keys), 0, checkpoint, finished);
        });
    }

    i64 GetFreeSpace() {
        i64 result = 0;
        Execute([&](auto& sink, auto&) { result = sink.GetFreeSpace(); });
        return result;
    }

    TWrite GrabWrite(ui64 expectedKey = 1, ui64 expectedShard = ShardId) {
        auto event = GrabWriteEvent();
        UNIT_ASSERT_VALUES_EQUAL(event->Get()->TabletId, expectedShard);
        CheckRows(event, {expectedKey});
        return event;
    }

    THashMap<ui64, TWrite> GrabWrites(const THashMap<ui64, TVector<ui64>>& expected) {
        THashMap<ui64, TWrite> result;
        for (size_t i = 0; i < expected.size(); ++i) {
            auto event = GrabWriteEvent();
            const auto shardId = event->Get()->TabletId;
            UNIT_ASSERT_C(expected.contains(shardId), "Unexpected write to shard " << shardId);
            CheckRows(event, expected.at(shardId));
            UNIT_ASSERT(result.emplace(shardId, std::move(event)).second);
        }
        return result;
    }

    void AssertNoWrites() {
        Execute();
        UNIT_ASSERT_C(Runtime.CaptureMailboxEvents(PipeCache.Hint(), PipeCache.NodeId()).empty(),
            "Unexpected write: a surviving shard must keep its existing in-flight request");
    }

    void Acknowledge(const TWrite& write) {
        Runtime.Send(new IEventHandle(write->Sender, PipeCache,
            NEvents::TDataEvents::TEvWriteResult::BuildCompleted(write->Get()->TabletId).release(), 0, write->Cookie));
        Execute();
    }

    void SendWriteError(const TWrite& write, const NKikimrDataEvents::TEvWriteResult::EStatus& status) {
        Runtime.Send(new IEventHandle(write->Sender, PipeCache,
            NEvents::TDataEvents::TEvWriteResult::BuildError(
                write->Get()->TabletId, /*txId*/ 0, status, "Shard is overloaded").release(),
            0, write->Cookie));
    }

    void FailWrite(const TWrite& write, const NKikimrDataEvents::TEvWriteResult::EStatus& status) {
        SendWriteError(write, status);
        Execute();
    }

    void FailWriteTerminally(const TWrite& write, const NKikimrDataEvents::TEvWriteResult::EStatus& status) {
        SendWriteError(write, status);
        Runtime.Send(Owner, Edge, new TSinkOwner::TEvExecute([](auto&, auto&) {}));
        UNIT_ASSERT(Runtime.GrabEdgeEvent<TEvents::TEvWakeup>(Edge, TDuration::Seconds(1)));
        UNIT_ASSERT_C(!Callbacks.Errors.Empty(),
            "Expected the query to fail once the retry budget is exhausted");
        UNIT_ASSERT_C(Runtime.CaptureMailboxEvents(PipeCache.Hint(), PipeCache.NodeId()).empty(),
            "Unexpected write: the failed writer must not resend anything");
    }

    void FailWriteSubscribed(const TWrite& write) {
        const auto* sent = static_cast<NEvents::TDataEvents::TEvWrite*>(write->Get()->Ev.Get());
        // Mimics a DataShard probability rejection that acknowledges the writer's
        // overload subscription (TDataShard::SetOverloadSubscribed): the reply
        // carries the request cookie and the subscribed seqNo (which is one less
        // than the writer's next expected seqNo), so the sink must wait for
        // TEvOverloadReady instead of retrying on its own.
        auto result = NEvents::TDataEvents::TEvWriteResult::BuildError(
            write->Get()->TabletId, /*txId*/ 0, NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED,
            "Shard is overloaded");
        result->Record.SetOverloadSubscribed(sent->Record.GetOverloadSubscribe());
        Runtime.Send(new IEventHandle(write->Sender, PipeCache, result.release(), 0, write->Cookie));
        Execute();
    }

    void ShardReady(const TWrite& write) {
        const auto* sent = static_cast<NEvents::TDataEvents::TEvWrite*>(write->Get()->Ev.Get());
        // Mimics the shard's NotifyOverloadSubscribers for the acknowledged
        // subscription: TEvOverloadReady with the subscribed seqNo.
        Runtime.Send(new IEventHandle(write->Sender, Edge,
            new TEvDataShard::TEvOverloadReady(write->Get()->TabletId, sent->Record.GetOverloadSubscribe())));
        Execute();
    }

    void Retry(const TWrite& write, TPartitions partitions = {}) {
        if (!partitions.empty()) {
            UNIT_ASSERT(Kind == ETableKind::Row);
            RowPartitions = std::move(partitions);
        }
        // Exhaust the write budget through the normal retry path, without timers.
        Runtime.Send(write->Sender, PipeCache, new TEvPipeCache::TEvDeliveryProblem(write->Get()->TabletId, false));
        Resolve();
    }

    void Resolve() {
        if (Kind == ETableKind::Column) {
            ResolveColumnTable();
        } else {
            ResolveRowTable();
        }
        Execute();
    }

    TCallbacks Callbacks;

private:
    TWrite GrabWriteEvent() {
        auto event = Runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(PipeCache, TDuration::Seconds(1));
        UNIT_ASSERT_C(event, "Checkpoint data was not sent to a shard");
        auto* write = dynamic_cast<NEvents::TDataEvents::TEvWrite*>(event->Get()->Ev.Get());
        UNIT_ASSERT(write);
        UNIT_ASSERT_VALUES_EQUAL(write->Record.GetTxMode(), NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        UNIT_ASSERT_VALUES_EQUAL(write->Record.OperationsSize(), 1);
        return event;
    }

    void CheckRows(const TWrite& event, const TVector<ui64>& keys) {
        const auto& write = *static_cast<NEvents::TDataEvents::TEvWrite*>(event->Get()->Ev.Get());
        const auto& operation = write.Record.GetOperations(0);
        const NEvWrite::TPayloadReader<NEvents::TDataEvents::TEvWrite> reader(write);
        const auto payload = reader.GetDataFromPayload(operation.GetPayloadIndex());
        if (Kind == ETableKind::Column) {
            UNIT_ASSERT_VALUES_EQUAL(operation.GetPayloadFormat(), NKikimrDataEvents::FORMAT_ARROW);
            const auto schema = arrow::schema({arrow::field("key", arrow::uint64()), arrow::field("value", arrow::binary())});
            const auto batch = NArrow::DeserializeBatch(payload, schema);
            UNIT_ASSERT(batch);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), keys.size());
            for (size_t i = 0; i < keys.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(static_cast<const arrow::UInt64Array&>(*batch->column(0)).Value(i), keys[i]);
                UNIT_ASSERT_VALUES_EQUAL(static_cast<const arrow::BinaryArray&>(*batch->column(1)).GetString(i), "value");
            }
        } else {
            UNIT_ASSERT_VALUES_EQUAL(operation.GetPayloadFormat(), NKikimrDataEvents::FORMAT_CELLVEC);
            const TSerializedCellMatrix cells(payload);
            UNIT_ASSERT_VALUES_EQUAL(cells.GetColCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(cells.GetRowCount(), keys.size());
            for (size_t i = 0; i < keys.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(cells.GetCell(i, 0).AsValue<ui64>(), keys[i]);
                UNIT_ASSERT_VALUES_EQUAL(cells.GetCell(i, 1).AsBuf(), "value");
            }
        }
    }

    void ResolveColumnTable() {
        auto event = Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(SchemeCache, TDuration::Seconds(1));
        UNIT_ASSERT_C(event, "Expected the writer to resolve the column table");
        auto request = std::move(event->Get()->Request);
        UNIT_ASSERT_VALUES_EQUAL(request->ResultSet.size(), 1);
        auto& entry = request->ResultSet.front();
        entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
        entry.Kind = NSchemeCache::TSchemeCacheNavigate::KindColumnTable;
        entry.TableId = TableId;
        auto info = MakeIntrusive<NSchemeCache::TSchemeCacheNavigate::TColumnTableInfo>();
        auto* schema = info->Description.MutableSchema();
        const auto settings = MakeSettings();
        for (const auto& column : settings.GetColumns()) {
            auto* description = schema->AddColumns();
            description->SetId(column.GetId());
            description->SetName(column.GetName());
            description->SetTypeId(column.GetTypeId());
        }
        schema->AddKeyColumnNames("key");
        auto* sharding = info->Description.MutableSharding();
        sharding->AddColumnShards(ShardId);
        sharding->MutableHashSharding()->AddColumns("key");
        entry.ColumnTableInfo = std::move(info);
        Runtime.Send(event->Sender, SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(request));
    }

    void ResolveRowTable() {
        auto event = Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvResolveKeySet>(SchemeCache, TDuration::Seconds(1));
        UNIT_ASSERT_C(event, "Expected the writer to resolve DataShards");
        auto request = std::move(event->Get()->Request);
        UNIT_ASSERT_VALUES_EQUAL(request->ResultSet.size(), 1);
        TVector<TKeyDesc::TPartitionInfo> partitions;
        for (const auto& [shardId, endKey] : RowPartitions) {
            TKeyDesc::TPartitionInfo partition(shardId);
            partition.Range.ConstructInPlace();
            if (endKey) {
                partition.Range->EndKeyPrefix = TSerializedCellVec(TVector<TCell>{TCell::Make(*endKey)});
            }
            partitions.push_back(std::move(partition));
        }
        auto& entry = request->ResultSet.front();
        entry.Status = NSchemeCache::TSchemeCacheRequest::EStatus::OkData;
        entry.KeyDescription->Partitioning = std::make_shared<TPartitioning>(std::move(partitions));
        Runtime.Send(event->Sender, SchemeCache, new TEvTxProxySchemeCache::TEvResolveKeySetResult(request));
    }

    const TWriteActorSettings PreviousSettings;
    const ETableKind Kind;
    TPartitions RowPartitions;
    TTestActorRuntime Runtime;
    TActorId Edge;
    TActorId SchemeCache;
    TActorId PipeCache;
    TActorId Owner;
};

class TBufferWriterBootstrap : public TActorBootstrapped<TBufferWriterBootstrap> {
public:
    TBufferWriterBootstrap(TKqpBufferWriterSettings settings, TActorId& writer)
        : Settings(std::move(settings))
        , Writer(writer)
    {}

    void Bootstrap() {
        const auto edge = Settings.SessionActorId;
        auto alloc = Settings.Alloc;
        TGuard guard(*alloc);
        Writer = Register(CreateKqpBufferWriterActor(std::move(Settings)));
        Send(edge, new TEvents::TEvWakeup());
        PassAway();
    }

private:
    TKqpBufferWriterSettings Settings;
    TActorId& Writer;
};

} // namespace

Y_UNIT_TEST_SUITE(KqpBufferWriteActor) {
    enum class EResolveOutcome {
        Rollback,
        LateBootstrap,
        RejectedLock,
        StalePartitioning,
        SerializableRead,
        TransferDisabled,
        MissingTableMetadata,
    };

    void CheckPendingRollback(TTestActorRuntime& runtime, TActorId writer, TActorId edge, TActorId pipeCache,
            const IKqpTransactionManagerPtr& manager, const std::function<void()>& deliverLateEvent) {
        auto rollback = MakeHolder<TEvKqpBuffer::TEvRollback>();
        rollback->ExecuterActorId = edge;
        runtime.Send(writer, edge, rollback.Release());
        auto rollbackWrite = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
        UNIT_ASSERT(rollbackWrite);
        UNIT_ASSERT_VALUES_EQUAL(rollbackWrite->Sender, writer);
        const auto& rollbackRecord = static_cast<const NEvents::TDataEvents::TEvWrite&>(*rollbackWrite->Get()->Ev).Record;
        UNIT_ASSERT(rollbackRecord.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Rollback);
        UNIT_ASSERT(manager->IsRollBack());

        deliverLateEvent();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT(manager->IsRollBack());
        UNIT_ASSERT_VALUES_EQUAL(manager->GetShardsCount(), 1);
        UNIT_ASSERT(manager->HasShard(ShardId));
        UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(ShardId).size(), 1);
        for (const auto& event : runtime.CaptureMailboxEvents(pipeCache.Hint(), pipeCache.NodeId())) {
            UNIT_ASSERT(event->GetTypeRewrite() != TEvPipeCache::EvForward);
        }
        runtime.Send(new IEventHandle(writer, pipeCache,
            NEvents::TDataEvents::TEvWriteResult::BuildCompleted(ShardId).release(), 0, rollbackWrite->Cookie));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvKqpBuffer::TEvResult>(edge));
    }

    TPartitioning::TCPtr MakeSplitPartitioning() {
        TKeyDesc::TPartitionInfo left(ReplacementShardId);
        left.Range.ConstructInPlace();
        left.Range->EndKeyPrefix = TSerializedCellVec(TVector<TCell>{TCell::Make(ui64(100))});
        TKeyDesc::TPartitionInfo right(OtherShardId);
        right.Range.ConstructInPlace();
        return std::make_shared<TPartitioning>(
            TVector<TKeyDesc::TPartitionInfo>{left, right}, 2);
    }

    void CheckWriterResolveOutcome(EResolveOutcome outcome, bool split) {
        TTestActorRuntime runtime;
        TAppPrepare app;
        app.FeatureFlags.SetEnableDataShardUncommittedWriteSeqNum(true);
        app.FeatureFlags.SetEnableDataShardLocksTransferOnSplit(outcome != EResolveOutcome::TransferDisabled);
        runtime.Initialize(app.Unwrap());
        runtime.SetDispatchTimeout(TDuration::Seconds(5));
        const auto edge = runtime.AllocateEdgeActor();
        const auto schemeCache = runtime.AllocateEdgeActor();
        const auto pipeCache = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeSchemeCacheID(), schemeCache);
        runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCache);

        TKeyDesc::TPartitionInfo partition(ShardId);
        partition.Range.ConstructInPlace();
        auto partitioning = std::make_shared<TPartitioning>(
            TVector<TKeyDesc::TPartitionInfo>{partition}, 1);
        auto manager = CreateKqpTransactionManager();
        manager->SetIsolationLevel(outcome == EResolveOutcome::SerializableRead
            ? NKqpProto::ISOLATION_LEVEL_SERIALIZABLE : NKqpProto::ISOLATION_LEVEL_READ_COMMITTED_RW);
        manager->SetHasSnapshot(true);
        if (outcome != EResolveOutcome::MissingTableMetadata) {
            manager->SetTableMeta(TableId, {partitioning, {NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)}});
        }
        if (outcome == EResolveOutcome::SerializableRead) {
            // Plain reads register locks without split/merge range metadata.
            manager->AddShard(ShardId, false, "/Root/Table", TableId.PathId);
            manager->AddAction(ShardId, IKqpTransactionManager::EAction::READ);
            NKikimrDataEvents::TLock readLock;
            readLock.SetLockId(17);
            readLock.SetDataShard(ShardId);
            readLock.SetSchemeShard(TableId.PathId.OwnerId);
            readLock.SetPathId(TableId.PathId.LocalPathId);
            UNIT_ASSERT(manager->AddLock(ShardId, readLock));
        }

        auto alloc = std::make_shared<TScopedAlloc>(__LOCATION__);
        alloc->Release();
        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TActorId writer;
        runtime.Register(new TBufferWriterBootstrap({
            .SessionActorId = edge,
            .TxManager = manager,
            .Counters = counters,
            .TxProxyMon = counters->TxProxyMon,
            .Alloc = alloc,
        }, writer));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge));

        auto settings = MakeSettings(ETableKind::Row);
        ActorIdToProto(writer, settings.MutableBufferActorId());
        settings.SetInconsistentTx(false);
        settings.SetLockTxId(17);
        settings.SetLockNodeId(runtime.GetNodeId());
        settings.SetLockMode(NKikimrDataEvents::PESSIMISTIC_NONE);
        settings.SetDisablePessimisticLocks(true);
        TCallbacks callbacks;
        const auto owner = runtime.Register(new TSinkOwner(callbacks, settings));
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        runtime.Send(owner, edge, new TSinkOwner::TEvExecute([](auto& sink, auto& factory) {
            sink.SendData(MakeData(factory, {1}), 0, Nothing(), true);
        }));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge));
        if (outcome == EResolveOutcome::MissingTableMetadata) {
            // Auxiliary writers resolve themselves without a table entry from
            // the executer, as happens for a fulltext relevance dictionary.
            auto initialResolve = runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvResolveKeySet>(schemeCache);
            UNIT_ASSERT(initialResolve);
            auto request = std::move(initialResolve->Get()->Request);
            UNIT_ASSERT_VALUES_EQUAL(request->ResultSet.size(), 1);
            request->ResultSet.front().Status = NSchemeCache::TSchemeCacheRequest::EStatus::OkData;
            request->ResultSet.front().KeyDescription->Partitioning = partitioning;
            runtime.Send(initialResolve->Sender, schemeCache,
                new TEvTxProxySchemeCache::TEvResolveKeySetResult(request));
        }
        runtime.WaitFor("forward sink finished", [&] { return callbacks.Finished || !callbacks.Errors.Empty(); });
        UNIT_ASSERT_C(callbacks.Errors.Empty(), callbacks.Errors.ToOneLineString());

        auto flush = MakeHolder<TEvKqpBuffer::TEvFlush>();
        flush->ExecuterActorId = edge;
        runtime.Send(writer, edge, flush.Release());
        auto dataWrite = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
        UNIT_ASSERT(dataWrite);
        UNIT_ASSERT_VALUES_EQUAL(dataWrite->Get()->TabletId, ShardId);
        const auto& record = static_cast<const NEvents::TDataEvents::TEvWrite&>(*dataWrite->Get()->Ev).Record;
        UNIT_ASSERT_VALUES_EQUAL(record.OperationsSize(), 1);
        if (outcome == EResolveOutcome::SerializableRead) {
            UNIT_ASSERT_C(callbacks.Errors.Empty(), callbacks.Errors.ToOneLineString());
            UNIT_ASSERT(!manager->BrokenLocks());
            if (split) {
                // The WSN write is still unacknowledged when its pipe breaks.
                UNIT_ASSERT(record.GetOperations(0).HasWriteSeqNum());
                runtime.Send(dataWrite->Sender, pipeCache, new TEvPipeCache::TEvDeliveryProblem(ShardId, false));
                auto resolve = runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvResolveKeySet>(schemeCache);
                UNIT_ASSERT(resolve);
                auto request = std::move(resolve->Get()->Request);
                auto& entry = request->ResultSet.at(0);
                entry.Status = NSchemeCache::TSchemeCacheRequest::EStatus::OkData;
                entry.KeyDescription->Partitioning = MakeSplitPartitioning();
                runtime.Send(resolve->Sender, schemeCache, new TEvTxProxySchemeCache::TEvResolveKeySetResult(request));

                auto error = runtime.GrabEdgeEvent<TEvKqpBuffer::TEvError>(edge);
                UNIT_ASSERT(error);
                UNIT_ASSERT(error->Get()->StatusCode == NYql::NDqProto::StatusIds::UNAVAILABLE);
                UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(ShardId).size(), 1);
                UNIT_ASSERT(!manager->HasShard(ReplacementShardId));
                UNIT_ASSERT(!manager->HasShard(OtherShardId));
                for (const auto& event : runtime.CaptureMailboxEvents(pipeCache.Hint(), pipeCache.NodeId())) {
                    UNIT_ASSERT(event->GetTypeRewrite() != TEvPipeCache::EvForward);
                }
            }
            runtime.Send(writer, edge, new TEvKqpBuffer::TEvTerminate());
            runtime.Send(owner, edge, new TEvents::TEvPoison());
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge));
            return;
        }
        NKikimrDataEvents::TLock lock;
        lock.SetLockId(17);
        lock.SetDataShard(ShardId);
        lock.SetCreatedOn(ShardId);
        lock.SetSchemeShard(TableId.PathId.OwnerId);
        lock.SetPathId(TableId.PathId.LocalPathId);
        lock.SetGeneration(3);
        lock.SetCounter(4);
        lock.SetHasWrites(true);
        *lock.AddWriteSeqNums() = record.GetOperations(0).GetWriteSeqNum();
        runtime.Send(new IEventHandle(dataWrite->Sender, pipeCache,
            NEvents::TDataEvents::TEvWriteResult::BuildCompleted(ShardId, 0, lock).release(),
            0, dataWrite->Cookie));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvKqpBuffer::TEvResult>(edge));
        UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(ShardId).size(), 1);
        if (outcome == EResolveOutcome::LateBootstrap) {
            // Table A already owns a lock. Delay the buffer's child for table B
            // until the rollback request to A is in flight.
            const TTableId otherTable(1, 3, 3);
            TKeyDesc::TPartitionInfo otherPartition(OtherShardId);
            otherPartition.Range.ConstructInPlace();
            manager->SetTableMeta(otherTable, {std::make_shared<TPartitioning>(
                TVector<TKeyDesc::TPartitionInfo>{otherPartition}, 1), {NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)}});
            settings.MutableTable()->SetTableId(otherTable.PathId.LocalPathId);
            settings.MutableTable()->SetPath("/Root/OtherTable");
            TCallbacks otherCallbacks;
            TBlockEvents<TEvents::TEvBootstrap> bootstrap(runtime, [&](const auto& ev) {
                return ev->Sender == writer;
            });
            const auto otherOwner = runtime.Register(new TSinkOwner(otherCallbacks, std::move(settings)));
            runtime.Send(otherOwner, edge, new TSinkOwner::TEvExecute([](auto& sink, auto& factory) {
                sink.SendData(MakeData(factory, {1}), 0, Nothing(), true);
            }), 0, /* viaActorSystem */ true);
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge));
            runtime.WaitFor("table writer bootstrap", [&] { return !bootstrap.empty(); });
            UNIT_ASSERT_VALUES_EQUAL(bootstrap.size(), 1);
            CheckPendingRollback(runtime, writer, edge, pipeCache, manager, [&] { bootstrap.Stop().Unblock(); });
            UNIT_ASSERT_C(otherCallbacks.Errors.Empty(), otherCallbacks.Errors.ToOneLineString());
            for (const auto& id : {owner, otherOwner}) {
                runtime.Send(id, edge, new TEvents::TEvPoison());
                UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge));
            }
            return;
        }
        if (outcome == EResolveOutcome::RejectedLock) {
            // Each copy is valid on its own. The resolve must reject their
            // inconsistent generations before the writer proceeds to prepare.
            const ui64 otherCopy = OtherShardId + 1;
            manager->AddShard(otherCopy, false, "/Root/Table", TableId.PathId);
            manager->AddAction(otherCopy, IKqpTransactionManager::EAction::WRITE);
            auto conflictingCopy = lock;
            conflictingCopy.SetGeneration(lock.GetGeneration() + 1);
            UNIT_ASSERT(manager->AddLock(otherCopy, conflictingCopy));
            UNIT_ASSERT(!manager->BrokenLocks());
        }

        auto commit = MakeHolder<TEvKqpBuffer::TEvCommit>();
        commit->ExecuterActorId = edge;
        commit->TxId = 99;
        runtime.Send(writer, edge, commit.Release());
        if (outcome == EResolveOutcome::TransferDisabled) {
            auto commitWrite = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
            UNIT_ASSERT(commitWrite);
            const auto& committed = static_cast<const NEvents::TDataEvents::TEvWrite&>(*commitWrite->Get()->Ev).Record;
            UNIT_ASSERT_VALUES_EQUAL(committed.GetLocks().LocksSize(), 1);
            const auto& expected = committed.GetLocks().GetLocks(0);
            UNIT_ASSERT_VALUES_EQUAL(expected.GetCreatedOn(), ShardId);
            UNIT_ASSERT_VALUES_EQUAL(expected.GetWriteSeqNums(0).GetWriteSeqNum(), lock.GetWriteSeqNums(0).GetWriteSeqNum());
            runtime.Send(new IEventHandle(commitWrite->Sender, pipeCache,
                NEvents::TDataEvents::TEvWriteResult::BuildCompleted(ShardId, 99).release(), 0, commitWrite->Cookie));
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvKqpBuffer::TEvResult>(edge));
            UNIT_ASSERT(manager->IsTxFinished());
            runtime.Send(owner, edge, new TEvents::TEvPoison());
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge));
            return;
        }
        auto resolve = runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvResolveKeySet>(schemeCache);
        UNIT_ASSERT(resolve);
        // Hold the child writer's SchemeCache reply until the test releases it.
        UNIT_ASSERT(resolve->Sender != writer);
        if (outcome == EResolveOutcome::StalePartitioning) {
            // An independent lock actor registers a successor while the writer's
            // pre-prepare request for the old topology is in flight.
            const auto current = MakeSplitPartitioning();
            manager->AddShard(ReplacementShardId, false, "/Root/Table", TableId.PathId,
                MakeShardMeta(TableId.PathId, current, ReplacementShardId, 1));
            manager->AddAction(ReplacementShardId, IKqpTransactionManager::EAction::PESSIMISTIC_LOCK);
            UNIT_ASSERT(manager->AddLock(ReplacementShardId, lock));
            auto reply = [&](const TPartitioning::TCPtr& topology) {
                auto request = std::move(resolve->Get()->Request);
                auto& entry = request->ResultSet.at(0);
                entry.Status = NSchemeCache::TSchemeCacheRequest::EStatus::OkData;
                entry.KeyDescription->Partitioning = topology;
                runtime.Send(resolve->Sender, schemeCache, new TEvTxProxySchemeCache::TEvResolveKeySetResult(request));
            };
            // A repeated stale result must neither complete the round nor
            // invalidate a participant. Only an accepted result resets the budget.
            const ui64 retries = split ? 1 : GetWriteActorSettings().MaxResolveAttempts;
            for (ui64 i = 0; i < retries; ++i) {
                reply(partitioning);
                resolve = runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvResolveKeySet>(schemeCache);
                UNIT_ASSERT(resolve);
                UNIT_ASSERT(!manager->BrokenLocks());
                UNIT_ASSERT(manager->HasShard(ShardId));
                UNIT_ASSERT_VALUES_EQUAL(manager->GetLocks(ReplacementShardId).size(), 1);
                for (const auto& event : runtime.CaptureMailboxEvents(pipeCache.Hint(), pipeCache.NodeId())) {
                    UNIT_ASSERT(event->GetTypeRewrite() != TEvPipeCache::EvForward);
                }
            }
            if (split) {
                reply(current);
                THashSet<ui64> prepared;
                for (size_t i = 0; i < 2; ++i) {
                    auto write = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
                    UNIT_ASSERT(write);
                    const auto& msg = static_cast<const NEvents::TDataEvents::TEvWrite&>(*write->Get()->Ev).Record;
                    UNIT_ASSERT(msg.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Commit);
                    UNIT_ASSERT_VALUES_EQUAL(msg.GetLocks().LocksSize(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(msg.GetLocks().GetLocks(0).GetCreatedOn(), ShardId);
                    prepared.insert(write->Get()->TabletId);
                }
                UNIT_ASSERT(prepared.contains(ReplacementShardId) && prepared.contains(OtherShardId));
                UNIT_ASSERT(!manager->HasShard(ShardId));
            } else {
                reply(partitioning);
                auto error = runtime.GrabEdgeEvent<TEvKqpBuffer::TEvError>(edge);
                UNIT_ASSERT(error);
                UNIT_ASSERT(error->Get()->StatusCode == NYql::NDqProto::StatusIds::UNAVAILABLE);
                UNIT_ASSERT(!manager->BrokenLocks());
            }
            runtime.Send(writer, edge, new TEvKqpBuffer::TEvTerminate());
            runtime.Send(owner, edge, new TEvents::TEvPoison());
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge));
            return;
        }
        auto request = std::move(resolve->Get()->Request);
        UNIT_ASSERT_VALUES_EQUAL(request->ResultSet.size(), 1);
        auto& entry = request->ResultSet.front();
        entry.Status = NSchemeCache::TSchemeCacheRequest::EStatus::OkData;
        entry.KeyDescription->Partitioning = split ? MakeSplitPartitioning() : partitioning;

        if (outcome == EResolveOutcome::MissingTableMetadata) {
            runtime.Send(resolve->Sender, schemeCache, new TEvTxProxySchemeCache::TEvResolveKeySetResult(request));
            auto write = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
            UNIT_ASSERT(write);
            const auto& record = static_cast<const NEvents::TDataEvents::TEvWrite&>(*write->Get()->Ev).Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetLocks().LocksSize(), 1);
            UNIT_ASSERT_EQUAL(record.GetLocks().GetOp(), NKikimrDataEvents::TKqpLocks::Commit);
            runtime.Send(new IEventHandle(write->Sender, pipeCache,
                NEvents::TDataEvents::TEvWriteResult::BuildCompleted(ShardId, 99).release(), 0, write->Cookie));
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvKqpBuffer::TEvResult>(edge));
            UNIT_ASSERT(manager->IsTxFinished());
            runtime.Send(owner, edge, new TEvents::TEvPoison());
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge));
            return;
        }
        if (outcome == EResolveOutcome::RejectedLock) {
            runtime.Send(resolve->Sender, schemeCache, new TEvTxProxySchemeCache::TEvResolveKeySetResult(request));
            auto error = runtime.GrabEdgeEvent<TEvKqpBuffer::TEvError>(edge);
            UNIT_ASSERT(error);
            UNIT_ASSERT(error->Get()->StatusCode == NYql::NDqProto::StatusIds::ABORTED);
            UNIT_ASSERT(manager->BrokenLocks());
            // OnError synchronously terminates the child writer. The resolve
            // handler must stop before accessing the cleared controller.
            for (const auto& event : runtime.CaptureMailboxEvents(pipeCache.Hint(), pipeCache.NodeId())) {
                UNIT_ASSERT(event->GetTypeRewrite() != TEvPipeCache::EvForward);
            }
            runtime.Send(writer, edge, new TEvKqpBuffer::TEvTerminate());
            runtime.Send(owner, edge, new TEvents::TEvPoison());
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge));
            return;
        }

        CheckPendingRollback(runtime, writer, edge, pipeCache, manager, [&] {
            runtime.Send(resolve->Sender, schemeCache, new TEvTxProxySchemeCache::TEvResolveKeySetResult(request));
        });
        UNIT_ASSERT_C(callbacks.Errors.Empty(), callbacks.Errors.ToOneLineString());

        runtime.Send(owner, edge, new TEvents::TEvPoison());
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge));
    }

    Y_UNIT_TEST(LateWriterBootstrapAfterRollback) {
        CheckWriterResolveOutcome(EResolveOutcome::LateBootstrap, false);
    }

    Y_UNIT_TEST(LateWriterResolveAfterRollback) {
        for (const bool split : {false, true}) {
            CheckWriterResolveOutcome(EResolveOutcome::Rollback, split);
        }
    }

    Y_UNIT_TEST(StalePartitioningRetriesBeforePrepare) {
        CheckWriterResolveOutcome(EResolveOutcome::StalePartitioning, true);
    }

    Y_UNIT_TEST(StalePartitioningRetryLimitReturnsUnavailable) {
        CheckWriterResolveOutcome(EResolveOutcome::StalePartitioning, false);
    }

    Y_UNIT_TEST(RejectedWriterResolveStopsProcessing) {
        for (const bool split : {false, true}) {
            CheckWriterResolveOutcome(EResolveOutcome::RejectedLock, split);
        }
    }

    Y_UNIT_TEST(SerializableReadBeforeFirstWrite) {
        CheckWriterResolveOutcome(EResolveOutcome::SerializableRead, false);
    }

    Y_UNIT_TEST(SerializableWriteRejectsSplitBeforeReroute) {
        const auto previous = GetWriteActorSettings();
        Y_DEFER { SetWriteActorSettings(MakeIntrusive<TWriteActorSettings>(previous)); };
        auto settings = MakeIntrusive<TWriteActorSettings>(previous);
        settings->MaxWriteAttempts = 1;
        SetWriteActorSettings(settings);
        CheckWriterResolveOutcome(EResolveOutcome::SerializableRead, true);
    }

    Y_UNIT_TEST(LocalWriteWithSeqNumCommitsWithTransferDisabled) {
        CheckWriterResolveOutcome(EResolveOutcome::TransferDisabled, false);
    }

    Y_UNIT_TEST(SelfResolvedWriterCommitsWithoutStoredKeyTypes) {
        CheckWriterResolveOutcome(EResolveOutcome::MissingTableMetadata, false);
    }

    void CheckLockTablesWithoutWriteActors(bool transfer, bool single = false) {
        TTestActorRuntime runtime;
        TAppPrepare app;
        app.FeatureFlags.SetEnableDataShardUncommittedWriteSeqNum(true);
        app.FeatureFlags.SetEnableDataShardLocksTransferOnSplit(transfer);
        runtime.Initialize(app.Unwrap());
        runtime.SetDispatchTimeout(TDuration::Seconds(5));
        const auto edge = runtime.AllocateEdgeActor();
        const auto schemeCache = runtime.AllocateEdgeActor();
        const auto pipeCache = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeSchemeCacheID(), schemeCache);
        runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCache);

        const TTableId otherTable(1, 3, 3);
        auto manager = CreateKqpTransactionManager();
        manager->SetIsolationLevel(NKqpProto::ISOLATION_LEVEL_READ_COMMITTED_RW);
        manager->SetHasSnapshot(true);
        for (const auto& [table, shard] : {std::pair{TableId, ShardId}, std::pair{otherTable, OtherShardId}}) {
            if (single && shard != ShardId) {
                continue;
            }
            manager->AddShard(shard, false, "/Root/Table", table.PathId);
            manager->AddAction(shard, shard == ShardId
                ? IKqpTransactionManager::EAction::PESSIMISTIC_LOCK : IKqpTransactionManager::EAction::WRITE);
            NKikimrDataEvents::TLock lock;
            lock.SetLockId(17);
            lock.SetDataShard(shard);
            lock.SetCreatedOn(shard);
            lock.SetSchemeShard(table.PathId.OwnerId);
            lock.SetPathId(table.PathId.LocalPathId);
            lock.SetGeneration(3);
            lock.SetCounter(4);
            lock.SetHasWrites(!single);
            if (!single) {
                auto* seq = lock.AddWriteSeqNums();
                seq->SetWriterIndex(1);
                seq->SetWriteSeqNum(5);
            }
            UNIT_ASSERT(manager->AddLock(shard, lock));
        }
        // Without a writer the original external shards still validate their locks.
        auto alloc = std::make_shared<TScopedAlloc>(__LOCATION__);
        alloc->Release();
        auto counters = MakeIntrusive<TKqpCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TActorId writer;
        runtime.Register(new TBufferWriterBootstrap({
            .SessionActorId = edge,
            .TxManager = manager,
            .Counters = counters,
            .TxProxyMon = counters->TxProxyMon,
            .Alloc = alloc,
        }, writer));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge));
        auto commit = MakeHolder<TEvKqpBuffer::TEvCommit>();
        commit->ExecuterActorId = edge;
        commit->TxId = 99;
        runtime.Send(writer, edge, commit.Release());
        if (!transfer && !single) {
            // With transfer disabled, validation must stay on the original
            // tablets. A missing lock there must prevent coordinator planning.
            THashSet<ui64> requested;
            for (size_t i = 0; i < 2; ++i) {
                auto write = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
                UNIT_ASSERT(write);
                const ui64 shard = write->Get()->TabletId;
                UNIT_ASSERT(shard == ShardId || shard == OtherShardId);
                UNIT_ASSERT(requested.insert(shard).second);
                const auto& record = static_cast<const NEvents::TDataEvents::TEvWrite&>(*write->Get()->Ev).Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetLocks().LocksSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetLocks().GetLocks(0).GetDataShard(), shard);
            }
            auto rejected = MakeHolder<NEvents::TDataEvents::TEvWriteResult>();
            rejected->Record.SetOrigin(ShardId);
            rejected->Record.SetTxId(99);
            rejected->Record.SetStatus(NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED);
            runtime.Send(writer, pipeCache, rejected.Release());
            const auto error = runtime.GrabEdgeEvent<TEvKqpBuffer::TEvError>(edge);
            UNIT_ASSERT(error);
            UNIT_ASSERT(error->Get()->StatusCode == NYql::NDqProto::StatusIds::ABORTED);
            return;
        }
        THashSet<ui64> prepared;
        const size_t participants = single ? 1 : 2;
        for (size_t i = 0; i < participants; ++i) {
            auto write = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
            UNIT_ASSERT(write);
            const auto& record = static_cast<const NEvents::TDataEvents::TEvWrite&>(*write->Get()->Ev).Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetTxMode(), NKikimrDataEvents::TEvWrite::MODE_PREPARE);
            UNIT_ASSERT_VALUES_EQUAL(record.OperationsSize(), 0);
            UNIT_ASSERT(record.GetLocks().GetOp() == NKikimrDataEvents::TKqpLocks::Commit);
            UNIT_ASSERT_VALUES_EQUAL(record.GetLocks().LocksSize(), 1);
            const ui64 shard = write->Get()->TabletId;
            UNIT_ASSERT(shard == ShardId || (!single && shard == OtherShardId));
            UNIT_ASSERT(prepared.insert(shard).second);
            auto result = MakeHolder<NEvents::TDataEvents::TEvWriteResult>();
            result->Record.SetOrigin(shard);
            result->Record.SetTxId(99);
            result->Record.SetStatus(NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
            result->Record.SetMinStep(100);
            result->Record.SetMaxStep(200);
            result->Record.AddDomainCoordinators(777);
            runtime.Send(writer, pipeCache, result.Release());
        }
        auto proposal = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeCache);
        UNIT_ASSERT(proposal);
        UNIT_ASSERT_VALUES_EQUAL(proposal->Get()->Ev->Type(), TEvTxProxy::TEvProposeTransaction::EventType);
        const auto& transaction = static_cast<const TEvTxProxy::TEvProposeTransaction&>(*proposal->Get()->Ev).Record.GetTransaction();
        UNIT_ASSERT_VALUES_EQUAL(transaction.AffectedSetSize(), participants);
        for (const auto& affected : transaction.GetAffectedSet()) {
            UNIT_ASSERT(prepared.contains(affected.GetTabletId()));
            UNIT_ASSERT(affected.GetFlags() & IKqpTransactionManager::EAction::WRITE);
        }
        for (const ui64 shard : prepared) {
            runtime.Send(writer, pipeCache, NEvents::TDataEvents::TEvWriteResult::BuildCompleted(shard, 99).release());
        }
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvKqpBuffer::TEvResult>(edge));
        UNIT_ASSERT(manager->IsTxFinished());
    }

    Y_UNIT_TEST(ExternalLockTablesKeepOriginalParticipants) {
        CheckLockTablesWithoutWriteActors(true);
    }

    Y_UNIT_TEST(DisabledTransferKeepsOriginalLockParticipants) {
        CheckLockTablesWithoutWriteActors(false);
    }

    Y_UNIT_TEST(SingleExternalPessimisticLockCommitsThroughPrepare) {
        for (const bool transfer : {false, true}) {
            CheckLockTablesWithoutWriteActors(transfer, true);
        }
    }
}

Y_UNIT_TEST_SUITE(KqpDirectWriteActor) {
    // These event-driven scenarios reproduce the stable-version failures.
    // Main retains in-flight column batches during re-resolution; the same tests
    // also verify checkpoint and backpressure behavior under that retry policy.
    Y_UNIT_TEST(ResumesAfterCheckpointFlushExhaustsSpace) {
        // Use the real Arrow batcher's logical size and capacity to put the
        // writer limit between its pre-flush and post-flush memory usage.
        i64 memoryLimit = 0;
        {
            auto alloc = std::make_shared<TScopedAlloc>(__LOCATION__);
            TMemoryUsageInfo memoryInfo("KqpDirectWriteActorTest");
            THolderFactory holderFactory(alloc->Ref(), memoryInfo);
            const auto settings = MakeSettings();
            const TVector<NKikimrKqp::TKqpColumnMetadataProto> columns(settings.GetColumns().begin(), settings.GetColumns().end());
            auto batcher = CreateColumnDataBatcher(columns, {0, 1}, alloc);
            batcher->AddData(MakeData(holderFactory, {1}));
            const auto batch = batcher->Build();
            UNIT_ASSERT(batch->GetMemory() > batch->GetSerializedMemory());
            memoryLimit = (batch->GetMemory() + batch->GetSerializedMemory()) / 2;
        }

        TSinkFixture fixture(ETableKind::Column, memoryLimit);
        fixture.Write(1, MakeCheckpoint(1));
        const auto original = fixture.GrabWrite();
        UNIT_ASSERT(fixture.GetFreeSpace() < 0);

        fixture.Retry(original);
        const auto retried = fixture.GrabWrite();
        UNIT_ASSERT(fixture.GetFreeSpace() < 0);
        UNIT_ASSERT(fixture.Callbacks.SavedCheckpoints.empty());
        const auto resumes = fixture.Callbacks.Resumes;

        fixture.Acknowledge(retried);
        UNIT_ASSERT(fixture.GetFreeSpace() > 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Resumes, resumes + 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.SavedCheckpoints, TVector<ui64>{1});
    }

    Y_UNIT_TEST(CheckpointFlushesAfterColumnResolve) {
        TSinkFixture fixture;
        fixture.Write(1, MakeCheckpoint(1));
        const auto original = fixture.GrabWrite();
        UNIT_ASSERT(fixture.GetFreeSpace() > 0);

        fixture.Retry(original);
        const auto retried = fixture.GrabWrite();
        UNIT_ASSERT(fixture.Callbacks.SavedCheckpoints.empty());
        fixture.Acknowledge(retried);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.SavedCheckpoints, TVector<ui64>{1});
    }

    Y_UNIT_TEST(FinishedSinkFlushesCheckpointAfterColumnResolve) {
        TSinkFixture fixture;
        fixture.Write(1, MakeCheckpoint(1));
        const auto original = fixture.GrabWrite();
        // The finish flag belongs to data queued behind the pending checkpoint;
        // the underlying table writer must remain open until it consumes that data.
        fixture.Write(2, Nothing(), true);

        fixture.Retry(original);
        const auto retried = fixture.GrabWrite();
        UNIT_ASSERT(fixture.Callbacks.SavedCheckpoints.empty());
        UNIT_ASSERT(!fixture.Callbacks.Finished);
        fixture.Acknowledge(retried);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.SavedCheckpoints, TVector<ui64>{1});
        UNIT_ASSERT(!fixture.Callbacks.Finished);

        const auto last = fixture.GrabWrite(2);
        fixture.Acknowledge(last);
        UNIT_ASSERT(fixture.Callbacks.Finished);
    }

    Y_UNIT_TEST(CheckpointsWaitForRetriedColumnWrites) {
        TSinkFixture fixture;
        fixture.Write(1, MakeCheckpoint(1));
        const auto original = fixture.GrabWrite();
        fixture.Write(2, MakeCheckpoint(2));

        // On stable, re-resolution moves the first checkpoint's data back into
        // unprepared Arrow batches. The checkpoint must still wait for its ACK.
        fixture.Retry(original);
        UNIT_ASSERT(fixture.Callbacks.SavedCheckpoints.empty());
        const auto retried = fixture.GrabWrite();
        fixture.Acknowledge(retried);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.SavedCheckpoints, TVector<ui64>{1});
        // Main retains the original cookie; stable replaces it during resharding.
        // In either case a duplicate/stale ACK cannot complete the next checkpoint.
        fixture.Acknowledge(original);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.SavedCheckpoints, TVector<ui64>{1});

        const auto second = fixture.GrabWrite(2);
        fixture.Acknowledge(second);
        const TVector<ui64> expected = {1, 2};
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.SavedCheckpoints, expected);
    }

    Y_UNIT_TEST(CheckpointWaitsForAllReplacementDataShards) {
        TSinkFixture fixture(ETableKind::Row);
        fixture.Write(TVector<ui64>{1, 3}, MakeCheckpoint(1));
        const auto original = fixture.GrabWrites({{ShardId, {1, 3}}}).at(ShardId);

        fixture.Retry(original, {{ReplacementShardId, 2}, {OtherShardId, Nothing()}});
        const auto replacements = fixture.GrabWrites({{ReplacementShardId, {1}}, {OtherShardId, {3}}});
        UNIT_ASSERT(fixture.Callbacks.SavedCheckpoints.empty());
        fixture.Acknowledge(original);
        UNIT_ASSERT(fixture.Callbacks.SavedCheckpoints.empty());
        fixture.Acknowledge(replacements.at(ReplacementShardId));
        UNIT_ASSERT(fixture.Callbacks.SavedCheckpoints.empty());
        fixture.Acknowledge(replacements.at(OtherShardId));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.SavedCheckpoints, TVector<ui64>{1});
        fixture.AssertNoWrites();
    }

    Y_UNIT_TEST(CheckpointKeepsSurvivingDataShardInFlight) {
        TSinkFixture fixture(ETableKind::Row, 64_MB, {{ShardId, 10}, {OtherShardId, Nothing()}});
        fixture.Write(TVector<ui64>{1, 20}, MakeCheckpoint(1));
        const auto original = fixture.GrabWrites({{ShardId, {1}}, {OtherShardId, {20}}});

        fixture.Retry(original.at(ShardId), {{ReplacementShardId, 10}, {OtherShardId, Nothing()}});
        const auto replacement = fixture.GrabWrite(1, ReplacementShardId);
        fixture.AssertNoWrites();
        fixture.Acknowledge(replacement);
        UNIT_ASSERT(fixture.Callbacks.SavedCheckpoints.empty());
        fixture.Acknowledge(original.at(ShardId));
        UNIT_ASSERT(fixture.Callbacks.SavedCheckpoints.empty());
        fixture.Acknowledge(original.at(OtherShardId));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.SavedCheckpoints, TVector<ui64>{1});
        fixture.AssertNoWrites();
    }

    Y_UNIT_TEST(FinishedSinkCompletesCheckpointAfterDataShardDeletion) {
        TSinkFixture fixture(ETableKind::Row);
        fixture.Write(1, MakeCheckpoint(1));
        const auto original = fixture.GrabWrite();
        fixture.Write(2, Nothing(), true);

        fixture.Retry(original, {{ReplacementShardId, Nothing()}});
        const auto replacement = fixture.GrabWrite(1, ReplacementShardId);
        UNIT_ASSERT(fixture.Callbacks.SavedCheckpoints.empty());
        UNIT_ASSERT(!fixture.Callbacks.Finished);
        fixture.Acknowledge(replacement);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.SavedCheckpoints, TVector<ui64>{1});
        UNIT_ASSERT(!fixture.Callbacks.Finished);
        const auto last = fixture.GrabWrite(2, ReplacementShardId);
        fixture.Acknowledge(last);
        UNIT_ASSERT(fixture.Callbacks.Finished);
    }

    Y_UNIT_TEST(ClosedWriterFinishesAfterDataShardDeletion) {
        TSinkFixture fixture(ETableKind::Row);
        fixture.Write(1, Nothing(), true);
        const auto original = fixture.GrabWrite();

        fixture.Retry(original, {{ReplacementShardId, Nothing()}});
        const auto replacement = fixture.GrabWrite(1, ReplacementShardId);
        UNIT_ASSERT(!fixture.Callbacks.Finished);
        fixture.Acknowledge(original);
        UNIT_ASSERT(!fixture.Callbacks.Finished);
        fixture.Acknowledge(replacement);
        UNIT_ASSERT(fixture.Callbacks.Finished);
    }

    // A late retryable rejection of a superseded message (the resend carries
    // a fresh cookie) must be dropped by the cookie filter instead of failing
    // the query or burning the resend budget; the query then completes on the
    // resend's own result.
    Y_UNIT_TEST(LateSupersededRetryableResultIsDropped) {
        TSinkFixture fixture;
        fixture.Write(1, Nothing(), true);
        const auto original = fixture.GrabWrite();

        fixture.Retry(original);
        const auto retried = fixture.GrabWrite();

        fixture.FailWrite(original, NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED);
        UNIT_ASSERT(fixture.Callbacks.Errors.Empty());
        UNIT_ASSERT(!fixture.Callbacks.Finished);

        fixture.Acknowledge(retried);
        UNIT_ASSERT(fixture.Callbacks.Finished);
        UNIT_ASSERT(fixture.Callbacks.Errors.Empty());
    }

    // A late fatal error of a superseded message must fail the query
    // immediately instead of being dropped: the latest attempt would hit the
    // same shard-side problem, so waiting for its answer only delays the
    // inevitable failure.
    Y_UNIT_TEST(LateSupersededFatalResultFailsImmediately) {
        TSinkFixture fixture;
        fixture.Write(1, Nothing(), true);
        const auto original = fixture.GrabWrite();

        fixture.Retry(original);
        const auto retried = fixture.GrabWrite();

        fixture.FailWriteTerminally(original, NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED);
        UNIT_ASSERT(!fixture.Callbacks.Finished);
    }

    Y_UNIT_TEST(ResumesAfterDataShardReplacementFreesSpace) {
        TSinkFixture fixture(ETableKind::Row, 1);
        fixture.Write(1, MakeCheckpoint(1));
        const auto original = fixture.GrabWrite();
        UNIT_ASSERT(fixture.GetFreeSpace() < 0);

        fixture.Retry(original, {{ReplacementShardId, Nothing()}});
        const auto replacement = fixture.GrabWrite(1, ReplacementShardId);
        UNIT_ASSERT(fixture.GetFreeSpace() < 0);
        const auto resumes = fixture.Callbacks.Resumes;
        fixture.Acknowledge(replacement);
        UNIT_ASSERT(fixture.GetFreeSpace() > 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.Resumes, resumes + 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.SavedCheckpoints, TVector<ui64>{1});
    }

    const NKikimrDataEvents::TEvWriteResult::EStatus OverloadStatuses[] = {
        NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED,
        NKikimrDataEvents::TEvWriteResult::STATUS_DISK_GROUP_OUT_OF_SPACE,
    };

    Y_UNIT_TEST(OverloadWithoutSubscriptionRetriesAndRecovers) {
        for (const auto status : OverloadStatuses) {
            TSinkFixture fixture;
            fixture.Write(1, MakeCheckpoint(1));
            const auto original = fixture.GrabWrite();

            fixture.FailWrite(original, status);
            fixture.Resolve();
            const auto retried = fixture.GrabWrite();
            fixture.Acknowledge(retried);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.SavedCheckpoints, TVector<ui64>{1});
        }
    }

    Y_UNIT_TEST(OverloadWithoutSubscriptionFailsAfterRetryBudget) {
        for (const auto status : OverloadStatuses) {
            TSinkFixture fixture(ETableKind::Column, 64_MB, {{ShardId, Nothing()}}, 1);
            fixture.Write(1, MakeCheckpoint(1));
            const auto original = fixture.GrabWrite();

            fixture.FailWrite(original, status);
            fixture.Resolve();
            const auto retried = fixture.GrabWrite();
            fixture.FailWriteTerminally(retried, status);
        }
    }

    // An acknowledged overload subscription (a probability rejection whose reply
    // round-trips the request cookie) must only make the sink wait for
    // TEvOverloadReady: the still-unacknowledged batch keeps its accumulated
    // send attempts, so the bounded retry budget is not silently refilled and a
    // later retry trigger re-resolves instead of resending at attempt zero.
    Y_UNIT_TEST(SubscribedOverloadRejectionPreservesRetryBudget) {
        TSinkFixture fixture; // MaxWriteAttempts = 1
        fixture.Write(1, MakeCheckpoint(1));
        const auto original = fixture.GrabWrite();

        fixture.FailWriteSubscribed(original);
        UNIT_ASSERT(fixture.Callbacks.Errors.Empty());
        fixture.AssertNoWrites();

        // The single attempt is still spent, so the retry trigger must take the
        // budget-exhausted re-resolve path rather than resend immediately.
        fixture.Retry(original);
        const auto retried = fixture.GrabWrite();
        fixture.Acknowledge(retried);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.SavedCheckpoints, TVector<ui64>{1});
    }

    // After an acknowledged overload subscription the sink must resend exactly
    // once TEvOverloadReady arrives (and not before), so a stalled overloaded
    // shard neither gets hammered nor stalls the writer forever.
    Y_UNIT_TEST(SubscribedOverloadRejectionWaitsForShardReady) {
        TSinkFixture fixture;
        fixture.Write(1, MakeCheckpoint(1));
        const auto original = fixture.GrabWrite();

        fixture.FailWriteSubscribed(original);
        fixture.AssertNoWrites();

        fixture.ShardReady(original);
        const auto retried = fixture.GrabWrite();
        fixture.Acknowledge(retried);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Callbacks.SavedCheckpoints, TVector<ui64>{1});
    }

}

} // namespace NKikimr::NKqp
