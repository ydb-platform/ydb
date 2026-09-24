#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/runtime/kqp_write_actor.h>
#include <ydb/core/kqp/runtime/kqp_write_actor_settings.h>
#include <ydb/core/kqp/runtime/kqp_write_table.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <yql/essentials/minikql/mkql_string_util.h>

#include <library/cpp/testing/unittest/registar.h>

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
        : Callbacks(callbacks)
        , Kind(kind)
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
        output.MutableSink()->MutableSettings()->PackFrom(MakeSettings(Kind));
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
    const ETableKind Kind;
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

} // namespace

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

}

} // namespace NKikimr::NKqp
