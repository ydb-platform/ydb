#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/dictionary/constructor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/constructor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/scheme/scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/tiering/tiering.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/storage.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>

#include <ydb/library/formats/arrow/size_calcer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr::NOlap::NActualizer {

namespace {

const TInternalPathId TestPathId = TInternalPathId::FromRawValue(1);
constexpr ui64 TestPortionId = 1;
constexpr ui32 PkColumnId = 1;
constexpr ui64 MemoryLimit = 512 * 1024 * 1024;

const TString Tier1 = NColumnShard::NTiers::TExternalStorageId("/Root/tier1").GetConfigPath();
const TString Tier2 = NColumnShard::NTiers::TExternalStorageId("/Root/tier2").GetConfigPath();
const TDuration Tier1EvictAfter = TDuration::Hours(1);
const TDuration Tier2EvictAfter = TDuration::Hours(2);

TIndexInfo MakeTestIndexInfo(const std::shared_ptr<TSchemaObjectsCache>& cache, const ui64 version, const bool schemeNeedActualization,
    const std::optional<TString>& sparseDefault = {}, const std::shared_ptr<NArrow::NAccessor::IConstructor>& payloadConstructor = nullptr) {
    NKikimrSchemeOp::TColumnTableSchema proto;
    *proto.MutableColumns()->Add() = NArrow::NTest::TTestColumn("pk", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)).CreateColumn(PkColumnId);
    if (sparseDefault || payloadConstructor) {
        const auto typeId = payloadConstructor && payloadConstructor->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray
                                ? NScheme::NTypeIds::JsonDocument
                                : NScheme::NTypeIds::Utf8;
        auto column = NArrow::NTest::TTestColumn("payload", NScheme::TTypeInfo(typeId)).CreateColumn(2);
        if (sparseDefault) {
            *column.MutableDefaultValue() =
                TColumnDefaultScalarValue(std::make_shared<arrow::StringScalar>(std::string(*sparseDefault))).SerializeToProto();
        }
        NArrow::NAccessor::TConstructorContainer accessor(
            payloadConstructor ? payloadConstructor : std::make_shared<NArrow::NAccessor::NSparsed::TConstructor>());
        *column.MutableDataAccessorConstructor() = accessor.SerializeToProto();
        *proto.AddColumns() = column;
        *proto.AddColumns() = NArrow::NTest::TTestColumn("absent", NScheme::TTypeInfo(NScheme::NTypeIds::Int64)).CreateColumn(3);
    }
    proto.AddKeyColumnNames("pk");
    proto.SetVersion(version);
    proto.MutableOptions()->SetSchemeNeedActualization(schemeNeedActualization);

    auto result = TIndexInfo::BuildFromProto(version, proto, TTestStoragesManager::GetInstance(), cache);
    UNIT_ASSERT(result);
    return std::move(*result);
}

TTiering MakeTestTiering() {
    TTiering result;
    UNIT_ASSERT(result.Add(std::make_shared<TTierInfo>(NColumnShard::NTiers::TExternalStorageId(Tier1), Tier1EvictAfter, "pk", 1)));
    UNIT_ASSERT(result.Add(std::make_shared<TTierInfo>(NColumnShard::NTiers::TExternalStorageId(Tier2), Tier2EvictAfter, "pk", 1)));
    return result;
}

class TTestEnv {
private:
    const TInstant TestStart = TInstant::Now();

    const std::shared_ptr<TSchemaObjectsCache> Cache = std::make_shared<TSchemaObjectsCache>();
    const std::shared_ptr<IStoragesManager> Storages = TTestStoragesManager::GetInstance();
    const std::shared_ptr<NDataLocks::TManager> DataLocksManager = std::make_shared<NDataLocks::TManager>();
    TVersionedIndex VersionedIndex;
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Portions;
    std::optional<TTieringActualizer> Actualizer;

    YDB_READONLY_DEF(std::shared_ptr<TController>, Controller);

public:
    TTestEnv()
        : Controller(std::make_shared<TController>())
    {
        VersionedIndex.AddIndex(TSnapshot(1, 1), Cache->UpsertIndexInfo(MakeTestIndexInfo(Cache, 1, false)));
        Actualizer.emplace(TestPathId, VersionedIndex, Storages);
    }

    TRWAddress MakeAddress(const TString& targetTier) const {
        const auto& indexInfo = VersionedIndex.GetLastSchema()->GetIndexInfo();
        return TRWAddress(indexInfo.GetUsedStorageIds(IStoragesManager::DefaultStorageId), indexInfo.GetUsedStorageIds(targetTier));
    }

    // Eviction of a portion living on the default storage to targetTier, i.e. features addressed as MakeAddress(targetTier).
    TPortionEvictionFeatures MakeEvictionFeatures(const TString& targetTier) const {
        const auto schema = VersionedIndex.GetLastSchema();
        TPortionEvictionFeatures result(schema, schema, IStoragesManager::DefaultStorageId);
        result.SetTargetTierName(targetTier);
        return result;
    }

    // A portion on the default storage whose eviction column (the pk) holds T - dataAge.
    std::shared_ptr<TPortionInfo> MakePortion(const TDuration dataAge) const {
        const TInstant maxPk = TestStart - dataAge;
        auto result =
            NTest::MakeTestCompactedPortion(TestPathId, TestPortionId, maxPk.Seconds(), maxPk.Seconds(), 10, TSnapshot(1, 1), std::nullopt);
        result->AddRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized);
        return result;
    }

    // Builds the context the way StartTtl does: a fresh one per round, sharing the tablet-wide controller.
    TTieringProcessContext MakeProcessContext() const {
        TSaverContext saverContext(Storages);
        return TTieringProcessContext(
            MemoryLimit, saverContext, DataLocksManager, VersionedIndex, NColumnShard::TEngineLogsCounters(), Controller);
    }

    // Queues a portion at T - insertedAgo, with its pk holding T - insertedAgo - ageOnInsert. The queue key is
    // decided at that instant and is never recomputed afterwards.
    void QueuePortion(const TDuration insertedAgo, const TDuration ageOnInsert) {
        Portions.emplace(TestPortionId, MakePortion(insertedAgo + ageOnInsert));
        Actualizer->Refresh(MakeTestTiering(), TAddExternalContext(TestStart - insertedAgo, Portions));
    }

    void QueueTieredPortion(const TString& storageId, const TDuration dataAge) {
        const TInstant maxPk = TestStart - dataAge;
        auto tiered = NTest::MakeTestCompactedPortion(
            TestPathId, TestPortionId, maxPk.Seconds(), maxPk.Seconds(), 10, TSnapshot(1, 1), std::nullopt, storageId);
        tiered->AddRuntimeFeature(TPortionInfo::ERuntimeFeature::Optimized);
        Portions.emplace(TestPortionId, std::move(tiered));
    }

    void RefreshTiering(const std::optional<TTiering>& tiering) {
        Actualizer->Refresh(tiering, TAddExternalContext(TestStart, Portions));
    }

    THashMap<TRWAddress, std::vector<TTaskConstructor>> ExtractTasks() {
        auto context = MakeProcessContext();
        TInternalTasksContext internalContext;
        Actualizer->ExtractTasks(context, TExternalTasksContext(Portions), internalContext);
        return context.GetTasks();
    }
};

// The scheme actualizer rewrites portions onto the latest critical schema. Its version is registered as v2, so a
// portion written under v1 is a rewrite candidate until it resolves to v2.
class TSchemeTestEnv {
private:
    const std::shared_ptr<TSchemaObjectsCache> Cache = std::make_shared<TSchemaObjectsCache>();
    const std::shared_ptr<IStoragesManager> Storages = TTestStoragesManager::GetInstance();
    const std::shared_ptr<NDataLocks::TManager> DataLocksManager = std::make_shared<NDataLocks::TManager>();
    const std::shared_ptr<TController> Controller = std::make_shared<TController>();
    TVersionedIndex VersionedIndex;
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Portions;
    std::optional<TSchemeActualizer> Actualizer;

public:
    TSchemeTestEnv() {
        VersionedIndex.AddIndex(TSnapshot(1, 1), Cache->UpsertIndexInfo(MakeTestIndexInfo(Cache, 1, false)));
        VersionedIndex.AddIndex(TSnapshot(2, 1), Cache->UpsertIndexInfo(MakeTestIndexInfo(Cache, 2, true)));
        Actualizer.emplace(TestPathId, VersionedIndex);
    }

    // Queues a portion written under schema v1 for rewrite to the critical schema v2.
    void QueuePortion() {
        Portions.emplace(TestPortionId, NTest::MakeTestCompactedPortion(TestPathId, TestPortionId, 10, 19, 10, TSnapshot(1, 1), std::nullopt));
        Actualizer->Refresh(TAddExternalContext(TInstant::Now(), Portions));
    }

    // Drops schema v1, so a portion written under it now resolves to v2 and no longer needs a rewrite.
    void CollapseSchemaOntoTarget() {
        VersionedIndex.EraseVersion(1);
    }

    THashMap<TRWAddress, std::vector<TTaskConstructor>> ExtractTasks() {
        TSaverContext saverContext(Storages);
        TTieringProcessContext context(
            MemoryLimit, saverContext, DataLocksManager, VersionedIndex, NColumnShard::TEngineLogsCounters(), Controller);
        TInternalTasksContext internalContext;
        Actualizer->ExtractTasks(context, TExternalTasksContext(Portions), internalContext);
        return context.GetTasks();
    }
};

class TDecimalRepairTestEnv {
private:
    const std::shared_ptr<TSchemaObjectsCache> Cache = std::make_shared<TSchemaObjectsCache>();
    const std::shared_ptr<IStoragesManager> Storages = TTestStoragesManager::GetInstance();
    const std::shared_ptr<NDataLocks::TManager> Locks = std::make_shared<NDataLocks::TManager>();
    const std::shared_ptr<TController> Controller = std::make_shared<TController>();
    TVersionedIndex VersionedIndex;
    TGranulesStorage Storage{ NColumnShard::TEngineLogsCounters(),
        std::make_shared<NDataAccessorControl::TActorAccessorsManager>(NActors::TActorId(1, 1)), Storages };
    std::shared_ptr<TGranuleMeta> Granule;
    std::shared_ptr<TPortionInfo> Portion;

public:
    explicit TDecimalRepairTestEnv(const std::optional<TSnapshot>& removed = {}) {
        VersionedIndex.AddIndex(TSnapshot(1, 1), Cache->UpsertIndexInfo(MakeTestIndexInfo(Cache, 1, false)));
        auto original = NTest::MakeTestCompactedPortion(TestPathId, TestPortionId, 10, 19, 10, TSnapshot(1, 1), removed);
        auto proto = original->GetMeta().SerializeToProto({ TUnifiedBlobId(1, 1, 1, 1, 1, 1, 100) }, NPortion::EProduced::SPLIT_COMPACTED);
        proto.SetRebuildDecimalPrimaryKey(true);
        auto ctor = original->BuildConstructor(true);
        TPortionMetaConstructor meta;
        UNIT_ASSERT(meta.LoadMetadata(proto, VersionedIndex.GetLastSchema()->GetIndexInfo(), TFakeGroupSelector()));
        ctor->MutableMeta() = std::move(meta);
        Portion = ctor->Build();
        Granule = Storage.RegisterTable(TestPathId, NColumnShard::TAgentGranuleDataCounters("repair_test").RegisterClient(), VersionedIndex);
        Granule->UpsertPortionOnLoad(Portion);
        Granule->OnAfterPortionsLoad();
    }

    auto LockPortion(const NDataLocks::ELockCategory category) {
        return Locks->RegisterLock<NDataLocks::TListPortionsLock>(
            "existing_operation", std::vector<std::shared_ptr<TPortionInfo>>{ Portion }, category);
    }

    void ReleaseLock(const std::shared_ptr<NDataLocks::TManager::TGuard>& guard) {
        guard->Release(*Locks);
    }

    void Start() {
        Granule->StartActualizationIndex();
    }

    auto ExtractTasks() {
        TTieringProcessContext context(
            MemoryLimit, TSaverContext(Storages), Locks, VersionedIndex, NColumnShard::TEngineLogsCounters(), Controller);
        Granule->BuildActualizationTasks(context, TDuration::Zero());
        return context.GetTasks();
    }

    const TPortionInfo& GetPortion() const {
        return *Portion;
    }

    std::shared_ptr<NDataLocks::ILock> BuildRepairLock() const {
        class TTestChanges: public TTTLColumnEngineChanges {
        public:
            using TColumnEngineChanges::BuildDataLock;
            using TTTLColumnEngineChanges::TTTLColumnEngineChanges;
        };

        const auto schema = VersionedIndex.GetLastSchema();
        TPortionEvictionFeatures features(schema, schema, IStoragesManager::DefaultStorageId);
        features.SetTargetTierName(IStoragesManager::DefaultStorageId);
        features.SetRebuildDecimalPrimaryKey(true);
        TTestChanges task(features.GetRWAddress(), TSaverContext(Storages));
        task.AddPortionToEvict(Portion, std::move(features));
        return task.BuildDataLock();
    }
};

class TActualizationBatchTestEnv {
public:
    const std::shared_ptr<TSchemaObjectsCache> Cache = std::make_shared<TSchemaObjectsCache>();
    const std::shared_ptr<IStoragesManager> Storages = TTestStoragesManager::GetInstance();
    const std::shared_ptr<NDataLocks::TManager> Locks = std::make_shared<NDataLocks::TManager>();
    const std::shared_ptr<TController> Controller = std::make_shared<TController>();
    TVersionedIndex Index;

    explicit TActualizationBatchTestEnv(
        const std::optional<TString>& sparseDefault = {}, const std::shared_ptr<NArrow::NAccessor::IConstructor>& payloadConstructor = nullptr) {
        Index.AddIndex(TSnapshot(1, 1), Cache->UpsertIndexInfo(MakeTestIndexInfo(Cache, 1, false, sparseDefault, payloadConstructor)));
        Index.AddIndex(TSnapshot(2, 1), Cache->UpsertIndexInfo(MakeTestIndexInfo(Cache, 2, true, sparseDefault, payloadConstructor)));
    }

    TPortionEvictionFeatures Features(const bool repair) const {
        const auto original = Index.GetSchemaVerified(1);
        TPortionEvictionFeatures result(original, repair ? original : Index.GetLastSchema(), IStoragesManager::DefaultStorageId);
        result.SetTargetTierName(IStoragesManager::DefaultStorageId);
        result.SetRebuildDecimalPrimaryKey(repair);
        return result;
    }

    TTieringProcessContext Context(const ui64 memoryLimit = MemoryLimit) const {
        return TTieringProcessContext(memoryLimit, TSaverContext(Storages), Locks, Index, NColumnShard::TEngineLogsCounters(), Controller);
    }

    static TPortionInfo::TConstPtr Portion(const ui64 id, const bool historical, const ui32 rows = 1000) {
        return NTest::MakeTestCompactedPortion(
            TestPathId, id, 10, 19, rows, TSnapshot(1, 1), historical ? std::make_optional(TSnapshot(10, 1)) : std::nullopt);
    }

    std::shared_ptr<TPortionDataAccessor> Accessor(
        const ui64 id, const ui32 rows, const std::vector<TColumnRecord>& records, const ui32 blobBytes = 100) const {
        auto portion = Portion(id, true, rows);
        const std::vector<TUnifiedBlobId> blobs = { TUnifiedBlobId(1, 1, 1, 1, 1, id, blobBytes) };
        auto proto = portion->GetMeta().SerializeToProto(blobs, NPortion::EProduced::SPLIT_COMPACTED);
        ui64 rawBytes = 0;
        for (const auto& chunk : records) {
            rawBytes += chunk.GetMeta().GetRawBytes();
        }
        proto.SetColumnRawBytes(rawBytes);
        proto.SetColumnBlobBytes(blobBytes);
        auto ctor = portion->BuildConstructor(true);
        TPortionMetaConstructor meta;
        UNIT_ASSERT(meta.LoadMetadata(proto, Index.GetSchemaVerified(1)->GetIndexInfo(), TFakeGroupSelector()));
        ctor->MutableMeta() = std::move(meta);
        return std::make_shared<TPortionDataAccessor>(ctor->Build(), blobs, records, std::vector<TIndexChunk>(), false);
    }

    std::shared_ptr<TPortionDataAccessor> EncodedAccessor(const ui64 id,
        const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& chunks, NBlobOperations::NRead::TCompositeReadBlobs& blobs) const {
        const auto loader = Index.GetSchemaVerified(1)->GetColumnLoaderVerified(2);
        std::vector<TColumnRecord> records;
        std::vector<TString> data;
        ui32 rows = 0;
        ui32 blobBytes = 0;
        for (const auto& chunk : chunks) {
            auto serialized =
                loader->GetAccessorConstructor()->SerializeToBlobAndMeta(chunk, loader->BuildAccessorContext(chunk->GetRecordsCount()));
            auto record = TColumnRecord::TTestInstanceBuilder::Build(2, records.size(), blobBytes, serialized.Blob.size(),
                chunk->GetRecordsCount(), chunk->GetRawSizeVerified());
            record.RegisterBlobIdx(0);
            record.MutableMeta().SetAdditionalAccessorData(std::move(serialized.Meta));
            records.emplace_back(std::move(record));
            rows += chunk->GetRecordsCount();
            blobBytes += serialized.Blob.size();
            data.emplace_back(std::move(serialized.Blob));
        }
        auto accessor = Accessor(id, rows, records, blobBytes);
        for (ui32 i = 0; i < records.size(); ++i) {
            blobs.Add(IStoragesManager::DefaultStorageId, accessor->RestoreBlobRange(records[i].GetBlobRange()), std::move(data[i]));
        }
        return accessor;
    }

    static auto BuildLock(const TTTLColumnEngineChanges& task) {
        struct TTestChanges: TTTLColumnEngineChanges {
            using TColumnEngineChanges::BuildDataLock;
        };

        return (task.*&TTestChanges::BuildDataLock)();
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TTieringProcessContextTests) {
    Y_UNIT_TEST(FirstTaskIsBuiltForFreeAddress) {
        TTestEnv env;
        auto context = env.MakeProcessContext();
        UNIT_ASSERT_EQUAL(context.AddPortion(env.MakePortion(TDuration::Zero()), env.MakeEvictionFeatures(Tier2), TDuration::Zero()),
            TTieringProcessContext::EAddPortionResult::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(context.GetTasks().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(context.GetTasks().begin()->first, env.MakeAddress(Tier2));
    }

    Y_UNIT_TEST(FirstTaskIsNotBuiltForAddressAtLimit) {
        TTestEnv env;
        // tier2 already holds the single task its address is allowed to have in progress
        env.GetController()->StartActualization(env.MakeAddress(Tier2));

        auto context = env.MakeProcessContext();
        UNIT_ASSERT_EQUAL(context.AddPortion(env.MakePortion(TDuration::Zero()), env.MakeEvictionFeatures(Tier2), TDuration::Zero()),
            TTieringProcessContext::EAddPortionResult::TASK_LIMIT_EXCEEDED);
        UNIT_ASSERT(context.GetTasks().empty());
    }
}

// A portion's target tier depends on how old its data is, so the address it is queued under and the address its task
// ends up under are computed at different instants and need not match. That is why the caller's per-queue check
// cannot stand in for the check above.
Y_UNIT_TEST_SUITE(TTieringActualizerTests) {
    Y_UNIT_TEST(PrefixChangesDoNotMoveExistingPortions) {
        using NColumnShard::NTiers::TExternalStorageId;
        const std::vector<std::optional<TString>> prefixes = { std::nullopt, TString(), TString("old/data"), TString("new/data") };
        for (auto&& oldPrefix : prefixes) {
            for (auto&& newPrefix : prefixes) {
                TTestEnv env;
                env.QueueTieredPortion(TExternalStorageId(Tier1, oldPrefix).ToString(), TDuration::Minutes(90));
                TTiering tiering;
                UNIT_ASSERT(tiering.Add(std::make_shared<TTierInfo>(TExternalStorageId(Tier1, newPrefix), Tier1EvictAfter, "pk", 1)));
                env.RefreshTiering(tiering);
                UNIT_ASSERT(env.ExtractTasks().empty());
            }
        }
    }

    Y_UNIT_TEST(NewEvictionsUseNewPrefix) {
        using NColumnShard::NTiers::TExternalStorageId;
        TTestEnv env;
        env.QueuePortion(TDuration::Zero(), TDuration::Minutes(90));
        const TString newTier = TExternalStorageId(Tier1, TString("new/data")).ToString();
        TTiering tiering;
        UNIT_ASSERT(tiering.Add(std::make_shared<TTierInfo>(TExternalStorageId::FromString(newTier), Tier1EvictAfter, "pk", 1)));
        env.RefreshTiering(tiering);
        const auto tasks = env.ExtractTasks();
        UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tasks.begin()->first, env.MakeAddress(newTier));
    }

    Y_UNIT_TEST(PrefixChangePreservesLaterTransitions) {
        using NColumnShard::NTiers::TExternalStorageId;
        const TString oldTier = TExternalStorageId(Tier1, TString("old/data")).ToString();
        for (const bool deleteTier : { false, true }) {
            TTestEnv env;
            env.QueueTieredPortion(oldTier, TDuration::Hours(3));
            TTiering tiering;
            UNIT_ASSERT(tiering.Add(std::make_shared<TTierInfo>(TExternalStorageId(Tier1, TString("new/data")), Tier1EvictAfter, "pk", 1)));
            UNIT_ASSERT(
                tiering.Add(deleteTier ? TTierInfo::MakeTtl(Tier2EvictAfter, "pk", 1)
                                       : std::make_shared<TTierInfo>(TExternalStorageId(Tier2, TString("cold")), Tier2EvictAfter, "pk", 1)));
            env.RefreshTiering(tiering);
            const auto tasks = env.ExtractTasks();
            UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
            const TString target = deleteTier ? TTierInfo::GetTtlTierName() : TExternalStorageId(Tier2, TString("cold")).ToString();
            UNIT_ASSERT_VALUES_EQUAL(tasks.begin()->first, TRWAddress({ oldTier }, { target }));
        }
    }

    Y_UNIT_TEST(DistinctPrefixesOfSameStorageRemainSeparateStages) {
        using NColumnShard::NTiers::TExternalStorageId;
        TTestEnv env;
        const TExternalStorageId warm(Tier1, TString("warm"));
        const TExternalStorageId cold(Tier1, TString("cold"));
        env.QueueTieredPortion(warm.ToString(), TDuration::Hours(3));
        TTiering tiering;
        UNIT_ASSERT(tiering.Add(std::make_shared<TTierInfo>(warm, Tier1EvictAfter, "pk", 1)));
        UNIT_ASSERT(tiering.Add(std::make_shared<TTierInfo>(cold, Tier2EvictAfter, "pk", 1)));
        env.RefreshTiering(tiering);
        const auto tasks = env.ExtractTasks();
        UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tasks.begin()->first, TRWAddress({ warm.ToString() }, { cold.ToString() }));
    }

    Y_UNIT_TEST(ResetTieringReturnsOldPrefixDataToDefaultStorage) {
        TTestEnv env;
        const TString oldTier = NColumnShard::NTiers::TExternalStorageId(Tier1, TString("old/data")).ToString();
        env.QueueTieredPortion(oldTier, TDuration::Hours(3));
        env.RefreshTiering(std::nullopt);
        const auto tasks = env.ExtractTasks();
        UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tasks.begin()->first, TRWAddress({ oldTier }, { IStoragesManager::DefaultStorageId }));
    }

    Y_UNIT_TEST(TargetTierIsRecomputedOnTaskBuild) {
        TTestEnv env;
        // at T-2h the data (T-2h30m) is 30 minutes old: due for no tier at all, tier1 being merely its next hop in
        // another 30 minutes, so tier1 is the only target the queueing could possibly settle on
        env.QueuePortion(TDuration::Hours(2), TDuration::Minutes(30));

        // at T the same data is 2h30m old, past tier2's 2h boundary. The task is addressed to tier2, so its address
        // was decided while it was being built and not while its portion was being queued.
        const auto tasks = env.ExtractTasks();
        UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tasks.begin()->first, env.MakeAddress(Tier2));
        UNIT_ASSERT_VALUES_EQUAL(tasks.begin()->second.size(), 1);
    }

    Y_UNIT_TEST(NoTasksOverLimitForRecomputedAddress) {
        TTestEnv env;
        // queued while headed for tier1, addressed to tier2 once built (see TargetTierIsRecomputedOnTaskBuild)
        env.QueuePortion(TDuration::Hours(2), TDuration::Minutes(30));

        // An eviction to tier2 started by an earlier round, before T. An address counts as busy from
        // StartActualization (TTTLColumnEngineChanges::DoStart) until FinishActualization (DoOnFinish), which runs
        // only when that task completes; nothing completes it here, so at T tier2 still holds its one allowed task.
        env.GetController()->StartActualization(env.MakeAddress(Tier2));

        // at T the queue key (tier1) is free, so the portion reaches the task building, but its task would belong to
        // the busy tier2 address and must not be built
        UNIT_ASSERT(env.ExtractTasks().empty());
    }
}

Y_UNIT_TEST_SUITE(TSchemeActualizerTests) {
    Y_UNIT_TEST(CollapsedSchemaPortionIsDropped) {
        TSchemeTestEnv env;
        env.QueuePortion();
        // once v1 collapses onto v2, the queued portion already carries the target schema: it must be dropped, not
        // turned into a task from an empty actualization info
        env.CollapseSchemaOntoTarget();
        UNIT_ASSERT(env.ExtractTasks().empty());
    }
}

Y_UNIT_TEST_SUITE(TDecimalRepairActualizerTests) {
    Y_UNIT_TEST(HistoricalRepairDoesNotHideActiveSchemaActualization) {
        for (const bool repairFirst : { false, true }) {
            TActualizationBatchTestEnv env;
            auto context = env.Context();
            for (ui32 i = 0; i < 4; ++i) {
                const bool repair = (i % 2 == 0) == repairFirst;
                UNIT_ASSERT_EQUAL(context.AddPortion(env.Portion(i + 1, repair), env.Features(repair), {}),
                    TTieringProcessContext::EAddPortionResult::SUCCESS);
            }
            UNIT_ASSERT_VALUES_EQUAL(context.GetTasks().size(), 1);   // same storage address
            const auto& tasks = context.GetTasks().begin()->second;
            UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 2);
            for (const auto& constructor : tasks) {
                const auto& task = *constructor.GetTask();
                UNIT_ASSERT_VALUES_EQUAL(constructor.GetPortionsCount(), 2);
                const auto lock = env.BuildLock(task);
                for (const auto& portion : task.GetPortionsInfo()) {
                    UNIT_ASSERT_VALUES_EQUAL(portion->IsRemovedFor(TSnapshot(20, 1)), task.IsKeyRebuild());
                    UNIT_ASSERT_VALUES_EQUAL(!!lock->IsLocked(*portion, NDataLocks::ELockCategory::Scan), task.IsKeyRebuild());
                    UNIT_ASSERT(lock->IsLocked(*portion, NDataLocks::ELockCategory::Actualization));
                }
            }
        }
    }

    Y_UNIT_TEST(SeparatedTasksShareStorageLimit) {
        TActualizationBatchTestEnv env;
        auto features = env.Features(true);
        for (ui32 i = 0; i < 15; ++i) {
            env.Controller->StartActualization(features.GetRWAddress());
        }
        auto context = env.Context();
        UNIT_ASSERT_EQUAL(context.AddPortion(env.Portion(1, true), env.Features(true), {}), TTieringProcessContext::EAddPortionResult::SUCCESS);
        UNIT_ASSERT_EQUAL(
            context.AddPortion(env.Portion(2, false), env.Features(false), {}), TTieringProcessContext::EAddPortionResult::TASK_LIMIT_EXCEEDED);
        UNIT_ASSERT_VALUES_EQUAL(context.GetTasks().begin()->second.size(), 1);
        UNIT_ASSERT_EQUAL(context.AddPortion(env.Portion(3, true), env.Features(true), {}), TTieringProcessContext::EAddPortionResult::SUCCESS);
    }

    Y_UNIT_TEST(RepairMemoryIncludesDenseDefaultsAndRetainedOutput) {
        const TString defaultValue(1024, 'x');
        TActualizationBatchTestEnv env(defaultValue);
        const auto portion = env.Portion(1, true, 4096);
        const auto features = env.Features(true);
        const auto loader = features.GetTargetScheme()->GetColumnLoaderVerified(2);
        const auto sparse = loader->BuildDefaultAccessor(portion->GetRecordsCount());
        UNIT_ASSERT(sparse->GetValueRawBytes() < defaultValue.size());
        const auto dense =
            NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(*loader->GetDefaultValue(), portion->GetRecordsCount()));
        const ui64 denseBytes = NArrow::GetArrayMemorySize(dense->data());
        auto predictor = TTTLColumnEngineChanges::BuildMemoryPredictor();
        const auto first = predictor->AddPortion(portion, features);
        UNIT_ASSERT(first > 3 * denseBytes);   // simultaneous restored, combined and reordered buffers
        const auto second = predictor->PredictMemory(portion, features);
        UNIT_ASSERT(second - first >= denseBytes);   // previous output stays alive
        UNIT_ASSERT_VALUES_EQUAL(predictor->AddPortion(portion, features), second);

        auto context = env.Context(second - 1);
        for (ui32 id = 1; id <= 2; ++id) {
            UNIT_ASSERT_EQUAL(
                context.AddPortion(env.Portion(id, true, 4096), env.Features(true), {}), TTieringProcessContext::EAddPortionResult::SUCCESS);
        }
        const auto& tasks = context.GetTasks().begin()->second;
        UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 2);   // split before exceeding the budget
        for (const auto& task : tasks) {
            UNIT_ASSERT_VALUES_EQUAL(task.GetMemoryUsage(), first);
            UNIT_ASSERT_VALUES_EQUAL(task.GetTask()->CalcMemoryForUsage(), first);
        }
    }

    Y_UNIT_TEST(OrdinaryActualizationKeepsMemoryEstimate) {
        TActualizationBatchTestEnv env;
        auto predictor = TTTLColumnEngineChanges::BuildMemoryPredictor();
        const auto portion = env.Portion(1, false);
        const auto features = env.Features(false);
        UNIT_ASSERT_VALUES_EQUAL(predictor->AddPortion(portion, features), portion->GetTotalBlobBytes() + portion->GetTotalRawBytes());
        UNIT_ASSERT_VALUES_EQUAL(predictor->AddPortion(portion, features), 2 * portion->GetTotalBlobBytes() + portion->GetTotalRawBytes());
    }

    Y_UNIT_TEST(RepairReservationIncludesDenseDefaultsAndExcludesInputBlobs) {
        TActualizationBatchTestEnv env(TString(1024, 'x'));
        auto features = env.Features(true);
        TTTLColumnEngineChanges task(features.GetRWAddress(), TSaverContext(env.Storages));
        std::vector<std::shared_ptr<TPortionDataAccessor>> accessors;
        ui64 inputBlobs = 0;
        for (ui64 id = 1; id <= 2; ++id) {
            auto accessor = env.Accessor(id, 4096, { TColumnRecord::TTestInstanceBuilder::Build(1, 0, 0, 100, 4096, 4096 * sizeof(ui64)) });
            inputBlobs += accessor->GetPortionInfo().GetTotalBlobBytes();
            task.AddPortionToEvict(accessor->GetPortionInfoPtr(), env.Features(true));
            accessors.emplace_back(std::move(accessor));
        }
        const TColumnEngineChanges& changes = task;
        const ui64 reserved = changes.CalcMemoryForConstruction(accessors);
        UNIT_ASSERT(reserved > 3 * 4096 * 1024);
        UNIT_ASSERT_VALUES_EQUAL(reserved + inputBlobs, task.CalcMemoryForUsage());
    }

    Y_UNIT_TEST(DictionaryRepairReservationAccountsForRepeatedLongValues) {
        TActualizationBatchTestEnv env({}, std::make_shared<NArrow::NAccessor::NDictionary::TConstructor>());
        constexpr ui32 ChunkRows = 100000;
        constexpr ui32 ValueBytes = 16 * 1024;
        const auto dictionary =
            NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::StringScalar(std::string(ValueBytes, 'x')), 1));
        const auto positions = NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt8Scalar(0), ChunkRows));
        const auto encoded = std::make_shared<NArrow::NAccessor::TDictionaryArray>(dictionary, positions);
        const ui64 expandedBytes = ui64(ChunkRows) * ValueBytes;
        UNIT_ASSERT(encoded->GetValueRawBytes() < expandedBytes / 1000);
        NBlobOperations::NRead::TCompositeReadBlobs blobs;
        const auto accessor = env.EncodedAccessor(1, { encoded, encoded }, blobs);
        auto features = env.Features(true);
        TTTLColumnEngineChanges task(features.GetRWAddress(), TSaverContext(env.Storages));
        task.AddPortionToEvict(accessor->GetPortionInfoPtr(), env.Features(true));
        const TColumnEngineChanges& changes = task;
        const ui64 reserved = changes.CalcMemoryForConstruction({ accessor }, &blobs);
        UNIT_ASSERT(reserved > 3 * 2 * expandedBytes);
        UNIT_ASSERT(reserved < task.CalcMemoryForUsage());

        const auto singleChunk = env.EncodedAccessor(2, { encoded }, blobs);
        task.AddPortionToEvict(singleChunk->GetPortionInfoPtr(), env.Features(true));
        const ui64 withRetainedOutput = changes.CalcMemoryForConstruction({ singleChunk, accessor }, &blobs);
        UNIT_ASSERT(withRetainedOutput >= reserved + expandedBytes);

        auto context = env.Context();
        for (const auto& portion : { accessor, singleChunk }) {
            UNIT_ASSERT_EQUAL(
                context.AddPortion(portion->GetPortionInfoPtr(), env.Features(true), {}), TTieringProcessContext::EAddPortionResult::SUCCESS);
        }
        UNIT_ASSERT_VALUES_EQUAL(context.GetTasks().begin()->second.size(), 2);
    }

    Y_UNIT_TEST(OrdinaryReservationKeepsMaximumPerColumn) {
        TActualizationBatchTestEnv env;
        auto features = env.Features(false);
        TTTLColumnEngineChanges task(features.GetRWAddress(), TSaverContext(env.Storages));
        std::vector<TColumnRecord> firstRecords;
        firstRecords.emplace_back(TColumnRecord::TTestInstanceBuilder::Build(1, 0, 0, 100, 500, 100));
        firstRecords.emplace_back(TColumnRecord::TTestInstanceBuilder::Build(1, 1, 100, 100, 500, 200));
        firstRecords.emplace_back(TColumnRecord::TTestInstanceBuilder::Build(2, 0, 200, 100, 1000, 600));
        const auto first = env.Accessor(1, 1000, firstRecords);
        std::vector<TColumnRecord> secondRecords;
        secondRecords.emplace_back(TColumnRecord::TTestInstanceBuilder::Build(1, 0, 0, 100, 1000, 400));
        secondRecords.emplace_back(TColumnRecord::TTestInstanceBuilder::Build(2, 0, 100, 100, 1000, 50));
        const auto second = env.Accessor(2, 1000, secondRecords);
        const TColumnEngineChanges& changes = task;
        UNIT_ASSERT_VALUES_EQUAL(changes.CalcMemoryForConstruction({ first, second }), 650);
        UNIT_ASSERT_VALUES_EQUAL(changes.GetMemoryForConstructionPreparation({ first, second }), 0);
    }

    Y_UNIT_TEST(DictionaryReservationDoesNotMultiplyPositionBytes) {
        using namespace NArrow::NAccessor;
        constexpr ui32 Rows = 100000;
        constexpr ui64 ReservationLimit = 64 * 1024 * 1024;
        for (const ui32 variants : { 1, 256, 65536 }) {
            TActualizationBatchTestEnv env({}, std::make_shared<NDictionary::TConstructor>());
            arrow::StringBuilder values;
            for (ui32 i = 0; i < variants; ++i) {
                const char value[] = { char(' ' + i % 64), char(' ' + (i / 64) % 64), char(' ' + (i / 4096) % 64) };
                UNIT_ASSERT(values.Append(value, sizeof(value)).ok());
            }
            arrow::UInt32Builder indexes;
            for (ui32 row = 0; row < Rows; ++row) {
                UNIT_ASSERT((row % 17 ? indexes.Append(row % variants) : indexes.AppendNull()).ok());
            }
            const auto positions = NArrow::TStatusValidator::GetValid(
                arrow::compute::Cast(*indexes.Finish().ValueOrDie(), NDictionary::TConstructor::GetTypeByVariantsCount(variants)));
            const auto encoded = std::make_shared<TDictionaryArray>(values.Finish().ValueOrDie(), positions);
            NBlobOperations::NRead::TCompositeReadBlobs blobs;
            const auto accessor = env.EncodedAccessor(1, { encoded }, blobs);
            auto features = env.Features(true);
            TTTLColumnEngineChanges task(features.GetRWAddress(), TSaverContext(env.Storages));
            task.AddPortionToEvict(accessor->GetPortionInfoPtr(), env.Features(true));
            const ui64 preparation = task.GetMemoryForConstructionPreparation({ accessor });
            const ui64 construction = task.CalcMemoryForConstruction({ accessor }, &blobs);
            const auto dense = encoded->GetChunkedArray()->chunk(0);
            UNIT_ASSERT(construction > 3 * NArrow::GetArrayMemorySize(dense->data()));
            UNIT_ASSERT(preparation > encoded->GetRawSizeVerified());
            UNIT_ASSERT_C(preparation + construction < ReservationLimit, variants << ": " << preparation + construction);
            UNIT_ASSERT(!blobs.IsEmpty());
        }
    }

    Y_UNIT_TEST(SubColumnsReservationIncludesExpandedDocuments) {
        using namespace NArrow::NAccessor;
        using namespace NArrow::NAccessor::NSubColumns;
        constexpr ui32 Rows = 2048;
        constexpr ui32 ValueBytes = 4096;
        TSettings settings;
        settings.SetDataExtractor(TDataAdapterContainer::GetDefault());
        settings.SetEnableNativeColumns(true);
        TActualizationBatchTestEnv env({}, std::make_shared<NSubColumns::TConstructor>(settings));
        const auto values = NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::StringScalar(std::string(ValueBytes, 'x')), 1));
        const auto positions = NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt8Scalar(0), Rows));
        const auto dictionary = std::make_shared<TDictionaryArray>(values, positions);
        auto statsBuilder = TDictStats::MakeBuilder();
        statsBuilder.Add(
            TString("\"nested\".\"payload\""), Rows, dictionary->GetValueRawBytes(), IChunkedArray::EType::Dictionary, EValueType::String);
        auto stats = statsBuilder.Finish();
        auto columns = std::make_shared<NArrow::TGeneralContainer>(Rows);
        columns->AddField(stats.GetField(0), dictionary).Validate();

        const auto otherValue = std::get<NBinaryJson::TBinaryJson>(NBinaryJson::SerializeToBinaryJson("{\"items\":[1,true,\"text\"]}"));
        auto othersStatsBuilder = TDictStats::MakeBuilder();
        othersStatsBuilder.Add(TString("other"), 1, otherValue.size(), IChunkedArray::EType::Array, EValueType::BinaryJson);
        auto othersBuilder = TOthersData::MakeMergedBuilder();
        othersBuilder->Add(0, 0, std::string_view(otherValue.data(), otherValue.size()));
        auto others = othersBuilder->Finish(TOthersData::TFinishContext(othersStatsBuilder.Finish()));
        const auto encoded =
            std::make_shared<TSubColumnsArray>(TColumnsData(std::move(stats), columns), std::move(others), arrow::binary(), Rows, settings);
        NBlobOperations::NRead::TCompositeReadBlobs blobs;
        const auto accessor = env.EncodedAccessor(1, { encoded }, blobs);
        auto features = env.Features(true);
        TTTLColumnEngineChanges task(features.GetRWAddress(), TSaverContext(env.Storages));
        task.AddPortionToEvict(accessor->GetPortionInfoPtr(), env.Features(true));
        const ui64 reserved = task.CalcMemoryForConstruction({ accessor }, &blobs);
        const auto dense = encoded->GetChunkedArray()->chunk(0);
        const ui64 denseBytes = NArrow::GetArrayMemorySize(dense->data());
        UNIT_ASSERT(denseBytes > ui64(Rows) * ValueBytes);
        UNIT_ASSERT(encoded->GetRawSizeVerified() < denseBytes / 100);
        UNIT_ASSERT(reserved > 3 * denseBytes);
        UNIT_ASSERT(reserved < 16 * denseBytes);
        UNIT_ASSERT(task.GetMemoryForConstructionPreparation({ accessor }) < denseBytes);
    }

    Y_UNIT_TEST(LateStartQueuesLoadedPortionsWithoutCriticalSchema) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.RunCall([] {
            TDecimalRepairTestEnv env;
            env.Start();
            const auto tasks = env.ExtractTasks();
            UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(tasks.begin()->second.front().GetPortionsCount(), 1);
            return true;
        });
    }

    Y_UNIT_TEST(LateStartQueuesHistoricalPortions) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.RunCall([] {
            TDecimalRepairTestEnv env(TSnapshot(10, 1));
            env.Start();
            const auto tasks = env.ExtractTasks();
            UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
            const auto lock = env.BuildRepairLock();
            UNIT_ASSERT(lock->IsLocked(env.GetPortion(), NDataLocks::ELockCategory::Cleanup));
            UNIT_ASSERT(lock->IsLocked(env.GetPortion(), NDataLocks::ELockCategory::Scan));
            UNIT_ASSERT(lock->IsLocked(env.GetPortion(), NDataLocks::ELockCategory::Compaction));
            return true;
        });
    }

    Y_UNIT_TEST(RepairWaitsForCleanupAndReaders) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.RunCall([] {
            for (const auto category : { NDataLocks::ELockCategory::Cleanup, NDataLocks::ELockCategory::Scan }) {
                TDecimalRepairTestEnv env(TSnapshot(10, 1));
                env.Start();
                const auto guard = env.LockPortion(category);
                const bool noTasksWhileLocked = env.ExtractTasks().empty();
                env.ReleaseLock(guard);
                UNIT_ASSERT(noTasksWhileLocked);
                UNIT_ASSERT_VALUES_EQUAL(env.ExtractTasks().size(), 1);
            }
            return true;
        });
    }
}

}   // namespace NKikimr::NOlap::NActualizer
