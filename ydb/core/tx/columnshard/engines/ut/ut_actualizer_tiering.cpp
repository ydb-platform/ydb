#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/tiering/tiering.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>

#include <library/cpp/testing/unittest/registar.h>

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

TIndexInfo MakeTestIndexInfo(const std::shared_ptr<TSchemaObjectsCache>& cache) {
    NKikimrSchemeOp::TColumnTableSchema proto;
    *proto.MutableColumns()->Add() = NArrow::NTest::TTestColumn("pk", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)).CreateColumn(PkColumnId);
    proto.AddKeyColumnNames("pk");
    proto.SetVersion(1);
    proto.MutableOptions()->MutableCompactionPlannerConstructor()->SetClassName("l-buckets");
    *proto.MutableOptions()->MutableCompactionPlannerConstructor()->MutableLBuckets() =
        NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLOptimizer();

    auto result = TIndexInfo::BuildFromProto(1, proto, TTestStoragesManager::GetInstance(), cache);
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
        VersionedIndex.AddIndex(TSnapshot(1, 1), Cache->UpsertIndexInfo(MakeTestIndexInfo(Cache)));
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

    THashMap<TRWAddress, std::vector<TTaskConstructor>> ExtractTasks() {
        auto context = MakeProcessContext();
        TInternalTasksContext internalContext;
        Actualizer->ExtractTasks(context, TExternalTasksContext(Portions), internalContext);
        return context.GetTasks();
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

}   // namespace NKikimr::NOlap::NActualizer
