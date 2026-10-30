#include <ydb/core/tx/columnshard/counters/portion_index.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/schema_diff.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NTest {

namespace {

const ui64 PresetId = 1;

class TNoDataAccessorsManager: public NDataAccessorControl::IDataAccessorsManager {
private:
    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& /*request*/) override {
        AFL_VERIFY(false);
    }

    virtual void DoAddPortion(const std::shared_ptr<TPortionDataAccessor>& /*accessor*/) override {
        AFL_VERIFY(false);
    }

    virtual void DoRemovePortion(const TPortionInfo::TConstPtr& /*portion*/) override {
        AFL_VERIFY(false);
    }

public:
    TNoDataAccessorsManager()
        : IDataAccessorsManager(NActors::TActorId())
    {
    }
};

NKikimrSchemeOp::TColumnTableSchema MakeSchemaV1() {
    NKikimrSchemeOp::TColumnTableSchema proto;
    *proto.AddColumns() =
        NArrow::NTest::TTestColumn("timestamp", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)).SetNullable(false).CreateColumn(1);
    *proto.AddColumns() = NArrow::NTest::TTestColumn("value", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)).CreateColumn(2);
    proto.AddKeyColumnNames("timestamp");
    proto.SetVersion(1);
    return proto;
}

// only adds a nullable column, so v1 may be ignored in favour of v2
NKikimrSchemeOp::TColumnTableSchema MakeSchemaV2() {
    NKikimrSchemeOp::TColumnTableSchema proto = MakeSchemaV1();
    *proto.AddColumns() = NArrow::NTest::TTestColumn("added", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)).CreateColumn(3);
    proto.SetVersion(2);
    return proto;
}

NKikimrTxColumnShard::TSchemaPresetVersionInfo MakeVersionInfoV1() {
    NKikimrTxColumnShard::TSchemaPresetVersionInfo info;
    info.SetId(PresetId);
    *info.MutableSchema() = MakeSchemaV1();
    return info;
}

class TShard {
private:
    const NKikimrTxColumnShard::TSchemaPresetVersionInfo V1 = MakeVersionInfoV1();
    const NKikimrSchemeOp::TColumnTableSchemaDiff V2 = TSchemaDiffView::MakeSchemasDiff(MakeSchemaV1(), MakeSchemaV2());
    TColumnEngineForLogs Engine;

public:
    TShard(const std::shared_ptr<TSchemaObjectsCache>& cache, const TSnapshot& v1Snapshot, const TSnapshot& v2Snapshot)
        : Engine(0, cache, std::make_shared<TNoDataAccessorsManager>(), TTestStoragesManager::GetInstance(), v1Snapshot, PresetId,
              IColumnEngine::TSchemaVersionView(V1), std::make_shared<NColumnShard::TPortionIndexStats>())
    {
        Engine.RegisterSchemaVersion(v2Snapshot, PresetId, IColumnEngine::TSchemaVersionView(V2));
    }

    TColumnEngineForLogs* operator->() {
        return &Engine;
    }
};

const TIndexInfo* GetIndexInfo(TShard& shard, const ui64 version) {
    return &shard->GetVersionedIndex().GetSchemaVerified(version)->GetIndexInfo();
}

}   // namespace

Y_UNIT_TEST_SUITE(TColumnEngineSharedSchemaCache) {
    Y_UNIT_TEST(SecondEngineReusesCachedVersions) {
        auto cache = std::make_shared<TSchemaObjectsCache>();
        TShard first(cache, TSnapshot(10, 1), TSnapshot(20, 1));
        TShard second(cache, TSnapshot(10, 1), TSnapshot(20, 1));

        UNIT_ASSERT_EQUAL(GetIndexInfo(second, 1), GetIndexInfo(first, 1));
        UNIT_ASSERT_EQUAL(GetIndexInfo(second, 2), GetIndexInfo(first, 2));
    }

    Y_UNIT_TEST(ReusedVersionKeepsOwnSnapshot) {
        auto cache = std::make_shared<TSchemaObjectsCache>();
        TShard first(cache, TSnapshot(10, 1), TSnapshot(20, 1));
        TShard second(cache, TSnapshot(15, 1), TSnapshot(25, 1));

        UNIT_ASSERT_VALUES_EQUAL(second->GetVersionedIndex().GetSchemaVerified(TSnapshot(20, 1))->GetVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(second->GetVersionedIndex().GetSchemaVerified(TSnapshot(25, 1))->GetVersion(), 2);
    }

    Y_UNIT_TEST(ReusedDiffVersionMarksPreviousIgnorable) {
        auto cache = std::make_shared<TSchemaObjectsCache>();
        TShard first(cache, TSnapshot(10, 1), TSnapshot(20, 1));
        TShard second(cache, TSnapshot(10, 1), TSnapshot(20, 1));

        UNIT_ASSERT_EQUAL(second->MutableVersionedIndex().ExtractIgnoreSchemaVersionFor(1), std::optional<ui64>(2));
    }
}

}   // namespace NKikimr::NOlap::NTest
