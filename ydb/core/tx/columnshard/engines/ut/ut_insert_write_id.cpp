#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/columnshard/counters/portion_index.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NTest {

namespace {

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

class TCountersOnlyDb: public IDbWrapper {
private:
    THashMap<ui32, ui64> Counters;

public:
    void WriteCounter(ui32 counterId, ui64 value) override {
        Counters[counterId] = value;
    }

    bool LoadCounters(const std::function<void(ui32 id, ui64 value)>& callback) override {
        for (const auto& [id, value] : Counters) {
            callback(id, value);
        }
        return true;
    }

    const IBlobGroupSelector* GetDsGroupSelector() const override {
        AFL_VERIFY(false);
        return nullptr;
    }

    void WriteColumns(
        const TPortionInfo&, const NKikimrTxColumnShard::TIndexPortionAccessor&, const NKikimrTxColumnShard::TIndexPortionBlobsInfo&) override {
        AFL_VERIFY(false);
    }

    void WriteColumn(const TPortionDataAccessor&, const TPortionInfo&, const TColumnRecord&, const ui32) override {
        AFL_VERIFY(false);
    }

    void EraseColumn(const TPortionInfo&, const TColumnRecord&) override {
        AFL_VERIFY(false);
    }

    bool LoadColumns(const std::function<void(TColumnChunkLoadContextV2&&)>&, const std::optional<TInternalPathId>, const std::optional<ui64>,
        const std::optional<TInternalPathId>, const std::optional<ui64>) override {
        AFL_VERIFY(false);
        return false;
    }

    void WritePortion(const std::vector<TUnifiedBlobId>&, const TPortionInfo&) override {
        AFL_VERIFY(false);
    }

    void CommitPortion(const TPortionInfo&, const TSnapshot&) override {
        AFL_VERIFY(false);
    }

    void ErasePortion(const TPortionInfo&) override {
        AFL_VERIFY(false);
    }

    bool LoadPortions(const std::function<bool(std::unique_ptr<TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&)>&,
        const std::optional<TInternalPathId>, const std::optional<ui64>) override {
        AFL_VERIFY(false);
        return false;
    }

    void WriteIndex(const TPortionDataAccessor&, const TPortionInfo&, const TIndexChunk&) override {
        AFL_VERIFY(false);
    }

    void EraseIndex(const TPortionInfo&, const TIndexChunk&) override {
        AFL_VERIFY(false);
    }

    bool LoadIndexes(const std::function<void(const TInternalPathId, const ui64, TIndexChunkLoadContext&&)>&,
        const std::optional<TInternalPathId>, const std::optional<ui64>, const std::optional<TInternalPathId>,
        const std::optional<ui64>) override {
        AFL_VERIFY(false);
        return false;
    }

    TConclusion<THashMap<TInternalPathId, std::map<TSnapshot, TGranuleShardingInfo>>> LoadGranulesShardingInfo() override {
        AFL_VERIFY(false);
        return TConclusionStatus::Fail("not supported");
    }
};

NKikimrTxColumnShard::TSchemaPresetVersionInfo MakeVersionInfo() {
    NKikimrTxColumnShard::TSchemaPresetVersionInfo info;
    info.SetId(1);
    auto& schema = *info.MutableSchema();
    *schema.AddColumns() =
        NArrow::NTest::TTestColumn("timestamp", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)).SetNullable(false).CreateColumn(1);
    schema.AddKeyColumnNames("timestamp");
    schema.SetVersion(1);
    return info;
}

TInsertWriteId StartEngineAndBuildInsertWriteId(TCountersOnlyDb& db) {
    const auto versionInfo = MakeVersionInfo();
    TColumnEngineForLogs engine(0, std::make_shared<TSchemaObjectsCache>(), std::make_shared<TNoDataAccessorsManager>(),
        TTestStoragesManager::GetInstance(), TSnapshot(1, 1), 1, IColumnEngine::TSchemaVersionView(versionInfo),
        std::make_shared<NColumnShard::TPortionIndexStats>());
    Y_ABORT_UNLESS(engine.LoadCounters(db));
    Y_ABORT_UNLESS(engine.FinishLoading());
    return engine.BuildNextInsertWriteId(db);
}

}   // namespace

Y_UNIT_TEST_SUITE(TColumnEngineInsertWriteId) {
    Y_UNIT_TEST(NotReusedAfterRebootWithoutWrittenPortions) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto [beforeReboot, afterReboot] = runtime.RunCall([] {
            TCountersOnlyDb db;
            const TInsertWriteId beforeReboot = StartEngineAndBuildInsertWriteId(db);
            return std::make_pair(beforeReboot, StartEngineAndBuildInsertWriteId(db));
        });
        UNIT_ASSERT_VALUES_EQUAL((ui64)afterReboot, (ui64)beforeReboot + 1);
    }
}

}   // namespace NKikimr::NOlap::NTest
