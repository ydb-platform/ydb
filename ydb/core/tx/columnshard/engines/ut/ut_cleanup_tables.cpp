#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/columnshard/counters/portion_index.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
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

}   // namespace

Y_UNIT_TEST_SUITE(TColumnEngineCleanupTables) {
    Y_UNIT_TEST(EmptyTableIsNotDroppedUntilEverythingIsLoaded) {
        TTestBasicRuntime runtime;
        runtime.Initialize(TAppPrepare().Unwrap());
        const auto [droppedWhileLoading, droppedAfterLoading] = runtime.RunCall([] {
            const auto versionInfo = MakeVersionInfo();
            TColumnEngineForLogs engine(0, std::make_shared<TSchemaObjectsCache>(), std::make_shared<TNoDataAccessorsManager>(),
                TTestStoragesManager::GetInstance(), TSnapshot(1, 1), 1, IColumnEngine::TSchemaVersionView(versionInfo),
                std::make_shared<NColumnShard::TPortionIndexStats>());
            const auto dataLocksManager = std::make_shared<NDataLocks::TManager>();
            const auto pathId = TInternalPathId::FromRawValue(1);
            engine.RegisterTableToLoad(pathId);
            const bool droppedWhileLoading = !!engine.StartCleanupTables({ pathId }, dataLocksManager);
            Y_ABORT_UNLESS(engine.FinishLoading());
            return std::make_pair(droppedWhileLoading, !!engine.StartCleanupTables({ pathId }, dataLocksManager));
        });
        UNIT_ASSERT(!droppedWhileLoading);
        UNIT_ASSERT(droppedAfterLoading);
    }
}

}   // namespace NKikimr::NOlap::NTest
