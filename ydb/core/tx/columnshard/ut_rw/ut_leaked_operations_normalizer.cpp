#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/normalizer/tablet/clean_orphaned_operations.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/shard_writer.h>

#include <inttypes.h>

namespace NKikimr::NColumnShard {

using namespace Tests;
using namespace NTxUT;

namespace {

constexpr ui64 SchemeShardPathId = 1;

class TOrphanedOperationFixture {
public:
    NYDBTest::TControllers::TGuard<NYDBTest::NColumnShard::TController> Controller =
        NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
    TTestBasicRuntime Runtime;
    TActorId Sender;
    NTxUT::TShardWriter Writer;

    TOrphanedOperationFixture()
        : Sender(Setup(Runtime))
        , Writer(Runtime, TTestTxConfig::TxTablet0, SchemeShardPathId, 222)
    {
        Controller->DisableBackground(NYDBTest::ICSController::EBackground::GC);
        Controller->DisableBackground(NYDBTest::ICSController::EBackground::Cleanup);
    }

    void WriteUncommittedPortion() {
        UNIT_ASSERT_VALUES_EQUAL(Writer.Write(MakeTestBatch<arrow::UInt64Type>({ "key" }, std::vector<ui64>{ 1, 2, 3 }), { 1 }, 111),
            NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    }

    void EraseTable() {
        const auto& tablesManager = Controller->GetTheOnlyShard()->GetTablesManager();
        const TInternalPathId pathId = *tablesManager.ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(SchemeShardPathId), false);
        const auto& table = tablesManager.GetTable(pathId, true);
        const ui64 rawPathId = pathId.GetRawValue();
        EraseRow("TableInfo", Sprintf("'('('PathId (Uint64 '%" PRIu64 ")))", rawPathId));
        for (const auto& unifiedPathId : table.GetPathIds()) {
            EraseRow("TableInfoV1", Sprintf("'('('PathId (Uint64 '%" PRIu64 ")) '('SchemeShardLocalPathId (Uint64 '%" PRIu64 ")))", rawPathId,
                                        unifiedPathId.SchemeShardLocalPathId.GetRawValue()));
        }
        for (const auto& version : table.GetVersions()) {
            EraseRow("TableVersionInfo",
                Sprintf("'('('PathId (Uint64 '%" PRIu64 ")) '('SinceStep (Uint64 '%" PRIu64 ")) '('SinceTxId (Uint64 '%" PRIu64 ")))", rawPathId,
                    version.GetPlanStep(), version.GetTxId()));
        }
    }

    void RebootOnUpgrade() {
        UpdateLocalDbTableRow(Runtime, TTestTxConfig::TxTablet0, "Value",
            Sprintf("'('('Id (Uint32 '%" PRIu32 ")))", static_cast<ui32>(Schema::EValueIds::LastNormalizerSequentialId)),
            Sprintf("'('('Digit (Uint64 '%" PRIu64 ")))", static_cast<ui64>(NOlap::ENormalizerSequentialId::RestoreAppearanceSnapshot)));
        Reboot();
    }

    ui64 CountOperationTxIds() {
        return CountLocalDbTableRows(
            Runtime, TTestTxConfig::TxTablet0, "OperationTxIds", "'('('TxId (Null) (Void)) '('LockId (Null) (Void)))", "'('TxId)");
    }

    ui64 CountTxInfo() {
        return CountLocalDbTableRows(Runtime, TTestTxConfig::TxTablet0, "TxInfo", "'('('TxId (Null) (Void)))", "'('TxId)");
    }

    void Reboot() {
        RebootTablet(Runtime, TTestTxConfig::TxTablet0, Sender);
    }

    ui64 CountOperations() {
        return CountLocalDbTableRows(Runtime, TTestTxConfig::TxTablet0, "Operations", "'('('WriteId (Null) (Void)))", "'('WriteId)");
    }

    ui64 CountPortionRows() {
        return CountLocalDbTableRows(
            Runtime, TTestTxConfig::TxTablet0, "IndexPortions", "'('('PathId (Null) (Void)) '('PortionId (Null) (Void)))", "'('PathId)");
    }

    ui64 CountColumnRows() {
        return CountLocalDbTableRows(
            Runtime, TTestTxConfig::TxTablet0, "IndexColumnsV2", "'('('PathId (Null) (Void)) '('PortionId (Null) (Void)))", "'('PathId)");
    }

    ui64 CountBlobsToDelete() {
        return CountLocalDbTableRows(Runtime, TTestTxConfig::TxTablet0, "BlobsToDelete", "'('('BlobId (Null) (Void)))", "'('BlobId)");
    }

    void AssertOrphansCleaned() {
        UNIT_ASSERT_VALUES_EQUAL(CountOperations(), 0);
        UNIT_ASSERT_VALUES_EQUAL(CountPortionRows(), 0);
        UNIT_ASSERT_VALUES_EQUAL(CountColumnRows(), 0);
        UNIT_ASSERT_VALUES_UNEQUAL(CountBlobsToDelete(), 0);
    }

private:
    static TActorId Setup(TTestBasicRuntime& runtime) {
        TTester::Setup(runtime);
        const std::vector<NArrow::NTest::TTestColumn> schema = { NArrow::NTest::TTestColumn("key", TTypeInfo(NTypeIds::Uint64)) };
        Y_UNUSED(PrepareTablet(runtime, SchemeShardPathId, schema));
        return runtime.AllocateEdgeActor();
    }

    void EraseRow(const TString& tableName, const TString& keySpec) {
        EraseLocalDbTableRow(Runtime, TTestTxConfig::TxTablet0, tableName, keySpec);
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(LeakedOperationsNormalizer) {
    Y_UNIT_TEST(CleanOrphanedOperationsNormalizer) {
        TOrphanedOperationFixture shard;
        shard.WriteUncommittedPortion();
        shard.EraseTable();
        UNIT_ASSERT_VALUES_EQUAL(shard.CountOperations(), 1);
        UNIT_ASSERT_VALUES_EQUAL(shard.CountPortionRows(), 1);

        auto* repair = shard.Runtime.GetAppData().ColumnShardConfig.MutableRepairs()->Add();
        repair->SetClassName(NOlap::TCleanOrphanedOperationsNormalizer::GetClassNameStatic());
        repair->SetDescription("orphaned operations");
        shard.Reboot();

        shard.AssertOrphansCleaned();
    }

    Y_UNIT_TEST(OrphanedOperationsCleanedOnUpgrade) {
        TOrphanedOperationFixture shard;
        shard.WriteUncommittedPortion();
        shard.EraseTable();
        UNIT_ASSERT_VALUES_EQUAL(shard.CountOperations(), 1);
        UNIT_ASSERT_VALUES_EQUAL(shard.CountPortionRows(), 1);

        shard.RebootOnUpgrade();

        shard.AssertOrphansCleaned();
    }

    Y_UNIT_TEST(UnproposedOperationsCleanedOnUpgrade) {
        TOrphanedOperationFixture shard;
        shard.WriteUncommittedPortion();
        UNIT_ASSERT_VALUES_EQUAL(shard.CountOperations(), 1);
        UNIT_ASSERT_VALUES_EQUAL(shard.CountPortionRows(), 1);

        shard.RebootOnUpgrade();

        shard.AssertOrphansCleaned();
    }

    Y_UNIT_TEST(ProposedOperationsPreservedOnUpgrade) {
        TOrphanedOperationFixture shard;
        shard.WriteUncommittedPortion();
        Y_UNUSED(shard.Writer.StartCommit(111));

        shard.RebootOnUpgrade();

        UNIT_ASSERT_VALUES_EQUAL(shard.CountOperations(), 1);
        UNIT_ASSERT_VALUES_EQUAL(shard.CountPortionRows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(shard.CountColumnRows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(shard.CountOperationTxIds(), 1);
        UNIT_ASSERT_VALUES_EQUAL(shard.CountTxInfo(), 1);
        UNIT_ASSERT_VALUES_EQUAL(shard.CountBlobsToDelete(), 0);
    }

    Y_UNIT_TEST(ProposedOrphanCompletesAfterCleanup) {
        TOrphanedOperationFixture shard;
        shard.WriteUncommittedPortion();
        const auto planStep = shard.Writer.StartCommit(111);
        shard.EraseTable();

        shard.RebootOnUpgrade();

        shard.AssertOrphansCleaned();
        UNIT_ASSERT_VALUES_EQUAL(shard.CountOperationTxIds(), 1);
        UNIT_ASSERT_VALUES_EQUAL(shard.CountTxInfo(), 1);
        PlanCommit(shard.Runtime, shard.Sender, planStep, 111);
        UNIT_ASSERT_VALUES_EQUAL(shard.CountOperationTxIds(), 0);
        UNIT_ASSERT_VALUES_EQUAL(shard.CountTxInfo(), 0);
    }
}

}   // namespace NKikimr::NColumnShard
