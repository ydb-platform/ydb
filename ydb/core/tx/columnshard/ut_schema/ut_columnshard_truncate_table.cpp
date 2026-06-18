#include <ydb/core/base/blobstorage.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup_portions.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/with_appended.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/columnshard/test_helper/test_combinator.h>

#include <ydb/library/actors/protos/unittests.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <arrow/api.h>
#include <arrow/ipc/reader.h>
#include <util/string/join.h>
#include <util/string/printf.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace Tests;
using namespace NTxUT;

using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;
using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

Y_UNIT_TEST_SUITE(TruncateTable) {
    Y_UNIT_TEST(EmptyTable) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // After truncation, reading should return no data
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }
    }

    Y_UNIT_TEST(WithData) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit data
        std::vector<ui64> writeIds;
        const bool ok =
            WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
        UNIT_ASSERT(ok);
        planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);

        // Verify data is readable before truncation
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }

        // Truncate the table
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // After truncation, reading should return no data
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }
    }

    Y_UNIT_TEST(TruncateAndInsert) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit initial data (100 rows)
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Truncate the table
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Write and commit new data (50 rows) after truncation
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // After truncation + insert, should see only the new 50 rows
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 50);
        }
    }

    Y_UNIT_TEST(TruncateAbsentTable) {
        // Truncation of an absent table is a no-op at the column shard level.
        // The proposal succeeds (PREPARED), and at plan time the truncation is silently skipped.
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        const ui64 absentPathId = 111;
        ui64 txId = 10;
        // Truncation of absent table succeeds at propose time but is a no-op at plan time
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(absentPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Original table should still be readable (empty, since no data was written)
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }
    }

    Y_UNIT_TEST(MultipleTruncates) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit data (100 rows)
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // First truncation
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Write new data (30 rows)
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 230 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Verify 30 rows
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 30);
        }

        // Second truncation
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // After second truncation, should be empty
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }

        // Write data again (20 rows)
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 300, 320 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Should see only the 20 new rows
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 20);
        }
    }

    Y_UNIT_TEST(TruncateAndDrop) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;

        // Write and commit data
        {
            std::vector<ui64> writeIds;
            const bool ok =
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds);
            UNIT_ASSERT(ok);
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        // Truncate the table
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Drop the table after truncation
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(pathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Reading from a dropped table should return no data
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }
    }

    Y_UNIT_TEST(TruncateSeqNoCheck) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;

        // Truncate with round=5
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 5), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        // Truncate with round=3 (lower) should fail
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 3), ++txId);

        // Truncate with round=6 (higher) should succeed
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 6), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
    }
}
}   // namespace NKikimr
