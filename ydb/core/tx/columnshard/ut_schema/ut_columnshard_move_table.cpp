#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/base/blobstorage.h>
#include <util/string/printf.h>
#include <arrow/api.h>
#include <arrow/ipc/reader.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/changes/with_appended.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup_portions.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/columnshard/test_helper/test_combinator.h>
#include <ydb/library/actors/protos/unittests.pb.h>
#include <util/string/join.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace Tests;
using namespace NTxUT;

using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;
using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;


Y_UNIT_TEST_SUITE(MoveTable) {
    Y_UNIT_TEST(EmptyTable) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTabe{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTabe.Schema);

        ui64 txId = 10;
        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::MoveTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, {planStep, txId});
    }

    Y_UNIT_TEST(WithUncomittedData) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTabe{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTabe.Schema);

        ui64 txId = 10;
        int writeId = 10;
        std::vector<ui64> writeIds;
        const bool ok = WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({0, 100}, testTabe.Schema), testTabe.Schema, true, &writeIds);
        UNIT_ASSERT(ok);
        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::MoveTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, {planStep, txId});
    }

    Y_UNIT_TEST_DUO(WithData, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTabe{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTabe.Schema);

        ui64 txId = 10;
        int writeId = 10;
        std::vector<ui64> writeIds;
        const bool ok =
            WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTabe.Schema), testTabe.Schema, true, &writeIds);
        UNIT_ASSERT(ok);
        planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);

        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::MoveTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        UNIT_ASSERT_EQUAL(1, NOlap::TSchemaCachesManager::GetCachedOwnersCount());

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTabe.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 100);
        }

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTabe.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
        }

    }

    Y_UNIT_TEST(RenameAbsentTable_Negative) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTabe{};
        const auto& planStep = PrepareTablet(runtime, srcPathId, testTabe.Schema);
        Y_UNUSED(planStep);

        const ui64 absentPathId = 111;
        const ui64 dstPathId = 2;
        ui64 txId = 10;
        ProposeSchemaTxFail(runtime, sender, TTestSchema::MoveTableTxBody(absentPathId, dstPathId, 1), ++txId);
    }

    Y_UNIT_TEST(RenameToItself_Negative) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTabe{};
        const auto& planStep = PrepareTablet(runtime, srcPathId, testTabe.Schema);
        Y_UNUSED(planStep);
        ui64 txId = 10;
        ProposeSchemaTxFail(runtime, sender, TTestSchema::MoveTableTxBody(srcPathId, srcPathId, 1), ++txId);
    }
}
}// namespace NKikimr