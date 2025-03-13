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
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/library/actors/protos/unittests.pb.h>
//#include <ydb/core/formats/arrow/simple_builder/filler.h>
//#include <ydb/core/formats/arrow/simple_builder/array.h>
//#include <ydb/core/formats/arrow/simple_builder/batch.h>
#include <util/string/join.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace Tests;
using namespace NTxUT;

namespace
{

namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;
using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

} //namespace

Y_UNIT_TEST_SUITE(MoveTable) {
    const TestTableDescription table;
    Y_UNIT_TEST(EmptyTable) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTabe{};
        PrepareTablet(runtime, srcPathId, testTabe.Schema);

        ui64 txId = 10;
        ui64 planStep = 10;
        const ui64 dstPathId = 2;
        TMessageSeqNo seqNo;
        UNIT_ASSERT(ProposeSchemaTx(runtime, sender, TTestSchema::MoveTableTxBody(srcPathId, dstPathId, ++seqNo), ++txId));
        PlanSchemaTx(runtime, sender, {++planStep, txId});
    }

    Y_UNIT_TEST(WithUncomittedData) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTabe{};
        PrepareTablet(runtime, srcPathId, testTabe.Schema);

        ui64 txId = 10;
        ui64 planStep = 10;
        int writeId = 10;
        std::vector<ui64> writeIds;
        bool ok = WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({0, 100}, testTabe.Schema), testTabe.Schema, true, &writeIds);
        UNIT_ASSERT(ok);
        const ui64 dstPathId = 2;
        TMessageSeqNo seqNo;
        UNIT_ASSERT(ProposeSchemaTx(runtime, sender, TTestSchema::MoveTableTxBody(srcPathId, dstPathId, ++seqNo), ++txId));
        PlanSchemaTx(runtime, sender, {++planStep, txId});
    }

    Y_UNIT_TEST(WithData) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTabe{};
        PrepareTablet(runtime, srcPathId, testTabe.Schema);

        ui64 txId = 10;
        ui64 planStep = 10;
        int writeId = 10;
        std::vector<ui64> writeIdsToCommit;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({0, 100}, testTabe.Schema), testTabe.Schema, true, &writeIdsToCommit));
        //Write overlapped data (subject for compaction)
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({50, 150}, testTabe.Schema), testTabe.Schema, true, &writeIdsToCommit));


        std::vector<ui64> writeIdsToBeRejected;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({100, 150}, testTabe.Schema), testTabe.Schema, true, &writeIdsToBeRejected));
        const auto writeDataTxId = ++txId; //11
        ProposeCommit(runtime, sender, writeDataTxId, writeIdsToCommit, true);

        const ui64 dstPathId = 2;
        TMessageSeqNo seqNo;
        const auto moveTableTxId = ++txId;
        auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
            NKikimrTxColumnShard::TX_KIND_SCHEMA, sender, moveTableTxId, TTestSchema::MoveTableTxBody(srcPathId, dstPathId, ++seqNo));
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
        
        ProposeCommit(runtime, sender, ++txId, writeIdsToBeRejected, 0, false);

        PlanCommit(runtime, sender, ++planStep, writeDataTxId);

        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_EQUAL(res.GetTxId(), moveTableTxId);
        UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_SCHEMA);
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);

        PlanSchemaTx(runtime, sender, {++planStep, moveTableTxId});

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_EQUAL(rb->num_rows(), 150);
        }

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            UNIT_ASSERT(!reader.ReadAll());
        }

    }

    Y_UNIT_TEST(RenameAbsentTable_Negative) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTabe{};
        PrepareTablet(runtime, srcPathId, testTabe.Schema);

        const ui64 absentPathId = 111;
        const ui64 dstPathId = 2;
        ui64 txId = 10;
        TMessageSeqNo seqNo;
        UNIT_ASSERT(!ProposeSchemaTx(runtime, sender, TTestSchema::MoveTableTxBody(absentPathId, dstPathId, ++seqNo), ++txId));
    }

    Y_UNIT_TEST(RenameToItself_Negative) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTabe{};
        PrepareTablet(runtime, srcPathId, testTabe.Schema);
        ui64 txId = 10;
        TMessageSeqNo seqNo;
        UNIT_ASSERT(!ProposeSchemaTx(runtime, sender, TTestSchema::MoveTableTxBody(srcPathId, srcPathId, ++seqNo), ++txId));
    }
}
}// namespace NKikimr