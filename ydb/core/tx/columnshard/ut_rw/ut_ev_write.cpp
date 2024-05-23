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
#include <ydb/core/tx/columnshard/common/tests/shard_reader.h>
#include <ydb/library/actors/protos/unittests.pb.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>
#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/batch.h>
#include <util/string/join.h>


namespace NKikimr {

using namespace NColumnShard;
using namespace Tests;
using namespace NTxUT;
namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

Y_UNIT_TEST_SUITE(EvWrite) {

    Y_UNIT_TEST(WriteInTransaction) {
        using namespace NArrow;

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();

        const ui64 ownerId = 0;
        const ui64 tableId = 1;
        const ui64 schemaVersion = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
            NArrow::NTest::TTestColumn("key", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8))
                                                                };
        const std::vector<ui32> columnsIds = {1, 2};
        PrepareTablet(runtime, tableId, schema);
        const ui64 txId = 111;

        NConstruction::IArrayBuilder::TPtr keyColumn = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key");
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(8, 100));

        auto batch = NConstruction::TRecordBatchConstructor({ keyColumn, column }).BuildBatch(2048);
        TString blobData = NArrow::SerializeBatchNoCompression(batch);
        UNIT_ASSERT(blobData.size() < TLimits::GetMaxBlobSize());

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        evWrite->SetTxId(txId);
        ui64 payloadIndex = NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
        evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, {ownerId, tableId, schemaVersion}, columnsIds, payloadIndex, NKikimrDataEvents::FORMAT_ARROW);

        TActorId sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evWrite.release());

        {
            TAutoPtr<NActors::IEventHandle> handle;
            auto event = runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);
            UNIT_ASSERT(event);
            UNIT_ASSERT_VALUES_EQUAL(event->Record.GetOrigin(), TTestTxConfig::TxTablet0);
            UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), txId);
            UNIT_ASSERT_VALUES_EQUAL((ui64)event->Record.GetStatus(), (ui64)NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);

            auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(10, txId), schema);
            UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), 0);

            PlanWriteTx(runtime, sender, NOlap::TSnapshot(11, txId));
        }

        auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(11, txId), schema);
        UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), 2048);
    }

    Y_UNIT_TEST(AbortInTransaction) {
        using namespace NArrow;

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();


        const ui64 ownerId = 0;
        const ui64 tableId = 1;
        const ui64 schemaVersion = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
            NArrow::NTest::TTestColumn("key", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8))
        };
        const std::vector<ui32> columnsIds = {1, 2};
        PrepareTablet(runtime, tableId, schema);
        const ui64 txId = 111;

        NConstruction::IArrayBuilder::TPtr keyColumn = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key");
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(8, 100));

        auto batch = NConstruction::TRecordBatchConstructor({ keyColumn, column }).BuildBatch(2048);
        TString blobData = NArrow::SerializeBatchNoCompression(batch);
        UNIT_ASSERT(blobData.size() < TLimits::GetMaxBlobSize());

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        evWrite->SetTxId(txId);
        ui64 payloadIndex = NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
        evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, {ownerId, tableId, schemaVersion}, columnsIds, payloadIndex, NKikimrDataEvents::FORMAT_ARROW);

        TActorId sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evWrite.release());

        ui64 outdatedStep = 11;
        {
            TAutoPtr<NActors::IEventHandle> handle;
            auto event = runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);
            UNIT_ASSERT(event);
            UNIT_ASSERT_VALUES_EQUAL((ui64)event->Record.GetStatus(), (ui64)NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);

            outdatedStep = event->Record.GetMaxStep() + 1;
            PlanWriteTx(runtime, sender, NOlap::TSnapshot(outdatedStep, txId + 1), false);
        }

        auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(outdatedStep, txId), schema);
        UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), 0);
    }

    Y_UNIT_TEST(WriteWithSplit) {
        using namespace NArrow;

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();


        const ui64 ownerId = 0;
        const ui64 tableId = 1;
        const ui64 schemaVersion = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
            NArrow::NTest::TTestColumn("key", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8))
        };
        const std::vector<ui32> columnsIds = {1, 2};
        PrepareTablet(runtime, tableId, schema);
        const ui64 txId = 111;

        NConstruction::IArrayBuilder::TPtr keyColumn = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key");
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(8, TLimits::GetMaxBlobSize() / 1024));

        auto batch = NConstruction::TRecordBatchConstructor({ keyColumn, column }).BuildBatch(2048);
        TString blobData = NArrow::SerializeBatchNoCompression(batch);
        UNIT_ASSERT(blobData.size() > TLimits::GetMaxBlobSize());

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        evWrite->SetTxId(txId);
        ui64 payloadIndex = NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
        evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, {ownerId, tableId, schemaVersion}, columnsIds, payloadIndex, NKikimrDataEvents::FORMAT_ARROW);

        TActorId sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evWrite.release());

        {
            TAutoPtr<NActors::IEventHandle> handle;
            auto event = runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);
            UNIT_ASSERT(event);
            UNIT_ASSERT_VALUES_EQUAL((ui64)event->Record.GetStatus(), (ui64)NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
            PlanWriteTx(runtime, sender, NOlap::TSnapshot(11, txId));
        }

        auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(11, txId), schema);
        UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), 2048);
    }

    Y_UNIT_TEST(WriteWithLock) {
        using namespace NArrow;

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();


        const ui64 ownerId = 0;
        const ui64 tableId = 1;
        const ui64 schemaVersion = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = {
                                                                    NArrow::NTest::TTestColumn("key", TTypeInfo(NTypeIds::Uint64) ),
                                                                    NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8) )
                                                                };
        const std::vector<ui32> columnsIds = {1, 2};
        PrepareTablet(runtime, tableId, schema);
        const ui64 txId = 111;
        const ui64 lockId = 110;

        {
            NConstruction::IArrayBuilder::TPtr keyColumn = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key");
            NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>("field", NConstruction::TStringPoolFiller(8, 100));
            auto batch = NConstruction::TRecordBatchConstructor({ keyColumn, column }).BuildBatch(2048);
            TString blobData = NArrow::SerializeBatchNoCompression(batch);
            UNIT_ASSERT(blobData.size() < TLimits::GetMaxBlobSize());
            auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            evWrite->SetLockId(lockId, 1);

            ui64 payloadIndex = NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
            evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, {ownerId, tableId, schemaVersion}, columnsIds, payloadIndex, NKikimrDataEvents::FORMAT_ARROW);

            TActorId sender = runtime.AllocateEdgeActor();
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evWrite.release());

            {
                TAutoPtr<NActors::IEventHandle> handle;
                auto event = runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);
                UNIT_ASSERT(event);
                UNIT_ASSERT_VALUES_EQUAL(event->Record.GetOrigin(), TTestTxConfig::TxTablet0);
                UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), lockId);
                UNIT_ASSERT_VALUES_EQUAL((ui64)event->Record.GetStatus(), (ui64)NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);

                auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(10, lockId), schema);
                UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), 0);
            }
        }
        {
            NConstruction::IArrayBuilder::TPtr keyColumn = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key", 2049);
            NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>("field", NConstruction::TStringPoolFiller(8, 100));
            auto batch = NConstruction::TRecordBatchConstructor({ keyColumn, column }).BuildBatch(2048);
            TString blobData = NArrow::SerializeBatchNoCompression(batch);
            UNIT_ASSERT(blobData.size() < TLimits::GetMaxBlobSize());
            auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
            evWrite->SetLockId(lockId, 1);

            ui64 payloadIndex = NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
            evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, {ownerId, tableId, schemaVersion}, columnsIds, payloadIndex, NKikimrDataEvents::FORMAT_ARROW);

            TActorId sender = runtime.AllocateEdgeActor();
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evWrite.release());

            {
                TAutoPtr<NActors::IEventHandle> handle;
                auto event = runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);
                UNIT_ASSERT(event);
                UNIT_ASSERT_VALUES_EQUAL(event->Record.GetOrigin(), TTestTxConfig::TxTablet0);
                UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), lockId);
                UNIT_ASSERT_VALUES_EQUAL((ui64)event->Record.GetStatus(), (ui64)NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);

                auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(10, txId), schema);
                UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), 0);
            }
        }
        {
            auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_PREPARE);
            evWrite->SetTxId(txId);
            evWrite->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            auto* lock = evWrite->Record.MutableLocks()->AddLocks();
            lock->SetLockId(lockId);
            lock->SetDataShard(TTestTxConfig::TxTablet0);
            evWrite->Record.MutableLocks()->AddSendingShards(TTestTxConfig::TxTablet0);

            TActorId sender = runtime.AllocateEdgeActor();
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evWrite.release());

            {
                TAutoPtr<NActors::IEventHandle> handle;
                auto event = runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);
                UNIT_ASSERT(event);
                UNIT_ASSERT_VALUES_EQUAL(event->Record.GetOrigin(), TTestTxConfig::TxTablet0);
                UNIT_ASSERT_VALUES_EQUAL((ui64)event->Record.GetStatus(), (ui64)NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
                UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), txId);
            }

            PlanWriteTx(runtime, sender, NOlap::TSnapshot(11, txId));
        }

        auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(11, txId), schema);
        UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), 2 * 2048);
    }
}
}
