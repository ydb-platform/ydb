#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/shard_writer.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/library/formats/arrow/simple_builder/array.h>
#include <ydb/library/formats/arrow/simple_builder/batch.h>
#include <ydb/library/formats/arrow/simple_builder/filler.h>
#include <ydb/library/testlib/helpers.h>

#include <arrow/api.h>
#include <util/string/printf.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace Tests;
using namespace NTxUT;

namespace {

namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

// Build a TEvRead request for point lookup by primary key.
std::unique_ptr<TEvDataShard::TEvRead> MakeReadRequest(
    ui64 readId, ui64 ownerId, ui64 tableId, const std::vector<ui32>& columnIds, const std::vector<TSerializedCellVec>& keys)
{
    auto request = std::make_unique<TEvDataShard::TEvRead>();
    auto& record = request->Record;
    record.SetReadId(readId);
    record.MutableTableId()->SetOwnerId(ownerId);
    record.MutableTableId()->SetTableId(tableId);
    for (ui32 colId : columnIds) {
        record.AddColumns(colId);
    }
    record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
    request->Keys.insert(request->Keys.end(), keys.begin(), keys.end());
    return request;
}

// Send a TEvRead and wait for the TEvReadResult.
std::unique_ptr<TEvDataShard::TEvReadResult> SendRead(TTestBasicRuntime& runtime, ui64 tabletId, TActorId sender, TEvDataShard::TEvRead* request)
{
    ForwardToTablet(runtime, tabletId, sender, request);
    auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(sender);
    return std::unique_ptr<TEvDataShard::TEvReadResult>(ev->Release().Release());
}

// Common test setup: creates and boots a column shard, returns runtime and sender.
struct TTestSetup {
    TTestBasicRuntime Runtime;
    TActorId Sender;
    ui64 TabletId = TTestTxConfig::TxTablet0;
    NKikimr::NYDBTest::TControllers::TGuard<TDefaultTestsController> CsControllerGuard;

    TTestSetup()
        : CsControllerGuard(NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>())
    {
        CsControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        CsControllerGuard->SetOverrideMaxReadStaleness(TDuration::Max());
        CsControllerGuard->SetOverrideBlobSplitSettings(NOlap::NSplitter::TSplitSettings());

        TTester::Setup(Runtime);

        Sender = Runtime.AllocateEdgeActor();
        CreateTestBootstrapper(Runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime.DispatchEvents(options);
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardEvRead) {
    Y_UNIT_TEST(PointLookupSingleKey) {
        TTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& sender = setup.Sender;

        ui64 tableId = 1;
        TestTableDescription table;
        auto planStep = SetupSchema(runtime, sender, tableId, table);
        const auto& ydbSchema = table.Schema;

        // Write data for keys [0, 100).
        ui64 writeId = 0;
        std::vector<ui64> intWriteIds;
        UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, MakeTestBlob({ 0, 100 }, ydbSchema), ydbSchema, true, &intWriteIds));

        // Commit.
        ui64 txId = 100;
        planStep = ProposeCommit(runtime, sender, txId, intWriteIds);
        PlanCommit(runtime, sender, planStep, txId);

        // Point lookup for key 50.
        {
            auto readSender = runtime.AllocateEdgeActor();
            TVector<TCell> keyCells = { TCell::Make(ui64(50)) };
            std::vector<TSerializedCellVec> keys = { TSerializedCellVec(TSerializedCellVec::Serialize(keyCells)) };

            auto req = MakeReadRequest(1, 1, tableId, TTestSchema::ExtractIds(ydbSchema), keys);
            auto res = SendRead(runtime, TTestTxConfig::TxTablet0, readSender, req.release());

            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRowCount(), 1u);
        }

        // Point lookup for key 0.
        {
            auto readSender = runtime.AllocateEdgeActor();
            TVector<TCell> keyCells = { TCell::Make(ui64(0)) };
            std::vector<TSerializedCellVec> keys = { TSerializedCellVec(TSerializedCellVec::Serialize(keyCells)) };

            auto req = MakeReadRequest(2, 1, tableId, TTestSchema::ExtractIds(ydbSchema), keys);
            auto res = SendRead(runtime, TTestTxConfig::TxTablet0, readSender, req.release());

            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRowCount(), 1u);
        }
    }

    Y_UNIT_TEST(PointLookupMultipleKeys) {
        TTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& sender = setup.Sender;

        ui64 tableId = 1;
        TestTableDescription table;
        auto planStep = SetupSchema(runtime, sender, tableId, table);
        const auto& ydbSchema = table.Schema;

        // Write data for keys [0, 100).
        ui64 writeId = 0;
        std::vector<ui64> intWriteIds;
        UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, MakeTestBlob({ 0, 100 }, ydbSchema), ydbSchema, true, &intWriteIds));

        // Commit.
        ui64 txId = 100;
        planStep = ProposeCommit(runtime, sender, txId, intWriteIds);
        PlanCommit(runtime, sender, planStep, txId);

        // Point lookup for keys 10, 20, 30.
        {
            auto readSender = runtime.AllocateEdgeActor();
            std::vector<TSerializedCellVec> keys;
            for (ui64 k : { 10, 20, 30 }) {
                TVector<TCell> keyCells = { TCell::Make(k) };
                keys.push_back(TSerializedCellVec(TSerializedCellVec::Serialize(keyCells)));
            }

            auto req = MakeReadRequest(1, 1, tableId, TTestSchema::ExtractIds(ydbSchema), keys);
            auto res = SendRead(runtime, TTestTxConfig::TxTablet0, readSender, req.release());

            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRowCount(), 3u);
        }
    }

    Y_UNIT_TEST(PointLookupMissingKey) {
        TTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& sender = setup.Sender;

        ui64 tableId = 1;
        TestTableDescription table;
        auto planStep = SetupSchema(runtime, sender, tableId, table);
        const auto& ydbSchema = table.Schema;

        // Write data for keys [0, 100).
        ui64 writeId = 0;
        std::vector<ui64> intWriteIds;
        UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, MakeTestBlob({ 0, 100 }, ydbSchema), ydbSchema, true, &intWriteIds));

        // Commit.
        ui64 txId = 100;
        planStep = ProposeCommit(runtime, sender, txId, intWriteIds);
        PlanCommit(runtime, sender, planStep, txId);

        // Point lookup for key 999 (does not exist).
        {
            auto readSender = runtime.AllocateEdgeActor();
            TVector<TCell> keyCells = { TCell::Make(ui64(999)) };
            std::vector<TSerializedCellVec> keys = { TSerializedCellVec(TSerializedCellVec::Serialize(keyCells)) };

            auto req = MakeReadRequest(1, 1, tableId, TTestSchema::ExtractIds(ydbSchema), keys);
            auto res = SendRead(runtime, TTestTxConfig::TxTablet0, readSender, req.release());

            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRowCount(), 0u);
        }
    }

    Y_UNIT_TEST(PointLookupSelectedColumns) {
        TTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& sender = setup.Sender;

        ui64 tableId = 1;
        TestTableDescription table;
        auto planStep = SetupSchema(runtime, sender, tableId, table);
        const auto& ydbSchema = table.Schema;

        // Write data for keys [0, 100).
        ui64 writeId = 0;
        std::vector<ui64> intWriteIds;
        UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, MakeTestBlob({ 0, 100 }, ydbSchema), ydbSchema, true, &intWriteIds));

        // Commit.
        ui64 txId = 100;
        planStep = ProposeCommit(runtime, sender, txId, intWriteIds);
        PlanCommit(runtime, sender, planStep, txId);

        // Point lookup requesting only the first column (timestamp, id=1).
        {
            auto readSender = runtime.AllocateEdgeActor();
            TVector<TCell> keyCells = { TCell::Make(ui64(50)) };
            std::vector<TSerializedCellVec> keys = { TSerializedCellVec(TSerializedCellVec::Serialize(keyCells)) };

            auto req = MakeReadRequest(1, 1, tableId, { 1 }, keys);
            auto res = SendRead(runtime, TTestTxConfig::TxTablet0, readSender, req.release());

            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRowCount(), 1u);
        }
    }

    Y_UNIT_TEST(PointLookupWithDataVerification) {
        TTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& sender = setup.Sender;

        ui64 tableId = 1;
        TestTableDescription table;
        auto planStep = SetupSchema(runtime, sender, tableId, table);
        const auto& ydbSchema = table.Schema;

        // Write data for keys [0, 10).
        ui64 writeId = 0;
        std::vector<ui64> intWriteIds;
        UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, MakeTestBlob({ 0, 10 }, ydbSchema), ydbSchema, true, &intWriteIds));

        // Commit.
        ui64 txId = 100;
        planStep = ProposeCommit(runtime, sender, txId, intWriteIds);
        PlanCommit(runtime, sender, planStep, txId);

        // Point lookup for key 5 and verify the returned row has cells.
        {
            auto readSender = runtime.AllocateEdgeActor();
            TVector<TCell> keyCells = { TCell::Make(ui64(5)) };
            std::vector<TSerializedCellVec> keys = { TSerializedCellVec(TSerializedCellVec::Serialize(keyCells)) };

            auto req = MakeReadRequest(1, 1, tableId, TTestSchema::ExtractIds(ydbSchema), keys);
            auto res = SendRead(runtime, TTestTxConfig::TxTablet0, readSender, req.release());

            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRowCount(), 1u);

            // Verify that the returned row has the expected number of cells.
            const auto& cells = res->GetCells(0);
            UNIT_ASSERT_VALUES_EQUAL(cells.size(), TTestSchema::ExtractIds(ydbSchema).size());
        }
    }

    Y_UNIT_TEST(NoKeysRejected) {
        TTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& sender = setup.Sender;

        ui64 tableId = 1;
        TestTableDescription table;
        Y_UNUSED(SetupSchema(runtime, sender, tableId, table));

        // TEvRead without keys should be rejected.
        {
            auto readSender = runtime.AllocateEdgeActor();
            auto req = MakeReadRequest(1, 1, tableId, { 1 }, {});
            auto res = SendRead(runtime, TTestTxConfig::TxTablet0, readSender, req.release());

            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
        }
    }

    Y_UNIT_TEST(NoTableIdRejected) {
        TTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& sender = setup.Sender;

        ui64 tableId = 1;
        TestTableDescription table;
        Y_UNUSED(SetupSchema(runtime, sender, tableId, table));

        // TEvRead without TableId should be rejected.
        {
            auto readSender = runtime.AllocateEdgeActor();
            auto req = std::make_unique<TEvDataShard::TEvRead>();
            auto& record = req->Record;
            record.SetReadId(1);
            // Don't set TableId
            record.AddColumns(1);
            record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);

            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, readSender, req.release());
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(readSender);
            auto* res = ev->Get();

            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
        }
    }

    Y_UNIT_TEST(RangeReadsRejected) {
        TTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& sender = setup.Sender;

        ui64 tableId = 1;
        TestTableDescription table;
        Y_UNUSED(SetupSchema(runtime, sender, tableId, table));

        // TEvRead with ranges (not point keys) should be rejected.
        {
            auto readSender = runtime.AllocateEdgeActor();
            auto req = std::make_unique<TEvDataShard::TEvRead>();
            auto& record = req->Record;
            record.SetReadId(1);
            record.MutableTableId()->SetOwnerId(1);
            record.MutableTableId()->SetTableId(tableId);
            record.AddColumns(1);
            record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            // Add a range using the struct member (not proto).
            TVector<TCell> fromCells = { TCell::Make(ui64(0)) };
            TVector<TCell> toCells = { TCell::Make(ui64(100)) };
            req->Ranges.push_back(TSerializedTableRange(fromCells, true, toCells, true));

            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, readSender, req.release());
            auto ev = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(readSender);
            auto* res = ev->Get();

            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::UNSUPPORTED);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
        }
    }

    Y_UNIT_TEST(UnknownTableRejected) {
        TTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& sender = setup.Sender;

        ui64 tableId = 1;
        TestTableDescription table;
        Y_UNUSED(SetupSchema(runtime, sender, tableId, table));

        // Request a table ID that was never set up (99999).
        {
            auto readSender = runtime.AllocateEdgeActor();
            TVector<TCell> keyCells = { TCell::Make(ui64(0)) };
            std::vector<TSerializedCellVec> keys = { TSerializedCellVec(TSerializedCellVec::Serialize(keyCells)) };

            auto req = MakeReadRequest(1, 1, 99999, { 1 }, keys);
            auto res = SendRead(runtime, TTestTxConfig::TxTablet0, readSender, req.release());

            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::NOT_FOUND);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
        }
    }

    Y_UNIT_TEST(UnknownColumnIdRejected) {
        TTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& sender = setup.Sender;

        ui64 tableId = 1;
        TestTableDescription table;
        Y_UNUSED(SetupSchema(runtime, sender, tableId, table));

        // Request a column ID that doesn't exist (9999).
        {
            auto readSender = runtime.AllocateEdgeActor();
            TVector<TCell> keyCells = { TCell::Make(ui64(0)) };
            std::vector<TSerializedCellVec> keys = { TSerializedCellVec(TSerializedCellVec::Serialize(keyCells)) };

            auto req = MakeReadRequest(1, 1, tableId, { 9999 }, keys);
            auto res = SendRead(runtime, TTestTxConfig::TxTablet0, readSender, req.release());

            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::SCHEME_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
        }
    }

    Y_UNIT_TEST(EmptyColumnsRequest) {
        TTestSetup setup;
        auto& runtime = setup.Runtime;
        auto& sender = setup.Sender;

        ui64 tableId = 1;
        TestTableDescription table;
        auto planStep = SetupSchema(runtime, sender, tableId, table);
        const auto& ydbSchema = table.Schema;

        // Write data for keys [0, 10).
        ui64 writeId = 0;
        std::vector<ui64> intWriteIds;
        UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, MakeTestBlob({ 0, 10 }, ydbSchema), ydbSchema, true, &intWriteIds));

        // Commit.
        ui64 txId = 100;
        planStep = ProposeCommit(runtime, sender, txId, intWriteIds);
        PlanCommit(runtime, sender, planStep, txId);

        // Request with no columns (e.g. Count(*) pattern) should still succeed,
        // falling back to reading the first PK column internally.
        {
            auto readSender = runtime.AllocateEdgeActor();
            TVector<TCell> keyCells = { TCell::Make(ui64(5)) };
            std::vector<TSerializedCellVec> keys = { TSerializedCellVec(TSerializedCellVec::Serialize(keyCells)) };

            auto req = MakeReadRequest(1, 1, tableId, {}, keys);
            auto res = SendRead(runtime, TTestTxConfig::TxTablet0, readSender, req.release());

            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetFinished(), true);
            UNIT_ASSERT_VALUES_EQUAL(res->Record.GetRowCount(), 1u);
        }
    }

}   // Y_UNIT_TEST_SUITE(TColumnShardEvRead)

}   // namespace NKikimr
