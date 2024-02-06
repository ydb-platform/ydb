#include "columnshard_ut_common.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/base/blobstorage.h>
#include <util/string/printf.h>
#include <arrow/api.h>
#include <arrow/ipc/reader.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/core/tx/data_events/backup_events.h>
#include <ydb/core/tx/columnshard/engines/changes/with_appended.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
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

namespace
{

namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

class TDisableCompactionController: public NKikimr::NYDBTest::NColumnShard::TController {
protected:
    virtual bool DoOnStartCompaction(std::shared_ptr<NOlap::TColumnEngineChanges>& changes) {
        changes = nullptr;
        return true;
    }
public:
};

[[maybe_unused]]
void TestWriteRead(bool reboots, const TestTableDescription& table = {}, TString codec = "") {
    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDisableCompactionController>();
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    // runtime.SetLogPriority(NKikimrServices::BLOB_CACHE, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    auto write = [&](TTestBasicRuntime& runtime, TActorId& sender, ui64 writeId, ui64 tableId,
                     const TString& data, const std::vector<std::pair<TString, TTypeInfo>>& ydbSchema, std::vector<ui64>& intWriteIds) {
        bool ok = WriteData(runtime, sender, writeId, tableId, data, ydbSchema, true, &intWriteIds);
        if (reboots) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        return ok;
    };

    auto proposeCommit = [&](TTestBasicRuntime& runtime, TActorId& sender, ui64 txId,
                             const std::vector<ui64>& writeIds) {
        ProposeCommit(runtime, sender, txId, writeIds);
        if (reboots) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
    };

    auto planCommit = [&](TTestBasicRuntime& runtime, TActorId& sender, ui64 planStep, ui64 txId) {
        PlanCommit(runtime, sender, planStep, txId);
        if (reboots) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
    };

    ui64 writeId = 0;
    ui64 tableId = 1;

    SetupSchema(runtime, sender, tableId, table, codec);

    const std::vector<std::pair<TString, TTypeInfo>>& ydbSchema = table.Schema;

    [[maybe_unused]]
    const std::vector<std::pair<TString, TTypeInfo>>& testYdbPk = table.Pk;

    // ----xx
    // -----xx..
    // xx----
    // -xxxxx
    std::vector<std::pair<ui64, ui64>> portion = {
        {200, 300},
    };

    // write 1: ins:1, cmt:0, idx:0

    std::vector<ui64> intWriteIds;
    UNIT_ASSERT(write(runtime, sender, writeId, tableId, MakeTestBlob(portion[0], ydbSchema), ydbSchema, intWriteIds));

    // read
    TAutoPtr<IEventHandle> handle;

    // commit 1: ins:0, cmt:1, idx:0

    ui64 planStep = 21;
    ui64 txId = 100;
    proposeCommit(runtime, sender, txId, intWriteIds);
    planCommit(runtime, sender, planStep, txId);

    {
        Cerr << "\n============================ Start: Step ==============================" << Endl;
        NActors::TLogContextGuard guard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("TEST_STEP", 3);
        NOlap::NTests::TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(planStep, txId));

        reader.SetReplyColumns(TTestSchema::ExtractNames(ydbSchema));
        auto rb = reader.ReadAll();
        UNIT_ASSERT(rb);

        NArrow::ExtractColumnsValidate(rb, TTestSchema::ExtractNames(ydbSchema));

        UNIT_ASSERT((ui32)rb->num_columns() == TTestSchema::ExtractNames(ydbSchema).size());
        UNIT_ASSERT(rb->num_rows());
        UNIT_ASSERT(reader.IsCorrectlyFinished());
        // UNIT_ASSERT(CheckOrdered(rb));
        // UNIT_ASSERT(DataHas({rb}, portion[0]));
         Cerr << "result: " << rb->ToString() << Endl;

        Cerr << "============================ Finish: Step ==============================\n" << Endl;
    }
    return;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TColumnShardBackup) {

    Y_UNIT_TEST(ActorScan) {
        using namespace NArrow;

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        const ui64 ownerId = 0;
        const ui64 tableId = 1;
        const ui64 schemaVersion = 1;
        const std::vector<std::pair<TString, TTypeInfo>> schema = {
                                                                    {"key", TTypeInfo(NTypeIds::Uint64) },
                                                                    {"field", TTypeInfo(NTypeIds::Utf8) }
                                                                };
        const std::vector<ui32> columnsIds = {1, 2};
        PrepareTablet(runtime, tableId, schema);
        const ui64 txId = 111;

        auto keyColumn = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key");
        auto column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(16, 16));

        auto batch = NConstruction::TRecordBatchConstructor({ keyColumn, column }).BuildBatch(2048);
        TString blobData = NArrow::SerializeBatchNoCompression(batch);
        UNIT_ASSERT(blobData.size() < TLimits::GetMaxBlobSize());

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(txId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        const ui64 payloadIndex = NEvWrite::TPayloadHelper<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
        evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, {ownerId, tableId, schemaVersion}, columnsIds, payloadIndex, NKikimrDataEvents::FORMAT_ARROW);

        TActorId sender = runtime.AllocateEdgeActor();

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evWrite.release());

        constexpr int writePlanStep = 11;

        {
            TAutoPtr<NActors::IEventHandle> handle;
            auto event = runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);

            UNIT_ASSERT(event);
            UNIT_ASSERT_VALUES_EQUAL(event->Record.GetOrigin(), TTestTxConfig::TxTablet0);
            UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), txId);

            PlanWriteTx(runtime, sender, NOlap::TSnapshot(writePlanStep, txId));
        }
        

        auto evBackup = std::make_unique<NKikimr::NEvents::TBackupEvents::TEvBackupShardPropose>();

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evBackup.release());

        {
            TAutoPtr<NActors::IEventHandle> handle;
            auto event = runtime.GrabEdgeEvent<NKikimr::NEvents::TBackupEvents::TEvBackupShardProposeResult>(handle);

            UNIT_ASSERT(event);
        }

        // Cerr << "\n =================================== start ReadAllAsBatch ===================================" << Endl;
        // auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(writePlanStep, txId), schema);
        // Cerr << " =================================== finish ReadAllAsBatch ===================================\n" << Endl;

        // UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), 2048);
        // Cerr << readResult->ToString() << Endl;
    }

}

} // namespace NKikimr