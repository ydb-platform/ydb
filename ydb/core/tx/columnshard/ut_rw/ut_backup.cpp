#include "columnshard_ut_common.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/batch.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/blobs_action/memory.h>
#include <ydb/core/tx/columnshard/blobs_action/storages_manager/manager.h>
#include <ydb/core/tx/columnshard/common/tests/shard_reader.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/with_appended.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/data_events/backup_events.h>

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

namespace {

namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

constexpr ui64 txId = 111;
constexpr int writePlanStep = 11;
constexpr ui64 tableId = 1;

const std::vector<std::pair<TString, TTypeInfo>> schema = {{"key", TTypeInfo(NTypeIds::Uint64)},
                                                               {"field", TTypeInfo(NTypeIds::Utf8)}};

class TDisableCompactionController : public NKikimr::NYDBTest::NColumnShard::TController {
protected:
    virtual bool DoOnStartCompaction(std::shared_ptr<NOlap::TColumnEngineChanges>& changes) {
        changes = nullptr;
        return true;
    }

public:
};

TActorIdentity PrepareCSTable(TTestBasicRuntime& runtime, TActorId& sender) {
    using namespace NArrow;

    const ui64 ownerId = 0;
    const ui64 schemaVersion = 1;
    
    const std::vector<ui32> columnsIds = {1, 2};
    auto res = PrepareTabletActor(runtime, tableId, schema);

    auto keyColumn =
        std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>(
            "key");
    auto column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
        "field", NConstruction::TStringPoolFiller(16, 16));

    auto batch = NConstruction::TRecordBatchConstructor({keyColumn, column}).BuildBatch(2048);
    TString blobData = NArrow::SerializeBatchNoCompression(batch);
    UNIT_ASSERT(blobData.size() < TLimits::GetMaxBlobSize());

    auto evWrite =
        std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(txId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
    const ui64 payloadIndex = NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
    evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, {ownerId, tableId, schemaVersion},
                          columnsIds, payloadIndex, NKikimrDataEvents::FORMAT_ARROW);

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evWrite.release());

    TAutoPtr<NActors::IEventHandle> handle;
    auto event = runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);

    UNIT_ASSERT(event);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetOrigin(), TTestTxConfig::TxTablet0);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), txId);

    PlanWriteTx(runtime, sender, NOlap::TSnapshot(writePlanStep, txId));

    return res->SelfId();
}

}   // anonymous namespace

// class TMemoryStorageManager: public NOlap::IStoragesManager {
// private:
//     TIntrusivePtr<TTabletStorageInfo> TabletInfo = new TTabletStorageInfo();
//     using TBase = NOlap::IStoragesManager;
// protected:
//     bool DoLoadIdempotency(NTable::TDatabase& /*database*/) override {
//         return true;
//     }
//     std::shared_ptr<NOlap::IBlobsStorageOperator> DoBuildOperator(const TString& /*storageId*/) override {
//         return std::make_shared<NOlap::TMemoryOperator>(storageId);
//     }
// public:
//     TMemoryStorageManager() = default;
// };


Y_UNIT_TEST_SUITE(TColumnShardBackup) {
    Y_UNIT_TEST(ActorScan) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        TActorId sender = runtime.AllocateEdgeActor();

        const auto csActorId = PrepareCSTable(runtime, sender);

        Cerr << "\n ======================================================================== \n" << Endl;

        // @todo almost is stub

        const auto storageId = "some storageId";
        const TActorIdentity tabletActorID(sender);
        auto sharedBlobsManager = std::make_shared<NOlap::NDataSharing::TSharedBlobsManager>(NOlap::TTabletId{});

        NKikimrSchemeOp::TStorageTierConfig cfgProto;
        cfgProto.SetName("some_name");
        ::NKikimrSchemeOp::TS3Settings s3_settings;
        s3_settings.set_endpoint("fake");
        *cfgProto.MutableObjectStorage() = s3_settings;

        // tierManager->GetS3Settings(); -> create fake externl op
        NColumnShard::NTiers::TTierConfig cfg("tier_name", cfgProto);

        // unqiue?
        // tierManager->GetS3Settings() with fake ep.
        // S3Settings from Config.GetPatchedConfig(secrets) in ctor TManager
        auto* tierManager = new NColumnShard::NTiers::TManager(tableId, tabletActorID, cfg);

        auto op = std::make_shared<NOlap::NBlobOperations::NTier::TOperator>(
            storageId,
            tableId,
            tabletActorID,
            tierManager, 
            sharedBlobsManager->GetStorageManagerGuarantee(storageId)
        );

        runtime.Register(NColumnShard::CreatBackupActor(op, sender, csActorId, txId, writePlanStep, tableId));

        TAutoPtr<NActors::IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<NKikimr::NEvents::TBackupEvents::TEvBackupShardProposeResult>(handle);

        UNIT_ASSERT(event);

        Cerr << "\n ======================================================================== \n" << Endl;

        // {
        //     NActors::TLogContextGuard guard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("TEST_STEP", 3);
        //     NOlap::NTests::TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId, NOlap::TSnapshot(writePlanStep, txId));

        //     for(const auto& name : TTestSchema::ExtractNames(schema)) {
        //         Cerr << "column: " << name << Endl;
        //     }
        //     reader.SetReplyColumns(TTestSchema::ExtractNames(schema));
            
        //     auto rb = reader.ReadAll();
        //     UNIT_ASSERT(rb);
        //     NArrow::ExtractColumnsValidate(rb, TTestSchema::ExtractNames(schema));
        //     UNIT_ASSERT((ui32)rb->num_columns() == TTestSchema::ExtractNames(schema).size());
        //     UNIT_ASSERT(rb->num_rows());
        //     UNIT_ASSERT(reader.IsCorrectlyFinished());

        //     Cerr << "reader read_all: " << rb->ToString() << Endl;
        // }
    }
}

}   // namespace NKikimr