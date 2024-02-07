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

constexpr ui64 txId = 111;
constexpr int writePlanStep = 11;
constexpr ui64 tableId = 1;

class TDisableCompactionController: public NKikimr::NYDBTest::NColumnShard::TController {
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
    const std::vector<std::pair<TString, TTypeInfo>> schema = {
                                                                    {"key", TTypeInfo(NTypeIds::Uint64) },
                                                                    {"field", TTypeInfo(NTypeIds::Utf8) }
                                                                };
    const std::vector<ui32> columnsIds = {1, 2};
    auto res = PrepareTabletActor(runtime, tableId, schema);
    

    auto keyColumn = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key");
    auto column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
        "field", NConstruction::TStringPoolFiller(16, 16));

    auto batch = NConstruction::TRecordBatchConstructor({ keyColumn, column }).BuildBatch(2048);
    TString blobData = NArrow::SerializeBatchNoCompression(batch);
    UNIT_ASSERT(blobData.size() < TLimits::GetMaxBlobSize());

    auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(txId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
    const ui64 payloadIndex = NEvWrite::TPayloadHelper<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
    evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, {ownerId, tableId, schemaVersion}, columnsIds, payloadIndex, NKikimrDataEvents::FORMAT_ARROW);

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evWrite.release());

    TAutoPtr<NActors::IEventHandle> handle;
    auto event = runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);

    UNIT_ASSERT(event);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetOrigin(), TTestTxConfig::TxTablet0);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), txId);

    PlanWriteTx(runtime, sender, NOlap::TSnapshot(writePlanStep, txId));

    return res->SelfId();
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TColumnShardBackup) {

    Y_UNIT_TEST(ActorScan) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        TActorId sender = runtime.AllocateEdgeActor();

        const auto csActorId = PrepareCSTable(runtime, sender);

        Cerr << "\n ======================================================================== \n" << Endl;
        runtime.Register(NColumnShard::CreatBackupActor(sender, csActorId, txId, writePlanStep, tableId));

        TAutoPtr<NActors::IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<NKikimr::NEvents::TBackupEvents::TEvBackupShardProposeResult>(handle);

        UNIT_ASSERT(event);
    }

}

} // namespace NKikimr