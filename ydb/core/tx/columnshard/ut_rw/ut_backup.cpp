#include "columnshard_ut_common.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/batch.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/tx/columnshard/blobs_action/storages_manager/manager.h>
#include <ydb/core/tx/columnshard/blobs_action/tier/storage.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/common/tests/shard_reader.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/with_appended.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/columnshard_ut_common.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/wrappers/fake_storage.h>

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

using TTestColumn = NArrow::NTest::TTestColumn;
using TFakeExternalStorage = NWrappers::NExternalStorage::TFakeExternalStorage;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

namespace NTypeIds = NScheme::NTypeIds;

constexpr ui64 txId = 111;
constexpr int writePlanStep = 11;
constexpr ui64 tableId = 1;

const std::vector<TTestColumn> schema = {TTestColumn{"key", TTypeInfo{NTypeIds::Uint64}},
                                         TTestColumn{"field", TTypeInfo{NTypeIds::Utf8}}};

namespace {

std::shared_ptr<arrow::RecordBatch> BuildBatch() {
    auto keyColumn = std::make_shared<
        NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key");
    auto column =
        std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TStringPoolFiller>>(
            "field", NArrow::NConstruction::TStringPoolFiller(8, 100));

    return NArrow::NConstruction::TRecordBatchConstructor({keyColumn, column}).BuildBatch(2048);
}

}   // namespace

TActorIdentity PrepareCSTable(TTestBasicRuntime& runtime, TActorId& sender) {
    using namespace NArrow;

    const ui64 ownerId = 0;
    const ui64 schemaVersion = 1;

    const std::vector<ui32> columnsIds = {1, 2};
    auto res = PrepareTablet(runtime, tableId, schema);
    UNIT_ASSERT(res);

    auto evWrite = PrepareEvWrite(BuildBatch(), txId, tableId, ownerId, schemaVersion, columnsIds);

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evWrite.release());

    TAutoPtr<NActors::IEventHandle> handle;
    auto event = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(handle);

    UNIT_ASSERT(event);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetOrigin(), TTestTxConfig::TxTablet0);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), txId);

    PlanWriteTx(runtime, sender, NOlap::TSnapshot(writePlanStep, txId));

    return res->SelfId();
}

Y_UNIT_TEST_SUITE(TColumnShardBackup) {
    Y_UNIT_TEST(ActorScan) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        TActorId sender = runtime.AllocateEdgeActor();

        const auto csActorId = PrepareCSTable(runtime, sender);
        auto op = PrepareInsertOp(sender, tableId);

        runtime.Register(CreatBackupActor(op, sender, csActorId, tableId, NOlap::TSnapshot{writePlanStep, txId},
                                          TTestSchema::ExtractNames(schema)));

        TAutoPtr<NActors::IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<NColumnShard::TEvPrivate::TEvBackupShardResult>(handle);

        UNIT_ASSERT(event);

        std::string expected;
        {
            NActors::TLogContextGuard guard =
                NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("TEST_STEP", 3);
            NOlap::NTests::TShardReader reader(runtime, TTestTxConfig::TxTablet0, tableId,
                                               NOlap::TSnapshot(writePlanStep, txId));

            reader.SetReplyColumns(TTestSchema::ExtractNames(schema));

            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            NArrow::ExtractColumnsValidate(rb, TTestSchema::ExtractNames(schema));
            UNIT_ASSERT((ui32)rb->num_columns() == TTestSchema::ExtractNames(schema).size());
            UNIT_ASSERT(rb->num_rows());
            UNIT_ASSERT(reader.IsCorrectlyFinished());

            expected = NArrow::SerializeBatchNoCompression(rb);
        }

        std::string actual;
        {   // get data from fake s3
            const auto bucketIds = Singleton<TFakeExternalStorage>()->GetBucketIds();
            UNIT_ASSERT(1 == bucketIds.size());

            auto& fakeBucketStorage = Singleton<TFakeExternalStorage>()->GetBucket(bucketIds.front());
            for (const auto& data : fakeBucketStorage) {
                actual = data.second;
                break;
            }
        }

        UNIT_ASSERT_EQUAL(actual, expected);
    }
}

}   // namespace NKikimr