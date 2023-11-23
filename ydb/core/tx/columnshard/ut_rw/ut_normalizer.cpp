#include "columnshard_ut_common.h"

#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <ydb/core/tx/columnshard/operations/write_data.h>

#include <ydb/core/formats/arrow/simple_builder/filler.h>
#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/batch.h>


namespace NKikimr {

using namespace NColumnShard;
using namespace Tests;
using namespace NTxUT;

struct TPortionRecord {
    ui64 Index = 0;
    ui64 Granule = 0;
    ui64 ColumnIdx = 0;
    ui64 PlanStep = 0;
    ui64 TxId = 0;
    ui64 Portion = 0;
    ui32 Chunk = 0;
    ui64 XPlanStep = 0;
    ui64 XTxId = 0;
    TString Blob;
    TString Metadata;
    ui32 Offset = 0;
    ui32 Size = 0;
};

class TPathIdCleaner : public NYDBTest::ILocalDBModifier {
public:
    virtual void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        THashMap<ui64, TPortionRecord> portion2Key;
        std::optional<ui64> pathId;
        {
            auto rowset = db.Table<Schema::IndexColumns>().Select();
            UNIT_ASSERT(rowset.IsReady());

            while (!rowset.EndOfSet()) {
                TPortionRecord key;
                key.Index = rowset.GetValue<Schema::IndexColumns::Index>();
                key.Granule = rowset.GetValue<Schema::IndexColumns::Granule>();
                key.ColumnIdx = rowset.GetValue<Schema::IndexColumns::ColumnIdx>();
                key.PlanStep = rowset.GetValue<Schema::IndexColumns::PlanStep>();
                key.TxId = rowset.GetValue<Schema::IndexColumns::TxId>();
                key.Portion = rowset.GetValue<Schema::IndexColumns::Portion>();
                key.Chunk = rowset.GetValue<Schema::IndexColumns::Chunk>();

                key.XPlanStep = rowset.GetValue<Schema::IndexColumns::XPlanStep>();
                key.XTxId = rowset.GetValue<Schema::IndexColumns::XTxId>();
                key.Blob = rowset.GetValue<Schema::IndexColumns::Blob>();
                key.Metadata = rowset.GetValue<Schema::IndexColumns::Metadata>();
                key.Offset = rowset.GetValue<Schema::IndexColumns::Offset>();
                key.Size = rowset.GetValue<Schema::IndexColumns::Size>();

                pathId = rowset.GetValue<Schema::IndexColumns::PathId>();

                portion2Key[key.Portion] = key;

                UNIT_ASSERT(rowset.Next());
            }
        }

        UNIT_ASSERT(pathId.has_value());

        for (auto&& [ portionId, key ] : portion2Key) {
            db.Table<Schema::IndexColumns>().Key(key.Index, key.Granule, key.ColumnIdx,
            key.PlanStep, key.TxId, key.Portion, key.Chunk).Delete();

            db.Table<Schema::IndexColumns>().Key(key.Index, 1, key.ColumnIdx,
            key.PlanStep, key.TxId, key.Portion, key.Chunk).Update(
                NIceDb::TUpdate<Schema::IndexColumns::XPlanStep>(key.XPlanStep),
                NIceDb::TUpdate<Schema::IndexColumns::XTxId>(key.XTxId),
                NIceDb::TUpdate<Schema::IndexColumns::Blob>(key.Blob),
                NIceDb::TUpdate<Schema::IndexColumns::Metadata>(key.Metadata),
                NIceDb::TUpdate<Schema::IndexColumns::Offset>(key.Offset),
                NIceDb::TUpdate<Schema::IndexColumns::Size>(key.Size),

                NIceDb::TNull<Schema::IndexColumns::PathId>()
            );
        }

        db.Table<Schema::IndexGranules>().Key(0, *pathId, "1").Update(
            NIceDb::TUpdate<Schema::IndexGranules::Granule>(1),
            NIceDb::TUpdate<Schema::IndexGranules::PlanStep>(1),
            NIceDb::TUpdate<Schema::IndexGranules::TxId>(1),
            NIceDb::TUpdate<Schema::IndexGranules::Metadata>("")
        );
    }
};

class TColumnChunksCleaner : public NYDBTest::ILocalDBModifier {
public:
    virtual void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        std::vector<TPortionRecord> portion2Key;
        std::optional<ui64> pathId;
        {
            auto rowset = db.Table<Schema::IndexColumns>().Select();
            UNIT_ASSERT(rowset.IsReady());

            while (!rowset.EndOfSet()) {
                TPortionRecord key;
                key.Index = rowset.GetValue<Schema::IndexColumns::Index>();
                key.Granule = rowset.GetValue<Schema::IndexColumns::Granule>();
                key.ColumnIdx = rowset.GetValue<Schema::IndexColumns::ColumnIdx>();
                key.PlanStep = rowset.GetValue<Schema::IndexColumns::PlanStep>();
                key.TxId = rowset.GetValue<Schema::IndexColumns::TxId>();
                key.Portion = rowset.GetValue<Schema::IndexColumns::Portion>();
                key.Chunk = rowset.GetValue<Schema::IndexColumns::Chunk>();

                key.XPlanStep = rowset.GetValue<Schema::IndexColumns::XPlanStep>();
                key.XTxId = rowset.GetValue<Schema::IndexColumns::XTxId>();
                key.Blob = rowset.GetValue<Schema::IndexColumns::Blob>();
                key.Metadata = rowset.GetValue<Schema::IndexColumns::Metadata>();
                key.Offset = rowset.GetValue<Schema::IndexColumns::Offset>();
                key.Size = rowset.GetValue<Schema::IndexColumns::Size>();

                pathId = rowset.GetValue<Schema::IndexColumns::PathId>();

                portion2Key.emplace_back(std::move(key));

                UNIT_ASSERT(rowset.Next());
            }
        }

        UNIT_ASSERT(pathId.has_value());

        for (auto&& key: portion2Key) {
            NKikimrTxColumnShard::TIndexColumnMeta metaProto;
            UNIT_ASSERT(metaProto.ParseFromArray(key.Metadata.data(), key.Metadata.size()));
            metaProto.ClearNumRows();
            metaProto.ClearRawBytes();

            db.Table<Schema::IndexColumns>().Key(key.Index, key.Granule, key.ColumnIdx,
            key.PlanStep, key.TxId, key.Portion, key.Chunk).Update(
                NIceDb::TUpdate<Schema::IndexColumns::Metadata>(metaProto.SerializeAsString())
            );
        }
    }
};

class TMinMaxCleaner : public NYDBTest::ILocalDBModifier {
public:
    virtual void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        std::vector<TPortionRecord> portion2Key;
        std::optional<ui64> pathId;
        {
            auto rowset = db.Table<Schema::IndexColumns>().Select();
            UNIT_ASSERT(rowset.IsReady());

            while (!rowset.EndOfSet()) {
                TPortionRecord key;
                key.Index = rowset.GetValue<Schema::IndexColumns::Index>();
                key.Granule = rowset.GetValue<Schema::IndexColumns::Granule>();
                key.ColumnIdx = rowset.GetValue<Schema::IndexColumns::ColumnIdx>();
                key.PlanStep = rowset.GetValue<Schema::IndexColumns::PlanStep>();
                key.TxId = rowset.GetValue<Schema::IndexColumns::TxId>();
                key.Portion = rowset.GetValue<Schema::IndexColumns::Portion>();
                key.Chunk = rowset.GetValue<Schema::IndexColumns::Chunk>();

                key.XPlanStep = rowset.GetValue<Schema::IndexColumns::XPlanStep>();
                key.XTxId = rowset.GetValue<Schema::IndexColumns::XTxId>();
                key.Blob = rowset.GetValue<Schema::IndexColumns::Blob>();
                key.Metadata = rowset.GetValue<Schema::IndexColumns::Metadata>();
                key.Offset = rowset.GetValue<Schema::IndexColumns::Offset>();
                key.Size = rowset.GetValue<Schema::IndexColumns::Size>();

                pathId = rowset.GetValue<Schema::IndexColumns::PathId>();

                portion2Key.emplace_back(std::move(key));

                UNIT_ASSERT(rowset.Next());
            }
        }

        UNIT_ASSERT(pathId.has_value());

        for (auto&& key: portion2Key) {
            NKikimrTxColumnShard::TIndexColumnMeta metaProto;
            UNIT_ASSERT(metaProto.ParseFromArray(key.Metadata.data(), key.Metadata.size()));
            if (metaProto.HasPortionMeta()) {
                metaProto.MutablePortionMeta()->ClearRecordSnapshotMax();
                metaProto.MutablePortionMeta()->ClearRecordSnapshotMin();
            }

            db.Table<Schema::IndexColumns>().Key(key.Index, key.Granule, key.ColumnIdx,
            key.PlanStep, key.TxId, key.Portion, key.Chunk).Update(
                NIceDb::TUpdate<Schema::IndexColumns::Metadata>(metaProto.SerializeAsString())
            );
        }
    }
};

template <class TLocalDBModifier>
class TPrepareLocalDBController: public NKikimr::NYDBTest::NColumnShard::TController {
private:
    using TBase = NKikimr::NYDBTest::ICSController;
public:
    NYDBTest::ILocalDBModifier::TPtr BuildLocalBaseModifier() const override {
        return std::make_shared<TLocalDBModifier>();
    }
};

Y_UNIT_TEST_SUITE(Normalizers) {

    template <class TLocalDBModifier>
    void TestNormalizerImpl() {
        using namespace NArrow;
        auto csControllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<TPrepareLocalDBController<TLocalDBModifier>>();

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        const ui64 tableId = 1;
        const std::vector<std::pair<TString, TTypeInfo>> schema = {
                                                                    {"key1", TTypeInfo(NTypeIds::Uint64) },
                                                                    {"key2", TTypeInfo(NTypeIds::Uint64) },
                                                                    {"field", TTypeInfo(NTypeIds::Utf8) }
                                                                };
        PrepareTablet(runtime, tableId, schema, 2);
        const ui64 txId = 111;

        NConstruction::IArrayBuilder::TPtr key1Column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key1");
        NConstruction::IArrayBuilder::TPtr key2Column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key2");
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(8, 100));

        auto batch = NConstruction::TRecordBatchConstructor({ key1Column, key2Column, column }).BuildBatch(2048);
        TString blobData = NArrow::SerializeBatchNoCompression(batch);

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(txId);
        ui64 payloadIndex = NEvWrite::TPayloadHelper<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData)); evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, tableId, 1, schema, payloadIndex, NKikimrDataEvents::FORMAT_ARROW);

        TActorId sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, evWrite.release());
        {
            TAutoPtr<NActors::IEventHandle> handle;
            auto event = runtime.GrabEdgeEvent<NKikimr::NEvents::TDataEvents::TEvWriteResult>(handle);
            UNIT_ASSERT(event);
            UNIT_ASSERT_VALUES_EQUAL((ui64)event->Record.GetStatus(), (ui64)NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);

            PlanWriteTx(runtime, sender, NOlap::TSnapshot(11, txId));
        }

        {
            auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(11, txId), schema);
            UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), 2048);
        }
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);

        {
            auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(11, txId), schema);
            UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), 2048);
        }
    }

    Y_UNIT_TEST(PathIdNormalizer) {
        TestNormalizerImpl<TPathIdCleaner>();
    }

    Y_UNIT_TEST(ColumnChunkNormalizer) {
        TestNormalizerImpl<TColumnChunksCleaner>();
    }

    Y_UNIT_TEST(MinMaxNormalizer) {
        TestNormalizerImpl<TMinMaxCleaner>();
    }
}

} // namespace NKikimr
