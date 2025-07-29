#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/shard_writer.h>

#include <ydb/library/formats/arrow/simple_builder/array.h>
#include <ydb/library/formats/arrow/simple_builder/batch.h>
#include <ydb/library/formats/arrow/simple_builder/filler.h>

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

class TNormalizerChecker {
public:
    virtual ~TNormalizerChecker() {
    }

    virtual void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& /*columnShardConfig*/) const {
    }

    virtual void CorrectFeatureFlagsOnStart(TFeatureFlags& /*featuresFlags*/) const {
    }

    virtual ui64 RecordsCountAfterReboot(const ui64 initialRecodsCount) const {
        return initialRecodsCount;
    }
};

class TColumnChunksCleaner: public NYDBTest::ILocalDBModifier {
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

        for (auto&& key : portion2Key) {
            NKikimrTxColumnShard::TIndexColumnMeta metaProto;
            UNIT_ASSERT(metaProto.ParseFromArray(key.Metadata.data(), key.Metadata.size()));
//            metaProto.ClearNumRows();
            metaProto.ClearRawBytes();

            db.Table<Schema::IndexColumns>()
                .Key(key.Index, key.Granule, key.ColumnIdx, key.PlanStep, key.TxId, key.Portion, key.Chunk)
                .Update(NIceDb::TUpdate<Schema::IndexColumns::Metadata>(metaProto.SerializeAsString()));
        }
    }
};

class TSchemaVersionsCleaner : public NYDBTest::ILocalDBModifier {
public:
    virtual void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        // Add invalid widow schema, if SchemaVersionCleaner will not erase it, then test will fail
        {
            NKikimrTxColumnShard::TSchemaPresetVersionInfo info;
            info.SetId(1);
            info.SetSinceStep(5);
            info.SetSinceTxId(1);
            info.MutableSchema()->SetVersion(0);
            db.Table<Schema::SchemaPresetVersionInfo>().Key(1, 5, 1).Update(
                NIceDb::TUpdate<Schema::SchemaPresetVersionInfo::InfoProto>(info.SerializeAsString()));
        }

        {
            // Add invalid widow table version, if SchemaVersionCleaner will not erase it, then test will fail
            NKikimrTxColumnShard::TTableVersionInfo versionInfo;
            versionInfo.SetSchemaPresetId(1);
            versionInfo.SetSinceStep(5);
            versionInfo.SetSinceTxId(1);
            db.Table<Schema::TableVersionInfo>().Key(1, 5, 1).Update(
                NIceDb::TUpdate<Schema::TableVersionInfo::InfoProto>(versionInfo.SerializeAsString()));
        }
    }
};

class TPortionsCleaner : public NYDBTest::ILocalDBModifier {
public:
    virtual void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        std::vector<NOlap::TPortionAddress> portions;
        {
            auto rowset = db.Table<Schema::IndexPortions>().Select();
            UNIT_ASSERT(rowset.IsReady());

            while (!rowset.EndOfSet()) {
                NOlap::TPortionAddress addr(
                    rowset.GetValue<Schema::IndexPortions::PathId>(), rowset.GetValue<Schema::IndexPortions::PortionId>());
                portions.emplace_back(addr);
                UNIT_ASSERT(rowset.Next());
            }
        }

        for (auto&& key : portions) {
            db.Table<Schema::IndexPortions>().Key(key.GetPathId(), key.GetPortionId()).Delete();
        }
    }
};

class TEmptyPortionsCleaner: public NYDBTest::ILocalDBModifier {
public:
    virtual void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for (size_t pathId = 100; pathId != 299; ++pathId) {
            for (size_t portionId = pathId * 100000 + 1000; portionId != pathId * 100000 + 1199; ++portionId) {
                NKikimrTxColumnShard::TIndexPortionMeta metaProto;
                metaProto.SetDeletionsCount(0);
                metaProto.SetIsInserted(true);

                const auto schema = std::make_shared<arrow::Schema>(
                    arrow::FieldVector({ std::make_shared<arrow::Field>("key1", arrow::uint64()), std::make_shared<arrow::Field>("key2", arrow::uint64()) }));
                auto batch = NArrow::MakeEmptyBatch(schema, 1);
                NArrow::TFirstLastSpecialKeys keys(batch);
                metaProto.SetPrimaryKeyBorders(keys.SerializePayloadToString());
                metaProto.MutableRecordSnapshotMin()->SetPlanStep(0);
                metaProto.MutableRecordSnapshotMin()->SetTxId(0);
                metaProto.MutableRecordSnapshotMax()->SetPlanStep(0);
                metaProto.MutableRecordSnapshotMax()->SetTxId(0);
                db.Table<Schema::IndexPortions>()
                    .Key(pathId, portionId)
                    .Update(NIceDb::TUpdate<Schema::IndexPortions::SchemaVersion>(1),
                        NIceDb::TUpdate<Schema::IndexPortions::Metadata>(metaProto.SerializeAsString()),
                        NIceDb::TUpdate<Schema::IndexPortions::MinSnapshotPlanStep>(10),
                        NIceDb::TUpdate<Schema::IndexPortions::MinSnapshotTxId>(10));
            }
        }
    }
};

class TTablesCleaner: public NYDBTest::ILocalDBModifier {
public:
    virtual void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        std::vector<ui64> tables;
        {
            auto rowset = db.Table<Schema::TableInfo>().Select();
            UNIT_ASSERT(rowset.IsReady());

            while (!rowset.EndOfSet()) {
                const auto pathId = rowset.GetValue<Schema::TableInfo::PathId>();
                tables.emplace_back(pathId);
                UNIT_ASSERT(rowset.Next());
            }
        }

        for (auto&& key : tables) {
            db.Table<Schema::TableInfo>().Key(key).Delete();
        }

        struct TKey {
            ui64 PathId;
            ui64 Step;
            ui64 TxId;
        };

        std::vector<TKey> versions;
        {
            auto rowset = db.Table<Schema::TableVersionInfo>().Select();
            UNIT_ASSERT(rowset.IsReady());

            while (!rowset.EndOfSet()) {
                TKey key;
                key.PathId = rowset.GetValue<Schema::TableVersionInfo::PathId>();
                key.Step = rowset.GetValue<Schema::TableVersionInfo::SinceStep>();
                key.TxId = rowset.GetValue<Schema::TableVersionInfo::SinceTxId>();
                versions.emplace_back(key);
                UNIT_ASSERT(rowset.Next());
            }
        }

        for (auto&& key : versions) {
            db.Table<Schema::TableVersionInfo>().Key(key.PathId, key.Step, key.TxId).Delete();
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

class TTrashUnusedInjector: public NYDBTest::ILocalDBModifier {
public:
    void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
        using namespace NColumnShard;

        NIceDb::TNiceDb db(txc.DB);

        if (db.HaveTable<Schema::IndexColumns>()) {
            for (size_t i = 0; i < 100; ++i) {
                db.Table<Schema::IndexColumns>().Key(1 + i, 2 + i, 3 + i, 4 + i, 5 + i, 6 + i, 7 + i).Update();
            }
        }
    }
};

template <ui64 ColumnId>
class TExtraColumnsInjector: public NYDBTest::ILocalDBModifier {
public:
    void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
        using namespace NColumnShard;

        NIceDb::TNiceDb db(txc.DB);

        THashMap<NOlap::TPortionAddress, NKikimrTxColumnShard::TIndexPortionAccessor> portions;

        auto rowset = db.Table<Schema::IndexColumnsV2>().Select();
        UNIT_ASSERT(rowset.IsReady());

        while (!rowset.EndOfSet()) {
            auto metadataString = rowset.GetValue<NColumnShard::Schema::IndexColumnsV2::Metadata>();
            NKikimrTxColumnShard::TIndexPortionAccessor metaProto;
            AFL_VERIFY(metaProto.ParseFromArray(metadataString.data(), metadataString.size()))("event", "cannot parse metadata as protobuf");
            {
                auto chunk = metaProto.AddChunks();
                chunk->SetSSColumnId(ColumnId);
                chunk->MutableBlobRangeLink()->SetSize(1);
                chunk->MutableChunkMetadata()->SetNumRows(20048);
                chunk->MutableChunkMetadata()->SetRawBytes(1);
            }
            AFL_VERIFY(portions.emplace(NOlap::TPortionAddress(rowset.GetValue<NColumnShard::Schema::IndexColumnsV2::PathId>(), rowset.GetValue<NColumnShard::Schema::IndexColumnsV2::PortionId>()), std::move(metaProto)).second);
            UNIT_ASSERT(rowset.Next());
        }

        for (auto&& [key, metadata] : portions) {
            db.Table<Schema::IndexColumnsV2>()
                .Key(key.GetPathId(), key.GetPortionId())
                .Update(NIceDb::TUpdate<Schema::IndexColumnsV2::Metadata>(metadata.SerializeAsString()));
        }
    }
};

Y_UNIT_TEST_SUITE(Normalizers) {
    template <class TLocalDBModifier>
    void TestNormalizerImpl(const TNormalizerChecker& checker = TNormalizerChecker()) {
        using namespace NArrow;
        auto csControllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<TPrepareLocalDBController<TLocalDBModifier>>();

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        checker.CorrectConfigurationOnStart(runtime.GetAppData().ColumnShardConfig); 
        checker.CorrectFeatureFlagsOnStart(runtime.GetAppData().FeatureFlags); 

        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = { NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)), NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8)) };
        const std::vector<ui32> columnsIds = { 1, 2, 3 };
        PrepareTablet(runtime, tableId, schema, 2);
        const ui64 txId = 111;

        NConstruction::IArrayBuilder::TPtr key1Column =
            std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key1");
        NConstruction::IArrayBuilder::TPtr key2Column =
            std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key2");
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(8, 100));

        auto batch = NConstruction::TRecordBatchConstructor({ key1Column, key2Column, column }).BuildBatch(20048);
        NTxUT::TShardWriter writer(runtime, TTestTxConfig::TxTablet0, tableId, 222);
        AFL_VERIFY(writer.Write(batch, {1, 2, 3}, txId) == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        AFL_VERIFY(writer.StartCommit(txId) == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
        PlanWriteTx(runtime, writer.GetSender(), NOlap::TSnapshot(11, txId));

        {
            auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(11, txId), schema);
            UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), 20048);
        }
        RebootTablet(runtime, TTestTxConfig::TxTablet0, writer.GetSender());

        {
            auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(11, txId), schema);
            UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), checker.RecordsCountAfterReboot(20048));
        }
    }

    Y_UNIT_TEST(ColumnChunkNormalizer) {
        TestNormalizerImpl<TColumnChunksCleaner>();
    }

    Y_UNIT_TEST(PortionsNormalizer) {
        class TLocalNormalizerChecker: public TNormalizerChecker {
        public:
            virtual ui64 RecordsCountAfterReboot(const ui64 /*initialRecodsCount*/) const override {
                return 0;
            }
            virtual void CorrectFeatureFlagsOnStart(TFeatureFlags & featuresFlags) const override{
                featuresFlags.SetEnableWritePortionsOnInsert(true);
            }
            virtual void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& columnShardConfig) const override {
                {
                    auto* repair = columnShardConfig.MutableRepairs()->Add();
                    repair->SetClassName("EmptyPortionsCleaner");
                    repair->SetDescription("Removing unsync portions");
                }
                {
                    auto* repair = columnShardConfig.MutableRepairs()->Add();
                    repair->SetClassName("LeakedBlobsNormalizer");
                    repair->SetDescription("Removing leaked blobs");
                }
            }
        };
        TestNormalizerImpl<TPortionsCleaner>(TLocalNormalizerChecker());
    }

    Y_UNIT_TEST(SchemaVersionsNormalizer) {
        class TLocalNormalizerChecker: public TNormalizerChecker {
        public:
            virtual void CorrectFeatureFlagsOnStart(TFeatureFlags& featuresFlags) const override {
                featuresFlags.SetEnableWritePortionsOnInsert(true);
            }
            virtual void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& columnShardConfig) const override {
                auto* repair = columnShardConfig.MutableRepairs()->Add();
                repair->SetClassName("SchemaVersionCleaner");
                repair->SetDescription("Removing unused schema versions");
            }
        };
        TestNormalizerImpl<TSchemaVersionsCleaner>(TLocalNormalizerChecker());
    }

    Y_UNIT_TEST(CleanEmptyPortionsNormalizer) {
        class TLocalNormalizerChecker: public TNormalizerChecker {
        public:
            virtual void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& columnShardConfig) const override {
                auto* repair = columnShardConfig.MutableRepairs()->Add();
                repair->SetClassName("EmptyPortionsCleaner");
                repair->SetDescription("Removing unsync portions");
            }
        };
        TestNormalizerImpl<TEmptyPortionsCleaner>(TLocalNormalizerChecker());
    }


    Y_UNIT_TEST(EmptyTablesNormalizer) {
        class TLocalNormalizerChecker: public TNormalizerChecker {
        public:
            ui64 RecordsCountAfterReboot(const ui64) const override {
                return 0;
            }
            virtual void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& columnShardConfig) const override {
                auto* repair = columnShardConfig.MutableRepairs()->Add();
                repair->SetClassName("PortionsCleaner");
                repair->SetDescription("Removing dirty portions withno tables");
            }
        };
        TLocalNormalizerChecker checker;
        TestNormalizerImpl<TTablesCleaner>(checker);
    }

    Y_UNIT_TEST(RemoveDeleteFlagNormalizer) {
        class TChecker: public TNormalizerChecker {
        public:
            virtual void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& columnShardConfig) const override {
                auto* repair = columnShardConfig.MutableRepairs()->Add();
                repair->SetClassName("RemoveDeleteFlag");
                repair->SetDescription("normalizer");
            }
        };

        TChecker checker;
        TestNormalizerImpl<TExtraColumnsInjector<NOlap::NPortion::TSpecialColumns::SPEC_COL_DELETE_FLAG_INDEX>>(checker);
    }

    Y_UNIT_TEST(RemoveWriteIdNormalizer) {
        class TChecker: public TNormalizerChecker {
        public:
            virtual void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& columnShardConfig) const override {
                auto* repair = columnShardConfig.MutableRepairs()->Add();
                repair->SetClassName("RemoveWriteId");
                repair->SetDescription("normalizer");
            }
        };

        TChecker checker;
        TestNormalizerImpl<TExtraColumnsInjector<NOlap::NPortion::TSpecialColumns::SPEC_COL_WRITE_ID_INDEX>>(checker);
    }
}

}   // namespace NKikimr
