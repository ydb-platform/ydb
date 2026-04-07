#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/shard_writer.h>

#include <arrow/record_batch.h>

#include <ydb/library/formats/arrow/simple_builder/array.h>
#include <ydb/library/formats/arrow/simple_builder/batch.h>
#include <ydb/library/formats/arrow/simple_builder/filler.h>
#include <ydb/library/testlib/helpers.h>

#include <set>
#include <tuple>
#include <type_traits>
#include <vector>

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

    virtual void OnControllerRegistered(NYDBTest::NColumnShard::TController& /*controller*/) const {
    }

    virtual void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& /*columnShardConfig*/) const {
    }

    virtual void CorrectFeatureFlagsOnStart(TFeatureFlags& /*featuresFlags*/) const {
    }

    /// Row count for `TestNormalizerImpl` data load (`BuildBatch` / read assertions). Must be divisible by
    /// `PortionCount()`
    virtual ui64 RowCount() const {
        return 20048;
    }

    virtual ui64 RowCountAfterReboot() const {
        return RowCount();
    }

    /// If N > 1, `TestNormalizerImpl` performs N separate write+commit cycles with equal-sized slices (N portions).
    virtual ui32 PortionCount() const {
        return 1;
    }

};

class TSchemaVersionsCleaner: public NYDBTest::ILocalDBModifier {
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

class TPortionsCleaner: public NYDBTest::ILocalDBModifier {
public:
    virtual void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        std::vector<NOlap::TPortionAddress> portions;
        {
            auto rowset = db.Table<Schema::IndexPortions>().Select();
            UNIT_ASSERT(rowset.IsReady());

            while (!rowset.EndOfSet()) {
                NOlap::TPortionAddress addr(TInternalPathId::FromRawValue(rowset.GetValue<Schema::IndexPortions::PathId>()),
                    rowset.GetValue<Schema::IndexPortions::PortionId>());
                portions.emplace_back(addr);
                UNIT_ASSERT(rowset.Next());
            }
        }

        for (auto&& key : portions) {
            db.Table<Schema::IndexPortions>().Key(key.GetPathId().GetRawValue(), key.GetPortionId()).Delete();
        }
    }
};

class TInsertedPortionsCleaner: public NYDBTest::ILocalDBModifier {
public:
    virtual void Apply(NTabletFlatExecutor::TTransactionContext&) const override {
    }
};

class TSubColumnsPortionsCleaner: public NYDBTest::ILocalDBModifier {
public:
    virtual void Apply(NTabletFlatExecutor::TTransactionContext&) const override {
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

                const auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector(
                    { std::make_shared<arrow::Field>("key1", arrow::uint64()), std::make_shared<arrow::Field>("key2", arrow::uint64()) }));
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

        std::vector<TInternalPathId> tables;
        {
            auto rowset = db.Table<Schema::TableInfo>().Select();
            UNIT_ASSERT(rowset.IsReady());

            while (!rowset.EndOfSet()) {
                const auto pathId = TInternalPathId::FromRawValue(rowset.GetValue<Schema::TableInfo::PathId>());
                tables.emplace_back(pathId);
                UNIT_ASSERT(rowset.Next());
            }
        }

        for (auto&& key : tables) {
            db.Table<Schema::TableInfo>().Key(key.GetRawValue()).Delete();
        }

        std::vector<std::pair<TInternalPathId, TSchemeShardLocalPathId>> tablesV1;
        {
            auto rowset = db.Table<Schema::TableInfoV1>().Select();
            UNIT_ASSERT(rowset.IsReady());

            while (!rowset.EndOfSet()) {
                const auto internalPathId = TInternalPathId::FromRawValue(rowset.GetValue<Schema::TableInfoV1::PathId>());
                const auto schemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(rowset.GetValue<Schema::TableInfoV1::SchemeShardLocalPathId>());
                tablesV1.emplace_back(internalPathId, schemeShardLocalPathId);
                UNIT_ASSERT(rowset.Next());
            }
        }

        for (auto&& [internalPathId, schemeShardLocalPathId] : tablesV1) {
            db.Table<Schema::TableInfoV1>().Key(internalPathId.GetRawValue(), schemeShardLocalPathId.GetRawValue()).Delete();
        }

        struct TKey {
            TInternalPathId PathId;
            ui64 Step;
            ui64 TxId;
        };

        std::vector<TKey> versions;
        {
            auto rowset = db.Table<Schema::TableVersionInfo>().Select();
            UNIT_ASSERT(rowset.IsReady());

            while (!rowset.EndOfSet()) {
                TKey key;
                key.PathId = TInternalPathId::FromRawValue(rowset.GetValue<Schema::TableVersionInfo::PathId>());
                key.Step = rowset.GetValue<Schema::TableVersionInfo::SinceStep>();
                key.TxId = rowset.GetValue<Schema::TableVersionInfo::SinceTxId>();
                versions.emplace_back(key);
                UNIT_ASSERT(rowset.Next());
            }
        }

        for (auto&& key : versions) {
            db.Table<Schema::TableVersionInfo>().Key(key.PathId.GetRawValue(), key.Step, key.TxId).Delete();
        }
    }
};

class TEraseMetaFromChunksV0: public NYDBTest::ILocalDBModifier {
public:
    virtual void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        auto rowset = db.Table<Schema::IndexColumns>().Select();
        UNIT_ASSERT(rowset.IsReady());

        while (!rowset.EndOfSet()) {
            NKikimrTxColumnShard::TIndexColumnMeta metaProto;
            UNIT_ASSERT(metaProto.ParseFromString(rowset.GetValue<Schema::IndexColumns::Metadata>()));
            metaProto.ClearPortionMeta();
            db.Table<Schema::IndexColumns>().Key(rowset.GetKey()).Update<Schema::IndexColumns::Metadata>(metaProto.SerializeAsString());
            UNIT_ASSERT(rowset.Next());
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

class TNoopLocalDBModifier: public NYDBTest::ILocalDBModifier {
public:
    void Apply(NTabletFlatExecutor::TTransactionContext&) const override {
    }
};

template <class TInitDBModifier, class TVerifyDBModifier>
class TInitVerifyDBController: public NKikimr::NYDBTest::NColumnShard::TController {
private:
    mutable bool verify = false;

public:
    void SetVerify() {
        verify = true;
    }

    NYDBTest::ILocalDBModifier::TPtr BuildLocalBaseModifier() const override {
        if (!verify) {
            return std::make_shared<TInitDBModifier>();
        }
        return std::make_shared<TVerifyDBModifier>();
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
            AFL_VERIFY(portions.emplace(NOlap::TPortionAddress(TInternalPathId::FromRawValue(rowset.GetValue<NColumnShard::Schema::IndexColumnsV2::PathId>()), rowset.GetValue<NColumnShard::Schema::IndexColumnsV2::PortionId>()), std::move(metaProto)).second);
            UNIT_ASSERT(rowset.Next());
        }

        for (auto&& [key, metadata] : portions) {
            db.Table<Schema::IndexColumnsV2>()
                .Key(key.GetPathId().GetRawValue(), key.GetPortionId())
                .Update(NIceDb::TUpdate<Schema::IndexColumnsV2::Metadata>(metadata.SerializeAsString()));
        }
    }
};

namespace {

// Scheme-shard local path id passed to PrepareTablet as `tableId`. Index tables use internal path ids, which may differ
// when the test controller forces GenerateInternalPathId; resolve the internal id from Schema::TableInfo in TInit.
constexpr ui64 SchemeShardPathId = 1;

}   // namespace

Y_UNIT_TEST_SUITE(Normalizers) {
template <class TInitDBModifier, class TVerifyDBModifier = TNoopLocalDBModifier,
    class TController = TInitVerifyDBController<TInitDBModifier, TVerifyDBModifier>>
void TestNormalizerImpl(const TNormalizerChecker& checker = TNormalizerChecker()) {
        using namespace NArrow;
        auto csControllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<TController>();
        checker.OnControllerRegistered(*csControllerGuard.operator->());

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        runtime.GetAppData().ColumnShardConfig.SetColumnChunksV0Usage(false);

        checker.CorrectConfigurationOnStart(runtime.GetAppData().ColumnShardConfig);
        checker.CorrectFeatureFlagsOnStart(runtime.GetAppData().FeatureFlags);

        const ui64 tableId = SchemeShardPathId;
        const std::vector<NArrow::NTest::TTestColumn> schema = { NArrow::NTest::TTestColumn("key1", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("key2", TTypeInfo(NTypeIds::Uint64)), NArrow::NTest::TTestColumn("field", TTypeInfo(NTypeIds::Utf8)) };
        const std::vector<ui32> columnsIds = { 1, 2, 3 };
        auto planStep = PrepareTablet(runtime, tableId, schema, 2);
        const ui64 baseTxId = 111;

        NConstruction::IArrayBuilder::TPtr key1Column =
            std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key1");
        NConstruction::IArrayBuilder::TPtr key2Column =
            std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("key2");
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
            "field", NConstruction::TStringPoolFiller(8, 100));

        const ui64 rowCount = checker.RowCount();
        const ui32 portionCount = checker.PortionCount();
        auto batchFull = NConstruction::TRecordBatchConstructor({ key1Column, key2Column, column }).BuildBatch(rowCount);
        NTxUT::TShardWriter writer(runtime, TTestTxConfig::TxTablet0, tableId, 222);
        ui64 snapshotTxId = baseTxId;
        UNIT_ASSERT_C(rowCount % portionCount == 0, "TestNormalizerRowCount must be divisible by PortionCount for multi-portion load");
        const ui64 rowsPerCommit = rowCount / portionCount;
        ui64 commitTxId = baseTxId;
        for (ui32 p = 0; p < portionCount; ++p) {
            auto batch = batchFull->Slice(p * rowsPerCommit, rowsPerCommit);
            AFL_VERIFY(writer.Write(batch, {1, 2, 3}, commitTxId) == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
            planStep = writer.StartCommit(commitTxId);
            PlanWriteTx(runtime, writer.GetSender(), NOlap::TSnapshot(planStep, commitTxId));
            ++commitTxId;
        }
        snapshotTxId = commitTxId - 1;

        {
            auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(planStep, snapshotTxId), schema);
            UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), rowCount);
        }
        RebootTablet(runtime, TTestTxConfig::TxTablet0, writer.GetSender());

        {
            auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(planStep, snapshotTxId), schema);
            UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), checker.RowCountAfterReboot());
        }
        if constexpr (!std::is_same_v<TVerifyDBModifier, TNoopLocalDBModifier>) {
            csControllerGuard->SetVerify();
            RebootTablet(runtime, TTestTxConfig::TxTablet0, writer.GetSender());
        }
    }

    Y_UNIT_TEST(PortionsNormalizer) {
        class TLocalNormalizerChecker: public TNormalizerChecker {
        public:
            virtual ui64 RowCountAfterReboot() const override {
                return 0;
            }
            virtual void CorrectFeatureFlagsOnStart(TFeatureFlags& featuresFlags) const override {
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

    Y_UNIT_TEST(InsertedPortionsCleanerNormalizer) {
        class TLocalNormalizerChecker: public TNormalizerChecker {
        public:
            virtual ui64 RowCountAfterReboot() const override {
                return 0;
            }
            virtual void CorrectFeatureFlagsOnStart(TFeatureFlags& /* featuresFlags */) const override {
            }
            virtual void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& columnShardConfig) const override {
                {
                    auto* repair = columnShardConfig.MutableRepairs()->Add();
                    repair->SetClassName("CleanInsertedPortions");
                    repair->SetDescription("Removing inserted portions");
                }
            }
        };
        TestNormalizerImpl<TInsertedPortionsCleaner>(TLocalNormalizerChecker());
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
            ui64 RowCountAfterReboot() const override {
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

    Y_UNIT_TEST(ChunksV0MetaNormalizer) {
        class TLocalNormalizerChecker: public TNormalizerChecker {
        public:
            virtual void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& columnShardConfig) const override {
                auto* repair = columnShardConfig.MutableRepairs()->Add();
                repair->SetClassName("RestoreV0ChunksMeta");
                repair->SetDescription("Restoring PortionMeta in IndexColumns");
            }
        };
        TLocalNormalizerChecker checker;
        TestNormalizerImpl<TEraseMetaFromChunksV0>(checker);
    }

    Y_UNIT_TEST(CleanIndexColumnsV0Normalizer) {
        class TInit: public NYDBTest::ILocalDBModifier {
        public:
            void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
                using namespace NColumnShard;

                NIceDb::TNiceDb db(txc.DB);
                UNIT_ASSERT_C(db.HaveTable<Schema::IndexColumns>(), "Expected IndexColumns table to exist for clean-up test");
                for (size_t i = 0; i < 100; ++i) {
                    db.Table<Schema::IndexColumns>().Key(1 + i, 2 + i, 3 + i, 4 + i, 5 + i, 6 + i, 7 + i).Update();
                }
            }
        };

        class TVerify: public NYDBTest::ILocalDBModifier {
        public:
            void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
                NIceDb::TNiceDb db(txc.DB);
                auto rowset = db.Table<Schema::IndexColumns>().Select();
                UNIT_ASSERT(rowset.IsReady());
                UNIT_ASSERT_C(
                    rowset.EndOfSet(), "Expected CleanIndexColumns normalizer to remove all rows from IndexColumns table");
            }
        };

        class TChecker: public TNormalizerChecker {
        public:
            virtual void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& columnShardConfig) const override {
                columnShardConfig.SetColumnChunksV0Usage(false);
                auto* repair = columnShardConfig.MutableRepairs()->Add();
                repair->SetClassName("CleanIndexColumns");
                repair->SetDescription("Cleaning old table");
            }
        };

        TChecker checker;
        TestNormalizerImpl<TInit, TVerify>(checker);
    }

    Y_UNIT_TEST(LeakedBlobsNormalizer) {
        class TExpectation {
            THashSet<TString> BlobIdLegacyKeys;

        public:
            void Clear() {
                BlobIdLegacyKeys.clear();
            }

            void AddKey(const TString& key) {
                BlobIdLegacyKeys.insert(key);
            }

            const THashSet<TString>& Keys() const {
                return BlobIdLegacyKeys;
            }
        };

        class TInit;
        class TVerify;

        class TController: public TInitVerifyDBController<TInit, TVerify> {
        public:
            mutable TExpectation Expectation;
        };

        // TestNormalizerImpl creates a table (scheme-shard local path SchemeShardPathId), writes rows in
        // PortionCount() commits (one portion per commit), then reboots. We remove index metadata for half of
        // those portions and expect BlobsToDeleteWT to list only blob ids belonging to the corrupted portions.
        class TInit: public NYDBTest::ILocalDBModifier {
        public:
            void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
                using namespace NColumnShard;

                auto* ctrl = NYDBTest::TControllers::GetControllerAs<TController>();
                UNIT_ASSERT_C(ctrl != nullptr, "LeakedBlobsNormalizer: expected TController registered via RegisterCSControllerGuard");
                ctrl->Expectation.Clear();

                NIceDb::TNiceDb db(txc.DB);
                ui64 internalPathId = 0;
                {
                    auto rowset = db.Table<Schema::TableInfo>().Select();
                    UNIT_ASSERT(rowset.IsReady());
                    while (!rowset.EndOfSet()) {
                        if (rowset.GetValue<Schema::TableInfo::SchemeShardLocalPathId>() ==
                            SchemeShardPathId) {
                            internalPathId = rowset.GetValue<Schema::TableInfo::PathId>();
                            break;
                        }
                        UNIT_ASSERT(rowset.Next());
                    }
                }
                UNIT_ASSERT_C(internalPathId != 0,
                    "LeakedBlobsNormalizer: no TableInfo row for the test table's scheme-shard local path "
                    "(cannot map to IndexPortions.PathId)");

                std::set<std::pair<ui64, ui64>> portionSet;
                {
                    auto rowset = db.Table<Schema::IndexPortions>().Select();
                    UNIT_ASSERT(rowset.IsReady());
                    while (!rowset.EndOfSet()) {
                        const ui64 pathId = rowset.GetValue<Schema::IndexPortions::PathId>();
                        if (pathId == internalPathId) {
                            portionSet.emplace(pathId, rowset.GetValue<Schema::IndexPortions::PortionId>());
                        }
                        UNIT_ASSERT(rowset.Next());
                    }
                }
                UNIT_ASSERT_C(!portionSet.empty(),
                    "LeakedBlobsNormalizer: no portions for the test table path before corruption "
                    "(data must come from TestNormalizerImpl write path)");

                std::vector<std::pair<ui64, ui64>> portionVec(portionSet.begin(), portionSet.end());
                UNIT_ASSERT_C(portionVec.size() == 10,
                    "LeakedBlobsNormalizer: expected 10 portions from multi-commit load "
                    "(adjust row count / commit count if portions merge)");
                const size_t corruptCount = portionVec.size() / 2;
                const std::set<std::pair<ui64, ui64>> corruptedPortions(portionVec.begin(), portionVec.begin() + corruptCount);

                {
                    auto rowset = db.Table<Schema::IndexColumnsV2>().Select();
                    UNIT_ASSERT(rowset.IsReady());
                    while (!rowset.EndOfSet()) {
                        const ui64 pathId = rowset.GetValue<Schema::IndexColumnsV2::PathId>();
                        const ui64 portionId = rowset.GetValue<Schema::IndexColumnsV2::PortionId>();
                        if (pathId != internalPathId) {
                            UNIT_ASSERT(rowset.Next());
                            continue;
                        }
                        UNIT_ASSERT_C(portionSet.contains({pathId, portionId}),
                            "LeakedBlobsNormalizer: IndexColumnsV2 row without matching IndexPortions for the test table");
                        if (!corruptedPortions.contains({pathId, portionId})) {
                            UNIT_ASSERT(rowset.Next());
                            continue;
                        }

                        const TString blobIdsProto = rowset.GetValue<Schema::IndexColumnsV2::BlobIds>();
                        NKikimrTxColumnShard::TIndexPortionBlobsInfo blobsInfo;
                        UNIT_ASSERT_C(blobsInfo.ParseFromArray(blobIdsProto.data(), blobIdsProto.size()),
                            "Failed to parse TIndexPortionBlobsInfo");
                        for (const TString& bin : blobsInfo.GetBlobIds()) {
                            UNIT_ASSERT_VALUES_EQUAL(bin.size(), NKikimr::TLogoBlobID::BinarySize);
                            const NKikimr::TLogoBlobID logoId = NKikimr::TLogoBlobID::FromBinary(bin);
                            ctrl->Expectation.AddKey(logoId.ToString());
                        }
                        UNIT_ASSERT(rowset.Next());
                    }
                }

                std::vector<std::tuple<ui64, ui64, ui32, ui32>> indexKeysToErase;
                {
                    auto rowset = db.Table<Schema::IndexIndexes>().Select();
                    UNIT_ASSERT(rowset.IsReady());
                    while (!rowset.EndOfSet()) {
                        const ui64 pathId = rowset.GetValue<Schema::IndexIndexes::PathId>();
                        const ui64 portionId = rowset.GetValue<Schema::IndexIndexes::PortionId>();
                        if (corruptedPortions.contains({pathId, portionId})) {
                            if (rowset.template HaveValue<Schema::IndexIndexes::Blob>()) {
                                const TString blobStr = rowset.GetValue<Schema::IndexIndexes::Blob>();
                                if (blobStr.size() == NKikimr::TLogoBlobID::BinarySize) {
                                    const NKikimr::TLogoBlobID logoId = NKikimr::TLogoBlobID::FromBinary(blobStr);
                                    ctrl->Expectation.AddKey(logoId.ToString());
                                }
                            }
                            indexKeysToErase.emplace_back(pathId, portionId, rowset.GetValue<Schema::IndexIndexes::IndexId>(),
                                rowset.GetValue<Schema::IndexIndexes::ChunkIdx>());
                        }
                        UNIT_ASSERT(rowset.Next());
                    }
                }
                for (const auto& key : indexKeysToErase) {
                    db.Table<Schema::IndexIndexes>()
                        .Key(std::get<0>(key), std::get<1>(key), std::get<2>(key), std::get<3>(key))
                        .Delete();
                }
                for (const auto& pathPortion : corruptedPortions) {
                    db.Table<Schema::IndexColumnsV2>().Key(pathPortion.first, pathPortion.second).Delete();
                    db.Table<Schema::IndexPortions>().Key(pathPortion.first, pathPortion.second).Delete();
                }
                UNIT_ASSERT_C(!ctrl->Expectation.Keys().empty(),
                    "LeakedBlobsNormalizer: no blob ids captured from the test table index before tear-down");
            }
        };

        class TVerify: public NYDBTest::ILocalDBModifier {
        public:
            void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
                using namespace NColumnShard;

                auto* ctrl = NYDBTest::TControllers::GetControllerAs<TController>();
                UNIT_ASSERT_C(ctrl != nullptr, "LeakedBlobsNormalizer: expected TController registered via RegisterCSControllerGuard");

                NIceDb::TNiceDb db(txc.DB);
                THashSet<TString> actualKeys;
                auto rowset = db.Table<Schema::BlobsToDeleteWT>().Select();
                UNIT_ASSERT(rowset.IsReady());
                while (!rowset.EndOfSet()) {
                    const TString blobIdStr = rowset.GetValue<Schema::BlobsToDeleteWT::BlobId>();
                    const ui64 tabletId = rowset.GetValue<Schema::BlobsToDeleteWT::TabletId>();
                    UNIT_ASSERT_VALUES_EQUAL_C(tabletId, TTestTxConfig::TxTablet0,
                        "BlobsToDeleteWT row must reference the column shard tablet under test");
                    actualKeys.insert(blobIdStr);
                    UNIT_ASSERT(rowset.Next());
                }
                const auto& expected = ctrl->Expectation.Keys();
                UNIT_ASSERT_VALUES_EQUAL_C(actualKeys.size(), expected.size(),
                    TStringBuilder() << "BlobsToDeleteWT size mismatch: expected " << expected.size() << " distinct keys, got "
                                     << actualKeys.size());
                for (const TString& key : expected) {
                    UNIT_ASSERT_C(actualKeys.contains(key),
                        TStringBuilder() << "Expected blob id missing in BlobsToDeleteWT: " << key);
                }
            }
        };

        class TChecker: public TNormalizerChecker {
        public:
            ui64 RowCount() const override {
                return 20000;
            }

            ui32 PortionCount() const override {
                return 10;
            }

            ui64 RowCountAfterReboot() const override {
                return RowCount() / 2;
            }

            void OnControllerRegistered(NYDBTest::NColumnShard::TController& controller) const override {
                // Otherwise blob GC (SetupGC) loads BlobsToDeleteWT, runs CollectGarbage, and EraseBlobToDelete —
                // rows vanish before TVerify reads the table (see IBlobsGCAction::OnExecuteTxAfterCleaning, blob_manager_db.cpp).
                controller.DisableBackground(NYDBTest::ICSController::EBackground::GC);
                // We do not want compaction to interfere. We expect 10 portions in init, but compaction may merge some and we'll get less.
                controller.DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
            }

            void CorrectFeatureFlagsOnStart(TFeatureFlags& featuresFlags) const override {
                featuresFlags.SetEnableWritePortionsOnInsert(true);
            }

            void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& columnShardConfig) const override {
                auto* repair = columnShardConfig.MutableRepairs()->Add();
                repair->SetClassName("LeakedBlobsNormalizer");
                repair->SetDescription("Detecting leaked blobs after index tear-down");
            }
        };

        TChecker checker;
        TestNormalizerImpl<TInit, TVerify, TController>(checker);
    }

    Y_UNIT_TEST(CleanIndexColumnsV1Normalizer) {
        class TInit: public NYDBTest::ILocalDBModifier {
        public:
            void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
                using namespace NColumnShard;

                NIceDb::TNiceDb db(txc.DB);
                UNIT_ASSERT_C(db.HaveTable<Schema::IndexColumnsV1>(), "Expected IndexColumnsV1 table to exist for clean-up test");
                for (size_t i = 0; i < 100; ++i) {
                    db.Table<Schema::IndexColumnsV1>().Key(1 + i, 2 + i, 3 + i, 4 + i).Update();
                }
            }
        };

        class TVerify: public NYDBTest::ILocalDBModifier {
        public:
            void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
                NIceDb::TNiceDb db(txc.DB);
                auto rowset = db.Table<Schema::IndexColumnsV1>().Select();
                UNIT_ASSERT(rowset.IsReady());
                UNIT_ASSERT_C(
                    rowset.EndOfSet(), "Expected CleanIndexColumnsV1 normalizer to remove all rows from IndexColumnsV1 table");
            }
        };

        class TChecker: public TNormalizerChecker {
        public:
            virtual void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& columnShardConfig) const override {
                columnShardConfig.SetColumnChunksV1Usage(false);
                auto* repair = columnShardConfig.MutableRepairs()->Add();
                repair->SetClassName("CleanIndexColumnsV1");
                repair->SetDescription("Cleaning old table");
            }
        };

        TChecker checker;
        TestNormalizerImpl<TInit, TVerify>(checker);
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

    Y_UNIT_TEST_TWIN(SubColumnsPortionsCleanerNormalizer, useSubcolumns) {
        class TLocalNormalizerChecker: public TNormalizerChecker {
        public:
            virtual void CorrectFeatureFlagsOnStart(TFeatureFlags& /* featuresFlags */) const override {
            }
            virtual void CorrectConfigurationOnStart(NKikimrConfig::TColumnShardConfig& columnShardConfig) const override {
                {
                    auto* repair = columnShardConfig.MutableRepairs()->Add();
                    repair->SetClassName("CleanSubColumnsPortions");
                    repair->SetDescription("Removing SubColumns portions");
                }
            }
        };

        TLocalNormalizerChecker checker;

        using namespace NArrow;
        auto csControllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<TPrepareLocalDBController<TSubColumnsPortionsCleaner>>();

        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        runtime.GetAppData().ColumnShardConfig.SetColumnChunksV0Usage(false);

        checker.CorrectConfigurationOnStart(runtime.GetAppData().ColumnShardConfig);
        checker.CorrectFeatureFlagsOnStart(runtime.GetAppData().FeatureFlags);

        const ui64 tableId = 1;
        const std::vector<NArrow::NTest::TTestColumn> schema = { NArrow::NTest::TTestColumn("id", TTypeInfo(NTypeIds::Uint64)),
            NArrow::NTest::TTestColumn("json_payload", TTypeInfo(NTypeIds::JsonDocument)) };
        const std::vector<ui32> columnsIds = { 1, 2 };
        const ui64 keySize = 1;
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard), &CreateColumnShard);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        runtime.DispatchEvents(options);

        TestTableDescription tableDescription;
        tableDescription.Schema = schema;
        tableDescription.Pk = {};

        for (ui64 i = 0; i < keySize; ++i) {
            Y_ABORT_UNLESS(i < schema.size());
            tableDescription.Pk.push_back(schema[i]);
        }
        TActorId sender = runtime.AllocateEdgeActor();
        TString codec = "none";
        auto specials = TTestSchema::TTableSpecials().WithCodec(codec);

        NKikimrTxColumnShard::TSchemaTxBody tx;
        auto* table = tx.MutableInitShard()->AddTables();
        tx.MutableInitShard()->SetOwnerPath("/Root/olap");
        tx.MutableInitShard()->SetOwnerPathId(tableId);
        NColumnShard::TSchemeShardLocalPathId::FromRawValue(tableId).ToProto(*table);
        auto mSchema = table->MutableSchema();

        NTxUT::TTestSchema::InitSchema(tableDescription.Schema, tableDescription.Pk, specials, mSchema);
        if (useSubcolumns) {
            mSchema->MutableColumns(1)->MutableDataAccessorConstructor()->SetClassName("SUB_COLUMNS");
        }
        NTxUT::TTestSchema::InitTiersAndTtl(specials, table->MutableTtlSettings());

        Cerr << "CreateStandaloneTable: " << tx << "\n";

        TString txBody;
        Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&txBody);
        auto planStep = SetupSchema(runtime, sender, txBody, 10);
        const ui64 txId = 111;

        class TJsonSeqFiller {
            TString Data = HexDecode("01030000410000001C00000020000000040500000406000002000000C0040000000500006100620000000000000010400000000000001440");
        public:
            using TValue = arrow::BinaryType;
            arrow::util::string_view GetValue(const ui32) const {
                return arrow::util::string_view(Data.data(), Data.size());
            };
        };

        NConstruction::IArrayBuilder::TPtr idColumn =
            std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TIntSeqFiller<arrow::UInt64Type>>>("id");
        NConstruction::IArrayBuilder::TPtr jsonColumn = std::make_shared<NConstruction::TSimpleArrayConstructor<TJsonSeqFiller>>(
            "json_payload", TJsonSeqFiller());

        auto batch = NConstruction::TRecordBatchConstructor({ idColumn, jsonColumn }).BuildBatch(20048);
        NTxUT::TShardWriter writer(runtime, TTestTxConfig::TxTablet0, tableId, 222);
        AFL_VERIFY(writer.Write(batch, columnsIds, txId) == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        planStep = writer.StartCommit(txId);
        PlanWriteTx(runtime, writer.GetSender(), NOlap::TSnapshot(planStep, txId));

        {
            auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(planStep, txId), schema);
            UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), 20048);
        }
        RebootTablet(runtime, TTestTxConfig::TxTablet0, writer.GetSender());

        {
            auto readResult = ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(planStep, txId), schema);
            UNIT_ASSERT_VALUES_EQUAL(readResult->num_rows(), useSubcolumns ? 0 : 20048);
        }
    }
}
}   // namespace NKikimr
