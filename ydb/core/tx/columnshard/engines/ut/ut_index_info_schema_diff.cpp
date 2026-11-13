#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/schema_diff.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NTest {

namespace {

<<<<<<< HEAD
NKikimrSchemeOp::TColumnTableSchema MakeSchemaV1() {
    NKikimrSchemeOp::TColumnTableSchema proto;
    const std::vector<NArrow::NTest::TTestColumn> columns = {
        NArrow::NTest::TTestColumn("timestamp", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)).SetNullable(false),
        NArrow::NTest::TTestColumn("value", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)),
        NArrow::NTest::TTestColumn("to_drop", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)),
    };
    *proto.MutableColumns()->Add() = columns[0].CreateColumn(1);
    *proto.MutableColumns()->Add() = columns[1].CreateColumn(2);
    *proto.MutableColumns()->Add() = columns[2].CreateColumn(3);
    proto.AddKeyColumnNames("timestamp");
    proto.SetVersion(1);
    return proto;
=======
const ui64 PresetId = 1;
const ui32 KeyColumnId = 1;

// everything the shard reads from a schema version, as text, so that two builds of one version compare in one assertion
TString Describe(const TIndexInfo& info) {
    TStringBuilder sb;
    sb << "version=" << info.GetVersion() << "\n";
    for (const ui32 columnId : info.GetColumnIds(true)) {
        const TColumnFeatures& features = info.GetColumnFeaturesVerified(columnId);
        sb << "column " << columnId << " index=" << info.GetColumnIndexVerified(columnId) << " name=" << features.GetColumnName()
           << " field=" << info.GetColumnFieldVerified(columnId)->ToString(true) << " type=" << features.GetTypeInfo().GetTypeId()
           << " pk=" << (features.GetPKColumnIndex() ? ::ToString(*features.GetPKColumnIndex()) : "none")
           << " nullable=" << features.GetIsNullable() << " minmax=" << features.GetNeedMinMax() << " sorted=" << features.GetIsSorted()
           << " storage=" << features.GetOperator()->GetStorageId()
           << " serializer=" << features.GetSerializer().SerializeToProto().ShortDebugString()
           << " accessor=" << features.GetDataAccessorConstructor().SerializeToProto().ShortDebugString()
           << " default=" << features.GetDefaultValue().SerializeToProto().ShortDebugString() << " " << features.DebugString() << "\n";
    }
    for (const std::string& name : info.GetColumnSTLNames(true)) {
        sb << "by name " << name << " id=" << info.GetColumnIdOptional(name).value_or(0)
           << " index=" << info.GetColumnIndexOptional(name).value_or(Max<ui32>()) << "\n";
    }
    sb << "pk=" << info.GetPrimaryKey()->ToString(true) << " ids=" << JoinSeq(",", info.GetPKColumnIds()) << "\n";
    std::map<ui32, TString> indexes;
    for (const auto& [indexId, meta] : info.GetIndexes()) {
        indexes.emplace(indexId, meta.SerializeToProto().ShortDebugString());
    }
    for (const auto& [indexId, proto] : indexes) {
        sb << "index " << indexId << " " << proto << "\n";
    }
    NStorageOptimizer::IOptimizerPlannerConstructor::TProto planner;
    info.GetCompactionPlannerConstructor()->SerializeToProto(planner);
    NDataAccessorControl::IManagerConstructor::TProto metadataManager;
    info.GetMetadataManagerConstructor()->SerializeToProto(metadataManager);
    sb << "options actualization=" << info.GetSchemeNeedActualization() << " reader=" << info.GetScanReaderPolicyName().value_or("none")
       << " deduplication=" << (info.GetDeduplicationEnabled() ? ::ToString(*info.GetDeduplicationEnabled()) : "none")
       << " cache_after_write=" << info.GetCacheBlobsAfterWrite() << " tiering_actualization=" << info.GetTieringNeedActualization()
       << " build_indexes=" << (info.GetInsertOptions().BuildIndexesEnabled ? ::ToString(*info.GetInsertOptions().BuildIndexesEnabled) : "none")
       << " build_indexes_min_bytes="
       << (info.GetInsertOptions().BuildIndexesMinBlobBytes ? ::ToString(*info.GetInsertOptions().BuildIndexesMinBlobBytes) : "none")
       << " planner=" << planner.ShortDebugString() << " metadata=" << metadataManager.ShortDebugString() << "\n";
    sb << "default serializer=" << info.GetDefaultSerializer().SerializeToProto().ShortDebugString() << "\n";
    sb << "storages=" << JoinSeq(",", info.GetUsedStorageIds("")) << "\n";
    return sb;
>>>>>>> b8b9df599a1 (Tiering actualization (#57209))
}

NKikimrSchemeOp::TColumnTableSchema MakeSchemaV2AfterDropAndAdd() {
    NKikimrSchemeOp::TColumnTableSchema proto;
    const std::vector<NArrow::NTest::TTestColumn> columns = {
        NArrow::NTest::TTestColumn("timestamp", NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)).SetNullable(false),
        NArrow::NTest::TTestColumn("value", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)),
        NArrow::NTest::TTestColumn("added", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)),
    };
    *proto.MutableColumns()->Add() = columns[0].CreateColumn(1);
    *proto.MutableColumns()->Add() = columns[1].CreateColumn(2);
    *proto.MutableColumns()->Add() = columns[2].CreateColumn(4);
    proto.AddKeyColumnNames("timestamp");
    proto.SetVersion(2);
    return proto;
}

}   // namespace

Y_UNIT_TEST_SUITE(TIndexInfoSchemaDiff) {
    Y_UNIT_TEST(GetColumnsConsistentAfterDropAndAddColumns) {
        auto storages = TTestStoragesManager::GetInstance();
        auto cache = std::make_shared<TSchemaObjectsCache>();

        const auto schemaV1 = MakeSchemaV1();
        auto indexInfoV1 = TIndexInfo::BuildFromProto(1, schemaV1, storages, cache);
        UNIT_ASSERT(indexInfoV1);

        const auto schemaV2 = MakeSchemaV2AfterDropAndAdd();
        const auto diffProto = TSchemaDiffView::MakeSchemasDiff(schemaV1, schemaV2);
        UNIT_ASSERT(diffProto.DropColumnsSize() > 0);
        UNIT_ASSERT(diffProto.UpsertColumnsSize() > 0);

        auto indexInfoV2 = TIndexInfo::BuildFromProto(diffProto, *indexInfoV1, storages, cache);
        UNIT_ASSERT(indexInfoV2);

        const auto& columnsMap = indexInfoV2->GetColumns();
        UNIT_ASSERT(!columnsMap.contains(3));
        UNIT_ASSERT(columnsMap.contains(4));
        UNIT_ASSERT_VALUES_EQUAL(columnsMap.at(4).first, "added");

<<<<<<< HEAD
        for (const ui32 columnId : indexInfoV2->GetColumnIds(false)) {
            auto it = columnsMap.find(columnId);
            UNIT_ASSERT_C(it != columnsMap.end(), TStringBuilder() << "missing column_id=" << columnId);
            UNIT_ASSERT(!it->second.first.empty());
        }
=======
    // a new column of a dropped one's name gets a new id
    Y_UNIT_TEST(DropColumnAndAddItsName) {
        TSchemaHistory history;
        history.NextVersion().DropColumn(2).AddColumn(4, "value", NScheme::NTypeIds::Uint64);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(ChangeColumnCompression) {
        TSchemaHistory history;
        history.NextVersion().SetCompression(2, NKikimrSchemeOp::ColumnCodecZSTD, 5);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(ChangeColumnCompressionLevel) {
        TSchemaHistory history;
        history.NextVersion().SetCompression(2, NKikimrSchemeOp::ColumnCodecZSTD, 5);
        history.NextVersion().SetCompression(2, NKikimrSchemeOp::ColumnCodecZSTD, 9);
        CheckDiffsBuildSchemas(history);
    }

    // a changed key column keeps its key order
    Y_UNIT_TEST(ChangeKeyColumnCompression) {
        TSchemaHistory history;
        history.NextVersion().SetCompression(KeyColumnId, NKikimrSchemeOp::ColumnCodecZSTD);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(ChangeColumnStorage) {
        TSchemaHistory history;
        history.NextVersion().SetStorage(3, IStoragesManager::MemoryStorageId);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(ChangeColumnDefault) {
        TSchemaHistory history;
        history.NextVersion().AddColumn(4, "with_default", NScheme::NTypeIds::Uint64);
        history.NextVersion().SetDefault(4, 7);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(ChangeColumnToSparsed) {
        TSchemaHistory history;
        history.NextVersion().SetAccessor(2, NArrow::NAccessor::TGlobalConst::SparsedDataAccessorName);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(ChangeColumnToDictionary) {
        TSchemaHistory history;
        history.NextVersion().SetAccessor(2, NArrow::NAccessor::TGlobalConst::DictionaryAccessorName);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(AddSubColumnsColumn) {
        TSchemaHistory history;
        history.NextVersion().AddColumn(4, "json", NScheme::NTypeIds::JsonDocument).SetSubColumns(4, 10);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(ChangeSubColumnsSettings) {
        TSchemaHistory history;
        history.NextVersion().AddColumn(4, "json", NScheme::NTypeIds::JsonDocument).SetSubColumns(4, 10);
        history.NextVersion().SetSubColumns(4, 20);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(AddIndex) {
        TSchemaHistory history;
        history.NextVersion().AddMaxIndex(10, "max_ts", KeyColumnId);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(DropIndex) {
        TSchemaHistory history;
        history.NextVersion().AddMaxIndex(10, "max_ts", KeyColumnId);
        history.NextVersion().DropIndex(10);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(ChangeIndex) {
        TSchemaHistory history;
        history.NextVersion().AddBloomIndex(10, "bloom_value", 2, 0.1);
        history.NextVersion().DropIndex(10).AddBloomIndex(10, "bloom_value", 2, 0.01);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(ChangeDefaultCompression) {
        TSchemaHistory history;
        history.NextVersion().SetDefaultCompression(NKikimrSchemeOp::ColumnCodecZSTD);
        CheckDiffsBuildSchemas(history);
    }

    // a new column without a serializer of its own takes the table's default
    Y_UNIT_TEST(AddColumnToTableWithDefaultCompression) {
        TSchemaHistory history;
        history.SetDefaultCompression(NKikimrSchemeOp::ColumnCodecZSTD);
        history.NextVersion().AddColumn(4, "added", NScheme::NTypeIds::Uint64);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(ChangeOptions) {
        TSchemaHistory history;
        auto& options = history.NextVersion().MutableOptions();
        options.SetSchemeNeedActualization(true);
        options.SetScanReaderPolicyName("SIMPLE");
        options.SetDeduplicationEnabled(true);
        options.SetCacheBlobsAfterWrite(true);
        options.SetTieringNeedActualization(true);
        options.MutableInsertOptions()->SetBuildIndexesEnabled(true);
        options.MutableInsertOptions()->SetBuildIndexesMinBlobBytes(1024);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(ChangeTieringActualizationOption) {
        TSchemaHistory history;
        history.NextVersion().MutableOptions().SetTieringNeedActualization(true);
        history.NextVersion().MutableOptions().SetTieringNeedActualization(false);
        CheckDiffsBuildSchemas(history);
        CheckMergedDiffBuildsSchema(history);
    }

    Y_UNIT_TEST(MergedAddAndDropColumn) {
        TSchemaHistory history;
        history.NextVersion().AddColumn(4, "added", NScheme::NTypeIds::Uint64);
        history.NextVersion().DropColumn(4);
        CheckMergedDiffBuildsSchema(history);
    }

    Y_UNIT_TEST(MergedAddAndChangeColumn) {
        TSchemaHistory history;
        history.NextVersion().AddColumn(4, "added", NScheme::NTypeIds::Uint64);
        history.NextVersion().SetCompression(4, NKikimrSchemeOp::ColumnCodecZSTD).SetDefault(4, 7);
        CheckMergedDiffBuildsSchema(history);
    }

    Y_UNIT_TEST(MergedAddAndDropIndex) {
        TSchemaHistory history;
        history.NextVersion().AddMaxIndex(10, "max_ts", KeyColumnId);
        history.NextVersion().DropIndex(10);
        CheckMergedDiffBuildsSchema(history);
    }

    Y_UNIT_TEST(MergedColumnAndIndexChanges) {
        TSchemaHistory history;
        history.NextVersion().DropColumn(2).AddColumn(4, "json", NScheme::NTypeIds::JsonDocument).SetSubColumns(4, 10);
        history.NextVersion().AddMaxIndex(10, "max_ts", KeyColumnId).SetCompression(3, NKikimrSchemeOp::ColumnCodecZSTD);
        history.NextVersion().SetDefaultCompression(NKikimrSchemeOp::ColumnCodecZSTD).AddColumn(5, "a_first", NScheme::NTypeIds::Uint64);
        CheckMergedDiffBuildsSchema(history);
>>>>>>> b8b9df599a1 (Tiering actualization (#57209))
    }
}

}   // namespace NKikimr::NOlap::NTest
