#include <ydb/core/formats/arrow/accessor/common/const.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/schema_diff.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/join.h>

namespace NKikimr::NOlap::NTest {

namespace {

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
       << " cache_after_write=" << info.GetCacheBlobsAfterWrite()
       << " build_indexes=" << (info.GetInsertOptions().BuildIndexesEnabled ? ::ToString(*info.GetInsertOptions().BuildIndexesEnabled) : "none")
       << " build_indexes_min_bytes="
       << (info.GetInsertOptions().BuildIndexesMinBlobBytes ? ::ToString(*info.GetInsertOptions().BuildIndexesMinBlobBytes) : "none")
       << " planner=" << planner.ShortDebugString() << " metadata=" << metadataManager.ShortDebugString() << "\n";
    sb << "default serializer=" << info.GetDefaultSerializer().SerializeToProto().ShortDebugString() << "\n";
    sb << "storages=" << JoinSeq(",", info.GetUsedStorageIds("")) << "\n";
    return sb;
}

NKikimrSchemeOp::TOlapColumnDescription MakeColumn(const ui32 id, const TString& name, const NScheme::TTypeId typeId) {
    return NArrow::NTest::TTestColumn(name, NScheme::TTypeInfo(typeId)).CreateColumn(id);
}

// the schema history of one table: each change starts a new version from the last one
class TSchemaHistory {
private:
    std::vector<NKikimrSchemeOp::TColumnTableSchema> Versions;

    NKikimrSchemeOp::TColumnTableSchema& Last() {
        return Versions.back();
    }

    NKikimrSchemeOp::TOlapColumnDescription& MutableColumn(const ui32 id) {
        for (auto& column : *Last().MutableColumns()) {
            if (column.GetId() == id) {
                return column;
            }
        }
        UNIT_FAIL("no column " << id);
        return *Last().MutableColumns()->begin();
    }

public:
    // a key column and two value columns
    TSchemaHistory() {
        NKikimrSchemeOp::TColumnTableSchema& schema = Versions.emplace_back();
        *schema.AddColumns() = MakeColumn(KeyColumnId, "timestamp", NScheme::NTypeIds::Timestamp);
        schema.MutableColumns(0)->SetNotNull(true);
        *schema.AddColumns() = MakeColumn(2, "value", NScheme::NTypeIds::Utf8);
        *schema.AddColumns() = MakeColumn(3, "payload", NScheme::NTypeIds::String);
        schema.AddKeyColumnNames("timestamp");
        schema.SetVersion(1);
    }

    const std::vector<NKikimrSchemeOp::TColumnTableSchema>& GetVersions() const {
        return Versions;
    }

    TSchemaHistory& NextVersion() {
        NKikimrSchemeOp::TColumnTableSchema next = Last();
        next.SetVersion(next.GetVersion() + 1);
        Versions.emplace_back(std::move(next));
        return *this;
    }

    TSchemaHistory& AddColumn(const ui32 id, const TString& name, const NScheme::TTypeId typeId) {
        *Last().AddColumns() = MakeColumn(id, name, typeId);
        return *this;
    }

    TSchemaHistory& DropColumn(const ui32 id) {
        auto* columns = Last().MutableColumns();
        for (auto it = columns->begin(); it != columns->end(); ++it) {
            if (it->GetId() == id) {
                columns->erase(it);
                return *this;
            }
        }
        UNIT_FAIL("no column " << id);
        return *this;
    }

    TSchemaHistory& SetCompression(const ui32 columnId, const NKikimrSchemeOp::EColumnCodec codec, const std::optional<i32> level = {}) {
        auto* serializer = MutableColumn(columnId).MutableSerializer();
        serializer->SetClassName("ARROW_SERIALIZER");
        serializer->MutableArrowCompression()->SetCodec(codec);
        if (level) {
            serializer->MutableArrowCompression()->SetLevel(*level);
        }
        return *this;
    }

    TSchemaHistory& SetAccessor(const ui32 columnId, const TString& className) {
        MutableColumn(columnId).MutableDataAccessorConstructor()->SetClassName(className);
        return *this;
    }

    TSchemaHistory& SetSubColumns(const ui32 columnId, const ui32 columnsLimit) {
        auto* accessor = MutableColumn(columnId).MutableDataAccessorConstructor();
        accessor->SetClassName(NArrow::NAccessor::TGlobalConst::SubColumnsDataAccessorName);
        accessor->MutableSubColumns()->MutableSettings()->SetColumnsLimit(columnsLimit);
        return *this;
    }

    TSchemaHistory& SetDefault(const ui32 columnId, const ui64 value) {
        MutableColumn(columnId).MutableDefaultValue()->MutableScalar()->SetUint64(value);
        return *this;
    }

    TSchemaHistory& SetStorage(const ui32 columnId, const TString& storageId) {
        MutableColumn(columnId).SetStorageId(storageId);
        return *this;
    }

    TSchemaHistory& AddMaxIndex(const ui32 indexId, const TString& name, const ui32 columnId) {
        auto* index = Last().AddIndexes();
        index->SetId(indexId);
        index->SetName(name);
        index->SetClassName("MAX");
        index->MutableMaxIndex()->SetColumnId(columnId);
        return *this;
    }

    TSchemaHistory& AddBloomIndex(const ui32 indexId, const TString& name, const ui32 columnId, const double falsePositive) {
        auto* index = Last().AddIndexes();
        index->SetId(indexId);
        index->SetName(name);
        index->SetClassName("BLOOM_FILTER");
        index->MutableBloomFilter()->AddColumnIds(columnId);
        index->MutableBloomFilter()->SetFalsePositiveProbability(falsePositive);
        return *this;
    }

    TSchemaHistory& DropIndex(const ui32 indexId) {
        auto* indexes = Last().MutableIndexes();
        for (auto it = indexes->begin(); it != indexes->end(); ++it) {
            if (it->GetId() == indexId) {
                indexes->erase(it);
                return *this;
            }
        }
        UNIT_FAIL("no index " << indexId);
        return *this;
    }

    TSchemaHistory& SetDefaultCompression(const NKikimrSchemeOp::EColumnCodec codec) {
        Last().MutableDefaultCompression()->SetCodec(codec);
        return *this;
    }

    NKikimrSchemeOp::TColumnTableSchemeOptions& MutableOptions() {
        return *Last().MutableOptions();
    }
};

TIndexInfo BuildFromSchema(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    // a cache of its own, so that nothing built from the diffs is reused
    auto result = TIndexInfo::BuildFromProto(PresetId, schema, TTestStoragesManager::GetInstance(), std::make_shared<TSchemaObjectsCache>());
    UNIT_ASSERT(result);
    return std::move(*result);
}

// every version is built from the previous one and its diff, as the shard loads it, and from its full schema; the two must be the same
void CheckDiffsBuildSchemas(const TSchemaHistory& history) {
    const auto& versions = history.GetVersions();
    const auto storages = TTestStoragesManager::GetInstance();
    const auto cache = std::make_shared<TSchemaObjectsCache>();
    auto fromDiffs = TIndexInfo::BuildFromProto(PresetId, versions.front(), storages, cache);
    UNIT_ASSERT(fromDiffs);
    for (ui32 i = 1; i < versions.size(); ++i) {
        fromDiffs = TIndexInfo::BuildFromProto(TSchemaDiffView::MakeSchemasDiff(versions[i - 1], versions[i]), *fromDiffs, storages, cache);
        UNIT_ASSERT(fromDiffs);
        UNIT_ASSERT_VALUES_EQUAL_C(Describe(*fromDiffs), Describe(BuildFromSchema(versions[i])), "version " << versions[i].GetVersion());
    }
}

// the versions after the first one are collapsed into one diff, as schema cleanup stores it, and applied to the first one
void CheckMergedDiffBuildsSchema(const TSchemaHistory& history) {
    const auto& versions = history.GetVersions();
    UNIT_ASSERT(versions.size() > 2);
    std::vector<NKikimrTxColumnShard::TSchemaPresetVersionInfo> rows;
    for (ui32 i = 1; i < versions.size(); ++i) {
        *rows.emplace_back().MutableDiff() = TSchemaDiffView::MakeSchemasDiff(versions[i - 1], versions[i]);
    }
    const NKikimrTxColumnShard::TSchemaPresetVersionInfo merged = TSchemaDiffView::Merge(rows);
    UNIT_ASSERT(merged.HasDiff());

    const auto storages = TTestStoragesManager::GetInstance();
    const auto cache = std::make_shared<TSchemaObjectsCache>();
    auto first = TIndexInfo::BuildFromProto(PresetId, versions.front(), storages, cache);
    UNIT_ASSERT(first);
    auto fromMerged = TIndexInfo::BuildFromProto(merged.GetDiff(), *first, storages, cache);
    UNIT_ASSERT(fromMerged);
    UNIT_ASSERT_VALUES_EQUAL(Describe(*fromMerged), Describe(BuildFromSchema(versions.back())));
}

}   // namespace

Y_UNIT_TEST_SUITE(TIndexInfoSchemaDiff) {
    Y_UNIT_TEST(AddColumn) {
        TSchemaHistory history;
        history.NextVersion().AddColumn(4, "added", NScheme::NTypeIds::Uint64);
        CheckDiffsBuildSchemas(history);
    }

    // a name sorted before the existing ones moves every name position
    Y_UNIT_TEST(AddColumnSortedFirstByName) {
        TSchemaHistory history;
        history.NextVersion().AddColumn(4, "a_first", NScheme::NTypeIds::Uint64);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(DropColumn) {
        TSchemaHistory history;
        history.NextVersion().DropColumn(2);
        CheckDiffsBuildSchemas(history);
    }

    Y_UNIT_TEST(DropLastColumn) {
        TSchemaHistory history;
        history.NextVersion().DropColumn(3);
        CheckDiffsBuildSchemas(history);
    }

    // more columns added than dropped moves the special columns
    Y_UNIT_TEST(DropColumnAndAddTwo) {
        TSchemaHistory history;
        history.NextVersion().DropColumn(2).AddColumn(4, "added", NScheme::NTypeIds::Uint64).AddColumn(5, "also_added", NScheme::NTypeIds::Utf8);
        CheckDiffsBuildSchemas(history);
    }

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
        options.MutableInsertOptions()->SetBuildIndexesEnabled(true);
        options.MutableInsertOptions()->SetBuildIndexesMinBlobBytes(1024);
        CheckDiffsBuildSchemas(history);
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
    }
}

}   // namespace NKikimr::NOlap::NTest
