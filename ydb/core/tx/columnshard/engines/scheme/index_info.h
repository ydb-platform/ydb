#pragma once

#include "column_features.h"
#include "objects_cache.h"
#include "schema_diff.h"
#include "tier_info.h"

#include "abstract/index_info.h"
#include "indexes/abstract/meta.h"

#include <ydb/core/formats/arrow/dictionary/object.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/tx/columnshard/common/scalars.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/data_accessor/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/column_ids.h>

#include <ydb/library/formats/arrow/transformer/abstract.h>

#include <library/cpp/string_utils/quote/quote.h>

namespace arrow {
class Array;
class Field;
class Schema;
}   // namespace arrow

namespace NKikimr::NOlap {
class TPortionInfo;
namespace NIndexes::NMax {
class TIndexMeta;
}

namespace NIndexes::NCountMinSketch {
class TIndexMeta;
}

namespace NStorageOptimizer {
class IOptimizerPlannerConstructor;
}
class TPortionInfoWithBlobs;
class TSnapshotColumnInfo;
class ISnapshotSchema;
using TNameTypeInfo = std::pair<TString, NScheme::TTypeInfo>;

/// Column engine index description in terms of tablet's local table.
/// We have to use YDB types for keys here.
struct TIndexInfo: public IIndexInfo {
private:
    using TColumns = THashMap<ui32, NTable::TColumn>;
    friend class TPortionInfo;
    friend class TPortionDataAccessor;

    std::vector<ui32> ColumnIdxSortedByName;
    std::vector<ui32> PKColumnIds;
    std::vector<TNameTypeInfo> PKColumns;

    std::vector<std::shared_ptr<TColumnFeatures>> ColumnFeatures;
    THashMap<ui32, NIndexes::TIndexMetaContainer> Indexes;
    std::shared_ptr<std::set<TString>> UsedStorageIds;

    bool SchemeNeedActualization = false;
    std::shared_ptr<NStorageOptimizer::IOptimizerPlannerConstructor> CompactionPlannerConstructor;
    std::shared_ptr<NDataAccessorControl::IManagerConstructor> MetadataManagerConstructor;
    std::optional<TString> ScanReaderPolicyName;

    ui64 Version = 0;
    std::vector<ui32> SchemaColumnIdsWithSpecials;
    std::shared_ptr<NArrow::TSchemaLite> SchemaWithSpecials;
    std::shared_ptr<arrow::Schema> PrimaryKey;
    NArrow::NSerialization::TSerializerContainer DefaultSerializer = NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();

    TIndexInfo() = default;

    static std::shared_ptr<arrow::Field> BuildArrowField(const NTable::TColumn& column, const std::shared_ptr<TSchemaObjectsCache>& cache) {
        auto arrowType = NArrow::GetArrowType(column.PType);
        AFL_VERIFY(arrowType.ok());
        auto f = std::make_shared<arrow::Field>(column.Name, arrowType.ValueUnsafe(), !column.NotNull);
        if (cache) {
            return cache->GetOrInsertField(f);
        } else {
            return f;
        }
    }

    static NTable::TColumn BuildColumnFromProto(
        const NKikimrSchemeOp::TOlapColumnDescription& col, const std::shared_ptr<TSchemaObjectsCache>& cache) {
        const ui32 id = col.GetId();
        const TString& name = cache->GetStringCache(col.GetName());
        const bool notNull = col.HasNotNull() ? col.GetNotNull() : false;
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(col.GetTypeId(), col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
        return NTable::TColumn(name, id, typeInfoMod.TypeInfo, cache->GetStringCache(typeInfoMod.TypeMod), notNull);
    }

    TIndexInfo(const TIndexInfo& original, const TSchemaDiffView& diff, const std::shared_ptr<IStoragesManager>& operators,
        const std::shared_ptr<TSchemaObjectsCache>& cache);

    void DeserializeOptionsFromProto(const NKikimrSchemeOp::TColumnTableSchemeOptions& optionsProto);
    bool DeserializeDefaultCompressionFromProto(const NKikimrSchemeOp::TCompressionOptions& compressionProto);
    TConclusion<std::shared_ptr<TColumnFeatures>> CreateColumnFeatures(const NTable::TColumn& col,
        const NKikimrSchemeOp::TOlapColumnDescription& colProto, const std::shared_ptr<IStoragesManager>& operators,
        const std::shared_ptr<TSchemaObjectsCache>& cache) const;

    void Validate() const;
    void Precalculate();
    void BuildColumnIndexByName();

    bool DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema, const std::shared_ptr<IStoragesManager>& operators,
        const std::shared_ptr<TSchemaObjectsCache>& cache);
    void InitializeCaches(const std::shared_ptr<IStoragesManager>& operators, const THashMap<ui32, NTable::TColumn>& columns,
        const std::shared_ptr<TSchemaObjectsCache>& cache, const bool withColumnFeatures = true);
    std::shared_ptr<TColumnFeatures> BuildDefaultColumnFeatures(
        const ui32 columnId, const THashMap<ui32, NTable::TColumn>& columns, const std::shared_ptr<IStoragesManager>& operators) const;
    std::shared_ptr<TColumnFeatures> BuildDefaultColumnFeatures(
        const NTable::TColumn& column, const std::shared_ptr<IStoragesManager>& operators) const;

    const TString& GetIndexStorageId(const ui32 indexId) const {
        auto it = Indexes.find(indexId);
        AFL_VERIFY(it != Indexes.end());
        return it->second->GetStorageId();
    }

    const TString& GetColumnStorageId(const ui32 columnId, const TString& specialTier) const {
        if (specialTier && specialTier != IStoragesManager::DefaultStorageId) {
            return specialTier;
        } else {
            return GetColumnFeaturesVerified(columnId).GetOperator()->GetStorageId();
        }
    }

    const TString& GetEntityStorageId(const ui32 entityId, const TString& specialTier) const {
        auto it = Indexes.find(entityId);
        if (it != Indexes.end()) {
            return it->second->GetStorageId();
        }
        return GetColumnStorageId(entityId, specialTier);
    }

    void SetAllKeys(const std::shared_ptr<IStoragesManager>& operators, const THashMap<ui32, NTable::TColumn>& columns);

    bool CompareColumnIdxByName(const ui32 lhs, const ui32 rhs) const {
        AFL_VERIFY(lhs < ColumnFeatures.size());
        AFL_VERIFY(rhs < ColumnFeatures.size());
        return ColumnFeatures[lhs]->GetColumnName() < ColumnFeatures[rhs]->GetColumnName();
    }

public:
    NSplitter::TEntityGroups GetEntityGroupsByStorageId(const TString& specialTier, const IStoragesManager& storages) const;
    std::optional<ui32> GetPKColumnIndexByIndexVerified(const ui32 columnIndex) const {
        AFL_VERIFY(columnIndex < ColumnFeatures.size());
        return ColumnFeatures[columnIndex]->GetPKColumnIndex();
    }

    static std::vector<std::shared_ptr<arrow::Field>> MakeArrowFields(
        const NTable::TScheme::TTableSchema::TColumns& columns, const std::vector<ui32>& ids, const std::shared_ptr<TSchemaObjectsCache>& cache);

    const std::shared_ptr<NStorageOptimizer::IOptimizerPlannerConstructor>& GetCompactionPlannerConstructor() const;
    const std::shared_ptr<NDataAccessorControl::IManagerConstructor>& GetMetadataManagerConstructor() const {
        AFL_VERIFY(MetadataManagerConstructor);
        return MetadataManagerConstructor;
    }
    bool IsNullableVerifiedByIndex(const ui32 colIndex) const {
        AFL_VERIFY(colIndex < ColumnFeatures.size());
        return ColumnFeatures[colIndex]->GetIsNullable();
    }

    bool IsNullableVerified(const ui32 colId) const {
        return GetColumnFeaturesVerified(colId).GetIsNullable();
    }

    std::shared_ptr<arrow::Scalar> GetColumnExternalDefaultValueVerified(const std::string& colName) const;
    std::shared_ptr<arrow::Scalar> GetColumnExternalDefaultValueVerified(const ui32 colId) const;
    std::shared_ptr<arrow::Scalar> GetColumnExternalDefaultValueByIndexVerified(const ui32 colIndex) const;

    const std::optional<TString>& GetScanReaderPolicyName() const {
        return ScanReaderPolicyName;
    }

    const TColumnFeatures& GetColumnFeaturesVerified(const ui32 columnId) const {
        return *ColumnFeatures[GetColumnIndexVerified(columnId)];
    }

    const std::shared_ptr<TColumnFeatures>& GetColumnFeaturesOptional(const ui32 columnId) const {
        if (auto idx = GetColumnIndexOptional(columnId)) {
            return ColumnFeatures[*idx];
        } else {
            return Default<std::shared_ptr<TColumnFeatures>>();
        }
    }

    bool GetSchemeNeedActualization() const {
        return SchemeNeedActualization;
    }

    std::set<TString> GetUsedStorageIds(const TString& portionTierName) const {
        if (portionTierName && portionTierName != IStoragesManager::DefaultStorageId) {
            return { portionTierName };
        } else {
            return *UsedStorageIds;
        }
    }

    const THashMap<ui32, NIndexes::TIndexMetaContainer>& GetIndexes() const {
        return Indexes;
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "("
           << "version=" << Version << ";"
           << ")";
        for (auto&& i : ColumnFeatures) {
            sb << i->GetColumnName() << ":" << i->DebugString() << ";";
        }
        return sb;
    }

    static TIndexInfo BuildDefault();

    static TIndexInfo BuildDefault(const std::shared_ptr<IStoragesManager>& operators, const TColumns& columns, const std::vector<ui32>& pkIds) {
        TIndexInfo result = BuildDefault();
        result.PKColumnIds = pkIds;
        result.SetAllKeys(operators, columns);
        result.Validate();
        return result;
    }

    std::vector<std::shared_ptr<IPortionDataChunk>> ActualizeColumnData(
        const std::vector<std::shared_ptr<IPortionDataChunk>>& source, const TIndexInfo& sourceIndexInfo, const ui32 columnId) const {
        return GetColumnFeaturesVerified(columnId).ActualizeColumnData(source, sourceIndexInfo.GetColumnFeaturesVerified(columnId));
    }

    static std::optional<TIndexInfo> BuildFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema,
        const std::shared_ptr<IStoragesManager>& operators, const std::shared_ptr<TSchemaObjectsCache>& cache);
    static std::optional<TIndexInfo> BuildFromProto(const NKikimrSchemeOp::TColumnTableSchemaDiff& schema, const TIndexInfo& prevSchema,
        const std::shared_ptr<IStoragesManager>& operators, const std::shared_ptr<TSchemaObjectsCache>& cache);

    bool HasColumnId(const ui32 columnId) const {
        return !!GetColumnIndexOptional(columnId);
    }

    bool HasColumnName(const std::string& columnName) const {
        return !!GetColumnIdOptional(columnName);
    }

    bool HasIndexId(const ui32 indexId) const {
        return Indexes.contains(indexId);
    }

    bool HasIndexes(const std::set<ui32>& indexIds) const {
        for (auto&& i : indexIds) {
            if (!Indexes.contains(i)) {
                return false;
            }
        }
        return true;
    }

    std::optional<ui32> GetColumnIndexOptional(const ui32 id) const;
    ui32 GetColumnIndexVerified(const ui32 id) const {
        auto result = GetColumnIndexOptional(id);
        AFL_VERIFY(result);
        return *result;
    }
    std::shared_ptr<arrow::Field> GetColumnFieldOptional(const ui32 columnId) const;
    std::shared_ptr<arrow::Field> GetColumnFieldVerified(const ui32 columnId) const;
    std::shared_ptr<arrow::Schema> GetColumnSchema(const ui32 columnId) const;
    std::shared_ptr<arrow::Schema> GetColumnsSchema(const std::set<ui32>& columnIds) const;
    TColumnSaver GetColumnSaver(const ui32 columnId) const;
    virtual const std::shared_ptr<TColumnLoader>& GetColumnLoaderOptional(const ui32 columnId) const override;
    std::optional<std::string> GetColumnNameOptional(const ui32 columnId) const {
        auto f = GetColumnFieldOptional(columnId);
        if (!f) {
            return {};
        }
        return f->name();
    }

    NIndexes::TIndexMetaContainer GetIndexOptional(const ui32 indexId) const {
        auto it = Indexes.find(indexId);
        if (it == Indexes.end()) {
            return NIndexes::TIndexMetaContainer();
        }
        return it->second;
    }

    NIndexes::TIndexMetaContainer GetIndexVerified(const ui32 indexId) const {
        auto it = Indexes.find(indexId);
        AFL_VERIFY(it != Indexes.end());
        return it->second;
    }

    std::optional<TString> GetIndexNameOptional(const ui32 indexId) const {
        auto meta = GetIndexOptional(indexId);
        if (!meta) {
            return {};
        }
        return meta->GetIndexName();
    }

    class TSecondaryData {
    private:
        using TStorageData = THashMap<ui32, std::shared_ptr<IPortionDataChunk>>;
        YDB_ACCESSOR_DEF(TStorageData, SecondaryInplaceData);
        using TPrimaryStorageData = THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>;
        YDB_ACCESSOR_DEF(TPrimaryStorageData, ExternalData);

    public:
        TSecondaryData() = default;
    };

    [[nodiscard]] TConclusion<TSecondaryData> AppendIndexes(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& primaryData,
        const std::shared_ptr<IStoragesManager>& operators) const {
        TSecondaryData result;
        result.MutableExternalData() = primaryData;
        for (auto&& i : Indexes) {
            auto conclusion = AppendIndex(primaryData, i.first, operators, result);
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        return result;
    }

    std::shared_ptr<NIndexes::NMax::TIndexMeta> GetIndexMetaMax(const ui32 columnId) const;
    std::shared_ptr<NIndexes::NCountMinSketch::TIndexMeta> GetIndexMetaCountMinSketch(const std::set<ui32>& columnIds) const;

    [[nodiscard]] TConclusionStatus AppendIndex(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& originalData,
        const ui32 indexId, const std::shared_ptr<IStoragesManager>& operators, TSecondaryData& result) const;

    /// Returns an id of the column located by name. The name should exists in the schema.
    ui32 GetColumnIdVerified(const std::string& name) const;
    std::set<ui32> GetColumnIdsVerified(const std::set<TString>& names) const {
        std::set<ui32> result;
        for (auto&& i : names) {
            AFL_VERIFY(result.emplace(GetColumnIdVerified(i)).second);
        }
        return result;
    }
    std::optional<ui32> GetColumnIdOptional(const std::string& name) const;
    std::optional<ui32> GetColumnIndexOptional(const std::string& name) const;

    /// Returns a name of the column located by id.
    TString GetColumnName(const ui32 id, bool required = true) const;

    /// Returns names of columns defined by the specific ids.
    std::vector<TString> GetColumnNames(const std::vector<ui32>& ids) const;
    std::vector<std::string> GetColumnSTLNames(const bool withSpecial = true) const;
    TColumnIdsView GetColumnIds(const bool withSpecial = true) const;
    ui32 GetColumnIdByIndexVerified(const ui32 index) const {
        AFL_VERIFY(index < SchemaColumnIdsWithSpecials.size());
        return SchemaColumnIdsWithSpecials[index];
    }
    const std::vector<ui32>& GetPKColumnIds() const {
        AFL_VERIFY(PKColumnIds.size());
        return PKColumnIds;
    }
    std::vector<ui32> GetEntityIds() const;

    /// Traditional Primary Key (includes uniqueness, search and sorting logic)
    const std::vector<TNameTypeInfo>& GetPrimaryKeyColumns() const {
        return PKColumns;
    }

    /// Returns id of the first column of the primary key.
    ui32 GetPKFirstColumnId() const {
        Y_ABORT_UNLESS(PKColumnIds.size());
        return PKColumnIds[0];
    }

    const std::shared_ptr<arrow::Schema>& GetReplaceKey() const {
        return PrimaryKey;
    }
    const std::shared_ptr<arrow::Schema>& GetPrimaryKey() const {
        return PrimaryKey;
    }

    std::vector<ui32> GetColumnIds(const std::vector<TString>& columnNames) const;

    NArrow::TSchemaLiteView ArrowSchema() const;
    const std::shared_ptr<NArrow::TSchemaLite>& ArrowSchemaWithSpecials() const;

    bool AllowTtlOverColumn(const TString& name) const;

    /// Returns whether the sorting keys defined.
    bool IsSorted() const {
        return true;
    }
    bool IsSortedColumn(const ui32 columnId) const {
        return GetPKFirstColumnId() == columnId;
    }

    ui64 GetVersion() const {
        return Version;
    }

    bool CheckCompatible(const TIndexInfo& other) const;
    NArrow::NSerialization::TSerializerContainer GetDefaultSerializer() const {
        return DefaultSerializer;
    }
};

std::shared_ptr<arrow::Schema> MakeArrowSchema(const NTable::TScheme::TTableSchema::TColumns& columns, const std::vector<ui32>& ids,
    const std::shared_ptr<TSchemaObjectsCache>& cache = nullptr);

/// Extracts columns with the specific ids from the schema.
std::vector<TNameTypeInfo> GetColumns(const NTable::TScheme::TTableSchema& tableSchema, const std::vector<ui32>& ids);

}   // namespace NKikimr::NOlap
