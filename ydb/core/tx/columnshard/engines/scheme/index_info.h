#pragma once

#include "column_features.h"
#include "tier_info.h"

#include "abstract/index_info.h"
#include "indexes/abstract/meta.h"
#include "statistics/abstract/operator.h"
#include "statistics/abstract/common.h"

#include <ydb/core/tx/columnshard/common/snapshot.h>

#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/common/scalars.h>
#include <ydb/core/tx/columnshard/common/portion.h>
#include <ydb/core/formats/arrow/dictionary/object.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/transformer/abstract.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace arrow {
    class Array;
    class Field;
    class Schema;
}

namespace NKikimr::NOlap {
namespace NStorageOptimizer {
class IOptimizerPlannerConstructor;
}
class TPortionInfoWithBlobs;
struct TInsertedData;
class TSnapshotColumnInfo;
class ISnapshotSchema;
using TNameTypeInfo = std::pair<TString, NScheme::TTypeInfo>;

/// Column engine index description in terms of tablet's local table.
/// We have to use YDB types for keys here.
struct TIndexInfo : public NTable::TScheme::TTableSchema, public IIndexInfo {
private:
    THashMap<ui32, TColumnFeatures> ColumnFeatures;
    THashMap<ui32, std::shared_ptr<arrow::Field>> ArrowColumnByColumnIdCache;
    THashMap<ui32, NIndexes::TIndexMetaContainer> Indexes;
    std::map<TString, NStatistics::TOperatorContainer> StatisticsByName;
    TIndexInfo(const TString& name);
    bool SchemeNeedActualization = false;
    std::shared_ptr<NStorageOptimizer::IOptimizerPlannerConstructor> CompactionPlannerConstructor;
    bool ExternalGuaranteeExclusivePK = false;
    bool DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema, const std::shared_ptr<IStoragesManager>& operators);
    TColumnFeatures& GetOrCreateColumnFeatures(const ui32 columnId) const;
    void InitializeCaches(const std::shared_ptr<IStoragesManager>& operators);
public:
    std::shared_ptr<NStorageOptimizer::IOptimizerPlannerConstructor> GetCompactionPlannerConstructor() const;

    bool IsNullableVerified(const std::string& fName) const {
        auto it = Columns.find(GetColumnIdVerified(fName));
        AFL_VERIFY(it != Columns.end());
        return !it->second.NotNull;
    }

    bool IsNullableVerified(const ui32 colId) const {
        auto it = Columns.find(colId);
        AFL_VERIFY(it != Columns.end());
        return !it->second.NotNull;
    }

    std::shared_ptr<arrow::Scalar> GetColumnDefaultWriteValueVerified(const std::string& colName) const;

    std::shared_ptr<arrow::Scalar> GetColumnDefaultReadValueVerified(const std::string& colName) const {
        auto& features = GetColumnFeaturesVerified(GetColumnIdVerified(colName));
        return features.GetDefaultReadValue();
    }

    bool GetExternalGuaranteeExclusivePK() const {
        return ExternalGuaranteeExclusivePK;
    }

    const TColumnFeatures& GetColumnFeaturesVerified(const ui32 columnId) const {
        auto it = ColumnFeatures.find(columnId);
        AFL_VERIFY(it != ColumnFeatures.end());
        return it->second;
    }

    NSplitter::TEntityGroups GetEntityGroupsByStorageId(const TString& specialTier, const IStoragesManager& storages) const;

    bool GetSchemeNeedActualization() const {
        return SchemeNeedActualization;
    }

    std::set<TString> GetUsedStorageIds(const TString& portionTierName) const {
        std::set<TString> result;
        if (portionTierName && portionTierName != IStoragesManager::DefaultStorageId) {
            result.emplace(portionTierName);
        } else {
            for (auto&& i : ColumnFeatures) {
                result.emplace(i.second.GetOperator()->GetStorageId());
            }
        }
        return result;
    }

    std::vector<std::shared_ptr<IPortionDataChunk>> MakeEmptyChunks(const ui32 columnId, const std::vector<ui32>& pages, const TSimpleColumnInfo& columnInfo) const;

    const std::map<TString, NStatistics::TOperatorContainer>& GetStatisticsByName() const {
        return StatisticsByName;
    }

    NStatistics::TOperatorContainer GetStatistics(const NStatistics::TIdentifier& id) const {
        for (auto&& i : StatisticsByName) {
            if (i.second->GetIdentifier() == id) {
                return i.second;
            }
        }
        return NStatistics::TOperatorContainer();
    }

    const THashMap<ui32, NIndexes::TIndexMetaContainer>& GetIndexes() const {
        return Indexes;
    }

    const TString& GetIndexStorageId(const ui32 indexId) const {
        auto it = Indexes.find(indexId);
        AFL_VERIFY(it != Indexes.end());
        return it->second->GetStorageId();
    }

    const TString& GetColumnStorageId(const ui32 columnId, const TString& specialTier) const {
        if (specialTier && specialTier != IStoragesManager::DefaultStorageId) {
            return specialTier;
        } else {
            auto it = ColumnFeatures.find(columnId);
            AFL_VERIFY(it != ColumnFeatures.end());
            return it->second.GetOperator()->GetStorageId();
        }
    }

    const TString& GetEntityStorageId(const ui32 entityId, const TString& specialTier) const {
        auto it = Indexes.find(entityId);
        if (it != Indexes.end()) {
            return it->second->GetStorageId();
        }
        return GetColumnStorageId(entityId, specialTier);
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "("
            << "version=" << Version << ";"
            << "name=" << Name << ";"
            << ")";
        for (auto&& i : ColumnFeatures) {
            sb << GetColumnName(i.first) << ":" << i.second.DebugString() << ";";
        }
        return sb;
    }

public:
    static TIndexInfo BuildDefault() {
        TIndexInfo result("dummy");
        return result;
    }

    std::vector<std::shared_ptr<IPortionDataChunk>> ActualizeColumnData(const std::vector<std::shared_ptr<IPortionDataChunk>>& source, const TIndexInfo& sourceIndexInfo, const ui32 columnId) const {
        auto itCurrent = ColumnFeatures.find(columnId);
        auto itPred = sourceIndexInfo.ColumnFeatures.find(columnId);
        AFL_VERIFY(itCurrent != ColumnFeatures.end());
        AFL_VERIFY(itPred != sourceIndexInfo.ColumnFeatures.end());
        return itCurrent->second.ActualizeColumnData(source, itPred->second);
    }

    static std::optional<TIndexInfo> BuildFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema, const std::shared_ptr<IStoragesManager>& operators);

    bool HasColumnId(const ui32 columnId) const {
        return ColumnFeatures.contains(columnId);
    }

    bool HasColumnName(const std::string& columnName) const {
        return !!GetColumnIdOptional(columnName);
    }

    bool HasIndexId(const ui32 indexId) const {
        return Indexes.contains(indexId);
    }

    std::shared_ptr<arrow::Field> GetColumnFieldOptional(const ui32 columnId) const;
    std::shared_ptr<arrow::Field> GetColumnFieldVerified(const ui32 columnId) const;
    std::shared_ptr<arrow::Schema> GetColumnSchema(const ui32 columnId) const;
    std::shared_ptr<arrow::Schema> GetColumnsSchema(const std::set<ui32>& columnIds) const;
    TColumnSaver GetColumnSaver(const ui32 columnId) const;
    virtual std::shared_ptr<TColumnLoader> GetColumnLoaderOptional(const ui32 columnId) const override;
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

    std::optional<TString> GetIndexNameOptional(const ui32 indexId) const {
        auto meta = GetIndexOptional(indexId);
        if (!meta) {
            return {};
        }
        return meta->GetIndexName();
    }

    void AppendIndexes(THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& originalData) const {
        for (auto&& i : Indexes) {
            std::shared_ptr<IPortionDataChunk> chunk = i.second->BuildIndex(i.first, originalData, *this);
            AFL_VERIFY(originalData.emplace(i.first, std::vector<std::shared_ptr<IPortionDataChunk>>({chunk})).second);
        }
    }

    void AppendIndex(THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& originalData, const ui32 indexId) const {
        auto it = Indexes.find(indexId);
        AFL_VERIFY(it != Indexes.end());
        std::shared_ptr<IPortionDataChunk> chunk = it->second->BuildIndex(indexId, originalData, *this);
        AFL_VERIFY(originalData.emplace(indexId, std::vector<std::shared_ptr<IPortionDataChunk>>({chunk})).second);
    }

    /// Returns an id of the column located by name. The name should exists in the schema.
    ui32 GetColumnIdVerified(const std::string& name) const;
    ui32 GetColumnId(const std::string& name) const {
        return GetColumnIdVerified(name);
    }
    std::set<ui32> GetColumnIdsVerified(const std::set<TString>& names) const {
        std::set<ui32> result;
        for (auto&& i : names) {
            AFL_VERIFY(result.emplace(GetColumnIdVerified(i)).second);
        }
        return result;
    }
    std::optional<ui32> GetColumnIdOptional(const std::string& name) const;

    /// Returns a name of the column located by id.
    TString GetColumnName(ui32 id, bool required = true) const;

    /// Returns names of columns defined by the specific ids.
    std::vector<TString> GetColumnNames(const std::vector<ui32>& ids) const;
    std::vector<std::string> GetColumnSTLNames(const std::vector<ui32>& ids) const;
    std::vector<ui32> GetColumnIds(const bool withSpecial = true) const;
    std::vector<ui32> GetEntityIds() const {
        auto result = GetColumnIds();
        for (auto&& i : Indexes) {
            result.emplace_back(i.first);
        }
        return result;
    }

    /// Returns info of columns defined by specific ids.
    std::vector<TNameTypeInfo> GetColumns(const std::vector<ui32>& ids) const;

    /// Traditional Primary Key (includes uniqueness, search and sorting logic)
    std::vector<TNameTypeInfo> GetPrimaryKeyColumns() const {
        return GetColumns(KeyColumns);
    }

    /// Returns id of the first column of the primary key.
    ui32 GetPKFirstColumnId() const {
        Y_ABORT_UNLESS(KeyColumns.size());
        return KeyColumns[0];
    }

    const std::shared_ptr<arrow::Schema>& GetReplaceKey() const { return PrimaryKey; }
    const std::shared_ptr<arrow::Schema>& GetPrimaryKey() const { return PrimaryKey; }

    /// Initializes sorting, replace, index and extended keys.
    void SetAllKeys(const std::shared_ptr<IStoragesManager>& operators);

    void CheckTtlColumn(const TString& ttlColumn) const {
        Y_ABORT_UNLESS(!ttlColumn.empty());
        Y_ABORT_UNLESS(MinMaxIdxColumnsIds.contains(GetColumnId(ttlColumn)));
    }

    std::vector<ui32> GetColumnIds(const std::vector<TString>& columnNames) const;

    std::shared_ptr<arrow::Schema> ArrowSchema() const;
    std::shared_ptr<arrow::Schema> ArrowSchemaWithSpecials() const;
    std::shared_ptr<arrow::Schema> AddColumns(const std::shared_ptr<arrow::Schema>& schema,
                                              const std::vector<TString>& columns) const;

    std::shared_ptr<arrow::Field> ArrowColumnFieldOptional(const ui32 columnId) const;
    std::shared_ptr<arrow::Field> ArrowColumnFieldVerified(const ui32 columnId) const;

    const THashSet<TString>& GetRequiredColumns() const {
        return RequiredColumns;
    }

    const THashSet<ui32>& GetMinMaxIdxColumns() const {
        return MinMaxIdxColumnsIds;
    }

    bool AllowTtlOverColumn(const TString& name) const;

    /// Returns whether the sorting keys defined.
    bool IsSorted() const { return true; }
    bool IsSortedColumn(const ui32 columnId) const { return GetPKFirstColumnId() == columnId; }

    ui64 GetVersion() const {
        return Version;
    }

    bool CheckCompatible(const TIndexInfo& other) const;
    NArrow::NSerialization::TSerializerContainer GetDefaultSerializer() const {
        return DefaultSerializer;
    }

private:
    ui64 Version = 0;
    TString Name;
    std::shared_ptr<arrow::Schema> Schema;
    std::shared_ptr<arrow::Schema> SchemaWithSpecials;
    std::shared_ptr<arrow::Schema> PrimaryKey;
    THashSet<TString> RequiredColumns;
    THashSet<ui32> MinMaxIdxColumnsIds;
    NArrow::NSerialization::TSerializerContainer DefaultSerializer = NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
};

std::shared_ptr<arrow::Schema> MakeArrowSchema(const NTable::TScheme::TTableSchema::TColumns& columns, const std::vector<ui32>& ids, const bool withSpecials = false);

/// Extracts columns with the specific ids from the schema.
std::vector<TNameTypeInfo> GetColumns(const NTable::TScheme::TTableSchema& tableSchema, const std::vector<ui32>& ids);

} // namespace NKikimr::NOlap
