#pragma once

#include "column_features.h"
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

class TSchemaObjectsCache {
private:
    THashMap<TString, std::shared_ptr<arrow::Field>> Fields;
    THashMap<TString, std::shared_ptr<TColumnFeatures>> ColumnFeatures;
    THashSet<TString> StringsCache;
    mutable ui64 AcceptionFieldsCount = 0;
    mutable ui64 AcceptionFeaturesCount = 0;

public:
    const TString& GetStringCache(const TString& original) {
        auto it = StringsCache.find(original);
        if (it == StringsCache.end()) {
            it = StringsCache.emplace(original).first;
        }
        return *it;
    }

    void RegisterField(const TString& fingerprint, const std::shared_ptr<arrow::Field>& f) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "register_field")("fp", fingerprint)("f", f->ToString());
        AFL_VERIFY(Fields.emplace(fingerprint, f).second);
    }
    void RegisterColumnFeatures(const TString& fingerprint, const std::shared_ptr<TColumnFeatures>& f) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "register_column_features")("fp", fingerprint)("info", f->DebugString());
        AFL_VERIFY(ColumnFeatures.emplace(fingerprint, f).second);
    }
    std::shared_ptr<arrow::Field> GetField(const TString& fingerprint) const {
        auto it = Fields.find(fingerprint);
        if (it == Fields.end()) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "get_field_miss")("fp", fingerprint)("count", Fields.size())(
                "acc", AcceptionFieldsCount);
            return nullptr;
        }
        if (++AcceptionFieldsCount % 1000 == 0) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "get_field_accept")("fp", fingerprint)("count", Fields.size())(
                "acc", AcceptionFieldsCount);
        }
        return it->second;
    }
    template <class TConstructor>
    TConclusion<std::shared_ptr<TColumnFeatures>> GetOrCreateColumnFeatures(const TString& fingerprint, const TConstructor& constructor) {
        auto it = ColumnFeatures.find(fingerprint);
        if (it == ColumnFeatures.end()) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "get_column_features_miss")("fp", UrlEscapeRet(fingerprint))(
                "count", ColumnFeatures.size())("acc", AcceptionFeaturesCount);
            TConclusion<std::shared_ptr<TColumnFeatures>> resultConclusion = constructor();
            if (resultConclusion.IsFail()) {
                return resultConclusion;
            }
            it = ColumnFeatures.emplace(fingerprint, resultConclusion.DetachResult()).first;
            AFL_VERIFY(it->second);
        } else {
            if (++AcceptionFeaturesCount % 1000 == 0) {
                AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "get_column_features_accept")("fp", UrlEscapeRet(fingerprint))(
                    "count", ColumnFeatures.size())("acc", AcceptionFeaturesCount);
            }
        }
        return it->second;
    }
};

/// Column engine index description in terms of tablet's local table.
/// We have to use YDB types for keys here.
struct TIndexInfo: public IIndexInfo {
private:
    using TColumns = THashMap<ui32, NTable::TColumn>;
    friend class TPortionInfo;

    class TNameInfo {
    private:
        YDB_READONLY_DEF(TString, Name);
        YDB_READONLY(ui32, ColumnId, 0);
        YDB_READONLY(ui32, ColumnIdx, 0);

    public:
        struct TNameComparator {
            bool operator()(const TNameInfo& l, const TNameInfo& r) const {
                return l.Name < r.Name;
            };
        };

        TNameInfo(const TString& name, const ui32 columnId, const ui32 columnIdx)
            : Name(name)
            , ColumnId(columnId)
            , ColumnIdx(columnIdx) {
        }

        static std::vector<TNameInfo> BuildColumnNames(const TColumns& columns) {
            std::vector<TNameInfo> result;
            for (auto&& i : columns) {
                result.emplace_back(TNameInfo(i.second.Name, i.first, 0));
            }
            {
                const auto pred = [](const TNameInfo& l, const TNameInfo& r) {
                    return l.ColumnId < r.ColumnId;
                };
                std::sort(result.begin(), result.end(), pred);
            }
            ui32 idx = 0;
            for (auto&& i : result) {
                i.ColumnIdx = idx++;
            }
            std::sort(result.begin(), result.end(), TNameInfo::TNameComparator());
            return result;
        }
    };

    std::vector<TNameInfo> ColumnNames;
    std::vector<ui32> PKColumnIds;
    std::vector<TNameTypeInfo> PKColumns;

    std::vector<std::shared_ptr<TColumnFeatures>> ColumnFeatures;
    THashMap<ui32, NIndexes::TIndexMetaContainer> Indexes;

    bool SchemeNeedActualization = false;
    std::shared_ptr<NStorageOptimizer::IOptimizerPlannerConstructor> CompactionPlannerConstructor;
    bool ExternalGuaranteeExclusivePK = false;

    ui64 Version = 0;
    TString Name;
    std::vector<ui32> SchemaColumnIds;
    std::vector<ui32> SchemaColumnIdsWithSpecials;
    std::shared_ptr<NArrow::TSchemaLite> SchemaWithSpecials;
    std::shared_ptr<NArrow::TSchemaLite> Schema;
    std::shared_ptr<arrow::Schema> PrimaryKey;
    NArrow::NSerialization::TSerializerContainer DefaultSerializer = NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();

    TIndexInfo(const TString& name);

    class TSchemaDiffView {
    private:
        ui64 Version = 0;
        const NKikimrSchemeOp::TColumnTableSchemeOptions* SchemaOptions = nullptr;
        const NKikimrSchemeOp::TCompressionOptions* CompressionOptions = nullptr;
        std::map<ui32, const NKikimrSchemeOp::TOlapColumnDescription*> ModifiedColumns;
        std::map<ui32, const NKikimrSchemeOp::TOlapIndexDescription*> ModifiedIndexes;

    public:
        TSchemaDiffView() = default;

        const NKikimrSchemeOp::TColumnTableSchemeOptions& GetSchemaOptions() const {
            AFL_VERIFY(SchemaOptions);
            return *SchemaOptions;
        }
        const NKikimrSchemeOp::TCompressionOptions* GetCompressionOptions() const {
            return CompressionOptions;
        }
        const std::map<ui32, const NKikimrSchemeOp::TOlapColumnDescription*>& GetModifiedColumns() const {
            return ModifiedColumns;
        }
        const std::map<ui32, const NKikimrSchemeOp::TOlapIndexDescription*>& GetModifiedIndexes() const {
            return ModifiedIndexes;
        }

        ui64 GetVersion() const {
            AFL_VERIFY(Version);
            return Version;
        };

        TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSchemaDiff& proto) {
            SchemaOptions = &proto.GetOptions();
            Version = proto.GetVersion();
            if (proto.HasDefaultCompression()) {
                CompressionOptions = &proto.GetDefaultCompression();
            }
            for (auto&& i : proto.GetUpsertColumns()) {
                AFL_VERIFY(ModifiedColumns.emplace(i.GetId(), &i).second);
            }
            for (auto&& i : proto.GetDropColumns()) {
                AFL_VERIFY(ModifiedColumns.emplace(i, nullptr).second);
            }
            for (auto&& i : proto.GetUpsertIndexes()) {
                AFL_VERIFY(ModifiedIndexes.emplace(i.GetId(), &i).second);
            }
            for (auto&& i : proto.GetDropIndexes()) {
                AFL_VERIFY(ModifiedIndexes.emplace(i, nullptr).second);
            }
            return TConclusionStatus::Success();
        }
    };

    static std::shared_ptr<arrow::Field> BuildArrowField(const NTable::TColumn& column, const std::shared_ptr<TSchemaObjectsCache>& cache) {
        auto arrowType = NArrow::GetArrowType(column.PType);
        AFL_VERIFY(arrowType.ok());
        auto f = std::make_shared<arrow::Field>(column.Name, arrowType.ValueUnsafe(), !column.NotNull);
        if (cache) {
            auto fFound = cache->GetField(f->ToString(true));
            if (!fFound) {
                cache->RegisterField(f->ToString(true), f);
                return f;
            } else {
                return fFound;
            }
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
        const std::shared_ptr<TSchemaObjectsCache>& cache) {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        const auto addFromOriginal = [&](const ui32 index) {
            AFL_VERIFY(index < original.SchemaColumnIdsWithSpecials.size());
            const ui32 originalColId = original.SchemaColumnIdsWithSpecials[index];
            SchemaColumnIdsWithSpecials.emplace_back(originalColId);
            if (!IIndexInfo::IsSpecialColumn(originalColId)) {
                AFL_VERIFY(index < original.SchemaColumnIds.size());
                SchemaColumnIds.emplace_back(originalColId);
                ColumnNames.emplace_back(TNameInfo(original.ColumnNames[index].GetName(), originalColId, ColumnNames.size()));
                fields.emplace_back(original.Schema->field(index));
            }
        };

        const auto addFromDiff = [&](const NKikimrSchemeOp::TOlapColumnDescription& col) {
            const ui32 colId = col.GetId();
            AFL_VERIFY(!IIndexInfo::IsSpecialColumn(colId));
            SchemaColumnIdsWithSpecials.emplace_back(colId);
            SchemaColumnIds.emplace_back(colId);
            ColumnNames.emplace_back(TNameInfo(col.GetName(), colId, ColumnNames.size()));
            auto tableCol = BuildColumnFromProto(col, cache);
            fields.emplace_back(BuildArrowField(tableCol, cache));
        };

        {
            auto it = diff.GetModifiedColumns().begin();
            ui32 i = 0;
            while (i < original.SchemaColumnIdsWithSpecials.size() || it != diff.GetModifiedColumns().end()) {
                AFL_VERIFY(i != original.SchemaColumnIdsWithSpecials.size());
                const ui32 originalColId = original.SchemaColumnIdsWithSpecials[i];

                if (it == diff.GetModifiedColumns().end() || originalColId < it->first) {
                    addFromOriginal(i);
                    ++i;
                } else if (it->first == originalColId) {
                    if (it->second) {
                        addFromDiff(*it->second);
                    }
                    ++it;
                    ++i;
                } else if (it->first < originalColId) {
                    AFL_VERIFY(it->second);
                    addFromDiff(*it->second);
                    ++it;
                }
            }
            Schema = std::make_shared<NArrow::TSchemaLite>(fields);
            IIndexInfo::AddSpecialFields(fields);
            SchemaWithSpecials = std::make_shared<NArrow::TSchemaLite>(fields);
            std::sort(ColumnNames.begin(), ColumnNames.end(), TNameInfo::TNameComparator());
            PKColumnIds = original.PKColumnIds;
            PKColumns = original.PKColumns;
        }
        {
            auto it = diff.GetModifiedColumns().begin();
            ui32 i = 0;
            while (i < original.SchemaColumnIdsWithSpecials.size() || it != diff.GetModifiedColumns().end()) {
                AFL_VERIFY(i != original.SchemaColumnIdsWithSpecials.size());
                const ui32 originalColId = original.SchemaColumnIdsWithSpecials[i];

                if (it == diff.GetModifiedColumns().end() || originalColId < it->first) {
                    ColumnFeatures.emplace_back(original.ColumnFeatures[i]);
                    ++i;
                } else if (it->first == originalColId) {
                    if (it->second) {
                        auto tableCol = BuildColumnFromProto(*it->second, cache);
                        tableCol.KeyOrder = original.ColumnFeatures[i]->GetPKColumnIndex().value_or(Max<NTable::TPos>());
                        ColumnFeatures.emplace_back(CreateColumnFeatures(tableCol, *it->second, operators, cache).DetachResult());
                    }
                    ++it;
                    ++i;
                } else if (it->first < originalColId) {
                    AFL_VERIFY(it->second);
                    auto tableCol = BuildColumnFromProto(*it->second, cache);
                    ColumnFeatures.emplace_back(CreateColumnFeatures(tableCol, *it->second, operators, cache).DetachResult());
                    ++it;
                }
            }
        }
        {
            TMemoryProfileGuard g("TIndexInfo::ApplyDiff::Indexes");
            Indexes = original.Indexes;
            for (auto&& i : diff.GetModifiedIndexes()) {
                if (!i.second) {
                    AFL_VERIFY(Indexes.erase(i.first));
                } else {
                    auto it = Indexes.find(i.first);
                    AFL_VERIFY(it != Indexes.end());
                    NIndexes::TIndexMetaContainer meta;
                    AFL_VERIFY(meta.DeserializeFromProto(*i.second));
                    it->second = std::move(meta);
                }
            }
        }

        DeserializeOptionsFromProto(diff.GetSchemaOptions());
        Version = diff.GetVersion();
        Name = original.Name;
        PrimaryKey = original.PrimaryKey;
        if (diff.GetCompressionOptions()) {
            DeserializeDefaultCompressionFromProto(*diff.GetCompressionOptions());
        }
    }

    void DeserializeOptionsFromProto(const NKikimrSchemeOp::TColumnTableSchemeOptions& optionsProto);
    bool DeserializeDefaultCompressionFromProto(const NKikimrSchemeOp::TCompressionOptions& compressionProto);
    TConclusion<std::shared_ptr<TColumnFeatures>> CreateColumnFeatures(const NTable::TColumn& col,
        const NKikimrSchemeOp::TOlapColumnDescription& colProto, const std::shared_ptr<IStoragesManager>& operators,
        const std::shared_ptr<TSchemaObjectsCache>& cache) const;

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

public:
    NSplitter::TEntityGroups GetEntityGroupsByStorageId(const TString& specialTier, const IStoragesManager& storages) const;
    std::optional<ui32> GetPKColumnIndexByIndexVerified(const ui32 columnIndex) const {
        AFL_VERIFY(columnIndex < ColumnFeatures.size());
        return ColumnFeatures[columnIndex]->GetPKColumnIndex();
    }

    static std::vector<std::shared_ptr<arrow::Field>> MakeArrowFields(
        const NTable::TScheme::TTableSchema::TColumns& columns, const std::vector<ui32>& ids, const std::shared_ptr<TSchemaObjectsCache>& cache);

    std::shared_ptr<NStorageOptimizer::IOptimizerPlannerConstructor> GetCompactionPlannerConstructor() const;
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

    bool GetExternalGuaranteeExclusivePK() const {
        return ExternalGuaranteeExclusivePK;
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
        std::set<TString> result;
        if (portionTierName && portionTierName != IStoragesManager::DefaultStorageId) {
            result.emplace(portionTierName);
        } else {
            for (auto&& i : ColumnFeatures) {
                result.emplace(i->GetOperator()->GetStorageId());
            }
        }
        return result;
    }

    const THashMap<ui32, NIndexes::TIndexMetaContainer>& GetIndexes() const {
        return Indexes;
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "("
           << "version=" << Version << ";"
           << "name=" << Name << ";"
           << ")";
        for (auto&& i : ColumnFeatures) {
            sb << i->GetColumnName() << ":" << i->DebugString() << ";";
        }
        return sb;
    }

    static TIndexInfo BuildDefault() {
        TIndexInfo result("dummy");
        return result;
    }

    static NKikimrSchemeOp::TColumnTableSchemaDiff MakeSchemasDiff(
        const NKikimrSchemeOp::TColumnTableSchema& current, const NKikimrSchemeOp::TColumnTableSchema& next) {
        NKikimrSchemeOp::TColumnTableSchemaDiff result;
        result.SetVersion(next.GetVersion());
        *result.MutableDefaultCompression() = next.GetDefaultCompression();
        *result.MutableOptions() = next.GetOptions();

        {
            THashMap<ui32, NKikimrSchemeOp::TOlapColumnDescription> nextIds;
            for (auto&& i : next.GetColumns()) {
                AFL_VERIFY(nextIds.emplace(i.GetId(), i).second);
            }
            for (auto&& i : current.GetColumns()) {
                auto it = nextIds.find(i.GetId());
                if (it == nextIds.end()) {
                    result.AddDropColumns(i.GetId());
                } else if (it->second.SerializeAsString() != i.SerializeAsString()) {
                    *result.AddUpsertColumns() = it->second;
                }
            }
        }
        {
            THashMap<ui32, NKikimrSchemeOp::TOlapIndexDescription> nextIds;
            for (auto&& i : next.GetIndexes()) {
                AFL_VERIFY(nextIds.emplace(i.GetId(), i).second);
            }
            for (auto&& i : current.GetIndexes()) {
                auto it = nextIds.find(i.GetId());
                if (it == nextIds.end()) {
                    result.AddDropIndexes(i.GetId());
                } else if (it->second.SerializeAsString() != i.SerializeAsString()) {
                    *result.AddUpsertIndexes() = it->second;
                }
            }
        }
        return result;
    }

    static TIndexInfo BuildDefault(
        const std::shared_ptr<IStoragesManager>& operators, const TColumns& columns, const std::vector<TString>& pkNames) {
        TIndexInfo result = BuildDefault();
        result.ColumnNames = TNameInfo::BuildColumnNames(columns);
        for (auto&& i : pkNames) {
            const ui32 columnId = result.GetColumnIdVerified(i);
            result.PKColumnIds.emplace_back(columnId);
        }
        result.SetAllKeys(operators, columns);
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
    std::vector<std::string> GetColumnSTLNames(const std::vector<ui32>& ids) const;
    const std::vector<ui32>& GetColumnIds(const bool withSpecial = true) const;
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

    const std::shared_ptr<NArrow::TSchemaLite>& ArrowSchema() const;
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
