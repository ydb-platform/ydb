#include "index_info.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>
#include <ydb/core/formats/arrow/transformer/dictionary.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NOlap {

TIndexInfo::TIndexInfo(const TString& name)
    : Name(name) {
    CompactionPlannerConstructor = NStorageOptimizer::IOptimizerPlannerConstructor::BuildDefault();
}

bool TIndexInfo::CheckCompatible(const TIndexInfo& other) const {
    if (!other.GetPrimaryKey()->Equals(PrimaryKey)) {
        return false;
    }
    return true;
}

ui32 TIndexInfo::GetColumnIdVerified(const std::string& name) const {
    auto id = GetColumnIdOptional(name);
    Y_ABORT_UNLESS(!!id, "undefined column %s", name.data());
    return *id;
}

std::optional<ui32> TIndexInfo::GetColumnIdOptional(const std::string& name) const {
    const auto pred = [](const TNameInfo& item, const std::string& value) {
        return item.GetName() < value;
    };
    auto it = std::lower_bound(ColumnNames.begin(), ColumnNames.end(), name, pred);
    if (it != ColumnNames.end() && it->GetName() == name) {
        return it->GetColumnId();
    }
    return IIndexInfo::GetColumnIdOptional(name);
}

std::optional<ui32> TIndexInfo::GetColumnIndexOptional(const std::string& name) const {
    const auto pred = [](const TNameInfo& item, const std::string& value) {
        return item.GetName() < value;
    };
    auto it = std::lower_bound(ColumnNames.begin(), ColumnNames.end(), name, pred);
    if (it != ColumnNames.end() && it->GetName() == name) {
        return it->GetColumnIdx();
    }
    return IIndexInfo::GetColumnIndexOptional(name, ColumnNames.size());
}

TString TIndexInfo::GetColumnName(const ui32 id, bool required) const {
    const auto& f = GetColumnFeaturesOptional(id);
    if (!f) {
        AFL_VERIFY(!required);
        return "";
    } else {
        return f->GetColumnName();
    }
}

const std::vector<ui32>& TIndexInfo::GetColumnIds(const bool withSpecial) const {
    if (withSpecial) {
        return SchemaColumnIdsWithSpecials;
    } else {
        return SchemaColumnIds;
    }
}

std::vector<TString> TIndexInfo::GetColumnNames(const std::vector<ui32>& ids) const {
    std::vector<TString> out;
    out.reserve(ids.size());
    for (ui32 id : ids) {
        out.push_back(GetColumnName(id));
    }
    return out;
}

std::vector<std::string> TIndexInfo::GetColumnSTLNames(const std::vector<ui32>& ids) const {
    std::vector<std::string> out;
    out.reserve(ids.size());
    for (ui32 id : ids) {
        out.push_back(GetColumnName(id));
    }
    return out;
}

const std::shared_ptr<NArrow::TSchemaLite>& TIndexInfo::ArrowSchema() const {
    AFL_VERIFY(Schema);
    return Schema;
}

const std::shared_ptr<NArrow::TSchemaLite>& TIndexInfo::ArrowSchemaWithSpecials() const {
    AFL_VERIFY(SchemaWithSpecials);
    return SchemaWithSpecials;
}

std::vector<ui32> TIndexInfo::GetColumnIds(const std::vector<TString>& columnNames) const {
    std::vector<ui32> ids;
    ids.reserve(columnNames.size());
    for (auto& name : columnNames) {
        auto columnId = GetColumnIdOptional(name);
        if (!columnId) {
            return {};
        }
        ids.emplace_back(*columnId);
    }
    return ids;
}

void TIndexInfo::SetAllKeys(const std::shared_ptr<IStoragesManager>& operators, const THashMap<ui32, NTable::TColumn>& columns) {
    /// @note Setting replace and sorting key to PK we are able to:
    /// * apply REPLACE by MergeSort
    /// * apply PK predicate before REPLACE
    PrimaryKey = MakeArrowSchema(columns, PKColumnIds, nullptr);

    AFL_VERIFY(PKColumns.empty());
    for (auto&& i : PKColumnIds) {
        auto it = columns.find(i);
        AFL_VERIFY(it != columns.end());
        PKColumns.emplace_back(TNameTypeInfo(it->second.Name, it->second.PType));
    }

    for (const auto& [colId, column] : columns) {
        if (NArrow::IsPrimitiveYqlType(column.PType)) {
            MinMaxIdxColumnsIds.insert(colId);
        }
    }
    MinMaxIdxColumnsIds.insert(GetPKFirstColumnId());
    if (!Schema) {
        AFL_VERIFY(!SchemaWithSpecials);
        InitializeCaches(operators, columns, nullptr);
    }
}

TColumnSaver TIndexInfo::GetColumnSaver(const ui32 columnId) const {
    return GetColumnFeaturesVerified(columnId).GetColumnSaver();
}

std::shared_ptr<TColumnLoader> TIndexInfo::GetColumnLoaderOptional(const ui32 columnId) const {
    const auto& cFeatures = GetColumnFeaturesOptional(columnId);
    if (!cFeatures) {
        return nullptr;
    } else {
        return cFeatures->GetLoader();
    }
}

std::optional<ui32> TIndexInfo::GetColumnIndexOptional(const ui32 id) const {
    auto it = std::lower_bound(SchemaColumnIdsWithSpecials.begin(), SchemaColumnIdsWithSpecials.end(), id);
    if (it == SchemaColumnIdsWithSpecials.end() || *it != id) {
        return std::nullopt;
    } else {
        return it - SchemaColumnIdsWithSpecials.begin();
    }
}

std::shared_ptr<arrow::Field> TIndexInfo::GetColumnFieldOptional(const ui32 columnId) const {
    const std::optional<ui32> index = GetColumnIndexOptional(columnId);
    if (!index) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("column_id", columnId)("event", "incorrect_column_id");
        return nullptr;
    }
    return ArrowSchemaWithSpecials()->GetFieldByIndexVerified(*index);
}

std::shared_ptr<arrow::Field> TIndexInfo::GetColumnFieldVerified(const ui32 columnId) const {
    auto result = GetColumnFieldOptional(columnId);
    AFL_VERIFY(!!result)("column_id", columnId);
    return result;
}

std::shared_ptr<arrow::Schema> TIndexInfo::GetColumnsSchema(const std::set<ui32>& columnIds) const {
    Y_ABORT_UNLESS(columnIds.size());
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto&& i : columnIds) {
        fields.emplace_back(GetColumnFieldVerified(i));
    }
    return std::make_shared<arrow::Schema>(fields);
}

std::shared_ptr<arrow::Schema> TIndexInfo::GetColumnSchema(const ui32 columnId) const {
    return GetColumnsSchema({ columnId });
}

bool TIndexInfo::DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema, const std::shared_ptr<IStoragesManager>& operators,
    const std::shared_ptr<TSchemaObjectsCache>& cache) {
    if (schema.GetEngine() != NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_index_info")("reason", "incorrect_engine_in_schema");
        return false;
    }
    AFL_VERIFY(cache);

    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::Optimizer");
        SchemeNeedActualization = schema.GetOptions().GetSchemeNeedActualization();
        ExternalGuaranteeExclusivePK = schema.GetOptions().GetExternalGuaranteeExclusivePK();
        if (schema.GetOptions().HasCompactionPlannerConstructor()) {
            auto container =
                NStorageOptimizer::TOptimizerPlannerConstructorContainer::BuildFromProto(schema.GetOptions().GetCompactionPlannerConstructor());
            CompactionPlannerConstructor = container.DetachResult().GetObjectPtrVerified();
        } else {
            AFL_VERIFY(!!CompactionPlannerConstructor);
        }
    }

    if (schema.HasDefaultCompression()) {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::Serializer");
        NArrow::NSerialization::TSerializerContainer container;
        if (!container.DeserializeFromProto(schema.GetDefaultCompression())) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_index_info")("reason", "cannot_parse_default_serializer");
            return false;
        }
        DefaultSerializer = container;
    }
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::Indexes");
        for (const auto& idx : schema.GetIndexes()) {
            NIndexes::TIndexMetaContainer meta;
            AFL_VERIFY(meta.DeserializeFromProto(idx));
            Indexes.emplace(meta->GetIndexId(), meta);
        }
    }
    THashMap<ui32, NTable::TColumn> columns;
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::Columns");
        for (const auto& col : schema.GetColumns()) {
            const ui32 id = col.GetId();
            const TString& name = cache->GetStringCache(col.GetName());
            const bool notNull = col.HasNotNull() ? col.GetNotNull() : false;
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(col.GetTypeId(), col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
            columns[id] = NTable::TColumn(name, id, typeInfoMod.TypeInfo, cache->GetStringCache(typeInfoMod.TypeMod), notNull);
        }
        ColumnNames = TNameInfo::BuildColumnNames(columns);
    }
    PKColumnIds.clear();
    for (const auto& keyName : schema.GetKeyColumnNames()) {
        const ui32 columnId = GetColumnIdVerified(keyName);
        auto it = columns.find(columnId);
        AFL_VERIFY(it != columns.end());
        it->second.KeyOrder = PKColumnIds.size();
        PKColumnIds.push_back(columnId);
    }
    InitializeCaches(operators, columns, cache, false);
    SetAllKeys(operators, columns);
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::Columns::Features");
        for (const auto& col : schema.GetColumns()) {
            THashMap<ui32, std::shared_ptr<TColumnFeatures>> it;
            const TString fingerprint = cache ? ("C:" + col.SerializeAsString()) : Default<TString>();
            const auto createPred = [&]() -> TConclusion<std::shared_ptr<TColumnFeatures>> {
                auto f = BuildDefaultColumnFeatures(col.GetId(), columns, operators);
                auto parsed = f->DeserializeFromProto(col, operators);
                if (parsed.IsFail()) {
                    return parsed;
                }
                return f;
            };
            auto fConclusion = cache->GetOrCreateColumnFeatures(fingerprint, createPred);
            if (fConclusion.IsFail()) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_column_feature")("reason", fConclusion.GetErrorMessage());
                return false;
            }
            ColumnFeatures.emplace_back(fConclusion.DetachResult());
        }
        for (auto&& cId : GetSystemColumnIds()) {
            THashMap<ui32, std::shared_ptr<TColumnFeatures>> it;
            const TString fingerprint = "SC:" + ::ToString(cId);
            const auto createPred = [&]() -> TConclusion<std::shared_ptr<TColumnFeatures>> {
                return BuildDefaultColumnFeatures(cId, {}, operators);
            };
            auto fConclusion = cache->GetOrCreateColumnFeatures(fingerprint, createPred);
            ColumnFeatures.emplace_back(fConclusion.DetachResult());
        }
        const auto pred = [](const std::shared_ptr<TColumnFeatures>& l, const std::shared_ptr<TColumnFeatures>& r) {
            return l->GetColumnId() < r->GetColumnId();
        };
        std::sort(ColumnFeatures.begin(), ColumnFeatures.end(), pred);
    }

    Version = schema.GetVersion();
    return true;
}

std::vector<TNameTypeInfo> GetColumns(const NTable::TScheme::TTableSchema& tableSchema, const std::vector<ui32>& ids) {
    std::vector<std::pair<TString, NScheme::TTypeInfo>> out;
    out.reserve(ids.size());
    for (const ui32 id : ids) {
        const auto ci = tableSchema.Columns.find(id);
        Y_ABORT_UNLESS(ci != tableSchema.Columns.end());
        out.emplace_back(ci->second.Name, ci->second.PType);
    }
    return out;
}

std::optional<TIndexInfo> TIndexInfo::BuildFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema,
    const std::shared_ptr<IStoragesManager>& operators, const std::shared_ptr<TSchemaObjectsCache>& cache) {
    TIndexInfo result("");
    if (!result.DeserializeFromProto(schema, operators, cache)) {
        return std::nullopt;
    }
    return result;
}

std::vector<std::shared_ptr<arrow::Field>> MakeArrowFields(const NTable::TScheme::TTableSchema::TColumns& columns, const std::vector<ui32>& ids,
    const std::shared_ptr<TSchemaObjectsCache>& cache) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (const ui32 id : ids) {
        AFL_VERIFY(!TIndexInfo::IsSpecialColumn(id));
        auto it = columns.find(id);
        AFL_VERIFY(it != columns.end());

        const auto& column = it->second;
        std::string colName(column.Name.data(), column.Name.size());
        auto arrowType = NArrow::GetArrowType(column.PType);
        AFL_VERIFY(arrowType.ok());
        auto f = std::make_shared<arrow::Field>(colName, arrowType.ValueUnsafe(), !column.NotNull);
        if (cache) {
            auto fFound = cache->GetField(f->ToString(true));
            if (!fFound) {
                cache->RegisterField(f->ToString(true), f);
                fields.emplace_back(f);
            } else {
                fields.emplace_back(fFound);
            }
        } else {
            fields.emplace_back(f);
        }
    }

    return fields;
}

std::shared_ptr<arrow::Schema> MakeArrowSchema(
    const NTable::TScheme::TTableSchema::TColumns& columns, const std::vector<ui32>& ids, const std::shared_ptr<TSchemaObjectsCache>& cache) {
    return std::make_shared<arrow::Schema>(MakeArrowFields(columns, ids, cache));
}

void TIndexInfo::InitializeCaches(const std::shared_ptr<IStoragesManager>& operators, const THashMap<ui32, NTable::TColumn>& columns, const std::shared_ptr<TSchemaObjectsCache>& cache,
    const bool withColumnFeatures) {
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::InitializeCaches::Schema");
        AFL_VERIFY(!Schema);
        SchemaColumnIds.reserve(columns.size());
        for (const auto& [id, _] : columns) {
            SchemaColumnIds.push_back(id);
        }

        std::sort(SchemaColumnIds.begin(), SchemaColumnIds.end());
        auto originalFields = MakeArrowFields(columns, SchemaColumnIds, cache);
        Schema = std::make_shared<NArrow::TSchemaLite>(originalFields);
        IIndexInfo::AddSpecialFields(originalFields);
        SchemaWithSpecials = std::make_shared<NArrow::TSchemaLite>(originalFields);
    }
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::InitializeCaches::SchemaFields");
        SchemaColumnIdsWithSpecials = IIndexInfo::AddSpecialFieldIds(SchemaColumnIds);
    }
    if (withColumnFeatures) {
        {
            TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::InitializeCaches::Columns");
            for (auto&& c : columns) {
                ColumnFeatures.emplace_back(BuildDefaultColumnFeatures(c.first, columns, operators));
            }
        }
        {
            TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::InitializeCaches::SysColumns");
            for (auto&& cId : GetSystemColumnIds()) {
                ColumnFeatures.emplace_back(BuildDefaultColumnFeatures(cId, columns, operators));
            }
        }
        const auto pred = [](const std::shared_ptr<TColumnFeatures>& l, const std::shared_ptr<TColumnFeatures>& r) {
            return l->GetColumnId() < r->GetColumnId();
        };
        std::sort(ColumnFeatures.begin(), ColumnFeatures.end(), pred);
    }
}

NSplitter::TEntityGroups TIndexInfo::GetEntityGroupsByStorageId(const TString& specialTier, const IStoragesManager& storages) const {
    NSplitter::TEntityGroups groups(storages.GetDefaultOperator()->GetBlobSplitSettings(), IStoragesManager::DefaultStorageId);
    for (auto&& i : GetEntityIds()) {
        auto storageId = GetEntityStorageId(i, specialTier);
        auto* group = groups.GetGroupOptional(storageId);
        if (!group) {
            group = &groups.RegisterGroup(storageId, storages.GetOperatorVerified(storageId)->GetBlobSplitSettings());
        }
        group->AddEntity(i);
    }
    return groups;
}

std::shared_ptr<NStorageOptimizer::IOptimizerPlannerConstructor> TIndexInfo::GetCompactionPlannerConstructor() const {
    AFL_VERIFY(!!CompactionPlannerConstructor);
    return CompactionPlannerConstructor;
}

std::shared_ptr<arrow::Scalar> TIndexInfo::GetColumnExternalDefaultValueVerified(const std::string& colName) const {
    const ui32 columnId = GetColumnIdVerified(colName);
    return GetColumnExternalDefaultValueVerified(columnId);
}

std::shared_ptr<arrow::Scalar> TIndexInfo::GetColumnExternalDefaultValueVerified(const ui32 columnId) const {
    return GetColumnFeaturesVerified(columnId).GetDefaultValue().GetValue();
}

NKikimr::TConclusionStatus TIndexInfo::AppendIndex(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& originalData,
    const ui32 indexId, const std::shared_ptr<IStoragesManager>& operators, TSecondaryData& result) const {
    auto it = Indexes.find(indexId);
    AFL_VERIFY(it != Indexes.end());
    auto& index = it->second;
    std::shared_ptr<IPortionDataChunk> chunk = index->BuildIndex(originalData, *this);
    auto opStorage = operators->GetOperatorVerified(index->GetStorageId());
    if ((i64)chunk->GetPackedSize() > opStorage->GetBlobSplitSettings().GetMaxBlobSize()) {
        return TConclusionStatus::Fail("blob size for secondary data (" + ::ToString(indexId) + ") bigger than limit (" +
                                       ::ToString(opStorage->GetBlobSplitSettings().GetMaxBlobSize()) + ")");
    }
    if (index->GetStorageId() == IStoragesManager::LocalMetadataStorageId) {
        AFL_VERIFY(result.MutableSecondaryInplaceData().emplace(indexId, chunk).second);
    } else {
        AFL_VERIFY(result.MutableExternalData().emplace(indexId, std::vector<std::shared_ptr<IPortionDataChunk>>({chunk})).second);
    }
    return TConclusionStatus::Success();
}

std::shared_ptr<NIndexes::NMax::TIndexMeta> TIndexInfo::GetIndexMetaMax(const ui32 columnId) const {
    for (auto&& i : Indexes) {
        if (i.second->GetClassName() != NIndexes::NMax::TIndexMeta::GetClassNameStatic()) {
            continue;
        }
        auto maxIndex = static_pointer_cast<NIndexes::NMax::TIndexMeta>(i.second.GetObjectPtr());
        if (maxIndex->GetColumnId() == columnId) {
            return maxIndex;
        }
    }
    return nullptr;
}

std::shared_ptr<NIndexes::NCountMinSketch::TIndexMeta> TIndexInfo::GetIndexMetaCountMinSketch(const std::set<ui32>& columnIds) const {
    for (auto&& i : Indexes) {
        if (i.second->GetClassName() != NIndexes::NCountMinSketch::TIndexMeta::GetClassNameStatic()) {
            continue;
        }
        auto index = static_pointer_cast<NIndexes::NCountMinSketch::TIndexMeta>(i.second.GetObjectPtr());
        if (index->GetColumnIds() == columnIds) {
            return index;
        }
    }
    return nullptr;
}

std::vector<ui32> TIndexInfo::GetEntityIds() const {
    auto result = GetColumnIds(true);
    for (auto&& i : Indexes) {
        result.emplace_back(i.first);
    }
    return result;
}

std::shared_ptr<NKikimr::NOlap::TColumnFeatures> TIndexInfo::BuildDefaultColumnFeatures(
    const ui32 columnId, const THashMap<ui32, NTable::TColumn>& columns, const std::shared_ptr<IStoragesManager>& operators) const {
    if (IsSpecialColumn(columnId)) {
        return std::make_shared<TColumnFeatures>(columnId, GetColumnFieldVerified(columnId), DefaultSerializer, operators->GetDefaultOperator(),
            false, false, false, IIndexInfo::DefaultColumnValue(columnId), std::nullopt);
    } else {
        auto itC = columns.find(columnId);
        AFL_VERIFY(itC != columns.end());
        return std::make_shared<TColumnFeatures>(columnId, GetColumnFieldVerified(columnId), DefaultSerializer, operators->GetDefaultOperator(),
            NArrow::IsPrimitiveYqlType(itC->second.PType), columnId == GetPKFirstColumnId(), false, nullptr, itC->second.GetCorrectKeyOrder());
    }
}

std::shared_ptr<arrow::Scalar> TIndexInfo::GetColumnExternalDefaultValueByIndexVerified(const ui32 colIndex) const {
    AFL_VERIFY(colIndex < ColumnFeatures.size())("index", colIndex)("size", ColumnFeatures.size());
    return ColumnFeatures[colIndex]->GetDefaultValue().GetValue();
}

}   // namespace NKikimr::NOlap
