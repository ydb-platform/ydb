#include "index_info.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>
#include <ydb/core/formats/arrow/transformer/dictionary.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NOlap {

static std::vector<TString> NamesOnly(const std::vector<TNameTypeInfo>& columns) {
    std::vector<TString> out;
    out.reserve(columns.size());
    for (const auto& [name, _] : columns) {
        out.push_back(name);
    }
    return out;
}

TIndexInfo::TIndexInfo(const TString& name)
    : NTable::TScheme::TTableSchema()
    , Name(name) {
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
    const auto ni = ColumnNames.find(name);

    if (ni != ColumnNames.end()) {
        return ni->second;
    }
    return IIndexInfo::GetColumnIdOptional(name);
}

TString TIndexInfo::GetColumnName(ui32 id, bool required) const {
    const auto ci = Columns.find(id);

    if (ci != Columns.end()) {
        return ci->second.Name;
    }

    return IIndexInfo::GetColumnName(id, required);
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
        const auto ci = Columns.find(id);
        Y_ABORT_UNLESS(ci != Columns.end());
        out.push_back(ci->second.Name);
    }
    return out;
}

std::vector<std::string> TIndexInfo::GetColumnSTLNames(const std::vector<ui32>& ids) const {
    std::vector<std::string> out;
    out.reserve(ids.size());
    for (ui32 id : ids) {
        const auto ci = Columns.find(id);
        Y_ABORT_UNLESS(ci != Columns.end());
        out.push_back(ci->second.Name);
    }
    return out;
}

std::vector<TNameTypeInfo> TIndexInfo::GetColumns(const std::vector<ui32>& ids) const {
    return NOlap::GetColumns(*this, ids);
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

void TIndexInfo::SetAllKeys(const std::shared_ptr<IStoragesManager>& operators) {
    /// @note Setting replace and sorting key to PK we are able to:
    /// * apply REPLACE by MergeSort
    /// * apply PK predicate before REPLACE
    {
        AFL_VERIFY(PKColumnIds.empty());
        const auto& primaryKeyNames = NamesOnly(GetPrimaryKeyColumns());
        PKColumnIds = GetColumnIds(primaryKeyNames);
        AFL_VERIFY(PKColumnIds.size());
        PrimaryKey = MakeArrowSchema(Columns, PKColumnIds, nullptr);
    }

    for (const auto& [colId, column] : Columns) {
        if (NArrow::IsPrimitiveYqlType(column.PType)) {
            MinMaxIdxColumnsIds.insert(colId);
        }
    }
    MinMaxIdxColumnsIds.insert(GetPKFirstColumnId());
    if (!Schema) {
        AFL_VERIFY(IdIntoIndex.empty());
        AFL_VERIFY(!SchemaWithSpecials);
        InitializeCaches(operators, nullptr);
    }
}

TColumnSaver TIndexInfo::GetColumnSaver(const ui32 columnId) const {
    auto it = ColumnFeatures.find(columnId);
    AFL_VERIFY(it != ColumnFeatures.end());
    return it->second->GetColumnSaver();
}

std::shared_ptr<TColumnLoader> TIndexInfo::GetColumnLoaderOptional(const ui32 columnId) const {
    auto it = ColumnFeatures.find(columnId);
    if (it == ColumnFeatures.end()) {
        return nullptr;
    } else {
        return it->second->GetLoader();
    }
}

std::optional<ui32> TIndexInfo::GetColumnIndexOptional(const ui32 id) const {
    auto it = IdIntoIndex.find(id);
    if (it == IdIntoIndex.end()) {
        return std::nullopt;
    } else {
        return it->second;
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
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::Columns");
        for (const auto& col : schema.GetColumns()) {
            const ui32 id = col.GetId();
            const TString& name = col.GetName();
            const bool notNull = col.HasNotNull() ? col.GetNotNull() : false;
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(col.GetTypeId(), col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
            Columns[id] = NTable::TColumn(name, id, typeInfoMod.TypeInfo, typeInfoMod.TypeMod, notNull);
            ColumnNames[name] = id;
        }
    }
    for (const auto& keyName : schema.GetKeyColumnNames()) {
        Y_ABORT_UNLESS(ColumnNames.contains(keyName));
        KeyColumns.push_back(ColumnNames[keyName]);
    }
    InitializeCaches(operators, cache);
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::Columns::Features");
        for (const auto& col : schema.GetColumns()) {
            auto it = ColumnFeatures.find(col.GetId());
            AFL_VERIFY(it != ColumnFeatures.end());
            const TString fingerprint = cache ? col.SerializeAsString() : Default<TString>();
            if (cache) {
                if (std::shared_ptr<TColumnFeatures> f = cache->GetColumnFeatures(fingerprint)) {
                    it->second = f;
                    continue;
                }
            }

            auto parsed = it->second->DeserializeFromProto(col, operators);
            if (!parsed) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_column_feature")("reason", parsed.GetErrorMessage());
                return false;
            }
            if (cache) {
                cache->RegisterColumnFeatures(fingerprint, it->second);
            }
        }
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

std::shared_ptr<arrow::Schema> MakeArrowSchema(
    const NTable::TScheme::TTableSchema::TColumns& columns, const std::vector<ui32>& ids, const std::shared_ptr<TSchemaObjectsCache>& cache) {
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

    return std::make_shared<arrow::Schema>(std::move(fields));
}

void TIndexInfo::InitializeCaches(const std::shared_ptr<IStoragesManager>& operators, const std::shared_ptr<TSchemaObjectsCache>& cache) {
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::InitializeCaches::Schema");
        AFL_VERIFY(!Schema);
        SchemaColumnIds.reserve(Columns.size());
        for (const auto& [id, _] : Columns) {
            SchemaColumnIds.push_back(id);
        }

        std::sort(SchemaColumnIds.begin(), SchemaColumnIds.end());
        auto originalSchema = MakeArrowSchema(Columns, SchemaColumnIds, cache);
        Schema = std::make_shared<NArrow::TSchemaLite>(originalSchema);
        SchemaWithSpecials = std::make_shared<NArrow::TSchemaLite>(IIndexInfo::AddSpecialFields(originalSchema));
    }
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::InitializeCaches::SchemaFields");
        SchemaColumnIdsWithSpecials = IIndexInfo::AddSpecialFieldIds(SchemaColumnIds);
        SchemaColumnIdsWithSpecialsSet = IIndexInfo::AddSpecialFieldIds(std::set<ui32>(SchemaColumnIds.begin(), SchemaColumnIds.end()));
        ui32 idx = 0;
        for (auto&& i : SchemaColumnIdsWithSpecials) {
            AFL_VERIFY(IdIntoIndex.emplace(i, idx++).second);
        }
    }
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::InitializeCaches::Columns");
        for (auto&& c : Columns) {
            AFL_VERIFY(ArrowColumnByColumnIdCache.emplace(c.first, GetColumnFieldVerified(c.first)).second);
            AFL_VERIFY(ColumnFeatures.emplace(c.first, std::make_shared<TColumnFeatures>(c.first, GetColumnFieldVerified(c.first), DefaultSerializer, operators->GetDefaultOperator(),
                NArrow::IsPrimitiveYqlType(c.second.PType), c.first == GetPKFirstColumnId(), nullptr)).second);
        }
    }
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::InitializeCaches::SysColumns");
        for (auto&& cId : GetSystemColumnIds()) {
            AFL_VERIFY(ArrowColumnByColumnIdCache.emplace(cId, GetColumnFieldVerified(cId)).second);
            AFL_VERIFY(ColumnFeatures.emplace(cId, std::make_shared<TColumnFeatures>(cId, GetColumnFieldVerified(cId), DefaultSerializer, operators->GetDefaultOperator(),
                false, false, IIndexInfo::DefaultColumnValue(cId))).second);
        }
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
    if (IIndexInfo::IsSpecialColumn(columnId)) {
        return IIndexInfo::DefaultColumnValue(columnId);
    }
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

}   // namespace NKikimr::NOlap
