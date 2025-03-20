#include "index_info.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/formats/arrow/transformer/dictionary.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#include <util/string/join.h>

namespace NKikimr::NOlap {

bool TIndexInfo::CheckCompatible(const TIndexInfo& other) const {
    if (!other.GetPrimaryKey()->Equals(PrimaryKey)) {
        return false;
    }
    return true;
}

ui32 TIndexInfo::GetColumnIdVerified(const std::string& name) const {
    auto id = GetColumnIdOptional(name);
    AFL_VERIFY(!!id)("column_name", name)("names", JoinSeq(",", ColumnIdxSortedByName));
    return *id;
}

std::optional<ui32> TIndexInfo::GetColumnIdOptional(const std::string& name) const {
    auto idx = GetColumnIndexOptional(name);
    if (!idx) {
        return std::nullopt;
    }
    AFL_VERIFY(*idx < SchemaColumnIdsWithSpecials.size());
    return SchemaColumnIdsWithSpecials[*idx];
}

std::optional<ui32> TIndexInfo::GetColumnIndexOptional(const std::string& name) const {
    auto it = std::lower_bound(ColumnIdxSortedByName.begin(), ColumnIdxSortedByName.end(), name, [this](const ui32 idx, const std::string name) {
        AFL_VERIFY(idx < ColumnFeatures.size());
        return ColumnFeatures[idx]->GetColumnName() < name;
    });
    if (it != ColumnIdxSortedByName.end() && SchemaWithSpecials->GetFieldByIndexVerified(*it)->name() == name) {
        return *it;
    }
    return std::nullopt;
}

TString TIndexInfo::GetColumnName(const ui32 id, bool required) const {
    const auto& f = GetColumnFeaturesOptional(id);
    if (!f) {
        AFL_VERIFY(!required)("id", id)("indexes", JoinSeq(",", SchemaColumnIdsWithSpecials));
        return "";
    } else {
        return f->GetColumnName();
    }
}

TColumnIdsView TIndexInfo::GetColumnIds(const bool withSpecial) const {
    if (withSpecial) {
        return { SchemaColumnIdsWithSpecials.begin(), SchemaColumnIdsWithSpecials.end() };
    } else {
        AFL_VERIFY(SpecialColumnsCount < SchemaColumnIdsWithSpecials.size());
        return { SchemaColumnIdsWithSpecials.begin(), SchemaColumnIdsWithSpecials.end() - SpecialColumnsCount };
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

std::vector<std::string> TIndexInfo::GetColumnSTLNames(const bool withSpecial) const {
    const TColumnIdsView ids = GetColumnIds(withSpecial);
    std::vector<std::string> out;
    out.reserve(ids.size());
    for (ui32 id : ids) {
        out.push_back(GetColumnName(id));
    }
    return out;
}

NArrow::TSchemaLiteView TIndexInfo::ArrowSchema() const {
    const auto& schema = ArrowSchemaWithSpecials();
    return std::span<const std::shared_ptr<arrow::Field>>(schema->fields().begin(), schema->fields().end() - SpecialColumnsCount);
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

    if (!SchemaWithSpecials) {
        InitializeCaches(operators, columns, nullptr);
        Precalculate();
    }
}

TColumnSaver TIndexInfo::GetColumnSaver(const ui32 columnId) const {
    return GetColumnFeaturesVerified(columnId).GetColumnSaver();
}

const std::shared_ptr<TColumnLoader>& TIndexInfo::GetColumnLoaderOptional(const ui32 columnId) const {
    const auto& cFeatures = GetColumnFeaturesOptional(columnId);
    if (!cFeatures) {
        return Default<std::shared_ptr<TColumnLoader>>();
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
    AFL_VERIFY(columnIds.size());
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto&& i : columnIds) {
        fields.emplace_back(GetColumnFieldVerified(i));
    }
    return std::make_shared<arrow::Schema>(fields);
}

std::shared_ptr<arrow::Schema> TIndexInfo::GetColumnsSchemaByOrderedIndexes(const std::vector<ui32>& columnIdxs) const {
    AFL_VERIFY(columnIdxs.size());
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::optional<ui32> predColumnIdx;
    for (auto&& i : columnIdxs) {
        if (predColumnIdx) {
            AFL_VERIFY(*predColumnIdx < i);
        }
        predColumnIdx = i;
        fields.emplace_back(ArrowSchemaWithSpecials()->GetFieldByIndexVerified(i));
    }
    return std::make_shared<arrow::Schema>(fields);
}

std::shared_ptr<arrow::Schema> TIndexInfo::GetColumnSchema(const ui32 columnId) const {
    return GetColumnsSchema({ columnId });
}

void TIndexInfo::DeserializeOptionsFromProto(const NKikimrSchemeOp::TColumnTableSchemeOptions& optionsProto) {
    TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::Options");
    SchemeNeedActualization = optionsProto.GetSchemeNeedActualization();
    if (optionsProto.HasScanReaderPolicyName()) {
        ScanReaderPolicyName = optionsProto.GetScanReaderPolicyName();
    }
    if (optionsProto.HasCompactionPlannerConstructor()) {
        auto container =
            NStorageOptimizer::TOptimizerPlannerConstructorContainer::BuildFromProto(optionsProto.GetCompactionPlannerConstructor());
        CompactionPlannerConstructor = container.DetachResult().GetObjectPtrVerified();
    } else {
        CompactionPlannerConstructor = NStorageOptimizer::IOptimizerPlannerConstructor::BuildDefault();
    }
    if (optionsProto.HasMetadataManagerConstructor()) {
        auto container =
            NDataAccessorControl::TMetadataManagerConstructorContainer::BuildFromProto(optionsProto.GetMetadataManagerConstructor());
        MetadataManagerConstructor = container.DetachResult().GetObjectPtrVerified();
    } else {
        MetadataManagerConstructor = NDataAccessorControl::IManagerConstructor::BuildDefault();
    }
}

bool TIndexInfo::DeserializeDefaultCompressionFromProto(const NKikimrSchemeOp::TCompressionOptions& compressionProto) {
    TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::Serializer");
    NArrow::NSerialization::TSerializerContainer container;
    if (!container.DeserializeFromProto(compressionProto)) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_index_info")("reason", "cannot_parse_default_serializer");
        return false;
    }
    DefaultSerializer = container;
    return true;
}

TConclusion<std::shared_ptr<TColumnFeatures>> TIndexInfo::CreateColumnFeatures(const NTable::TColumn& col,
    const NKikimrSchemeOp::TOlapColumnDescription& colProto, const std::shared_ptr<IStoragesManager>& operators,
    const std::shared_ptr<TSchemaObjectsCache>& cache) const {
    const TString fingerprint = cache ? ("C:" + colProto.SerializeAsString()) : Default<TString>();
    const auto createPred = [&]() -> TConclusion<std::shared_ptr<TColumnFeatures>> {
        auto f = BuildDefaultColumnFeatures(col, operators);
        auto parsed = f->DeserializeFromProto(colProto, operators);
        if (parsed.IsFail()) {
            return parsed;
        }
        return f;
    };
    return cache->GetOrCreateColumnFeatures(fingerprint, createPred);
}

bool TIndexInfo::DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema, const std::shared_ptr<IStoragesManager>& operators,
    const std::shared_ptr<TSchemaObjectsCache>& cache) {
    AFL_VERIFY(cache);

    DeserializeOptionsFromProto(schema.GetOptions());

    if (schema.HasDefaultCompression()) {
        if (!DeserializeDefaultCompressionFromProto(schema.GetDefaultCompression())) {
            return false;
        }
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
    AFL_VERIFY(PKColumnIds.empty());
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::Columns");
        THashMap<TString, ui32> columnIds;
        for (const auto& col : schema.GetColumns()) {
            auto tableCol = BuildColumnFromProto(col, cache);
            auto id = tableCol.Id;
            AFL_VERIFY(columnIds.emplace(tableCol.Name, id).second);
            AFL_VERIFY(columns.emplace(id, std::move(tableCol)).second);
        }
        for (const auto& keyName : schema.GetKeyColumnNames()) {
            const ui32* findColumnId = columnIds.FindPtr(keyName);
            AFL_VERIFY(findColumnId);
            auto it = columns.find(*findColumnId);
            AFL_VERIFY(it != columns.end());
            it->second.KeyOrder = PKColumnIds.size();
            PKColumnIds.push_back(*findColumnId);
        }
    }
    InitializeCaches(operators, columns, cache, false);
    SetAllKeys(operators, columns);
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::Columns::Features");
        for (const auto& col : schema.GetColumns()) {
            auto it = columns.find(col.GetId());
            AFL_VERIFY(it != columns.end());
            auto fConclusion = CreateColumnFeatures(it->second, col, operators, cache);
            if (fConclusion.IsFail()) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_build_column_feature")("reason", fConclusion.GetErrorMessage());
                return false;
            }
            ColumnFeatures.emplace_back(fConclusion.DetachResult());
        }
        for (auto&& cId : GetSystemColumnIds()) {
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
    Precalculate();
    Validate();
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
    TIndexInfo result;
    if (!result.DeserializeFromProto(schema, operators, cache)) {
        return std::nullopt;
    }
    return result;
}

std::optional<TIndexInfo> TIndexInfo::BuildFromProto(const NKikimrSchemeOp::TColumnTableSchemaDiff& diff, const TIndexInfo& prevSchema,
    const std::shared_ptr<IStoragesManager>& operators, const std::shared_ptr<TSchemaObjectsCache>& cache) {
    TSchemaDiffView diffView;
    diffView.DeserializeFromProto(diff).Validate();
    return TIndexInfo(prevSchema, diffView, operators, cache);
}

std::vector<std::shared_ptr<arrow::Field>> TIndexInfo::MakeArrowFields(
    const NTable::TScheme::TTableSchema::TColumns& columns, const std::vector<ui32>& ids, const std::shared_ptr<TSchemaObjectsCache>& cache) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (const ui32 id : ids) {
        AFL_VERIFY(!TIndexInfo::IsSpecialColumn(id));
        auto it = columns.find(id);
        AFL_VERIFY(it != columns.end());
        auto f = TIndexInfo::BuildArrowField(it->second, cache);
        fields.emplace_back(f);
    }

    return fields;
}

std::shared_ptr<arrow::Schema> MakeArrowSchema(
    const NTable::TScheme::TTableSchema::TColumns& columns, const std::vector<ui32>& ids, const std::shared_ptr<TSchemaObjectsCache>& cache) {
    return std::make_shared<arrow::Schema>(TIndexInfo::MakeArrowFields(columns, ids, cache));
}

void TIndexInfo::InitializeCaches(const std::shared_ptr<IStoragesManager>& operators, const THashMap<ui32, NTable::TColumn>& columns,
    const std::shared_ptr<TSchemaObjectsCache>& cache, const bool withColumnFeatures) {
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::InitializeCaches::Schema");
        AFL_VERIFY(!SchemaWithSpecials);
        SchemaColumnIdsWithSpecials.reserve(columns.size());
        for (const auto& [id, _] : columns) {
            SchemaColumnIdsWithSpecials.push_back(id);
        }

        std::sort(SchemaColumnIdsWithSpecials.begin(), SchemaColumnIdsWithSpecials.end());
        auto originalFields = TIndexInfo::MakeArrowFields(columns, SchemaColumnIdsWithSpecials, cache);
        IIndexInfo::AddSpecialFields(originalFields);
        SchemaWithSpecials = std::make_shared<NArrow::TSchemaLite>(originalFields);
    }
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::InitializeCaches::SchemaFields");
        IIndexInfo::AddSpecialFieldIds(SchemaColumnIdsWithSpecials);
    }
    if (withColumnFeatures) {
        AFL_VERIFY(ColumnFeatures.empty());
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

const std::shared_ptr<NStorageOptimizer::IOptimizerPlannerConstructor>& TIndexInfo::GetCompactionPlannerConstructor() const {
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
    const ui32 indexId, const std::shared_ptr<IStoragesManager>& operators, const ui32 recordsCount, TSecondaryData& result) const {
    auto it = Indexes.find(indexId);
    AFL_VERIFY(it != Indexes.end());
    auto& index = it->second;
    TMemoryProfileGuard mpg("IndexConstruction::" + index->GetIndexName());
    auto indexChunkConclusion = index->BuildIndexOptional(originalData, recordsCount, *this);
    if (indexChunkConclusion.IsFail()) {
        return indexChunkConclusion;
    }
    if (!*indexChunkConclusion) {
        return TConclusionStatus::Success();
    }
    auto chunk = indexChunkConclusion.DetachResult();
    auto opStorage = operators->GetOperatorVerified(index->GetStorageId());
    if ((i64)chunk->GetPackedSize() > opStorage->GetBlobSplitSettings().GetMaxBlobSize()) {
        return TConclusionStatus::Fail("blob size for secondary data (" + ::ToString(indexId) + ":" + ::ToString(chunk->GetPackedSize()) + ":" +
                                       ::ToString(recordsCount) + ") bigger than limit (" +
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
    const TColumnIdsView columnIds = GetColumnIds(true);
    std::vector<ui32> result(columnIds.begin(), columnIds.end());
    for (auto&& i : Indexes) {
        result.emplace_back(i.first);
    }
    return result;
}

std::shared_ptr<NKikimr::NOlap::TColumnFeatures> TIndexInfo::BuildDefaultColumnFeatures(
    const NTable::TColumn& column, const std::shared_ptr<IStoragesManager>& operators) const {
    AFL_VERIFY(!IsSpecialColumn(column.Id));
    return std::make_shared<TColumnFeatures>(column.Id, GetColumnFieldVerified(column.Id), DefaultSerializer, operators->GetDefaultOperator(),
        NArrow::IsPrimitiveYqlType(column.PType), column.Id == GetPKFirstColumnId(), false, nullptr, column.GetCorrectKeyOrder());
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

TIndexInfo::TIndexInfo(const TIndexInfo& original, const TSchemaDiffView& diff, const std::shared_ptr<IStoragesManager>& operators,
    const std::shared_ptr<TSchemaObjectsCache>& cache) {
    {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        const auto addFromOriginal = [&](const ui32 index) {
            AFL_VERIFY(index < original.SchemaColumnIdsWithSpecials.size());
            const ui32 originalColId = original.SchemaColumnIdsWithSpecials[index];
            SchemaColumnIdsWithSpecials.emplace_back(originalColId);
            if (!IIndexInfo::IsSpecialColumn(originalColId)) {
                AFL_VERIFY(index < original.SchemaColumnIdsWithSpecials.size() - SpecialColumnsCount);
                fields.emplace_back(original.SchemaWithSpecials->field(index));
            }
        };

        const auto addFromDiff = [&](const NKikimrSchemeOp::TOlapColumnDescription& col, const std::optional<ui32> /*originalIndex*/) {
            const ui32 colId = col.GetId();
            AFL_VERIFY(!IIndexInfo::IsSpecialColumn(colId));
            SchemaColumnIdsWithSpecials.emplace_back(colId);
            auto tableCol = BuildColumnFromProto(col, cache);
            fields.emplace_back(BuildArrowField(tableCol, cache));
        };
        diff.ApplyForColumns(original.SchemaColumnIdsWithSpecials, addFromOriginal, addFromDiff);
        IIndexInfo::AddSpecialFields(fields);
        SchemaWithSpecials = std::make_shared<NArrow::TSchemaLite>(fields);
        PKColumnIds = original.PKColumnIds;
        PKColumns = original.PKColumns;
    }
    {
        const auto addFromOriginal = [&](const ui32 index) {
            ColumnFeatures.emplace_back(original.ColumnFeatures[index]);
        };

        const auto addFromDiff = [&](const NKikimrSchemeOp::TOlapColumnDescription& col, const std::optional<ui32> originalIndex) {
            auto tableCol = BuildColumnFromProto(col, cache);
            if (originalIndex && original.ColumnFeatures[*originalIndex]->GetPKColumnIndex()) {
                tableCol.KeyOrder = *original.ColumnFeatures[*originalIndex]->GetPKColumnIndex();
            }
            ColumnFeatures.emplace_back(CreateColumnFeatures(tableCol, col, operators, cache).DetachResult());
        };
        diff.ApplyForColumns(original.SchemaColumnIdsWithSpecials, addFromOriginal, addFromDiff);
    }
    {
        TMemoryProfileGuard g("TIndexInfo::ApplyDiff::Indexes");
        Indexes = original.Indexes;
        for (auto&& i : diff.GetModifiedIndexes()) {
            if (!i.second) {
                AFL_VERIFY(Indexes.erase(i.first));
            } else {
                auto it = Indexes.find(i.first);
                NIndexes::TIndexMetaContainer meta;
                AFL_VERIFY(meta.DeserializeFromProto(*i.second));
                if (it != Indexes.end()) {
                    it->second = std::move(meta);
                } else {
                    Indexes.emplace(i.first, std::move(meta));
                }
            }
        }
    }

    DeserializeOptionsFromProto(diff.GetSchemaOptions());
    Version = diff.GetVersion();
    PrimaryKey = original.PrimaryKey;
    if (diff.GetCompressionOptions()) {
        DeserializeDefaultCompressionFromProto(*diff.GetCompressionOptions());
    }
    Precalculate();
    Validate();
}

void TIndexInfo::Precalculate() {
    BuildColumnIndexByName();
    UsedStorageIds = std::make_shared<std::set<TString>>();
    for (auto&& i : ColumnFeatures) {
        UsedStorageIds->emplace(i->GetOperator()->GetStorageId());
    }
}

void TIndexInfo::BuildColumnIndexByName() {
    const ui32 columnCount = SchemaColumnIdsWithSpecials.size();
    std::erase_if(ColumnIdxSortedByName, [columnCount](const ui32 idx) {
        return idx >= columnCount;
    });
    ColumnIdxSortedByName.reserve(columnCount);
    for (ui32 i = 0; i < columnCount; ++i) {
        ColumnIdxSortedByName.push_back(i);
    }

    std::sort(ColumnIdxSortedByName.begin(), ColumnIdxSortedByName.end(), [this](const ui32 lhs, const ui32 rhs) {
        return CompareColumnIdxByName(lhs, rhs);
    });
}

void TIndexInfo::Validate() const {
    AFL_VERIFY(!!UsedStorageIds);
    AFL_VERIFY(ColumnFeatures.size() == SchemaColumnIdsWithSpecials.size());
    AFL_VERIFY(ColumnFeatures.size() == (ui32)SchemaWithSpecials->num_fields());
    {
        ui32 idx = 0;
        for (auto&& i : SchemaColumnIdsWithSpecials) {
            AFL_VERIFY(i == ColumnFeatures[idx]->GetColumnId());
            AFL_VERIFY(SchemaWithSpecials->field(idx)->name() == ColumnFeatures[idx]->GetColumnName());
            ++idx;
        }
    }
    AFL_VERIFY(std::is_sorted(SchemaColumnIdsWithSpecials.begin(), SchemaColumnIdsWithSpecials.end()));

    AFL_VERIFY(ColumnFeatures.size() == ColumnIdxSortedByName.size());
    AFL_VERIFY(std::is_sorted(ColumnIdxSortedByName.begin(), ColumnIdxSortedByName.end(), [this](const ui32 lhs, const ui32 rhs) {
        return CompareColumnIdxByName(lhs, rhs);
    }));

    {
        ui32 pkIdx = 0;
        for (auto&& i : PKColumnIds) {
            const ui32 idx = GetColumnIndexVerified(i);
            AFL_VERIFY(ColumnFeatures[idx]->GetPKColumnIndex());
            AFL_VERIFY(*ColumnFeatures[idx]->GetPKColumnIndex() == pkIdx);
            ++pkIdx;
        }
    }
}

TIndexInfo TIndexInfo::BuildDefault() {
    TIndexInfo result;
    result.CompactionPlannerConstructor = NStorageOptimizer::IOptimizerPlannerConstructor::BuildDefault();
    result.MetadataManagerConstructor = NDataAccessorControl::IManagerConstructor::BuildDefault();
    return result;
}

TConclusion<std::shared_ptr<arrow::Array>> TIndexInfo::BuildDefaultColumn(const ui32 fieldIndex, const ui32 rowsCount, const bool force) const {
    auto defaultValue = GetColumnExternalDefaultValueByIndexVerified(fieldIndex);
    auto f = ArrowSchemaWithSpecials()->GetFieldByIndexVerified(fieldIndex);
    if (!defaultValue && !IsNullableVerifiedByIndex(fieldIndex)) {
        if (force) {
            defaultValue = NArrow::DefaultScalar(f->type());
        } else {
            return TConclusionStatus::Fail("not nullable field with no default: " + f->name());
        }
    }
    return NArrow::TThreadSimpleArraysCache::Get(f->type(), defaultValue, rowsCount);
}

ui32 TIndexInfo::GetColumnIndexVerified(const ui32 id) const {
    auto result = GetColumnIndexOptional(id);
    AFL_VERIFY(result)("id", id)("indexes", JoinSeq(",", SchemaColumnIdsWithSpecials));
    return *result;
}

std::vector<std::shared_ptr<NIndexes::TSkipIndex>> TIndexInfo::FindSkipIndexes(
    const NIndexes::NRequest::TOriginalDataAddress& originalDataAddress, const NArrow::NSSA::EIndexCheckOperation op) const {
    std::vector<std::shared_ptr<NIndexes::TSkipIndex>> result;
    for (auto&& [_, i] : Indexes) {
        if (!i->IsSkipIndex()) {
            continue;
        }
        auto skipIndex = std::static_pointer_cast<NIndexes::TSkipIndex>(i.GetObjectPtrVerified());
        if (skipIndex->IsAppropriateFor(originalDataAddress, op)) {
            result.emplace_back(skipIndex);
        }
    }
    return result;
}

}   // namespace NKikimr::NOlap
