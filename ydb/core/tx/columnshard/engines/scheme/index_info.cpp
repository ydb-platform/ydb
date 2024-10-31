#include "index_info.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/formats/arrow/transformer/dictionary.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap {

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

    if (!Schema) {
        AFL_VERIFY(!SchemaWithSpecials);
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

void TIndexInfo::DeserializeOptionsFromProto(const NKikimrSchemeOp::TColumnTableSchemeOptions& optionsProto) {
    TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::Options");
    SchemeNeedActualization = optionsProto.GetSchemeNeedActualization();
    ExternalGuaranteeExclusivePK = optionsProto.GetExternalGuaranteeExclusivePK();
    if (optionsProto.HasCompactionPlannerConstructor()) {
        auto container =
            NStorageOptimizer::TOptimizerPlannerConstructorContainer::BuildFromProto(optionsProto.GetCompactionPlannerConstructor());
        CompactionPlannerConstructor = container.DetachResult().GetObjectPtrVerified();
    } else {
        CompactionPlannerConstructor = NStorageOptimizer::IOptimizerPlannerConstructor::BuildDefault();
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
        for (const auto& col : schema.GetColumns()) {
            auto tableCol = BuildColumnFromProto(col, cache);
            auto id = tableCol.Id;
            AFL_VERIFY(columns.emplace(id, std::move(tableCol)).second);
        }
        ColumnNames = TNameInfo::BuildColumnNames(columns);
        for (const auto& keyName : schema.GetKeyColumnNames()) {
            const ui32 columnId = GetColumnIdVerified(keyName);
            auto it = columns.find(columnId);
            AFL_VERIFY(it != columns.end());
            it->second.KeyOrder = PKColumnIds.size();
            PKColumnIds.push_back(columnId);
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
        AFL_VERIFY(!Schema);
        SchemaColumnIds.reserve(columns.size());
        for (const auto& [id, _] : columns) {
            SchemaColumnIds.push_back(id);
        }

        std::sort(SchemaColumnIds.begin(), SchemaColumnIds.end());
        auto originalFields = TIndexInfo::MakeArrowFields(columns, SchemaColumnIds, cache);
        Schema = std::make_shared<NArrow::TSchemaLite>(originalFields);
        IIndexInfo::AddSpecialFields(originalFields);
        SchemaWithSpecials = std::make_shared<NArrow::TSchemaLite>(originalFields);
    }
    {
        TMemoryProfileGuard g("TIndexInfo::DeserializeFromProto::InitializeCaches::SchemaFields");
        SchemaColumnIdsWithSpecials = IIndexInfo::AddSpecialFieldIds(SchemaColumnIds);
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
                AFL_VERIFY(index < original.SchemaColumnIds.size());
                SchemaColumnIds.emplace_back(originalColId);
                ColumnNames.emplace_back(TNameInfo(original.ColumnFeatures[index]->GetColumnName(), originalColId, ColumnNames.size()));
                fields.emplace_back(original.Schema->field(index));
            }
        };

        const auto addFromDiff = [&](const NKikimrSchemeOp::TOlapColumnDescription& col, const std::optional<ui32> /*originalIndex*/) {
            const ui32 colId = col.GetId();
            AFL_VERIFY(!IIndexInfo::IsSpecialColumn(colId));
            SchemaColumnIdsWithSpecials.emplace_back(colId);
            SchemaColumnIds.emplace_back(colId);
            ColumnNames.emplace_back(TNameInfo(col.GetName(), colId, ColumnNames.size()));
            auto tableCol = BuildColumnFromProto(col, cache);
            fields.emplace_back(BuildArrowField(tableCol, cache));
        };
        diff.ApplyForColumns(original.SchemaColumnIdsWithSpecials, addFromOriginal, addFromDiff);
        Schema = std::make_shared<NArrow::TSchemaLite>(fields);
        IIndexInfo::AddSpecialFields(fields);
        SchemaWithSpecials = std::make_shared<NArrow::TSchemaLite>(fields);
        std::sort(ColumnNames.begin(), ColumnNames.end(), TNameInfo::TNameComparator());
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
    UsedStorageIds = std::make_shared<std::set<TString>>();
    for (auto&& i : ColumnFeatures) {
        UsedStorageIds->emplace(i->GetOperator()->GetStorageId());
    }
}

void TIndexInfo::Validate() const {
    AFL_VERIFY(!!UsedStorageIds);
    AFL_VERIFY(ColumnFeatures.size() == SchemaColumnIdsWithSpecials.size());
    AFL_VERIFY(ColumnFeatures.size() == (ui32)SchemaWithSpecials->num_fields());
    AFL_VERIFY(ColumnFeatures.size() == (ui32)Schema->num_fields() + IIndexInfo::SpecialColumnsCount);
    AFL_VERIFY(ColumnFeatures.size() == SchemaColumnIds.size() + IIndexInfo::SpecialColumnsCount);
    {
        ui32 idx = 0;
        for (auto&& i : SchemaColumnIds) {
            AFL_VERIFY(i == ColumnFeatures[idx]->GetColumnId());
            AFL_VERIFY(Schema->field(idx)->name() == ColumnFeatures[idx]->GetColumnName());
            AFL_VERIFY(Schema->field(idx)->Equals(SchemaWithSpecials->field(idx)));
            ++idx;
        }
    }

    for (auto&& i : ColumnNames) {
        AFL_VERIFY(ColumnFeatures[i.GetColumnIdx()]->GetColumnId() == i.GetColumnId());
        AFL_VERIFY(ColumnFeatures[i.GetColumnIdx()]->GetColumnName() == i.GetName());
    }

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
    return result;
}

}   // namespace NKikimr::NOlap
