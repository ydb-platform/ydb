#include "index_info.h"

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/sort_cursor.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/formats/arrow/serializer/batch_only.h>
#include <ydb/core/formats/arrow/transformer/dictionary.h>
#include <ydb/core/formats/arrow/serializer/full.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr::NOlap {

const TString TIndexInfo::STORE_INDEX_STATS_TABLE = TString("/") + NSysView::SysPathName + "/" + NSysView::StorePrimaryIndexStatsName;
const TString TIndexInfo::TABLE_INDEX_STATS_TABLE = TString("/") + NSysView::SysPathName + "/" + NSysView::TablePrimaryIndexStatsName;

static std::vector<TString> NamesOnly(const std::vector<TNameTypeInfo>& columns) {
    std::vector<TString> out;
    out.reserve(columns.size());
    for (const auto& [name, _] : columns) {
        out.push_back(name);
    }
    return out;
}

TIndexInfo::TIndexInfo(const TString& name, ui32 id)
    : NTable::TScheme::TTableSchema()
    , Id(id)
    , Name(name)
{}

std::shared_ptr<arrow::RecordBatch> TIndexInfo::AddSpecialColumns(const std::shared_ptr<arrow::RecordBatch>& batch, const TSnapshot& snapshot) {
    Y_ABORT_UNLESS(batch);
    i64 numColumns = batch->num_columns();
    i64 numRows = batch->num_rows();

    auto res = batch->AddColumn(numColumns, arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64()),
                                NArrow::MakeUI64Array(snapshot.GetPlanStep(), numRows));
    Y_ABORT_UNLESS(res.ok());
    res = (*res)->AddColumn(numColumns + 1, arrow::field(SPEC_COL_TX_ID, arrow::uint64()),
                            NArrow::MakeUI64Array(snapshot.GetTxId(), numRows));
    Y_ABORT_UNLESS(res.ok());
    Y_ABORT_UNLESS((*res)->num_columns() == numColumns + 2);
    return *res;
}

std::shared_ptr<arrow::Schema> TIndexInfo::ArrowSchemaSnapshot() {
    static std::shared_ptr<arrow::Schema> result = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64()),
        arrow::field(SPEC_COL_TX_ID, arrow::uint64())
    });
    return result;
}

bool TIndexInfo::IsSpecialColumn(const arrow::Field& field) {
    return IsSpecialColumn(field.name());
}

bool TIndexInfo::IsSpecialColumn(const std::string& fieldName) {
    return fieldName == SPEC_COL_PLAN_STEP
        || fieldName == SPEC_COL_TX_ID;
}

bool TIndexInfo::IsSpecialColumn(const ui32 fieldId) {
    return fieldId == (ui32)ESpecialColumn::PLAN_STEP
        || fieldId == (ui32)ESpecialColumn::TX_ID;
}

ui32 TIndexInfo::GetColumnId(const std::string& name) const {
    auto id = GetColumnIdOptional(name);
    Y_ABORT_UNLESS(!!id, "undefined column %s", name.data());
    return *id;
}

std::optional<ui32> TIndexInfo::GetColumnIdOptional(const std::string& name) const {
    const auto ni = ColumnNames.find(name);

    if (ni != ColumnNames.end()) {
        return ni->second;
    }
    if (name == SPEC_COL_PLAN_STEP) {
        return ui32(ESpecialColumn::PLAN_STEP);
    } else if (name == SPEC_COL_TX_ID) {
        return ui32(ESpecialColumn::TX_ID);
    }
    return {};
}

TString TIndexInfo::GetColumnName(ui32 id, bool required) const {
    if (ESpecialColumn(id) == ESpecialColumn::PLAN_STEP) {
        return SPEC_COL_PLAN_STEP;
    } else if (ESpecialColumn(id) == ESpecialColumn::TX_ID) {
        return SPEC_COL_TX_ID;
    } else {
        const auto ci = Columns.find(id);

        if (!required && ci == Columns.end()) {
            return {};
        }

        Y_ABORT_UNLESS(ci != Columns.end());
        return ci->second.Name;
    }
}

std::vector<ui32> TIndexInfo::GetColumnIds() const {
    std::vector<ui32> result;
    for (auto&& i : Columns) {
        result.emplace_back(i.first);
    }
    result.emplace_back((ui32)ESpecialColumn::PLAN_STEP);
    result.emplace_back((ui32)ESpecialColumn::TX_ID);
    return result;
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

std::vector<TNameTypeInfo> TIndexInfo::GetColumns(const std::vector<ui32>& ids) const {
    return NOlap::GetColumns(*this, ids);
}

std::shared_ptr<arrow::Schema> TIndexInfo::ArrowSchema() const {
    if (!Schema) {
        std::vector<ui32> ids;
        ids.reserve(Columns.size());
        for (const auto& [id, _] : Columns) {
            ids.push_back(id);
        }

        // The ids had a set type before so we keep them sorted.
        std::sort(ids.begin(), ids.end());
        Schema = MakeArrowSchema(Columns, ids);
    }

    return Schema;
}

std::shared_ptr<arrow::Schema> TIndexInfo::ArrowSchemaWithSpecials() const {
    if (SchemaWithSpecials) {
        return SchemaWithSpecials;
    }

    const auto& schema = ArrowSchema();

    std::vector<std::shared_ptr<arrow::Field>> extended;
    extended.reserve(schema->num_fields() + 3);

    // Place special fields at the beginning of the schema.
    extended.push_back(arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64()));
    extended.push_back(arrow::field(SPEC_COL_TX_ID, arrow::uint64()));
    // Append fields from the regular schema afterward.
    extended.insert(extended.end(), schema->fields().begin(), schema->fields().end());

    SchemaWithSpecials = std::make_shared<arrow::Schema>(std::move(extended));
    return SchemaWithSpecials;
}

std::shared_ptr<arrow::Schema> TIndexInfo::AddColumns(
    const std::shared_ptr<arrow::Schema>& src,
    const std::vector<TString>& columns) const
{
    std::shared_ptr<arrow::Schema> all = ArrowSchemaWithSpecials();
    auto fields = src->fields();

    for (const auto& col : columns) {
        const std::string name(col.data(), col.size());
        if (!src->GetFieldByName(name)) {
            auto field = all->GetFieldByName(name);
            if (!field) {
                return {};
            }
            fields.push_back(field);
        }
    }
    return std::make_shared<arrow::Schema>(std::move(fields));
}

std::shared_ptr<arrow::Schema> TIndexInfo::ArrowSchema(const std::vector<ui32>& columnIds, bool withSpecials) const {
    return MakeArrowSchema(Columns, columnIds, withSpecials);
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

std::shared_ptr<arrow::Schema> TIndexInfo::ArrowSchema(const std::vector<TString>& names) const {
    auto columnIds = GetColumnIds(names);
    if (columnIds.empty()) {
        return {};
    }
    return MakeArrowSchema(Columns, columnIds);
}

std::shared_ptr<arrow::Field> TIndexInfo::ArrowColumnField(ui32 columnId) const {
    auto it = ArrowColumnByColumnIdCache.find(columnId);
    if (it == ArrowColumnByColumnIdCache.end()) {
        it = ArrowColumnByColumnIdCache.emplace(columnId, ArrowSchema()->GetFieldByName(GetColumnName(columnId, true))).first;
    }
    return it->second;
}

void TIndexInfo::SetAllKeys() {
    /// @note Setting replace and sorting key to PK we are able to:
    /// * apply REPLACE by MergeSort
    /// * apply PK predicate before REPLACE
    const auto& primaryKeyNames = NamesOnly(GetPrimaryKey());
    // Update set of required columns with names from primary key.
    for (const auto& name: primaryKeyNames) {
        RequiredColumns.insert(name);
    }

    std::vector<std::shared_ptr<arrow::Field>> fields;
    if (primaryKeyNames.size()) {
        SortingKey = ArrowSchema(primaryKeyNames);
        ReplaceKey = SortingKey;
        fields = ReplaceKey->fields();
        IndexKey = ReplaceKey;
    }

    fields.push_back(arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64()));
    fields.push_back(arrow::field(SPEC_COL_TX_ID, arrow::uint64()));
    ExtendedKey = std::make_shared<arrow::Schema>(std::move(fields));

    for (const auto& [colId, column] : Columns) {
        if (NArrow::IsPrimitiveYqlType(column.PType)) {
            MinMaxIdxColumnsIds.insert(colId);
        }
    }
    MinMaxIdxColumnsIds.insert(GetPKFirstColumnId());
}

std::shared_ptr<NArrow::TSortDescription> TIndexInfo::SortDescription() const {
    if (GetSortingKey()) {
        auto key = GetExtendedKey(); // Sort with extended key, greater snapshot first
        Y_ABORT_UNLESS(key && key->num_fields() > 2);
        auto description = std::make_shared<NArrow::TSortDescription>(key);
        description->Directions[key->num_fields() - 1] = -1;
        description->Directions[key->num_fields() - 2] = -1;
        description->NotNull = true; // TODO
        return description;
    }
    return {};
}

std::shared_ptr<NArrow::TSortDescription> TIndexInfo::SortReplaceDescription() const {
    if (GetSortingKey()) {
        auto key = GetExtendedKey(); // Sort with extended key, greater snapshot first
        Y_ABORT_UNLESS(key && key->num_fields() > 2);
        auto description = std::make_shared<NArrow::TSortDescription>(key, GetReplaceKey());
        description->Directions[key->num_fields() - 1] = -1;
        description->Directions[key->num_fields() - 2] = -1;
        description->NotNull = true; // TODO
        return description;
    }
    return {};
}

bool TIndexInfo::AllowTtlOverColumn(const TString& name) const {
    auto it = ColumnNames.find(name);
    if (it == ColumnNames.end()) {
        return false;
    }
    return MinMaxIdxColumnsIds.contains(it->second);
}

TColumnSaver TIndexInfo::GetColumnSaver(const ui32 columnId, const TSaverContext& context) const {
    arrow::ipc::IpcWriteOptions options;
    options.use_threads = false;

    NArrow::NTransformation::ITransformer::TPtr transformer;
    std::unique_ptr<arrow::util::Codec> columnCodec;
    {
        auto it = ColumnFeatures.find(columnId);
        if (it != ColumnFeatures.end()) {
            transformer = it->second.GetSaveTransformer();
            columnCodec = it->second.GetCompressionCodec();
        }
    }

    if (context.GetExternalCompression()) {
        options.codec = context.GetExternalCompression()->BuildArrowCodec();
    } else if (columnCodec) {
        options.codec = std::move(columnCodec);
    } else if (DefaultCompression) {
        options.codec = DefaultCompression->BuildArrowCodec();
    } else {
        options.codec = NArrow::TCompression::BuildDefaultCodec();
    }

    if (!transformer) {
        return TColumnSaver(transformer, std::make_shared<NArrow::NSerialization::TBatchPayloadSerializer>(options));
    } else {
        return TColumnSaver(transformer, std::make_shared<NArrow::NSerialization::TFullDataSerializer>(options));
    }
}

TColumnFeatures& TIndexInfo::GetOrCreateColumnFeatures(const ui32 columnId) const {
    auto it = ColumnFeatures.find(columnId);
    if (it == ColumnFeatures.end()) {
        it = ColumnFeatures.emplace(columnId, TColumnFeatures::BuildFromIndexInfo(columnId, *this)).first;
    }
    return it->second;
}

std::shared_ptr<TColumnLoader> TIndexInfo::GetColumnLoader(const ui32 columnId) const {
    TColumnFeatures& features = GetOrCreateColumnFeatures(columnId);
    return features.GetLoader();
}

std::shared_ptr<arrow::Schema> TIndexInfo::GetColumnsSchema(const std::set<ui32>& columnIds) const {
    Y_ABORT_UNLESS(columnIds.size());
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto&& i : columnIds) {
        std::shared_ptr<arrow::Schema> schema;
        if (IsSpecialColumn(i)) {
            schema = ArrowSchemaSnapshot();
        } else {
            schema = ArrowSchema();
        }
        auto field = schema->GetFieldByName(GetColumnName(i));
        Y_ABORT_UNLESS(field);
        fields.emplace_back(field);
    }
    return std::make_shared<arrow::Schema>(fields);
}

std::shared_ptr<arrow::Schema> TIndexInfo::GetColumnSchema(const ui32 columnId) const {
    return GetColumnsSchema({columnId});
}

bool TIndexInfo::DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    if (schema.GetEngine() != NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_index_info")("reason", "incorrect_engine_in_schema");
        return false;
    }

    for (const auto& col : schema.GetColumns()) {
        const ui32 id = col.GetId();
        const TString& name = col.GetName();
        const bool notNull = col.HasNotNull() ? col.GetNotNull() : false;
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(col.GetTypeId(),
            col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
        Columns[id] = NTable::TColumn(name, id, typeInfoMod.TypeInfo, typeInfoMod.TypeMod, notNull);
        ColumnNames[name] = id;
    }
    for (const auto& col : schema.GetColumns()) {
        std::optional<TColumnFeatures> cFeatures = TColumnFeatures::BuildFromProto(col, *this);
        if (!cFeatures) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_column_feature");
            return false;
        }
        ColumnFeatures.emplace(col.GetId(), *cFeatures);
    }

    for (const auto& keyName : schema.GetKeyColumnNames()) {
        Y_ABORT_UNLESS(ColumnNames.contains(keyName));
        KeyColumns.push_back(ColumnNames[keyName]);
    }

    if (schema.HasDefaultCompression()) {
        auto result = NArrow::TCompression::BuildFromProto(schema.GetDefaultCompression());
        if (!result) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_index_info")("reason", result.GetErrorMessage());
            return false;
        }
        DefaultCompression = *result;
    }

    Version = schema.GetVersion();
    return true;
}

std::shared_ptr<arrow::Schema> MakeArrowSchema(const NTable::TScheme::TTableSchema::TColumns& columns, const std::vector<ui32>& ids, bool withSpecials) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(withSpecials ? ids.size() + 2 : ids.size());

    if (withSpecials) {
        // Place special fields at the beginning of the schema.
        fields.push_back(arrow::field(TIndexInfo::SPEC_COL_PLAN_STEP, arrow::uint64()));
        fields.push_back(arrow::field(TIndexInfo::SPEC_COL_TX_ID, arrow::uint64()));
    }

    for (const ui32 id: ids) {
        if (TIndexInfo::IsSpecialColumn(id)) {
            AFL_VERIFY(withSpecials);
            continue;
        }
        auto it = columns.find(id);
        AFL_VERIFY(it != columns.end());

        const auto& column = it->second;
        std::string colName(column.Name.data(), column.Name.size());
        fields.emplace_back(std::make_shared<arrow::Field>(colName, NArrow::GetArrowType(column.PType), !column.NotNull));
    }

    return std::make_shared<arrow::Schema>(std::move(fields));
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

std::optional<TIndexInfo> TIndexInfo::BuildFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    TIndexInfo result("", 0);
    if (!result.DeserializeFromProto(schema)) {
        return std::nullopt;
    }
    return result;
}

std::shared_ptr<arrow::Field> TIndexInfo::SpecialColumnField(const ui32 columnId) const {
    return ArrowSchemaSnapshot()->GetFieldByName(GetColumnName(columnId, true));
}

} // namespace NKikimr::NOlap
