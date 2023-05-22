#include "index_info.h"
#include "insert_table.h"
#include "column_engine.h"

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/sort_cursor.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/formats/arrow/serializer/batch_only.h>
#include <ydb/core/formats/arrow/transformer/dictionary.h>
#include <ydb/core/formats/arrow/serializer/full.h>

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

TIndexInfo::TIndexInfo(const TString& name, ui32 id, bool compositeIndexKey)
    : NTable::TScheme::TTableSchema()
    , Id(id)
    , Name(name)
    , CompositeIndexKey(compositeIndexKey)
{}

std::shared_ptr<arrow::RecordBatch> TIndexInfo::AddSpecialColumns(const std::shared_ptr<arrow::RecordBatch>& batch, const TSnapshot& snapshot) {
    Y_VERIFY(batch);
    i64 numColumns = batch->num_columns();
    i64 numRows = batch->num_rows();

    auto res = batch->AddColumn(numColumns, arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64()),
                                NArrow::MakeUI64Array(snapshot.GetPlanStep(), numRows));
    Y_VERIFY(res.ok());
    res = (*res)->AddColumn(numColumns + 1, arrow::field(SPEC_COL_TX_ID, arrow::uint64()),
                            NArrow::MakeUI64Array(snapshot.GetTxId(), numRows));
    Y_VERIFY(res.ok());
    Y_VERIFY((*res)->num_columns() == numColumns + 2);
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
    Y_VERIFY(!!id, "undefined column %s", name.data());
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

        Y_VERIFY(ci != Columns.end());
        return ci->second.Name;
    }
}

std::vector<TString> TIndexInfo::GetColumnNames(const std::vector<ui32>& ids) const {
    std::vector<TString> out;
    out.reserve(ids.size());
    for (ui32 id : ids) {
        const auto ci = Columns.find(id);
        Y_VERIFY(ci != Columns.end());
        out.push_back(ci->second.Name);
    }
    return out;
}

std::vector<TNameTypeInfo> TIndexInfo::GetColumns(const std::vector<ui32>& ids) const {
    return NOlap::GetColumns(*this, ids);
}

std::shared_ptr<arrow::RecordBatch> TIndexInfo::PrepareForInsert(const TString& data, const TString& metadata,
                                                                 TString& strError) const {
    std::shared_ptr<arrow::Schema> schema = ArrowSchema();
    std::shared_ptr<arrow::Schema> differentSchema;
    if (metadata.size()) {
        differentSchema = NArrow::DeserializeSchema(metadata);
        if (!differentSchema) {
            strError = "DeserializeSchema() failed";
            return {};
        }
    }

    auto batch = NArrow::DeserializeBatch(data, (differentSchema ? differentSchema : schema));
    if (!batch) {
        strError = "DeserializeBatch() failed";
        return {};
    }
    if (batch->num_rows() == 0) {
        strError = "empty batch";
        return {};
    }

    // Correct schema
    if (differentSchema) {
        batch = NArrow::ExtractColumns(batch, ArrowSchema());
        if (!batch) {
            strError = "cannot correct schema";
            return {};
        }
    }

    if (!batch->schema()->Equals(ArrowSchema())) {
        strError = "unexpected schema for insert batch: '" + batch->schema()->ToString() + "'";
        return {};
    }

    // Check PK is NOT NULL
    for (auto& field : SortingKey->fields()) {
        auto column = batch->GetColumnByName(field->name());
        if (!column) {
            strError = "missing PK column '" + field->name() + "'";
            return {};
        }
        if (NArrow::HasNulls(column)) {
            strError = "PK column '" + field->name() + "' contains NULLs";
            return {};
        }
    }

    auto status = batch->ValidateFull();
    if (!status.ok()) {
        strError = status.ToString();
        return {};
    }

    Y_VERIFY(SortingKey);
    batch = NArrow::SortBatch(batch, SortingKey);
    Y_VERIFY_DEBUG(NArrow::IsSorted(batch, SortingKey));
    return batch;
}

std::shared_ptr<arrow::Schema> TIndexInfo::ArrowSchema() const {
    if (Schema) {
        return Schema;
    }

    std::vector<ui32> ids;
    ids.reserve(Columns.size());
    for (const auto& [id, _] : Columns) {
        ids.push_back(id);
    }

    // The ids had a set type before so we keep them sorted.
    std::sort(ids.begin(), ids.end());
    Schema = MakeArrowSchema(Columns, ids);
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
    return ArrowSchema()->GetFieldByName(GetColumnName(columnId, true));
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
        if (CompositeIndexKey) {
            IndexKey = ReplaceKey;
        } else {
            IndexKey = std::make_shared<arrow::Schema>(arrow::FieldVector({ fields[0] }));
        }
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
        Y_VERIFY(key && key->num_fields() > 2);
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
        Y_VERIFY(key && key->num_fields() > 2);
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
    if (context.GetExternalCompression()) {
        options.codec = context.GetExternalCompression()->BuildArrowCodec();
    } else {
        options.codec = DefaultCompression.BuildArrowCodec();
    }
    options.use_threads = false;
    auto it = ColumnFeatures.find(columnId);
    NArrow::NTransformation::ITransformer::TPtr transformer;
    if (it != ColumnFeatures.end()) {
        transformer = it->second.GetSaveTransformer();
        auto codec = it->second.GetCompressionCodec();
        if (!!codec) {
            options.codec = std::move(codec);
        }
    }
    if (!transformer) {
        return TColumnSaver(transformer, std::make_shared<NArrow::NSerialization::TBatchPayloadSerializer>(options));
    } else {
        return TColumnSaver(transformer, std::make_shared<NArrow::NSerialization::TFullDataSerializer>(options));
    }
}

std::shared_ptr<TColumnLoader> TIndexInfo::GetColumnLoader(const ui32 columnId) const {
    auto it = ColumnFeatures.find(columnId);
    NArrow::NTransformation::ITransformer::TPtr transformer;
    if (it != ColumnFeatures.end()) {
        transformer = it->second.GetLoadTransformer();
    }
    if (!transformer) {
        return std::make_shared<TColumnLoader>(transformer,
            std::make_shared<NArrow::NSerialization::TBatchPayloadDeserializer>(GetColumnSchema(columnId)),
            GetColumnSchema(columnId), columnId);
    } else {
        return std::make_shared<TColumnLoader>(transformer,
            std::make_shared<NArrow::NSerialization::TFullDataDeserializer>(),
            GetColumnSchema(columnId), columnId);
    }
}

std::shared_ptr<arrow::Schema> TIndexInfo::GetColumnSchema(const ui32 columnId) const {
    std::shared_ptr<arrow::Schema> schema = Schema;
    if (IsSpecialColumn(columnId)) {
        schema = ArrowSchemaSnapshot();
    }
    auto field = schema->GetFieldByName(GetColumnName(columnId));
    Y_VERIFY(field);
    std::vector<std::shared_ptr<arrow::Field>> fields = { field };
    return std::make_shared<arrow::Schema>(fields);
}

bool TIndexInfo::DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    if (schema.GetEngine() != NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_index_info")("reason", "incorrect_engine_in_schema");
        return false;
    }

    for (const auto& col : schema.GetColumns()) {
        const ui32 id = col.GetId();
        const TString& name = col.GetName();
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(col.GetTypeId(),
            col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
        Columns[id] = NTable::TColumn(name, id, typeInfoMod.TypeInfo, typeInfoMod.TypeMod);
        ColumnNames[name] = id;
        std::optional<TColumnFeatures> cFeatures = TColumnFeatures::BuildFromProto(col);
        if (!cFeatures) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_column_feature");
            return false;
        }
        ColumnFeatures.emplace(id, *cFeatures);
    }

    for (const auto& keyName : schema.GetKeyColumnNames()) {
        Y_VERIFY(ColumnNames.contains(keyName));
        KeyColumns.push_back(ColumnNames[keyName]);
    }

    if (schema.HasDefaultCompression()) {
        Y_VERIFY(DefaultCompression.DeserializeFromProto(schema.GetDefaultCompression()));
    }
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
        auto it = columns.find(id);
        if (it == columns.end()) {
            continue;
        }

        const auto& column = it->second;
        std::string colName(column.Name.data(), column.Name.size());
        fields.emplace_back(std::make_shared<arrow::Field>(colName, NArrow::GetArrowType(column.PType)));
    }

    return std::make_shared<arrow::Schema>(std::move(fields));
}

std::vector<TNameTypeInfo> GetColumns(const NTable::TScheme::TTableSchema& tableSchema, const std::vector<ui32>& ids) {
    std::vector<std::pair<TString, NScheme::TTypeInfo>> out;
    out.reserve(ids.size());
    for (const ui32 id : ids) {
        const auto ci = tableSchema.Columns.find(id);
        Y_VERIFY(ci != tableSchema.Columns.end());
        out.emplace_back(ci->second.Name, ci->second.PType);
    }
    return out;
}

NArrow::NTransformation::ITransformer::TPtr TColumnFeatures::GetSaveTransformer() const {
    NArrow::NTransformation::ITransformer::TPtr transformer;
    if (LowCardinality.value_or(false)) {
        transformer = std::make_shared<NArrow::NTransformation::TDictionaryPackTransformer>();
    }
    return transformer;
}

NArrow::NTransformation::ITransformer::TPtr TColumnFeatures::GetLoadTransformer() const {
    NArrow::NTransformation::ITransformer::TPtr transformer;
    if (LowCardinality.value_or(false)) {
        transformer = std::make_shared<NArrow::NTransformation::TDictionaryUnpackTransformer>();
    }
    return transformer;
}

} // namespace NKikimr::NOlap
