#include "index_info.h"
#include "insert_table.h"
#include "column_engine.h"

#include <ydb/core/formats/arrow_batch_builder.h>
#include <ydb/core/formats/sort_cursor.h>
#include <ydb/core/sys_view/common/schema.h>

namespace NKikimr::NOlap {

const TString TIndexInfo::STORE_INDEX_STATS_TABLE = TString("/") + NSysView::SysPathName + "/" + NSysView::StorePrimaryIndexStatsName;
const TString TIndexInfo::TABLE_INDEX_STATS_TABLE = TString("/") + NSysView::SysPathName + "/" + NSysView::TablePrimaryIndexStatsName;

static TVector<TString> NamesOnly(const TVector<TNameTypeInfo>& columns) {
    TVector<TString> out;
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

std::shared_ptr<arrow::RecordBatch> TIndexInfo::AddSpecialColumns(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const ui64 planStep,
    const ui64 txId)
{
    Y_VERIFY(batch);
    i64 numColumns = batch->num_columns();
    i64 numRows = batch->num_rows();

    auto res = batch->AddColumn(numColumns, arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64()),
                                NArrow::MakeUI64Array(planStep, numRows));
    Y_VERIFY(res.ok());
    res = (*res)->AddColumn(numColumns + 1, arrow::field(SPEC_COL_TX_ID, arrow::uint64()),
                            NArrow::MakeUI64Array(txId, numRows));
    Y_VERIFY(res.ok());
    Y_VERIFY((*res)->num_columns() == numColumns + 2);
    return *res;
}

std::shared_ptr<arrow::Schema> TIndexInfo::ArrowSchemaSnapshot() {
    return std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64()),
        arrow::field(SPEC_COL_TX_ID, arrow::uint64())
    });
}

bool TIndexInfo::IsSpecialColumn(const arrow::Field& field) {
    const auto& name = field.name();
    return (name == SPEC_COL_PLAN_STEP)
        || (name == SPEC_COL_TX_ID);
}


ui32 TIndexInfo::GetColumnId(const TString& name) const {
    const auto ni = ColumnNames.find(name);

    if (ni == ColumnNames.end()) {
        if (name == SPEC_COL_PLAN_STEP) {
            return ui32(ESpecialColumn::PLAN_STEP);
        } else if (name == SPEC_COL_TX_ID) {
            return ui32(ESpecialColumn::TX_ID);
        }
        Y_VERIFY(false);
    }

    return ni->second;
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

TVector<TString> TIndexInfo::GetColumnNames(const TVector<ui32>& ids) const {
    TVector<TString> out;
    out.reserve(ids.size());
    for (ui32 id : ids) {
        const auto ci = Columns.find(id);
        Y_VERIFY(ci != Columns.end());
        out.push_back(ci->second.Name);
    }
    return out;
}

TVector<TNameTypeInfo> TIndexInfo::GetColumns(const TVector<ui32>& ids) const {
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

    TVector<ui32> ids;
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
    const TVector<TString>& columns) const
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

std::shared_ptr<arrow::Schema> TIndexInfo::ArrowSchema(const TVector<ui32>& columnIds) const {
    return MakeArrowSchema(Columns, columnIds);
}

std::shared_ptr<arrow::Schema> TIndexInfo::ArrowSchema(const TVector<TString>& names) const {
    TVector<ui32> ids;
    ids.reserve(names.size());
    for (auto& name : names) {
        auto it = ColumnNames.find(name);
        if (it == ColumnNames.end()) {
            return {};
        }
        ids.emplace_back(it->second);
    }

    return MakeArrowSchema(Columns, ids);
}

std::shared_ptr<arrow::Field> TIndexInfo::ArrowColumnField(ui32 columnId) const {
    return ArrowSchema()->GetFieldByName(GetColumnName(columnId, true));
}

void TIndexInfo::SetAllKeys() {
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
        IndexKey = std::make_shared<arrow::Schema>(arrow::FieldVector({ fields[0] }));
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

void TIndexInfo::UpdatePathTiering(THashMap<ui64, NOlap::TTiering>& pathTiering) const {
    auto schema = ArrowSchema(); // init Schema if not yet

    for (auto& [pathId, tiering] : pathTiering) {
        for (auto& [tierName, tierInfo] : tiering.TierByName) {
            if (!tierInfo->EvictColumn) {
                tierInfo->EvictColumn = schema->GetFieldByName(tierInfo->EvictColumnName);
            }
            // TODO: eviction with recompression is not supported yet
            if (tierInfo->NeedExport) {
                tierInfo->Compression = {};
            }
        }
        if (tiering.Ttl && !tiering.Ttl->EvictColumn) {
            tiering.Ttl->EvictColumn = schema->GetFieldByName(tiering.Ttl->EvictColumnName);
        }
    }
}

void TIndexInfo::SetPathTiering(THashMap<ui64, TTiering>&& pathTierings) {
    PathTiering = std::move(pathTierings);
}

const TTiering* TIndexInfo::GetTiering(ui64 pathId) const {
    auto it = PathTiering.find(pathId);
    if (it != PathTiering.end()) {
        return &it->second;
    }
    return nullptr;
}

std::shared_ptr<arrow::Schema> MakeArrowSchema(const NTable::TScheme::TTableSchema::TColumns& columns, const TVector<ui32>& ids) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(ids.size());

    for (const ui32 id: ids) {
        auto it = columns.find(id);
        if (it == columns.end()) {
            return {};
        }

        const auto& column = it->second;
        std::string colName(column.Name.data(), column.Name.size());
        fields.emplace_back(std::make_shared<arrow::Field>(colName, NArrow::GetArrowType(column.PType)));
    }

    return std::make_shared<arrow::Schema>(std::move(fields));
}

TVector<TNameTypeInfo> GetColumns(const NTable::TScheme::TTableSchema& tableSchema, const TVector<ui32>& ids) {
    TVector<std::pair<TString, NScheme::TTypeInfo>> out;
    out.reserve(ids.size());
    for (const ui32 id : ids) {
        const auto ci = tableSchema.Columns.find(id);
        Y_VERIFY(ci != tableSchema.Columns.end());
        out.emplace_back(ci->second.Name, ci->second.PType);
    }
    return out;
}

} // namespace NKikimr::NOlap
