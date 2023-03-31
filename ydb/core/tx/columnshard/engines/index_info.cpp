#include "index_info.h"
#include "insert_table.h"
#include "column_engine.h"
#include <ydb/core/formats/arrow_batch_builder.h>
#include <ydb/core/formats/sort_cursor.h>
#include <ydb/core/sys_view/common/schema.h>

namespace NKikimr::NOlap {

const TString TIndexInfo::STORE_INDEX_STATS_TABLE = TString("/") + NSysView::SysPathName + "/" + NSysView::StorePrimaryIndexStatsName;
const TString TIndexInfo::TABLE_INDEX_STATS_TABLE = TString("/") + NSysView::SysPathName + "/" + NSysView::TablePrimaryIndexStatsName;

TVector<TRawTypeValue> TIndexInfo::ExtractKey(const THashMap<ui32, TCell>& fields, bool allowNulls) const {
    TVector<TRawTypeValue> key;
    key.reserve(KeyColumns.size());

    for (ui32 columnId : KeyColumns) {
        auto& column = Columns.find(columnId)->second;
        auto it = fields.find(columnId);
        Y_VERIFY(it != fields.end());

        const TCell& cell = it->second;
        Y_VERIFY(allowNulls || !cell.IsNull());

        key.emplace_back(TRawTypeValue(cell.AsRef(), column.PType));
    }

    return key;
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
        auto tmp = status.ToString();
        strError = TString(tmp.data(), tmp.size());
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

    TSet<ui32> ids;
    for (auto& [id, column] : Columns) {
        ids.insert(id);
    }

    Schema = MakeArrowSchema(Columns, ids);
    return Schema;
}

bool TIndexInfo::IsSpecialColumn(const arrow::Field& field) {
    const auto& name = field.name();
    return (name == SPEC_COL_PLAN_STEP)
        || (name == SPEC_COL_TX_ID);
}

std::shared_ptr<arrow::Schema> TIndexInfo::ArrowSchemaWithSpecials() const {
    if (SchemaWithSpecials) {
        return SchemaWithSpecials;
    }

    auto schema = ArrowSchema();

    std::vector<std::shared_ptr<arrow::Field>> extended;
    extended.reserve(schema->num_fields() + 3);

    extended.push_back(arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64()));
    extended.push_back(arrow::field(SPEC_COL_TX_ID, arrow::uint64()));
    for (auto& field : schema->fields()) {
        extended.push_back(field);
    }

    SchemaWithSpecials = std::make_shared<arrow::Schema>(extended);
    return SchemaWithSpecials;
}

std::shared_ptr<arrow::Schema> TIndexInfo::AddColumns(std::shared_ptr<arrow::Schema> src,
                                                      const TVector<TString>& columns) const {
    std::shared_ptr<arrow::Schema> all = ArrowSchemaWithSpecials();
    auto fields = src->fields();

    for (auto& col : columns) {
        std::string name(col.data(), col.size());
        if (!src->GetFieldByName(name)) {
            auto field = all->GetFieldByName(name);
            if (!field) {
                return {};
            }
            fields.push_back(field);
        }
    }
    return std::make_shared<arrow::Schema>(fields);
}

std::shared_ptr<arrow::Schema> TIndexInfo::ArrowSchemaSnapshot() {
    std::vector<std::shared_ptr<arrow::Field>> snap = {
        arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64()),
        arrow::field(SPEC_COL_TX_ID, arrow::uint64())
    };

    return std::make_shared<arrow::Schema>(snap);
}

std::shared_ptr<arrow::RecordBatch> TIndexInfo::AddSpecialColumns(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                                  ui64 planStep, ui64 txId)
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
    auto columnName = GetColumnName(columnId, true);
    return ArrowSchema()->GetFieldByName(columnName);
}

void TIndexInfo::SetAllKeys() {
    auto pk = NamesOnly(GetPK());
    AddRequiredColumns(pk);

    std::vector<std::shared_ptr<arrow::Field>> fields;
    if (pk.size()) {
        SortingKey = ArrowSchema(pk);
        ReplaceKey = SortingKey;
        fields = ReplaceKey->fields();

        std::vector<std::shared_ptr<arrow::Field>> indexFields = { fields[0] };
        IndexKey = std::make_shared<arrow::Schema>(indexFields);
    }

    fields.push_back(arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64()));
    fields.push_back(arrow::field(SPEC_COL_TX_ID, arrow::uint64()));
    ExtendedKey = std::make_shared<arrow::Schema>(fields);

    for (auto& [colId, column] : Columns) {
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
    return MinMaxIdxColumnsIds.count(it->second);
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

}
