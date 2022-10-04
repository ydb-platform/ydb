#include "index_info.h"
#include "insert_table.h"
#include "column_engine.h"
#include <ydb/core/formats/arrow_batch_builder.h>
#include <ydb/core/formats/sort_cursor.h>
#include <ydb/core/sys_view/common/schema.h>

namespace NKikimr::NOlap {

const TString TIndexInfo::STORE_INDEX_STATS_TABLE = TString("/") + NSysView::SysPathName + "/" + NSysView::StorePrimaryIndexStatsName;
const TString TIndexInfo::TABLE_INDEX_STATS_TABLE = TString("/") + NSysView::SysPathName + "/" + NSysView::TablePrimaryIndexStatsName;

void ScalarToConstant(const arrow::Scalar& scalar, NKikimrSSA::TProgram_TConstant& value) {
    switch (scalar.type->id()) {
        case arrow::Type::BOOL:
            value.SetBool(static_cast<const arrow::BooleanScalar&>(scalar).value);
            break;
        case arrow::Type::UINT8:
            value.SetUint32(static_cast<const arrow::UInt8Scalar&>(scalar).value);
            break;
        case arrow::Type::UINT16:
            value.SetUint32(static_cast<const arrow::UInt16Scalar&>(scalar).value);
            break;
        case arrow::Type::UINT32:
            value.SetUint32(static_cast<const arrow::UInt32Scalar&>(scalar).value);
            break;
        case arrow::Type::UINT64:
            value.SetUint64(static_cast<const arrow::UInt64Scalar&>(scalar).value);
            break;
        case arrow::Type::INT8:
            value.SetInt32(static_cast<const arrow::Int8Scalar&>(scalar).value);
            break;
        case arrow::Type::INT16:
            value.SetInt32(static_cast<const arrow::Int16Scalar&>(scalar).value);
            break;
        case arrow::Type::INT32:
            value.SetInt32(static_cast<const arrow::Int32Scalar&>(scalar).value);
            break;
        case arrow::Type::INT64:
            value.SetInt64(static_cast<const arrow::Int64Scalar&>(scalar).value);
            break;
        case arrow::Type::FLOAT:
            value.SetFloat(static_cast<const arrow::FloatScalar&>(scalar).value);
            break;
        case arrow::Type::DOUBLE:
            value.SetDouble(static_cast<const arrow::DoubleScalar&>(scalar).value);
            break;
        case arrow::Type::TIMESTAMP:
            value.SetUint64(static_cast<const arrow::TimestampScalar&>(scalar).value);
            break;
        default:
            Y_VERIFY(false, "Some types are not supported in min-max index yet"); // TODO
    }
}

std::shared_ptr<arrow::Scalar> ConstantToScalar(const NKikimrSSA::TProgram_TConstant& value,
                                                const std::shared_ptr<arrow::DataType>& type) {
    switch (type->id()) {
        case arrow::Type::BOOL:
            return std::make_shared<arrow::BooleanScalar>(value.GetBool());
        case arrow::Type::UINT8:
            return std::make_shared<arrow::UInt8Scalar>(value.GetUint32());
        case arrow::Type::UINT16:
            return std::make_shared<arrow::UInt16Scalar>(value.GetUint32());
        case arrow::Type::UINT32:
            return std::make_shared<arrow::UInt32Scalar>(value.GetUint32());
        case arrow::Type::UINT64:
            return std::make_shared<arrow::UInt64Scalar>(value.GetUint64());
        case arrow::Type::INT8:
            return std::make_shared<arrow::Int8Scalar>(value.GetInt32());
        case arrow::Type::INT16:
            return std::make_shared<arrow::Int16Scalar>(value.GetInt32());
        case arrow::Type::INT32:
            return std::make_shared<arrow::Int32Scalar>(value.GetInt32());
        case arrow::Type::INT64:
            return std::make_shared<arrow::Int64Scalar>(value.GetInt64());
        case arrow::Type::FLOAT:
            return std::make_shared<arrow::FloatScalar>(value.GetFloat());
        case arrow::Type::DOUBLE:
            return std::make_shared<arrow::DoubleScalar>(value.GetDouble());
        case arrow::Type::TIMESTAMP:
            return std::make_shared<arrow::TimestampScalar>(value.GetUint64(), type);
        default:
            Y_VERIFY(false, "Some types are not supported in min-max index yet"); // TODO
    }
    return {};
}

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
    if (metadata.size()) {
        schema = NArrow::DeserializeSchema(metadata);
        if (!schema) {
            strError = "DeserializeSchema() failed";
            return {};
        }
    }

    auto batch = NArrow::DeserializeBatch(data, schema);
    if (!batch) {
        strError = "DeserializeBatch() failed";
        return {};
    }
    if (batch->num_rows() == 0) {
        strError = "empty batch";
        return {};
    }
    auto status = batch->ValidateFull();
    if (!status.ok()) {
        auto tmp = status.ToString();
        strError = TString(tmp.data(), tmp.size());
        return {};
    }

    // Require all the columns for now. It's possible to ommit some in future.
    for (auto& field : schema->fields()) {
        if (!batch->GetColumnByName(field->name())) {
            strError = "missing column '" + field->name() + "'";
            return {};
        }
    }

    // Correct schema
    if (metadata.size()) {
        batch = NArrow::ExtractColumns(batch, ArrowSchema());
        if (!batch) {
            strError = "cannot correct schema";
            return {};
        }
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

void TIndexInfo::SetAllKeys(const TVector<TString>& columns, const TVector<int>& indexKeyPos) {
    AddRequiredColumns(columns);
    MinMaxIdxColumnsIds.insert(GetPKFirstColumnId());

    std::vector<std::shared_ptr<arrow::Field>> fileds;
    if (columns.size()) {
        SortingKey = ArrowSchema(columns);
        ReplaceKey = SortingKey;
        fileds = ReplaceKey->fields();
    }

    fileds.push_back(arrow::field(SPEC_COL_PLAN_STEP, arrow::uint64()));
    fileds.push_back(arrow::field(SPEC_COL_TX_ID, arrow::uint64()));
    ExtendedKey = std::make_shared<arrow::Schema>(fileds);

    std::vector<std::shared_ptr<arrow::Field>> indexFields;
    indexFields.reserve(indexKeyPos.size());
    for (int pos : indexKeyPos) {
        indexFields.push_back(fileds[pos]);
    }
    IndexKey = std::make_shared<arrow::Schema>(indexFields);
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

}
