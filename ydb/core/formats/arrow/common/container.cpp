#include "container.h"
#include <ydb/library/actors/core/log.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NArrow {

NKikimr::TConclusionStatus TGeneralContainer::MergeColumnsStrictly(const TGeneralContainer& container) {
    if (RecordsCount != container.RecordsCount) {
        return TConclusionStatus::Fail(TStringBuilder() << "inconsistency records count in additional container: " <<
            container.GetSchema()->ToString() << ". expected: " << RecordsCount << ", reality: " << container.GetRecordsCount());
    }
    for (i32 i = 0; i < container.Schema->num_fields(); ++i) {
        auto addFieldResult = AddField(container.Schema->field(i), container.Columns[i]);
        if (!addFieldResult) {
            return addFieldResult;
        }
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TGeneralContainer::AddField(const std::shared_ptr<arrow::Field>& f, const std::shared_ptr<NAccessor::IChunkedArray>& data) {
    AFL_VERIFY(f);
    AFL_VERIFY(data);
    if (data->GetRecordsCount() != RecordsCount) {
        return TConclusionStatus::Fail(TStringBuilder() << "inconsistency records count in new column: " <<
            f->name() << ". expected: " << RecordsCount << ", reality: " << data->GetRecordsCount());
    }
    if (!data->GetDataType()->Equals(f->type())) {
        return TConclusionStatus::Fail("schema and data type are not equals: " + data->GetDataType()->ToString() + " vs " + f->type()->ToString());
    }
    if (Schema->GetFieldByName(f->name())) {
        return TConclusionStatus::Fail("field name duplication: " + f->name());
    }
    auto resultAdd = Schema->AddField(Schema->num_fields(), f);
    if (!resultAdd.ok()) {
        return TConclusionStatus::Fail("internal schema error on add field: " + resultAdd.status().ToString());
    }
    Schema = *resultAdd;
    Columns.emplace_back(data);
    return TConclusionStatus::Success();
}

TGeneralContainer::TGeneralContainer(const std::shared_ptr<arrow::Schema>& schema, std::vector<std::shared_ptr<NAccessor::IChunkedArray>>&& columns)
    : Schema(schema)
    , Columns(std::move(columns))
{
    AFL_VERIFY(schema);
    std::optional<ui64> recordsCount;
    AFL_VERIFY(Schema->num_fields() == (i32)Columns.size())("schema", Schema->num_fields())("columns", Columns.size());
    for (i32 i = 0; i < Schema->num_fields(); ++i) {
        AFL_VERIFY(Columns[i]);
        AFL_VERIFY(Schema->field(i)->type()->Equals(Columns[i]->GetDataType()));
        if (!recordsCount) {
            recordsCount = Columns[i]->GetRecordsCount();
        } else {
            AFL_VERIFY(*recordsCount == Columns[i]->GetRecordsCount())
                ("event", "inconsistency_records_count")("expect", *recordsCount)("real", Columns[i]->GetRecordsCount())("field_name", Schema->field(i)->name());
        }
    }
    AFL_VERIFY(recordsCount);
    RecordsCount = *recordsCount;
}

TGeneralContainer::TGeneralContainer(const std::shared_ptr<arrow::Table>& table) {
    AFL_VERIFY(table);
    Schema = table->schema();
    RecordsCount = table->num_rows();
    for (auto&& i : table->columns()) {
        if (i->num_chunks() == 1) {
            Columns.emplace_back(std::make_shared<NAccessor::TTrivialArray>(i->chunk(0)));
        } else {
            Columns.emplace_back(std::make_shared<NAccessor::TTrivialChunkedArray>(i));
        }
    }
}

TGeneralContainer::TGeneralContainer(const std::shared_ptr<arrow::RecordBatch>& table) {
    AFL_VERIFY(table);
    Schema = table->schema();
    RecordsCount = table->num_rows();
    for (auto&& i : table->columns()) {
        Columns.emplace_back(std::make_shared<NAccessor::TTrivialArray>(i));
    }
}

std::shared_ptr<NKikimr::NArrow::NAccessor::IChunkedArray> TGeneralContainer::GetAccessorByNameVerified(const std::string& fieldId) const {
    auto result = GetAccessorByNameOptional(fieldId);
    AFL_VERIFY(result)("event", "cannot_find_accessor_in_general_container")("field_id", fieldId)("schema", Schema->ToString());
    return result;
}

std::shared_ptr<NKikimr::NArrow::TGeneralContainer> TGeneralContainer::BuildEmptySame() const {
    std::vector<std::shared_ptr<NAccessor::IChunkedArray>> columns;
    for (auto&& c : Columns) {
        columns.emplace_back(std::make_shared<NAccessor::TTrivialArray>(NArrow::TThreadSimpleArraysCache::GetNull(c->GetDataType(), 0)));
    }
    return std::make_shared<TGeneralContainer>(Schema, std::move(columns));
}

std::shared_ptr<arrow::Table> TGeneralContainer::BuildTable(const std::optional<std::set<std::string>>& columnNames /*= {}*/) const {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    ui32 count = 0;
    for (i32 i = 0; i < Schema->num_fields(); ++i) {
        if (columnNames && !columnNames->contains(Schema->field(i)->name())) {
            continue;
        }
        ++count;
        columns.emplace_back(Columns[i]->GetChunkedArray());
        fields.emplace_back(Schema->field(i));
    }
    AFL_VERIFY(!columnNames || count == columnNames->size());
    return arrow::Table::Make(std::make_shared<arrow::Schema>(fields), columns, RecordsCount);
}

}
