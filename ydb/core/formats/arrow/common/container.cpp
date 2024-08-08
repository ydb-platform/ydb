#include "container.h"
#include <ydb/library/actors/core/log.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NArrow {

TConclusionStatus TGeneralContainer::MergeColumnsStrictly(const TGeneralContainer& container) {
    if (!container.RecordsCount) {
        return TConclusionStatus::Success();
    }
    if (!RecordsCount) {
        RecordsCount = container.RecordsCount;
    }
    if (*RecordsCount != *container.RecordsCount) {
        return TConclusionStatus::Fail(TStringBuilder() << "inconsistency records count in additional container: " <<
            container.GetSchema()->ToString() << ". expected: " << RecordsCount << ", reality: " << container.GetRecordsCount());
    }
    for (i32 i = 0; i < container.Schema->num_fields(); ++i) {
        auto addFieldResult = AddField(container.Schema->field(i), container.Columns[i]);
        if (addFieldResult.IsFail()) {
            return addFieldResult;
        }
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TGeneralContainer::AddField(const std::shared_ptr<arrow::Field>& f, const std::shared_ptr<NAccessor::IChunkedArray>& data) {
    AFL_VERIFY(f);
    AFL_VERIFY(data);
    if (RecordsCount && data->GetRecordsCount() != *RecordsCount) {
        return TConclusionStatus::Fail(TStringBuilder() << "inconsistency records count in new column: " <<
            f->name() << ". expected: " << RecordsCount << ", reality: " << data->GetRecordsCount());
    }
    if (!data->GetDataType()->Equals(f->type())) {
        return TConclusionStatus::Fail("schema and data type are not equals: " + data->GetDataType()->ToString() + " vs " + f->type()->ToString());
    }
    {
        auto conclusion = Schema->AddField(f);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    RecordsCount = data->GetRecordsCount();
    Columns.emplace_back(data);
    return TConclusionStatus::Success();
}

TConclusionStatus TGeneralContainer::AddField(const std::shared_ptr<arrow::Field>& f, const std::shared_ptr<arrow::ChunkedArray>& data) {
    return AddField(f, std::make_shared<NAccessor::TTrivialChunkedArray>(data));
}

TConclusionStatus TGeneralContainer::AddField(const std::shared_ptr<arrow::Field>& f, const std::shared_ptr<arrow::Array>& data) {
    return AddField(f, std::make_shared<NAccessor::TTrivialArray>(data));
}

void TGeneralContainer::Initialize() {
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
    AFL_VERIFY(!RecordsCount || *RecordsCount == *recordsCount);
    RecordsCount = *recordsCount;
}

TGeneralContainer::TGeneralContainer(const std::vector<std::shared_ptr<arrow::Field>>& fields, std::vector<std::shared_ptr<NAccessor::IChunkedArray>>&& columns)
    : Schema(std::make_shared<NModifier::TSchema>(fields))
    , Columns(std::move(columns))
{
    Initialize();
}

TGeneralContainer::TGeneralContainer(const std::shared_ptr<NModifier::TSchema>& schema, std::vector<std::shared_ptr<NAccessor::IChunkedArray>>&& columns)
    : Schema(std::make_shared<NModifier::TSchema>(schema))
    , Columns(std::move(columns))
{
    Initialize();
}

TGeneralContainer::TGeneralContainer(const std::shared_ptr<arrow::Schema>& schema, std::vector<std::shared_ptr<NAccessor::IChunkedArray>>&& columns)
    : Schema(std::make_shared<NModifier::TSchema>(schema))
    , Columns(std::move(columns))
{
    Initialize();
}

TGeneralContainer::TGeneralContainer(const std::shared_ptr<arrow::Table>& table) {
    AFL_VERIFY(table);
    Schema = std::make_shared<NModifier::TSchema>(table->schema());
    RecordsCount = table->num_rows();
    for (auto&& i : table->columns()) {
        if (i->num_chunks() == 1) {
            Columns.emplace_back(std::make_shared<NAccessor::TTrivialArray>(i->chunk(0)));
        } else {
            Columns.emplace_back(std::make_shared<NAccessor::TTrivialChunkedArray>(i));
        }
    }
    Initialize();
}

TGeneralContainer::TGeneralContainer(const std::shared_ptr<arrow::RecordBatch>& table) {
    AFL_VERIFY(table);
    Schema = std::make_shared<NModifier::TSchema>(table->schema());
    RecordsCount = table->num_rows();
    for (auto&& i : table->columns()) {
        Columns.emplace_back(std::make_shared<NAccessor::TTrivialArray>(i));
    }
    Initialize();
}

TGeneralContainer::TGeneralContainer(const ui32 recordsCount)
    : RecordsCount(recordsCount)
    , Schema(std::make_shared<NModifier::TSchema>()) {
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

std::shared_ptr<arrow::Table> TGeneralContainer::BuildTableOptional(const std::optional<std::set<std::string>>& columnNames /*= {}*/) const {
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (i32 i = 0; i < Schema->num_fields(); ++i) {
        if (columnNames && !columnNames->contains(Schema->field(i)->name())) {
            continue;
        }
        columns.emplace_back(Columns[i]->GetChunkedArray());
        fields.emplace_back(Schema->field(i));
    }
    if (fields.empty()) {
        return nullptr;
    }
    AFL_VERIFY(RecordsCount);
    return arrow::Table::Make(std::make_shared<arrow::Schema>(fields), columns, *RecordsCount);
}

std::shared_ptr<arrow::Table> TGeneralContainer::BuildTableVerified(const std::optional<std::set<std::string>>& columnNames /*= {}*/) const {
    auto result = BuildTableOptional(columnNames);
    AFL_VERIFY(result);
    AFL_VERIFY(!columnNames || result->schema()->num_fields() == (i32)columnNames->size());
    return result;
}

std::shared_ptr<NArrow::NAccessor::IChunkedArray> TGeneralContainer::GetAccessorByNameOptional(const std::string& fieldId) const {
    int idx = Schema->GetFieldIndex(fieldId);
    if (idx == -1) {
        return nullptr;
    }
    AFL_VERIFY((ui32)idx < Columns.size())("idx", idx)("count", Columns.size());
    return Columns[idx];
}

TConclusionStatus TGeneralContainer::SyncSchemaTo(const std::shared_ptr<arrow::Schema>& schema, const IFieldsConstructor* defaultFieldsConstructor, const bool forceDefaults) {
    std::shared_ptr<NModifier::TSchema> schemaNew = std::make_shared<NModifier::TSchema>();
    std::vector<std::shared_ptr<NAccessor::IChunkedArray>> columnsNew;
    if (!RecordsCount) {
        return TConclusionStatus::Fail("original container has not data");
    }
    for (auto&& i : schema->fields()) {
        const int idx = Schema->GetFieldIndex(i->name());
        if (idx == -1) {
            if (!defaultFieldsConstructor) {
                return TConclusionStatus::Fail("haven't field for sync: '" + i->name() + "'");
            } else {
                schemaNew->AddField(i).Validate();
                auto defConclusion = defaultFieldsConstructor->GetDefaultColumnElementValue(i, forceDefaults);
                if (defConclusion.IsFail()) {
                    return defConclusion;
                }
                columnsNew.emplace_back(std::make_shared<NAccessor::TTrivialArray>(NArrow::TThreadSimpleArraysCache::Get(i->type(), *defConclusion, *RecordsCount)));
            }
        } else {
            const auto& fOwned = Schema->GetFieldVerified(idx);
            if (!fOwned->type()->Equals(i->type())) {
                return TConclusionStatus::Fail("different field types for '" + i->name() + "'. Have " + fOwned->type()->ToString() + ", need " + i->type()->ToString());
            }
            schemaNew->AddField(fOwned).Validate();
            columnsNew.emplace_back(Columns[idx]);
        }
    }
    std::swap(Schema, schemaNew);
    std::swap(columnsNew, Columns);
    return TConclusionStatus::Success();
}

TString TGeneralContainer::DebugString() const {
    TStringBuilder result;
    if (RecordsCount) {
        result << "records_count=" << *RecordsCount << ";";
    }
    result << "schema=" << Schema->ToString() << ";";
    return result;
}

TConclusion<std::shared_ptr<arrow::Scalar>> IFieldsConstructor::GetDefaultColumnElementValue(const std::shared_ptr<arrow::Field>& field, const bool force) const {
    AFL_VERIFY(field);
    auto result = DoGetDefaultColumnElementValue(field->name());
    if (result) {
        return result;
    }
    if (force) {
        return NArrow::DefaultScalar(field->type());
    }
    return TConclusionStatus::Fail("have not default value for column " + field->name());
}

}
