#include "process_columns.h"
#include "common/adapter.h"
#include "modifier/subset.h"

#include <util/string/join.h>

namespace NKikimr::NArrow {

namespace {
template <class TDataContainer, class TStringImpl>
std::shared_ptr<TDataContainer> ExtractColumnsValidateImpl(const std::shared_ptr<TDataContainer>& srcBatch,
    const std::vector<TStringImpl>& columnNames) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(columnNames.size());
    std::vector<std::shared_ptr<typename NAdapter::TDataBuilderPolicy<TDataContainer>::TColumn>> columns;
    columns.reserve(columnNames.size());

    auto srcSchema = srcBatch->schema();
    for (auto& name : columnNames) {
        const int pos = srcSchema->GetFieldIndex(name);
        if (Y_LIKELY(pos > -1)) {
            fields.push_back(srcSchema->field(pos));
            columns.push_back(srcBatch->column(pos));
        }
    }

    return NAdapter::TDataBuilderPolicy<TDataContainer>::Build(std::move(fields), std::move(columns), srcBatch->num_rows());
}

template <class TDataContainer>
TConclusion<std::shared_ptr<TDataContainer>> AdaptColumnsImpl(const std::shared_ptr<TDataContainer>& srcBatch,
    const std::shared_ptr<arrow::Schema>& dstSchema, TSchemaSubset* subset) {
    AFL_VERIFY(srcBatch);
    AFL_VERIFY(dstSchema);
    std::vector<std::shared_ptr<typename NAdapter::TDataBuilderPolicy<TDataContainer>::TColumn>> columns;
    columns.reserve(dstSchema->num_fields());
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(dstSchema->num_fields());
    std::set<ui32> fieldIdx;
    ui32 idx = 0;
    for (auto& field : dstSchema->fields()) {
        const int index = srcBatch->schema()->GetFieldIndex(field->name());
        if (index > -1) {
            if (subset) {
                fieldIdx.emplace(idx);
            }
            columns.push_back(srcBatch->column(index));
            fields.emplace_back(field);
            auto srcField = srcBatch->schema()->field(index);
            if (field->Equals(srcField)) {
                AFL_VERIFY(columns.back()->type()->Equals(field->type()))("event", "cannot_use_incoming_batch")("reason", "invalid_column_type")("column", field->name())
                    ("column_type", field->type()->ToString())("incoming_type", columns.back()->type()->ToString());
            } else {
                AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "cannot_use_incoming_batch")("reason", "invalid_column_type")("column", field->name())
                    ("column_type", field->ToString(true))("incoming_type", srcField->ToString(true));
                return TConclusionStatus::Fail("incompatible column types");
            }
        } else if (!subset) {
            AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "not_found_column")("column", field->name())
                ("column_type", field->type()->ToString())("columns", JoinSeq(",", srcBatch->schema()->field_names()));
            return TConclusionStatus::Fail("not found column '" + field->name() + "'");
        }
        ++idx;
    }
    if (subset) {
        *subset = TSchemaSubset(fieldIdx, dstSchema->num_fields());
    }
    return NAdapter::TDataBuilderPolicy<TDataContainer>::Build(std::make_shared<arrow::Schema>(fields), std::move(columns), srcBatch->num_rows());
}

template <class TDataContainer, class TStringType>
std::shared_ptr<TDataContainer> ExtractImpl(const TColumnOperator::EExtractProblemsPolicy& policy,
    const std::shared_ptr<TDataContainer>& incoming, const std::vector<TStringType>& columnNames) {
    AFL_VERIFY(incoming);
    AFL_VERIFY(columnNames.size());
    auto result = ExtractColumnsValidateImpl(incoming, columnNames);
    switch (policy) {
        case TColumnOperator::EExtractProblemsPolicy::Verify:
            AFL_VERIFY((ui32)result->num_columns() == columnNames.size())("schema", incoming->schema()->ToString())("required", JoinSeq(",", columnNames));
            break;
        case TColumnOperator::EExtractProblemsPolicy::Null:
            if ((ui32)result->num_columns() != columnNames.size()) {
                return nullptr;
            }
            break;
        case TColumnOperator::EExtractProblemsPolicy::Skip:
            break;
    }
    return result;
}

template <class TDataContainer, class TStringType>
TConclusion<std::shared_ptr<TDataContainer>> ReorderImpl(const std::shared_ptr<TDataContainer>& incoming, const std::vector<TStringType>& columnNames) {
    AFL_VERIFY(!!incoming);
    AFL_VERIFY(columnNames.size());
    if ((ui32)incoming->num_columns() < columnNames.size()) {
        return TConclusionStatus::Fail("not enough columns for exact reordering");
    }
    if ((ui32)incoming->num_columns() > columnNames.size()) {
        return TConclusionStatus::Fail("need extraction before reorder call");
    }
    auto result = ExtractColumnsValidateImpl(incoming, columnNames);
    AFL_VERIFY(result);
    if ((ui32)result->num_columns() != columnNames.size()) {
        return TConclusionStatus::Fail("not enough fields for exact reordering");
    }
    return result;
}

}

std::shared_ptr<arrow::RecordBatch> TColumnOperator::Extract(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::string>& columnNames) {
    return ExtractImpl(AbsentColumnPolicy, incoming, columnNames);
}

std::shared_ptr<arrow::Table> TColumnOperator::Extract(const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::string>& columnNames) {
    return ExtractImpl(AbsentColumnPolicy, incoming, columnNames);
}

std::shared_ptr<arrow::RecordBatch> TColumnOperator::Extract(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<TString>& columnNames) {
    return ExtractImpl(AbsentColumnPolicy, incoming, columnNames);
}

std::shared_ptr<arrow::Table> TColumnOperator::Extract(const std::shared_ptr<arrow::Table>& incoming, const std::vector<TString>& columnNames) {
    return ExtractImpl(AbsentColumnPolicy, incoming, columnNames);
}

NKikimr::TConclusion<std::shared_ptr<arrow::RecordBatch>> TColumnOperator::Adapt(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<arrow::Schema>& dstSchema, TSchemaSubset* subset) {
    return AdaptColumnsImpl(incoming, dstSchema, subset);
}

NKikimr::TConclusion<std::shared_ptr<arrow::Table>> TColumnOperator::Adapt(const std::shared_ptr<arrow::Table>& incoming, const std::shared_ptr<arrow::Schema>& dstSchema, TSchemaSubset* subset) {
    return AdaptColumnsImpl(incoming, dstSchema, subset);
}

NKikimr::TConclusion<std::shared_ptr<arrow::RecordBatch>> TColumnOperator::Reorder(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::string>& columnNames) {
    return ReorderImpl(incoming, columnNames);
}

NKikimr::TConclusion<std::shared_ptr<arrow::Table>> TColumnOperator::Reorder(const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::string>& columnNames) {
    return ReorderImpl(incoming, columnNames);
}

NKikimr::TConclusion<std::shared_ptr<arrow::RecordBatch>> TColumnOperator::Reorder(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<TString>& columnNames) {
    return ReorderImpl(incoming, columnNames);
}

NKikimr::TConclusion<std::shared_ptr<arrow::Table>> TColumnOperator::Reorder(const std::shared_ptr<arrow::Table>& incoming, const std::vector<TString>& columnNames) {
    return ReorderImpl(incoming, columnNames);
}

}