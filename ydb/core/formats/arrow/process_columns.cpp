#include "process_columns.h"

#include "common/adapter.h"
#include "modifier/schema.h"
#include "modifier/subset.h"

#include <util/string/join.h>

namespace NKikimr::NArrow {

namespace {

template <class T, class TOutput>
class TColumnNameAccessor {
public:
    static const std::string& GetFieldName(const T& val) {
        return val;
    }
};

template <>
class TColumnNameAccessor<std::shared_ptr<arrow::Field>> {
public:
    static const std::string& GetFieldName(const T& val) {
        return val->name();
    }
};

template <class TDataContainer, class TStringContainer>
std::shared_ptr<TDataContainer> ExtractColumnsValidateImpl(
    const std::shared_ptr<TDataContainer>& srcBatch, const std::vector<TStringContainer>& columnNames) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(columnNames.size());
    std::vector<std::shared_ptr<typename NAdapter::TDataBuilderPolicy<TDataContainer>::TColumn>> columns;
    columns.reserve(columnNames.size());

    auto srcSchema = srcBatch->schema();
    for (auto& name : columnNames) {
        const int pos = srcSchema->GetFieldIndex(TColumnNameAccessor<TStringContainer>::GetFieldName(name));
        if (Y_LIKELY(pos > -1)) {
            fields.push_back(srcSchema->field(pos));
            columns.push_back(srcBatch->column(pos));
        }
    }

    return NAdapter::TDataBuilderPolicy<TDataContainer>::Build(std::move(fields), std::move(columns), srcBatch->num_rows());
}

template <class TDataContainer, class TSchemaImpl>
TConclusion<std::shared_ptr<TDataContainer>> AdaptColumnsImpl(
    const std::shared_ptr<TDataContainer>& srcBatch, const std::shared_ptr<TSchemaImpl>& dstSchema, TSchemaSubset* subset) {
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
                AFL_VERIFY(columns.back()->type()->Equals(field->type()))("event", "cannot_use_incoming_batch")("reason", "invalid_column_type")(
                    "column", field->name())("column_type", field->type()->ToString())("incoming_type", columns.back()->type()->ToString());
            } else {
                AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "cannot_use_incoming_batch")("reason", "invalid_column_type")(
                    "column", field->name())("column_type", field->ToString(true))("incoming_type", srcField->ToString(true));
                return TConclusionStatus::Fail("incompatible column types");
            }
        } else if (!subset) {
            AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "not_found_column")("column", field->name())(
                "column_type", field->type()->ToString())("columns", JoinSeq(",", srcBatch->schema()->field_names()));
            return TConclusionStatus::Fail("not found column '" + field->name() + "'");
        }
        ++idx;
    }
    if (subset) {
        *subset = TSchemaSubset(fieldIdx, dstSchema->num_fields());
    }
    return NAdapter::TDataBuilderPolicy<TDataContainer>::Build(std::make_shared<arrow::Schema>(fields), std::move(columns), srcBatch->num_rows());
}

template <class TDataContainer, class TStringContainer>
std::shared_ptr<TDataContainer> ExtractImpl(const TColumnOperator::EExtractProblemsPolicy& policy,
    const std::shared_ptr<TDataContainer>& incoming, const std::vector<TStringContainer>& columnNames) {
    AFL_VERIFY(incoming);
    AFL_VERIFY(columnNames.size());
    auto result = ExtractColumnsValidateImpl(incoming, columnNames);
    switch (policy) {
        case TColumnOperator::EExtractProblemsPolicy::Verify:
            AFL_VERIFY((ui32)result->num_columns() == columnNames.size())("schema", incoming->schema()->ToString())(
                                                          "required", JoinSeq(",", columnNames));
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
TConclusion<std::shared_ptr<TDataContainer>> ReorderImpl(
    const std::shared_ptr<TDataContainer>& incoming, const std::vector<TStringType>& columnNames) {
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

}   // namespace

std::shared_ptr<arrow::RecordBatch> TColumnOperator::Extract(
    const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::string>& columnNames) {
    return ExtractImpl(AbsentColumnPolicy, incoming, columnNames);
}

std::shared_ptr<arrow::Table> TColumnOperator::Extract(
    const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::string>& columnNames) {
    return ExtractImpl(AbsentColumnPolicy, incoming, columnNames);
}

std::shared_ptr<arrow::Table> TColumnOperator::Extract(
    const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::shared_ptr<arrow::Field>>& columns) {
    return ExtractImpl(AbsentColumnPolicy, incoming, columns);
}

std::shared_ptr<arrow::RecordBatch> TColumnOperator::Extract(
    const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<TString>& columnNames) {
    return ExtractImpl(AbsentColumnPolicy, incoming, columnNames);
}

std::shared_ptr<arrow::Table> TColumnOperator::Extract(const std::shared_ptr<arrow::Table>& incoming, const std::vector<TString>& columnNames) {
    return ExtractImpl(AbsentColumnPolicy, incoming, columnNames);
}

NKikimr::TConclusion<std::shared_ptr<arrow::RecordBatch>> TColumnOperator::Adapt(
    const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<arrow::Schema>& dstSchema, TSchemaSubset* subset) {
    return AdaptColumnsImpl(incoming, dstSchema, subset);
}

NKikimr::TConclusion<std::shared_ptr<arrow::Table>> TColumnOperator::Adapt(
    const std::shared_ptr<arrow::Table>& incoming, const std::shared_ptr<arrow::Schema>& dstSchema, TSchemaSubset* subset) {
    return AdaptColumnsImpl(incoming, dstSchema, subset);
}

NKikimr::TConclusion<std::shared_ptr<arrow::RecordBatch>> TColumnOperator::Adapt(
    const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<NArrow::TSchemaLite>& dstSchema, TSchemaSubset* subset) {
    return AdaptColumnsImpl(incoming, dstSchema, subset);
}

NKikimr::TConclusion<std::shared_ptr<arrow::Table>> TColumnOperator::Adapt(
    const std::shared_ptr<arrow::Table>& incoming, const std::shared_ptr<NArrow::TSchemaLite>& dstSchema, TSchemaSubset* subset) {
    return AdaptColumnsImpl(incoming, dstSchema, subset);
}

NKikimr::TConclusion<std::shared_ptr<arrow::RecordBatch>> TColumnOperator::Reorder(
    const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::string>& columnNames) {
    return ReorderImpl(incoming, columnNames);
}

NKikimr::TConclusion<std::shared_ptr<arrow::Table>> TColumnOperator::Reorder(
    const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::string>& columnNames) {
    return ReorderImpl(incoming, columnNames);
}

NKikimr::TConclusion<std::shared_ptr<arrow::RecordBatch>> TColumnOperator::Reorder(
    const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<TString>& columnNames) {
    return ReorderImpl(incoming, columnNames);
}

NKikimr::TConclusion<std::shared_ptr<arrow::Table>> TColumnOperator::Reorder(
    const std::shared_ptr<arrow::Table>& incoming, const std::vector<TString>& columnNames) {
    return ReorderImpl(incoming, columnNames);
}
namespace {
template <class TDataContainer, class TSchemaImpl>
TConclusion<std::shared_ptr<TSchemaSubset>> BuildSequentialSubsetImpl(
    const std::shared_ptr<TDataContainer>& srcBatch, const std::shared_ptr<TSchemaImpl>& dstSchema) {
    TSchemaSubset result;
    AFL_VERIFY(srcBatch);
    AFL_VERIFY(dstSchema);
    if (dstSchema->num_fields() < srcBatch->num_fields()) {
        AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "incorrect columns set: destination must been wider than source")(
            "source", srcBatch->schema()->ToString())("destination", dstSchema->schema()->ToString());
        return TConclusionStatus::Fail("incorrect columns set: destination must been wider than source");
    }
    std::set<ui32> fieldIdx;
    auto itSrc = srcBatch->schema()->fields().begin();
    auto itDst = dstSchema->schema()->fields().begin();
    while (itSrc != srcBatch->schema()->fields().end() && itDst != dstSchema->schema()->fields().end()) {
        if (itSrc->name() != itDst->name()) {
            ++itDst;
        } else {
            fieldIdx.emplace(itDst - dstSchema->schema()->fields().begin());
            if (!(*itDst)->Equals(*itSrc)) {
                AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "cannot_use_incoming_batch")("reason", "invalid_column_type")(
                    "column_type", (*itDst)->ToString(true))("incoming_type", (*itSrc)->ToString(true));
                return TConclusionStatus::Fail("incompatible column types");
            }

            ++itDst;
            ++itSrc;
        }
        ++idx;
    }
    if (itDst == dstSchema->schema()->fields().end() && itSrc != srcBatch->schema()->fields().end()) {
        AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "incorrect columns order in source set")("source", srcBatch->schema()->ToString())(
            "destination", dstSchema->schema()->ToString());
        return TConclusionStatus::Fail("incorrect columns order in source set");
    }
    return TSchemaSubset(fieldIdx, dstSchema->num_fields());
}
}   // namespace

NKikimr::TConclusion<NKikimr::NArrow::TSchemaSubset> TColumnOperator::BuildSequentialSubset(
    const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<NArrow::TSchemaLite>& dstSchema) {
    return BuildSequentialSubsetImpl(incoming, dstSchema);
}

}   // namespace NKikimr::NArrow
