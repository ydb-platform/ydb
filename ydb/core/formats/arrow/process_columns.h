#pragma once
#include "common/adapter.h"

#include <ydb/library/conclusion/result.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/string/join.h>

namespace NKikimr::NArrow {

class TColumnOperator {
public:
    enum class EExtractProblemsPolicy {
        Null,
        Verify,
        Skip
    };
private:
    EExtractProblemsPolicy AbsentColumnPolicy = EExtractProblemsPolicy::Verify;

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
        const std::shared_ptr<arrow::Schema>& dstSchema) {
        std::vector<std::shared_ptr<typename NAdapter::TDataBuilderPolicy<TDataContainer>::TColumn>> columns;
        columns.reserve(dstSchema->num_fields());

        for (auto& field : dstSchema->fields()) {
            const int index = srcBatch->schema()->GetFieldIndex(field->name());
            if (index > -1) {
                columns.push_back(srcBatch->column(index));
                auto srcField = srcBatch->schema()->field(index);
                if (field->Equals(srcField)) {
                    AFL_VERIFY(columns.back()->type()->Equals(field->type()))("event", "cannot_use_incoming_batch")("reason", "invalid_column_type")("column", field->name())
                        ("column_type", field->type()->ToString())("incoming_type", columns.back()->type()->ToString());
                } else {
                    AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "cannot_use_incoming_batch")("reason", "invalid_column_type")("column", field->name())
                        ("column_type", field->ToString(true))("incoming_type", srcField->ToString(true));
                    return TConclusionStatus::Fail("incompatible column types");
                }
            } else {
                AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "not_found_column")("column", field->name())
                    ("column_type", field->type()->ToString())("columns", JoinSeq(",", srcBatch->schema()->field_names()));
                return TConclusionStatus::Fail("not found column '" + field->name() + "'");
            }
        }

        return NAdapter::TDataBuilderPolicy<TDataContainer>::Build(dstSchema, std::move(columns), srcBatch->num_rows());
    }

public:
    TColumnOperator& NullIfAbsent() {
        AbsentColumnPolicy = EExtractProblemsPolicy::Null;
        return *this;
    }

    TColumnOperator& VerifyIfAbsent() {
        AbsentColumnPolicy = EExtractProblemsPolicy::Verify;
        return *this;
    }

    TColumnOperator& SkipIfAbsent() {
        AbsentColumnPolicy = EExtractProblemsPolicy::Skip;
        return *this;
    }

    template <class TDataContainer, class TStringType>
    std::shared_ptr<arrow::RecordBatch> Extract(const std::shared_ptr<TDataContainer>& incoming, const std::vector<TStringType>& columnNames) {
        AFL_VERIFY(incoming);
        AFL_VERIFY(columnNames.size());
        auto result = ExtractColumnsValidateImpl(incoming, columnNames);
        switch (AbsentColumnPolicy) {
            case EExtractProblemsPolicy::Verify:
                AFL_VERIFY((ui32)result->num_columns() == columnNames.size())("schema", incoming->schema()->ToString())("required", JoinSeq(",", columnNames));
                break;
            case EExtractProblemsPolicy::Null:
                if ((ui32)result->num_columns() != columnNames.size()) {
                    return nullptr;
                }
                break;
            case EExtractProblemsPolicy::Skip:
                break;
        }
        return result;
    }

    template <class TDataContainer>
    TConclusion<std::shared_ptr<arrow::RecordBatch>> Adapt(const std::shared_ptr<TDataContainer>& incoming, const std::shared_ptr<arrow::Schema>& dstSchema) {
        AFL_VERIFY(incoming);
        AFL_VERIFY(dstSchema);
        switch (AbsentColumnPolicy) {
            case EExtractProblemsPolicy::Null:
                return AdaptColumnsImpl(incoming, dstSchema);
            case EExtractProblemsPolicy::Verify:
                return AdaptColumnsImpl(incoming, dstSchema);
            case EExtractProblemsPolicy::Skip:
                return AdaptColumnsImpl(incoming, dstSchema);
        }
    }

    template <class TDataContainer, class TStringType>
    TConclusion<std::shared_ptr<arrow::RecordBatch>> Reorder(const std::shared_ptr<TDataContainer>& incoming, const std::vector<TStringType>& columnNames) {
        AFL_VERIFY(!!incoming);
        AFL_VERIFY(columnNames.size());
        if (incoming->num_columns() < columnNames.size()) {
            return TConclusionStatus::Fail("not enough columns for exact reordering");
        }
        if (incoming->num_columns() > columnNames.size()) {
            return TConclusionStatus::Fail("need extraction before reorder call");
        }
        auto result = ExtractColumnsValidateImpl(incoming, columnNames);
        AFL_VERIFY(result);
        if (result->num_columns() != columnNames.size()) {
            return TConclusionStatus::Fail("not enough fields for exact reordering");
        }
        return result;
    }
};

}