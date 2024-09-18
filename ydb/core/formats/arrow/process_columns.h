#pragma once
#include <ydb/library/conclusion/result.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NArrow {

class TSchemaSubset;
class TSchemaLite;

class TColumnOperator {
public:
    enum class EExtractProblemsPolicy {
        Null,
        Verify,
        Skip
    };

    enum class ECheckFieldTypesPolicy {
        Ignore,
        Error,
        Verify
    };

private:
    EExtractProblemsPolicy AbsentColumnPolicy = EExtractProblemsPolicy::Verify;
    ECheckFieldTypesPolicy DifferentColumnTypesPolicy = ECheckFieldTypesPolicy::Error;

public:
    TColumnOperator& VerifyOnDifferentFieldTypes() {
        DifferentColumnTypesPolicy = ECheckFieldTypesPolicy::Verify;
        return *this;
    };

    TColumnOperator& ErrorOnDifferentFieldTypes() {
        DifferentColumnTypesPolicy = ECheckFieldTypesPolicy::Error;
        return *this;
    };

    TColumnOperator& IgnoreOnDifferentFieldTypes() {
        DifferentColumnTypesPolicy = ECheckFieldTypesPolicy::Ignore;
        return *this;
    };

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

    template <class TExtChecker>
    TConclusion<std::shared_ptr<arrow::RecordBatch>> AdaptExt(const std::shared_ptr<arrow::RecordBatch>& incoming,
        const std::vector<std::shared_ptr<arrow::Field>>& columns, const TExtChecker& checker) const {
        AFL_VERIFY(incoming);
        AFL_VERIFY(columns.size());
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.reserve(dstSchema->num_fields());
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(dstSchema->num_fields());
        ui32 idx = 0;
        for (auto& dstField : columns) {
            const int index = incoming->schema()->GetFieldIndex(dstField->name());
            if (index > -1) {
                columns.push_back(incoming->column(index));
                fields.emplace_back(dstField);
                auto srcField = incoming->schema()->field(index);
                if (DifferentColumnTypesPolicy != ECheckFieldTypesPolicy::Ignore  && !dstField->Equals(srcField)) {
                    if (DifferentColumnTypesPolicy == ECheckFieldTypesPolicy::Error) {
                        AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "cannot_use_incoming_batch")("reason", "invalid_column_type")(
                            "dst_column", dstField->ToString(true))("src_column", srcField->ToString(true));
                        return TConclusionStatus::Fail("incompatible column types for '" + dstField->name() + "'");
                    } else if (DifferentColumnTypesPolicy == ECheckFieldTypesPolicy::Verify) {
                        AFL_VERIFY(false)("event", "cannot_use_incoming_batch")("reason", "invalid_column_type")(
                            "dst_column", dstField->ToString(true))("src_column", srcField->ToString(true));
                    } else {
                        AFL_VERIFY(false);
                    }
                }
                auto resultCheck = checker(index, idx);
                if (resultCheck.IsFail()) {
                    return resultCheck;
                }
            } else if (AbsentColumnPolicy == EExtractProblemsPolicy::Skip) {
            } else if (AbsentColumnPolicy == EExtractProblemsPolicy::Verify) {
                AFL_VERIFY(false)("event", "cannot_use_incoming_batch")("reason", "absent_field")("dst_column", dstField->ToString(true));
            } else if (AbsentColumnPolicy == EExtractProblemsPolicy::Null) {
                AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "cannot_use_incoming_batch")("reason", "absent_field")(
                    "dst_column", dstField->ToString(true));
                return TConclusionStatus::Fail("not found column '" + field->name() + "'");
            } else {
                AFL_VERIFY(false);
            }
            ++idx;
        }
        if (fields.empty()) {
            return TConclusionStatus::Fail("not found any column");
        }
        return NAdapter::TDataBuilderPolicy<TDataContainer>::Build(std::make_shared<arrow::Schema>(fields), std::move(columns), incoming->num_rows());
    }

    std::shared_ptr<arrow::RecordBatch> Extract(
        const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::string>& columnNames);
    std::shared_ptr<arrow::Table> Extract(const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::string>& columnNames);
    std::shared_ptr<arrow::Table> Extract(
        const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::shared_ptr<arrow::Field>>& columns);
    std::shared_ptr<arrow::RecordBatch> Extract(
        const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::shared_ptr<arrow::Field>>& columns);
    std::shared_ptr<arrow::RecordBatch> Extract(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<TString>& columnNames);
    std::shared_ptr<arrow::Table> Extract(const std::shared_ptr<arrow::Table>& incoming, const std::vector<TString>& columnNames);

    TConclusion<TSchemaSubset> BuildSequentialSubset(
        const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<NArrow::TSchemaLite>& dstSchema);

    TConclusion<std::shared_ptr<arrow::RecordBatch>> Adapt(
        const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<arrow::Schema>& dstSchema, TSchemaSubset* subset = nullptr);
    TConclusion<std::shared_ptr<arrow::Table>> Adapt(
        const std::shared_ptr<arrow::Table>& incoming, const std::shared_ptr<arrow::Schema>& dstSchema, TSchemaSubset* subset = nullptr);
    TConclusion<std::shared_ptr<arrow::RecordBatch>> Adapt(const std::shared_ptr<arrow::RecordBatch>& incoming,
        const std::shared_ptr<NArrow::TSchemaLite>& dstSchema, TSchemaSubset* subset = nullptr);
    TConclusion<std::shared_ptr<arrow::Table>> Adapt(
        const std::shared_ptr<arrow::Table>& incoming, const std::shared_ptr<NArrow::TSchemaLite>& dstSchema, TSchemaSubset* subset = nullptr);

    TConclusion<std::shared_ptr<arrow::RecordBatch>> Reorder(
        const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::string>& columnNames);
    TConclusion<std::shared_ptr<arrow::Table>> Reorder(
        const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::string>& columnNames);
    TConclusion<std::shared_ptr<arrow::RecordBatch>> Reorder(
        const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<TString>& columnNames);
    TConclusion<std::shared_ptr<arrow::Table>> Reorder(const std::shared_ptr<arrow::Table>& incoming, const std::vector<TString>& columnNames);
};

}   // namespace NKikimr::NArrow
