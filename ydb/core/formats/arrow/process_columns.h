#pragma once
#include <ydb/library/conclusion/result.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <functional>

namespace NKikimr::NArrow {

class TSchemaSubset;
class TSchemaLite;

class TColumnOperator {
public:
    enum class EAbsentFieldPolicy {
        Error,
        Verify,
        Skip
    };

    enum class ECheckFieldTypesPolicy {
        Ignore,
        Error,
        Verify
    };

private:
    EAbsentFieldPolicy AbsentColumnPolicy = EAbsentFieldPolicy::Verify;
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

    TColumnOperator& ErrorIfAbsent() {
        AbsentColumnPolicy = EAbsentFieldPolicy::Error;
        return *this;
    }

    TColumnOperator& VerifyIfAbsent() {
        AbsentColumnPolicy = EAbsentFieldPolicy::Verify;
        return *this;
    }

    TColumnOperator& SkipIfAbsent() {
        AbsentColumnPolicy = EAbsentFieldPolicy::Skip;
        return *this;
    }

    TConclusion<std::shared_ptr<arrow::RecordBatch>> AdaptIncomingToDestinationExt(const std::shared_ptr<arrow::RecordBatch>& incoming,
        const std::shared_ptr<TSchemaLite>& dstSchema, const std::function<TConclusionStatus(const ui32, const i32)>& checker,
        const std::function<i32(const std::string&)>& nameResolver) const;

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
