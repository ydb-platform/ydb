#pragma once
#include <ydb/library/conclusion/result.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NArrow {

class TSchemaSubset;

class TColumnOperator {
public:
    enum class EExtractProblemsPolicy {
        Null,
        Verify,
        Skip
    };
private:
    EExtractProblemsPolicy AbsentColumnPolicy = EExtractProblemsPolicy::Verify;

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

    std::shared_ptr<arrow::RecordBatch> Extract(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::string>& columnNames);
    std::shared_ptr<arrow::Table> Extract(const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::string>& columnNames);
    std::shared_ptr<arrow::RecordBatch> Extract(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<TString>& columnNames);
    std::shared_ptr<arrow::Table> Extract(const std::shared_ptr<arrow::Table>& incoming, const std::vector<TString>& columnNames);

    TConclusion<std::shared_ptr<arrow::RecordBatch>> Adapt(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<arrow::Schema>& dstSchema, TSchemaSubset* subset = nullptr);
    TConclusion<std::shared_ptr<arrow::Table>> Adapt(const std::shared_ptr<arrow::Table>& incoming, const std::shared_ptr<arrow::Schema>& dstSchema, TSchemaSubset* subset = nullptr);

    TConclusion<std::shared_ptr<arrow::RecordBatch>> Reorder(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::string>& columnNames);
    TConclusion<std::shared_ptr<arrow::Table>> Reorder(const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::string>& columnNames);
    TConclusion<std::shared_ptr<arrow::RecordBatch>> Reorder(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<TString>& columnNames);
    TConclusion<std::shared_ptr<arrow::Table>> Reorder(const std::shared_ptr<arrow::Table>& incoming, const std::vector<TString>& columnNames);
};

}