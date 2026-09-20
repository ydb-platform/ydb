#include "validator.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>

#include <limits>
#include <set>
#include <utility>

namespace NKikimr::NOlap {

TNotNullValidator::TNotNullValidator(std::vector<std::string> columnNames)
    : ColumnNames(std::move(columnNames))
{
    if (ColumnNames.empty()) {
        (void)Fail("No columns supplied for NOT NULL validation");
        return;
    }
    std::set<std::string> names;
    for (const auto& name : ColumnNames) {
        if (!names.insert(name).second) {
            (void)Fail("Duplicate column supplied for NOT NULL validation: " + name);
            return;
        }
    }
}

arrow::Status TNotNullValidator::AddBatch(const arrow::Table& batch) {
    if (State != EState::Scanning) {
        return Fail("Data received after NOT NULL validation finished");
    }
    const auto status = batch.Validate();
    if (!status.ok()) {
        return Fail("Invalid batch during NOT NULL validation: " + status.ToString());
    }
    for (const auto& name : ColumnNames) {
        const auto indices = batch.schema()->GetAllFieldIndices(name);
        if (indices.size() != 1) {
            return Fail("Missing or ambiguous column during NOT NULL validation: " + name);
        }
        const auto& column = batch.column(indices.front());
        // Internal accessors may be encoded, but scan output must be decoded.
        // Dictionary entries can themselves contain NULLs, which an indices
        // bitmap does not describe. Never treat such a projection as proof.
        if (column->type()->id() == arrow::Type::DICTIONARY) {
            return Fail("Expected decoded column during NOT NULL validation: " + name);
        }
        for (const auto& chunk : column->chunks()) {
            if (chunk->null_count() != 0) {
                return Fail("Column contains NULL values: " + name);
            }
        }
    }
    if (static_cast<uint64_t>(batch.num_rows()) > std::numeric_limits<uint64_t>::max() - CheckedRows) {
        return Fail("Row count overflow during NOT NULL validation");
    }
    CheckedRows += batch.num_rows();
    return arrow::Status::OK();
}

arrow::Status TNotNullValidator::Finish() {
    if (State == EState::Failed) {
        return arrow::Status::Invalid(ErrorMessage);
    }
    State = EState::Validated;
    return arrow::Status::OK();
}

arrow::Status TNotNullValidator::Fail(const std::string& errorMessage) {
    if (State != EState::Failed) {
        State = EState::Failed;
        ErrorMessage = errorMessage.empty() ? "NOT NULL validation failed" : errorMessage;
    }
    return arrow::Status::Invalid(ErrorMessage);
}

}   // namespace NKikimr::NOlap
