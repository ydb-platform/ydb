#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/status.h>

#include <cstdint>
#include <string>
#include <vector>

namespace arrow {
class Table;
}

namespace NKikimr::NOlap {

// Consumes decoded, visible rows from a scan at a fixed schema and snapshot.
// The caller must fence concurrent writes and resolve target column IDs against
// that schema before constructing this validator. Successful batches alone do
// not prove the constraint: Finish() must follow a successful scan EOF.
class TNotNullValidator {
public:
    explicit TNotNullValidator(std::vector<std::string> columnNames);

    arrow::Status AddBatch(const arrow::Table& batch);
    arrow::Status Finish();
    arrow::Status Fail(const std::string& errorMessage);

    bool IsValidated() const {
        return State == EState::Validated;
    }

    bool IsFailed() const {
        return State == EState::Failed;
    }

    uint64_t GetCheckedRows() const {
        return CheckedRows;
    }

    const std::string& GetErrorMessage() const {
        return ErrorMessage;
    }

private:
    enum class EState {
        Scanning,
        Validated,
        Failed,
    };

    const std::vector<std::string> ColumnNames;
    EState State = EState::Scanning;
    uint64_t CheckedRows = 0;
    std::string ErrorMessage;
};

}   // namespace NKikimr::NOlap
