#pragma once
#include <util/system/types.h>
#include <util/generic/string.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>

#include <vector>
#include <optional>
#include "xx_hash.h"

namespace NKikimr::NArrow::NHash {

class TXX64 {
public:
    enum class ENoColumnPolicy {
        Ignore,
        Verify,
        ReturnEmpty
    };
private:
    ui64 Seed = 0;
    const std::vector<TString> ColumnNames;
    const ENoColumnPolicy NoColumnPolicy;

    std::vector<std::shared_ptr<arrow::Array>> GetColumns(const std::shared_ptr<arrow::RecordBatch>& batch) const;

public:
    TXX64(const std::vector<TString>& columnNames, const ENoColumnPolicy noColumnPolicy, const ui64 seed = 0);
    TXX64(const std::vector<std::string>& columnNames, const ENoColumnPolicy noColumnPolicy, const ui64 seed = 0);

    static void AppendField(const std::shared_ptr<arrow::Array>& array, const int row, NXX64::TStreamStringHashCalcer& hashCalcer);
    static void AppendField(const std::shared_ptr<arrow::Scalar>& scalar, NXX64::TStreamStringHashCalcer& hashCalcer);
    static ui64 CalcHash(const std::shared_ptr<arrow::Scalar>& scalar);
    std::optional<std::vector<ui64>> Execute(const std::shared_ptr<arrow::RecordBatch>& batch) const;
    std::shared_ptr<arrow::Array> ExecuteToArray(const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& hashFieldName) const;
};

}
