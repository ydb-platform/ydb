#pragma once

#include <ydb/library/formats/arrow/csv/converter/csv_arrow.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace NKikimr::NFormats {

class TArrowCSVTable: public TArrowCSV {
    using TArrowCSV::TArrowCSV;
public:
    /// If header is true read column names from first line after skipRows. Parse columns as strings in this case.
    /// @note It's possible to skip header with skipRows and use typed columns instead.
    static arrow::Result<TArrowCSV> Create(const std::vector<NYdb::NTable::TTableColumn>& columns, bool header = false);

private:
    static NYdb::TTypeParser ExtractType(const NYdb::TType& type);
    static arrow::Result<std::shared_ptr<arrow::DataType>> GetArrowType(const NYdb::TType& type);
    static arrow::Result<std::shared_ptr<arrow::DataType>> GetCSVArrowType(const NYdb::TType& type);
};

}