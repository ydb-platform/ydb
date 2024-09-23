#pragma once
#include <ydb/library/accessor/accessor.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

namespace NKikimr::NArrow::NAccessor {

class TChunkConstructionData {
private:
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, DefaultValue);
    YDB_READONLY_DEF(std::shared_ptr<arrow::DataType>, ColumnType);

public:
    TChunkConstructionData(
        const ui32 recordsCount, const std::shared_ptr<arrow::Scalar>& defaultValue, const std::shared_ptr<arrow::DataType>& columnType)
        : RecordsCount(recordsCount)
        , DefaultValue(defaultValue)
        , ColumnType(columnType) {
    }
};

}   // namespace NKikimr::NArrow::NAccessor
