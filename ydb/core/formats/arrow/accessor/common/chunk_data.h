#pragma once
#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NArrow::NSerialization {
class ISerializer;
}

namespace NKikimr::NArrow::NAccessor {

class TChunkConstructionData {
private:
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY_DEF(std::optional<ui32>, NotNullRecordsCount);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, DefaultValue);
    YDB_READONLY_DEF(std::shared_ptr<arrow::DataType>, ColumnType);
    YDB_READONLY_DEF(std::shared_ptr<NSerialization::ISerializer>, DefaultSerializer);

public:
    TChunkConstructionData(const ui32 recordsCount, const std::shared_ptr<arrow::Scalar>& defaultValue,
        const std::shared_ptr<arrow::DataType>& columnType, const std::shared_ptr<NSerialization::ISerializer>& defaultSerializer,
        const std::optional<ui32>& notNullRecordsCount = std::nullopt);

    TChunkConstructionData GetSubset(const ui32 recordsCount, const std::optional<ui32>& notNullRecordsCount = std::nullopt) const;

    bool HasNullRecordsCount() const {
        return !!NotNullRecordsCount;
    }
    ui32 GetNullRecordsCountVerified() const;
};

}   // namespace NKikimr::NArrow::NAccessor
