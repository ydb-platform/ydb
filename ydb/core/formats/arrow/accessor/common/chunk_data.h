#pragma once
#include "additional_data.h"

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
    YDB_READONLY_DEF(std::shared_ptr<IAdditionalAccessorData>, AdditionalAccessorData);

public:
    TChunkConstructionData(const ui32 recordsCount, const std::shared_ptr<arrow::Scalar>& defaultValue,
        const std::shared_ptr<arrow::DataType>& columnType, const std::shared_ptr<NSerialization::ISerializer>& defaultSerializer,
        const std::optional<ui32>& notNullRecordsCount = std::nullopt,
        std::shared_ptr<IAdditionalAccessorData> additionalAccessorData = nullptr);

    TChunkConstructionData GetSubset(const ui32 recordsCount, const std::optional<ui32>& notNullRecordsCount = std::nullopt) const;

    TChunkConstructionData WithAdditionalAccessorData(std::shared_ptr<IAdditionalAccessorData> additionalAccessorData) const;

    bool HasNullRecordsCount() const {
        return !!NotNullRecordsCount;
    }
    ui32 GetNullRecordsCountVerified() const;
    bool HasAdditionalAccessorData() const {
        return AdditionalAccessorData != nullptr;
    }
};

}   // namespace NKikimr::NArrow::NAccessor
