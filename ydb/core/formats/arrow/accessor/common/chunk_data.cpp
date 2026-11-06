#include "chunk_data.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NArrow::NAccessor {

TChunkConstructionData::TChunkConstructionData(const ui32 recordsCount, const std::shared_ptr<arrow::Scalar>& defaultValue,
    const std::shared_ptr<arrow::DataType>& columnType, const std::shared_ptr<NSerialization::ISerializer>& defaultSerializer,
    const std::optional<ui32>& notNullRecordsCount, std::shared_ptr<IAdditionalAccessorData> additionalAccessorData)
    : RecordsCount(recordsCount)
    , NotNullRecordsCount(notNullRecordsCount)
    , DefaultValue(defaultValue)
    , ColumnType(columnType)
    , DefaultSerializer(defaultSerializer)
    , AdditionalAccessorData(std::move(additionalAccessorData)) {
    YDB_LOG_VERIFY(ColumnType);
    YDB_LOG_VERIFY(RecordsCount);
    YDB_LOG_VERIFY(!NotNullRecordsCount || *NotNullRecordsCount <= RecordsCount,
        {"records", RecordsCount},
        {"notNull", NotNullRecordsCount});
    YDB_LOG_VERIFY(!!DefaultSerializer);
}

TChunkConstructionData TChunkConstructionData::GetSubset(const ui32 recordsCount, const std::optional<ui32>& notNullRecordsCount) const {
    YDB_LOG_VERIFY(recordsCount <= RecordsCount,
        {"sub", recordsCount},
        {"global", RecordsCount});
    return TChunkConstructionData(recordsCount, DefaultValue, ColumnType, DefaultSerializer, notNullRecordsCount, AdditionalAccessorData);
}

TChunkConstructionData TChunkConstructionData::WithAdditionalAccessorData(std::shared_ptr<IAdditionalAccessorData> additionalAccessorData) const {
    return TChunkConstructionData(RecordsCount, DefaultValue, ColumnType, DefaultSerializer, NotNullRecordsCount, std::move(additionalAccessorData));
}

ui32 TChunkConstructionData::GetNullRecordsCountVerified() const {
    YDB_LOG_VERIFY(NotNullRecordsCount);
    return RecordsCount - *NotNullRecordsCount;
}

}   // namespace NKikimr::NArrow::NAccessor
