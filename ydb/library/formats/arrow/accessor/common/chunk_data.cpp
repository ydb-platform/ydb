#include "chunk_data.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NArrow::NAccessor {

TChunkConstructionData::TChunkConstructionData(const ui32 recordsCount, const std::shared_ptr<arrow::Scalar>& defaultValue,
    const std::shared_ptr<arrow::DataType>& columnType, const std::shared_ptr<NSerialization::ISerializer>& defaultSerializer)
    : RecordsCount(recordsCount)
    , DefaultValue(defaultValue)
    , ColumnType(columnType)
    , DefaultSerializer(defaultSerializer) {
    AFL_VERIFY(ColumnType);
    AFL_VERIFY(RecordsCount);
    AFL_VERIFY(!!DefaultSerializer);
}

}   // namespace NKikimr::NArrow::NAccessor
