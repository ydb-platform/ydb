#include "similar_packer.h"
#include "stats.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NArrow::NSplitter {

std::optional<TBatchSerializationStat> TSerializationStats::GetStatsForRecordBatch(const std::shared_ptr<arrow::Schema>& schema) const {
    std::optional<TBatchSerializationStat> result;
    for (auto&& i : schema->fields()) {
        auto columnInfo = GetColumnInfo(i->name());
        if (!columnInfo || columnInfo->GetRecordsCount() == 0) {
            return {};
        } else if (!result) {
            result = TBatchSerializationStat();
        }
        result->Merge(*columnInfo);
    }
    return result;
}

std::optional<TBatchSerializationStat> TSerializationStats::GetStatsForRecordBatch(const std::shared_ptr<arrow::RecordBatch>& rb) const {
    return GetStatsForRecordBatch(rb->schema());
}

TSimpleSerializationStat::TSimpleSerializationStat(const ui64 bytes, const ui64 recordsCount, const ui64 rawBytes)
    : SerializedBytes(bytes)
    , RecordsCount(recordsCount)
    , RawBytes(rawBytes) {
//    Y_ABORT_UNLESS(SerializedBytes);
    Y_ABORT_UNLESS(RecordsCount);
    //    Y_ABORT_UNLESS(RawBytes);
}

std::vector<i64> TSimpleSerializationStat::SplitRecords(
    const ui32 recordsCount, const ui32 expectedRecordsCount, const ui32 expectedColumnPageSize, const ui32 maxBlobSize) {
    if (!SerializedBytes || !RecordsCount) {
        return TSimilarPacker::SplitWithExpected(recordsCount, expectedRecordsCount);
    } else {
        const ui32 recordsCountPerExpectedPageSize =
            std::min<ui32>(std::max<ui32>(expectedRecordsCount, expectedColumnPageSize / GetSerializedBytesPerRecord()),
                maxBlobSize / GetSerializedBytesPerRecord());
        return TSimilarPacker::SplitWithExpected(recordsCount, recordsCountPerExpectedPageSize);
    }
}

std::vector<i64> TBatchSerializationStat::SplitRecordsForBlobSize(const i64 recordsCount, const ui64 blobSize) const {
    if (!SerializedBytesPerRecord || blobSize < SerializedBytesPerRecord) {
        return { recordsCount };
    }
    const ui32 recordsCountPerBlob = blobSize / SerializedBytesPerRecord;
    return TSimilarPacker::SplitWithExpected(recordsCount, recordsCountPerBlob);
}

}   // namespace NKikimr::NArrow::NSplitter
