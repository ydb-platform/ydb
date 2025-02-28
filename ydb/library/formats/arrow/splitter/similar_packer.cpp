#include "similar_packer.h"

#include <ydb/library/actors/core/log.h>

#include <util/string/join.h>

namespace NKikimr::NArrow::NSplitter {

std::vector<ui32> TSimilarPacker::SizesToRecordsCount(
    const ui32 serializedRecordsCount, const TString& dataSerialization, const std::vector<ui64>& splitPartSizesExt) {
    auto splitPartSizesLocal = splitPartSizesExt;
    Y_ABORT_UNLESS(serializedRecordsCount);
    {
        ui32 sumSizes = 0;
        for (auto&& i : splitPartSizesExt) {
            sumSizes += i;
        }
        Y_ABORT_UNLESS(sumSizes <= dataSerialization.size());

        if (sumSizes < dataSerialization.size()) {
            splitPartSizesLocal.emplace_back(dataSerialization.size() - sumSizes);
        }
    }
    Y_ABORT_UNLESS(dataSerialization.size() > splitPartSizesLocal.size());
    std::vector<ui32> recordsCount;
    i64 remainedRecordsCount = serializedRecordsCount;
    const double rowsPerByte = 1.0 * serializedRecordsCount / dataSerialization.size();
    i32 remainedParts = splitPartSizesLocal.size();
    for (ui32 idx = 0; idx < splitPartSizesLocal.size(); ++idx) {
        AFL_VERIFY(remainedRecordsCount >= remainedParts)("remained_records_count", remainedRecordsCount)("remained_parts", remainedParts)(
            "idx", idx)("size", splitPartSizesLocal.size())("sizes", JoinSeq(",", splitPartSizesLocal))("data_size", dataSerialization.size());
        --remainedParts;
        i64 expectedRecordsCount = rowsPerByte * splitPartSizesLocal[idx];
        if (expectedRecordsCount < 1) {
            expectedRecordsCount = 1;
        } else if (remainedRecordsCount < expectedRecordsCount + remainedParts) {
            expectedRecordsCount = remainedRecordsCount - remainedParts;
        }
        if (idx + 1 == splitPartSizesLocal.size()) {
            expectedRecordsCount = remainedRecordsCount;
        }
        Y_ABORT_UNLESS(expectedRecordsCount);
        recordsCount.emplace_back(expectedRecordsCount);
        remainedRecordsCount -= expectedRecordsCount;
        Y_ABORT_UNLESS(remainedRecordsCount >= 0);
    }
    Y_ABORT_UNLESS(remainedRecordsCount == 0);
    return recordsCount;
}

}   // namespace NKikimr::NArrow::NSplitter
