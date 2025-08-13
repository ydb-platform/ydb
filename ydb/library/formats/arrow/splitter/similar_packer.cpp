#include "similar_packer.h"

#include <ydb/library/actors/core/log.h>

#include <util/string/join.h>
#include <util/string/type.h>

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

std::vector<i64> TSimilarPacker::SplitWithExpected(
    const i64 count, const ui32 expectation, const bool canDrop /*= true*/, const bool canGrow /*= true*/) {
    AFL_VERIFY(canDrop || canGrow);
    AFL_VERIFY(expectation);
    if (count <= expectation) {
        return { count };
    }
    // count > 2 * alpha * (partsCountBase + 1) - condition to use partsCountBase with no correction (+1)
    const i64 alpha = count % expectation;
    const i64 partsCountBase = 1.0 * count / expectation;
    const bool cond = (2 * alpha * (partsCountBase + 1) < count);
    const i64 partsCount = partsCountBase + (cond ? 0 : 1);

    const i64 partResultCount = count / partsCount;
    const i64 sumResultCount = partsCount * partResultCount;
    AFL_VERIFY(sumResultCount <= count)("count", count)("expect", sumResultCount)("propose", partResultCount);
    const ui32 delta = count - sumResultCount;
    AFL_VERIFY(delta < partsCount)("delta", delta)("part_size", partResultCount)("expectation", expectation)("count", count)(
        "parts_count", partsCount);
    std::vector<i64> result(partsCount - delta, partResultCount);
    for (ui32 i = 0; i < delta; ++i) {
        result.emplace_back(partResultCount + 1);
    }
    return result;
}

}   // namespace NKikimr::NArrow::NSplitter
