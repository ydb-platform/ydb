#pragma once

#include <util/generic/utility.h>
#include <util/system/types.h>

namespace NKikimr::NGRpcProxy::V1 {

// Shared by FillBatchedData and UTs: mid-batch reads may return resultOffset < ReadOffset
// when LogicalMessageCount > 1; the ENSURE requires coverage of ReadOffset.
inline ui64 BatchedResultMessageCount(ui64 logicalMessageCount) {
    return Max<ui64>(1, logicalMessageCount);
}

inline bool BatchedResultCoversReadOffset(ui64 resultOffset, ui64 logicalMessageCount, ui64 readOffset) {
    return resultOffset + BatchedResultMessageCount(logicalMessageCount) > readOffset;
}

inline void AdvanceReadOffsetFromBatchedResult(ui64 resultOffset, ui64 logicalMessageCount, ui64& readOffset) {
    readOffset = resultOffset + BatchedResultMessageCount(logicalMessageCount);
}

} // namespace NKikimr::NGRpcProxy::V1
