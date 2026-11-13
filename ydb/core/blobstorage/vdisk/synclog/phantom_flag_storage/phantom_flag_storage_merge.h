#pragma once

#include <ydb/core/base/logoblob.h>

#include <span>

namespace NKikimr::NSyncLog {

// Sort in place and return the prefix of distinct keys.
std::span<TLogoBlobID> SortPhantomFlagKeys(std::span<TLogoBlobID> keys);

enum class EPhantomFlagRunMerge : ui8 {
    Union = 0,
    LeftDifference,
};

struct TPhantomFlagRunMergeProgress {
    size_t LeftConsumed = 0;
    size_t RightConsumed = 0;
    size_t Written = 0;
};

// Each input is a block from a strictly increasing run. A final block has eof set.
// Advance the input spans by the returned counts and refill an exhausted input
// before resuming if it has more blocks. Output must not overlap either input.
// LeftDifference can finish with unread right keys once the left run is exhausted.
TPhantomFlagRunMergeProgress MergeSortedPhantomFlagRuns(
    std::span<const TLogoBlobID> left, bool leftEof,
    std::span<const TLogoBlobID> right, bool rightEof,
    std::span<TLogoBlobID> output, EPhantomFlagRunMerge mode = EPhantomFlagRunMerge::Union);

} // namespace NKikimr::NSyncLog
