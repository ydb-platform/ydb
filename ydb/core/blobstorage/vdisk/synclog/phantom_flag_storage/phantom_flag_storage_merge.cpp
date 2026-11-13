#include "phantom_flag_storage_merge.h"

#include <algorithm>

namespace NKikimr::NSyncLog {

std::span<TLogoBlobID> SortPhantomFlagKeys(std::span<TLogoBlobID> keys) {
    if (keys.size() < 2) {
        return keys;
    }
    std::sort(keys.begin(), keys.end());
    const auto uniqueEnd = std::unique(keys.begin(), keys.end());
    return keys.first(static_cast<size_t>(uniqueEnd - keys.begin()));
}

TPhantomFlagRunMergeProgress MergeSortedPhantomFlagRuns(
    std::span<const TLogoBlobID> left, bool leftEof,
    std::span<const TLogoBlobID> right, bool rightEof,
    std::span<TLogoBlobID> output, EPhantomFlagRunMerge mode)
{
    TPhantomFlagRunMergeProgress progress;
    const bool mergeUnion = mode == EPhantomFlagRunMerge::Union;
    while (progress.Written < output.size()) {
        const bool hasLeft = progress.LeftConsumed < left.size();
        const bool hasRight = progress.RightConsumed < right.size();
        if ((!hasLeft && !leftEof) || (!hasRight && !rightEof)) {
            // An unread key can precede or match the other run's next key.
            break;
        }

        if (!hasLeft) {
            if (!hasRight || !mergeUnion) {
                break;
            }
            output[progress.Written++] = right[progress.RightConsumed++];
            continue;
        }
        if (!hasRight) {
            output[progress.Written++] = left[progress.LeftConsumed++];
            continue;
        }

        const TLogoBlobID& leftKey = left[progress.LeftConsumed];
        const TLogoBlobID& rightKey = right[progress.RightConsumed];
        const int order = leftKey.Compare(rightKey);
        if (order < 0) {
            output[progress.Written++] = leftKey;
            ++progress.LeftConsumed;
        } else if (order > 0) {
            if (mergeUnion) {
                output[progress.Written++] = rightKey;
            }
            ++progress.RightConsumed;
        } else {
            if (mergeUnion) {
                output[progress.Written++] = leftKey;
            }
            ++progress.LeftConsumed;
            ++progress.RightConsumed;
        }
    }
    return progress;
}

} // namespace NKikimr::NSyncLog
