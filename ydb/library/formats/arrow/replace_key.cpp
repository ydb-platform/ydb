#include "replace_key.h"

namespace NKikimr::NArrow {

size_t TReplaceKeyHelper::LowerBound(const std::vector<TRawReplaceKey>& batchKeys, const TReplaceKey& key, size_t offset) {
    Y_ABORT_UNLESS(offset <= batchKeys.size());
    if (offset == batchKeys.size()) {
        return offset;
    }
    auto start = batchKeys.begin() + offset;
    auto it = std::lower_bound(start, batchKeys.end(), key.ToRaw());
    return it - batchKeys.begin();
}

}
