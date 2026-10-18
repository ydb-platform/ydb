#include "index_info_ext.h"

namespace NKikimr::NOlap::NIndexes {

std::vector<std::shared_ptr<TSkipIndex>> FindSkipIndexes(
    const TIndexInfo& indexInfo, const NRequest::TOriginalDataAddress& originalDataAddress, const NArrow::NSSA::TIndexCheckOperation& op) {
    std::vector<std::shared_ptr<TSkipIndex>> result;
    for (auto&& [_, i] : indexInfo.GetIndexes()) {
        if (!i->IsSkipIndex()) {
            continue;
        }
        auto skipIndex = std::static_pointer_cast<TSkipIndex>(i.GetObjectPtrVerified());
        if (skipIndex->IsAppropriateFor(originalDataAddress, op)) {
            result.emplace_back(skipIndex);
        }
    }
    return result;
}

}   // namespace NKikimr::NOlap::NIndexes
