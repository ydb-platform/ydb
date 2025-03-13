#include "collection.h"
#include "meta.h"

#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>

namespace NKikimr::NOlap::NIndexes {

std::shared_ptr<IIndexMeta> TIndexesCollection::FindIndexFor(
    const NRequest::TOriginalDataAddress& address, const NArrow::NSSA::EIndexCheckOperation op) const {
    auto it = IndexByOriginalData.find(address);
    if (it == IndexByOriginalData.end()) {
        return nullptr;
    }
    for (auto&& i : it->second) {
        if (!i.second->GetIndexMeta()->IsSkipIndex()) {
            continue;
        }
        if (std::static_pointer_cast<TSkipIndex>(i.second->GetIndexMeta())->IsAppropriateFor(address, op)) {
            return i.second->GetIndexMeta();
        }
    }
    return nullptr;
}

}   // namespace NKikimr::NOlap::NIndexes
