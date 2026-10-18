#include "index_info_ext.h"

namespace NKikimr::NOlap::NIndexes::NMax {

std::shared_ptr<TIndexMeta> GetIndexMeta(const TIndexInfo& indexInfo, const ui32 columnId) {
    for (auto&& i : indexInfo.GetIndexes()) {
        if (i.second->GetClassName() != TIndexMeta::GetClassNameStatic()) {
            continue;
        }
        auto maxIndex = static_pointer_cast<TIndexMeta>(i.second.GetObjectPtr());
        if (maxIndex->GetColumnId() == columnId) {
            return maxIndex;
        }
    }
    return nullptr;
}

}   // namespace NKikimr::NOlap::NIndexes::NMax
