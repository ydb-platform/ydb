#include "index_info_ext.h"

namespace NKikimr::NOlap::NIndexes::NMinMax {

std::shared_ptr<TIndexMeta> GetIndexMeta(const TIndexInfo& indexInfo, const ui32 columnId) {
    for (auto&& i : indexInfo.GetIndexes()) {
        if (i.second->GetClassName() != TIndexMeta::GetClassNameStatic()) {
            continue;
        }
        auto minMaxIndex = static_pointer_cast<TIndexMeta>(i.second.GetObjectPtr());
        if (minMaxIndex->GetColumnId() == columnId) {
            return minMaxIndex;
        }
    }
    return nullptr;
}

}   // namespace NKikimr::NOlap::NIndexes::NMinMax
