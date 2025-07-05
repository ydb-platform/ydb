#include "constructors.h"

namespace NKikimr::NOlap::NReader::NCommon {

void TSortedPortionsSources::DoInitCursor(const std::shared_ptr<IScanCursor>& cursor) {
    while (HeapSources.size()) {
        bool usage = false;
        if (!context->GetCommonContext()->GetScanCursor()->CheckEntityIsBorder(HeapSources.front(), usage)) {
            std::pop_heap(HeapSources.begin(), HeapSources.end());
            HeapSources.pop_back();
            continue;
        }
        if (usage) {
            HeapSources.front().SetIsStartedByCursor();
        } else {
            std::pop_heap(HeapSources.begin(), HeapSources.end());
            HeapSources.pop_back();
        }
        break;
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple
