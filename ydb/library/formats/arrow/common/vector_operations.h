#pragma once

#include <ydb/library/actors/core/log.h>

#include <util/system/types.h>

#include <vector>

namespace NKikimr::NArrow::NUtil {

template <typename T>
class TDefaultErasePolicy {
public:
    void OnEraseItem(const T& /*item*/) const {
    }
    void OnMoveItem(const T& /*item*/, const ui64 /*new_index*/) const {
    }
};

template <typename T, typename ErasePolicy = TDefaultErasePolicy<T>>
void EraseItems(std::vector<T>& container, const std::vector<ui32>& idxsToErase, const ErasePolicy& policy = TDefaultErasePolicy<T>()) {
    if (idxsToErase.empty()) {
        return;
    }
    AFL_VERIFY(idxsToErase.front() < container.size());

    auto itNextEraseIdx = idxsToErase.begin();
    ui64 writeIdx = idxsToErase.front();
    ui64 readIdx = idxsToErase.front();
    while (readIdx != container.size()) {
        AFL_VERIFY(itNextEraseIdx != idxsToErase.end() && readIdx == *itNextEraseIdx);

        policy.OnEraseItem(container[readIdx]);
        ++readIdx;
        ++itNextEraseIdx;
        if (itNextEraseIdx != idxsToErase.end()) {
            AFL_VERIFY(*itNextEraseIdx > *std::prev(itNextEraseIdx));
            AFL_VERIFY(*itNextEraseIdx < container.size());
        }

        const ui64 nextReadIdx = itNextEraseIdx == idxsToErase.end() ? container.size() : *itNextEraseIdx;
        while (readIdx != nextReadIdx) {
            std::swap(container[writeIdx], container[readIdx]);
            policy.OnMoveItem(container[writeIdx], writeIdx);
            ++writeIdx;
            ++readIdx;
        }
    }

    container.resize(writeIdx);
    AFL_VERIFY(itNextEraseIdx == idxsToErase.end());
}

}   // namespace NKikimr::NArrow::NUtil
