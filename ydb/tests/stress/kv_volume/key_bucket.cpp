#include "key_bucket.h"

#include "utils.h"

#include <algorithm>
#include <functional>
#include <random>

namespace NKvVolumeStress {

void TKeyBucket::AddKey(const TString& key, const TKeyInfo& keyInfo) {
    TWriteGuard lock(Mutex_);
    Keys_.push_back(TStoredKey{key, keyInfo});
}

void TKeyBucket::AddKeys(const TVector<std::pair<TString, TKeyInfo>>& keys) {
    if (keys.empty()) {
        return;
    }

    TWriteGuard lock(Mutex_);
    Keys_.reserve(Keys_.size() + keys.size());
    for (const auto& [key, keyInfo] : keys) {
        Keys_.push_back(TStoredKey{key, keyInfo});
    }
}

TVector<std::pair<TString, TKeyInfo>> TKeyBucket::PickKeys(ui32 count, bool erase) {
    if (count == 0) {
        return {};
    }

    if (!erase) {
        TReadGuard lock(Mutex_);
        const TVector<size_t> selectedIndices = SelectRandomIndices(Keys_.size(), count);
        TVector<std::pair<TString, TKeyInfo>> pickedKeys;
        pickedKeys.reserve(selectedIndices.size());
        for (const size_t idx : selectedIndices) {
            const auto& key = Keys_[idx];
            pickedKeys.emplace_back(key.Key, key.Info);
        }
        return pickedKeys;
    }

    TWriteGuard lock(Mutex_);
    TVector<size_t> selectedIndices = SelectRandomIndices(Keys_.size(), count);
    std::sort(selectedIndices.begin(), selectedIndices.end(), std::greater<size_t>());

    TVector<std::pair<TString, TKeyInfo>> pickedKeys;
    pickedKeys.reserve(selectedIndices.size());
    for (const size_t idx : selectedIndices) {
        const size_t lastIdx = Keys_.size() - 1;
        if (idx != lastIdx) {
            std::swap(Keys_[idx], Keys_[lastIdx]);
        }
        pickedKeys.emplace_back(std::move(Keys_.back().Key), Keys_.back().Info);
        Keys_.pop_back();
    }

    return pickedKeys;
}

TVector<std::pair<TString, TKeyInfo>> TKeyBucket::Drain() {
    TWriteGuard lock(Mutex_);

    TVector<std::pair<TString, TKeyInfo>> remaining;
    remaining.reserve(Keys_.size());
    for (auto& key : Keys_) {
        remaining.emplace_back(std::move(key.Key), key.Info);
    }
    Keys_.clear();

    return remaining;
}

TVector<size_t> TKeyBucket::SelectRandomIndices(size_t totalCount, size_t selectCount) {
    const size_t limit = std::min(totalCount, selectCount);
    TVector<size_t> selected;
    selected.reserve(limit);

    if (limit == 0) {
        return selected;
    }

    for (size_t i = 0; i < totalCount; ++i) {
        if (i < limit) {
            selected.push_back(i);
            continue;
        }

        std::uniform_int_distribution<size_t> distribution(0, i);
        const size_t replaceIdx = distribution(RandomEngine());
        if (replaceIdx < limit) {
            selected[replaceIdx] = i;
        }
    }

    return selected;
}

} // namespace NKvVolumeStress
