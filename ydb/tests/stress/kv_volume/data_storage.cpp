#include "data_storage.h"

#include "utils.h"

#include <algorithm>

namespace NKvVolumeStress {

void TDataStorage::AddKey(const TString& actionName, const TString& key, const TKeyInfo& keyInfo) {
    std::lock_guard lock(Mutex_);
    KeysByAction_[actionName][key] = keyInfo;
}

TVector<std::pair<TString, TKeyInfo>> TDataStorage::PickKeys(const TVector<TString>& actionNames, ui32 count, bool erase) {
    std::lock_guard lock(Mutex_);

    TVector<std::pair<TString, TKeyInfo>> candidates;
    THashSet<TString> seen;
    for (const TString& actionName : actionNames) {
        const auto actionIt = KeysByAction_.find(actionName);
        if (actionIt == KeysByAction_.end()) {
            continue;
        }

        for (const auto& [key, info] : actionIt->second) {
            if (seen.insert(key).second) {
                candidates.emplace_back(key, info);
            }
        }
    }

    if (candidates.empty()) {
        return {};
    }

    std::shuffle(candidates.begin(), candidates.end(), RandomEngine());
    const ui32 limit = std::min<ui32>(count, candidates.size());
    candidates.resize(limit);

    if (erase) {
        for (const auto& [key, _] : candidates) {
            for (const TString& actionName : actionNames) {
                auto actionIt = KeysByAction_.find(actionName);
                if (actionIt == KeysByAction_.end()) {
                    continue;
                }
                if (actionIt->second.erase(key)) {
                    break;
                }
            }
        }
    }

    return candidates;
}

} // namespace NKvVolumeStress
