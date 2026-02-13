#pragma once

#include "types.h"

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <mutex>

namespace NKvVolumeStress {

class TDataStorage {
public:
    void AddKey(const TString& actionName, const TString& key, const TKeyInfo& keyInfo);
    TVector<std::pair<TString, TKeyInfo>> PickKeys(const TVector<TString>& actionNames, ui32 count, bool erase);

private:
    std::mutex Mutex_;
    THashMap<TString, THashMap<TString, TKeyInfo>> KeysByAction_;
};

} // namespace NKvVolumeStress
