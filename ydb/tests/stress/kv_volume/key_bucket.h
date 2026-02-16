#pragma once

#include "types.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/rwlock.h>

namespace NKvVolumeStress {

class TKeyBucket {
public:
    void AddKey(const TString& key, const TKeyInfo& keyInfo);
    void AddKeys(const TVector<std::pair<TString, TKeyInfo>>& keys);
    TVector<std::pair<TString, TKeyInfo>> PickKeys(ui32 count, bool erase);
    TVector<std::pair<TString, TKeyInfo>> Drain();

private:
    static TVector<size_t> SelectRandomIndices(size_t totalCount, size_t selectCount);

    struct TStoredKey {
        TString Key;
        TKeyInfo Info;
    };

    TRWMutex Mutex_;
    TVector<TStoredKey> Keys_;
};

} // namespace NKvVolumeStress
