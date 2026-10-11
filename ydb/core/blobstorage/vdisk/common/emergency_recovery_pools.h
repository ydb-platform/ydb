#pragma once

#include <util/generic/strbuf.h>

#include <initializer_list>

namespace NKikimr {

// Fill with StoragePoolName prefixes to enable full sync during lost-data
// recovery and suppress automatic DoNotKeep in matching pools. Empty prefixes are ignored.
inline constexpr std::initializer_list<TStringBuf> EmergencyRecoveryPoolPrefixes = {};

inline bool IsEmergencyRecoveryPool(TStringBuf storagePoolName,
        std::initializer_list<TStringBuf> prefixes = EmergencyRecoveryPoolPrefixes) {
    for (const TStringBuf prefix : prefixes) {
        if (!prefix.empty() && storagePoolName.StartsWith(prefix)) {
            return true;
        }
    }
    return false;
}

} // namespace NKikimr
