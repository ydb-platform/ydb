#pragma once

#include <util/generic/strbuf.h>

#include <initializer_list>

namespace NKikimr {

// Fill with exact StoragePoolName values to enable full sync during lost-data
// recovery and suppress automatic DoNotKeep in these pools.
inline constexpr std::initializer_list<TStringBuf> EmergencyRecoveryPools = {};

inline bool IsEmergencyRecoveryPool(TStringBuf storagePoolName) {
    for (const TStringBuf pool : EmergencyRecoveryPools) {
        if (pool == storagePoolName) {
            return true;
        }
    }
    return false;
}

} // namespace NKikimr
