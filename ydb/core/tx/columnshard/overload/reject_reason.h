#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/tx/columnshard/counters/columnshard.h>

namespace NKikimr::NColumnShard::NOverload {

enum class ERejectReason : int {
    YellowChannels                 = 0,
    OverloadByShardWritesInFly     = 1,
    OverloadByShardWritesSizeInFly = 2,

    RejectReasonCount
};

enum class ERejectReasons : ui32 {
    None                           = 0,
    YellowChannels                 = 1 << static_cast<int>(ERejectReason::YellowChannels),
    OverloadByShardWritesInFly     = 1 << static_cast<int>(ERejectReason::OverloadByShardWritesInFly),
    OverloadByShardWritesSizeInFly = 1 << static_cast<int>(ERejectReason::OverloadByShardWritesSizeInFly),
};

static constexpr int RejectReasonCount = static_cast<int>(ERejectReason::RejectReasonCount);

inline ERejectReasons operator|(ERejectReasons a, ERejectReasons b) { return ERejectReasons(ui32(a) | ui32(b)); }
inline ERejectReasons operator&(ERejectReasons a, ERejectReasons b) { return ERejectReasons(ui32(a) & ui32(b)); }
inline ERejectReasons operator-(ERejectReasons a, ERejectReasons b) { return ERejectReasons(ui32(a) & ~ui32(b)); }
inline ERejectReasons& operator|=(ERejectReasons& a, ERejectReasons b) { return a = (a | b); }
inline ERejectReasons& operator&=(ERejectReasons& a, ERejectReasons b) { return a = (a & b); }
inline ERejectReasons& operator-=(ERejectReasons& a, ERejectReasons b) { return a = (a - b); }

inline int RejectReasonIndex(ERejectReason reason) {
    return static_cast<int>(reason);
}

inline ERejectReasons MakeRejectReasons(ERejectReason reason) {
    return ERejectReasons(1 << int(reason));
}

inline ERejectReasons MakeRejectReasons(EOverloadStatus status) {
    switch (status) {
        case EOverloadStatus::Disk:
            return ERejectReasons::YellowChannels;
        case EOverloadStatus::ShardWritesInFly:
            return ERejectReasons::OverloadByShardWritesInFly;
        case EOverloadStatus::ShardWritesSizeInFly:
            return ERejectReasons::OverloadByShardWritesSizeInFly;
        default:
            return ERejectReasons::None;
    }
}

template<class TCallback>
inline void EnumerateRejectReasons(ERejectReasons reasons, TCallback&& callback) {
    for (int i = 0; i < static_cast<int>(ERejectReason::RejectReasonCount); ++i) {
        if ((reasons & ERejectReasons(1 << i)) != ERejectReasons::None) {
            callback(ERejectReason(i));
        }
    }
}

} // namespace NKikimr::NDataShard
