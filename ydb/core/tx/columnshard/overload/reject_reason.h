#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/tx/columnshard/counters/columnshard.h>

namespace NKikimr::NColumnShard::NOverload {

    enum class ERejectReason : ui32 {
        YellowChannels = 0
    };

    enum class ERejectReasons : ui32 {
        None = 0,
        YellowChannels = 1 << static_cast<ui32>(ERejectReason::YellowChannels),
    };

    // Note: must be synchronized with both enums above
    static constexpr int RejectReasonCount = 8;

    inline ERejectReasons operator|(ERejectReasons a, ERejectReasons b) { return ERejectReasons(ui32(a) | ui32(b)); }
    inline ERejectReasons operator&(ERejectReasons a, ERejectReasons b) { return ERejectReasons(ui32(a) & ui32(b)); }
    inline ERejectReasons operator-(ERejectReasons a, ERejectReasons b) { return ERejectReasons(ui32(a) & ~ui32(b)); }
    inline ERejectReasons& operator|=(ERejectReasons& a, ERejectReasons b) { return a = (a | b); }
    inline ERejectReasons& operator&=(ERejectReasons& a, ERejectReasons b) { return a = (a & b); }
    inline ERejectReasons& operator-=(ERejectReasons& a, ERejectReasons b) { return a = (a - b); }

    inline int RejectReasonIndex(ERejectReason reason) {
        return int(reason);
    }

    inline ERejectReasons MakeRejectReasons(ERejectReason reason) {
        return ERejectReasons(1 << int(reason));
    }

    inline ERejectReasons MakeRejectReasons(EOverloadStatus status) {
        switch (status) {
            case EOverloadStatus::Disk:
                return ERejectReasons::YellowChannels;
            default:
                return ERejectReasons::None;
        }
    }

    template<class TCallback>
    inline void EnumerateRejectReasons(ERejectReasons reasons, TCallback&& callback) {
        for (int i = 0; i < RejectReasonCount; ++i) {
            if ((reasons & ERejectReasons(1 << i)) != ERejectReasons::None) {
                callback(ERejectReason(i));
            }
        }
    }
} // namespace NKikimr::NDataShard
