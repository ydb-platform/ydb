#pragma once
#include "defs.h"

namespace NKikimr::NDataShard {

    // Note: must be synchronized with ERejectReasons below
    enum class ERejectReason : int {
        WrongState = 0,
        Dropping = 1,
        OverloadByLag = 2,
        OverloadByProbability = 3,
        OverloadByTxInFly = 4,
        YellowChannels = 5,
        DiskSpace = 6,
        ChangesQueueOverflow = 7,
    };

    // Note: must be synchronized with ERejectReason above
    enum class ERejectReasons : ui32 {
        None = 0,
        WrongState = 1 << 0,
        Dropping = 1 << 1,
        OverloadByLag = 1 << 2,
        OverloadByProbability = 1 << 3,
        OverloadByTxInFly = 1 << 4,
        YellowChannels = 1 << 5,
        DiskSpace = 1 << 6,
        ChangesQueueOverflow = 1 << 7,
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

    template<class TCallback>
    inline void EnumerateRejectReasons(ERejectReasons reasons, TCallback&& callback) {
        for (int i = 0; i < RejectReasonCount; ++i) {
            if ((reasons & ERejectReasons(1 << i)) != ERejectReasons::None) {
                callback(ERejectReason(i));
            }
        }
    }

} // namespace NKikimr::NDataShard
