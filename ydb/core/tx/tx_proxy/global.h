#pragma once
#include <util/system/types.h>

namespace NKikimr::NTxProxy {
class TLimits {
public:
    static constexpr ui64 MemoryInFlightWriting = (ui64)1 << 30;
};
}