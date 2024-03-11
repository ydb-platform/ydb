#pragma once
#include <util/system/types.h>

namespace NKikimr::NOlap {
class TGlobalLimits {
public:
    static const inline ui64 TxWriteLimitBytes = 256 * 1024 * 1024;
};
}