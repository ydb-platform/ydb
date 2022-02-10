#pragma once

#include <util/system/types.h>

namespace NKikimr {
namespace NSchemeShard {

struct TUserAttributesLimits {
    static constexpr ui32 MaxNameLen = 100;
    static constexpr ui32 MaxValueLen = 4 * 1024;
    static constexpr ui32 MaxBytes = 10 * 1024;
};

}
}
