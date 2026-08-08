#pragma once

#include <util/system/types.h>

namespace NKikimr {
namespace NSchemeShard {

struct TUserAttributesLimits {
    static constexpr ui32 MaxNameLen = 100;
    static constexpr ui32 MaxValueLen = 4 * 1024;
    static constexpr ui32 MaxBytes = 10 * 1024;
    // __monitoring_project_id becomes a label on every detailed metric, so it is
    // capped much tighter than a free-form attribute value
    static constexpr ui32 MaxMonitoringProjectIdLen = 256;
};

}
}
