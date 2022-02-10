#pragma once

#include <ydb/core/protos/config_units.pb.h>

#include "defs.h"

namespace NKikimr {

    inline TDuration DurationFromProto(const NKikimrConfigUnits::TDuration& pb) {
        return TDuration::MicroSeconds((pb.GetSeconds() * 1000 + pb.GetMilliseconds()) * 1000 + pb.GetMicroseconds());
    }

} // NKikimr
