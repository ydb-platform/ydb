#pragma once

#include "defs.h"

#include <ydb/core/protos/bootstrap.pb.h>

namespace NKikimr {

enum class EBootType : ui8 {
    Normal = 0,
    Recovery = 1,
};

EBootType BootTypeFromProto(NKikimrConfig::TBootstrap::EBootType type);

} // namespace NKikimr
