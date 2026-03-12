#pragma once

#include <util/system/types.h>

namespace NKikimr {
namespace NMiniKQL {

enum class EBuildSide : ui32 {
    Right = 0,
    Left = 1,
};

struct TBlockHashJoinSettings {
    EBuildSide BuildSide = EBuildSide::Right;
    bool SpillJoinResults = false;

    bool LeftIsBuild() const { return BuildSide == EBuildSide::Left; }
};

} // namespace NMiniKQL
} // namespace NKikimr
