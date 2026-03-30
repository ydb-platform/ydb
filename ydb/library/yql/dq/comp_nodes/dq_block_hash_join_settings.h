#pragma once

#include <util/system/types.h>

namespace NKikimr {
namespace NMiniKQL {

enum class EBuildSide : ui32 {
    Right = 0,
    Left = 1,
};

enum class EAnyJoinSide : ui32 {
    None  = 0,
    Left  = 1,
    Right = 2,
    Both  = 3,
};

struct TBlockHashJoinSettings {
    EBuildSide BuildSide = EBuildSide::Right;
    EAnyJoinSide Any = EAnyJoinSide::None;

    bool LeftIsBuild() const { return BuildSide == EBuildSide::Left; }

    bool BuildSideAny() const {
        const auto buildFlag = LeftIsBuild() ? EAnyJoinSide::Left : EAnyJoinSide::Right;
        return (static_cast<ui32>(Any) & static_cast<ui32>(buildFlag)) != 0;
    }
};

} // namespace NMiniKQL
} // namespace NKikimr
