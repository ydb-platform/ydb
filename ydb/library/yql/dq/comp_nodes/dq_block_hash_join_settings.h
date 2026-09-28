#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NKikimr {
namespace NMiniKQL {

enum class EBuildSide : ui32 {
    Right = 0,
    Left = 1,
};

inline constexpr TStringBuf EqualNullsSettingName = "EqualNulls";

struct TBlockHashJoinSettings {
    EBuildSide BuildSide = EBuildSide::Right;
    // 0-based join-key positions (index in leftKeyColumns / rightKeyColumns)
    // that use IS NOT DISTINCT FROM: NULL matches NULL. Other keys keep SQL
    // equality, so a NULL on either side does not match. Rows with NULL in
    // these keys are valid join keys.
    TVector<ui32> EqualNullsKeys;

    bool LeftIsBuild() const { return BuildSide == EBuildSide::Left; }
};

} // namespace NMiniKQL
} // namespace NKikimr
