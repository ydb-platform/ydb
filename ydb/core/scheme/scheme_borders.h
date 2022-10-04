#pragma once

#include "defs.h"
#include "scheme_tablecell.h"

namespace NKikimr {

    enum EPrefixMode : ui8 {
        PrefixModeFlagMin = 0, // Missing key columns are NULL
        PrefixModeFlagMax = 1, // Missing key columns are +INF
        PrefixModeFlagMask = 1,
        PrefixModeOrderDec = (0 << 1), // Key prefix decremended
        PrefixModeOrderCur = (1 << 1), // Key prefix taken as is
        PrefixModeOrderInc = (2 << 1), // Key prefix incremented
        PrefixModeOrderMask = (3 << 1),
        PrefixModeRightBorderNonInclusive = PrefixModeFlagMin | PrefixModeOrderDec,
        PrefixModeLeftBorderInclusive = PrefixModeFlagMin | PrefixModeOrderCur,
        PrefixModeRightBorderInclusive = PrefixModeFlagMax | PrefixModeOrderCur,
        PrefixModeLeftBorderNonInclusive = PrefixModeFlagMax | PrefixModeOrderInc,
    };

    /**
     * Compares two prefix borders
     */
    int ComparePrefixBorders(
        TConstArrayRef<NScheme::TTypeInfo> keyTypes,
        TConstArrayRef<TCell> left, EPrefixMode leftMode,
        TConstArrayRef<TCell> right, EPrefixMode rightMode);

}
