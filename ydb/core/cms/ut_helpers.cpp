#include "ut_helpers.h"

Y_DECLARE_OUT_SPEC(, NKikimrWhiteboard::TTabletStateInfo::ETabletState, os, state) {
    os << NKikimrWhiteboard::TTabletStateInfo::ETabletState_Name(state);
}
