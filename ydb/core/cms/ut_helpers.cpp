#include "ut_helpers.h"

Y_DECLARE_OUT_SPEC(, NKikimrCms::TAction::EType, os, type) {
    os << NKikimrCms::TAction::EType_Name(type);
}

Y_DECLARE_OUT_SPEC(, NKikimrWhiteboard::TTabletStateInfo::ETabletState, os, state) {
    os << NKikimrWhiteboard::TTabletStateInfo::ETabletState_Name(state);
}
