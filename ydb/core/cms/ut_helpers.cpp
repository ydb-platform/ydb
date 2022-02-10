#include "ut_helpers.h"

template<>
void Out<NKikimrCms::TAction::EType>(IOutputStream &os, NKikimrCms::TAction::EType type)
{
    os << NKikimrCms::TAction::EType_Name(type);
}

template<>
void Out<NKikimrWhiteboard::TTabletStateInfo::ETabletState>(IOutputStream &os, NKikimrWhiteboard::TTabletStateInfo::ETabletState state)
{
    os << NKikimrWhiteboard::TTabletStateInfo::ETabletState_Name(state);
}
