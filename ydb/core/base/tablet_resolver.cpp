#include "tablet_resolver.h"
#include <util/stream/str.h>
#include <util/string/builder.h>

namespace NKikimr {

TString TEvTabletResolver::TEvForward::TResolveFlags::ToString() const {
    TStringStream str;
    str << (ui32) LocalNodePrio << ':'
        << (ui32) LocalDcPrio << ':'
        << (ui32) FollowerPrio << ':'
        << ((FollowerId) ? ::ToString(*FollowerId) : TString("(undefined)"));
    return str.Str();
}

TString TEvTabletResolver::TEvForward::ToString() const {
    TStringStream str;
    str << "{EvForward TabletID: " << TabletID;
    str << " Ev: " << (Ev ? Ev->ToString() : "nullptr");
    str << " Flags: " << ResolveFlags.ToString();
    str << "}";
    return str.Str();
}

TString TEvTabletResolver::TEvTabletProblem::ToString() const {
    TStringStream str;
    str << "{EvTabletProblem TabletID: " << TabletID;
    str << " Actor: " << Actor;
    str << "}";
    return str.Str();
}

TString TEvTabletResolver::TEvNodeProblem::ToString() const {
    return TStringBuilder()
        << "{EvNodeProblem NodeId: " << NodeId
        << " CacheEpoch: " << CacheEpoch
        << "}";
}

TString TEvTabletResolver::TEvForwardResult::ToString() const {
    TStringStream str;
    str << "{EvForwardResult Status: " << (ui32)Status;
    str << " TabletID: " << TabletID;
    str << " TabletActor: " << TabletActor.ToString();
    str << " Tablet: " << Tablet.ToString();
    str << " CacheEpoch: " << CacheEpoch;
    str << "}";
    return str.Str();
}

} // namespace NKikimr
