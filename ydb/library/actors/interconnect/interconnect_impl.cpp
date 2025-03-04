#include "interconnect_impl.h"

namespace NActors {

TString TEvInterconnect::TEvResolveNode::ToString() const {
    return TStringBuilder() << ToStringHeader() << " " << Record.ShortDebugString();
}

TMonotonic TEvInterconnect::TEvResolveNode::GetMonotonicDeadline(const TActorContext& ctx) const {
    TMonotonic deadline = TMonotonic::Max();
    if (Record.HasDeadline()) {
        auto clockWallNow = ctx.Now();
        auto clockWallDeadline = TInstant::FromValue(Record.GetDeadline());

        TDuration diff = TDuration::Zero();
        if (clockWallDeadline > clockWallNow) {
            diff = clockWallDeadline - clockWallNow;
        }

        deadline = ctx.Monotonic() + diff;
    }
    return deadline;
}

} // namespace NActors
