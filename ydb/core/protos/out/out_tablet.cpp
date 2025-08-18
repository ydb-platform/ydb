#include <ydb/core/protos/tablet.pb.h>

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(, NKikimrTabletBase::TEvTabletStateUpdate::EState, stream, value) {
    stream << NKikimrTabletBase::TEvTabletStateUpdate::EState_Name(value);
}
