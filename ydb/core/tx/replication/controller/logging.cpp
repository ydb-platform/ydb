#include "logging.h"

#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

TTabletLogPrefix::TTabletLogPrefix(const TController* self)
    : TabletId(self->TabletID())
{
}

TTabletLogPrefix::TTabletLogPrefix(const TController* self, const TString& txName)
    : TabletId(self->TabletID())
    , TxName(txName)
{
}

void TTabletLogPrefix::Out(IOutputStream& output) const {
    output << "[controller " << TabletId << "]";
    if (TxName) {
        output << "[" << TxName << "]";
    }
    output << " ";
}

TActorLogPrefix::TActorLogPrefix(const TString& activity, ui64 rid, ui64 tid)
    : Activity(activity)
    , ReplicationId(rid)
    , TargetId(tid)
{
}

void TActorLogPrefix::Out(IOutputStream& output) const {
    output << "[" << Activity << "]";
    if (ReplicationId) {
        output << "[rid " << ReplicationId << "]";
    }
    if (TargetId) {
        output << "[tid " << TargetId << "]";
    }
    output << " ";
}

}

Y_DECLARE_OUT_SPEC(, NKikimr::NReplication::NController::TTabletLogPrefix, output, value) {
    value.Out(output);
}

Y_DECLARE_OUT_SPEC(, NKikimr::NReplication::NController::TActorLogPrefix, output, value) {
    value.Out(output);
}
