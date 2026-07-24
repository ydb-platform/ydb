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


NActors::NStructuredLog::TStructuredMessage CreateActorLogPrefix(const TString& activity, ui64 rid, ui64 tid)
{
    NStructuredLog::TStructuredMessage result;
    YDB_LOG_UPDATE_MESSAGE(result,
        {"activity", activity});
    if (rid) {
        YDB_LOG_UPDATE_MESSAGE(result,
            {"replicationId", rid});
    }
    if (tid) {
        YDB_LOG_UPDATE_MESSAGE(result,
            {"targetId", tid});
    }
    return result;
}
}

Y_DECLARE_OUT_SPEC(, NKikimr::NReplication::NController::TTabletLogPrefix, output, value) {
    value.Out(output);
}
