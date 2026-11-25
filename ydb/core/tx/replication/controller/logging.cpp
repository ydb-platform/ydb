#include "logging.h"

#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

NActors::NStructuredLog::TStructuredMessage CreateTabletLogPrefix(const TController* self) {
    NStructuredLog::TStructuredMessage result;
    YDB_LOG_UPDATE_MESSAGE(result,
        {"tabletId", self->TabletID()});
    return result;
}

NActors::NStructuredLog::TStructuredMessage CreateTabletLogPrefix(const TController* self, const TString& txName) {
    NStructuredLog::TStructuredMessage result;
    YDB_LOG_UPDATE_MESSAGE(result,
        {"tabletId", self->TabletID()},
        {"txName", txName});
    return result;
}

NActors::NStructuredLog::TStructuredMessage CreateActorLogPrefix(const TString& activity, ui64 rid, ui64 tid) {
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
