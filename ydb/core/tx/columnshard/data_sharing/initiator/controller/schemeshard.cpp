#include "schemeshard.h"

namespace NKikimr::NOlap::NDataSharing {

void TSSInitiatorController::DoProposeError(const TString& sessionId, const TString& message) const {
    YDB_LOG_VERIFY(false,
        {"error", "on_propose"},
        {"sessionId", sessionId},
        {"reason", message});
}

}   // namespace NKikimr::NOlap::NDataSharing
