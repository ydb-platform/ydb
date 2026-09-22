#pragma once

#include "defs.h"

#include <util/generic/vector.h>

namespace NKikimr::NCms {

// Check one batch through DBSController and send TEvNbs2MaintenanceResult to
// client.
IActor* CreateNbs2MaintenanceChecker(const TActorId& client, ui64 attemptId,
    TVector<ui32> nodeIds, TDuration timeout);

} // namespace NKikimr::NCms
