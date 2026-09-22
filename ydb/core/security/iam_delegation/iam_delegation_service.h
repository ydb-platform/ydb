#pragma once

#include "events.h"
#include "settings.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NIamDelegation {

// Node-local service that sets up and revokes IAM delegations (ServiceControlService.SetupDelegation /
// RevokeDelegation) on behalf of YDB and waits for the resulting operations.
// Requests: TEvIamDelegation::TEvSetupDelegation / TEvRevokeDelegation (replies carry the request cookie).
// The calls are authorized with the system service account token asked from systemTokenService
// (see system_token_service.h).
NActors::IActor* CreateIamDelegationService(const TIamDelegationSettings& settings, const NActors::TActorId& systemTokenService);

} // namespace NKikimr::NIamDelegation
