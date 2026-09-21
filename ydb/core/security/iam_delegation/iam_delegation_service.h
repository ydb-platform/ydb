#pragma once

#include "events.h"
#include "settings.h"
#include "system_token_source.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NIamDelegation {

// Node-local service that sets up and revokes IAM delegations (ServiceControlService.SetupDelegation /
// RevokeDelegation) on behalf of YDB and waits for the resulting operations.
// Requests: TEvIamDelegation::TEvSetupDelegation / TEvRevokeDelegation (replies carry the request cookie).
NActors::IActor* CreateIamDelegationService(const TIamDelegationSettings& settings, ISystemTokenSource::TPtr tokenSource);

} // namespace NKikimr::NIamDelegation
