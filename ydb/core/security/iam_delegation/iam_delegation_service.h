#pragma once

#include "settings.h"

#include <ydb/core/base/iam_delegation.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NIamDelegation {

// Node-local service that sets up and revokes IAM delegations (ServiceControlService.SetupDelegation /
// RevokeDelegation) on behalf of YDB and waits for the resulting operations.
// Requests: TEvIamDelegation::TEvSetupDelegation / TEvRevokeDelegation (replies carry the request cookie).
// The actor subscribes once to Config.SystemTokenName in the node token manager.
// Setup/revoke use bounded attempts; OperationPollTimeout starts after their pending operation
// reply and bounds all subsequent credential waits, poll calls and retry delays. A local timeout
// does not cancel an IAM operation or a gRPC request already sent.
NActors::IActor* CreateIamDelegationService(const TIamDelegationSettings& settings);

} // namespace NKikimr::NIamDelegation
