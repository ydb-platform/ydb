#pragma once

#include "events.h"
#include "settings.h"
#include "system_token_source.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NIamDelegation {

// Node-local cache of IAM tokens of delegated service accounts (IamTokenService.CreateForService
// authorized with the system service account). A token is minted on the first request for its key and
// refreshed ahead of expiry for as long as the key is in use; TEvGetToken always answers with the
// current one (or the error of the last attempt). Keys nobody asks for are dropped after IdleKeyTtl.
NActors::IActor* CreateIamDelegatedTokenService(const TIamDelegationSettings& settings, ISystemTokenSource::TPtr tokenSource);

} // namespace NKikimr::NIamDelegation
