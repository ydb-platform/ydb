#pragma once

#include "settings.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NIamDelegation {

// One-shot actor: finds the cloud of a service account (ServiceAccountService.Get, then FolderService.Resolve)
// with the user's token. Replies to replyTo with TEvResolveCloudResult carrying the cookie, then dies.
// Requires settings.CanResolveCloud().
NActors::IActor* CreateCloudResolver(const TIamDelegationSettings& settings, const TString& userToken,
    const TString& serviceAccountId, const NActors::TActorId& replyTo, ui64 cookie = 0);

} // namespace NKikimr::NIamDelegation
