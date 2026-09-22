#pragma once

#include "settings.h"

#include <ydb/library/actors/async/async.h>

namespace NKikimr::NIamDelegation {

struct TResolvedCloud {
    TString FolderId;
    TString CloudId;
};

// Finds the cloud of a service account on the user's behalf: ServiceAccountService.Get for its folder, then
// FolderService.Resolve for the cloud of the folder, both authorized with the user's token. Throws
// TIamCallError. Requires settings.CanResolveCloud().
//
// A nested coroutine of the calling actor. The two ycloud client actors it registers live in the caller's
// mailbox for the duration of the call, so a reply of theirs arriving after the caller cancelled the call
// (a timeout) reaches the caller's state function, which must ignore TEvGetServiceAccountResponse,
// TEvResolveFoldersResponse and TEvUndelivered.
NActors::async<TResolvedCloud> ResolveCloud(TIamDelegationSettings settings, TString userToken, TString serviceAccountId);

} // namespace NKikimr::NIamDelegation
