#pragma once

#include <ydb/library/actors/core/actor.h>

#include <util/generic/string.h>

namespace NKikimr::NIamDelegation {

// Node-local actor holding the token of YDB's own (system) service account: it owns the SDK credentials
// provider of the VM metadata service (host/port from AuthConfig.LocalMetadataService) and answers every
// TEvIamDelegation::TEvGetSystemToken with TEvSystemTokenReady sent to the requester with its cookie.
// Nothing else on the node sees the provider. Registered under MakeIamSystemTokenServiceId().
NActors::IActor* CreateIamSystemTokenService(const TString& host, ui32 port);

} // namespace NKikimr::NIamDelegation
