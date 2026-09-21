#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <util/generic/ptr.h>

namespace NKikimr::NIamDelegation {

// Source of the token of YDB's own (system) service account, used to authorize calls to
// ServiceControlService and IamTokenService.CreateForService.
//
// RequestToken is asynchronous and may be called from any thread; the result is delivered to the
// recipient as TEvIamDelegation::TEvSystemTokenReady with the given cookie.
class ISystemTokenSource : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ISystemTokenSource>;

    virtual void RequestToken(NActors::TActorSystem* actorSystem, const NActors::TActorId& recipient, ui64 cookie) = 0;
};

// Obtains the token from the VM metadata service through the SDK credentials provider
// (the same source the SDK-based IAM impersonation uses); host/port from AuthConfig.LocalMetadataService.
ISystemTokenSource::TPtr CreateVmMetadataSystemTokenSource(const TString& host, ui32 port);

// Always returns the given token: a credential obtained elsewhere, e.g. the user's own token for the calls
// the cloud resolver makes on the user's behalf.
ISystemTokenSource::TPtr CreateStaticSystemTokenSource(const TString& token);

} // namespace NKikimr::NIamDelegation
