#pragma once

#include <util/generic/strbuf.h>

namespace NKikimr::NIamDelegation::NDiagnostics {

inline constexpr TStringBuf AuthenticationFailed =
    "authentication failed (UNAUTHENTICATED): IAM rejected the request credentials";
inline constexpr TStringBuf PermissionDenied =
    "permission denied (PERMISSION_DENIED): the authenticated request was not authorized";

// Fixed requirements from the IAM ServiceControl contract. A bare refusal does
// not identify which principal or prerequisite failed. Never copy remote text.
inline constexpr TStringBuf DelegatorRequiredRights =
    "the calling system service account requires role internal.service-control.delegator on the service registry's iam.gizmo resource; "
    "when references are requested, it also requires role service-control.services.referenceUpdater on the referrer resource type";
inline constexpr TStringBuf SetupRequiredRights =
    "the initiating subject requires iam.serviceAccounts.use (role iam.serviceAccounts.user) on the target service account; "
    "the calling system service account requires role internal.service-control.delegator on the service registry's iam.gizmo resource; "
    "when references are requested, it also requires role service-control.services.referenceUpdater on the referrer resource type";
inline constexpr TStringBuf PollRequiredRights =
    "read the operation as its original initiating system service account; another identity requires "
    "<service>.<resource>.get on the associated resource";

inline TStringBuf RequiredRights(TStringBuf method) {
    if (method == "ServiceControl.SetupDelegation") {
        return SetupRequiredRights;
    }
    if (method == "ServiceControl.RevokeDelegation") {
        return DelegatorRequiredRights;
    }
    if (method == "OperationService.Get") {
        return PollRequiredRights;
    }
    return {};
}

} // namespace NKikimr::NIamDelegation::NDiagnostics
