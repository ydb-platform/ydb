#include "iam_actor_base.h"
#include "policy/diagnostics.h"

#include <ydb/library/actors/async/wait_for_event.h>

#include <google/rpc/error_details.pb.h>

#include <algorithm>

namespace NKikimr::NIamDelegation {

bool IsRetryableGrpcStatus(const NYdbGrpc::TGrpcStatus& status) {
    if (status.InternalError) {
        return true;
    }
    switch (status.GRpcStatusCode) {
        case grpc::StatusCode::UNAVAILABLE:
        case grpc::StatusCode::DEADLINE_EXCEEDED:
        case grpc::StatusCode::RESOURCE_EXHAUSTED:
        case grpc::StatusCode::UNKNOWN:
        case grpc::StatusCode::INTERNAL:
        case grpc::StatusCode::ABORTED:
            return true;
        default:
            return false;
    }
}

TStringBuf HintForIamFailureType(TStringBuf type) {
    if (type == "BAD_SERVICE_ACCOUNT_CLOUD") {
        return "the service account does not belong to the cloud of the delegation, set RESOURCE to the cloud of the service account";
    }
    if (type == "SERVICE_NOT_ENABLED" || type == "SYSTEM_FOLDER_NOT_FOUND" || type == "AGENT_SA_NOT_FOUND"
        || type == "RESOURCE_CONTAINER_SETTINGS_NOT_FOUND")
    {
        return "the YDB service is not enabled in the cloud of the delegation, it must be enabled in the cloud of the service account (RESOURCE)";
    }
    if (type == "RESOURCE_CONTAINER_NOT_FOUND") {
        return "the cloud of the delegation does not exist, check RESOURCE";
    }
    if (type == "SERVICE_ACCOUNT_NOT_FOUND") {
        return "check SERVICE_ACCOUNT_ID";
    }
    if (type == "SUBJECT_NOT_FOUND" || type == "BAD_SUBJECT_TYPE") {
        return "the user running the statement is not a subject a delegation can be made on behalf of";
    }
    return {};
}

namespace {
constexpr size_t MaxIamFailureDetailsBytes = 64 * 1024;
struct TIamFailureType {
    TStringBuf Type;
    EDelegationFailureReason Reason;
};
constexpr TIamFailureType SafeFailureTypes[] = {
    {"BAD_SERVICE_ACCOUNT_CLOUD", EDelegationFailureReason::WrongCloud},
    {"SERVICE_NOT_ENABLED", EDelegationFailureReason::ServiceNotEnabled},
    {"SYSTEM_FOLDER_NOT_FOUND", EDelegationFailureReason::ServiceNotEnabled},
    {"AGENT_SA_NOT_FOUND", EDelegationFailureReason::ServiceNotEnabled},
    {"RESOURCE_CONTAINER_SETTINGS_NOT_FOUND", EDelegationFailureReason::ServiceNotEnabled},
    {"RESOURCE_CONTAINER_NOT_FOUND", EDelegationFailureReason::CloudNotFound},
    {"SERVICE_ACCOUNT_NOT_FOUND", EDelegationFailureReason::ServiceAccountNotFound},
    {"SUBJECT_NOT_FOUND", EDelegationFailureReason::SubjectNotDelegatable},
    {"BAD_SUBJECT_TYPE", EDelegationFailureReason::SubjectNotDelegatable},
};
} // namespace

TVector<EDelegationFailureReason> ExtractIamFailureReasons(const google::rpc::Status& status) {
    TVector<EDelegationFailureReason> reasons;
    if (status.code() == grpc::StatusCode::UNAUTHENTICATED) {
        // Rejected credentials cannot establish a business prerequisite failure.
        reasons.push_back(EDelegationFailureReason::AuthenticationFailed);
        return reasons;
    }
    if (status.code() == grpc::StatusCode::PERMISSION_DENIED) {
        reasons.push_back(EDelegationFailureReason::PermissionDenied);
    }
    if (status.ByteSizeLong() > MaxIamFailureDetailsBytes) {
        return reasons;
    }
    for (const auto& detail : status.details()) {
        google::rpc::PreconditionFailure failure;
        if (!detail.Is<google::rpc::PreconditionFailure>() || !detail.UnpackTo(&failure)) {
            continue;
        }
        for (const auto& violation : failure.violations()) {
            for (const auto& known : SafeFailureTypes) {
                if (TStringBuf(violation.type()) == known.Type) {
                    if (std::find(reasons.begin(), reasons.end(), known.Reason) == reasons.end()) {
                        reasons.push_back(known.Reason);
                    }
                    break;
                }
            }
        }
    }
    return reasons;
}

TVector<EDelegationFailureReason> ExtractIamFailureReasons(const NYdbGrpc::TGrpcStatus& status) {
    if (status.InternalError) {
        return {};
    }
    google::rpc::Status details;
    if (status.Details.size() > MaxIamFailureDetailsBytes || !details.ParseFromString(status.Details)) {
        details.Clear();
    }
    // The actual reply code wins over an untrusted or malformed details payload.
    details.set_code(status.GRpcStatusCode);
    return ExtractIamFailureReasons(details);
}

namespace {
TString ExplainFailureReasons(const TVector<EDelegationFailureReason>& reasons, TStringBuf method) {
    TStringBuilder out;
    for (const auto reason : reasons) {
        if (reason == EDelegationFailureReason::AuthenticationFailed) {
            out << "; " << NDiagnostics::AuthenticationFailed;
            continue;
        }
        if (reason == EDelegationFailureReason::PermissionDenied) {
            out << "; " << NDiagnostics::PermissionDenied;
            if (const auto rights = NDiagnostics::RequiredRights(method); !rights.empty()) {
                out << "; " << rights;
            }
            continue;
        }
        for (const auto& known : SafeFailureTypes) {
            if (known.Reason == reason) {
                out << "; " << known.Type << ": " << HintForIamFailureType(known.Type);
                break;
            }
        }
    }
    return out;
}
} // namespace

TString ExplainIamFailure(const google::rpc::Status& status, TStringBuf method) {
    return ExplainFailureReasons(ExtractIamFailureReasons(status), method);
}

TString ExplainIamFailure(const NYdbGrpc::TGrpcStatus& status, TStringBuf method) {
    return ExplainFailureReasons(ExtractIamFailureReasons(status), method);
}

NKikimrIamDelegation::EServiceHealth IamServiceHealth(const NYdbGrpc::TGrpcStatus& status) {
    if (status.InternalError || IsRetryableGrpcStatus(status)) {
        return NKikimrIamDelegation::SERVICE_UNAVAILABLE;
    }
    if (status.GRpcStatusCode == grpc::StatusCode::UNAUTHENTICATED) {
        return NKikimrIamDelegation::SERVICE_CREDENTIALS_REJECTED;
    }
    // A binding refusal proves the service is reachable; it cannot throttle
    // unrelated bindings just because they share the same system identity.
    return NKikimrIamDelegation::SERVICE_HEALTHY;
}

NKikimrIamDelegation::TServiceKey IamServiceKey(const TIamDelegationSettings& settings) {
    NKikimrIamDelegation::TServiceKey key;
    key.SetEndpoint(TStringBuilder() << (settings.Config.GetEnableSsl() ? "tls:" : "plain:")
        << settings.Config.GetServiceControlEndpoint());
    key.SetServiceId(settings.Config.GetServiceId());
    key.SetMicroserviceId(settings.Config.GetMicroserviceId());
    key.SetSystemTokenName(settings.Config.GetSystemTokenName());
    key.SetResourceType(settings.Config.GetResourceType());
    return key;
}

Ydb::StatusIds::StatusCode MapGrpcStatus(int code) {
    switch (static_cast<grpc::StatusCode>(code)) {
        case grpc::StatusCode::OK:
            return Ydb::StatusIds::SUCCESS;
        case grpc::StatusCode::PERMISSION_DENIED:
        case grpc::StatusCode::UNAUTHENTICATED:
            return Ydb::StatusIds::UNAUTHORIZED;
        case grpc::StatusCode::INVALID_ARGUMENT:
        case grpc::StatusCode::FAILED_PRECONDITION:
        case grpc::StatusCode::OUT_OF_RANGE:
        case grpc::StatusCode::ALREADY_EXISTS:
            return Ydb::StatusIds::BAD_REQUEST;
        case grpc::StatusCode::NOT_FOUND:
            return Ydb::StatusIds::NOT_FOUND;
        case grpc::StatusCode::UNIMPLEMENTED:
            return Ydb::StatusIds::UNSUPPORTED;
        case grpc::StatusCode::DEADLINE_EXCEEDED:
            return Ydb::StatusIds::TIMEOUT;
        case grpc::StatusCode::RESOURCE_EXHAUSTED:
            return Ydb::StatusIds::OVERLOADED;
        case grpc::StatusCode::CANCELLED:
            return Ydb::StatusIds::CANCELLED;
        case grpc::StatusCode::INTERNAL:
        case grpc::StatusCode::DATA_LOSS:
            return Ydb::StatusIds::INTERNAL_ERROR;
        default: // UNAVAILABLE, UNKNOWN, ABORTED and anything new: a transport-level problem
            return Ydb::StatusIds::UNAVAILABLE;
    }
}

namespace {

NActors::async<void> WaitSystemToken(TIamCallCredentials& credentials) {
    while (credentials.Status.Code == TEvTokenManager::TStatus::ECode::NOT_READY) {
        co_await credentials.Updated.Wait();
    }
}

} // namespace

void CheckIamCallDeadline(TMonotonic deadline) {
    if (NActors::TActivationContext::Monotonic() >= deadline) {
        throw TIamCallError(Ydb::StatusIds::TIMEOUT) << "IAM operation polling deadline exceeded";
    }
}

NActors::async<TString> GetIamCallToken(TIamCallCredentials& credentials, TMonotonic deadline) {
    CheckIamCallDeadline(deadline);
    const auto tokenDeadline = Min(deadline, NActors::TActivationContext::Monotonic() + TIamDelegationSettings::RequestTimeout);
    const bool ready = co_await NActors::WithDeadline(tokenDeadline, &WaitSystemToken, std::ref(credentials));
    CheckIamCallDeadline(deadline);
    if (!ready) {
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "timeout while obtaining the system service account token";
    }
    if (credentials.Status.Code != TEvTokenManager::TStatus::ECode::SUCCESS) {
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "system service account token is unavailable";
    }
    if (credentials.Token.empty()) {
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "system service account token is empty";
    }
    co_return credentials.Token;
}

NActors::async<NActors::IEventHandle::TPtr> IamClientRequest(NActors::TActorId client, NActors::IEventBase* request) {
    co_return co_await NActors::ActorRequest<NActors::IEventHandle>(client, request, NActors::IEventHandle::FlagTrackDelivery);
}


} // namespace NKikimr::NIamDelegation
