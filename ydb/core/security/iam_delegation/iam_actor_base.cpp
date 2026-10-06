#include "iam_actor_base.h"

#include <ydb/library/actors/async/wait_for_event.h>

#include <google/rpc/error_details.pb.h>

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

TString ExplainIamFailure(const google::rpc::Status& status) {
    TStringBuilder out;
    for (const auto& detail : status.details()) {
        google::rpc::PreconditionFailure failure;
        if (!detail.Is<google::rpc::PreconditionFailure>() || !detail.UnpackTo(&failure)) {
            continue;
        }
        for (const auto& violation : failure.violations()) {
            out << "; " << violation.type();
            if (!violation.description().empty()) {
                out << " (" << violation.description() << ")";
            }
            if (const TStringBuf hint = HintForIamFailureType(violation.type())) {
                out << ": " << hint;
            }
        }
    }
    return out;
}

TString ExplainIamFailure(const NYdbGrpc::TGrpcStatus& status) {
    google::rpc::Status rpcStatus;
    if (status.Details.empty() || !rpcStatus.ParseFromString(status.Details)) {
        return {};
    }
    return ExplainIamFailure(rpcStatus);
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

NActors::async<TEvIamDelegation::TEvSystemTokenReady::TPtr> WaitSystemToken(NActors::TActorId service) {
    const ui64 cookie = NActors::AllocateWaitCookie();
    NActors::TActivationContext::AsActorContext().Send(service, new TEvIamDelegation::TEvGetSystemToken(), 0, cookie);
    co_return co_await NActors::ActorWaitForEvent<TEvIamDelegation::TEvSystemTokenReady>(cookie);
}

NActors::async<TString> GetIamCallToken(const TIamDelegationSettings& settings, const TIamCallCredentials& credentials) {
    if (!credentials.SystemTokenService) {
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "no system token service to obtain the system service account token from";
    }
    auto ev = co_await NActors::WithTimeout(settings.RequestTimeout, &WaitSystemToken, credentials.SystemTokenService);
    if (!ev) {
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "timeout while obtaining the system service account token";
    }
    if (!(*ev)->Get()->Error.empty()) {
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << (*ev)->Get()->Error;
    }
    if ((*ev)->Get()->Token.empty()) {
        throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "system service account token is empty";
    }
    co_return (*ev)->Get()->Token;
}

NActors::async<NActors::IEventHandle::TPtr> IamClientRequest(NActors::TActorId client, NActors::IEventBase* request) {
    co_return co_await NActors::ActorRequest<NActors::IEventHandle>(client, request, NActors::IEventHandle::FlagTrackDelivery);
}

TBackoff IamCallBackoff(const TIamDelegationSettings& settings) {
    // MaxRetries counts attempts; TBackoff counts retries after the first attempt
    return TBackoff(settings.MaxRetries > 0 ? settings.MaxRetries - 1 : 0, TDuration::MilliSeconds(200), TDuration::Seconds(10));
}

} // namespace NKikimr::NIamDelegation
