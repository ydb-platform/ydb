#pragma once

#include "events.h"
#include "settings.h"

#include <ydb/core/util/backoff.h>
#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/sleep.h>
#include <ydb/library/actors/async/timeout.h>
#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/ycloud/api/events.h>

#include <google/rpc/error_details.pb.h>
#include <google/rpc/status.pb.h>
#include <grpcpp/support/status_code_enum.h>

#include <util/generic/guid.h>
#include <util/generic/yexception.h>

#include <concepts>
#include <functional>

namespace NKikimr::NIamDelegation {

// Error of an IAM call that must not be retried (or whose retries are exhausted). GrpcCode is the code of
// the last gRPC answer when there was one (grpc::StatusCode::OK when the failure was not a gRPC answer).
class TIamCallError : public yexception {
public:
    explicit TIamCallError(Ydb::StatusIds::StatusCode status, grpc::StatusCode grpcCode = grpc::StatusCode::OK)
        : Status(status)
        , GrpcCode(grpcCode)
    {}

    const Ydb::StatusIds::StatusCode Status;
    const grpc::StatusCode GrpcCode;
};

inline bool IsRetryableGrpcStatus(const NYdbGrpc::TGrpcStatus& status) {
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

// A hint for the user for the ServiceControl failure types (ServiceControlFailureType in
// service_control.proto) that the user can act on; empty for the rest.
inline TStringBuf HintForIamFailureType(TStringBuf type) {
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

// IAM reports the failure type as Violation.type of a google.rpc.PreconditionFailure in the error
// details. Returns "; TYPE (description): hint" for every violation found, or an empty string.
inline TString ExplainIamFailure(const google::rpc::Status& status) {
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

// The gRPC error details carry a serialized google.rpc.Status.
inline TString ExplainIamFailure(const NYdbGrpc::TGrpcStatus& status) {
    google::rpc::Status rpcStatus;
    if (status.Details.empty() || !rpcStatus.ParseFromString(status.Details)) {
        return {};
    }
    return ExplainIamFailure(rpcStatus);
}

inline Ydb::StatusIds::StatusCode MapGrpcStatus(int code) {
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

// A request event of a ycloud actor client (NCloud::TEvGrpcProtoRequest): the proto to fill and the credentials.
template <class TEv>
concept CIamRequestEvent = std::default_initializable<TEv> && requires (TEv ev) {
    ev.Request;
    { ev.Token } -> std::same_as<TString&>;
    { ev.RequestId } -> std::same_as<TString&>;
};

// A response event of a ycloud actor client (NCloud::TEvGrpcProtoResponse): its event type and the gRPC status.
template <class TEv>
concept CIamResponseEvent = requires (TEv ev) {
    typename TEv::TPtr;
    { TEv::EventType } -> std::convertible_to<ui32>;
    { ev.Status } -> std::same_as<NYdbGrpc::TGrpcStatus&>;
};

// Fills the proto of a request event.
template <class TFill, class TRequestEv>
concept CIamRequestFiller = std::invocable<TFill&, decltype(std::declval<TRequestEv&>().Request)&>;

// What CallWithRetry does with a NOT_FOUND answer.
enum class ENotFound {
    IsError,   // thrown as TIamCallError like any other non-retryable status
    IsAbsent,  // a null response: the object does not exist, which the caller treats as a legitimate outcome
};

// What authorizes an IAM call: the token of YDB's own (system) service account, asked from the system token
// service actor before every attempt, or a token given as is (the user's own token for the lookups made on
// the user's behalf).
struct TIamCallCredentials {
    static TIamCallCredentials SystemToken(const NActors::TActorId& service) {
        return {.SystemTokenService = service};
    }

    static TIamCallCredentials Token(TString token) {
        return {.GivenToken = std::move(token)};
    }

    NActors::TActorId SystemTokenService;
    TString GivenToken;
};

// The coroutines below are nested ones and must be awaited from a coroutine of the calling actor.

// Asks the system token service. The reply is an ordinary mailbox event: it cannot be handled before this
// turn ends, so the wait below still intercepts it.
inline NActors::async<TEvIamDelegation::TEvSystemTokenReady::TPtr> WaitSystemToken(NActors::TActorId service) {
    const ui64 cookie = NActors::AllocateWaitCookie();
    NActors::TActivationContext::AsActorContext().Send(service, new TEvIamDelegation::TEvGetSystemToken(), 0, cookie);
    co_return co_await NActors::ActorWaitForEvent<TEvIamDelegation::TEvSystemTokenReady>(cookie);
}

// The token of the next call: the one given as is, or the system service account's from the service.
inline NActors::async<TString> GetIamCallToken(const TIamDelegationSettings& settings, const TIamCallCredentials& credentials) {
    if (!credentials.GivenToken.empty()) {
        co_return credentials.GivenToken;
    }
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

// Sends the request to the client actor and waits for its reply of any type: the typed response, or
// TEvUndelivered when the client actor is gone (a typed wait would never resume in that case).
inline NActors::async<NActors::IEventHandle::TPtr> IamClientRequest(NActors::TActorId client, NActors::IEventBase* request) {
    co_return co_await NActors::ActorRequest<NActors::IEventHandle>(client, request, NActors::IEventHandle::FlagTrackDelivery);
}

inline TBackoff IamCallBackoff(const TIamDelegationSettings& settings) {
    // MaxRetries counts attempts; TBackoff counts retries after the first attempt
    return TBackoff(settings.MaxRetries > 0 ? settings.MaxRetries - 1 : 0, TDuration::MilliSeconds(200), TDuration::Seconds(10));
}

// Sends TRequestEv (filled by fill) to the client actor authorized with the credentials and waits for
// TResponseEv. Failures to obtain the token, a missing client actor and retryable gRPC errors are retried
// with exponential backoff up to settings.MaxRetries attempts in total; other errors are thrown as
// TIamCallError; NOT_FOUND is handled according to notFound.
template <CIamRequestEvent TRequestEv, CIamResponseEvent TResponseEv, CIamRequestFiller<TRequestEv> TFill>
NActors::async<typename TResponseEv::TPtr> IamCallWithRetry(const TIamDelegationSettings& settings, const TIamCallCredentials& credentials,
    NActors::TActorId client, TStringBuf method, TFill fill, ENotFound notFound = ENotFound::IsError)
{
    TBackoff backoff = IamCallBackoff(settings);
    // the same request id for every attempt lets IAM deduplicate retries of one call
    const TString requestId = CreateGuidAsString();
    for (;;) {
        TString retryableError;
        Ydb::StatusIds::StatusCode retryableStatus = Ydb::StatusIds::UNAVAILABLE;
        grpc::StatusCode retryableGrpcCode = grpc::StatusCode::OK;

        TString token;
        try {
            token = co_await GetIamCallToken(settings, credentials);
        } catch (const TIamCallError& e) {
            retryableError = e.what();
            retryableStatus = e.Status;
        }

        if (retryableError.empty()) {
            auto request = MakeHolder<TRequestEv>();
            fill(request->Request);
            request->Token = token;
            request->RequestId = requestId;

            auto response = co_await NActors::WithTimeout(settings.RequestTimeout + TDuration::Seconds(1), &IamClientRequest, client, request.Release());
            if (!response) {
                retryableError = "no response from the client actor";
            } else if ((*response)->GetTypeRewrite() == NActors::TEvents::TEvUndelivered::EventType) {
                retryableError = "the client actor is not available";
            } else {
                if ((*response)->GetTypeRewrite() != TResponseEv::EventType) {
                    throw TIamCallError(Ydb::StatusIds::INTERNAL_ERROR) << method << ": unexpected reply " << (*response)->GetTypeName();
                }
                typename TResponseEv::TPtr typed(reinterpret_cast<NActors::TEventHandle<TResponseEv>*>(response->Release()));
                const auto& status = typed->Get()->Status;
                if (status.Ok()) {
                    co_return typed;
                }
                if (notFound == ENotFound::IsAbsent && !status.InternalError && status.GRpcStatusCode == grpc::StatusCode::NOT_FOUND) {
                    co_return typename TResponseEv::TPtr();
                }
                const auto grpcCode = static_cast<grpc::StatusCode>(status.GRpcStatusCode);
                if (!IsRetryableGrpcStatus(status)) {
                    throw TIamCallError(MapGrpcStatus(status.GRpcStatusCode), grpcCode) << method << " failed: " << status.Msg << ExplainIamFailure(status);
                }
                retryableError = TStringBuilder() << status.GRpcStatusCode << " " << status.Msg;
                retryableStatus = MapGrpcStatus(status.GRpcStatusCode);
                retryableGrpcCode = grpcCode;
            }
        }

        YDB_LOG_WARN_COMP(NKikimrServices::IAM_DELEGATION, "IAM call failed with a retryable error",
            {"method", method},
            {"error", retryableError},
            {"attempt", backoff.GetIteration() + 1}
        );
        if (!backoff.HasMore()) {
            // MaxRetries = 0 still makes the one attempt
            throw TIamCallError(retryableStatus, retryableGrpcCode) << method << " failed after " << Max<ui32>(settings.MaxRetries, 1) << " attempts: " << retryableError;
        }
        co_await NActors::AsyncSleepFor(backoff.Next());
    }
}

// The longest one call with all its retries can take: the attempts themselves plus the backoff between them.
inline TDuration MaxIamCallDuration(const TIamDelegationSettings& settings) {
    TDuration total;
    TBackoff backoff = IamCallBackoff(settings);
    while (backoff.HasMore()) {
        total += backoff.Next();
    }
    return (settings.RequestTimeout * 2 + TDuration::Seconds(1)) * settings.MaxRetries + total;
}

// Common part of the IAM delegation service actors: their calls are authorized with the system service
// account token asked from the system token service, and an exception that escapes a handler or a
// top-level coroutine is logged and the actor lives on (the request it was serving is lost, its sender
// times out; the others are not).
template <class TDerived>
class TIamActorBase : public NActors::TActorBootstrapped<TDerived>, public NActors::IActorExceptionHandler {
protected:
    using TBase = NActors::TActorBootstrapped<TDerived>;

    TIamActorBase(TIamDelegationSettings settings, const NActors::TActorId& systemTokenService)
        : Settings(std::move(settings))
        , Credentials(TIamCallCredentials::SystemToken(systemTokenService))
    {
        static_assert(std::derived_from<TDerived, TIamActorBase>, "TDerived must derive from TIamActorBase<TDerived>");
    }

    using NActors::IActorExceptionHandler::OnUnhandledException;
    bool OnUnhandledException(const std::exception& e) override {
        YDB_LOG_ERROR_COMP(NKikimrServices::IAM_DELEGATION, "Unhandled exception in an IAM delegation actor",
            {"actor", TBase::SelfId()}, {"exception", e.what()});
        return true;
    }

    // IamCallWithRetry with the settings and credentials of the actor.
    template <CIamRequestEvent TRequestEv, CIamResponseEvent TResponseEv, CIamRequestFiller<TRequestEv> TFill>
    NActors::async<typename TResponseEv::TPtr> CallWithRetry(NActors::TActorId client, TStringBuf method, TFill fill, ENotFound notFound = ENotFound::IsError) {
        co_return co_await IamCallWithRetry<TRequestEv, TResponseEv>(Settings, Credentials, client, method, std::move(fill), notFound);
    }

    TDuration MaxCallDuration() const {
        return MaxIamCallDuration(Settings);
    }

    // Starts a task of the actor: the async coroutine callback(args...) runs concurrently with the
    // handlers, resumed by its own events, and is cancelled at PassAway. Awaiting the same coroutine
    // inline would run it as a part of the current handler instead. (A void coroutine of an actor is
    // such a task by itself; this is the explicit way to start one.)
    template <class TCallback, class... TArgs>
        requires NActors::IsSpecificAsyncCoroutineCallable<TCallback, void, TArgs...>
    void Spawn(TCallback callback, TArgs... args) {
        co_await std::invoke(callback, args...);
    }

protected:
    const TIamDelegationSettings Settings;
    const TIamCallCredentials Credentials;
};

} // namespace NKikimr::NIamDelegation
