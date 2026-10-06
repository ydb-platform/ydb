#pragma once

#include "settings.h"

#include <ydb/core/base/iam_delegation.h>

#include <ydb/core/security/token_manager/token_manager.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/event.h>
#include <ydb/library/actors/async/sleep.h>
#include <ydb/library/actors/async/timeout.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/ycloud/api/events.h>

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

bool IsRetryableGrpcStatus(const NYdbGrpc::TGrpcStatus& status);

// A hint for the user for the ServiceControl failure types (ServiceControlFailureType in
// service_control.proto) that the user can act on; empty for the rest.
TStringBuf HintForIamFailureType(TStringBuf type);

// IAM reports the failure type as Violation.type of a google.rpc.PreconditionFailure in the error
// details. Returns "; TYPE (description): hint" for every violation found, or an empty string.
TString ExplainIamFailure(const google::rpc::Status& status);

// The gRPC error details carry a serialized google.rpc.Status.
TString ExplainIamFailure(const NYdbGrpc::TGrpcStatus& status);

Ydb::StatusIds::StatusCode MapGrpcStatus(int code);

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

// Owned by the long-lived subscribing actor and accessed only in its mailbox, including by
// nested coroutines. Updates do not include expiry; any unsuccessful update invalidates the cache.
struct TIamCallCredentials {
    TString Token;
    TEvTokenManager::TStatus Status{TEvTokenManager::TStatus::ECode::NOT_READY, {}};
    NActors::TAsyncEvent Updated;
};

// The coroutines below are nested ones and must be awaited from a coroutine of the owning actor.
NActors::async<TString> GetIamCallToken(TIamCallCredentials& credentials, TMonotonic deadline);

// Absolute deadline of the polling phase. Initial setup/revoke calls use Max() and retain their
// per-attempt credential and client timeouts and bounded retry count.
void CheckIamCallDeadline(TMonotonic deadline);

// Sends the request to the client actor and waits for its reply of any type: the typed response, or
// TEvUndelivered when the client actor is gone (a typed wait would never resume in that case).
NActors::async<NActors::IEventHandle::TPtr> IamClientRequest(NActors::TActorId client, NActors::IEventBase* request);

TBackoff IamCallBackoff(const TIamDelegationSettings& settings);

// Sends TRequestEv (filled by fill) to the client actor authorized with the credentials and waits for
// TResponseEv. Failures to obtain the token, a missing client actor and retryable gRPC errors are retried
// with exponential backoff up to settings.MaxRetries attempts in total; other errors are thrown as
// TIamCallError; NOT_FOUND is handled according to notFound.
template <CIamRequestEvent TRequestEv, CIamResponseEvent TResponseEv, CIamRequestFiller<TRequestEv> TFill>
NActors::async<typename TResponseEv::TPtr> IamCallWithRetry(const TIamDelegationSettings& settings, TIamCallCredentials& credentials,
    NActors::TActorId client, TStringBuf method, TFill fill, ENotFound notFound = ENotFound::IsError, TMonotonic deadline = TMonotonic::Max())
{
    TBackoff backoff = IamCallBackoff(settings);
    // the same request id for every attempt lets IAM deduplicate retries of one call
    const TString requestId = CreateGuidAsString();
    for (;;) {
        CheckIamCallDeadline(deadline);
        TString retryableError;
        Ydb::StatusIds::StatusCode retryableStatus = Ydb::StatusIds::UNAVAILABLE;
        grpc::StatusCode retryableGrpcCode = grpc::StatusCode::OK;

        TString token;
        try {
            token = co_await GetIamCallToken(credentials, deadline);
        } catch (const TIamCallError& e) {
            retryableError = e.what();
            retryableStatus = e.Status;
        }

        CheckIamCallDeadline(deadline);
        if (retryableError.empty()) {
            auto request = MakeHolder<TRequestEv>();
            fill(request->Request);
            request->Token = token;
            request->RequestId = requestId;

            const auto responseDeadline = Min(deadline, NActors::TActivationContext::Monotonic() + settings.RequestTimeout + TDuration::Seconds(1));
            auto response = co_await NActors::WithDeadline(responseDeadline, &IamClientRequest, client, request.Release());
            CheckIamCallDeadline(deadline);
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
        co_await NActors::AsyncSleepUntil(Min(deadline, NActors::TActivationContext::Monotonic() + backoff.Next()));
    }
}

// Common part of the IAM delegation service actors: their calls are authorized with the system service
// account token cached from token-manager updates, and an exception that escapes a handler or a
// top-level coroutine is logged and the actor lives on (the request it was serving is lost, its sender
// times out; the others are not).
template <class TDerived>
class TIamActorBase : public NActors::TActorBootstrapped<TDerived>, public NActors::IActorExceptionHandler {
protected:
    using TBase = NActors::TActorBootstrapped<TDerived>;

    explicit TIamActorBase(TIamDelegationSettings settings)
        : Settings(std::move(settings))
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
    NActors::async<typename TResponseEv::TPtr> CallWithRetry(NActors::TActorId client, TStringBuf method, TFill fill, ENotFound notFound = ENotFound::IsError, TMonotonic deadline = TMonotonic::Max()) {
        co_return co_await IamCallWithRetry<TRequestEv, TResponseEv>(Settings, Credentials, client, method, std::move(fill), notFound, deadline);
    }

protected:
    const TIamDelegationSettings Settings;
    TIamCallCredentials Credentials;
};

} // namespace NKikimr::NIamDelegation
