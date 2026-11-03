#pragma once

#include "settings.h"
#include <ydb/core/util/async_backoff.h>

#include <ydb/core/base/iam_delegation.h>

#include <ydb/core/security/token_manager/token_manager.h>
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

// Error of an IAM request. GrpcCode is the code of
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

// Bounded diagnostic groups; remote text is never returned.
enum class EDelegationFailureReason {
    WrongCloud, ServiceNotEnabled, CloudNotFound, ServiceAccountNotFound,
    SubjectNotDelegatable, AuthenticationFailed, PermissionDenied,
};

bool IsRetryableGrpcStatus(const NYdbGrpc::TGrpcStatus& status);

// A hint for the user for the ServiceControl failure types (ServiceControlFailureType in
// service_control.proto) that the user can act on; empty for the rest.
TStringBuf HintForIamFailureType(TStringBuf type);

// Explain authoritative authentication/permission codes and allowlisted
// google.rpc.PreconditionFailure types using fixed text. Known method names
// add the corresponding IAM requirements; remote text is never returned.
TString ExplainIamFailure(const google::rpc::Status& status, TStringBuf method = {});

// The gRPC error details carry a serialized google.rpc.Status.
TString ExplainIamFailure(const NYdbGrpc::TGrpcStatus& status, TStringBuf method = {});

// Bounded allowlist for durable diagnostics. Unknown types and oversized or
// malformed details contribute no prerequisite reason. The authoritative reply
// code retains its authentication/permission reason without those details.
TVector<EDelegationFailureReason> ExtractIamFailureReasons(const google::rpc::Status& status);
TVector<EDelegationFailureReason> ExtractIamFailureReasons(const NYdbGrpc::TGrpcStatus& status);

enum class EIamServiceHealth { Healthy, Unavailable, CredentialsRejected };
EIamServiceHealth IamServiceHealth(const NYdbGrpc::TGrpcStatus& status);

Ydb::StatusIds::StatusCode MapGrpcStatus(int code);

// Owned by the long-lived subscribing actor and accessed only in its mailbox, including by
// nested coroutines. Updates do not include expiry; any unsuccessful update invalidates the cache.
struct TIamCallCredentials {
    TString Token;
    TEvTokenManager::TStatus Status{TEvTokenManager::TStatus::ECode::NOT_READY, {}};
    NActors::TAsyncEvent Updated;
};

// The coroutines below are nested ones and must be awaited from a coroutine of the owning actor.
NActors::async<TString> GetIamCallToken(TIamCallCredentials& credentials, TMonotonic deadline);

// Reject work at or beyond its absolute deadline.
void CheckIamCallDeadline(TMonotonic deadline);

// Sends the request to the client actor and waits for its reply of any type: the typed response, or
// TEvUndelivered when the client actor is gone (a typed wait would never resume in that case).
NActors::async<NActors::IEventHandle::TPtr> IamClientRequest(NActors::TActorId client, NActors::IEventBase* request);

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
        , RetryBackoff(TDuration::Seconds(1), TDuration::Minutes(1))
    {
        static_assert(std::derived_from<TDerived, TIamActorBase>, "TDerived must derive from TIamActorBase<TDerived>");
    }

    using NActors::IActorExceptionHandler::OnUnhandledException;
    bool OnUnhandledException(const std::exception&) override {
        YDB_LOG_ERROR_COMP(NKikimrServices::IAM_DELEGATION, "Unhandled exception in an IAM delegation actor",
            {"actor", TBase::SelfId()});
        return true;
    }


protected:
    const TIamDelegationSettings Settings;
    TIamCallCredentials Credentials;
    TAsyncBackoff RetryBackoff;
};

} // namespace NKikimr::NIamDelegation
