#pragma once

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <google/rpc/status.pb.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NIamDelegation {

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

} // namespace NKikimr::NIamDelegation
