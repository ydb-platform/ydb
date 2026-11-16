#include <ydb/core/security/iam_delegation/iam_actor_base.h>

#include <library/cpp/testing/unittest/registar.h>
#include <google/rpc/error_details.pb.h>

namespace NKikimr::NIamDelegation {
namespace {

constexpr TStringBuf Sentinel = "credential-sentinel-never-expose";

void CheckAuthenticationFailure(const NYdbGrpc::TGrpcStatus& status, TStringBuf expected) {
    UNIT_ASSERT(!IsRetryableGrpcStatus(status));
    const auto reasons = ExtractIamFailureReasons(status);
    UNIT_ASSERT_VALUES_EQUAL(reasons.size(), 1);
    UNIT_ASSERT_STRING_CONTAINS(ExplainIamFailure(status), expected);
    UNIT_ASSERT(!ExplainIamFailure(status).Contains(Sentinel));
}

} // namespace

Y_UNIT_TEST_SUITE(IamAuthenticationFailures) {
    Y_UNIT_TEST(AuthenticationAndPermissionFailuresHaveDifferentReasons) {
        NYdbGrpc::TGrpcStatus status;
        status.GRpcStatusCode = grpc::StatusCode::UNAUTHENTICATED;
        status.Msg = TString(Sentinel);
        CheckAuthenticationFailure(status, "authentication failed");
        const auto authentication = ExtractIamFailureReasons(status);
        status.GRpcStatusCode = grpc::StatusCode::PERMISSION_DENIED;
        CheckAuthenticationFailure(status, "permission denied");
        UNIT_ASSERT(authentication != ExtractIamFailureReasons(status));
    }

    Y_UNIT_TEST(TransportCodeTakesPrecedenceOverContradictoryDetails) {
        for (auto code : {grpc::StatusCode::UNAUTHENTICATED, grpc::StatusCode::PERMISSION_DENIED}) {
            NYdbGrpc::TGrpcStatus status;
            status.GRpcStatusCode = code;
            google::rpc::Status details;
            details.set_code(code == grpc::StatusCode::UNAUTHENTICATED
                ? grpc::StatusCode::PERMISSION_DENIED : grpc::StatusCode::UNAUTHENTICATED);
            details.set_message(TString(Sentinel));
            status.Details = details.SerializeAsString();
            CheckAuthenticationFailure(status, code == grpc::StatusCode::UNAUTHENTICATED
                ? "authentication failed" : "permission denied");
        }
    }

    Y_UNIT_TEST(MalformedAndOversizedDetailsCannotHideAuthenticationKind) {
        for (auto code : {grpc::StatusCode::UNAUTHENTICATED, grpc::StatusCode::PERMISSION_DENIED}) {
            for (const auto& details : {TString("malformed"), TString(65537, 'x')}) {
                NYdbGrpc::TGrpcStatus status;
                status.GRpcStatusCode = code;
                status.Details = details;
                CheckAuthenticationFailure(status, code == grpc::StatusCode::UNAUTHENTICATED
                    ? "authentication failed" : "permission denied");
            }
        }
    }

    Y_UNIT_TEST(AuthenticationFailureIgnoresUntrustedBusinessPrerequisites) {
        NYdbGrpc::TGrpcStatus status;
        status.GRpcStatusCode = grpc::StatusCode::UNAUTHENTICATED;
        google::rpc::Status details;
        details.set_code(grpc::StatusCode::PERMISSION_DENIED);
        google::rpc::PreconditionFailure prerequisites;
        auto* violation = prerequisites.add_violations();
        violation->set_type("SUBJECT_NOT_FOUND");
        details.add_details()->PackFrom(prerequisites);
        status.Details = details.SerializeAsString();
        CheckAuthenticationFailure(status, "authentication failed");
        UNIT_ASSERT(!ExplainIamFailure(status).Contains("subject"));
    }

    Y_UNIT_TEST(InternalTransportErrorsDoNotBlameCredentialsOrPermissions) {
        for (auto code : {grpc::StatusCode::UNAUTHENTICATED, grpc::StatusCode::PERMISSION_DENIED}) {
            NYdbGrpc::TGrpcStatus status;
            status.InternalError = true;
            status.GRpcStatusCode = code;
            google::rpc::Status details;
            details.set_code(code);
            status.Details = details.SerializeAsString();
            UNIT_ASSERT(IsRetryableGrpcStatus(status));
            UNIT_ASSERT(ExtractIamFailureReasons(status).empty());
            UNIT_ASSERT(ExplainIamFailure(status).empty());
        }
    }

    Y_UNIT_TEST(TerminalOperationStatusKeepsAuthenticationKind) {
        for (auto code : {grpc::StatusCode::UNAUTHENTICATED, grpc::StatusCode::PERMISSION_DENIED}) {
            google::rpc::Status status;
            status.set_code(code);
            status.set_message(TString(Sentinel));
            UNIT_ASSERT_VALUES_EQUAL(ExtractIamFailureReasons(status).size(), 1);
            UNIT_ASSERT_STRING_CONTAINS(ExplainIamFailure(status), code == grpc::StatusCode::UNAUTHENTICATED
                ? "authentication failed" : "permission denied");
            UNIT_ASSERT(!ExplainIamFailure(status).Contains(Sentinel));
        }
    }
}

} // namespace NKikimr::NIamDelegation
