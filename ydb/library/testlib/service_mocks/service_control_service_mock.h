#pragma once

#include <ydb/public/api/client/yc_private/iam/service_control_service.grpc.pb.h>

#include <google/rpc/error_details.pb.h>
#include <google/rpc/status.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

// Mock of yandex.cloud.priv.iam.v1.ServiceControlService that records SetupDelegation/RevokeDelegation
// requests and lets tests inject failures and not-yet-done operations.
class TServiceControlServiceMock : public yandex::cloud::priv::iam::v1::ServiceControlService::Service {
public:
    struct TRecordedCall {
        TString Method; // "SetupDelegation" | "RevokeDelegation"
        TString Authorization;
        TInstant Timestamp; // when the call was received
        yandex::cloud::priv::iam::v1::SetupDelegationRequest Setup;
        yandex::cloud::priv::iam::v1::RevokeDelegationRequest Revoke;
    };

    TMutex Mutex;
    TDeque<TRecordedCall> Calls;

    // expected "authorization" header (e.g. "Bearer ssa-token"); empty = do not check
    TString ExpectedAuthorization;
    // operations returned with done=false for the first NotDoneCount calls
    ui32 NotDoneCount = 0;
    // error injected into the next FailCount calls
    ui32 FailCount = 0;
    grpc::StatusCode FailStatus = grpc::StatusCode::UNAVAILABLE;
    // ServiceControlFailureType attached to injected failures (direct and operation) as a
    // google.rpc.PreconditionFailure violation, the way IAM reports them; empty = no details
    TString FailureType;
    // error reported inside a done operation for the next OperationErrorCount calls
    ui32 OperationErrorCount = 0;
    ui32 NextOperationId = 1;

    ui32 SetupCalls() {
        with_lock (Mutex) {
            return CountIf(Calls, [](const auto& c) { return c.Method == "SetupDelegation"; });
        }
    }

    ui32 RevokeCalls() {
        with_lock (Mutex) {
            return CountIf(Calls, [](const auto& c) { return c.Method == "RevokeDelegation"; });
        }
    }

    TRecordedCall LastCall() {
        with_lock (Mutex) {
            UNIT_ASSERT(!Calls.empty());
            return Calls.back();
        }
    }

    grpc::Status SetupDelegation(grpc::ServerContext* context,
                                 const yandex::cloud::priv::iam::v1::SetupDelegationRequest* request,
                                 ydb::yc::priv::operation::Operation* response) override
    {
        TRecordedCall call;
        call.Method = "SetupDelegation";
        call.Setup = *request;
        return Record(context, std::move(call), response);
    }

    grpc::Status RevokeDelegation(grpc::ServerContext* context,
                                  const yandex::cloud::priv::iam::v1::RevokeDelegationRequest* request,
                                  ydb::yc::priv::operation::Operation* response) override
    {
        TRecordedCall call;
        call.Method = "RevokeDelegation";
        call.Revoke = *request;
        return Record(context, std::move(call), response);
    }

private:
    grpc::Status Record(grpc::ServerContext* context, TRecordedCall&& call, ydb::yc::priv::operation::Operation* response) {
        auto [begin, end] = context->client_metadata().equal_range("authorization");
        if (begin != end) {
            call.Authorization = TString(begin->second.data(), begin->second.size());
        }
        call.Timestamp = TInstant::Now();
        with_lock (Mutex) {
            Calls.push_back(std::move(call));
            if (!ExpectedAuthorization.empty() && Calls.back().Authorization != ExpectedAuthorization) {
                return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "unexpected authorization");
            }
            if (FailCount > 0) {
                --FailCount;
                if (FailureType.empty()) {
                    return grpc::Status(FailStatus, "injected failure");
                }
                google::rpc::Status details;
                details.set_code(FailStatus);
                details.set_message("injected failure");
                AddFailureDetails(details);
                return grpc::Status(FailStatus, "injected failure", details.SerializeAsString());
            }
            response->set_id(TStringBuilder() << "op-" << NextOperationId++);
            response->set_description(Calls.back().Method);
            if (NotDoneCount > 0) {
                --NotDoneCount;
                response->set_done(false);
            } else if (OperationErrorCount > 0) {
                --OperationErrorCount;
                response->set_done(true);
                response->mutable_error()->set_code(grpc::StatusCode::PERMISSION_DENIED);
                response->mutable_error()->set_message("injected operation error");
                if (!FailureType.empty()) {
                    AddFailureDetails(*response->mutable_error());
                }
            } else {
                response->set_done(true);
            }
            return grpc::Status::OK;
        }
    }

    void AddFailureDetails(google::rpc::Status& status) const {
        google::rpc::PreconditionFailure failure;
        auto* violation = failure.add_violations();
        violation->set_type(FailureType);
        violation->set_subject("service-control");
        violation->set_description("injected " + FailureType);
        status.add_details()->PackFrom(failure);
    }
};
