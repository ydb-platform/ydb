#pragma once

#include <ydb/public/api/client/yc_private/iam/iam_token_service.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

#include <atomic>

class TIamTokenServiceMock : public yandex::cloud::priv::iam::v1::IamTokenService::Service {
public:
    THashMap<TString, yandex::cloud::priv::iam::v1::CreateIamTokenResponse> IamTokens;
    TString Identity;

    TMaybe<grpc::Status> CheckAuthorization(grpc::ServerContext* context) {
        if (!Identity.empty()) {
            auto[reqIdBegin, reqIdEnd] = context->client_metadata().equal_range("authorization");
            UNIT_ASSERT_C(reqIdBegin != reqIdEnd, "Authorization is expected.");
            if (Identity != TStringBuf(reqIdBegin->second.cbegin(), reqIdBegin->second.cend())) {
                return grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
                                    TStringBuilder() << "Access for user " << Identity << " is forbidden");
            }
        }

        return Nothing();
    }

    virtual grpc::Status CreateForServiceAccount(grpc::ServerContext* context,
                             const yandex::cloud::priv::iam::v1::CreateIamTokenForServiceAccountRequest* request,
                             yandex::cloud::priv::iam::v1::CreateIamTokenResponse* response) override
    {
        auto status = CheckAuthorization(context);
        if (status.Defined()) {
            return *status;
        }

        TString id = request->service_account_id();
        auto it = IamTokens.find(id);
        if (it != IamTokens.end()) {
            response->CopyFrom(it->second);
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Iam token not found");
        }
    }

    // CreateForService: tokens are looked up by "<resource_id>/<target_service_account_id>" in ServiceTokens.
    // Every call is recorded in CreateForServiceRequests; the returned expires_at is Now + ServiceTokenLifetime.
    // The maps are written by tests while gRPC threads serve requests: use the setters (they take ServiceMutex).
    THashMap<TString, TString> ServiceTokens;
    TDuration ServiceTokenLifetime = TDuration::Hours(12);
    std::atomic<ui32> ServiceTokenFailCount = 0;
    grpc::StatusCode ServiceTokenFailStatus = grpc::StatusCode::UNAVAILABLE;
    TMutex ServiceMutex;
    TVector<yandex::cloud::priv::iam::v1::CreateIamTokenForServiceRequest> CreateForServiceRequests;
    std::atomic<ui32> CreateForServiceCalls = 0;    // requests received (recorded before the gate)
    std::atomic<ui32> CreateForServiceAnswered = 0; // requests that passed the gate and were answered
    // Gate of held calls: when HoldServiceTokenCallsFrom is not 0, every call whose ordinal (1-based) is at
    // least that value, for the target set with SetServiceTokenHoldTarget (every target when it is empty),
    // blocks after being recorded until ReleaseHeldServiceTokenCalls() is called. Lets a test order events
    // deterministically. The knobs are atomics (or set under ServiceMutex) because tests flip them while
    // the gRPC threads serve requests.
    std::atomic<ui32> HoldServiceTokenCallsFrom = 0;
    TCondVar ServiceTokenHoldCondVar;
    // When set, every reply carries a distinct token "<ServiceTokens[key]>-<ordinal of the call>", so that a
    // test can tell a refreshed token from the one it replaced. Off by default: the plain stored token is returned.
    std::atomic<bool> UniqueServiceTokens = false;
    // When set, replies carry no expires_at (a misbehaving token service).
    std::atomic<bool> OmitServiceTokenExpiry = false;
    TString ServiceTokenHoldTarget; // written through SetServiceTokenHoldTarget, read under ServiceMutex

    static TString ServiceTokenKey(const TString& resourceId, const TString& targetServiceAccountId) {
        return resourceId + "/" + targetServiceAccountId;
    }

    void SetServiceToken(const TString& resourceId, const TString& targetServiceAccountId, const TString& token) {
        with_lock (ServiceMutex) {
            ServiceTokens[ServiceTokenKey(resourceId, targetServiceAccountId)] = token;
        }
    }

    void EraseServiceToken(const TString& resourceId, const TString& targetServiceAccountId) {
        with_lock (ServiceMutex) {
            ServiceTokens.erase(ServiceTokenKey(resourceId, targetServiceAccountId));
        }
    }

    void ReleaseHeldServiceTokenCalls() {
        with_lock (ServiceMutex) {
            HoldServiceTokenCallsFrom = 0;
            ServiceTokenHoldCondVar.BroadCast();
        }
    }

    void SetServiceTokenHoldTarget(const TString& targetServiceAccountId) {
        with_lock (ServiceMutex) {
            ServiceTokenHoldTarget = targetServiceAccountId;
        }
    }

    virtual grpc::Status CreateForService(grpc::ServerContext* context,
                             const yandex::cloud::priv::iam::v1::CreateIamTokenForServiceRequest* request,
                             yandex::cloud::priv::iam::v1::CreateIamTokenResponse* response) override
    {
        auto status = CheckAuthorization(context);
        if (status.Defined()) {
            return *status;
        }

        with_lock (ServiceMutex) {
            const ui32 ordinal = ++CreateForServiceCalls;
            CreateForServiceRequests.push_back(*request);
            const auto held = [&]() {
                const ui32 from = HoldServiceTokenCallsFrom.load();
                return from != 0 && ordinal >= from
                    && (ServiceTokenHoldTarget.empty() || ServiceTokenHoldTarget == request->target_service_account_id());
            };
            while (held()) {
                ServiceTokenHoldCondVar.WaitI(ServiceMutex);
            }
            ++CreateForServiceAnswered;
            if (ServiceTokenFailCount.load() > 0) {
                --ServiceTokenFailCount;
                return grpc::Status(ServiceTokenFailStatus, "injected failure");
            }
            auto it = ServiceTokens.find(ServiceTokenKey(request->resource_id(), request->target_service_account_id()));
            if (it == ServiceTokens.end()) {
                return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "delegation not found");
            }
            if (UniqueServiceTokens.load()) {
                response->set_iam_token(TStringBuilder() << it->second << "-" << ordinal);
            } else {
                response->set_iam_token(it->second);
            }
            const auto now = TInstant::Now();
            response->mutable_issued_at()->set_seconds(now.Seconds());
            if (!OmitServiceTokenExpiry.load()) {
                response->mutable_expires_at()->set_seconds((now + ServiceTokenLifetime).Seconds());
            }
            return grpc::Status::OK;
        }
    }

};

