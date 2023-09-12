#pragma once

#include <ydb/public/api/client/yc_private/servicecontrol/access_service.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <iterator>

class TAccessServiceMock : public yandex::cloud::priv::servicecontrol::v1::AccessService::Service {
public:
    template <class TResonseProto>
    struct TResponse {
        TResonseProto Response;
        grpc::Status Status = grpc::Status::OK;
        bool RequireRequestId = false;
    };

    THashMap<TString, TResponse<yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse>> AuthenticateData;
    THashMap<TString, TResponse<yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse>> AuthorizeData;

    template <class TResonseProto>
    void CheckRequestId(grpc::ServerContext* ctx, const TResponse<TResonseProto>& resp, const TString& token) {
        if (resp.RequireRequestId) {
            auto [reqIdBegin, reqIdEnd] = ctx->client_metadata().equal_range("x-request-id");
            UNIT_ASSERT_C(reqIdBegin != reqIdEnd, "RequestId is expected. Token: " << token);
            UNIT_ASSERT_VALUES_EQUAL_C(std::distance(reqIdBegin, reqIdEnd), 1, "Only one RequestId is expected. Token: " << token);
            UNIT_ASSERT_C(!reqIdBegin->second.empty(), "RequestId is expected to be not empty. Token: " << token);
        }
    }

    grpc::Status Authenticate(
            grpc::ServerContext* ctx,
            const yandex::cloud::priv::servicecontrol::v1::AuthenticateRequest* request,
            yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse* response) override {
        TString key;
        if (request->has_signature()) {
            key = request->signature().v4_parameters().service();
        } else {
            key = request->iam_token();
        }

        auto it = AuthenticateData.find(key);
        if (it != AuthenticateData.end()) {
            response->CopyFrom(it->second.Response);
            CheckRequestId(ctx, it->second, key);
            return it->second.Status;
        } else {
            return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "Permission Denied");
        }
    }

    grpc::Status Authorize(
            grpc::ServerContext* ctx,
            const yandex::cloud::priv::servicecontrol::v1::AuthorizeRequest* request,
            yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse* response) override {
        const TString& lastResourceId = request->resource_path(request->resource_path_size() - 1).id();
        const TString& token = request->signature().access_key_id() + request->iam_token() + "-" + request->permission() + "-" + lastResourceId;
        auto it = AuthorizeData.find(token);
        if (it != AuthorizeData.end()) {
            response->CopyFrom(it->second.Response);
            CheckRequestId(ctx, it->second, token);
            return it->second.Status;
        } else {
            return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "Permission Denied");
        }
    }
};

class TTicketParserAccessServiceMock : public yandex::cloud::priv::servicecontrol::v1::AccessService::Service {
public:
    std::atomic_uint64_t AuthenticateCount = 0;
    std::atomic_uint64_t AuthorizeCount= 0;

    THashSet<TString> InvalidTokens = {"invalid"};
    THashSet<TString> UnavailableTokens;
    THashSet<TString> AllowedUserTokens = {"user1"};
    THashMap<TString, TString> AllowedServiceTokens = {{"service1", "root1/folder1"}};

    THashSet<TString> InvalidApiKeys = {"ApiKey-value-invalid"};
    THashSet<TString> UnavailableApiKeys;
    THashSet<TString> AllowedUserApiKeys = {"ApiKey-value-valid"};

    bool ShouldGenerateRetryableError = false;
    bool ShouldGenerateOneRetryableError = false;

    grpc::Status Authenticate(
            grpc::ServerContext*,
            const yandex::cloud::priv::servicecontrol::v1::AuthenticateRequest* request,
            yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse* response) override {

        ++AuthenticateCount;
        if (request->has_signature()) {
            if (ShouldGenerateRetryableError) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable");
            }
            if (ShouldGenerateOneRetryableError) {
                ShouldGenerateOneRetryableError = false;
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable");
            }
            response->mutable_subject()->mutable_user_account()->set_id("user1");
            return grpc::Status::OK;
        } else {
            TString token = request->iam_token();
            if (InvalidTokens.count(token) > 0) {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid Token");
            }
            if (UnavailableTokens.count(token) > 0) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable");
            }
            if (AllowedUserTokens.count(token) > 0) {
                response->mutable_subject()->mutable_user_account()->set_id(token);
                return grpc::Status::OK;
            }
            if (AllowedServiceTokens.count(token) > 0) {
                response->mutable_subject()->mutable_service_account()->set_id(token);
                response->mutable_subject()->mutable_service_account()->set_folder_id(AllowedServiceTokens[token]);
                return grpc::Status::OK;
            }

            TString apiKey = request->api_key();
            if (InvalidApiKeys.count(apiKey) > 0) {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid ApiKey");
            }
            if (UnavailableApiKeys.count(apiKey) > 0) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable");
            }
            if (AllowedUserApiKeys.count(apiKey) > 0) {
                response->mutable_subject()->mutable_user_account()->set_id(apiKey);
                return grpc::Status::OK;
            }

            return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Access Denied");
        }
    }

    THashSet<TString> AllowedUserPermissions = {"user1-something.read", "ApiKey-value-valid-something.read", "ApiKey-value-valid-ydb.api.kafkaPlainAuth"};
    THashMap<TString, TString> AllowedServicePermissions = {{"service1-something.write", "root1/folder1"}};
    THashSet<TString> AllowedResourceIds = {};
    THashSet<TString> UnavailableUserPermissions;

    grpc::Status Authorize(
            grpc::ServerContext*,
            const yandex::cloud::priv::servicecontrol::v1::AuthorizeRequest* request,
            yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse* response) override {
        ++AuthorizeCount;
        if (request->has_signature()) {
            if (ShouldGenerateRetryableError) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable");
            }
            if (ShouldGenerateOneRetryableError) {
                ShouldGenerateOneRetryableError = false;
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable");
            }
            response->mutable_subject()->mutable_user_account()->set_id("user1");
            return grpc::Status::OK;
        } else {
            TString token = request->has_iam_token() ? request->iam_token() : request->api_key();
            if (UnavailableUserPermissions.count(token + '-' + request->permission()) > 0) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable");
            }
            bool allowedResource = true;
            if (!AllowedResourceIds.empty()) {
                allowedResource = false;
                for (const auto& resourcePath : request->resource_path()) {
                    if (AllowedResourceIds.count(resourcePath.id()) > 0) {
                        allowedResource = true;
                    }
                }
            }
            if (allowedResource) {
                if (AllowedUserPermissions.count(token + '-' + request->permission()) > 0) {
                    response->mutable_subject()->mutable_user_account()->set_id(token);
                    return grpc::Status::OK;
                }
                if (AllowedServicePermissions.count(token + '-' + request->permission()) > 0) {
                    response->mutable_subject()->mutable_service_account()->set_id(token);
                    response->mutable_subject()->mutable_service_account()->set_folder_id(AllowedServicePermissions[token + '-' + request->permission()]);
                    return grpc::Status::OK;
                }
            }
            return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Access Denied");
        }
    }
};
