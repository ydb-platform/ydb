#pragma once

#include "ydb/library/testlib/service_mocks/common.h"

#include <util/system/mutex.h>
#include <ydb/public/api/client/yc_private/servicecontrol/access_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/accessservice/access_service.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <iterator>

namespace {

bool IsServiceAuthenticated(const THashSet<TString>& allowedServiceAuthTokens, grpc::ServerContext* ctx) {
    if (allowedServiceAuthTokens.empty()) {
        return true;
    }
    const auto& meta = ctx->client_metadata();
    const auto authorizationHeaderRange = meta.equal_range("authorization");
    bool isServiceAuthenticated = false;
    for (auto it = authorizationHeaderRange.first; it != authorizationHeaderRange.second; ++it) {
        if (allowedServiceAuthTokens.contains(TString(it->second.cbegin(), it->second.cend()))) {
            isServiceAuthenticated = true;
            break;
        }
    }
    return isServiceAuthenticated;
}

} // namespace

class TAccessServiceMock : public yandex::cloud::priv::servicecontrol::v1::AccessService::Service {
public:
    template <class TResponseProto>
    struct TResponse {
        TResponseProto Response;
        grpc::Status Status = grpc::Status::OK;
        bool RequireRequestId = false;
    };

    THashMap<TString, TResponse<yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse>> AuthenticateData;
    THashMap<TString, TResponse<yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse>> AuthorizeData;

    TMutex UserIpMutex;
    TString CapturedXUserIP;

    template <class TResponseProto>
    void CheckRequestId(grpc::ServerContext* ctx, const TResponse<TResponseProto>& resp, const TString& token) {
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
        yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse* response) override
    {
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
        yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse* response) override
    {
        {
            std::lock_guard guard(UserIpMutex);
            CapturedXUserIP = NTestUtils::CaptureXUserIP(ctx);
        }

        if (request->resource_path_size() == 0) {
            return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "Permission Denied");
        }

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

class TAccessServiceMockV2 : public yandex::cloud::priv::accessservice::v2::AccessService::Service {
public:
    template <class TResponseProto>
    struct TResponse {
        TResponseProto Response;
        grpc::Status Status = grpc::Status::OK;
        bool RequireRequestId = false;
    };

    THashMap<TString, TResponse<yandex::cloud::priv::accessservice::v2::AuthenticateResponse>> AuthenticateData;
    THashMap<TString, TResponse<yandex::cloud::priv::accessservice::v2::AuthorizeResponse>> AuthorizeData;

    TMutex UserIpMutex;
    TString CapturedXUserIP;

    template <class TResponseProto>
    void CheckRequestId(grpc::ServerContext* ctx, const TResponse<TResponseProto>& resp, const TString& token) {
        if (resp.RequireRequestId) {
            auto [reqIdBegin, reqIdEnd] = ctx->client_metadata().equal_range("x-request-id");
            UNIT_ASSERT_C(reqIdBegin != reqIdEnd, "RequestId is expected. Token: " << token);
            UNIT_ASSERT_VALUES_EQUAL_C(std::distance(reqIdBegin, reqIdEnd), 1, "Only one RequestId is expected. Token: " << token);
            UNIT_ASSERT_C(!reqIdBegin->second.empty(), "RequestId is expected to be not empty. Token: " << token);
        }
    }

    grpc::Status Authenticate(
        grpc::ServerContext* ctx,
        const yandex::cloud::priv::accessservice::v2::AuthenticateRequest* request,
        yandex::cloud::priv::accessservice::v2::AuthenticateResponse* response) override
    {
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
        const yandex::cloud::priv::accessservice::v2::AuthorizeRequest* request,
        yandex::cloud::priv::accessservice::v2::AuthorizeResponse* response) override
    {
        {
            std::lock_guard guard(UserIpMutex);
            CapturedXUserIP = NTestUtils::CaptureXUserIP(ctx);
        }

        if (request->resource_path_size() == 0) {
            return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "Permission Denied");
        }

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

    grpc::Status BulkAuthorize(
        grpc::ServerContext* ctx,
        const yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest* request,
        yandex::cloud::priv::accessservice::v2::BulkAuthorizeResponse* response) override
    {
        {
            std::lock_guard guard(UserIpMutex);
            CapturedXUserIP = NTestUtils::CaptureXUserIP(ctx);
        }

        for (const auto& action : request->actions().items()) {
            if (action.resource_path_size() == 0) {
                return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "Permission Denied");
            }

            const TString& lastResourceId = action.resource_path(action.resource_path_size() - 1).id();
            const TString& token = request->signature().access_key_id() + request->iam_token() + "-" + action.permission() + "-" + lastResourceId;

            auto it = AuthorizeData.find(token);
            if (it != AuthorizeData.end() && it->second.Status.ok()) {
                CheckRequestId(ctx, it->second, token);
                if (it->second.Response.has_subject()) {
                    response->mutable_subject()->CopyFrom(it->second.Response.subject());
                }
            } else {
                auto* result_item = response->mutable_results()->add_items();
                result_item->set_permission(action.permission());
                result_item->mutable_resource_path()->CopyFrom(action.resource_path());
                result_item->mutable_permission_denied_error()->set_message((it != AuthorizeData.end()) ? it->second.Status.error_message() : "Permission denied");
            }
        }

        return grpc::Status::OK;
    }
};

class TTicketParserAccessServiceMockBase {
public:
    std::atomic_uint64_t AuthenticateCount = 0;
    std::atomic_uint64_t AuthorizeCount = 0;
    bool ShouldGenerateRetryableError = false;
    bool ShouldGenerateOneRetryableError = false;

    THashSet<TString> InvalidTokens = {"invalid"};
    THashSet<TString> UnavailableTokens;
    THashSet<TString> AllowedUserTokens = {"user1"};
    THashMap<TString, TString> AllowedServiceTokens = {{"service1", "root1/folder1"}};

    THashSet<TString> InvalidApiKeys = {"ApiKey-value-invalid"};
    THashSet<TString> UnavailableApiKeys;
    THashSet<TString> AllowedUserApiKeys = {"ApiKey-value-valid"};

    THashSet<TString> AllowedUserPermissions = {
        "user1-something.read",
        "ApiKey-value-valid-something.read",
        "ApiKey-value-valid-ydb.streams.write",
        "user1-monitoring.view"};
    THashMap<TString, TString> AllowedServicePermissions = {{"service1-something.write", "root1/folder1"}};
    THashSet<TString> AllowedResourceIds = {};
    THashSet<TString> UnavailableUserPermissions;

    TMutex UserIPMutex;
    TString CapturedXUserIP;

    template <typename TRequest, typename TResponse>
    grpc::Status HandleAuthenticateBase(const TRequest* request, TResponse* response) {
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
        }

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

    template <typename TRequest, typename TResponse>
    grpc::Status HandleAuthorizeBase(grpc::ServerContext* ctx, const TRequest* request, TResponse* response) {
        {
            std::lock_guard guard(UserIPMutex);
            CapturedXUserIP = NTestUtils::CaptureXUserIP(ctx);
        }

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
        }

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
};

class TTicketParserAccessServiceMock
    : public yandex::cloud::priv::servicecontrol::v1::AccessService::Service
    , public TTicketParserAccessServiceMockBase
{
public:
    grpc::Status Authenticate(
        grpc::ServerContext*,
        const yandex::cloud::priv::servicecontrol::v1::AuthenticateRequest* request,
        yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse* response) override
    {
        return HandleAuthenticateBase(request, response);
    }

    grpc::Status Authorize(
        grpc::ServerContext* ctx,
        const yandex::cloud::priv::servicecontrol::v1::AuthorizeRequest* request,
        yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse* response) override
    {
        return HandleAuthorizeBase(ctx, request, response);
    }
};

class TTicketParserAccessServiceMockV2
    : public yandex::cloud::priv::accessservice::v2::AccessService::Service
    , public TTicketParserAccessServiceMockBase
{
public:
    bool isUserAuthenticated = true;
    TString UnauthenticatedErrorMessage = "User is unauthenticated";
    THashSet<TString> AllowedServiceAuthTokens;

    grpc::Status Authenticate(
        grpc::ServerContext*,
        const yandex::cloud::priv::accessservice::v2::AuthenticateRequest* request,
        yandex::cloud::priv::accessservice::v2::AuthenticateResponse* response) override
    {
        return HandleAuthenticateBase(request, response);
    }

    grpc::Status Authorize(
        grpc::ServerContext* ctx,
        const yandex::cloud::priv::accessservice::v2::AuthorizeRequest* request,
        yandex::cloud::priv::accessservice::v2::AuthorizeResponse* response) override
    {
        return HandleAuthorizeBase(ctx, request, response);
    }

    ::grpc::Status BulkAuthorize(
        ::grpc::ServerContext* ctx,
        const ::yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest* request,
        ::yandex::cloud::priv::accessservice::v2::BulkAuthorizeResponse* response) override
    {
        {
            std::lock_guard guard(UserIPMutex);
            CapturedXUserIP = NTestUtils::CaptureXUserIP(ctx);
        }

        if (!IsServiceAuthenticated(AllowedServiceAuthTokens, ctx)) {
            return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Unauthenticated service");
        }

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
            if (!isUserAuthenticated) {
                auto error = response->mutable_unauthenticated_error();
                error->set_message(UnauthenticatedErrorMessage);
                return grpc::Status(grpc::StatusCode::OK, "OK");
            }
            TString token = request->has_iam_token() ? request->iam_token() : request->api_key();
            if (request->has_actions()) {
                const auto& actions = request->actions();
                bool wasFoundFirstAccessDenied = false;
                for (const auto& action : actions.items()) {
                    if (UnavailableUserPermissions.count(token + '-' + action.permission()) > 0) {
                        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable");
                    }

                    bool allowedResource = true;
                    if (!AllowedResourceIds.empty()) {
                        allowedResource = false;
                        for (const auto& resourcePath : action.resource_path()) {
                            if (AllowedResourceIds.count(resourcePath.id()) > 0) {
                                allowedResource = true;
                            }
                        }
                    }
                    if (allowedResource) {
                        if (AllowedUserPermissions.count(token + '-' + action.permission()) > 0) {
                            response->mutable_subject()->mutable_user_account()->set_id(token);

                        } else if (AllowedServicePermissions.count(token + '-' + action.permission()) > 0) {
                            response->mutable_subject()->mutable_service_account()->set_id(token);
                            response->mutable_subject()->mutable_service_account()->set_folder_id(AllowedServicePermissions[token + '-' + action.permission()]);
                        } else {
                            if (request->result_filter() == yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest::ALL_FAILED) {
                                SetAccessDenied(response->mutable_results(), action);
                            } else {
                                if (!wasFoundFirstAccessDenied) {
                                    SetAccessDenied(response->mutable_results(), action);
                                    wasFoundFirstAccessDenied = true;
                                }
                            }
                        }
                    } else {
                        SetAccessDenied(response->mutable_results(), action);
                    }
                }
            }
            return grpc::Status(grpc::StatusCode::OK, "OK");
        }
    }

private:
    void SetAccessDenied(
        ::yandex::cloud::priv::accessservice::v2::BulkAuthorizeResponse_Results* results,
        const ::yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest_Action& action)
    {
        auto result = results->add_items();
        result->set_permission(action.permission());
        for (const auto& resource_path : action.resource_path()) {
            auto rp = result->add_resource_path();
            rp->CopyFrom(resource_path);
        }
        auto error = result->mutable_permission_denied_error();
        error->set_message("Access Denied");
    }
};
