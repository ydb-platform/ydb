#pragma once

#include <ydb/public/api/client/nc_private/accessservice/access_service.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <iterator>

class TNebiusAccessServiceMock : public nebius::iam::v1::AccessService::Service {
public:
    template <class TResonseProto>
    struct TResponse {
        TResonseProto Response;
        grpc::Status Status = grpc::Status::OK;
        bool RequireRequestId = false;
    };

    THashMap<TString, TResponse<nebius::iam::v1::AuthenticateResponse>> AuthenticateData;
    THashMap<TString, TResponse<nebius::iam::v1::AuthorizeResponse>> AuthorizeData;

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
            const nebius::iam::v1::AuthenticateRequest* request,
            nebius::iam::v1::AuthenticateResponse* response) override {
        TString key = request->iam_token();

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
            const nebius::iam::v1::AuthorizeRequest* request,
            nebius::iam::v1::AuthorizeResponse* response) override {
        UNIT_ASSERT_VALUES_EQUAL_C(request->checks_size(), 1, "Nebius access service mock does not support multiple checks yet");
        const auto checkIt = request->checks().find(0);
        UNIT_ASSERT(checkIt != request->checks().end());
        const auto& check = checkIt->second;
        const TString& lastResourceId = check.resource_path().path(check.resource_path().path_size() - 1).id();
        const TString& token = check.iam_token() + "-" + check.permission().name() + "-" + lastResourceId;
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

class TTicketParserNebiusAccessServiceMock : public nebius::iam::v1::AccessService::Service {
public:
    std::atomic_uint64_t AuthenticateCount = 0;
    std::atomic_uint64_t AuthorizeCount = 0;

    THashSet<TString> InvalidTokens = {"invalid"};
    THashSet<TString> UnavailableTokens;
    THashSet<TString> AllowedUserTokens = {"user1"};
    THashMap<TString, TString> AllowedServiceTokens = {{"service1", "root1/folder1"}};

    bool ShouldGenerateRetryableError = false;
    bool ShouldGenerateOneRetryableError = false;

    template <class TResponse>
    grpc::Status LogResponse(const grpc::Status& status, const TResponse* response, const TString& method) {
        Cerr << "NebiusAccessService::" << method << " response\n"
             << response->Utf8DebugString()
             << "\n"
             << (int)status.error_code() << ": \"" << status.error_message() << "\""
             << Endl;
        return status;
    }

    grpc::Status Authenticate(
            grpc::ServerContext*,
            const nebius::iam::v1::AuthenticateRequest* request,
            nebius::iam::v1::AuthenticateResponse* response) override {
        Cerr << "NebiusAccessService::Authenticate request\n"
             << request->Utf8DebugString()
             << Endl;

        ++AuthenticateCount;

        if (ShouldGenerateRetryableError) {
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable");
        }
        if (ShouldGenerateOneRetryableError) {
            ShouldGenerateOneRetryableError = false;
            return LogResponse(
                grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable"),
                response,
                "Authenticate");
        }

        const TString& token = request->iam_token();
        UNIT_ASSERT(token);

        if (IsIn(InvalidTokens, token)) {
            return LogResponse(
                grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid Token"),
                response,
                "Authenticate");
        }

        if (IsIn(UnavailableTokens, token)) {
            return LogResponse(
                grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable"),
                response,
                "Authenticate");
        }

        if (IsIn(AllowedUserTokens, token)) {
            response->mutable_account()->mutable_user_account()->set_id(token);
            return LogResponse(
                grpc::Status::OK,
                response,
                "Authenticate");
        }

        if (IsIn(AllowedServiceTokens, token)) {
            response->mutable_account()->mutable_service_account()->set_id(token);
            return LogResponse(
                grpc::Status::OK,
                response,
                "Authenticate");
        }

        return LogResponse(
                grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Access Denied"),
                response,
                "Authenticate");
    }

    THashSet<TString> AllowedUserPermissions = {
        "user1-something.read",
        "user1-monitoring.view"};
    THashMap<TString, TString> AllowedServicePermissions = {{"service1-something.write", "root1/folder1"}};
    THashSet<TString> AllowedResourceIds;
    THashSet<TString> UnavailableUserPermissions;
    TString ContainerId;

    grpc::Status Authorize(
            grpc::ServerContext*,
            const nebius::iam::v1::AuthorizeRequest* request,
            nebius::iam::v1::AuthorizeResponse* response) override {
        Cerr << "NebiusAccessService::Authorize request\n"
             << request->Utf8DebugString()
             << Endl;

        ++AuthorizeCount;

        if (ShouldGenerateRetryableError) {
            return LogResponse(
                grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable"),
                response,
                "Authorize");
        }
        if (ShouldGenerateOneRetryableError) {
            ShouldGenerateOneRetryableError = false;
            return LogResponse(
                grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable"),
                response,
                "Authorize");
        }

        for (const auto& [checkId, check] : request->checks()) {
            const TString& token = check.iam_token();
            const TString& permission = check.permission().name();
            UNIT_ASSERT(permission);

            const TString mockPermToFind = TStringBuilder() << token << '-' << permission;

            if (IsIn(UnavailableTokens, token)) {
                return LogResponse(
                    grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable"),
                    response,
                    "Authorize");
            }

            if (IsIn(UnavailableUserPermissions, mockPermToFind)) {
                return LogResponse(
                    grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service Unavailable"),
                    response,
                    "Authorize");
            }

            auto& result = (*response->mutable_results())[checkId];
            result.set_resultcode(nebius::iam::v1::AuthorizeResult::PERMISSION_DENIED);

            if (ContainerId && check.container_id() != ContainerId) {
                result.set_resultcode(nebius::iam::v1::AuthorizeResult::PERMISSION_DENIED);
                continue;
            }

            bool allowedResource = true;
            if (!AllowedResourceIds.empty()) {
                allowedResource = false;
                if (IsIn(AllowedResourceIds, check.container_id())) {
                    allowedResource = true;
                }
                for (const auto& resourcePath : check.resource_path().path()) {
                    if (IsIn(AllowedResourceIds, resourcePath.id())) {
                        allowedResource = true;
                    }
                }
            }

            if (allowedResource) {
                if (IsIn(AllowedUserTokens, token)) {
                    if (IsIn(AllowedUserPermissions, mockPermToFind)) {
                        result.mutable_account()->mutable_user_account()->set_id(token);
                        result.set_resultcode(nebius::iam::v1::AuthorizeResult::OK);
                    }
                }

                if (IsIn(AllowedServiceTokens, token)) {
                    if (IsIn(AllowedServicePermissions, mockPermToFind)) {
                        result.mutable_account()->mutable_service_account()->set_id(token);
                        result.set_resultcode(nebius::iam::v1::AuthorizeResult::OK);
                    }
                }
            }
        }

        return LogResponse(
            grpc::Status(grpc::StatusCode::OK, "OK"),
            response,
            "Authorize");
    }
};
