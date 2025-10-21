#pragma once

#include <ydb/public/api/client/nc_private/iam/v1/profile_service.grpc.pb.h>

class TProfileServiceMock : public nebius::iam::v1::ProfileService::Service {
public:
    static constexpr TStringBuf VALID_USER_TOKEN = "good-user-token";
    static constexpr TStringBuf VALID_SERVICE_TOKEN = "good-service-token";
    static constexpr TStringBuf BAD_TOKEN = "bad-token";
    static constexpr TStringBuf USER_ACCOUNT_ID = "extended-user-account";
    static constexpr TStringBuf SERVICE_ACCOUNT_ID = "extended-service-account";

    static inline const TString BEARER_VALID_USER_TOKEN = TString("Bearer ") + VALID_USER_TOKEN;
    static inline const TString BEARER_VALID_SERVICE_TOKEN = TString("Bearer ") + VALID_SERVICE_TOKEN;

    THashSet<TString> AllowedTokens = { BEARER_VALID_USER_TOKEN, BEARER_VALID_SERVICE_TOKEN };
    TString UserAccountId = TString(USER_ACCOUNT_ID);

    grpc::Status Get(
        grpc::ServerContext* context,
        const nebius::iam::v1::GetProfileRequest*,
        nebius::iam::v1::GetProfileResponse* response) override
    {
        TString token;
        auto authHeader = context->client_metadata().find("authorization");
        if (authHeader != context->client_metadata().end()) {
            token = TString(authHeader->second.data(), authHeader->second.length());
        }

        if (!AllowedTokens.contains(token)) {
            Cerr << "TProfileServiceMock Get: Invalid or missing token: " << token << Endl;
            return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Invalid or missing token");
        }

        if (token == BEARER_VALID_USER_TOKEN) {
            auto* profile = response->mutable_user_profile();
            profile->set_id(UserAccountId);

            auto* fedInfo = profile->mutable_federation_info();
            fedInfo->set_federation_user_account_id("john.smith@testcorp.example");
            fedInfo->set_federation_id("federation-testcorp");

            auto attrs = profile->mutable_attributes();
            attrs->set_given_name("John");
            attrs->set_family_name("Smith");
            attrs->set_email("john.smith@test.corp");
            attrs->set_name("john.smith@test.corp");
            attrs->set_preferred_username("John Smith");
            attrs->set_picture("1234");
            attrs->set_locale("en-US");
            attrs->set_phone_number("+1234567890");
            attrs->set_sub("john.smith@test.corp");

            auto* tenant = profile->add_tenants();
            tenant->set_tenant_id("tenant-testcorp-1");
            tenant->set_tenant_user_account_id("tenantuseraccount-alpha001");
        } else if (token == BEARER_VALID_SERVICE_TOKEN) {
            auto* profile = response->mutable_service_account_profile();
            profile->mutable_info()->mutable_metadata()->set_id(SERVICE_ACCOUNT_ID);
            profile->mutable_info()->mutable_metadata()->set_parent_id("project-e0tydb");
            profile->mutable_info()->mutable_metadata()->set_name("ydb");
            profile->mutable_info()->mutable_spec()->set_description("ydb");
            profile->mutable_info()->mutable_status()->set_active(true);
        }

        return grpc::Status::OK;
    }
};

std::unique_ptr<grpc::Server> CreateProfileServiceMock(grpc::Service* service, const TString& endpoint) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(endpoint, grpc::InsecureServerCredentials());
    builder.RegisterService(service);
    return builder.BuildAndStart();
}
