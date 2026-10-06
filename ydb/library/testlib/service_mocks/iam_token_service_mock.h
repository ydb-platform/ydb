#pragma once

#include <ydb/public/api/client/yc_private/iam/iam_token_service.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>

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
    THashMap<TString, TString> ServiceTokens;

    static TString ServiceTokenKey(const TString& resourceId, const TString& targetServiceAccountId) {
        return resourceId + "/" + targetServiceAccountId;
    }

    virtual grpc::Status CreateForService(grpc::ServerContext* context,
                             const yandex::cloud::priv::iam::v1::CreateIamTokenForServiceRequest* request,
                             yandex::cloud::priv::iam::v1::CreateIamTokenResponse* response) override
    {
        auto status = CheckAuthorization(context);
        if (status.Defined()) {
            return *status;
        }

        auto it = ServiceTokens.find(ServiceTokenKey(request->resource_id(), request->target_service_account_id()));
        if (it == ServiceTokens.end()) {
            return grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "delegation not found");
        }
        response->set_iam_token(it->second);
        return grpc::Status::OK;
    }

};

