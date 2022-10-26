#pragma once

#include <ydb/public/api/client/yc_private/iam/service_account_service.grpc.pb.h>

class TServiceAccountServiceMock : public yandex::cloud::priv::iam::v1::ServiceAccountService::Service {
public:
    THashMap<TString, yandex::cloud::priv::iam::v1::ServiceAccount> ServiceAccountData;
    THashMap<TString, yandex::cloud::priv::iam::v1::IamToken> IamTokens;
    TString Identity;

    TMaybe<grpc::Status> CheckAuthorization(grpc::ServerContext* context) {
        if (!Identity.empty()) {
            auto[reqIdBegin, reqIdEnd] = context->client_metadata().equal_range("authorization");
            if (reqIdBegin == reqIdEnd) {
                return grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
                                    TStringBuilder() << "Authorization data is not provided");
	    } else if (Identity != TStringBuf(reqIdBegin->second.cbegin(), reqIdBegin->second.cend())) {
                return grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
                                    TStringBuilder() << "Access for user " << Identity << " is forbidden");
            }
        }

        return Nothing();
    }

    virtual grpc::Status Get(grpc::ServerContext* context,
                             const yandex::cloud::priv::iam::v1::GetServiceAccountRequest* request,
                             yandex::cloud::priv::iam::v1::ServiceAccount* response) override
    {
        auto status = CheckAuthorization(context);
        if (status.Defined()) {
            return *status;
        }

        TString id = request->service_account_id();
        auto it = ServiceAccountData.find(id);
        if (it != ServiceAccountData.end()) {
            response->CopyFrom(it->second);
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Not Found");
        }
    }

    virtual grpc::Status IssueToken(grpc::ServerContext* context,
                             const yandex::cloud::priv::iam::v1::IssueTokenRequest* request,
                             yandex::cloud::priv::iam::v1::IamToken* response) override
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

};

