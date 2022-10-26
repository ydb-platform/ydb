#pragma once

#include <ydb/public/api/client/yc_private/iam/user_account_service.grpc.pb.h>

class TUserAccountServiceMock : public yandex::cloud::priv::iam::v1::UserAccountService::Service {
public:
    THashMap<TString, yandex::cloud::priv::iam::v1::UserAccount> UserAccountData;

    virtual grpc::Status Get(grpc::ServerContext*,
                             const yandex::cloud::priv::iam::v1::GetUserAccountRequest* request,
                             yandex::cloud::priv::iam::v1::UserAccount* response) override {
        TString id = request->user_account_id();
        auto it = UserAccountData.find(id);
        if (it != UserAccountData.end()) {
            response->CopyFrom(it->second);
            return grpc::Status::OK;
        } else {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Not Found");
        }
    }
};

