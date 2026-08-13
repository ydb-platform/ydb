#pragma once

#include <ydb/library/testlib/service_mocks/common.h>
#include <ydb/public/api/client/yc_private/iam/user_account_service.grpc.pb.h>

#include <util/system/mutex.h>

class TUserAccountServiceMock : public yandex::cloud::priv::iam::v1::UserAccountService::Service {
public:
    THashMap<TString, yandex::cloud::priv::iam::v1::UserAccount> UserAccountData;
    TMutex MetadataMutex;
    TString CapturedUserAgent;

    virtual grpc::Status Get(grpc::ServerContext* context,
                             const yandex::cloud::priv::iam::v1::GetUserAccountRequest* request,
                             yandex::cloud::priv::iam::v1::UserAccount* response) override {
        with_lock (MetadataMutex) {
            CapturedUserAgent = NTestUtils::CaptureUserAgent(context);
        }

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
