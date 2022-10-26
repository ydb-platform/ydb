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

    virtual grpc::Status Authenticate(
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

    virtual grpc::Status Authorize(
            grpc::ServerContext* ctx,
            const yandex::cloud::priv::servicecontrol::v1::AuthorizeRequest* request,
            yandex::cloud::priv::servicecontrol::v1::AuthorizeResponse* response) override
    {
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
