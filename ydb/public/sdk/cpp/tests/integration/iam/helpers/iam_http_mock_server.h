#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/types.h>

#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/testing/common/network.h>

#include <util/datetime/base.h>

#include <mutex>
#include <string>

namespace NYdb::NTest {

class TMetadataServer : public THttpServer::ICallBack {
public:
    class TRequest : public TRequestReplier {
    public:
        explicit TRequest(TMetadataServer* server)
            : Server(server)
        {}

        bool DoReply(const TReplyParams& params) override;

        TMetadataServer* Server = nullptr;
    };

    TMetadataServer();
    ~TMetadataServer();

    TClientRequest* CreateClient() override;

    void SetResponse(HttpCodes code, const std::string& response);

    int GetRequestCount() const;
    void ResetRequestCount();

    NTesting::TPortHolder PortHolder;
    uint16_t Port;
    THttpServer::TOptions HttpOptions;
    THttpServer HttpServer;
    HttpCodes StatusCode = HTTP_OK;
    std::string Response;
    mutable std::mutex Lock;
    int RequestCount = 0;
};

std::string MakeTokenResponse(const std::string& token, int expiresIn);
std::string MakeTokenResponseWithExpiry(const std::string& token, const TInstant& expiry);
std::string MakeTokenResponseNoExpiry(const std::string& token);

TIamHost MakeMetadataParams(uint16_t port);

} // namespace NYdb::NTest
