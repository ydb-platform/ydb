#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/types.h>

#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/testing/common/network.h>

#include <util/datetime/base.h>

#include <mutex>
#include <string>

namespace NYdb::NTest {

inline constexpr const char* kMetadataTokenPath =
    "/computeMetadata/v1/instance/service-accounts/default/token";
inline constexpr const char* kMetadataFlavorHeader = "Metadata-Flavor";
inline constexpr const char* kMetadataFlavorValue = "Google";

struct TMetadataRequestInfo {
    std::string Method;
    std::string Path;
    std::string MetadataFlavor;
    bool IsValid = false;
};

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
    void SetStrictMode(bool strict);

    int GetRequestCount() const;
    TMetadataRequestInfo GetLastRequest() const;
    bool HasLastRequest() const;

    NTesting::TPortHolder PortHolder;
    uint16_t Port;
    THttpServer::TOptions HttpOptions;
    THttpServer HttpServer;

private:
    mutable std::mutex Lock_;
    HttpCodes StatusCode_ = HTTP_OK;
    std::string Response_;
    bool StrictMode_ = true;
    int RequestCount_ = 0;
    TMetadataRequestInfo LastRequest_;
    bool HasLastRequest_ = false;
};

std::string MakeTokenResponse(const std::string& token, int expiresIn);
std::string MakeTokenResponseWithExpiry(const std::string& token, const TInstant& expiry);
std::string MakeTokenResponseNoExpiry(const std::string& token);

TIamHost MakeMetadataParams(uint16_t port);

} // namespace NYdb::NTest
