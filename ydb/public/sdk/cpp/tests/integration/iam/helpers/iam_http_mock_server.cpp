#include "iam_http_mock_server.h"

#include <library/cpp/json/json_writer.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/types.h>

namespace NYdb::NTest {

bool TMetadataServer::TRequest::DoReply(const TReplyParams& params) {
    {
        std::lock_guard lock(Server->Lock);
        ++Server->RequestCount;
    }

    THttpResponse resp(Server->StatusCode);
    resp.SetContent(TString{Server->Response});
    resp.OutTo(params.Output);
    return true;
}

TMetadataServer::TMetadataServer()
    : PortHolder(NTesting::GetFreePort())
    , Port(static_cast<std::uint16_t>(PortHolder))
    , HttpOptions(Port)
    , HttpServer(this, HttpOptions)
{
    HttpServer.Start();
}

TMetadataServer::~TMetadataServer() {
    HttpServer.Stop();
}

TClientRequest* TMetadataServer::CreateClient() {
    return new TRequest(this);
}

void TMetadataServer::SetResponse(HttpCodes code, const std::string& response) {
    StatusCode = code;
    Response = response;
}

int TMetadataServer::GetRequestCount() const {
    std::lock_guard lock(Lock);
    return RequestCount;
}

void TMetadataServer::ResetRequestCount() {
    std::lock_guard lock(Lock);
    RequestCount = 0;
}

std::string MakeTokenResponse(const std::string& token, int expiresIn) {
    TStringStream ss;
    NJson::TJsonWriter w(&ss, false);
    w.OpenMap();
    w.WriteKey("access_token");
    w.Write(token);
    w.WriteKey("token_type");
    w.Write("Bearer");
    w.WriteKey("expires_in");
    w.Write(expiresIn);
    w.CloseMap();
    w.Flush();
    return ss.Str();
}

std::string MakeTokenResponseWithExpiry(const std::string& token, const TInstant& expiry) {
    TStringStream ss;
    NJson::TJsonWriter w(&ss, false);
    w.OpenMap();
    w.WriteKey("access_token");
    w.Write(token);
    w.WriteKey("token_type");
    w.Write("Bearer");
    w.WriteKey("expiry");
    w.Write(expiry.FormatGmTime("%Y-%m-%dT%H:%M:%S.000000000Z"));
    w.CloseMap();
    w.Flush();
    return ss.Str();
}

std::string MakeTokenResponseNoExpiry(const std::string& token) {
    TStringStream ss;
    NJson::TJsonWriter w(&ss, false);
    w.OpenMap();
    w.WriteKey("access_token");
    w.Write(token);
    w.WriteKey("token_type");
    w.Write("Bearer");
    w.CloseMap();
    w.Flush();
    return ss.Str();
}

TIamHost MakeMetadataParams(uint16_t port) {
    TIamHost params;
    params.Host = "localhost";
    params.Port = port;
    params.RefreshPeriod = TDuration::Hours(1);
    return params;
}

} // namespace NYdb::NTest
