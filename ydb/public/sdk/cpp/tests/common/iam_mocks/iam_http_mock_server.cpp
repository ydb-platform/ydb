#include "iam_http_mock_server.h"

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/json/json_writer.h>

namespace NYdb::NTest {

namespace {

bool IsValidMetadataRequest(const TStringBuf& method, const TStringBuf& path,
                            const THttpHeaders& headers)
{
    if (method != "GET") {
        return false;
    }
    if (path != kMetadataTokenPath) {
        return false;
    }
    const THttpInputHeader* flavorHeader = headers.FindHeader(kMetadataFlavorHeader);
    return flavorHeader && flavorHeader->Value() == kMetadataFlavorValue;
}

} // namespace

bool TMetadataServer::TRequest::DoReply(const TReplyParams& params) {
    const TParsedHttpFull parsed(params.Input.FirstLine());
    const auto& headers = params.Input.Headers();

    TMetadataRequestInfo requestInfo;
    requestInfo.Method = TString{parsed.Method};
    requestInfo.Path = TString{parsed.Path};
    if (const THttpInputHeader* flavorHeader = headers.FindHeader(kMetadataFlavorHeader)) {
        requestInfo.MetadataFlavor = flavorHeader->Value();
    }
    requestInfo.IsValid = IsValidMetadataRequest(parsed.Method, parsed.Path, headers);

    HttpCodes statusCode;
    std::string responseBody;
    bool strictMode;
    {
        std::lock_guard lock(Server->Lock_);
        ++Server->RequestCount_;
        Server->LastRequest_ = std::move(requestInfo);
        Server->HasLastRequest_ = true;
        strictMode = Server->StrictMode_;
        if (!Server->LastRequest_.IsValid && strictMode) {
            statusCode = HTTP_NOT_FOUND;
            responseBody = "";
        } else {
            statusCode = Server->StatusCode_;
            responseBody = Server->Response_;
        }
    }

    THttpResponse resp(statusCode);
    resp.SetContent(TString{responseBody});
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
    std::lock_guard lock(Lock_);
    StatusCode_ = code;
    Response_ = response;
}

void TMetadataServer::SetStrictMode(bool strict) {
    std::lock_guard lock(Lock_);
    StrictMode_ = strict;
}

int TMetadataServer::GetRequestCount() const {
    std::lock_guard lock(Lock_);
    return RequestCount_;
}

TMetadataRequestInfo TMetadataServer::GetLastRequest() const {
    std::lock_guard lock(Lock_);
    return LastRequest_;
}

bool TMetadataServer::HasLastRequest() const {
    std::lock_guard lock(Lock_);
    return HasLastRequest_;
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
