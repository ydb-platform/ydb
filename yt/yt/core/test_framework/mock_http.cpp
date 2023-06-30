#include "mock_http.h"

#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/server.h>

#include <util/string/join.h>

namespace NYT::NHttp {
namespace {

TServerConfigPtr MakeConfig()
{
    auto config = New<TServerConfig>();
    config->Port = 0;
    return config;
}

TMockHeaders DumpHeadersSafe(const THeadersPtr& headers)
{
    return headers ? headers->Dump() : TMockHeaders();
}

class THandler
    : public IHttpHandler
{
public:
    THandler(TMockServer* mockServer)
        : MockServer_(mockServer)
    { }

    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        TString path = Join('?', req->GetUrl().Path, req->GetUrl().RawQuery);
        TString body = ToString(req->ReadAll());
        TMockHeaders headers = req->GetHeaders()->Dump();

        TMockResponse mockRsp;
        switch (req->GetMethod()) {
            case EMethod::Delete:
                mockRsp = MockServer_->Delete(path, headers);
                break;
            case EMethod::Get:
                mockRsp = MockServer_->Get(path, headers);
                break;
            case EMethod::Head:
                mockRsp = MockServer_->Head(path, headers);
                break;
            case EMethod::Post:
                mockRsp = MockServer_->Post(path, body, headers);
                break;
            case EMethod::Put:
                mockRsp = MockServer_->Put(path, body, headers);
                break;
            default:
                THROW_ERROR_EXCEPTION("Unsupported");
        }
        rsp->SetStatus(mockRsp.StatusCode);
        for (const auto& [name, value] : mockRsp.Headers) {
            rsp->GetHeaders()->Add(name, value);
        }
        NConcurrency::WaitFor(rsp->WriteBody(TSharedRef::FromString(mockRsp.Body))).ThrowOnError();
    }

private:
    TMockServer* MockServer_;
}; // class THandler

class TMockResponseStream
    : public IResponse
{
public:
    TMockResponseStream(const TMockResponse& response)
        : Response_(response)
        , Headers_(New<THeaders>())
        , Trailers_(New<THeaders>())
    { }

    EStatusCode GetStatusCode() override
    {
        return Response_.StatusCode;
    }

    const THeadersPtr& GetHeaders() override
    {
        for (const auto& [name, value] : Response_.Headers) {
            Headers_->Add(name, value);
        }
        return Headers_;
    }

    const THeadersPtr& GetTrailers() override
    {
        return Trailers_;
    }

    TFuture<TSharedRef> Read() override
    {
        TSharedRef result;
        if (!Response_.Body.empty()) {
            result = TSharedRef::FromString(Response_.Body);
            Response_.Body.clear();
        }
        return MakeFuture(result);
    }

private:
    TMockResponse Response_;
    THeadersPtr Headers_;
    THeadersPtr Trailers_;
}; // class TMockResponseStream

}

TMockServer::TMockServer(TServerConfigPtr config)
    : Server_(CreateServer(config ? std::move(config) : MakeConfig()))
    , Handler_(New<THandler>(this))
{
    Server_->AddHandler("/", Handler_);
    Server_->Start();
}

TMockServer::~TMockServer()
{
    Server_->Stop();
}

ui16 TMockServer::GetPort()
{
    return Server_->GetAddress().GetPort();
}

TFuture<IResponsePtr> TMockClient::Get(const TString& url, const THeadersPtr& headers)
{
    auto mockRsp = Get(url, DumpHeadersSafe(headers));
    return MakeFuture<IResponsePtr>(New<TMockResponseStream>(mockRsp));
}

TFuture<IResponsePtr> TMockClient::Post(
    const TString& url, const TSharedRef& body, const THeadersPtr& headers)
{
    auto mockRsp = Post(url, ToString(body), DumpHeadersSafe(headers));
    return MakeFuture<IResponsePtr>(New<TMockResponseStream>(mockRsp));
}

TFuture<IResponsePtr> TMockClient::Patch(
    const TString& url, const TSharedRef& body, const THeadersPtr& headers)
{
    auto mockRsp = Patch(url, ToString(body), DumpHeadersSafe(headers));
    return MakeFuture<IResponsePtr>(New<TMockResponseStream>(mockRsp));
}

TFuture<IResponsePtr> TMockClient::Put(
    const TString& url, const TSharedRef& body, const THeadersPtr& headers)
{
    auto mockRsp = Put(url, ToString(body), DumpHeadersSafe(headers));
    return MakeFuture<IResponsePtr>(New<TMockResponseStream>(mockRsp));
}

TFuture<IResponsePtr> TMockClient::Delete(const TString& url, const THeadersPtr& headers)
{
    auto mockRsp = Delete(url, DumpHeadersSafe(headers));
    return MakeFuture<IResponsePtr>(New<TMockResponseStream>(mockRsp));
}

TFuture<IActiveRequestPtr> TMockClient::StartPost(
    const TString& /*url*/, const THeadersPtr& /*headers*/)
{
    YT_UNIMPLEMENTED();
}

TFuture<IActiveRequestPtr> TMockClient::StartPatch(
    const TString& /*url*/, const THeadersPtr& /*headers*/)
{
    YT_UNIMPLEMENTED();
}

TFuture<IActiveRequestPtr> TMockClient::StartPut(
    const TString& /*url*/, const THeadersPtr& /*headers*/)
{
    YT_UNIMPLEMENTED();
}

} // namespace NYT::NHttp
