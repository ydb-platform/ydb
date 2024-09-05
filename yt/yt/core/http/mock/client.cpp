#include "client.h"

#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/server.h>

#include <util/string/join.h>

namespace NYT::NHttp {
namespace {

////////////////////////////////////////////////////////////////////////////////

TMockHeaders DumpHeadersSafe(const THeadersPtr& headers)
{
    return headers ? headers->Dump() : TMockHeaders();
}

class TMockResponseStream
    : public IResponse
{
public:
    TMockResponseStream(const TMockResponse& response)
        : StatusCode_(response.StatusCode)
        , Body_(response.Body)
        , Headers_(New<THeaders>())
        , Trailers_(New<THeaders>())
    {
        for (const auto& [name, value] : response.Headers) {
            Headers_->Add(name, value);
        }
    }

    EStatusCode GetStatusCode() override
    {
        return StatusCode_;
    }

    const THeadersPtr& GetHeaders() override
    {
        return Headers_;
    }

    const THeadersPtr& GetTrailers() override
    {
        return Trailers_;
    }

    TFuture<TSharedRef> Read() override
    {
        TSharedRef result;
        if (!Body_.empty()) {
            result = TSharedRef::FromString(std::exchange(Body_, TString()));
        }
        return MakeFuture(result);
    }

private:
    EStatusCode StatusCode_;
    TString Body_;
    THeadersPtr Headers_;
    THeadersPtr Trailers_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

TFuture<IResponsePtr> TMockClient::Get(const TString& url, const THeadersPtr& headers)
{
    auto mockResponse = Get(url, DumpHeadersSafe(headers));
    return MakeFuture<IResponsePtr>(New<TMockResponseStream>(mockResponse));
}

TFuture<IResponsePtr> TMockClient::Post(const TString& url, const TSharedRef& body, const THeadersPtr& headers)
{
    auto mockResponse = Post(url, ToString(body), DumpHeadersSafe(headers));
    return MakeFuture<IResponsePtr>(New<TMockResponseStream>(mockResponse));
}

TFuture<IResponsePtr> TMockClient::Patch(const TString& url, const TSharedRef& body, const THeadersPtr& headers)
{
    auto mockResponse = Patch(url, ToString(body), DumpHeadersSafe(headers));
    return MakeFuture<IResponsePtr>(New<TMockResponseStream>(mockResponse));
}

TFuture<IResponsePtr> TMockClient::Put(const TString& url, const TSharedRef& body, const THeadersPtr& headers)
{
    auto mockResponse = Put(url, ToString(body), DumpHeadersSafe(headers));
    return MakeFuture<IResponsePtr>(New<TMockResponseStream>(mockResponse));
}

TFuture<IResponsePtr> TMockClient::Delete(const TString& url, const THeadersPtr& headers)
{
    auto mockRsp = Delete(url, DumpHeadersSafe(headers));
    return MakeFuture<IResponsePtr>(New<TMockResponseStream>(mockRsp));
}

TFuture<IActiveRequestPtr> TMockClient::StartPost(const TString& /*url*/, const THeadersPtr& /*headers*/)
{
    YT_UNIMPLEMENTED();
}

TFuture<IActiveRequestPtr> TMockClient::StartPatch(const TString& /*url*/, const THeadersPtr& /*headers*/)
{
    YT_UNIMPLEMENTED();
}

TFuture<IActiveRequestPtr> TMockClient::StartPut(const TString& /*url*/, const THeadersPtr& /*headers*/)
{
    YT_UNIMPLEMENTED();
}

TFuture<IResponsePtr> TMockClient::Request(EMethod method, const TString& url, const std::optional<TSharedRef>& body, const THeadersPtr& headers)
{
    auto mockResponse = Request(method, url, ToString(body), DumpHeadersSafe(headers));
    return MakeFuture<IResponsePtr>(New<TMockResponseStream>(mockResponse));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
