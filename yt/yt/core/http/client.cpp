#include "client.h"
#include "connection_pool.h"
#include "connection_reuse_helpers.h"
#include "http.h"
#include "config.h"
#include "stream.h"
#include "private.h"
#include "helpers.h"

#include <yt/yt/core/net/dialer.h>
#include <yt/yt/core/net/config.h>
#include <yt/yt/core/net/connection.h>

#include <yt/yt/core/concurrency/poller.h>

#include <util/string/cast.h>

namespace NYT::NHttp {

using namespace NConcurrency;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
{
public:
    TClient(
        TClientConfigPtr config,
        IDialerPtr dialer,
        IInvokerPtr invoker)
        : Config_(std::move(config))
        , Dialer_(std::move(dialer))
        , Invoker_(std::move(invoker))
        , ConnectionPool_(New<TConnectionPool>(Dialer_, Config_, Invoker_))
    { }

    TFuture<IResponsePtr> Get(
        const TString& url,
        const THeadersPtr& headers) override
    {
        return Request(EMethod::Get, url, std::nullopt, headers);
    }

    TFuture<IResponsePtr> Post(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers) override
    {
        return Request(EMethod::Post, url, TSharedRef{body}, headers);
    }

    TFuture<IResponsePtr> Patch(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers) override
    {
        return Request(EMethod::Patch, url, TSharedRef{body}, headers);
    }

    TFuture<IResponsePtr> Put(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers) override
    {
        return Request(EMethod::Put, url, TSharedRef{body}, headers);
    }

    TFuture<IResponsePtr> Delete(
        const TString& url,
        const THeadersPtr& headers) override
    {
        return Request(EMethod::Delete, url, std::nullopt, headers);
    }

    TFuture<IActiveRequestPtr> StartPost(
        const TString& url,
        const THeadersPtr& headers) override
    {
        return StartRequest(EMethod::Post, url, headers);
    }

    TFuture<IActiveRequestPtr> StartPatch(
        const TString& url,
        const THeadersPtr& headers) override
    {
        return StartRequest(EMethod::Patch, url, headers);
    }

    TFuture<IActiveRequestPtr> StartPut(
        const TString& url,
        const THeadersPtr& headers) override
    {
        return StartRequest(EMethod::Put, url, headers);
    }

    TFuture<IResponsePtr> Request(
        EMethod method,
        const TString& url,
        const std::optional<TSharedRef>& body,
        const THeadersPtr& headers) override
    {
        return WrapError(url, BIND([=, this, this_ = MakeStrong(this)] {
            return DoRequest(method, url, body, headers);
        }));
    }

private:
    const TClientConfigPtr Config_;
    const IDialerPtr Dialer_;
    const IInvokerPtr Invoker_;
    const TConnectionPoolPtr ConnectionPool_;

    static int GetDefaultPort(const TUrlRef& parsedUrl)
    {
        if (parsedUrl.Protocol == "https") {
            return 443;
        } else {
            return 80;
        }
    }

    TNetworkAddress GetAddress(const TUrlRef& parsedUrl)
    {
        auto host = parsedUrl.Host;
        TNetworkAddress address;

        if (auto ipOrError = TNetworkAddress::TryParse(host); ipOrError.IsOK()) {
            address = ipOrError.Value();
        } else {
            auto asyncAddress = TAddressResolver::Get()->Resolve(ToString(host));
            address = WaitFor(asyncAddress)
                .ValueOrThrow();
        }

        return TNetworkAddress(address, parsedUrl.Port.value_or(GetDefaultPort(parsedUrl)));
    }

    std::pair<THttpOutputPtr, THttpInputPtr> Connect(const TUrlRef& urlRef)
    {
        auto context = New<TDialerContext>();
        context->Host = urlRef.Host;

        auto address = GetAddress(urlRef);

        // TODO(aleexfi): Enable connection pool by default
        if (Config_->MaxIdleConnections == 0) {
            auto connection = WaitFor(Dialer_->Dial(address, std::move(context)))
                .ValueOrThrow();

            auto input = New<THttpInput>(
                connection,
                address,
                Invoker_,
                EMessageType::Response,
                Config_);

            auto output = New<THttpOutput>(
                connection,
                EMessageType::Request,
                Config_);

            return {std::move(output), std::move(input)};
        } else {
            auto connection = WaitFor(ConnectionPool_->Connect(address, std::move(context)))
                .ValueOrThrow();

            auto reusableState = New<NDetail::TReusableConnectionState>(connection, ConnectionPool_);

            auto input = New<NDetail::TConnectionReuseWrapper<THttpInput>>(
                connection,
                address,
                Invoker_,
                EMessageType::Response,
                Config_);
            input->SetReusableState(reusableState);

            auto output = New<NDetail::TConnectionReuseWrapper<THttpOutput>>(
                connection,
                EMessageType::Request,
                Config_);
            output->SetReusableState(reusableState);

            return {std::move(output), std::move(input)};
        }
    }

    template <typename T>
    TFuture<T> WrapError(const TString& url, TCallback<T()> action)
    {
        return BIND([=, this_ = MakeStrong(this), action = std::move(action)] {
            try {
                return action();
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("HTTP request failed")
                    << TErrorAttribute("url", SanitizeUrl(url))
                    << ex;
            }
        })
            .AsyncVia(Invoker_)
            .Run();
    }

    class TActiveRequest
        : public IActiveRequest
    {
    public:
        TActiveRequest(
            THttpOutputPtr request,
            THttpInputPtr response,
            TIntrusivePtr<TClient> client,
            TString url)
            : Request_(std::move(request))
            , Response_(std::move(response))
            , Client_(std::move(client))
            , Url_(std::move(url))
        { }

        TFuture<IResponsePtr> Finish() override
        {
            return Client_->WrapError(Url_, BIND([this, this_ = MakeStrong(this)] {
                WaitFor(Request_->Close())
                    .ThrowOnError();

                // Waits for response headers internally.
                Response_->GetStatusCode();

                return IResponsePtr(Response_);
            }));
        }

        NConcurrency::IAsyncOutputStreamPtr GetRequestStream() override
        {
            return Request_;
        }

        IResponsePtr GetResponse() override
        {
            return Response_;
        }

    private:
        const THttpOutputPtr Request_;
        const THttpInputPtr Response_;
        const TIntrusivePtr<TClient> Client_;
        const TString Url_;
    };

    std::pair<THttpOutputPtr, THttpInputPtr> StartAndWriteHeaders(
        EMethod method,
        const TString& url,
        const THeadersPtr& headers)
    {
        THttpOutputPtr request;
        THttpInputPtr response;

        auto urlRef = ParseUrl(url);

        std::tie(request, response) = Connect(urlRef);

        request->SetHost(urlRef.Host, urlRef.PortStr);
        if (headers) {
            request->SetHeaders(headers);
        }

        auto requestPath = (urlRef.RawQuery.empty() && Config_->OmitQuestionMarkForEmptyQuery)
            ? TString(urlRef.Path)
            : Format("%v?%v", urlRef.Path, urlRef.RawQuery);
        request->WriteRequest(method, requestPath);

        return {std::move(request), std::move(response)};
    }

    TFuture<IActiveRequestPtr> StartRequest(
        EMethod method,
        const TString& url,
        const THeadersPtr& headers)
    {
        return WrapError(url, BIND([=, this, this_ = MakeStrong(this)] {
            auto [request, response] = StartAndWriteHeaders(method, url, headers);
            return IActiveRequestPtr(New<TActiveRequest>(request, response, this_, url));
        }));
    }

    IResponsePtr DoRequest(
        EMethod method,
        const TString& url,
        const std::optional<TSharedRef>& body,
        const THeadersPtr& headers,
        int redirectCount = 0)
    {
        auto [request, response] = StartAndWriteHeaders(method, url, headers);

        if (body) {
            WaitFor(request->WriteBody(*body))
                .ThrowOnError();
        } else {
            WaitFor(request->Close())
                .ThrowOnError();
        }

        if (Config_->IgnoreContinueResponses) {
            while (response->GetStatusCode() == EStatusCode::Continue) {
                response->Reset();
            }
        }

        // Waits for response headers internally.
        auto redirectUrl = response->TryGetRedirectUrl();
        if (redirectUrl && redirectCount < Config_->MaxRedirectCount) {
            return DoRequest(method, *redirectUrl, body, headers, redirectCount + 1);
        }

        return response;
    }
};

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    TClientConfigPtr config,
    IDialerPtr dialer,
    IInvokerPtr invoker)
{
    return New<TClient>(
        std::move(config),
        std::move(dialer),
        std::move(invoker));
}

IClientPtr CreateClient(
    TClientConfigPtr config,
    IPollerPtr poller)
{
    auto invoker = poller->GetInvoker();
    return CreateClient(
        std::move(config),
        CreateDialer(New<TDialerConfig>(), std::move(poller), HttpLogger()),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
