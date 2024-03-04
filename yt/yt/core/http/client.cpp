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
        const TClientConfigPtr& config,
        const IDialerPtr& dialer,
        const IInvokerPtr& invoker)
        : Config_(config)
        , Dialer_(dialer)
        , Invoker_(invoker)
        , ConnectionPool_(New<TConnectionPool>(dialer, config, invoker))
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

private:
    const TClientConfigPtr Config_;
    const IDialerPtr Dialer_;
    const IInvokerPtr Invoker_;
    TConnectionPoolPtr ConnectionPool_;

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

        auto tryIP = TNetworkAddress::TryParse(host);
        if (tryIP.IsOK()) {
            address = tryIP.Value();
        } else {
            auto asyncAddress = TAddressResolver::Get()->Resolve(ToString(host));
            address = WaitFor(asyncAddress)
                .ValueOrThrow();
        }

        return TNetworkAddress(address, parsedUrl.Port.value_or(GetDefaultPort(parsedUrl)));
    }

    std::pair<THttpOutputPtr, THttpInputPtr> OpenHttp(const TUrlRef& urlRef)
    {
        auto context = New<TRemoteContext>();
        context->Host = urlRef.Host;
        auto address = GetAddress(urlRef);

        // TODO(aleexfi): Enable connection pool by default
        if (Config_->MaxIdleConnections == 0) {
            auto connection = WaitFor(Dialer_->Dial(address, std::move(context))).ValueOrThrow();

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
            auto connection = WaitFor(ConnectionPool_->Connect(address, std::move(context))).ValueOrThrow();

            auto reuseSharedState = New<NDetail::TReusableConnectionState>(connection, ConnectionPool_);

            auto input = New<NDetail::TConnectionReuseWrapper<THttpInput>>(
                connection,
                address,
                Invoker_,
                EMessageType::Response,
                Config_);
            input->SetReusableState(reuseSharedState);

            auto output = New<NDetail::TConnectionReuseWrapper<THttpOutput>>(
                connection,
                EMessageType::Request,
                Config_);
            output->SetReusableState(reuseSharedState);

            return {std::move(output), std::move(input)};
        }
    }

    template <typename T>
    TFuture<T> WrapError(const TString& url, TCallback<T()> action)
    {
        return BIND([=, this_ = MakeStrong(this)] {
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
            TString url
        )
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

                return IResponsePtr{Response_};
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
        THttpOutputPtr Request_;
        THttpInputPtr Response_;
        TIntrusivePtr<TClient> Client_;
        TString Url_;
    };

    std::pair<THttpOutputPtr, THttpInputPtr> StartAndWriteHeaders(
        EMethod method,
        const TString& url,
        const THeadersPtr& headers)
    {
        THttpOutputPtr request;
        THttpInputPtr response;

        auto urlRef = ParseUrl(url);

        std::tie(request, response) = OpenHttp(urlRef);

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
            return IActiveRequestPtr{New<TActiveRequest>(request, response, this_, url)};
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

        // Waits for response headers internally.
        auto redirectUrl = response->TryGetRedirectUrl();
        if (redirectUrl && redirectCount < Config_->MaxRedirectCount) {
            return DoRequest(method, *redirectUrl, body, headers, redirectCount + 1);
        }

        return IResponsePtr(response);
    }

    TFuture<IResponsePtr> Request(
        EMethod method,
        const TString& url,
        const std::optional<TSharedRef>& body,
        const THeadersPtr& headers)
    {
        return WrapError(url, BIND([=, this, this_ = MakeStrong(this)] {
            return DoRequest(method, url, body, headers);
        }));
    }
};

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    const TClientConfigPtr& config,
    const IDialerPtr& dialer,
    const IInvokerPtr& invoker)
{
    return New<TClient>(config, dialer, invoker);
}

IClientPtr CreateClient(
    const TClientConfigPtr& config,
    const IPollerPtr& poller)
{
    return CreateClient(
        config,
        CreateDialer(New<TDialerConfig>(), poller, HttpLogger),
        poller->GetInvoker());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
