#include "http_client.h"

#include "abortable_http_response.h"
#include "core.h"
#include "helpers.h"
#include "http.h"

#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/http.h>

#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT::NHttpClient {

namespace {

TString CreateHost(TStringBuf host, TStringBuf port)
{
    if (!port.empty()) {
        return Format("%v:%v", host, port);
    }

    return TString(host);
}

TMaybe<TErrorResponse> GetErrorResponse(const TString& hostName, const TString& requestId, const NHttp::IResponsePtr& response)
{
    auto httpCode = response->GetStatusCode();
    if (httpCode == NHttp::EStatusCode::OK || httpCode == NHttp::EStatusCode::Accepted) {
        return {};
    }

    TErrorResponse errorResponse(static_cast<int>(httpCode), requestId);

    auto logAndSetError = [&] (const TString& rawError) {
        YT_LOG_ERROR("RSP %v - HTTP %v - %v",
            requestId,
            httpCode,
            rawError.data());
        errorResponse.SetRawError(rawError);
    };

    switch (httpCode) {
        case NHttp::EStatusCode::TooManyRequests:
            logAndSetError("request rate limit exceeded");
            break;

        case NHttp::EStatusCode::InternalServerError:
            logAndSetError("internal error in proxy " + hostName);
            break;

        default: {
            TStringStream httpHeaders;
            httpHeaders << "HTTP headers (";
            for (const auto& [headerName, headerValue] : response->GetHeaders()->Dump()) {
                httpHeaders << headerName << ": " << headerValue << "; ";
            }
            httpHeaders << ")";

            auto errorString = Sprintf("RSP %s - HTTP %d - %s",
                requestId.data(),
                static_cast<int>(httpCode),
                httpHeaders.Str().data());

            YT_LOG_ERROR("%v",
                errorString.data());

            if (auto errorHeader = response->GetHeaders()->Find("X-YT-Error")) {
                errorResponse.ParseFromJsonError(*errorHeader);
                if (errorResponse.IsOk()) {
                    return Nothing();
                }
                return errorResponse;
            }

            errorResponse.SetRawError(
                    errorString + " - X-YT-Error is missing in headers");
            break;
        }
    }

    return errorResponse;
}

void CheckErrorResponse(const TString& hostName, const TString& requestId, const NHttp::IResponsePtr& response)
{
    auto errorResponse = GetErrorResponse(hostName, requestId, response);
    if (errorResponse) {
        throw *errorResponse;
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDefaultHttpResponse
    : public IHttpResponse
{
public:
    TDefaultHttpResponse(std::unique_ptr<THttpRequest> request)
        : Request_(std::move(request))
    { }

    int GetStatusCode() override
    {
        return Request_->GetHttpCode();
    }

    IInputStream* GetResponseStream() override
    {
        return Request_->GetResponseStream();
    }

    TString GetResponse() override
    {
        return Request_->GetResponse();
    }

    TString GetRequestId() const override
    {
        return Request_->GetRequestId();
    }

private:
    std::unique_ptr<THttpRequest> Request_;
};

class TDefaultHttpRequest
    : public IHttpRequest
{
public:
    TDefaultHttpRequest(std::unique_ptr<THttpRequest> request, IOutputStream* stream)
        : Request_(std::move(request))
        , Stream_(stream)
    { }

    IOutputStream* GetStream() override
    {
        return Stream_;
    }

    IHttpResponsePtr Finish() override
    {
        Request_->FinishRequest();
        return std::make_unique<TDefaultHttpResponse>(std::move(Request_));
    }

private:
    std::unique_ptr<THttpRequest> Request_;
    IOutputStream* Stream_;
};

class TDefaultHttpClient
    : public IHttpClient
{
public:
    IHttpResponsePtr Request(const TString& url, const TString& requestId, const THttpConfig& config, const THttpHeader& header, TMaybe<TStringBuf> body) override
    {
        auto request = std::make_unique<THttpRequest>(requestId);

        auto urlRef = NHttp::ParseUrl(url);

        request->Connect(CreateHost(urlRef.Host, urlRef.PortStr), config.SocketTimeout);
        request->SmallRequest(header, body);
        return std::make_unique<TDefaultHttpResponse>(std::move(request));
    }

    IHttpRequestPtr StartRequest(const TString& url, const TString& requestId, const THttpConfig& config, const THttpHeader& header) override
    {
        auto request = std::make_unique<THttpRequest>(requestId);

        auto urlRef = NHttp::ParseUrl(url);

        request->Connect(CreateHost(urlRef.Host, urlRef.PortStr), config.SocketTimeout);
        auto stream = request->StartRequest(header);
        return std::make_unique<TDefaultHttpRequest>(std::move(request), stream);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCoreRequestContext
{
    TString HostName;
    TString Url;
    TString RequestId;
    bool LogResponse;
    TInstant StartTime;
    TString LoggedAttributes;
};

class TCoreHttpResponse
    : public IHttpResponse
{
public:
    TCoreHttpResponse(
        TCoreRequestContext context,
        NHttp::IResponsePtr response)
        : Context_(std::move(context))
        , Response_(std::move(response))
    { }

    int GetStatusCode() override
    {
        return static_cast<int>(Response_->GetStatusCode());
    }

    IInputStream* GetResponseStream() override
    {
        if (!Stream_) {
            auto stream = std::make_unique<TWrappedStream>(
                NConcurrency::CreateSyncAdapter(NConcurrency::CreateCopyingAdapter(Response_), NConcurrency::EWaitForStrategy::WaitFor),
                Response_,
                Context_.RequestId);
            CheckErrorResponse(Context_.HostName, Context_.RequestId, Response_);

            if (TConfig::Get()->UseAbortableResponse) {
                Y_ABORT_UNLESS(!Context_.Url.empty());
                Stream_ = std::make_unique<TAbortableCoreHttpResponse>(std::move(stream), Context_.Url);
            } else {
                Stream_ = std::move(stream);
            }
        }

        return Stream_.get();
    }

    TString GetResponse() override
    {
        auto result = GetResponseStream()->ReadAll();

        TStringStream loggedAttributes;
        loggedAttributes
            << "Time: " << TInstant::Now() - Context_.StartTime << "; "
            << "HostName: " << Context_.HostName << "; "
            << Context_.LoggedAttributes;

        if (Context_.LogResponse) {
            constexpr auto sizeLimit = 1 << 7;
            YT_LOG_DEBUG("RSP %v - received response (Response: '%v'; %v)",
                Context_.RequestId,
                TruncateForLogs(result, sizeLimit),
                loggedAttributes.Str());
        } else {
            YT_LOG_DEBUG("RSP %v - received response of %v bytes (%v)",
                Context_.RequestId,
                result.size(),
                loggedAttributes.Str());
        }
        return result;
    }

    TString GetRequestId() const override
    {
        return Context_.RequestId;
    }

private:
    class TWrappedStream
        : public IInputStream
    {
    public:
        TWrappedStream(std::unique_ptr<IInputStream> underlying, NHttp::IResponsePtr response, TString requestId)
            : Underlying_(std::move(underlying))
            , Response_(std::move(response))
            , RequestId_(std::move(requestId))
        { }

    protected:
        size_t DoRead(void* buf, size_t len) override
        {
            size_t read = Underlying_->Read(buf, len);

            if (read == 0 && len != 0) {
                CheckTrailers(Response_->GetTrailers());
            }
            return read;
        }

        size_t DoSkip(size_t len) override
        {
            size_t skipped = Underlying_->Skip(len);
            if (skipped == 0 && len != 0) {
                CheckTrailers(Response_->GetTrailers());
            }
            return skipped;
        }

    private:
        void CheckTrailers(const NHttp::THeadersPtr& trailers)
        {
            if (auto errorResponse = ParseError(trailers)) {
                errorResponse->SetIsFromTrailers(true);
                YT_LOG_ERROR("RSP %v - %v",
                    RequestId_,
                    errorResponse.GetRef().what());
                ythrow errorResponse.GetRef();
            }
        }

        TMaybe<TErrorResponse> ParseError(const NHttp::THeadersPtr& headers)
        {
            if (auto errorHeader = headers->Find("X-YT-Error")) {
                TErrorResponse errorResponse(static_cast<int>(Response_->GetStatusCode()), RequestId_);
                errorResponse.ParseFromJsonError(*errorHeader);
                if (errorResponse.IsOk()) {
                    return Nothing();
                }
                return errorResponse;
            }
            return Nothing();
        }

    private:
        std::unique_ptr<IInputStream> Underlying_;
        NHttp::IResponsePtr Response_;
        TString RequestId_;
    };

private:
    TCoreRequestContext Context_;
    NHttp::IResponsePtr Response_;
    std::unique_ptr<IInputStream> Stream_;
};

class TCoreHttpRequest
    : public IHttpRequest
{
public:
    TCoreHttpRequest(TCoreRequestContext context, NHttp::IActiveRequestPtr activeRequest)
        : Context_(std::move(context))
        , ActiveRequest_(std::move(activeRequest))
        , Stream_(NConcurrency::CreateBufferedSyncAdapter(ActiveRequest_->GetRequestStream()))
        , WrappedStream_(this, Stream_.get())
    { }

    IOutputStream* GetStream() override
    {
        return &WrappedStream_;
    }

    IHttpResponsePtr Finish() override
    {
        WrappedStream_.Flush();
        auto response = ActiveRequest_->Finish().Get().ValueOrThrow();
        return std::make_unique<TCoreHttpResponse>(std::move(Context_), std::move(response));
    }

    IHttpResponsePtr FinishWithError()
    {
        auto response = ActiveRequest_->GetResponse();
        return std::make_unique<TCoreHttpResponse>(std::move(Context_), std::move(response));
    }

private:
    class TWrappedStream
        : public IOutputStream
    {
    public:
        TWrappedStream(TCoreHttpRequest* httpRequest, IOutputStream* underlying)
            : HttpRequest_(httpRequest)
            , Underlying_(underlying)
        { }

    private:
        void DoWrite(const void* buf, size_t len) override
        {
            WrapWriteFunc([&] {
                Underlying_->Write(buf, len);
            });
        }

        void DoWriteV(const TPart* parts, size_t count) override
        {
            WrapWriteFunc([&] {
                Underlying_->Write(parts, count);
            });
        }

        void DoWriteC(char ch) override
        {
            WrapWriteFunc([&] {
                Underlying_->Write(ch);
            });
        }

        void DoFlush() override
        {
            WrapWriteFunc([&] {
                Underlying_->Flush();
            });
        }

        void DoFinish() override
        {
            WrapWriteFunc([&] {
                Underlying_->Finish();
            });
        }

        void WrapWriteFunc(std::function<void()> func)
        {
            CheckErrorState();
            try {
                func();
            } catch (const std::exception&) {
                HandleWriteException();
            }
        }

        // In many cases http proxy stops reading request and resets connection
        // if error has happend. This function tries to read error response
        // in such cases.
        void HandleWriteException() {
            Y_ABORT_UNLESS(WriteError_ == nullptr);
            WriteError_ = std::current_exception();
            Y_ABORT_UNLESS(WriteError_ != nullptr);
            try {
                HttpRequest_->FinishWithError()->GetResponseStream();
            } catch (const TErrorResponse &) {
                throw;
            } catch (...) {
            }
            std::rethrow_exception(WriteError_);
        }

        void CheckErrorState()
        {
            if (WriteError_) {
                std::rethrow_exception(WriteError_);
            }
        }

    private:
        TCoreHttpRequest* const HttpRequest_;
        IOutputStream* Underlying_;
        std::exception_ptr WriteError_;
    };

private:
    TCoreRequestContext Context_;
    NHttp::IActiveRequestPtr ActiveRequest_;
    std::unique_ptr<IOutputStream> Stream_;
    TWrappedStream WrappedStream_;
};

class TCoreHttpClient
    : public IHttpClient
{
public:
    TCoreHttpClient(bool useTLS, const TConfigPtr& config)
        : Poller_(NConcurrency::CreateThreadPoolPoller(1, "http_poller"))  // TODO(nadya73): YT-18363: move threads count to config
    {
        if (useTLS) {
            auto httpsConfig = NYT::New<NYT::NHttps::TClientConfig>();
            httpsConfig->MaxIdleConnections = config->ConnectionPoolSize;
            Client_ = NHttps::CreateClient(httpsConfig, Poller_);
        } else {
            auto httpConfig = NYT::New<NYT::NHttp::TClientConfig>();
            httpConfig->MaxIdleConnections = config->ConnectionPoolSize;
            Client_ = NHttp::CreateClient(httpConfig, Poller_);
        }
    }

    IHttpResponsePtr Request(const TString& url, const TString& requestId, const THttpConfig& /*config*/, const THttpHeader& header, TMaybe<TStringBuf> body) override
    {
        TCoreRequestContext context = CreateContext(url, requestId, header);

        // TODO(nadya73): YT-18363: pass socket timeouts from THttpConfig

        NHttp::IResponsePtr response;

        auto logRequest = [&](bool includeParameters) {
            LogRequest(header, url, includeParameters, requestId, context.HostName);
            context.LoggedAttributes = GetLoggedAttributes(header, url, includeParameters, 128);
        };

        if (!body && (header.GetMethod() == "PUT" || header.GetMethod() == "POST")) {
            const auto& parameters = header.GetParameters();
            auto parametersStr = NodeToYsonString(parameters);

            bool includeParameters = false;
            auto headers = header.GetHeader(context.HostName, requestId, includeParameters).Get();

            logRequest(includeParameters);

            auto activeRequest = StartRequestImpl(header.GetMethod(), url, headers);

            activeRequest->GetRequestStream()->Write(TSharedRef::FromString(parametersStr)).Get().ThrowOnError();
            response = activeRequest->Finish().Get().ValueOrThrow();
        } else {
            auto bodyRef = TSharedRef::FromString(TString(body ? *body : ""));
            bool includeParameters = true;
            auto headers = header.GetHeader(context.HostName, requestId, includeParameters).Get();

            logRequest(includeParameters);

            if (header.GetMethod() == "GET") {
                response = RequestImpl(header.GetMethod(), url, headers, bodyRef);
            } else {
                auto activeRequest = StartRequestImpl(header.GetMethod(), url, headers);

                auto request = std::make_unique<TCoreHttpRequest>(std::move(context), std::move(activeRequest));
                if (body) {
                    request->GetStream()->Write(*body);
                }
                return request->Finish();
            }
        }

        return std::make_unique<TCoreHttpResponse>(std::move(context), std::move(response));
    }

    IHttpRequestPtr StartRequest(const TString& url, const TString& requestId, const THttpConfig& /*config*/, const THttpHeader& header) override
    {
        TCoreRequestContext context = CreateContext(url, requestId, header);

        LogRequest(header, url, true, requestId, context.HostName);
        context.LoggedAttributes = GetLoggedAttributes(header, url, true, 128);

        auto headers = header.GetHeader(context.HostName, requestId, true).Get();
        auto activeRequest = StartRequestImpl(header.GetMethod(), url, headers);

        return std::make_unique<TCoreHttpRequest>(std::move(context), std::move(activeRequest));
    }

private:
    TCoreRequestContext CreateContext(const TString& url, const TString& requestId, const THttpHeader& header)
    {
        TCoreRequestContext context;
        context.Url = url;
        context.RequestId = requestId;

        auto urlRef = NHttp::ParseUrl(url);
        context.HostName = CreateHost(urlRef.Host, urlRef.PortStr);

        context.LogResponse = false;
        auto outputFormat = header.GetOutputFormat();
        if (outputFormat && outputFormat->IsTextYson()) {
            context.LogResponse = true;
        }
        context.StartTime = TInstant::Now();
        return context;
    }

    NHttp::IResponsePtr RequestImpl(const TString& method, const TString& url, const NHttp::THeadersPtr& headers, const TSharedRef& body)
    {
        if (method == "GET") {
            return Client_->Get(url, headers).Get().ValueOrThrow();
        } else if (method == "POST") {
            return Client_->Post(url, body, headers).Get().ValueOrThrow();
        } else if (method == "PUT") {
            return Client_->Put(url, body, headers).Get().ValueOrThrow();
        } else {
            YT_LOG_FATAL("Unsupported http method (Method: %v, Url: %v)",
                method,
                url);
        }
    }

    NHttp::IActiveRequestPtr StartRequestImpl(const TString& method, const TString& url, const NHttp::THeadersPtr& headers)
    {
        if (method == "POST") {
            return Client_->StartPost(url, headers).Get().ValueOrThrow();
        } else if (method == "PUT") {
            return Client_->StartPut(url, headers).Get().ValueOrThrow();
        } else {
            YT_LOG_FATAL("Unsupported http method (Method: %v, Url: %v)",
                method,
                url);
        }
    }

    NConcurrency::IThreadPoolPollerPtr Poller_;
    NHttp::IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

IHttpClientPtr CreateDefaultHttpClient()
{
    return std::make_shared<TDefaultHttpClient>();
}

IHttpClientPtr CreateCoreHttpClient(bool useTLS, const TConfigPtr& config)
{
    return std::make_shared<TCoreHttpClient>(useTLS, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpClient
