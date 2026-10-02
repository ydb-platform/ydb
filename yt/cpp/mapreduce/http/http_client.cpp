#include "http_client.h"

#include "abortable_http_response.h"
#include "core.h"
#include "helpers.h"
#include "http.h"

#include <yt/cpp/mapreduce/common/abortable_stream.h>
#include <yt/cpp/mapreduce/common/expected_error_guard.h>
#include <yt/cpp/mapreduce/common/halting_stream.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/cpp/mapreduce/interface/error_codes.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/async_stream_helpers.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/compression.h>
#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/http.h>

#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yt/logging/logger.h>

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

    auto logAndSetError = [&] (int code, const TString& rawError) {
        YT_TLOG_ERROR("Response carries an HTTP error")
            .With("RequestId", requestId)
            .With("HttpCode", httpCode)
            .With("Error", rawError);
        return TErrorResponse(TYtError(code, rawError), requestId);
    };


    switch (httpCode) {
        case NHttp::EStatusCode::TooManyRequests:
            return logAndSetError(NClusterErrorCodes::NSecurityClient::RequestQueueSizeLimitExceeded, "request rate limit exceeded");

        case NHttp::EStatusCode::InternalServerError:
            return logAndSetError(NClusterErrorCodes::NRpc::Unavailable, "internal error in proxy " + hostName);

        case NHttp::EStatusCode::ServiceUnavailable:
            return logAndSetError(NClusterErrorCodes::NBus::TransportError, "service unavailable");

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

            TMaybe<TErrorResponse> errorResponse;
            if (auto errorHeader = response->GetHeaders()->Find("X-YT-Error")) {
                TYtError error;
                error.ParseFrom(*errorHeader);

                if (error.GetCode() != 0) {
                    errorResponse.Emplace(std::move(error), requestId);
                }
            } else {
                errorResponse = TErrorResponse(TYtError(errorString + " - X-YT-Error is missing in headers"), requestId);
            }

            if (errorResponse && TExpectedErrorGuard::IsErrorExpected(*errorResponse)) {
                YT_TLOG_INFO("Response carries an expected error")
                    .With("Error", errorString);
            } else {
                YT_TLOG_ERROR("Response carries an error")
                    .With("Error", errorString);
            }

            return errorResponse;
        }
    }
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

    IAbortableInputStream* GetResponseStream() override
    {
        if (!Stream_) {
            Stream_ = NDetail::CreateAbortableInputStreamAdapterFallback(Request_->GetResponseStream());
        }
        return Stream_.get();
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
    std::unique_ptr<IAbortableInputStream> Stream_;
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
        auto urlRef = NHttp::ParseUrl(url);
        auto host = CreateHost(urlRef.Host, urlRef.PortStr);

        auto request = std::make_unique<THttpRequest>(requestId, host, header, config.SocketTimeout);

        request->SmallRequest(body);
        return std::make_unique<TDefaultHttpResponse>(std::move(request));
    }

    IHttpRequestPtr StartRequest(const TString& url, const TString& requestId, const THttpConfig& config, const THttpHeader& header) override
    {
        auto urlRef = NHttp::ParseUrl(url);
        auto host = CreateHost(urlRef.Host, urlRef.PortStr);

        auto request = std::make_unique<THttpRequest>(requestId, host, header, config.SocketTimeout);

        auto stream = request->StartRequest();
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
    NLogging::TLoggingTagList LoggedAttributes;
    TMaybe<NHttp::TContentEncoding> ContentEncoding;
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

    IAbortableInputStream* GetResponseStream() override
    {
        if (!Stream_) {
            NConcurrency::IAsyncInputStreamPtr asyncStream = GetDecompressedStream();

            if (TConfig::Get()->UseHaltingResponse) {
                asyncStream = NDetail::CreateHaltingAsyncStream(std::move(asyncStream), TConfig::Get()->HaltingResponseBytesLimit);
            }
            auto stream = std::make_unique<TWrappedStream>(
                NDetail::CreateAbortableInputStreamAdapter(std::move(asyncStream)),
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

        auto tags = NLogging::TLoggingTagList()
            .With("RequestId", Context_.RequestId)
            .With("Time", TInstant::Now() - Context_.StartTime)
            .With("HostName", Context_.HostName);
        tags.Add(Context_.LoggedAttributes);

        if (Context_.LogResponse) {
            constexpr auto sizeLimit = 1 << 7;
            YT_TLOG_DEBUG("Response received")
                .With(tags)
                .With("Response", TruncateForLogs(result, sizeLimit));
        } else {
            YT_TLOG_DEBUG("Response received")
                .With(tags)
                .With("Size", result.size());
        }
        return result;
    }

    TString GetRequestId() const override
    {
        return Context_.RequestId;
    }
private:
    NConcurrency::IAsyncInputStreamPtr GetDecompressedStream()
    {
        if (auto encoding = Response_->GetHeaders()->Find("Content-Encoding")) {
            if (!NHttp::IsContentEncodingSupported(*encoding)) {
                ythrow yexception() << "Unsupported content encoding: " << *encoding;
            }
            if (*encoding != NHttp::IdentityContentEncoding) {
                return NHttp::CreateDecompressingAdapter(Response_, *encoding, GetSyncInvoker());
            }
        }
        return NConcurrency::CreateCopyingAdapter(Response_);
    }
    class TWrappedStream
        : public IAbortableInputStream
    {
    public:
        TWrappedStream(std::unique_ptr<IAbortableInputStream> underlying, NHttp::IResponsePtr response, TString requestId)
            : Underlying_(std::move(underlying))
            , Response_(std::move(response))
            , RequestId_(std::move(requestId))
        { }

        void Abort() override
        {
            Underlying_->Abort();
        }

        bool IsAborted() const override
        {
            return Underlying_->IsAborted();
        }

    protected:
        size_t DoRead(void* buf, size_t len) override
        {
            size_t read = Underlying_->Read(buf, len);

            if (read == 0 && len != 0) {
                CheckTrailers(GetTrailers());
            }
            return read;
        }

        size_t DoSkip(size_t len) override
        {
            size_t skipped = Underlying_->Skip(len);
            if (skipped == 0 && len != 0) {
                CheckTrailers(GetTrailers());
            }
            return skipped;
        }

    private:
        NHttp::THeadersPtr GetTrailers()
        {
            auto chunk = Response_->Read().BlockingGet().ValueOrThrow();
            while (chunk) {
                chunk = Response_->Read().BlockingGet().ValueOrThrow();
            }
            return Response_->GetTrailers();
        }

        void CheckTrailers(const NHttp::THeadersPtr& trailers)
        {
            if (auto errorResponse = ParseError(trailers)) {
                errorResponse->SetIsFromTrailers(true);
                YT_TLOG_ERROR("Response trailers carry an error")
                    .With("RequestId", RequestId_)
                    .With("Error", errorResponse.GetRef().what());
                ythrow errorResponse.GetRef();
            }
        }

        TMaybe<TErrorResponse> ParseError(const NHttp::THeadersPtr& headers)
        {
            if (auto errorHeader = headers->Find("X-YT-Error")) {
                TYtError error;
                error.ParseFrom(*errorHeader);
                TErrorResponse errorResponse(std::move(error), RequestId_);
                if (errorResponse.IsOk()) {
                    return Nothing();
                }
                return errorResponse;
            }
            return Nothing();
        }

    private:
        std::unique_ptr<IAbortableInputStream> Underlying_;
        NHttp::IResponsePtr Response_;
        TString RequestId_;
    };

private:
    TCoreRequestContext Context_;
    NHttp::IResponsePtr Response_;
    std::unique_ptr<IAbortableInputStream> Stream_;
};

class TCoreHttpRequest
    : public IHttpRequest
{
public:
    TCoreHttpRequest(TCoreRequestContext context, NHttp::IActiveRequestPtr activeRequest)
        : TCoreHttpRequest(PrepareInitArgs(std::move(context), std::move(activeRequest)))
    { }

    IOutputStream* GetStream() override
    {
        return &WrappedStream_;
    }

    IHttpResponsePtr Finish() override
    {
        WrappedStream_.Finish();
        auto response = ActiveRequest_->Finish().BlockingGet().ValueOrThrow();
        return std::make_unique<TCoreHttpResponse>(std::move(Context_), std::move(response));
    }

    IHttpResponsePtr FinishWithError()
    {
        auto response = ActiveRequest_->GetResponse();
        return std::make_unique<TCoreHttpResponse>(std::move(Context_), std::move(response));
    }

private:
    struct TInitializationArgs
    {
        NConcurrency::IAsyncOutputStreamPtr Compressor;
        TCoreRequestContext Context;
        NHttp::IActiveRequestPtr ActiveRequest;
    };

    static TInitializationArgs PrepareInitArgs(
        TCoreRequestContext context,
        NHttp::IActiveRequestPtr activeRequest)
    {
        auto compressor = GetCompressor(context, activeRequest);
        return {
            std::move(compressor),
            std::move(context),
            std::move(activeRequest)
        };
    }

    TCoreHttpRequest(TInitializationArgs args)
        : Context_(std::move(args.Context))
        , ActiveRequest_(std::move(args.ActiveRequest))
        , Stream_(NConcurrency::CreateBufferedSyncAdapter(args.Compressor ? args.Compressor : ActiveRequest_->GetRequestStream()))
        , WrappedStream_(this, Stream_.get(), args.Compressor)
    { }

    static NConcurrency::IAsyncOutputStreamPtr GetCompressor(const TCoreRequestContext& context, const NHttp::IActiveRequestPtr& activeRequest)
    {
        if (auto encoding = context.ContentEncoding) {
            if (!NHttp::IsContentEncodingSupported(*encoding)) {
                ythrow yexception() << "Unsupported content encoding: " << *encoding;
            }
            if (*encoding != NHttp::IdentityContentEncoding) {
                return NHttp::CreateCompressingAdapter(activeRequest->GetRequestStream(), *encoding, GetSyncInvoker());
            }
        }
        return nullptr;
    }

    class TWrappedStream
        : public IOutputStream
    {
    public:
        TWrappedStream(TCoreHttpRequest* httpRequest, IOutputStream* underlying, NConcurrency::IAsyncOutputStreamPtr compressor)
            : HttpRequest_(httpRequest)
            , Underlying_(underlying)
            , Compressor_(compressor)
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
            Flush();
            WrapWriteFunc([&] {
                Underlying_->Finish();
                CloseCompressor();
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
        void HandleWriteException()
        {
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

        void CloseCompressor()
        {
            if (Compressor_) {
                auto future = Compressor_->Close();
                future.BlockingGet().ThrowOnError();
            }
        }

    private:
        TCoreHttpRequest* const HttpRequest_;
        IOutputStream* Underlying_;
        std::exception_ptr WriteError_;
        NConcurrency::IAsyncOutputStreamPtr Compressor_;
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
            httpsConfig->DnsResolveOptions = GetDnsResolveOptions(config);
            Client_ = NHttps::CreateClient(httpsConfig, Poller_);
        } else {
            auto httpConfig = NYT::New<NYT::NHttp::TClientConfig>();
            httpConfig->MaxIdleConnections = config->ConnectionPoolSize;
            httpConfig->DnsResolveOptions = GetDnsResolveOptions(config);
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

            YT_TLOG_DEBUG("Requesting connection from connection pool")
                .With("RequestId", context.RequestId)
                .With("HostName", context.HostName);

            logRequest(includeParameters);
            return NonGetRequestImpl(header, url, headers, context, parametersStr);
        } else {
            auto bodyRef = TSharedRef::FromString(TString(body ? *body : ""));
            bool includeParameters = true;
            auto headers = header.GetHeader(context.HostName, requestId, includeParameters).Get();

            YT_TLOG_DEBUG("Requesting connection from connection pool")
                .With("RequestId", context.RequestId)
                .With("HostName", context.HostName);
            logRequest(includeParameters);

            if (header.GetMethod() == "GET") {
                response = RequestImpl(header.GetMethod(), url, headers, bodyRef);
            } else {
                return NonGetRequestImpl(header, url, headers, context, body);
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
        context.ContentEncoding = header.GetRequestCompression();
        context.StartTime = TInstant::Now();
        return context;
    }


    NHttp::IResponsePtr RequestImpl(const TString& method, const TString& url, const NHttp::THeadersPtr& headers, const TSharedRef& body)
    {
        if (method == "GET") {
            return Client_->Get(url, headers).BlockingGet().ValueOrThrow();
        } else if (method == "POST") {
            return Client_->Post(url, body, headers).BlockingGet().ValueOrThrow();
        } else if (method == "PUT") {
            return Client_->Put(url, body, headers).BlockingGet().ValueOrThrow();
        } else {
            YT_TLOG_FATAL("Unsupported http method")
                .With("Method", method)
                .With("Url", url);
        }
    }

    NHttp::IActiveRequestPtr StartRequestImpl(const TString& method, const TString& url, const NHttp::THeadersPtr& headers)
    {
        if (method == "POST") {
            return Client_->StartPost(url, headers).BlockingGet().ValueOrThrow();
        } else if (method == "PUT") {
            return Client_->StartPut(url, headers).BlockingGet().ValueOrThrow();
        } else {
            YT_TLOG_FATAL("Unsupported http method")
                .With("Method", method)
                .With("Url", url);
        }
    }

    IHttpResponsePtr NonGetRequestImpl(const THttpHeader& header, const TString& url, const NHttp::THeadersPtr& headers, const TCoreRequestContext& context, TMaybe<TStringBuf> message)
    {
        auto activeRequest = StartRequestImpl(header.GetMethod(), url, headers);
        YT_TLOG_DEBUG("Connection established")
            .With("RequestId", context.RequestId)
            .With("HostName", context.HostName);
        auto request = std::make_unique<TCoreHttpRequest>(context, std::move(activeRequest));
        if (message) {
            request->GetStream()->Write(*message);
        }
        return request->Finish();
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
