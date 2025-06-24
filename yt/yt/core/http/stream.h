#pragma once

#include "http.h"
#include "config.h"

#include <yt/yt/core/net/public.h>
#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/address.h>

#include <contrib/deprecated/http-parser/http_parser.h>

#include <util/stream/buffer.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMessageType,
    (Request)
    (Response)
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EParserState,
    (Initialized)
    (HeadersFinished)
    (MessageFinished)
);

class THttpParser
{
public:
    explicit THttpParser(http_parser_type parserType);

    static http_parser_settings GetParserSettings();

    std::pair<int, int> GetVersion() const;
    EMethod GetMethod() const;
    EStatusCode GetStatusCode() const;
    TString GetFirstLine();

    const THeadersPtr& GetHeaders() const;
    const THeadersPtr& GetTrailers() const;

    void Reset();
    bool ShouldKeepAlive() const;

    EParserState GetState() const;
    TSharedRef GetLastBodyChunk();
    TSharedRef Feed(const TSharedRef& buf);

private:
    http_parser Parser_{};

    TStringBuilder FirstLine_;
    TStringBuilder NextField_;
    TStringBuilder NextValue_;

    THeadersPtr Headers_;
    THeadersPtr Trailers_;

    EParserState State_ = EParserState::Initialized;

    const TSharedRef* InputBuffer_ = nullptr;
    TSharedRef LastBodyChunk_;

    bool ShouldKeepAlive_ = false;
    bool HeaderBuffered_ = false;

    void MaybeFlushHeader(bool trailer);

    static int OnUrl(http_parser* parser, const char *at, size_t length);
    static int OnStatus(http_parser* parser, const char *at, size_t length);
    static int OnHeaderField(http_parser* parser, const char *at, size_t length);
    static int OnHeaderValue(http_parser* parser, const char *at, size_t length);
    static int OnHeadersComplete(http_parser* parser);
    static int OnBody(http_parser* parser, const char *at, size_t length);
    static int OnMessageComplete(http_parser* parser);
};

////////////////////////////////////////////////////////////////////////////////

class THttpInput
    : public IRequest
    , public IResponse
{
public:
    THttpInput(
        NNet::IConnectionPtr connection,
        const NNet::TNetworkAddress& remoteAddress,
        IInvokerPtr readInvoker,
        EMessageType messageType,
        THttpIOConfigPtr config);

    EMethod GetMethod() override;
    const TUrlRef& GetUrl() override;
    std::pair<int, int> GetVersion() override;
    const THeadersPtr& GetHeaders() override;

    EStatusCode GetStatusCode() override;
    const THeadersPtr& GetTrailers() override;

    TFuture<TSharedRef> Read() override;

    const NNet::TNetworkAddress& GetRemoteAddress() const override;

    TConnectionId GetConnectionId() const override;

    TRequestId GetRequestId() const override;
    void SetRequestId(TRequestId requestId);

    i64 GetReadByteCount() const override;

    bool IsExpecting100Continue() const;

    bool IsSafeToReuse() const;
    void Reset();

    // Returns false if connection was closed before receiving first byte.
    bool ReceiveHeaders();

    TInstant GetStartTime() const override;

    bool IsHttps() const override;
    void SetHttps();

    int GetPort() const override;
    void SetPort(int port);

    std::optional<TString> TryGetRedirectUrl();

private:
    const NNet::IConnectionPtr Connection_;
    const NNet::TNetworkAddress RemoteAddress_;
    const EMessageType MessageType_;
    const THttpIOConfigPtr Config_;
    const IInvokerPtr ReadInvoker_;

    TSharedMutableRef InputBuffer_;
    TSharedRef UnconsumedData_;

    bool HeadersReceived_ = false;
    THttpParser Parser_;

    TString RawUrl_;
    TUrlRef Url_;
    int Port_;
    THeadersPtr Headers_;

    // Debug
    TRequestId RequestId_;
    i64 StartByteCount_ = 0;
    NNet::TConnectionStatistics StartStatistics_;
    TInstant LastProgressLogTime_;
    TInstant StartTime_;

    bool SafeToReuse_ = false;
    bool IsHttps_ = false;

    TError AnnotateError(const TError& error);

    void FinishHeaders();
    void FinishMessage();
    void EnsureHeadersReceived();

    TSharedRef DoRead();

    void MaybeLogSlowProgress();

    bool IsRedirectCode(EStatusCode code) const;
};

DEFINE_REFCOUNTED_TYPE(THttpInput)

////////////////////////////////////////////////////////////////////////////////

class THttpOutput
    : public IResponseWriter
{
public:
    THttpOutput(
        THeadersPtr headers,
        NNet::IConnectionPtr connection,
        EMessageType messageType,
        THttpIOConfigPtr config);

    THttpOutput(
        NNet::IConnectionPtr connection,
        EMessageType messageType,
        THttpIOConfigPtr config);

    const THeadersPtr& GetHeaders() override;
    void SetHeaders(const THeadersPtr& headers);
    void SetHost(TStringBuf host, TStringBuf port);
    bool AreHeadersFlushed() const override;

    const THeadersPtr& GetTrailers() override;

    void Flush100Continue();

    void WriteRequest(EMethod method, const TString& path);
    std::optional<EStatusCode> GetStatus() const override;
    void SetStatus(EStatusCode status) override;

    TFuture<void> Write(const TSharedRef& data) override;
    TFuture<void> Flush() override;
    TFuture<void> Close() override;

    TFuture<void> WriteBody(const TSharedRef& smallBody) override;

    void AddConnectionCloseHeader() override;

    bool IsSafeToReuse() const;
    void Reset();

    void SetRequestId(TRequestId requestId);

    i64 GetWriteByteCount() const override;

private:
    const NNet::IConnectionPtr Connection_;
    const EMessageType MessageType_;
    const THttpIOConfigPtr Config_;

    const TClosure OnWriteFinish_;

    // Debug
    TRequestId RequestId_;
    i64 StartByteCount_ = 0;
    NNet::TConnectionStatistics StartStatistics_;
    bool HeadersLogged_ = false;
    TInstant LastProgressLogTime_;

    static const THashSet<TString, TCaseInsensitiveStringHasher, TCaseInsensitiveStringEqualityComparer> FilteredHeaders_;

    bool ConnectionClose_ = false;

    // Headers
    THeadersPtr Headers_;
    std::optional<EStatusCode> Status_;
    std::optional<EMethod> Method_;
    std::optional<TString> HostHeader_;
    TString Path_;
    bool HeadersFlushed_ = false;
    bool MessageFinished_ = false;

    // Trailers
    THeadersPtr Trailers_;

    TError AnnotateError(const TError& error);

    TFuture<void> FinishChunked();

    TSharedRef GetHeadersPart(std::optional<size_t> contentLength);
    TSharedRef GetTrailersPart();

    static TSharedRef GetChunkHeader(size_t size);

    void OnWriteFinish();
};

DEFINE_REFCOUNTED_TYPE(THttpOutput)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
