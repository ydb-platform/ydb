#pragma once
#include <util/datetime/base.h>
#include <util/string/builder.h>
#include <util/system/thread.h>
#include <util/system/hp_timer.h>
#include <util/generic/hash_set.h>
#include <util/generic/buffer.h>
#include <util/generic/intrlist.h>
#include "http_config.h"

// TODO(xenoxeno): hide in implementation
template <typename Type>
struct THash<TIntrusivePtr<Type>> {
    size_t operator ()(const TIntrusivePtr<Type>& ptr) const { return reinterpret_cast<size_t>(ptr.Get()); }
};

template<>
inline void Out<NHttp::THttpConfig::SocketAddressType>(IOutputStream& o, const NHttp::THttpConfig::SocketAddressType& x) {
    o << x->ToString();
}

namespace NHttp {

bool IsIPv6(const TString& host);
bool IsIPv4(const TString& host);
bool CrackURL(TStringBuf url, TStringBuf& scheme, TStringBuf& host, TStringBuf& uri);
void CrackAddress(const TString& address, TString& hostname, TIpPort& port);
void TrimBegin(TStringBuf& target, char delim);
void TrimEnd(TStringBuf& target, char delim);
void Trim(TStringBuf& target, char delim);
void TrimEnd(TString& target, char delim);
TString CompressDeflate(TStringBuf source);
TString DecompressDeflate(TStringBuf source);

struct TLessNoCase {
    bool operator()(TStringBuf l, TStringBuf r) const {
        auto ll = l.length();
        auto rl = r.length();
        if (ll != rl) {
            return ll < rl;
        }
        return strnicmp(l.data(), r.data(), ll) < 0;
    }
};

struct TEqNoCase {
    bool operator()(TStringBuf l, TStringBuf r) const {
        auto ll = l.length();
        auto rl = r.length();
        if (ll != rl) {
            return false;
        }
        return strnicmp(l.data(), r.data(), ll) == 0;
    }
};

struct TSensors {
    TString Direction;
    TString Host;
    TString Url;
    TString Status;
    TDuration Time;

    TSensors(
            TStringBuf direction,
            TStringBuf host,
            TStringBuf url,
            TStringBuf status,
            TDuration time)
        : Direction(direction)
        , Host(host)
        , Url(url)
        , Status(status)
        , Time(time)
    {}
};

struct TUrlParameters {
    THashMap<TStringBuf, TStringBuf> Parameters;

    TUrlParameters(TStringBuf url);
    TString operator [](TStringBuf name) const;
    bool Has(TStringBuf name) const;
    TStringBuf Get(TStringBuf name) const; // raw
    TString Render() const;
};

struct TCookies {
    THashMap<TStringBuf, TStringBuf> Cookies;

    TCookies(TStringBuf cookie);
    TCookies(const TCookies&) = delete;
    TStringBuf operator [](TStringBuf name) const;
    bool Has(TStringBuf name) const;
    TStringBuf Get(TStringBuf name) const; // raw
    TString Render() const;
};

struct TCookiesBuilder : TCookies {
    TDeque<std::pair<TString, TString>> Data;

    TCookiesBuilder();
    void Set(TStringBuf name, TStringBuf data);
};

struct THeaders {
    TMap<TStringBuf, TStringBuf, TLessNoCase> Headers;

    THeaders() = default;
    THeaders(TStringBuf headers);
    THeaders(const THeaders&) = delete;
    const TStringBuf operator [](TStringBuf name) const;
    bool Has(TStringBuf name) const;
    TStringBuf Get(TStringBuf name) const; // raw
    size_t Parse(TStringBuf headers);
    TString Render() const;
};

struct THeadersBuilder : THeaders {
    TDeque<std::pair<TString, TString>> Data;

    THeadersBuilder();
    THeadersBuilder(TStringBuf headers);
    THeadersBuilder(const THeadersBuilder& builder);
    void Set(TStringBuf name, TStringBuf data);
    void Erase(TStringBuf name);
};

class TSocketBuffer : public TBuffer, public THttpConfig {
public:
    TSocketBuffer()
        : TBuffer(BUFFER_SIZE)
    {}

    bool EnsureEnoughSpaceAvailable(size_t need) {
        size_t avail = Avail();
        if (avail < need) {
            Reserve(Capacity() + std::max(need, BUFFER_MIN_STEP));
            return false;
        }
        return true;
    }

    // non-destructive variant of AsString
    TString AsString() const {
        return TString(Data(), Size());
    }
};

class THttpRequest {
public:
    TStringBuf Method;
    TStringBuf URL;
    TStringBuf Protocol;
    TStringBuf Version;
    TStringBuf Headers;

    TStringBuf Host;
    TStringBuf Accept;
    TStringBuf Connection;
    TStringBuf ContentType;
    TStringBuf ContentLength;
    TStringBuf AcceptEncoding;
    TStringBuf TransferEncoding;

    TStringBuf Body;

    static const TMap<TStringBuf, TStringBuf THttpRequest::*, TLessNoCase> HeadersLocation;

    template <TStringBuf THttpRequest::* Header>
    static TStringBuf GetName();
    void Clear();
    TString GetURL() const;
};

class THttpResponse {
public:
    TStringBuf Protocol;
    TStringBuf Version;
    TStringBuf Status;
    TStringBuf Message;
    TStringBuf Headers;

    TStringBuf Connection;
    TStringBuf ContentType;
    TStringBuf ContentLength;
    TStringBuf TransferEncoding;
    TStringBuf LastModified;
    TStringBuf ContentEncoding;

    TStringBuf Body;

    static const TMap<TStringBuf, TStringBuf THttpResponse::*, TLessNoCase> HeadersLocation;

    template <TStringBuf THttpResponse::* Header>
    static TStringBuf GetName();
    void Clear();
};

template <typename HeaderType, typename BufferType>
class THttpParser : public HeaderType, public BufferType {
public:
    enum class EParseStage : ui8 {
        Method,
        URL,
        Protocol,
        Version,
        Status,
        Message,
        Header,
        Body,
        ChunkLength,
        ChunkData,
        Done,
        Error,
    };

    static constexpr size_t MaxMethodSize = 8;
    static constexpr size_t MaxURLSize = 2048;
    static constexpr size_t MaxProtocolSize = 4;
    static constexpr size_t MaxVersionSize = 4;
    static constexpr size_t MaxStatusSize = 3;
    static constexpr size_t MaxMessageSize = 1024;
    static constexpr size_t MaxHeaderSize = 8192;
    static constexpr size_t MaxChunkLengthSize = 8;
    static constexpr size_t MaxChunkSize = 256 * 1024 * 1024;
    static constexpr size_t MaxChunkContentSize = 1 * 1024 * 1024 * 1024;

    EParseStage Stage;
    EParseStage LastSuccessStage;
    TStringBuf Line;
    TStringBuf& Header = Line;
    size_t ChunkLength = 0;
    size_t ContentSize = 0;
    TString Content; // body storage
    std::optional<size_t> TotalSize;

    THttpParser(const THttpParser& src)
        : HeaderType(src)
        , BufferType(src)
        , Stage(src.Stage)
        , LastSuccessStage(src.LastSuccessStage)
        , Line()
        , Header(Line)
        , ChunkLength(src.ChunkLength)
        , ContentSize(src.ContentSize)
        , Content(src.Content)
    {}

    template <typename StringType>
    bool ProcessData(StringType& target, TStringBuf& source, char delim, size_t maxLen) {
        TStringBuf maxSource(source.substr(0, maxLen + 1 - target.size()));
        size_t pos = maxSource.find(delim);
        target += maxSource.substr(0, pos);
        source.Skip(pos);
        if (target.size() > maxLen) {
            Stage = EParseStage::Error;
            return false;
        }
        if (!source.empty() && *source.begin() == delim) {
            source.Skip(1);
        }
        return pos != TStringBuf::npos;
    }

    template <typename StringType>
    bool ProcessData(StringType& target, TStringBuf& source, TStringBuf delim, size_t maxLen) {
        if (delim.empty()) {
            return false;
        }
        if (delim.size() == 1) {
            return ProcessData(target, source, delim[0], maxLen);
        }
        if (ProcessData(target, source, delim.back(), maxLen + 1)) {
            for (signed i = delim.size() - 2; i >= 0; --i) {
                TrimEnd(target, delim[i]);
            }
            return true;
        }
        return false;
    }

    template <typename StringType>
    bool ProcessData(StringType& target, TStringBuf& source, size_t size) {
        TStringBuf maxSource(source.substr(0, size - target.size()));
        target += maxSource;
        source.Skip(maxSource.size());
        if (target.size() > size && !source.empty()) {
            Stage = EParseStage::Error;
            return false;
        }
        return target.size() == size;
    }

    bool ProcessHeader(TStringBuf& header) {
        TStringBuf name;
        TStringBuf value;
        if (!header.TrySplit(':', name, value)) {
            return false;
        }
        TrimBegin(name, ' ');
        Trim(value, ' ');
        auto cit = HeaderType::HeadersLocation.find(name);
        if (cit != HeaderType::HeadersLocation.end()) {
            this->*cit->second = value;
        }
        header.Clear();
        return true;
    }

    size_t ParseHex(TStringBuf value) {
        size_t result = 0;
        for (char ch : value) {
            if (ch >= '0' && ch <= '9') {
                result *= 16;
                result += ch - '0';
            } else if (ch >= 'a' && ch <= 'f') {
                result *= 16;
                result += 10 + ch - 'a';
            } else if (ch >= 'A' && ch <= 'F') {
                result *= 16;
                result += 10 + ch - 'A';
            } else if (ch == ';') {
                break;
            } else if (isspace(ch)) {
                continue;
            } else {
                Stage = EParseStage::Error;
                return 0;
            }
        }
        return result;
    }

    void Advance(size_t len);
    void ConnectionClosed();

    size_t GetBodySizeFromTotalSize() const {
        return TotalSize.value() - (HeaderType::Headers.end() - BufferType::Data());
    }

    void Clear() {
        BufferType::Clear();
        HeaderType::Clear();
        Stage = GetInitialStage();
        Line.Clear();
        Content.clear();
    }

    bool IsReady() const {
        return Stage == EParseStage::Done;
    }

    bool IsError() const {
        return Stage == EParseStage::Error;
    }

    TStringBuf GetErrorText() const {
        switch (LastSuccessStage) {
        case EParseStage::Method:
            return "Invalid http method";
        case EParseStage::URL:
            return "Invalid url";
        case EParseStage::Protocol:
            return "Invalid http protocol";
        case EParseStage::Version:
            return "Invalid http version";
        case EParseStage::Status:
            return "Invalid http status";
        case EParseStage::Message:
            return "Invalid http message";
        case EParseStage::Header:
            return "Invalid http header";
        case EParseStage::Body:
            return "Invalid content body";
        case EParseStage::ChunkLength:
        case EParseStage::ChunkData:
            return "Broken chunked data";
        case EParseStage::Done:
            return "Everything is fine";
        case EParseStage::Error:
            return "Error on error"; // wat? ...because we don't want to include default label here
        }
    }

    bool IsDone() const {
        return IsReady() || IsError();
    }

    bool HaveBody() const;

    bool EnsureEnoughSpaceAvailable(size_t need = BufferType::BUFFER_MIN_STEP) {
        bool result = BufferType::EnsureEnoughSpaceAvailable(need);
        if (!result && !BufferType::Empty()) {
            Reparse();
        }
        return true;
    }

    void Reparse() {
        size_t size = BufferType::Size();
        Clear();
        Advance(size);
    }

    TStringBuf GetRawData() const {
        return TStringBuf(BufferType::Data(), BufferType::Size());
    }

    TString GetObfuscatedData() const {
        THeaders headers(HeaderType::Headers);
        TStringBuf authorization(headers["Authorization"]);
        TStringBuf cookie(headers["Cookie"]);
        TStringBuf x_ydb_auth_ticket(headers["x-ydb-auth-ticket"]);
        TStringBuf x_yacloud_subjecttoken(headers["x-yacloud-subjecttoken"]);
        TString data(GetRawData());
        if (!authorization.empty()) {
            auto pos = data.find(authorization);
            if (pos != TString::npos) {
                data.replace(pos, authorization.size(), TString("<obfuscated>"));
            }
        }
        if (!cookie.empty()) {
            auto pos = data.find(cookie);
            if (pos != TString::npos) {
                data.replace(pos, cookie.size(), TString("<obfuscated>"));
            }
        }
        if (!x_ydb_auth_ticket.empty()) {
            auto pos = data.find(x_ydb_auth_ticket);
            if (pos != TString::npos) {
                data.replace(pos, x_ydb_auth_ticket.size(), TString("<obfuscated>"));
            }
        }
        if (!x_yacloud_subjecttoken.empty()) {
            auto pos = data.find(x_yacloud_subjecttoken);
            if (pos != TString::npos) {
                data.replace(pos, x_yacloud_subjecttoken.size(), TString("<obfuscated>"));
            }
        }
        return data;
    }

    static EParseStage GetInitialStage();

    THttpParser()
        : Stage(GetInitialStage())
        , LastSuccessStage(Stage)
    {}

    THttpParser(TStringBuf data)
        : Stage(GetInitialStage())
        , LastSuccessStage(Stage)
    {
        BufferType::Assign(data.data(), data.size());
        BufferType::Clear(); // reset position to 0
        TotalSize = data.size();
        Advance(data.size());
    }
};

template <typename HeaderType, typename BufferType>
class THttpRenderer : public HeaderType, public BufferType {
public:
    enum class ERenderStage {
        Init,
        Header,
        Body,
        Done,
        Error,
    };

    ERenderStage Stage = ERenderStage::Init;
    TString Content; // body storage

    //THttpRenderer(TStringBuf method, TStringBuf url, TStringBuf protocol, TStringBuf version); // request
    void InitRequest(TStringBuf method, TStringBuf url, TStringBuf protocol, TStringBuf version) {
        Y_DEBUG_ABORT_UNLESS(Stage == ERenderStage::Init);
        AppendParsedValue<&THttpRequest::Method>(method);
        Append(' ');
        AppendParsedValue<&THttpRequest::URL>(url);
        Append(' ');
        AppendParsedValue<&THttpRequest::Protocol>(protocol);
        Append('/');
        AppendParsedValue<&THttpRequest::Version>(version);
        Append("\r\n");
        Stage = ERenderStage::Header;
        HeaderType::Headers = TStringBuf(BufferType::Pos(), size_t(0));
    }

    //THttpRenderer(TStringBuf protocol, TStringBuf version, TStringBuf status, TStringBuf message); // response
    void InitResponse(TStringBuf protocol, TStringBuf version, TStringBuf status, TStringBuf message) {
        Y_DEBUG_ABORT_UNLESS(Stage == ERenderStage::Init);
        AppendParsedValue<&THttpResponse::Protocol>(protocol);
        Append('/');
        AppendParsedValue<&THttpResponse::Version>(version);
        Append(' ');
        AppendParsedValue<&THttpResponse::Status>(status);
        Append(' ');
        AppendParsedValue<&THttpResponse::Message>(message);
        Append("\r\n");
        Stage = ERenderStage::Header;
        HeaderType::Headers = TStringBuf(BufferType::Pos(), size_t(0));
    }

    void Append(TStringBuf text) {
        EnsureEnoughSpaceAvailable(text.size());
        BufferType::Append(text.data(), text.size());
    }

    void Append(char c) {
        EnsureEnoughSpaceAvailable(sizeof(c));
        BufferType::Append(c);
    }

    template <TStringBuf HeaderType::* string>
    void AppendParsedValue(TStringBuf value) {
        Append(value);
        static_cast<HeaderType*>(this)->*string = TStringBuf(BufferType::Pos() - value.size(), value.size());
    }

    template <TStringBuf HeaderType::* name>
    void Set(TStringBuf value) {
        Y_DEBUG_ABORT_UNLESS(Stage == ERenderStage::Header);
        Append(HeaderType::template GetName<name>());
        Append(": ");
        AppendParsedValue<name>(value);
        Append("\r\n");
        HeaderType::Headers = TStringBuf(HeaderType::Headers.Data(), BufferType::Pos() - HeaderType::Headers.Data());
    }

    void Set(TStringBuf name, TStringBuf value) {
        Y_DEBUG_ABORT_UNLESS(Stage == ERenderStage::Header);
        Append(name);
        Append(": ");
        auto data = BufferType::Pos();
        Append(value);
        auto cit = HeaderType::HeadersLocation.find(name);
        if (cit != HeaderType::HeadersLocation.end()) {
            (this->*cit->second) = TStringBuf(data, BufferType::Pos());
        }
        Append("\r\n");
        HeaderType::Headers = TStringBuf(HeaderType::Headers.Data(), BufferType::Pos() - HeaderType::Headers.Data());
    }

    void Set(const THeaders& headers) {
        Y_DEBUG_ABORT_UNLESS(Stage == ERenderStage::Header);
        for (const auto& [name, value] : headers.Headers) {
            Set(name, value);
        }
        HeaderType::Headers = TStringBuf(HeaderType::Headers.Data(), BufferType::Pos() - HeaderType::Headers.Data());
    }

    static constexpr TStringBuf ALLOWED_CONTENT_ENCODINGS[] = {"deflate"};

    void SetContentEncoding(TStringBuf contentEncoding) {
        Y_DEBUG_ABORT_UNLESS(Stage == ERenderStage::Header);
        if (Count(ALLOWED_CONTENT_ENCODINGS, contentEncoding) != 0) {
            Set("Content-Encoding", contentEncoding);
        }
    }

    void FinishHeader() {
        Append("\r\n");
        HeaderType::Headers = TStringBuf(HeaderType::Headers.Data(), BufferType::Pos() - HeaderType::Headers.Data());
        Stage = ERenderStage::Body;
    }

    void SetBody(TStringBuf body) {
        Y_DEBUG_ABORT_UNLESS(Stage == ERenderStage::Header);
        if (HeaderType::ContentLength.empty()) {
            Set<&HeaderType::ContentLength>(ToString(body.size()));
        }
        FinishHeader();
        AppendParsedValue<&HeaderType::Body>(body);
        Stage = ERenderStage::Done;
    }

    void FinishBody() {
        Stage = ERenderStage::Done;
    }

    bool IsDone() const {
        return Stage == ERenderStage::Done;
    }

    void Finish() {
        switch (Stage) {
        case ERenderStage::Header:
            FinishHeader();
            FinishBody();
            break;
        case ERenderStage::Body:
            FinishBody();
            break;
        default:
            break;
        }
    }

    bool EnsureEnoughSpaceAvailable(size_t need = BufferType::BUFFER_MIN_STEP) {
        bool result = BufferType::EnsureEnoughSpaceAvailable(need);
        if (!result && !BufferType::Empty()) {
            Reparse();
        }
        return true;
    }

    void Clear() {
        BufferType::Clear();
        HeaderType::Clear();
    }

    void Reparse() {
        // move-magic
        size_t size = BufferType::Size();
        THttpParser<HeaderType, BufferType> parser;
        // move the buffer to parser
        static_cast<BufferType&>(parser) = std::move(static_cast<BufferType&>(*this));
        // reparse
        parser.Clear();
        parser.Advance(size);
        // move buffer and result back
        bool needReassignBody = (parser.Body.data() == parser.Content.data());
        static_cast<HeaderType&>(*this) = std::move(static_cast<HeaderType&>(parser));
        static_cast<BufferType&>(*this) = std::move(static_cast<BufferType&>(parser));
        if (needReassignBody) {
            Content = std::move(parser.Content);
            HeaderType::Body = Content;
        }
        switch (parser.Stage) {
        case THttpParser<HeaderType, BufferType>::EParseStage::Method:
        case THttpParser<HeaderType, BufferType>::EParseStage::URL:
        case THttpParser<HeaderType, BufferType>::EParseStage::Protocol:
        case THttpParser<HeaderType, BufferType>::EParseStage::Version:
        case THttpParser<HeaderType, BufferType>::EParseStage::Status:
        case THttpParser<HeaderType, BufferType>::EParseStage::Message:
            Stage = ERenderStage::Init;
            break;
        case THttpParser<HeaderType, BufferType>::EParseStage::Header:
            Stage = ERenderStage::Header;
            break;
        case THttpParser<HeaderType, BufferType>::EParseStage::Body:
        case THttpParser<HeaderType, BufferType>::EParseStage::ChunkLength:
        case THttpParser<HeaderType, BufferType>::EParseStage::ChunkData:
            Stage = ERenderStage::Body;
            break;
        case THttpParser<HeaderType, BufferType>::EParseStage::Done:
            Stage = ERenderStage::Done;
            break;
        case THttpParser<HeaderType, BufferType>::EParseStage::Error:
            Stage = ERenderStage::Error;
            break;
        }
        Y_ABORT_UNLESS(size == BufferType::Size());
    }

    TStringBuf GetRawData() const {
        return TStringBuf(BufferType::Data(), BufferType::Size());
    }
};

template <>
template <>
inline void THttpRenderer<THttpResponse, TSocketBuffer>::Set<&THttpResponse::Body>(TStringBuf value) {
    SetBody(value);
}

template <>
template <>
inline void THttpRenderer<THttpRequest, TSocketBuffer>::Set<&THttpRequest::Body>(TStringBuf value) {
    SetBody(value);
}

template <>
template <>
inline void THttpRenderer<THttpResponse, TSocketBuffer>::Set<&THttpResponse::ContentEncoding>(TStringBuf value) {
    SetContentEncoding(value);
}

struct THttpEndpointInfo {
    TString WorkerName;
    bool Secure = false;
    const std::vector<TString> CompressContentTypes; // content types, which will be automatically compressed on response

    THttpEndpointInfo() = default;

protected:
    THttpEndpointInfo(std::vector<TString> compressContentTypes)
        : CompressContentTypes(std::move(compressContentTypes))
    {}
};

class THttpIncomingRequest;
using THttpIncomingRequestPtr = TIntrusivePtr<THttpIncomingRequest>;

class THttpOutgoingResponse;
using THttpOutgoingResponsePtr = TIntrusivePtr<THttpOutgoingResponse>;

class THttpOutgoingRequest;
using THttpOutgoingRequestPtr = TIntrusivePtr<THttpOutgoingRequest>;

class THttpIncomingRequest :
        public THttpParser<THttpRequest, TSocketBuffer>,
        public TRefCounted<THttpIncomingRequest, TAtomicCounter> {
public:
    std::shared_ptr<THttpEndpointInfo> Endpoint;
    THttpConfig::SocketAddressType Address;
    THPTimer Timer;

    THttpIncomingRequest()
        : Endpoint(std::make_shared<THttpEndpointInfo>())
    {}

    THttpIncomingRequest(std::shared_ptr<THttpEndpointInfo> endpoint, const THttpConfig::SocketAddressType& address)
        : Endpoint(std::move(endpoint))
        , Address(address)
    {}

    THttpIncomingRequest(TStringBuf content, std::shared_ptr<THttpEndpointInfo> endpoint, const THttpConfig::SocketAddressType& address)
        : THttpParser(content)
        , Endpoint(std::move(endpoint))
        , Address(address)
    {}

    bool IsConnectionClose() const {
        if (Connection.empty()) {
            return Version == "1.0";
        } else {
            return TEqNoCase()(Connection, "close");
        }
    }

    TStringBuf GetConnection() const {
        if (!Connection.empty()) {
            if (TEqNoCase()(Connection, "keep-alive")) {
                return "keep-alive";
            }
            if (TEqNoCase()(Connection, "close")) {
                return "close";
            }
        }
        return Version == "1.0" ? "close" : "keep-alive";
    }

    THttpOutgoingResponsePtr CreateResponseOK(TStringBuf body, TStringBuf contentType = "text/html", TInstant lastModified = TInstant());
    THttpOutgoingResponsePtr CreateResponseString(TStringBuf data);
    THttpOutgoingResponsePtr CreateResponseBadRequest(TStringBuf html = TStringBuf(), TStringBuf contentType = "text/html"); // 400
    THttpOutgoingResponsePtr CreateResponseNotFound(TStringBuf html = TStringBuf(), TStringBuf contentType = "text/html"); // 404
    THttpOutgoingResponsePtr CreateResponseServiceUnavailable(TStringBuf html = TStringBuf(), TStringBuf contentType = "text/html"); // 503
    THttpOutgoingResponsePtr CreateResponseGatewayTimeout(TStringBuf html = TStringBuf(), TStringBuf contentType = "text/html"); // 504
    THttpOutgoingResponsePtr CreateResponse(TStringBuf status, TStringBuf message);
    THttpOutgoingResponsePtr CreateResponse(TStringBuf status, TStringBuf message, const THeaders& headers);
    THttpOutgoingResponsePtr CreateResponse(TStringBuf status, TStringBuf message, const THeaders& headers, TStringBuf body);
    THttpOutgoingResponsePtr CreateResponse(
            TStringBuf status,
            TStringBuf message,
            TStringBuf contentType,
            TStringBuf body = TStringBuf(),
            TInstant lastModified = TInstant());

    THttpOutgoingResponsePtr CreateIncompleteResponse(TStringBuf status, TStringBuf message, const THeaders& headers = {});
    THttpOutgoingResponsePtr CreateIncompleteResponse(TStringBuf status, TStringBuf message, const THeaders& headers, TStringBuf body);

    THttpIncomingRequestPtr Duplicate();
    THttpOutgoingRequestPtr Forward(TStringBuf baseUrl) const;

private:
    THttpOutgoingResponsePtr ConstructResponse(TStringBuf status, TStringBuf message);
    void FinishResponse(THttpOutgoingResponsePtr& response, TStringBuf body = TStringBuf());
};

class THttpIncomingResponse;
using THttpIncomingResponsePtr = TIntrusivePtr<THttpIncomingResponse>;

class THttpIncomingResponse :
        public THttpParser<THttpResponse, TSocketBuffer>,
        public TRefCounted<THttpIncomingResponse, TAtomicCounter> {
public:
    THttpIncomingResponse(THttpOutgoingRequestPtr request);

    THttpOutgoingRequestPtr GetRequest() const {
        return Request;
    }

    THttpIncomingResponsePtr Duplicate(THttpOutgoingRequestPtr request);
    THttpOutgoingResponsePtr Reverse(THttpIncomingRequestPtr request);

protected:
    THttpOutgoingRequestPtr Request;
};

class THttpOutgoingRequest :
        public THttpRenderer<THttpRequest, TSocketBuffer>,
        public TRefCounted<THttpOutgoingRequest, TAtomicCounter> {
public:
    THPTimer Timer;
    bool Secure = false;

    THttpOutgoingRequest() = default;
    THttpOutgoingRequest(TStringBuf method, TStringBuf url, TStringBuf protocol, TStringBuf version);
    THttpOutgoingRequest(TStringBuf method, TStringBuf scheme, TStringBuf host, TStringBuf uri, TStringBuf protocol, TStringBuf version);
    static THttpOutgoingRequestPtr CreateRequestString(TStringBuf data);
    static THttpOutgoingRequestPtr CreateRequestString(const TString& data);
    static THttpOutgoingRequestPtr CreateRequestGet(TStringBuf url);
    static THttpOutgoingRequestPtr CreateRequestGet(TStringBuf host, TStringBuf uri); // http only
    static THttpOutgoingRequestPtr CreateRequestPost(TStringBuf url, TStringBuf contentType = {}, TStringBuf body = {});
    static THttpOutgoingRequestPtr CreateRequestPost(TStringBuf host, TStringBuf uri, TStringBuf contentType, TStringBuf body); // http only
    static THttpOutgoingRequestPtr CreateRequest(TStringBuf method, TStringBuf url, TStringBuf contentType = TStringBuf(), TStringBuf body = TStringBuf());
    static THttpOutgoingRequestPtr CreateHttpRequest(TStringBuf method, TStringBuf host, TStringBuf uri, TStringBuf contentType = TStringBuf(), TStringBuf body = TStringBuf());
    THttpOutgoingRequestPtr Duplicate();
};

class THttpOutgoingResponse :
        public THttpRenderer<THttpResponse, TSocketBuffer>,
        public TRefCounted<THttpOutgoingResponse, TAtomicCounter> {
public:
    THttpOutgoingResponse(THttpIncomingRequestPtr request);
    THttpOutgoingResponse(THttpIncomingRequestPtr request, TStringBuf protocol, TStringBuf version, TStringBuf status, TStringBuf message);

    bool IsConnectionClose() const {
        if (!Connection.empty()) {
            return TEqNoCase()(Connection, "close");
        } else {
            return Request->IsConnectionClose();
        }
    }

    bool IsNeedBody() const {
        return GetRequest()->Method != "HEAD" && Status != "204";
    }

    bool EnableCompression() {
        TStringBuf acceptEncoding = Request->AcceptEncoding;
        std::vector<TStringBuf> encodings;
        TStringBuf encoding;
        while (acceptEncoding.NextTok(',', encoding)) {
            Trim(encoding, ' ');
            if (Count(ALLOWED_CONTENT_ENCODINGS, encoding) != 0) {
                encodings.push_back(encoding);
            }
        }
        if (!encodings.empty()) {
            // TODO: prioritize encodings
            SetContentEncoding(encodings.front());
            return true;
        }
        return false;
    }

    void SetBody(TStringBuf body) {
        if (ContentEncoding == "deflate") {
            TString compressedBody = CompressDeflate(body);
            THttpRenderer<THttpResponse, TSocketBuffer>::SetBody(compressedBody);
            Body = Content = body;
        } else {
            THttpRenderer<THttpResponse, TSocketBuffer>::SetBody(body);
        }
    }

    void SetBody(const TString& body) {
        if (ContentEncoding == "deflate") {
            TString compressedBody = CompressDeflate(body);
            THttpRenderer<THttpResponse, TSocketBuffer>::SetBody(compressedBody);
            Body = Content = body;
        } else {
            THttpRenderer<THttpResponse, TSocketBuffer>::SetBody(body);
        }
    }

    THttpIncomingRequestPtr GetRequest() const {
        return Request;
    }

    THttpOutgoingResponsePtr Duplicate(THttpIncomingRequestPtr request);

// it's temporary accessible for cleanup
//protected:
    THttpIncomingRequestPtr Request;
    std::unique_ptr<TSensors> Sensors;
};

}
