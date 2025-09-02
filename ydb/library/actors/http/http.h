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

struct THeaders;

bool IsIPv6(const TString& host);
bool IsIPv4(const TString& host);
bool CrackURL(TStringBuf url, TStringBuf& scheme, TStringBuf& host, TStringBuf& uri);
void CrackAddress(const TString& address, TString& hostname, TIpPort& port);
[[nodiscard]] TStringBuf TrimBegin(TStringBuf target, char delim);
[[nodiscard]] TStringBuf TrimEnd(TStringBuf target, char delim);
[[nodiscard]] TStringBuf Trim(TStringBuf target, char delim);
void TrimEnd(TString& target, char delim);
TString CompressDeflate(TStringBuf source);
TString DecompressDeflate(TStringBuf source);
TString GetObfuscatedData(TString data, const THeaders& headers);
TString ToHex(size_t value);
bool IsReadableContent(TStringBuf contentType);

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
    const TString operator [](TStringBuf name) const;
    bool Has(TStringBuf name) const;
    TStringBuf Get(TStringBuf name) const; // raw
    TString Render() const;
};

struct TUrlParametersBuilder : TUrlParameters {
    TDeque<std::pair<TString, TString>> Data;

    using TUrlParameters::TUrlParameters;
    TUrlParametersBuilder();
    void Set(TStringBuf name, TStringBuf data);
};

struct TCookies {
    THashMap<TStringBuf, TStringBuf> Cookies;

    TCookies(TStringBuf cookie);
    TCookies(const TCookies&) = delete;
    const TStringBuf operator [](TStringBuf name) const;
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

    bool IsChunkedEncoding() const;
};

struct THeadersBuilder : THeaders {
    TDeque<std::pair<TString, TString>> Data;

    THeadersBuilder();
    THeadersBuilder(TStringBuf headers);
    THeadersBuilder(const THeadersBuilder& builder);
    THeadersBuilder(std::initializer_list<std::pair<TString, TString>> headers);
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
            auto data1 = Data();
            Reserve(Capacity() + std::max(need, BUFFER_MIN_STEP));
            auto data2 = Data();
            return data1 == data2;
        }
        return true;
    }

    // non-destructive version of AsString
    TString AsString() const {
        return TString(Data(), Size());
    }

    size_t Advance(size_t size) {
        TBuffer::Advance(size);
        return size;
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
    TStringBuf ContentEncoding;

    TStringBuf Body;

    static const TMap<TStringBuf, TStringBuf THttpRequest::*, TLessNoCase> HeadersLocation;

    template <TStringBuf THttpRequest::* Header>
    static TStringBuf GetName();
    void Clear();
    TString GetURL() const;
    TString GetURI() const;
    TUrlParameters GetParameters() const;
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

template<typename HeaderType>
class THttpParser : public HeaderType, public TSocketBuffer {
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
    bool Streaming = false; // true if we are in streaming mode, i.e. don't collect all data in one buffer
    TStringBuf Line;
    TStringBuf& Header = Line;
    size_t ChunkLength = 0;
    TString Content; // body storage
    std::optional<size_t> TotalSize;

    THttpParser(const THttpParser& src)
        : TSocketBuffer(src)
        , Stage(src.Stage)
        , LastSuccessStage(src.LastSuccessStage)
        , Line()
        , Header(Line)
        , ChunkLength(src.ChunkLength)
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
                target = TrimEnd(target, delim[i]);
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

    bool ProcessHeaderValue(TStringBuf& header) {
        TStringBuf name;
        TStringBuf value;
        if (!header.TrySplit(':', name, value)) {
            return false;
        }
        name = TrimBegin(name, ' ');
        value = Trim(value, ' ');
        auto cit = HeaderType::HeadersLocation.find(name);
        if (cit != HeaderType::HeadersLocation.end()) {
            this->*cit->second = value;
        }
        header.Clear();
        return true;
    }

    void ProcessHeader(TStringBuf& data) {
        if (ProcessData(Header, data, "\r\n", MaxHeaderSize)) {
            if (Header.empty()) {
                if (HasBody() && (HeaderType::ContentLength.empty() || HeaderType::ContentLength != "0")) {
                    Stage = EParseStage::Body;
                } else if (TotalSize.has_value() && !data.empty()) {
                    Stage = EParseStage::Body;
                } else {
                    Stage = EParseStage::Done;
                }
                HeaderType::Headers = TStringBuf(HeaderType::Headers.data(), data.data() - HeaderType::Headers.data());
            } else if (!ProcessHeaderValue(Header)) {
                Stage = EParseStage::Error;
            }
        }
    }

    void ProcessBody(TStringBuf& data) {
        if (IsChunkedEncoding()) {
            Stage = EParseStage::ChunkLength;
            Line = {};
        } else if (!HeaderType::ContentLength.empty()) {
            if (is_not_number(HeaderType::ContentLength)) {
                // Invalid content length
                Stage = EParseStage::Error;
            } else if (ProcessData(HeaderType::Body, data, FromStringWithDefault(HeaderType::ContentLength, 0))) {
                Stage = EParseStage::Done;
                if (HeaderType::Body && HeaderType::ContentEncoding == "deflate") {
                    Content = DecompressDeflate(HeaderType::Body);
                    HeaderType::Body = Content;
                }
            }
        } else if (TotalSize.has_value()) {
            if (ProcessData(Content, data, GetBodySizeFromTotalSize())) {
                HeaderType::Body = Content;
                Stage = EParseStage::Done;
                if (HeaderType::Body && HeaderType::ContentEncoding == "deflate") {
                    Content = DecompressDeflate(HeaderType::Body);
                    HeaderType::Body = Content;
                }
            }
        } else {
            // Invalid body encoding
            Stage = EParseStage::Error;
        }
    }

    void ProcessChunkLength(TStringBuf& data) {
        if (ProcessData(Line, data, "\r\n", MaxChunkLengthSize)) {
            if (!Line.empty()) {
                ChunkLength = ParseHex(Line);
                if (ChunkLength <= MaxChunkSize) {
                    if (Content.size() + ChunkLength <= MaxChunkContentSize) {
                        Stage = EParseStage::ChunkData;
                        Line = {}; // clear line for chunk data
                        if (Streaming) {
                            HeaderType::Body = Content = {};
                        }
                    } else {
                        // Invalid chunk content length
                        Stage = EParseStage::Error;
                    }
                } else {
                    // Invalid chunk length
                    Stage = EParseStage::Error;
                }
            } else {
                // Invalid body encoding
                Stage = EParseStage::Error;
            }
        }
    }

    void ProcessChunkData(TStringBuf& data) {
        if (ProcessData(Line, data, ChunkLength + 2)) {
            if (Line.ends_with("\r\n")) {
                Line.remove_suffix(2); // remove trailing \r\n
                if (ChunkLength == 0) {
                    Stage = EParseStage::Done;
                } else {
                    // append chunk data to content
                    if (HeaderType::ContentEncoding == "deflate") {
                        HeaderType::Body = Content += DecompressDeflate(Line);
                    } else {
                        if (HeaderType::Body.empty()) {
                            HeaderType::Body = Line;
                        } else {
                            HeaderType::Body = Content = TString(HeaderType::Body) + Line;
                        }
                    }
                    Stage = EParseStage::ChunkLength;
                    Line = {}; // clear line for next chunk
                }
            } else {
                // Invalid chunk data
                Stage = EParseStage::Error;
            }
        }
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

    [[nodiscard]] size_t AdvancePartial(size_t len);

    void Advance(size_t len) {
        while (len > 0) {
            len -= AdvancePartial(len);
        }
    }

    void TruncateToHeaders() {
        if (HasHeaders()) {
            auto begin = Data();
            auto end = Data() + Size();
            auto desiredEnd = HeaderType::Headers.data() + HeaderType::Headers.size();
            if (begin < desiredEnd && desiredEnd < end) {
                Resize(desiredEnd - begin);
            }
        }
    }

    void ConnectionClosed();

    size_t GetHeadersSize() const { // including request line
        if (HeaderType::Headers.empty()) {
            return TSocketBuffer::Size();
        }
        return HeaderType::Headers.end() - TSocketBuffer::Data();
    }

    size_t GetBodySizeFromTotalSize() const {
        return TotalSize.value() - GetHeadersSize();
    }

    void Clear() {
        TSocketBuffer::Clear();
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

    bool IsStartOfChunk() const {
        return Stage == EParseStage::ChunkLength;
    }

    bool HasNewStreamingDataChunk() const {
        return Streaming && IsStartOfChunk() && !HeaderType::Body.empty();
    }

    TString ExtractDataChunk() {
        TString chunk;
        if (!Content.empty()) {
            chunk = std::move(Content);
            Content.clear();
        } else {
            chunk = TString(HeaderType::Body);
        }
        HeaderType::Body = {};
        return chunk;
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

    bool HasBody() const;
    bool ExpectedBody() const;

    bool HasHeaders() const {
        switch (Stage) {
        case EParseStage::Header:
        case EParseStage::Body:
        case EParseStage::ChunkLength:
        case EParseStage::ChunkData:
        case EParseStage::Done:
            return true;
        default:
            return false;
        }
    }

    bool HasCompletedHeaders() const {
        switch (Stage) {
        case EParseStage::Body:
        case EParseStage::ChunkLength:
        case EParseStage::ChunkData:
        case EParseStage::Done:
            return true;
        default:
            return false;
        }
    }

    bool HaveBody() const { return HasBody(); } // deprecated, use HasBody() instead

    bool IsChunkedEncoding() const {
        return TEqNoCase()(HeaderType::TransferEncoding, "chunked");
    }

    // switch to streaming mode, i.e. we will not collect all data in one buffer.
    // instead we expect to receive data chunk by chunk. every chunk overwrites the previous one.
    void SwitchToStreaming() {
        Streaming = true;
    }

    bool EnsureEnoughSpaceAvailable(size_t need = TSocketBuffer::BUFFER_MIN_STEP) {
        bool result = TSocketBuffer::EnsureEnoughSpaceAvailable(need);
        if (!result && !TSocketBuffer::Empty()) {
            Reparse();
        }
        return true;
    }

    void Reparse() {
        size_t size = TSocketBuffer::Size();
        Clear();
        Advance(size);
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
        TSocketBuffer::Assign(data.data(), data.size());
        TSocketBuffer::Clear(); // reset position to 0
        TotalSize = data.size();
        Advance(data.size());
    }

    TString AsReadableString() const {
        if (IsReadableContent(HeaderType::ContentType)) {
            return TString(Data(), GetHeadersSize()) + HeaderType::Body;
        } else {
            return TString(Data(), GetHeadersSize());
        }
    }

    TString GetObfuscatedData() const {
        return NHttp::GetObfuscatedData(AsReadableString(), HeaderType::Headers);
    }
};

template<typename HeaderType>
class THttpRenderer : public HeaderType, public TSocketBuffer {
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
        HeaderType::Headers = TStringBuf(TSocketBuffer::Pos(), size_t(0));
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
        HeaderType::Headers = TStringBuf(TSocketBuffer::Pos(), size_t(0));
    }

    void Append(TStringBuf text) {
        EnsureEnoughSpaceAvailable(text.size());
        TSocketBuffer::Append(text.data(), text.size());
    }

    void Append(char c) {
        EnsureEnoughSpaceAvailable(sizeof(c));
        TSocketBuffer::Append(c);
    }

    template <TStringBuf HeaderType::* string>
    void AppendParsedValue(TStringBuf value) {
        Append(value);
        static_cast<HeaderType*>(this)->*string = TStringBuf(TSocketBuffer::Pos() - value.size(), value.size());
    }

    template <TStringBuf HeaderType::* name>
    void Set(TStringBuf value) {
        Y_DEBUG_ABORT_UNLESS(Stage == ERenderStage::Header);
        Append(HeaderType::template GetName<name>());
        Append(": ");
        AppendParsedValue<name>(value);
        Append("\r\n");
        HeaderType::Headers = TStringBuf(HeaderType::Headers.data(), TSocketBuffer::Pos() - HeaderType::Headers.data());
    }

    void Set(TStringBuf name, TStringBuf value) {
        Y_DEBUG_ABORT_UNLESS(Stage == ERenderStage::Header);
        EnsureEnoughSpaceAvailable(name.size() + 2 + value.size() + 2);
        Append(name);
        Append(": ");
        Append(value);
        auto cit = HeaderType::HeadersLocation.find(name);
        if (cit != HeaderType::HeadersLocation.end()) {
            (this->*cit->second) = TStringBuf(TSocketBuffer::Pos() - value.size(), TSocketBuffer::Pos());
        }
        Append("\r\n");
        HeaderType::Headers = TStringBuf(HeaderType::Headers.data(), TSocketBuffer::Pos() - HeaderType::Headers.data());
    }

    void Set(const THeaders& headers) {
        Y_DEBUG_ABORT_UNLESS(Stage == ERenderStage::Header);
        for (const auto& [name, value] : headers.Headers) {
            Set(name, value);
        }
        HeaderType::Headers = TStringBuf(HeaderType::Headers.data(), TSocketBuffer::Pos() - HeaderType::Headers.data());
    }

    static constexpr TStringBuf ALLOWED_CONTENT_ENCODINGS[] = {"deflate"};

    void SetContentEncoding(TStringBuf contentEncoding) {
        Y_DEBUG_ABORT_UNLESS(Stage == ERenderStage::Header);
        if (Count(ALLOWED_CONTENT_ENCODINGS, contentEncoding) != 0) {
            Set("Content-Encoding", contentEncoding);
        }
    }

    bool IsChunkedEncoding() const {
        return TEqNoCase()(HeaderType::TransferEncoding, "chunked");
    }

    void FinishHeader() {
        Append("\r\n");
        HeaderType::Headers = TStringBuf(HeaderType::Headers.data(), TSocketBuffer::Pos() - HeaderType::Headers.data());
        Stage = ERenderStage::Body;
    }

    size_t GetHeadersSize() const { // including request line
        if (HeaderType::Headers.empty()) {
            return TSocketBuffer::Size();
        }
        return HeaderType::Headers.end() - TSocketBuffer::Data();
    }

    void SetBody(TStringBuf body) {
        Y_DEBUG_ABORT_UNLESS(Stage == ERenderStage::Header);
        if (HeaderType::ContentLength.empty()) {
            Set<&HeaderType::ContentLength>(ToString(body.size()));
        }
        FinishHeader();
        bool chunkedEncoding = IsChunkedEncoding();
        if (chunkedEncoding) {
            Append(ToHex(body.size()) + "\r\n");
        }
        AppendParsedValue<&HeaderType::Body>(body);
        if (chunkedEncoding) {
            Append("\r\n");
        }
        Stage = ERenderStage::Done;
    }

    void FinishBody() {
        Stage = ERenderStage::Done;
    }

    bool IsDone() const {
        return Stage == ERenderStage::Done;
    }

    void Finish() { // do not use when initiating chunked transfer
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

    bool EnsureEnoughSpaceAvailable(size_t need = TSocketBuffer::BUFFER_MIN_STEP) {
        bool result = TSocketBuffer::EnsureEnoughSpaceAvailable(need);
        if (!result && !TSocketBuffer::Empty()) {
            Reparse();
        }
        return true;
    }

    void Clear() {
        TSocketBuffer::Clear();
        HeaderType::Clear();
    }

    void Reparse() {
        // move-magic
        size_t size = TSocketBuffer::Size();
        THttpParser<HeaderType> parser;
        // move the buffer to parser
        static_cast<TSocketBuffer&>(parser) = std::move(static_cast<TSocketBuffer&>(*this));
        // reparse
        parser.Clear();
        parser.Advance(size);
        // move buffer and result back
        bool needReassignBody = (parser.Body.data() == parser.Content.data());
        static_cast<HeaderType&>(*this) = std::move(static_cast<HeaderType&>(parser));
        static_cast<TSocketBuffer&>(*this) = std::move(static_cast<TSocketBuffer&>(parser));
        if (needReassignBody) {
            Content = std::move(parser.Content);
            HeaderType::Body = Content;
        }
        switch (parser.Stage) {
        case THttpParser<HeaderType>::EParseStage::Method:
        case THttpParser<HeaderType>::EParseStage::URL:
        case THttpParser<HeaderType>::EParseStage::Protocol:
        case THttpParser<HeaderType>::EParseStage::Version:
        case THttpParser<HeaderType>::EParseStage::Status:
        case THttpParser<HeaderType>::EParseStage::Message:
            Stage = ERenderStage::Init;
            break;
        case THttpParser<HeaderType>::EParseStage::Header:
            Stage = ERenderStage::Header;
            break;
        case THttpParser<HeaderType>::EParseStage::Body:
        case THttpParser<HeaderType>::EParseStage::ChunkLength:
        case THttpParser<HeaderType>::EParseStage::ChunkData:
            Stage = ERenderStage::Body;
            break;
        case THttpParser<HeaderType>::EParseStage::Done:
            Stage = ERenderStage::Done;
            break;
        case THttpParser<HeaderType>::EParseStage::Error:
            Stage = ERenderStage::Error;
            break;
        }
        Y_ABORT_UNLESS(size == TSocketBuffer::Size());
    }

    TString AsReadableString() const {
        if (IsReadableContent(HeaderType::ContentType)) {
            return TString(Data(), GetHeadersSize()) + HeaderType::Body;
        } else {
            return TString(Data(), GetHeadersSize());
        }
    }

    TString GetObfuscatedData() const {
        return NHttp::GetObfuscatedData(AsReadableString(), HeaderType::Headers);
    }
};

using THttpRequestParser = THttpParser<THttpRequest>;
using THttpResponseParser = THttpParser<THttpResponse>;
using THttpRequestRenderer = THttpRenderer<THttpRequest>;
using THttpResponseRenderer = THttpRenderer<THttpResponse>;

template <>
template <>
inline void THttpResponseRenderer::Set<&THttpResponse::Body>(TStringBuf value) {
    SetBody(value);
}

template <>
template <>
inline void THttpRequestRenderer::Set<&THttpRequest::Body>(TStringBuf value) {
    SetBody(value);
}

template <>
template <>
inline void THttpResponseRenderer::Set<&THttpResponse::ContentEncoding>(TStringBuf value) {
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

class THttpDataChunk : public TSocketBuffer {
public:
    bool EndOfData = false;
    size_t DataSize = 0;

    THttpDataChunk() = default;

    bool EnsureEnoughSpaceAvailable(size_t need = TSocketBuffer::BUFFER_MIN_STEP) {
        return TSocketBuffer::EnsureEnoughSpaceAvailable(need);
    }

    void Append(TStringBuf text) {
        EnsureEnoughSpaceAvailable(text.size());
        TSocketBuffer::Append(text.data(), text.size());
    }

    bool IsEndOfData() const {
        return EndOfData;
    }

    void SetData(TStringBuf data) {
        TSocketBuffer::Clear();
        EndOfData = false;
        DataSize = data.size();
        EnsureEnoughSpaceAvailable(DataSize + 4/*crlfcrlf*/ + 16);
        Append(ToHex(DataSize) + "\r\n");
        Append(TStringBuf(data));
        Append("\r\n");
    }

    void SetEndOfData() {
        if (!IsEndOfData()) {
            Append("0\r\n\r\n");
            EndOfData = true;
        }
    }
};

class THttpOutgoingDataChunk;
using THttpOutgoingDataChunkPtr = TIntrusivePtr<THttpOutgoingDataChunk>;

class THttpIncomingRequest;
using THttpIncomingRequestPtr = TIntrusivePtr<THttpIncomingRequest>;

class THttpOutgoingResponse;
using THttpOutgoingResponsePtr = TIntrusivePtr<THttpOutgoingResponse>;

class THttpOutgoingRequest;
using THttpOutgoingRequestPtr = TIntrusivePtr<THttpOutgoingRequest>;

class THttpIncomingResponse;
using THttpIncomingResponsePtr = TIntrusivePtr<THttpIncomingResponse>;

class THttpIncomingRequest :
        public THttpRequestParser,
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
    THttpOutgoingResponsePtr CreateResponseTooManyRequests(TStringBuf html = TStringBuf(), TStringBuf contentType = "text/html"); // 429
    THttpOutgoingResponsePtr CreateResponseServiceUnavailable(TStringBuf html = TStringBuf(), TStringBuf contentType = "text/html"); // 503
    THttpOutgoingResponsePtr CreateResponseGatewayTimeout(TStringBuf html = TStringBuf(), TStringBuf contentType = "text/html"); // 504
    THttpOutgoingResponsePtr CreateResponseTemporaryRedirect(TStringBuf location); // 307
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

class THttpOutgoingRequest :
        public THttpRequestRenderer,
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

    bool IsConnectionClose() const {
        return TEqNoCase()(Connection, "close");
    }

    TString GetDestination() {
        return Secure ? (TStringBuilder() << "https://" << Host) : (TStringBuilder() << "http://" << Host);
    }
};

class THttpIncomingResponse :
        public THttpResponseParser,
        public TRefCounted<THttpIncomingResponse, TAtomicCounter> {
public:
    THttpIncomingResponse(THttpOutgoingRequestPtr request);

    THttpOutgoingRequestPtr GetRequest() const {
        return Request;
    }

    THttpIncomingResponsePtr Duplicate(THttpOutgoingRequestPtr request);
    THttpOutgoingResponsePtr Reverse(THttpIncomingRequestPtr request);

    bool IsConnectionClose() const {
        return Request->IsConnectionClose() || TEqNoCase()(Connection, "close");
    }

protected:
    THttpOutgoingRequestPtr Request;
};

class THttpOutgoingResponse :
        public THttpResponseRenderer,
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
        return GetRequest()->Method != "HEAD" && Status != "204" && Status != "202";
    }

    bool EnableCompression() {
        TStringBuf acceptEncoding = Request->AcceptEncoding;
        std::vector<TStringBuf> encodings;
        TStringBuf encoding;
        while (acceptEncoding.NextTok(',', encoding)) {
            encoding = Trim(encoding, ' ');
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
            THttpRenderer<THttpResponse>::SetBody(compressedBody);
            Body = Content = body;
        } else {
            THttpRenderer<THttpResponse>::SetBody(body);
        }
    }

    void SetBody(const TString& body) {
        if (ContentEncoding == "deflate") {
            TString compressedBody = CompressDeflate(body);
            THttpRenderer<THttpResponse>::SetBody(compressedBody);
            Body = Content = body;
        } else {
            THttpRenderer<THttpResponse>::SetBody(body);
        }
    }

    THttpIncomingRequestPtr GetRequest() const {
        return Request;
    }

    THttpOutgoingResponsePtr Duplicate(THttpIncomingRequestPtr request);
    THttpOutgoingDataChunkPtr CreateDataChunk(TStringBuf data = {}); // empty chunk means end of data
    THttpOutgoingDataChunkPtr CreateIncompleteDataChunk(); // to construct it later

    TSocketBuffer* GetActiveBuffer();
    void AddDataChunk(THttpOutgoingDataChunkPtr dataChunk);

// it's temporary accessible for cleanup
//protected:
    THttpIncomingRequestPtr Request;
    std::deque<THttpOutgoingDataChunkPtr> DataChunks;
    std::unique_ptr<TSensors> Sensors;
};

class THttpOutgoingDataChunk :
        public THttpDataChunk,
        public TRefCounted<THttpOutgoingDataChunk, TAtomicCounter> {
public:
    THttpOutgoingDataChunk(THttpOutgoingResponsePtr response, TStringBuf data);
    THttpOutgoingDataChunk(THttpOutgoingResponsePtr response); // incomplete chunk

    THttpOutgoingResponsePtr GetResponse() const {
        return Response;
    }

    THttpIncomingRequestPtr GetRequest() const {
        return Response->GetRequest();
    }

protected:
    THttpOutgoingResponsePtr Response;
};

}
