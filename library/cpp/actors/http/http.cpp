#include "http.h"
#include <library/cpp/string_utils/quote/quote.h>

inline TStringBuf operator +(TStringBuf l, TStringBuf r) {
    if (l.empty()) {
        return r;
    }
    if (r.empty()) {
        return l;
    }
    if (l.end() == r.begin()) {
        return TStringBuf(l.data(), l.size() + r.size());
    }
    if (r.end() == l.begin()) {
        return TStringBuf(r.data(), l.size() + r.size());
    }
    Y_FAIL("oops");
    return TStringBuf();
}

inline TStringBuf operator +=(TStringBuf& l, TStringBuf r) {
    return l = l + r;
}

namespace NHttp {

template <> TStringBuf THttpRequest::GetName<&THttpRequest::Host>() { return "Host"; }
template <> TStringBuf THttpRequest::GetName<&THttpRequest::Accept>() { return "Accept"; }
template <> TStringBuf THttpRequest::GetName<&THttpRequest::Connection>() { return "Connection"; }
template <> TStringBuf THttpRequest::GetName<&THttpRequest::ContentType>() { return "Content-Type"; }
template <> TStringBuf THttpRequest::GetName<&THttpRequest::ContentLength>() { return "Content-Length"; }
template <> TStringBuf THttpRequest::GetName<&THttpRequest::TransferEncoding>() { return "Transfer-Encoding"; }

const TMap<TStringBuf, TStringBuf THttpRequest::*, TLessNoCase> THttpRequest::HeadersLocation = {
    { THttpRequest::GetName<&THttpRequest::Host>(), &THttpRequest::Host },
    { THttpRequest::GetName<&THttpRequest::Accept>(), &THttpRequest::Accept },
    { THttpRequest::GetName<&THttpRequest::Connection>(), &THttpRequest::Connection },
    { THttpRequest::GetName<&THttpRequest::ContentType>(), &THttpRequest::ContentType },
    { THttpRequest::GetName<&THttpRequest::ContentLength>(), &THttpRequest::ContentLength },
    { THttpRequest::GetName<&THttpRequest::TransferEncoding>(), &THttpRequest::TransferEncoding },
};

template <> TStringBuf THttpResponse::GetName<&THttpResponse::Connection>() { return "Connection"; }
template <> TStringBuf THttpResponse::GetName<&THttpResponse::ContentType>() { return "Content-Type"; }
template <> TStringBuf THttpResponse::GetName<&THttpResponse::ContentLength>() { return "Content-Length"; }
template <> TStringBuf THttpResponse::GetName<&THttpResponse::TransferEncoding>() { return "Transfer-Encoding"; }
template <> TStringBuf THttpResponse::GetName<&THttpResponse::LastModified>() { return "Last-Modified"; }
template <> TStringBuf THttpResponse::GetName<&THttpResponse::ContentEncoding>() { return "Content-Encoding"; }

const TMap<TStringBuf, TStringBuf THttpResponse::*, TLessNoCase> THttpResponse::HeadersLocation = {
    { THttpResponse::GetName<&THttpResponse::Connection>(), &THttpResponse::Connection },
    { THttpResponse::GetName<&THttpResponse::ContentType>(), &THttpResponse::ContentType },
    { THttpResponse::GetName<&THttpResponse::ContentLength>(), &THttpResponse::ContentLength },
    { THttpResponse::GetName<&THttpResponse::TransferEncoding>(), &THttpResponse::TransferEncoding },
    { THttpResponse::GetName<&THttpResponse::LastModified>(), &THttpResponse::LastModified },
    { THttpResponse::GetName<&THttpResponse::ContentEncoding>(), &THttpResponse::ContentEncoding }
};

void THttpRequest::Clear() {
    // a dirty little trick
    this->~THttpRequest(); // basically, do nothing
    new (this) THttpRequest(); // reset all fields
}

template <>
void THttpParser<THttpRequest, TSocketBuffer>::Advance(size_t len) {
    TStringBuf data(Pos(), len);
    while (!data.empty()) {
        if (Stage != EParseStage::Error) {
            LastSuccessStage = Stage;
        }
        switch (Stage) {
            case EParseStage::Method: {
                if (ProcessData(Method, data, ' ', MaxMethodSize)) {
                    Stage = EParseStage::URL;
                }
                break;
            }
            case EParseStage::URL: {
                if (ProcessData(URL, data, ' ', MaxURLSize)) {
                    Stage = EParseStage::Protocol;
                }
                break;
            }
            case EParseStage::Protocol: {
                if (ProcessData(Protocol, data, '/', MaxProtocolSize)) {
                    Stage = EParseStage::Version;
                }
                break;
            }
            case EParseStage::Version: {
                if (ProcessData(Version, data, "\r\n", MaxVersionSize)) {
                    Stage = EParseStage::Header;
                    Headers = data;
                }
                break;
            }
            case EParseStage::Header: {
                if (ProcessData(Header, data, "\r\n", MaxHeaderSize)) {
                    if (Header.empty()) {
                        Headers = TStringBuf(Headers.data(), data.begin() - Headers.begin());
                        if (HaveBody()) {
                            Stage = EParseStage::Body;
                        } else {
                            Stage = EParseStage::Done;
                        }
                    } else {
                        ProcessHeader(Header);
                    }
                }
                break;
            }
            case EParseStage::Body: {
                if (!ContentLength.empty()) { 
                    if (ProcessData(Content, data, FromString(ContentLength))) { 
                        Body = Content; 
                        Stage = EParseStage::Done; 
                    } 
                } else if (TransferEncoding == "chunked") { 
                    Stage = EParseStage::ChunkLength; 
                } else { 
                    // Invalid body encoding 
                    Stage = EParseStage::Error; 
                }
                break;
            }
            case EParseStage::ChunkLength: { 
                if (ProcessData(Line, data, "\r\n", MaxChunkLengthSize)) { 
                    if (!Line.empty()) { 
                        ChunkLength = ParseHex(Line); 
                        if (ChunkLength <= MaxChunkSize) { 
                            ContentSize = Content.size() + ChunkLength; 
                            if (ContentSize <= MaxChunkContentSize) { 
                                Stage = EParseStage::ChunkData; 
                                Line.Clear(); 
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
                break; 
            } 
            case EParseStage::ChunkData: { 
                if (!IsError()) { 
                    if (ProcessData(Content, data, ContentSize)) { 
                        if (ProcessData(Line, data, 2)) { 
                            if (Line == "\r\n") { 
                                if (ChunkLength == 0) { 
                                    Body = Content; 
                                    Stage = EParseStage::Done; 
                                } else { 
                                    Stage = EParseStage::ChunkLength; 
                                } 
                                Line.Clear(); 
                            } else { 
                                // Invalid body encoding 
                                Stage = EParseStage::Error; 
                            } 
                        } 
                    } 
                } 
                break; 
            } 
 
            case EParseStage::Done:
            case EParseStage::Error: {
                data.Clear();
                break;
            }
            default:
                Y_FAIL("Invalid processing sequence");
                break;
        }
    }
    TSocketBuffer::Advance(len);
}

template <>
THttpParser<THttpRequest, TSocketBuffer>::EParseStage THttpParser<THttpRequest, TSocketBuffer>::GetInitialStage() {
    return EParseStage::Method;
}

template <>
THttpParser<THttpResponse, TSocketBuffer>::EParseStage THttpParser<THttpResponse, TSocketBuffer>::GetInitialStage() {
    return EParseStage::Protocol;
}

void THttpResponse::Clear() {
    // a dirty little trick
    this->~THttpResponse(); // basically, do nothing
    new (this) THttpResponse(); // reset all fields
}

template <>
void THttpParser<THttpResponse, TSocketBuffer>::Advance(size_t len) {
    TStringBuf data(Pos(), len);
    while (!data.empty()) {
        if (Stage != EParseStage::Error) {
            LastSuccessStage = Stage;
        }
        switch (Stage) {
            case EParseStage::Protocol: {
                if (ProcessData(Protocol, data, '/', MaxProtocolSize)) {
                    Stage = EParseStage::Version;
                }
                break;
            }
            case EParseStage::Version: {
                if (ProcessData(Version, data, ' ', MaxVersionSize)) {
                    Stage = EParseStage::Status;
                }
                break;
            }
            case EParseStage::Status: {
                if (ProcessData(Status, data, ' ', MaxStatusSize)) {
                    Stage = EParseStage::Message;
                }
                break;
            }
            case EParseStage::Message: {
                if (ProcessData(Message, data, "\r\n", MaxMessageSize)) {
                    Stage = EParseStage::Header;
                    Headers = TStringBuf(data.data(), size_t(0));
                }
                break;
            }
            case EParseStage::Header: {
                if (ProcessData(Header, data, "\r\n", MaxHeaderSize)) {
                    if (Header.empty()) {
                        if (HaveBody() && (ContentLength.empty() || ContentLength != "0")) {
                            Stage = EParseStage::Body;
                        } else {
                            Stage = EParseStage::Done;
                        }
                    } else {
                        ProcessHeader(Header);
                    }
                    Headers = TStringBuf(Headers.data(), data.data() - Headers.data());
                }
                break;
            }
            case EParseStage::Body: {
                if (!ContentLength.empty()) {
                    if (ProcessData(Body, data, FromString(ContentLength))) {
                        Stage = EParseStage::Done;
                    }
                } else if (TransferEncoding == "chunked") {
                    Stage = EParseStage::ChunkLength;
                } else {
                    // Invalid body encoding
                    Stage = EParseStage::Error;
                }
                break;
            }
            case EParseStage::ChunkLength: {
                if (ProcessData(Line, data, "\r\n", MaxChunkLengthSize)) {
                    if (!Line.empty()) {
                        ChunkLength = ParseHex(Line);
                        if (ChunkLength <= MaxChunkSize) {
                            ContentSize = Content.size() + ChunkLength;
                            if (ContentSize <= MaxChunkContentSize) {
                                Stage = EParseStage::ChunkData;
                                Line.Clear();
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
                break;
            }
            case EParseStage::ChunkData: {
                if (!IsError()) {
                    if (ProcessData(Content, data, ContentSize)) {
                        if (ProcessData(Line, data, 2)) {
                            if (Line == "\r\n") {
                                if (ChunkLength == 0) {
                                    Body = Content;
                                    Stage = EParseStage::Done;
                                } else {
                                    Stage = EParseStage::ChunkLength;
                                }
                                Line.Clear();
                            } else {
                                // Invalid body encoding
                                Stage = EParseStage::Error;
                            }
                        }
                    }
                }
                break;
            }
            case EParseStage::Done:
            case EParseStage::Error:
                data.Clear();
                break;
            default:
                // Invalid processing sequence
                Stage = EParseStage::Error;
                break;
        }
    }
    TSocketBuffer::Advance(len);
}

template <>
void THttpParser<THttpResponse, TSocketBuffer>::ConnectionClosed() {
    if (Stage == EParseStage::Done) {
        return;
    }
    if (Stage == EParseStage::Body) {
        // ?
        Stage = EParseStage::Done;
    } else {
        LastSuccessStage = Stage;
        Stage = EParseStage::Error;
    }
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponseString(TStringBuf data) {
    THttpOutgoingResponsePtr response = new THttpOutgoingResponse(this);
    response->Append(data);
    response->Reparse();
    return response;
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponseOK(TStringBuf body, TStringBuf contentType, TInstant lastModified) {
    return CreateResponse("200", "OK", contentType, body, lastModified);
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponseBadRequest(TStringBuf html, TStringBuf contentType) {
    if (html.empty() && IsError()) {
        contentType = "text/plain";
        html = GetErrorText();
    }
    return CreateResponse("400", "Bad Request", contentType, html);
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponseNotFound(TStringBuf html, TStringBuf contentType) {
    return CreateResponse("404", "Not Found", contentType, html);
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponseServiceUnavailable(TStringBuf html, TStringBuf contentType) {
    return CreateResponse("503", "Service Unavailable", contentType, html);
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponseGatewayTimeout(TStringBuf html, TStringBuf contentType) {
    return CreateResponse("504", "Gateway Timeout", contentType, html);
}

THttpIncomingResponse::THttpIncomingResponse(THttpOutgoingRequestPtr request)
    : Request(request)
{}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponse(TStringBuf status, TStringBuf message, TStringBuf contentType, TStringBuf body, TInstant lastModified) {
    TStringBuf version = Version;
    if (version != "1.0" && version != "1.1") {
        version = "1.1";
    }
    THttpOutgoingResponsePtr response = new THttpOutgoingResponse(this, "HTTP", version, status, message);
    response->Set<&THttpResponse::Connection>(GetConnection());
    if (!WorkerName.empty()) {
        response->Set("X-Worker-Name", WorkerName);
    }
    if (!contentType.empty() && !body.empty()) {
        response->Set<&THttpResponse::ContentType>(contentType);
    }
    if (lastModified) {
        response->Set<&THttpResponse::LastModified>(lastModified.FormatGmTime("%a, %d %b %Y %H:%M:%S GMT"));
    }
    if (response->IsNeedBody() || !body.empty()) {
        if (Method == "HEAD") {
            response->Set<&THttpResponse::ContentLength>(ToString(body.size()));
        } else {
            response->Set<&THttpResponse::Body>(body);
        }
    }
    return response;
}

THttpIncomingRequestPtr THttpIncomingRequest::Duplicate() {
    THttpIncomingRequestPtr request = new THttpIncomingRequest(*this);
    request->Reparse();
    request->Timer.Reset();
    return request;
}

THttpIncomingResponsePtr THttpIncomingResponse::Duplicate(THttpOutgoingRequestPtr request) {
    THttpIncomingResponsePtr response = new THttpIncomingResponse(*this);
    response->Reparse();
    response->Request = request;
    return response;
}

THttpOutgoingResponsePtr THttpOutgoingResponse::Duplicate(THttpIncomingRequestPtr request) {
    THttpOutgoingResponsePtr response = new THttpOutgoingResponse(*this);
    response->Reparse();
    response->Request = request;
    return response;
}


THttpOutgoingResponsePtr THttpIncomingResponse::Reverse(THttpIncomingRequestPtr request) {
    THttpOutgoingResponsePtr response = new THttpOutgoingResponse(request);
    response->Assign(Data(), Size());
    response->Reparse();
    return response;
}

THttpOutgoingRequest::THttpOutgoingRequest(TStringBuf method, TStringBuf scheme, TStringBuf host, TStringBuf uri, TStringBuf protocol, TStringBuf version) {
    Secure = (scheme == "https");
    TString urie = UrlEscapeRet(uri);
    InitRequest(method, urie, protocol, version);
    if (host) {
        Set<&THttpRequest::Host>(host);
    }
}

THttpOutgoingRequest::THttpOutgoingRequest(TStringBuf method, TStringBuf url, TStringBuf protocol, TStringBuf version) {
    TStringBuf scheme, host, uri;
    if (!CrackURL(url, scheme, host, uri)) {
        Y_FAIL("Invalid URL specified");
    }
    if (!scheme.empty() && scheme != "http" && scheme != "https") {
        Y_FAIL("Invalid URL specified");
    }
    Secure = (scheme == "https");
    TString urie = UrlEscapeRet(uri);
    InitRequest(method, urie, protocol, version);
    if (host) {
        Set<&THttpRequest::Host>(host);
    }
}

THttpOutgoingRequestPtr THttpOutgoingRequest::CreateRequestString(const TString& data) {
    THttpOutgoingRequestPtr request = new THttpOutgoingRequest();
    request->Assign(data.data(), data.size());
    request->Reparse();
    return request;
}

THttpOutgoingRequestPtr THttpOutgoingRequest::CreateRequestGet(TStringBuf url) {
    return CreateRequest("GET", url);
}

THttpOutgoingRequestPtr THttpOutgoingRequest::CreateRequestGet(TStringBuf host, TStringBuf uri) {
    return CreateHttpRequest("GET", host, uri);
}

THttpOutgoingRequestPtr THttpOutgoingRequest::CreateRequestPost(TStringBuf url, TStringBuf contentType, TStringBuf body) {
    return CreateRequest("POST", url, contentType, body);
}

THttpOutgoingRequestPtr THttpOutgoingRequest::CreateRequestPost(TStringBuf host, TStringBuf uri, TStringBuf contentType, TStringBuf body) {
    return CreateHttpRequest("POST", host, uri, contentType, body);
}

THttpOutgoingRequestPtr THttpOutgoingRequest::CreateRequest(TStringBuf method, TStringBuf url, TStringBuf contentType, TStringBuf body) {
    THttpOutgoingRequestPtr request = new THttpOutgoingRequest(method, url, "HTTP", "1.1");
    request->Set<&THttpRequest::Accept>("*/*");
    if (!contentType.empty()) {
        request->Set<&THttpRequest::ContentType>(contentType);
        request->Set<&THttpRequest::Body>(body);
    }
    return request;
}

THttpOutgoingRequestPtr THttpOutgoingRequest::CreateHttpRequest(TStringBuf method, TStringBuf host, TStringBuf uri, TStringBuf contentType, TStringBuf body) {
    THttpOutgoingRequestPtr request = new THttpOutgoingRequest(method, "http", host, uri, "HTTP", "1.1");
    request->Set<&THttpRequest::Accept>("*/*");
    if (!contentType.empty()) {
        request->Set<&THttpRequest::ContentType>(contentType);
        request->Set<&THttpRequest::Body>(body);
    }
    return request;
}

THttpOutgoingRequestPtr THttpOutgoingRequest::Duplicate() {
    THttpOutgoingRequestPtr request = new THttpOutgoingRequest(*this);
    request->Reparse();
    return request;
}

THttpOutgoingResponse::THttpOutgoingResponse(THttpIncomingRequestPtr request)
    : Request(request)
{}

THttpOutgoingResponse::THttpOutgoingResponse(THttpIncomingRequestPtr request, TStringBuf protocol, TStringBuf version, TStringBuf status, TStringBuf message)
    : Request(request)
{
    InitResponse(protocol, version, status, message);
}

const size_t THttpConfig::BUFFER_MIN_STEP;
const TDuration THttpConfig::CONNECTION_TIMEOUT;

TUrlParameters::TUrlParameters(TStringBuf url) {
    TStringBuf base;
    TStringBuf params;
    if (url.TrySplit('?', base, params)) {
        for (TStringBuf param = params.NextTok('&'); !param.empty(); param = params.NextTok('&')) {
            TStringBuf name = param.NextTok('=');
            Parameters[name] = param;
        }
    }
}

TString TUrlParameters::operator [](TStringBuf name) const {
    TString value(Get(name));
    CGIUnescape(value);
    return value;
}

bool TUrlParameters::Has(TStringBuf name) const {
    return Parameters.count(name) != 0;
}

TStringBuf TUrlParameters::Get(TStringBuf name) const {
    auto it = Parameters.find(name);
    if (it != Parameters.end()) {
        return it->second;
    }
    return TStringBuf();
}

TString TUrlParameters::Render() const {
    TStringBuilder parameters;
    for (const std::pair<TStringBuf, TStringBuf> parameter : Parameters) {
        if (parameters.empty()) {
            parameters << '?';
        } else {
            parameters << '&';
        }
        parameters << parameter.first;
        parameters << '=';
        parameters << parameter.second;
    }
    return parameters;
}

TCookies::TCookies(TStringBuf cookie) {
    for (TStringBuf param = cookie.NextTok(';'); !param.empty(); param = cookie.NextTok(';')) {
        param.SkipPrefix(" ");
        TStringBuf name = param.NextTok('=');
        Cookies[name] = param;
    }
}

TStringBuf TCookies::operator [](TStringBuf name) const {
    return Get(name);
}

bool TCookies::Has(TStringBuf name) const {
    return Cookies.count(name) != 0;
}

TStringBuf TCookies::Get(TStringBuf name) const {
    auto it = Cookies.find(name);
    if (it != Cookies.end()) {
        return it->second;
    }
    return TStringBuf();
}

TString TCookies::Render() const {
    TStringBuilder cookies;
    for (const std::pair<TStringBuf, TStringBuf> cookie : Cookies) {
        if (!cookies.empty()) {
            cookies << ' ';
        }
        cookies << cookie.first;
        cookies << '=';
        cookies << cookie.second;
        cookies << ';';
    }
    return cookies;
}

TCookiesBuilder::TCookiesBuilder()
    :TCookies(TStringBuf())
{}

void TCookiesBuilder::Set(TStringBuf name, TStringBuf data) {
    Data.emplace_back(name, data);
    Cookies[Data.back().first] = Data.back().second;
}

THeaders::THeaders(TStringBuf headers) {
    for (TStringBuf param = headers.NextTok("\r\n"); !param.empty(); param = headers.NextTok("\r\n")) {
        TStringBuf name = param.NextTok(":");
        param.SkipPrefix(" ");
        Headers[name] = param;
    }
}

TStringBuf THeaders::operator [](TStringBuf name) const {
    return Get(name);
}

bool THeaders::Has(TStringBuf name) const {
    return Headers.count(name) != 0;
}

TStringBuf THeaders::Get(TStringBuf name) const {
    auto it = Headers.find(name);
    if (it != Headers.end()) {
        return it->second;
    }
    return TStringBuf();
}

TString THeaders::Render() const {
    TStringBuilder headers;
    for (const std::pair<TStringBuf, TStringBuf> header : Headers) {
        headers << header.first;
        headers << ": ";
        headers << header.second;
        headers << "\r\n";
    }
    return headers;
}

THeadersBuilder::THeadersBuilder()
    :THeaders(TStringBuf())
{}

THeadersBuilder::THeadersBuilder(const THeadersBuilder& builder) {
    for (const auto& pr : builder.Headers) {
        Set(pr.first, pr.second);
    }
}

void THeadersBuilder::Set(TStringBuf name, TStringBuf data) {
    Data.emplace_back(name, data);
    Headers[Data.back().first] = Data.back().second;
}

}
