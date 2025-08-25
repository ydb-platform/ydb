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
    Y_ABORT("oops");
    return TStringBuf();
}

inline TStringBuf operator +=(TStringBuf& l, TStringBuf r) {
    return l = l + r;
}

static bool is_not_number(TStringBuf v) {
    return v.empty() || std::find_if_not(v.begin(), v.end(), [](unsigned char c) { return std::isdigit(c); }) != v.end();
}

namespace NHttp {

template <> TStringBuf THttpRequest::GetName<&THttpRequest::Host>() { return "Host"; }
template <> TStringBuf THttpRequest::GetName<&THttpRequest::Accept>() { return "Accept"; }
template <> TStringBuf THttpRequest::GetName<&THttpRequest::Connection>() { return "Connection"; }
template <> TStringBuf THttpRequest::GetName<&THttpRequest::ContentType>() { return "Content-Type"; }
template <> TStringBuf THttpRequest::GetName<&THttpRequest::ContentLength>() { return "Content-Length"; }
template <> TStringBuf THttpRequest::GetName<&THttpRequest::TransferEncoding>() { return "Transfer-Encoding"; }
template <> TStringBuf THttpRequest::GetName<&THttpRequest::AcceptEncoding>() { return "Accept-Encoding"; }
template <> TStringBuf THttpRequest::GetName<&THttpRequest::ContentEncoding>() { return "Content-Encoding"; }

const TMap<TStringBuf, TStringBuf THttpRequest::*, TLessNoCase> THttpRequest::HeadersLocation = {
    { THttpRequest::GetName<&THttpRequest::Host>(), &THttpRequest::Host },
    { THttpRequest::GetName<&THttpRequest::Accept>(), &THttpRequest::Accept },
    { THttpRequest::GetName<&THttpRequest::Connection>(), &THttpRequest::Connection },
    { THttpRequest::GetName<&THttpRequest::ContentType>(), &THttpRequest::ContentType },
    { THttpRequest::GetName<&THttpRequest::ContentLength>(), &THttpRequest::ContentLength },
    { THttpRequest::GetName<&THttpRequest::TransferEncoding>(), &THttpRequest::TransferEncoding },
    { THttpRequest::GetName<&THttpRequest::AcceptEncoding>(), &THttpRequest::AcceptEncoding },
    { THttpRequest::GetName<&THttpRequest::ContentEncoding>(), &THttpRequest::ContentEncoding },
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
    { THttpResponse::GetName<&THttpResponse::ContentEncoding>(), &THttpResponse::ContentEncoding },
};

void THttpRequest::Clear() {
    // a dirty little trick
    this->~THttpRequest(); // basically, do nothing
    new (this) THttpRequest(); // reset all fields
}

TString THttpRequest::GetURL() const {
    return UrlUnescapeRet(URL);
}

TString THttpRequest::GetURI() const {
    return UrlUnescapeRet(URL.Before('?'));
}

TUrlParameters THttpRequest::GetParameters() const {
    return TUrlParameters(URL);
}

template <>
bool THttpParser<THttpRequest>::HasBody() const {
    if (!Body.empty()) {
        return true;
    }
    return !ContentLength.empty() || !TransferEncoding.empty();
}

template <>
size_t THttpParser<THttpRequest>::AdvancePartial(size_t len) {
    TStringBuf data(Pos(), len);
    while (!data.empty()) {
        if (Stage != EParseStage::Error) {
            LastSuccessStage = Stage;
        }
        switch (Stage) {
            case EParseStage::Method:
                if (ProcessData(Method, data, ' ', MaxMethodSize)) {
                    Stage = EParseStage::URL;
                }
                break;
            case EParseStage::URL:
                if (ProcessData(URL, data, ' ', MaxURLSize)) {
                    Stage = EParseStage::Protocol;
                }
                break;
            case EParseStage::Protocol:
                if (ProcessData(Protocol, data, '/', MaxProtocolSize)) {
                    Stage = EParseStage::Version;
                }
                break;
            case EParseStage::Version:
                if (ProcessData(Version, data, "\r\n", MaxVersionSize)) {
                    Stage = EParseStage::Header;
                    Headers = data;
                }
                break;
            case EParseStage::Header:
                ProcessHeader(data);
                if (HasCompletedHeaders()) {
                    return TSocketBuffer::Advance(len - data.size());
                }
                break;
            case EParseStage::Body:
                ProcessBody(data);
                break;
            case EParseStage::ChunkLength:
                ProcessChunkLength(data);
                break;
            case EParseStage::ChunkData:
                ProcessChunkData(data);
                if (HasNewStreamingDataChunk()) {
                    return TSocketBuffer::Advance(len - data.size());
                }
                break;
            case EParseStage::Done:
            case EParseStage::Error:
                data = {};
                break;
            default:
                Y_ABORT("Invalid processing sequence");
                break;
        }
    }
    return TSocketBuffer::Advance(len - data.size());
}

template <>
THttpParser<THttpRequest>::EParseStage THttpParser<THttpRequest>::GetInitialStage() {
    return EParseStage::Method;
}

template <>
bool THttpParser<THttpResponse>::ExpectedBody() const {
    return !Status.starts_with("1") && Status != "204" && Status != "304" && Status != "202";
}

template <>
bool THttpParser<THttpResponse>::HasBody() const {
    if (!Body.empty()) {
        return true;
    }
    return ExpectedBody() && (!ContentType.empty() || !ContentLength.empty() || !TransferEncoding.empty());
}

template <>
THttpParser<THttpResponse>::EParseStage THttpParser<THttpResponse>::GetInitialStage() {
    return EParseStage::Protocol;
}

void THttpResponse::Clear() {
    // a dirty little trick
    this->~THttpResponse(); // basically, do nothing
    new (this) THttpResponse(); // reset all fields
}

template <>
size_t THttpParser<THttpResponse>::AdvancePartial(size_t len) {
    TStringBuf data(Pos(), len);
    while (!data.empty()) {
        if (Stage != EParseStage::Error) {
            LastSuccessStage = Stage;
        }
        switch (Stage) {
            case EParseStage::Protocol:
                if (ProcessData(Protocol, data, '/', MaxProtocolSize)) {
                    Stage = EParseStage::Version;
                }
                break;
            case EParseStage::Version:
                if (ProcessData(Version, data, ' ', MaxVersionSize)) {
                    Stage = EParseStage::Status;
                }
                break;
            case EParseStage::Status:
                if (ProcessData(Status, data, ' ', MaxStatusSize)) {
                    Stage = EParseStage::Message;
                }
                break;
            case EParseStage::Message:
                if (ProcessData(Message, data, "\r\n", MaxMessageSize)) {
                    Stage = EParseStage::Header;
                    Headers = TStringBuf(data.data(), size_t(0));
                }
                break;
            case EParseStage::Header:
                ProcessHeader(data);
                if (HasCompletedHeaders()) {
                    return TSocketBuffer::Advance(len - data.size());
                }
                break;
            case EParseStage::Body:
                ProcessBody(data);
                break;
            case EParseStage::ChunkLength:
                ProcessChunkLength(data);
                break;
            case EParseStage::ChunkData:
                ProcessChunkData(data);
                if (HasNewStreamingDataChunk()) {
                    return TSocketBuffer::Advance(len - data.size());
                }
                break;
            case EParseStage::Done:
            case EParseStage::Error:
                data = {};
                break;
            default:
                // Invalid processing sequence
                Stage = EParseStage::Error;
                break;
        }
    }
    auto advanced = TSocketBuffer::Advance(len - data.size());
    return advanced;
}

template <>
void THttpParser<THttpResponse>::ConnectionClosed() {
    if (Stage == EParseStage::Done) {
        return;
    }
    LastSuccessStage = Stage;
    Stage = EParseStage::Error;
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponseString(TStringBuf data) {
    THttpParser<THttpResponse> parser(data);
    THeadersBuilder headers(parser.Headers);
    if (!headers.Has("X-Worker-Name")) {
        if (!Endpoint->WorkerName.empty()) {
            headers.Set("X-Worker-Name", Endpoint->WorkerName);
        }
    }
    THttpOutgoingResponsePtr response = new THttpOutgoingResponse(this);
    response->InitResponse(parser.Protocol, parser.Version, parser.Status, parser.Message);
    if (parser.IsDone() && parser.HasBody()) {
        if (parser.ContentType && !Endpoint->CompressContentTypes.empty()) {
            TStringBuf contentType = Trim(parser.ContentType.Before(';'), ' ');
            if (Count(Endpoint->CompressContentTypes, contentType) != 0) {
                if (response->EnableCompression()) {
                    headers.Erase("Content-Length"); // we will need new length after compression
                }
            }
        }
        headers.Erase("Transfer-Encoding"); // we erase transfer-encoding because we convert body to content-length
        response->Set(headers);
        response->SetBody(parser.Body);
    } else if (parser.HasHeaders()) {
        response->Set(headers);
        if (parser.ExpectedBody() && !headers.IsChunkedEncoding() && !response->ContentLength) {
            response->Set<&THttpResponse::ContentLength>("0"); // workaround for buggy responses
        }
        if (parser.HasCompletedHeaders()) {
            response->FinishHeader(); // for partial responses (data follows later)
        }
        if (!headers.IsChunkedEncoding()) {
            response->FinishBody();
        }
    }
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

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponseTooManyRequests(TStringBuf html, TStringBuf contentType) {
    return CreateResponse("429", "Too Many Requests", contentType, html);
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponseServiceUnavailable(TStringBuf html, TStringBuf contentType) {
    return CreateResponse("503", "Service Unavailable", contentType, html);
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponseGatewayTimeout(TStringBuf html, TStringBuf contentType) {
    return CreateResponse("504", "Gateway Timeout", contentType, html);
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponseTemporaryRedirect(TStringBuf location) {
    THttpOutgoingResponsePtr response = CreateIncompleteResponse("307", "Temporary redirect");
    response->Set("Location", location);
    FinishResponse(response);
    return response;
}

THttpIncomingResponse::THttpIncomingResponse(THttpOutgoingRequestPtr request)
    : Request(request)
{}

THttpOutgoingResponsePtr THttpIncomingRequest::ConstructResponse(TStringBuf status, TStringBuf message) {
    TStringBuf version = Version;
    if (version != "1.0" && version != "1.1") {
        version = "1.1";
    }
    THttpOutgoingResponsePtr response = new THttpOutgoingResponse(this, "HTTP", version, status, message);
    return response;
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateIncompleteResponse(TStringBuf status, TStringBuf message, const THeaders& headers) {
    THttpOutgoingResponsePtr response = ConstructResponse(status, message);
    if (!headers.Has("Connection")) {
        response->Set<&THttpResponse::Connection>(GetConnection());
    }
    if (!headers.Has("X-Worker-Name")) {
        if (!Endpoint->WorkerName.empty()) {
            response->Set("X-Worker-Name", Endpoint->WorkerName);
        }
    }
    response->Set(headers);
    return response;
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateIncompleteResponse(TStringBuf status, TStringBuf message, const THeaders& headers, TStringBuf body) {
    THttpOutgoingResponsePtr response = CreateIncompleteResponse(status, message, headers);
    if (!response->ContentType.empty() && !body.empty()) {
        if (!Endpoint->CompressContentTypes.empty()) {
            TStringBuf contentType = Trim(response->ContentType.Before(';'), ' ');
            if (Count(Endpoint->CompressContentTypes, contentType) != 0) {
                response->EnableCompression();
            }
        }
    }
    return response;
}

void THttpIncomingRequest::FinishResponse(THttpOutgoingResponsePtr& response, TStringBuf body) {
    if (response->IsNeedBody() || !body.empty()) {
        if (Method == "HEAD") {
            response->Set<&THttpResponse::ContentLength>(ToString(body.size()));
            response->FinishHeader();
            response->FinishBody();
        } else {
            response->SetBody(body);
        }
    } else {
        response->FinishHeader();
        response->FinishBody();
    }
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponse(TStringBuf status, TStringBuf message) {
    THttpOutgoingResponsePtr response = CreateIncompleteResponse(status, message);
    FinishResponse(response);
    return response;
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponse(TStringBuf status, TStringBuf message, const THeaders& headers) {
    THttpOutgoingResponsePtr response = CreateIncompleteResponse(status, message, headers);
    FinishResponse(response);
    return response;
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponse(TStringBuf status, TStringBuf message, const THeaders& headers, TStringBuf body) {
    THttpOutgoingResponsePtr response = CreateIncompleteResponse(status, message, headers, body);
    FinishResponse(response, body);
    return response;
}

THttpOutgoingResponsePtr THttpIncomingRequest::CreateResponse(TStringBuf status, TStringBuf message, TStringBuf contentType, TStringBuf body, TInstant lastModified) {
    NHttp::THeadersBuilder headers;
    if (!contentType.empty() && !body.empty()) {
        headers.Set("Content-Type", contentType);
    }
    if (lastModified) {
        headers.Set("Last-Modified", lastModified.FormatGmTime("%a, %d %b %Y %H:%M:%S GMT"));
    }
    return CreateResponse(status, message, headers, body);
}

THttpOutgoingDataChunkPtr THttpOutgoingResponse::CreateDataChunk(TStringBuf data) {
    return new THttpOutgoingDataChunk(this, data);
}

THttpOutgoingDataChunkPtr THttpOutgoingResponse::CreateIncompleteDataChunk() {
    return new THttpOutgoingDataChunk(this);
}

THttpIncomingRequestPtr THttpIncomingRequest::Duplicate() {
    THttpIncomingRequestPtr request = new THttpIncomingRequest(*this);
    request->Reparse();
    request->Timer.Reset();
    return request;
}

THttpOutgoingRequestPtr THttpIncomingRequest::Forward(TStringBuf baseUrl) const {
    TStringBuf newScheme;
    TStringBuf newHost;
    TStringBuf emptyUri; // it supposed to be be empty
    if (!CrackURL(baseUrl, newScheme, newHost, emptyUri)) {
        // TODO(xenoxeno)
        Y_ABORT("Invalid URL specified");
    }
    THttpOutgoingRequestPtr request = new THttpOutgoingRequest(Method, newScheme, newHost, GetURL(), Protocol, Version);
    THeadersBuilder newHeaders(Headers);
    newHeaders.Erase("Accept-Encoding");
    newHeaders.Erase("Host"); // host being set by THttpOutgoingRequest constructor
    request->Set(newHeaders);
    if (Body) {
        request->SetBody(Body);
    }
    request->Finish();
    return request;
}

THttpIncomingResponsePtr THttpIncomingResponse::Duplicate(THttpOutgoingRequestPtr request) {
    THttpIncomingResponsePtr response = new THttpIncomingResponse(*this);
    response->Reparse();
    response->Request = request;
    return response;
}

THttpOutgoingResponsePtr THttpOutgoingResponse::Duplicate(THttpIncomingRequestPtr request) {
    THeadersBuilder headers(Headers);
    if (!headers.Has("X-Worker-Name")) {
        if (!request->Endpoint->WorkerName.empty()) {
            headers.Set("X-Worker-Name", request->Endpoint->WorkerName);
        }
    }
    THttpOutgoingResponsePtr response = new THttpOutgoingResponse(request);
    response->InitResponse(Protocol, Version, Status, Message);
    if (Body) {
        if (ContentType && !request->Endpoint->CompressContentTypes.empty()) {
            TStringBuf contentType = Trim(ContentType.Before(';'), ' ');
            if (Count(request->Endpoint->CompressContentTypes, contentType) != 0) {
                if (response->EnableCompression()) {
                    headers.Erase("Content-Length"); // we will need new length after compression
                }
            }
        }
        response->Set(headers);
        response->SetBody(Body);
    } else {
        response->Set(headers);
        if (!response->ContentLength) {
            response->Set<&THttpResponse::ContentLength>("0");
        }
    }
    return response;
}

TSocketBuffer* THttpOutgoingResponse::GetActiveBuffer() {
    if (Size() == 0) {
        for (auto itChunk = DataChunks.begin(); itChunk != DataChunks.end();) {
            if ((*itChunk)->Size() > 0) {
                return itChunk->Get();
            } else {
                itChunk = DataChunks.erase(itChunk);
            }
        }
    }
    return this;
}

void THttpOutgoingResponse::AddDataChunk(THttpOutgoingDataChunkPtr dataChunk) {
    DataChunks.emplace_back(std::move(dataChunk));
    if (DataChunks.back()->IsEndOfData()) {
        FinishBody();
    }
}

THttpOutgoingDataChunk::THttpOutgoingDataChunk(THttpOutgoingResponsePtr response, TStringBuf data)
    : Response(std::move(response))
{
    if (data) {
        if (Response->ContentEncoding == "deflate") {
            SetData(CompressDeflate(data));
        } else {
            SetData(data);
        }
    } else {
        SetEndOfData();
    }
}

THttpOutgoingDataChunk::THttpOutgoingDataChunk(THttpOutgoingResponsePtr response)
    : Response(std::move(response))
{}

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
        Y_ABORT("Invalid URL specified");
    }
    if (!scheme.empty() && scheme != "http" && scheme != "https") {
        Y_ABORT("Invalid URL specified");
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

const TString TUrlParameters::operator [](TStringBuf name) const {
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

TUrlParametersBuilder::TUrlParametersBuilder()
    : TUrlParameters(TStringBuf())
{}

void TUrlParametersBuilder::Set(TStringBuf name, TStringBuf data) {
    Data.emplace_back(name, data);
    Parameters[Data.back().first] = Data.back().second;
}

TCookies::TCookies(TStringBuf cookie) {
    for (TStringBuf param = cookie.NextTok(';'); !param.empty(); param = cookie.NextTok(';')) {
        param.SkipPrefix(" ");
        TStringBuf name = param.NextTok('=');
        Cookies[name] = param;
    }
}

const TStringBuf TCookies::operator [](TStringBuf name) const {
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
    Parse(headers);
}

size_t THeaders::Parse(TStringBuf headers) {
    auto start = headers.begin();
    for (TStringBuf param = headers.NextTok("\r\n"); !param.empty(); param = headers.NextTok("\r\n")) {
        TStringBuf name = param.NextTok(":");
        param.SkipPrefix(" ");
        Headers[name] = param;
    }
    return headers.begin() - start;
}

const TStringBuf THeaders::operator [](TStringBuf name) const {
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

bool THeaders::IsChunkedEncoding() const {
    auto it = Headers.find("Transfer-Encoding");
    if (it == Headers.end()) {
        return false;
    }
    return TEqNoCase()(it->second, "chunked"); // TODO: add support for multiple comma-separated values
}

THeadersBuilder::THeadersBuilder()
    : THeaders(TStringBuf())
{}

THeadersBuilder::THeadersBuilder(TStringBuf headers)
    : THeaders(headers)
{}

THeadersBuilder::THeadersBuilder(const THeadersBuilder& builder) {
    for (const auto& pr : builder.Headers) {
        Set(pr.first, pr.second);
    }
}

THeadersBuilder::THeadersBuilder(std::initializer_list<std::pair<TString, TString>> headers) {
    for (const auto& pr : headers) {
        Set(pr.first, pr.second);
    }
}

void THeadersBuilder::Set(TStringBuf name, TStringBuf data) {
    Data.emplace_back(name, data);
    auto it = Headers.find(Data.back().first);
    if (it != Headers.end()) {
        it->second = Data.back().second; // update existing header
    } else {
        Headers[Data.back().first] = Data.back().second; // add new header
    }
}

void THeadersBuilder::Erase(TStringBuf name) {
    Headers.erase(name);
}

}
