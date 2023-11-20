#include "http.h"

#include "abortable_http_response.h"
#include "core.h"
#include "helpers.h"

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/yt/core/http/http.h>

#include <library/cpp/json/json_writer.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/generic/singleton.h>
#include <util/generic/algorithm.h>

#include <util/stream/mem.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/escape.h>
#include <util/string/printf.h>

#include <util/system/byteorder.h>
#include <util/system/getpid.h>

#include <exception>


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class THttpRequest::TRequestStream
    : public IOutputStream
{
public:
    TRequestStream(THttpRequest* httpRequest, const TSocket& s)
        : HttpRequest_(httpRequest)
        , SocketOutput_(s)
        , HttpOutput_(static_cast<IOutputStream*>(&SocketOutput_))
    {
        HttpOutput_.EnableKeepAlive(true);
    }

private:
    void DoWrite(const void* buf, size_t len) override
    {
        WrapWriteFunc([&] {
            HttpOutput_.Write(buf, len);
        });
    }

    void DoWriteV(const TPart* parts, size_t count) override
    {
        WrapWriteFunc([&] {
            HttpOutput_.Write(parts, count);
        });
    }

    void DoWriteC(char ch) override
    {
        WrapWriteFunc([&] {
            HttpOutput_.Write(ch);
        });
    }

    void DoFlush() override
    {
        WrapWriteFunc([&] {
            HttpOutput_.Flush();
        });
    }

    void DoFinish() override
    {
        WrapWriteFunc([&] {
            HttpOutput_.Finish();
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
            HttpRequest_->GetResponseStream();
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
    THttpRequest* const HttpRequest_;
    TSocketOutput SocketOutput_;
    THttpOutput HttpOutput_;
    std::exception_ptr WriteError_;
};

////////////////////////////////////////////////////////////////////////////////

THttpHeader::THttpHeader(const TString& method, const TString& command, bool isApi)
    : Method(method)
    , Command(command)
    , IsApi(isApi)
{ }

void THttpHeader::AddParameter(const TString& key, TNode value, bool overwrite)
{
    auto it = Parameters.find(key);
    if (it == Parameters.end()) {
        Parameters.emplace(key, std::move(value));
    } else {
        if (overwrite) {
            it->second = std::move(value);
        } else {
            ythrow yexception() << "Duplicate key: " << key;
        }
    }
}

void THttpHeader::MergeParameters(const TNode& newParameters, bool overwrite)
{
    for (const auto& p : newParameters.AsMap()) {
        AddParameter(p.first, p.second, overwrite);
    }
}

void THttpHeader::RemoveParameter(const TString& key)
{
    Parameters.erase(key);
}

TNode THttpHeader::GetParameters() const
{
    return Parameters;
}

void THttpHeader::AddTransactionId(const TTransactionId& transactionId, bool overwrite)
{
    if (transactionId) {
        AddParameter("transaction_id", GetGuidAsString(transactionId), overwrite);
    } else {
        RemoveParameter("transaction_id");
    }
}

void THttpHeader::AddPath(const TString& path, bool overwrite)
{
    AddParameter("path", path, overwrite);
}

void THttpHeader::AddOperationId(const TOperationId& operationId, bool overwrite)
{
    AddParameter("operation_id", GetGuidAsString(operationId), overwrite);
}

void THttpHeader::AddMutationId()
{
    TGUID guid;

    // Some users use `fork()' with yt wrapper
    // (actually they use python + multiprocessing)
    // and CreateGuid is not resistant to `fork()', so spice it a little bit.
    //
    // Check IGNIETFERRO-610
    CreateGuid(&guid);
    guid.dw[2] = GetPID() ^ MicroSeconds();

    AddParameter("mutation_id", GetGuidAsString(guid), true);
}

bool THttpHeader::HasMutationId() const
{
    return Parameters.contains("mutation_id");
}

void THttpHeader::SetToken(const TString& token)
{
    Token = token;
}

void THttpHeader::SetProxyAddress(const TString& proxyAddress)
{
    ProxyAddress = proxyAddress;
}

void THttpHeader::SetHostPort(const TString& hostPort)
{
    HostPort = hostPort;
}

void THttpHeader::SetImpersonationUser(const TString& impersonationUser)
{
    ImpersonationUser = impersonationUser;
}

void THttpHeader::SetServiceTicket(const TString& ticket)
{
    ServiceTicket = ticket;
}

void THttpHeader::SetInputFormat(const TMaybe<TFormat>& format)
{
    InputFormat = format;
}

void THttpHeader::SetOutputFormat(const TMaybe<TFormat>& format)
{
    OutputFormat = format;
}

TMaybe<TFormat> THttpHeader::GetOutputFormat() const
{
    return OutputFormat;
}

void THttpHeader::SetRequestCompression(const TString& compression)
{
    RequestCompression = compression;
}

void THttpHeader::SetResponseCompression(const TString& compression)
{
    ResponseCompression = compression;
}

TString THttpHeader::GetCommand() const
{
    return Command;
}

TString THttpHeader::GetUrl(bool needProxy) const
{
    TStringStream url;

    if (needProxy && !ProxyAddress.empty()) {
        url << ProxyAddress << "/";
        return url.Str();
    }

    if (!ProxyAddress.empty()) {
        url << HostPort;
    }

    if (IsApi) {
        url << "/api/" << TConfig::Get()->ApiVersion << "/" << Command;
    } else {
        url << "/" << Command;
    }

    return url.Str();
}

bool THttpHeader::ShouldAcceptFraming() const
{
    return TConfig::Get()->CommandsWithFraming.contains(Command);
}

TString THttpHeader::GetHeaderAsString(const TString& hostName, const TString& requestId, bool includeParameters) const
{
    TStringStream result;

    result << Method << " " << GetUrl() << " HTTP/1.1\r\n";

    GetHeader(HostPort.Empty() ? hostName : HostPort, requestId, includeParameters).Get()->WriteTo(&result);

    if (ShouldAcceptFraming()) {
        result << "X-YT-Accept-Framing: 1\r\n";
    }

    result << "\r\n";

    return result.Str();
}

NHttp::THeadersPtrWrapper THttpHeader::GetHeader(const TString& hostName, const TString& requestId, bool includeParameters) const
{
    auto headers = New<NHttp::THeaders>();

    headers->Add("Host", hostName);
    headers->Add("User-Agent", TProcessState::Get()->ClientVersion);

    if (!Token.empty()) {
        headers->Add("Authorization", "OAuth " + Token);
    }
    if (!ServiceTicket.empty()) {
        headers->Add("X-Ya-Service-Ticket", ServiceTicket);
    }
    if (!ImpersonationUser.empty()) {
        headers->Add("X-Yt-User-Name", ImpersonationUser);
    }

    if (Method == "PUT" || Method == "POST") {
        headers->Add("Transfer-Encoding", "chunked");
    }

    headers->Add("X-YT-Correlation-Id", requestId);
    headers->Add("X-YT-Header-Format", "<format=text>yson");

    headers->Add("Content-Encoding", RequestCompression);
    headers->Add("Accept-Encoding", ResponseCompression);

    auto printYTHeader = [&headers] (const char* headerName, const TString& value) {
        static const size_t maxHttpHeaderSize = 64 << 10;
        if (!value) {
            return;
        }
        if (value.size() <= maxHttpHeaderSize) {
            headers->Add(headerName, value);
            return;
        }

        TString encoded;
        Base64Encode(value, encoded);
        auto ptr = encoded.data();
        auto finish = encoded.data() + encoded.size();
        size_t index = 0;
        do {
            auto end = Min(ptr + maxHttpHeaderSize, finish);
            headers->Add(Format("%v%v", headerName, index++), TString(ptr, end));
            ptr = end;
        } while (ptr != finish);
    };

    if (InputFormat) {
        printYTHeader("X-YT-Input-Format", NodeToYsonString(InputFormat->Config));
    }
    if (OutputFormat) {
        printYTHeader("X-YT-Output-Format", NodeToYsonString(OutputFormat->Config));
    }
    if (includeParameters) {
        printYTHeader("X-YT-Parameters", NodeToYsonString(Parameters));
    }

    return NHttp::THeadersPtrWrapper(std::move(headers));
}

const TString& THttpHeader::GetMethod() const
{
    return Method;
}

////////////////////////////////////////////////////////////////////////////////

TAddressCache* TAddressCache::Get()
{
    return Singleton<TAddressCache>();
}

bool ContainsAddressOfRequiredVersion(const TAddressCache::TAddressPtr& address)
{
    if (!TConfig::Get()->ForceIpV4 && !TConfig::Get()->ForceIpV6) {
        return true;
    }

    for (auto i = address->Begin(); i != address->End(); ++i) {
        const auto& addressInfo = *i;
        if (TConfig::Get()->ForceIpV4 && addressInfo.ai_family == AF_INET) {
            return true;
        }
        if (TConfig::Get()->ForceIpV6 && addressInfo.ai_family == AF_INET6) {
            return true;
        }
    }
    return false;
}

TAddressCache::TAddressPtr TAddressCache::Resolve(const TString& hostName)
{
    auto address = FindAddress(hostName);
    if (address) {
        return address;
    }

    TString host(hostName);
    ui16 port = 80;

    auto colon = hostName.find(':');
    if (colon != TString::npos) {
        port = FromString<ui16>(hostName.substr(colon + 1));
        host = hostName.substr(0, colon);
    }

    auto retryPolicy = CreateDefaultRequestRetryPolicy(TConfig::Get());
    auto error = yexception() << "can not resolve address of required version for host " << hostName;
    while (true) {
        address = new TNetworkAddress(host, port);
        if (ContainsAddressOfRequiredVersion(address)) {
            break;
        }
        retryPolicy->NotifyNewAttempt();
        YT_LOG_DEBUG("Failed to resolve address of required version for host %v, retrying: %v",
            hostName,
            retryPolicy->GetAttemptDescription());
        if (auto backoffDuration = retryPolicy->OnGenericError(error)) {
            NDetail::TWaitProxy::Get()->Sleep(*backoffDuration);
        } else {
            ythrow error;
        }
    }

    AddAddress(hostName, address);
    return address;
}

TAddressCache::TAddressPtr TAddressCache::FindAddress(const TString& hostName) const
{
    TCacheEntry entry;
    {
        TReadGuard guard(Lock_);
        auto it = Cache_.find(hostName);
        if (it == Cache_.end()) {
            return nullptr;
        }
        entry = it->second;
    }

    if (TInstant::Now() > entry.ExpirationTime) {
        YT_LOG_DEBUG("Address resolution cache entry for host %v is expired, will retry resolution",
            hostName);
        return nullptr;
    }

    if (!ContainsAddressOfRequiredVersion(entry.Address)) {
        YT_LOG_DEBUG("Address of required version not found for host %v, will retry resolution",
            hostName);
        return nullptr;
    }

    return entry.Address;
}

void TAddressCache::AddAddress(TString hostName, TAddressPtr address)
{
    auto entry = TCacheEntry{
        .Address = std::move(address),
        .ExpirationTime = TInstant::Now() + TConfig::Get()->AddressCacheExpirationTimeout,
    };

    {
        TWriteGuard guard(Lock_);
        Cache_.emplace(std::move(hostName), std::move(entry));
    }
}

////////////////////////////////////////////////////////////////////////////////

TConnectionPool* TConnectionPool::Get()
{
    return Singleton<TConnectionPool>();
}

TConnectionPtr TConnectionPool::Connect(
    const TString& hostName,
    TDuration socketTimeout)
{
    Refresh();

    if (socketTimeout == TDuration::Zero()) {
        socketTimeout = TConfig::Get()->SocketTimeout;
    }

    {
        auto guard = Guard(Lock_);
        auto now = TInstant::Now();
        auto range = Connections_.equal_range(hostName);
        for (auto it = range.first; it != range.second; ++it) {
            auto& connection = it->second;
            if (connection->DeadLine < now) {
                continue;
            }
            if (!AtomicCas(&connection->Busy, 1, 0)) {
                continue;
            }

            connection->DeadLine = now + socketTimeout;
            connection->Socket->SetSocketTimeout(socketTimeout.Seconds());
            return connection;
        }
    }

    TConnectionPtr connection(new TConnection);

    auto networkAddress = TAddressCache::Get()->Resolve(hostName);
    TSocketHolder socket(DoConnect(networkAddress));
    SetNonBlock(socket, false);

    connection->Socket.Reset(new TSocket(socket.Release()));

    connection->DeadLine = TInstant::Now() + socketTimeout;
    connection->Socket->SetSocketTimeout(socketTimeout.Seconds());

    {
        auto guard = Guard(Lock_);
        static ui32 connectionId = 0;
        connection->Id = ++connectionId;
        Connections_.insert({hostName, connection});
    }

    YT_LOG_DEBUG("New connection to %v #%v opened",
        hostName,
        connection->Id);

    return connection;
}

void TConnectionPool::Release(TConnectionPtr connection)
{
    auto socketTimeout = TConfig::Get()->SocketTimeout;
    auto newDeadline = TInstant::Now() + socketTimeout;

    {
        auto guard = Guard(Lock_);
        connection->DeadLine = newDeadline;
    }

    connection->Socket->SetSocketTimeout(socketTimeout.Seconds());
    AtomicSet(connection->Busy, 0);

    Refresh();
}

void TConnectionPool::Invalidate(
    const TString& hostName,
    TConnectionPtr connection)
{
    auto guard = Guard(Lock_);
    auto range = Connections_.equal_range(hostName);
    for (auto it = range.first; it != range.second; ++it) {
        if (it->second == connection) {
            YT_LOG_DEBUG("Closing connection #%v",
                connection->Id);
            Connections_.erase(it);
            return;
        }
    }
}

void TConnectionPool::Refresh()
{
    auto guard = Guard(Lock_);

    // simple, since we don't expect too many connections
    using TItem = std::pair<TInstant, TConnectionMap::iterator>;
    std::vector<TItem> sortedConnections;
    for (auto it = Connections_.begin(); it != Connections_.end(); ++it) {
        sortedConnections.emplace_back(it->second->DeadLine, it);
    }

    std::sort(
        sortedConnections.begin(),
        sortedConnections.end(),
        [] (const TItem& a, const TItem& b) -> bool {
            return a.first < b.first;
        });

    auto removeCount = static_cast<int>(Connections_.size()) - TConfig::Get()->ConnectionPoolSize;

    const auto now = TInstant::Now();
    for (const auto& item : sortedConnections) {
        const auto& mapIterator = item.second;
        auto connection = mapIterator->second;
        if (AtomicGet(connection->Busy)) {
            continue;
        }

        if (removeCount > 0) {
            Connections_.erase(mapIterator);
            YT_LOG_DEBUG("Closing connection #%v (too many opened connections)",
                connection->Id);
            --removeCount;
            continue;
        }

        if (connection->DeadLine < now) {
            Connections_.erase(mapIterator);
            YT_LOG_DEBUG("Closing connection #%v (timeout)",
                connection->Id);
        }
    }
}

SOCKET TConnectionPool::DoConnect(TAddressCache::TAddressPtr address)
{
    int lastError = 0;

    for (auto i = address->Begin(); i != address->End(); ++i) {
        struct addrinfo* info = &*i;

        if (TConfig::Get()->ForceIpV4 && info->ai_family != AF_INET) {
            continue;
        }

        if (TConfig::Get()->ForceIpV6 && info->ai_family != AF_INET6) {
            continue;
        }

        TSocketHolder socket(
            ::socket(info->ai_family, info->ai_socktype, info->ai_protocol));

        if (socket.Closed()) {
            lastError = LastSystemError();
            continue;
        }

        SetNonBlock(socket, true);
        if (TConfig::Get()->SocketPriority) {
            SetSocketPriority(socket, *TConfig::Get()->SocketPriority);
        }

        if (connect(socket, info->ai_addr, info->ai_addrlen) == 0)
            return socket.Release();

        int err = LastSystemError();
        if (err == EINPROGRESS || err == EAGAIN || err == EWOULDBLOCK) {
            struct pollfd p = {
                socket,
                POLLOUT,
                0
            };
            const ssize_t n = PollD(&p, 1, TInstant::Now() + TConfig::Get()->ConnectTimeout);
            if (n < 0) {
                ythrow TSystemError(-(int)n) << "can not connect to " << info;
            }
            CheckedGetSockOpt(socket, SOL_SOCKET, SO_ERROR, err, "socket error");
            if (!err)
                return socket.Release();
        }

        lastError = err;
        continue;
    }

    ythrow TSystemError(lastError) << "can not connect to " << *address;
}

////////////////////////////////////////////////////////////////////////////////

static TMaybe<TString> GetProxyName(const THttpInput& input)
{
    if (auto proxyHeader = input.Headers().FindHeader("X-YT-Proxy")) {
        return proxyHeader->Value();
    }
    return Nothing();
}

THttpResponse::THttpResponse(
    IInputStream* socketStream,
    const TString& requestId,
    const TString& hostName)
    : HttpInput_(socketStream)
    , RequestId_(requestId)
    , HostName_(GetProxyName(HttpInput_).GetOrElse(hostName))
    , Unframe_(HttpInput_.Headers().HasHeader("X-YT-Framing"))
{
    HttpCode_ = ParseHttpRetCode(HttpInput_.FirstLine());
    if (HttpCode_ == 200 || HttpCode_ == 202) {
        return;
    }

    ErrorResponse_ = TErrorResponse(HttpCode_, RequestId_);

    auto logAndSetError = [&] (const TString& rawError) {
        YT_LOG_ERROR("RSP %v - HTTP %v - %v",
            RequestId_,
            HttpCode_,
            rawError.data());
        ErrorResponse_->SetRawError(rawError);
    };

    switch (HttpCode_) {
        case 429:
            logAndSetError("request rate limit exceeded");
            break;

        case 500:
            logAndSetError(::TStringBuilder() << "internal error in proxy " << HostName_);
            break;

        default: {
            TStringStream httpHeaders;
            httpHeaders << "HTTP headers (";
            for (const auto& header : HttpInput_.Headers()) {
                httpHeaders << header.Name() << ": " << header.Value() << "; ";
            }
            httpHeaders << ")";

            auto errorString = Sprintf("RSP %s - HTTP %d - %s",
                RequestId_.data(),
                HttpCode_,
                httpHeaders.Str().data());

            YT_LOG_ERROR("%v",
                errorString.data());

            if (auto parsedResponse = ParseError(HttpInput_.Headers())) {
                ErrorResponse_ = parsedResponse.GetRef();
            } else {
                ErrorResponse_->SetRawError(
                    errorString + " - X-YT-Error is missing in headers");
            }
            break;
        }
    }
}

const THttpHeaders& THttpResponse::Headers() const
{
    return HttpInput_.Headers();
}

void THttpResponse::CheckErrorResponse() const
{
    if (ErrorResponse_) {
        throw *ErrorResponse_;
    }
}

bool THttpResponse::IsExhausted() const
{
    return IsExhausted_;
}

int THttpResponse::GetHttpCode() const
{
    return HttpCode_;
}

const TString& THttpResponse::GetHostName() const
{
    return HostName_;
}

bool THttpResponse::IsKeepAlive() const
{
    return HttpInput_.IsKeepAlive();
}

TMaybe<TErrorResponse> THttpResponse::ParseError(const THttpHeaders& headers)
{
    for (const auto& header : headers) {
        if (header.Name() == "X-YT-Error") {
            TErrorResponse errorResponse(HttpCode_, RequestId_);
            errorResponse.ParseFromJsonError(header.Value());
            if (errorResponse.IsOk()) {
                return Nothing();
            }
            return errorResponse;
        }
    }
    return Nothing();
}

size_t THttpResponse::DoRead(void* buf, size_t len)
{
    size_t read;
    if (Unframe_) {
        read = UnframeRead(buf, len);
    } else {
        read = HttpInput_.Read(buf, len);
    }
    if (read == 0 && len != 0) {
        // THttpInput MUST return defined (but may be empty)
        // trailers when it is exhausted.
        Y_ABORT_UNLESS(HttpInput_.Trailers().Defined(),
            "trailers MUST be defined for exhausted stream");
        CheckTrailers(HttpInput_.Trailers().GetRef());
        IsExhausted_ = true;
    }
    return read;
}

size_t THttpResponse::DoSkip(size_t len)
{
    size_t skipped;
    if (Unframe_) {
        skipped = UnframeSkip(len);
    } else {
        skipped = HttpInput_.Skip(len);
    }
    if (skipped == 0 && len != 0) {
        // THttpInput MUST return defined (but may be empty)
        // trailers when it is exhausted.
        Y_ABORT_UNLESS(HttpInput_.Trailers().Defined(),
            "trailers MUST be defined for exhausted stream");
        CheckTrailers(HttpInput_.Trailers().GetRef());
        IsExhausted_ = true;
    }
    return skipped;
}

void THttpResponse::CheckTrailers(const THttpHeaders& trailers)
{
    if (auto errorResponse = ParseError(trailers)) {
        errorResponse->SetIsFromTrailers(true);
        YT_LOG_ERROR("RSP %v - %v",
            RequestId_,
            errorResponse.GetRef().what());
        ythrow errorResponse.GetRef();
    }
}

static ui32 ReadDataFrameSize(THttpInput* stream)
{
    ui32 littleEndianSize;
    auto read = stream->Load(&littleEndianSize, sizeof(littleEndianSize));
    if (read < sizeof(littleEndianSize)) {
        ythrow yexception() << "Bad data frame header: " <<
            "expected " << sizeof(littleEndianSize) << " bytes, got " << read;
    }
    return LittleToHost(littleEndianSize);
}

bool THttpResponse::RefreshFrameIfNecessary()
{
    while (RemainingFrameSize_ == 0) {
        ui8 frameTypeByte;
        auto read = HttpInput_.Read(&frameTypeByte, sizeof(frameTypeByte));
        if (read == 0) {
            return false;
        }
        auto frameType = static_cast<EFrameType>(frameTypeByte);
        switch (frameType) {
            case EFrameType::KeepAlive:
                break;
            case EFrameType::Data:
                RemainingFrameSize_ = ReadDataFrameSize(&HttpInput_);
                break;
            default:
                ythrow yexception() << "Bad frame type " << static_cast<int>(frameTypeByte);
        }
    }
    return true;
}

size_t THttpResponse::UnframeRead(void* buf, size_t len)
{
    if (!RefreshFrameIfNecessary()) {
        return 0;
    }
    auto read = HttpInput_.Read(buf, Min(len, RemainingFrameSize_));
    RemainingFrameSize_ -= read;
    return read;
}

size_t THttpResponse::UnframeSkip(size_t len)
{
    if (!RefreshFrameIfNecessary()) {
        return 0;
    }
    auto skipped = HttpInput_.Skip(Min(len, RemainingFrameSize_));
    RemainingFrameSize_ -= skipped;
    return skipped;
}

////////////////////////////////////////////////////////////////////////////////

THttpRequest::THttpRequest()
{
    RequestId = CreateGuidAsString();
}

THttpRequest::THttpRequest(const TString& requestId)
    : RequestId(requestId)
{ }

THttpRequest::~THttpRequest()
{
    if (!Connection) {
        return;
    }

    if (Input && Input->IsKeepAlive() && Input->IsExhausted()) {
        // We should return to the pool only connections where HTTP response was fully read.
        // Otherwise next reader might read our remaining data and misinterpret them (YT-6510).
        TConnectionPool::Get()->Release(Connection);
    } else {
        TConnectionPool::Get()->Invalidate(HostName, Connection);
    }
}

TString THttpRequest::GetRequestId() const
{
    return RequestId;
}

void THttpRequest::Connect(TString hostName, TDuration socketTimeout)
{
    HostName = std::move(hostName);
    YT_LOG_DEBUG("REQ %v - requesting connection to %v from connection pool",
        RequestId,
        HostName);

    StartTime_ = TInstant::Now();
    Connection = TConnectionPool::Get()->Connect(HostName, socketTimeout);

    YT_LOG_DEBUG("REQ %v - connection #%v",
        RequestId,
        Connection->Id);
}

IOutputStream* THttpRequest::StartRequestImpl(const THttpHeader& header, bool includeParameters)
{
    auto strHeader = header.GetHeaderAsString(HostName, RequestId, includeParameters);
    Url_ = header.GetUrl(true);

    LogRequest(header, Url_, includeParameters, RequestId, HostName);

    LoggedAttributes_ = GetLoggedAttributes(header, Url_, includeParameters, 128);

    auto outputFormat = header.GetOutputFormat();
    if (outputFormat && outputFormat->IsTextYson()) {
        LogResponse = true;
    }

    RequestStream_ = MakeHolder<TRequestStream>(this, *Connection->Socket.Get());

    RequestStream_->Write(strHeader.data(), strHeader.size());
    return RequestStream_.Get();
}

IOutputStream* THttpRequest::StartRequest(const THttpHeader& header)
{
    return StartRequestImpl(header, true);
}

void THttpRequest::FinishRequest()
{
    RequestStream_->Flush();
    RequestStream_->Finish();
}

void THttpRequest::SmallRequest(const THttpHeader& header, TMaybe<TStringBuf> body)
{
    if (!body && (header.GetMethod() == "PUT" || header.GetMethod() == "POST")) {
        const auto& parameters = header.GetParameters();
        auto parametersStr = NodeToYsonString(parameters);
        auto* output = StartRequestImpl(header, false);
        output->Write(parametersStr);
        FinishRequest();
    } else {
        auto* output = StartRequest(header);
        if (body) {
            output->Write(*body);
        }
        FinishRequest();
    }
}

THttpResponse* THttpRequest::GetResponseStream()
{
    if (!Input) {
        SocketInput.Reset(new TSocketInput(*Connection->Socket.Get()));
        if (TConfig::Get()->UseAbortableResponse) {
            Y_ABORT_UNLESS(!Url_.empty());
            Input.Reset(new TAbortableHttpResponse(SocketInput.Get(), RequestId, HostName, Url_));
        } else {
            Input.Reset(new THttpResponse(SocketInput.Get(), RequestId, HostName));
        }
        Input->CheckErrorResponse();
    }
    return Input.Get();
}

TString THttpRequest::GetResponse()
{
    TString result = GetResponseStream()->ReadAll();

    TStringStream loggedAttributes;
    loggedAttributes
        << "Time: " << TInstant::Now() - StartTime_ << "; "
        << "HostName: " << GetResponseStream()->GetHostName() << "; "
        << LoggedAttributes_;

    if (LogResponse) {
        constexpr auto sizeLimit = 1 << 7;
        YT_LOG_DEBUG("RSP %v - received response (Response: '%v'; %v)",
            RequestId,
            TruncateForLogs(result, sizeLimit),
            loggedAttributes.Str());
    } else {
        YT_LOG_DEBUG("RSP %v - received response of %v bytes (%v)",
            RequestId,
            result.size(),
            loggedAttributes.Str());
    }
    return result;
}

int THttpRequest::GetHttpCode() {
    return GetResponseStream()->GetHttpCode();
}

void THttpRequest::InvalidateConnection()
{
    TConnectionPool::Get()->Invalidate(HostName, Connection);
    Connection.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
