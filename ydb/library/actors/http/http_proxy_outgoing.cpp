#include "http_proxy.h"
#include "http_proxy_sock_impl.h"
#include "http2.h"

#include <type_traits>

namespace NHttp {

template <typename TSocketImpl>
class TOutgoingConnectionActor : public NActors::TActorBootstrapped<TOutgoingConnectionActor<TSocketImpl>>, public TSocketImpl, virtual public THttpConfig {
public:
    using TBase = NActors::TActorBootstrapped<TOutgoingConnectionActor<TSocketImpl>>;
    using TSelf = TOutgoingConnectionActor<TSocketImpl>;
    using TBase::Become;
    using TBase::Send;
    using TBase::Schedule;
    using TBase::SelfId;

    TActorId Owner;
    SocketAddressType Address;
    TString Destination;
    TActorId RequestOwner;
    THttpOutgoingRequestPtr Request;
    THttpIncomingResponsePtr Response;
    TInstant LastActivity;
    TDuration ConnectionTimeout = CONNECTION_TIMEOUT;
    bool AllowConnectionReuse = false;
    bool UseHttp2 = false;
    NActors::TPollerToken::TPtr PollerToken;

    enum class EStreamState {
        Unknown,
        Declined,
        Approved,
    } StreamState = EStreamState::Unknown;
    std::vector<TString> StreamContentTypes;

    // HTTP/2 state (allocated on demand when HTTP/2 mode is activated)
    struct THttp2State {
        std::unique_ptr<NHttp2::TSession> Session;

        struct TPendingRequest {
            THttpOutgoingRequestPtr Request;
            TActorId RequestOwner;
            uint32_t StreamId;
        };
        THashMap<uint32_t, TPendingRequest> PendingRequests;

        TString OutputBuffer;
        size_t OutputOffset = 0;
        size_t PendingWriteSize = 0; // for SSL_write retry with same length
        bool Error = false;

        TDeque<TEvHttpProxy::TEvHttpOutgoingRequest::TPtr> RequestQueue;
    };
    std::unique_ptr<THttp2State> H2;

    TOutgoingConnectionActor(const TActorId& owner, TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event)
        : Owner(owner)
    {
        InitiateRequest(event);
    }

    static constexpr char ActorName[] = "OUT_CONNECTION_ACTOR";

    void Bootstrap() {
        PerformRequest();
    }

    bool IsAlive() const {
        return static_cast<bool>(Owner);
    }

    void PassAway() override {
        if (IsAlive()) {
            if (H2) {
                if (H2->Session && !H2->Error) {
                    H2->Session->SendGoaway();
                    FlushH2Output(); // best-effort, may fail silently
                }
                for (auto& [streamId, pending] : H2->PendingRequests) {
                    Send(pending.RequestOwner, new TEvHttpProxy::TEvHttpIncomingResponse(pending.Request, nullptr, "Connection closed"));
                }
                H2->PendingRequests.clear();
                for (auto& event : H2->RequestQueue) {
                    Send(event->Sender, new TEvHttpProxy::TEvHttpIncomingResponse(event->Get()->Request, nullptr, "Connection closed"));
                }
                H2->RequestQueue.clear();
            }
            Send(Owner, new TEvHttpProxy::TEvHttpOutgoingConnectionClosed(SelfId(), Destination));
            TSocketImpl::Shutdown(); // to avoid errors when connection already closed
            TBase::PassAway();
            Owner = {};
        }
    }

    TString GetSocketName() {
        TStringBuilder builder;
        if (TSocketImpl::Socket) {
            builder << "(#" << TSocketImpl::GetRawSocket();
            if (Address && Address->SockAddr()->sa_family) {
                builder << "," << Address;
            }
            builder << ") ";
        }
        return builder;
    }

    TString GetRequestDebugText() {
        TStringBuilder text;
        if (Request) {
            text << Request->Method << " " << Request->URL;
            if (Request->Body) {
                text << ", " << Request->Body.Size() << " bytes";
            }
        }
        return text;
    }

    TString GetResponseDebugText() {
        TStringBuilder text;
        if (Response) {
            text << Response->Status << " " << Response->Message;
            if (Response->Body) {
                text << ", " << Response->Body.Size() << " bytes";
            }
        }
        return text;
    }

    void ReplyAndPassAway() {
        if (RequestOwner) {
            if (StreamState == EStreamState::Approved) {
                ALOG_DEBUG(HttpLog, GetSocketName() << "-> (end of stream)");
                auto dataChunk = std::make_unique<TEvHttpProxy::TEvHttpIncomingDataChunk>(Response);
                dataChunk->SetEndOfData();
                Send(RequestOwner, dataChunk.release());
            } else {
                ALOG_DEBUG(HttpLog, GetSocketName() << "-> (" << GetResponseDebugText() << ")");
                ALOG_TRACE(HttpLog, GetSocketName() << "Response:\n" << Response->GetObfuscatedData());
                Send(RequestOwner, new TEvHttpProxy::TEvHttpIncomingResponse(Request, Response));
                RequestOwner = TActorId();
            }
            THolder<TEvHttpProxy::TEvReportSensors> sensors(BuildOutgoingRequestSensors(Request, Response));
            Send(Owner, sensors.Release());
        }
        if (!AllowConnectionReuse || Response->IsConnectionClose()) {
            ALOG_DEBUG(HttpLog, GetSocketName() << "connection closed");
            PassAway();
        } else {
            CheckClose();
            if (IsAlive()) {
                ALOG_DEBUG(HttpLog, GetSocketName() << "connection available for reuse");
                ConnectionTimeout = CONNECTION_TIMEOUT;
                Send(Owner, new TEvHttpProxy::TEvHttpOutgoingConnectionAvailable(SelfId(), Destination));
            }
        }
    }

    void ReplyErrorAndPassAway(const TString& error) {
        if (error) {
            ALOG_ERROR(HttpLog, GetSocketName() << "connection closed with error: " << error);
        } else {
            ALOG_DEBUG(HttpLog, GetSocketName() << "connection closed");
        }
        // TODO(xenoxeno): reply with error on data chunk
        if (RequestOwner) {
            if (StreamState == EStreamState::Approved) {
                auto dataChunk = std::make_unique<TEvHttpProxy::TEvHttpIncomingDataChunk>(Response);
                dataChunk->Error = error ? error : "ConnectionClosed";
                Send(RequestOwner, dataChunk.release());
            } else {
                if (!error && Response && !Response->IsReady()) {
                    Send(RequestOwner, new TEvHttpProxy::TEvHttpIncomingResponse(Request, Response, "ConnectionClosed")); // connection closed prematurely
                } else {
                    Send(RequestOwner, new TEvHttpProxy::TEvHttpIncomingResponse(Request, Response, error));
                }
            }
            RequestOwner = TActorId();
            THolder<TEvHttpProxy::TEvReportSensors> sensors(BuildOutgoingRequestSensors(Request, Response));
            Send(Owner, sensors.Release());
        }
        PassAway();
    }

protected:
    void FailConnection(const TString& error) {
        if (Request) {
            return ReplyErrorAndPassAway(error);
        }
        return TBase::Become(&TOutgoingConnectionActor::StateFailed);
    }

    void Connect() {
        TSocketImpl::Create(Address->SockAddr()->sa_family);
        TSocketImpl::SetNonBlock();
        TSocketImpl::SetTimeout(ConnectionTimeout);
        RegisterPoller();
        TBase::Become(&TOutgoingConnectionActor::StateConnecting);
    }

    void InitiateRequest(TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event) {
        Request = std::move(event->Get()->Request);
        Destination = Request->GetDestination();
        Response = nullptr;
        RequestOwner = event->Sender;
        if (event->Get()->Timeout) {
            ConnectionTimeout = event->Get()->Timeout;
        }
        AllowConnectionReuse = event->Get()->AllowConnectionReuse;
        UseHttp2 = event->Get()->UseHttp2;
        StreamContentTypes = event->Get()->StreamContentTypes;
        StreamState = EStreamState::Unknown;
    }

    void PerformRequest() {
        TSocketImpl::SetHost(TString(Request->Host));
        Request->Timer.Reset();
        ALOG_DEBUG(HttpLog, GetSocketName() << "resolving " << TSocketImpl::Host);
        Send(Owner, new TEvHttpProxy::TEvResolveHostRequest(TSocketImpl::Host));
        Schedule(ConnectionTimeout, new NActors::TEvents::TEvWakeup());
        LastActivity = NActors::TActivationContext::Now();
        TBase::Become(&TOutgoingConnectionActor::StateResolving);
    }

    void FlushOutput() {
        if (IsAlive() && Request != nullptr) {
            Request->Finish();
            while (auto size = Request->Size()) {
                bool read = false, write = false;
                ssize_t res = TSocketImpl::Send(Request->Data(), size, read, write);
                if (res > 0) {
                    LastActivity = NActors::TActivationContext::Now();
                    Request->ChopHead(res);
                } else if (-res == EINTR) {
                    continue;
                } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                    if (PollerToken) {
                        if (!read && !write) {
                            write = true;
                        }
                        if (PollerToken->RequestNotificationAfterWouldBlock(read, write)) {
                            continue;
                        }
                    }
                    break;
                } else {
                    ReplyErrorAndPassAway(res == 0 ? "" : strerror(-res));
                    break;
                }
            }
        }
    }

    void CheckClose() {
        char buf[8];
        while (IsAlive()) {
            bool read = false, write = false;
            ssize_t res = TSocketImpl::Recv(&buf, 0, read, write);
            if (res > 0) {
                return ReplyErrorAndPassAway("Unexpected data received");
            } else if (-res == EINTR) {
                continue;
            } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                if (PollerToken) {
                    if (!read && !write) {
                        read = true;
                    }
                    if (PollerToken->RequestNotificationAfterWouldBlock(read, write)) {
                        continue;
                    }
                }
                return;
            } else {
                return ReplyErrorAndPassAway(res == 0 ? "" : strerror(-res));
            }
        }
    }

    void PullInput() {
        while (IsAlive()) {
            if (Response == nullptr) {
                Response = new THttpIncomingResponse(Request);
            }
            if (!Response->EnsureEnoughSpaceAvailable()) {
                return ReplyErrorAndPassAway("Not enough space in socket buffer");
            }
            bool read = false, write = false;
            ssize_t res = TSocketImpl::Recv(Response->Pos(), Response->Avail(), read, write);
            if (res > 0) {
                LastActivity = NActors::TActivationContext::Now();
                do {
                    res -= Response->AdvancePartial(res);
                    if (StreamState == EStreamState::Unknown && Response->HasCompletedHeaders()) {
                        auto contentType = Response->ContentType.Before(';');
                        if (Response->IsChunkedEncoding() && std::ranges::find(StreamContentTypes, contentType) != std::ranges::end(StreamContentTypes)) {
                            ALOG_DEBUG(HttpLog, GetSocketName() << "-> (" << GetResponseDebugText() << ") (incomplete)");
                            Send(RequestOwner, new TEvHttpProxy::TEvHttpIncompleteIncomingResponse(Request, Response));
                            StreamState = EStreamState::Approved;
                            Response->SwitchToStreaming();
                        } else {
                            StreamState = EStreamState::Declined;
                        }
                    }

                    if (Response->HasNewStreamingDataChunk()) {
                        ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") -> (data chunk " << Response->ChunkLength << " bytes)");
                        auto dataChunk = std::make_unique<TEvHttpProxy::TEvHttpIncomingDataChunk>(Response);
                        dataChunk->SetData(Response->ExtractDataChunk());
                        Send(RequestOwner, dataChunk.release());
                        if (res == 0) {
                            // when we finish reading at the end of a chunk we could remove processed chunks to save memory and allocations very easily
                            Response->TruncateToHeaders();
                        }
                    }
                } while (res > 0);
                if (Response->IsDone()) {
                    return ReplyAndPassAway();
                }
            } else if (-res == EINTR) {
                continue;
            } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                if (PollerToken) {
                    if (!read && !write) {
                        read = true;
                    }
                    if (PollerToken->RequestNotificationAfterWouldBlock(read, write)) {
                        continue;
                    }
                }
                return;
            } else {
                if (res == 0) {
                    Response->ConnectionClosed();
                }
                if (Response->IsDone() && Response->IsReady()) {
                    return ReplyAndPassAway();
                }
                return ReplyErrorAndPassAway(res == 0 ? "" : strerror(-res));
            }
        }
    }

    void RegisterPoller() {
        Send(NActors::MakePollerActorId(), new NActors::TEvPollerRegister(TSocketImpl::Socket, SelfId(), SelfId()));
    }

    int DoConnect(bool& read, bool& write) {
        if constexpr (std::is_same_v<TSocketImpl, TSecureSocketImpl>) {
            if (UseHttp2) {
                if (!TSocketImpl::Ssl) {
                    TSocketImpl::InitClientSslWithAlpn();
                }
                ERR_clear_error();
                return TSocketImpl::ProcessSslResult(SSL_connect(TSocketImpl::Ssl.Get()), read, write);
            }
        }
        return TSocketImpl::OnConnect(read, write);
    }

    void OnConnect() {
        bool read = false, write = false;
        if (int res = DoConnect(read, write); res != 1) {
            if (-res == EAGAIN) {
                if (PollerToken) {
                    PollerToken->Request(read, write);
                }
                return;
            } else {
                return ReplyErrorAndPassAway(strerror(-res));
            }
        }

        bool useH2 = UseHttp2;
        if constexpr (std::is_same_v<TSocketImpl, TSecureSocketImpl>) {
            TStringBuf alpn = TSocketImpl::GetAlpnProtocol();
            useH2 = (alpn == "h2"); // For TLS, ALPN negotiation is authoritative
        }

        if (useH2) {
            ALOG_DEBUG(HttpLog, GetSocketName() << "HTTP/2 outgoing connection opened");
            SwitchToHttp2();
            return;
        }

        ALOG_DEBUG(HttpLog, GetSocketName() << "outgoing connection opened");
        TBase::Become(&TOutgoingConnectionActor::StateConnected);
        ALOG_DEBUG(HttpLog, GetSocketName() << "<- (" << GetRequestDebugText() << ")");
        ALOG_TRACE(HttpLog, GetSocketName() << "Request:\n" << Request->GetObfuscatedData());
        Send(SelfId(), new NActors::TEvPollerReady(nullptr, true, true));
    }

    void SwitchToHttp2() {
        if (!H2) {
            H2 = std::make_unique<THttp2State>();
        }
        InitHttp2Session();
        if (!FlushH2Output()) { // send connection preface + SETTINGS
            PassAway();
            return;
        }

        // Move the initial request to HTTP/2 flow
        if (Request) {
            SendH2Request(std::move(Request), RequestOwner);
            Request = nullptr;
            RequestOwner = {};
        }

        TBase::Become(&TOutgoingConnectionActor::StateConnectedHttp2);
        Send(SelfId(), new NActors::TEvPollerReady(nullptr, true, true));
    }

    void InitHttp2Session() {
        NHttp2::TSessionCallbacks callbacks;
        callbacks.OnResponse = [this](uint32_t streamId, NHttp2::TStream& stream) {
            OnHttp2Response(streamId, stream);
        };
        callbacks.OnSend = [this](TString data) {
            H2->OutputBuffer.append(data);
        };
        callbacks.OnError = [this](TString error) {
            ALOG_ERROR(HttpLog, GetSocketName() << "HTTP/2 error: " << error);
            H2->Error = true;
        };
        callbacks.OnGoaway = [this](uint32_t lastStreamId, NHttp2::EErrorCode errorCode) {
            ALOG_DEBUG(HttpLog, GetSocketName() << "HTTP/2 GOAWAY received, lastStreamId=" << lastStreamId << " error=" << static_cast<uint32_t>(errorCode));
        };

        H2->Session = std::make_unique<NHttp2::TSession>(NHttp2::TSession::ERole::Client, std::move(callbacks));
        H2->Session->Initialize();
    }

    void OnHttp2Response(uint32_t streamId, NHttp2::TStream& stream) {
        auto it = H2->PendingRequests.find(streamId);
        if (it == H2->PendingRequests.end()) {
            ALOG_ERROR(HttpLog, GetSocketName() << "HTTP/2 response for unknown stream " << streamId);
            return;
        }

        auto& pending = it->second;

        THttpIncomingResponsePtr response = NHttp2::StreamToIncomingResponse(stream, pending.Request);

        ALOG_DEBUG(HttpLog, GetSocketName() << "HTTP/2 stream " << streamId << " -> (" << response->Status << " " << response->Message << ")");
        Send(pending.RequestOwner, new TEvHttpProxy::TEvHttpIncomingResponse(pending.Request, response));

        THolder<TEvHttpProxy::TEvReportSensors> sensors(BuildOutgoingRequestSensors(pending.Request, response));
        Send(Owner, sensors.Release());

        H2->PendingRequests.erase(it);
    }

    void SendH2Request(THttpOutgoingRequestPtr request, TActorId requestOwner) {
        TString method, path, authority, scheme;
        TVector<std::pair<TString, TString>> headers;
        TString body;
        NHttp2::OutgoingRequestToStream(request, method, path, authority, scheme, headers, body);

        uint32_t streamId = H2->Session->SendRequest(method, path, authority, scheme, headers, body);

        ALOG_DEBUG(HttpLog, GetSocketName() << "HTTP/2 stream " << streamId << " <- (" << request->Method << " " << request->URL << ")");

        typename THttp2State::TPendingRequest pending;
        pending.Request = request;
        pending.RequestOwner = requestOwner;
        pending.StreamId = streamId;
        H2->PendingRequests[streamId] = std::move(pending);
    }

    static constexpr size_t READ_BUFFER_SIZE = 64 * 1024;

    void ReadH2Input() {
        char readBuffer[READ_BUFFER_SIZE];
        while (IsAlive()) {
            bool read = false, write = false;
            ssize_t res = TSocketImpl::Recv(readBuffer, READ_BUFFER_SIZE, read, write);
            if (res > 0) {
                LastActivity = NActors::TActivationContext::Now();
                H2->Session->Feed(readBuffer, static_cast<size_t>(res));
                if (H2->Error) {
                    PassAway();
                    return;
                }
                if (!FlushH2Output()) {
                    PassAway();
                    return;
                }
            } else if (-res == EINTR) {
                continue;
            } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                if (PollerToken) {
                    if (!read && !write) {
                        read = true;
                    }
                    if (PollerToken->RequestNotificationAfterWouldBlock(read, write)) {
                        continue;
                    }
                }
                return;
            } else {
                if (res == 0) {
                    ALOG_DEBUG(HttpLog, GetSocketName() << "HTTP/2 connection closed by peer");
                } else {
                    ALOG_ERROR(HttpLog, GetSocketName() << "HTTP/2 error in Receive: " << strerror(-res));
                }
                PassAway();
                return;
            }
        }
    }

    bool FlushH2Output() {
        while (H2->OutputOffset < H2->OutputBuffer.size()) {
            size_t size = H2->PendingWriteSize ? H2->PendingWriteSize : (H2->OutputBuffer.size() - H2->OutputOffset);
            bool read = false, write = false;
            ssize_t res = TSocketImpl::Send(H2->OutputBuffer.data() + H2->OutputOffset, size, read, write);
            if (res > 0) {
                LastActivity = NActors::TActivationContext::Now();
                H2->OutputOffset += res;
                H2->PendingWriteSize = 0;
            } else if (-res == EINTR) {
                continue;
            } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                H2->PendingWriteSize = size;
                if (PollerToken) {
                    if (!read && !write) {
                        write = true;
                    }
                    if (PollerToken->RequestNotificationAfterWouldBlock(read, write)) {
                        continue;
                    }
                }
                break;
            } else {
                ALOG_ERROR(HttpLog, GetSocketName() << "HTTP/2 error in FlushOutput: " << strerror(-res));
                H2->Error = true;
                return false;
            }
        }
        if (H2->OutputOffset == H2->OutputBuffer.size()) {
            H2->OutputBuffer.clear();
            H2->OutputOffset = 0;
        } else if (H2->OutputOffset > 0) {
            H2->OutputBuffer = H2->OutputBuffer.substr(H2->OutputOffset);
            H2->OutputOffset = 0;
        }
        return true;
    }

    static int GetPort(SocketAddressType address) {
        switch (address->SockAddr()->sa_family) {
            case AF_INET:
                return ntohs(reinterpret_cast<sockaddr_in*>(address->SockAddr())->sin_port);
            case AF_INET6:
                return ntohs(reinterpret_cast<sockaddr_in6*>(address->SockAddr())->sin6_port);
        }
        return {};
    }

    static void SetPort(SocketAddressType address, int port) {
        switch (address->SockAddr()->sa_family) {
            case AF_INET:
                reinterpret_cast<sockaddr_in*>(address->SockAddr())->sin_port = htons(port);
                break;
            case AF_INET6:
                reinterpret_cast<sockaddr_in6*>(address->SockAddr())->sin6_port = htons(port);
                break;
        }
    }

    void HandleResolving(TEvHttpProxy::TEvResolveHostResponse::TPtr& event) {
        LastActivity = NActors::TActivationContext::Now();
        if (!event->Get()->Error.empty()) {
            return FailConnection(event->Get()->Error);
        }
        Address = event->Get()->Address;
        if (GetPort(Address) == 0) {
            SetPort(Address, Request->Secure ? 443 : 80);
        }
        Connect();
    }

    void HandleConnecting(NActors::TEvPollerReady::TPtr&) {
        LastActivity = NActors::TActivationContext::Now();
        int res = TSocketImpl::GetError();
        if (res == 0) {
            OnConnect();
        } else {
            FailConnection(TStringBuilder() << strerror(res));
        }
    }

    void HandleConnecting(NActors::TEvPollerRegisterResult::TPtr& ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        PollerToken->Request(true, true);
        LastActivity = NActors::TActivationContext::Now();
        ALOG_DEBUG(HttpLog, GetSocketName() << "connecting...");
        int res = TSocketImpl::Connect(Address);
        switch (-res) {
        case 0:
            return OnConnect();
        case EINPROGRESS:
        case EAGAIN:
            // waiting for poller
            return;
        default:
            return ReplyErrorAndPassAway(strerror(-res));
        }
    }

    void HandleWaiting(TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event) {
        InitiateRequest(event);
        PerformRequest();
    }

    void HandleConnected(TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event) {
        InitiateRequest(event);
        Schedule(ConnectionTimeout, new NActors::TEvents::TEvWakeup());
        LastActivity = NActors::TActivationContext::Now();
        ALOG_DEBUG(HttpLog, GetSocketName() << "<- (" << GetRequestDebugText() << ")");
        ALOG_TRACE(HttpLog, GetSocketName() << "Request:\n" << Request->GetObfuscatedData());
        FlushOutput();
        PullInput();
    }

    void HandleConnected(NActors::TEvPollerReady::TPtr& event) {
        LastActivity = NActors::TActivationContext::Now();
        if (event->Get()->Write && RequestOwner) {
            FlushOutput();
        }
        if (event->Get()->Read) {
            if (RequestOwner) {
                PullInput();
            } else {
                CheckClose();
            }
        }
    }

    void HandleFailed(TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event) {
        Request = std::move(event->Get()->Request);
        RequestOwner = event->Sender;
        ReplyErrorAndPassAway("Failed");
    }

    void HandleTimeout() {
        TDuration inactivityTime = NActors::TActivationContext::Now() - LastActivity;
        if (inactivityTime >= ConnectionTimeout) {
            if (H2) {
                if (!H2->PendingRequests.empty()) {
                    ALOG_ERROR(HttpLog, GetSocketName() << "HTTP/2 connection timed out");
                } else {
                    ALOG_DEBUG(HttpLog, GetSocketName() << "HTTP/2 connection closed due to inactivity");
                }
                PassAway();
            } else if (RequestOwner) {
                FailConnection("Connection timed out");
            } else {
                ALOG_DEBUG(HttpLog, GetSocketName() << "connection closed due to inactivity");
                PassAway();
            }
        } else {
            Schedule(Min(ConnectionTimeout - inactivityTime, TDuration::MilliSeconds(100)), new NActors::TEvents::TEvWakeup());
        }
    }

    STATEFN(StateResolving) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHttpProxy::TEvResolveHostResponse, HandleResolving);
            cFunc(NActors::TEvents::TEvWakeup::EventType, HandleTimeout);
        }
    }

    STATEFN(StateConnecting) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvPollerReady, HandleConnecting);
            cFunc(NActors::TEvents::TEvWakeup::EventType, HandleTimeout);
            hFunc(NActors::TEvPollerRegisterResult, HandleConnecting);
        }
    }

    STATEFN(StateConnected) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvPollerReady, HandleConnected);
            cFunc(NActors::TEvents::TEvWakeup::EventType, HandleTimeout);
            hFunc(TEvHttpProxy::TEvHttpOutgoingRequest, HandleConnected);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateFailed) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHttpProxy::TEvHttpOutgoingRequest, HandleFailed);
        }
    }

    void HandleConnectedHttp2(NActors::TEvPollerReady::TPtr& event) {
        LastActivity = NActors::TActivationContext::Now();
        if (event->Get()->Write) {
            if (!FlushH2Output()) {
                PassAway();
                return;
            }
        }
        if (event->Get()->Read) {
            ReadH2Input();
        }
    }

    void HandleConnectedHttp2(TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event) {
        LastActivity = NActors::TActivationContext::Now();
        SendH2Request(std::move(event->Get()->Request), event->Sender);
        if (!FlushH2Output()) {
            PassAway();
        }
    }

    STATEFN(StateConnectedHttp2) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvPollerReady, HandleConnectedHttp2);
            cFunc(NActors::TEvents::TEvWakeup::EventType, HandleTimeout);
            hFunc(TEvHttpProxy::TEvHttpOutgoingRequest, HandleConnectedHttp2);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }
};

NActors::IActor* CreateOutgoingConnectionActor(const TActorId& owner, TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event) {
    if (event->Get()->Request->Secure) {
        return new TOutgoingConnectionActor<TSecureSocketImpl>(owner, event);
    } else {
        return new TOutgoingConnectionActor<TPlainSocketImpl>(owner, event);
    }
}

}
