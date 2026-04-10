#include "http_proxy.h"
#include "http_proxy_sock_impl.h"
#include "http_proxy_ssl.h"
#include "http2.h"
#include "http2_frames.h"

#include <openssl/x509.h>
#include <openssl/pem.h>

#include <type_traits>

namespace NHttp {

using namespace NActors;

template <typename TSocketImpl>
class TIncomingConnectionActor : public TActor<TIncomingConnectionActor<TSocketImpl>>, public TSocketImpl, virtual public THttpConfig {
public:
    using TBase = TActor<TIncomingConnectionActor<TSocketImpl>>;
    using TBase::Send;
    using TBase::Schedule;
    using TBase::SelfId;
    using TBase::Become;

    static constexpr bool RecycleRequests = true;

    std::shared_ptr<TPrivateEndpointInfo> Endpoint;
    SocketAddressType Address;
    TList<THttpIncomingRequestPtr> Requests;

    struct TResponseState {
        THttpOutgoingResponsePtr Response;
        NActors::TActorId ProgressNotificationActor; // The actor to send progress notifications to
        ui64 ProgressNotificationCookie = 0; // Cookie for progress notifications
        ui64 ProgressNotificationBytes = 0; // The byte interval for progress notifications
        ui64 ProgressBytes = 0; // Total bytes sent so far
        ui64 ProgressChunks = 0; // Total data chunks sent so far

        operator bool() const {
            return Response != nullptr;
        }
    };

    THashMap<THttpIncomingRequestPtr, TResponseState> Responses;
    THttpIncomingRequestPtr CurrentRequest;
    TResponseState CurrentResponse;
    TDeque<THttpIncomingRequestPtr> RecycledRequests;

    // HTTP/2 state (allocated on demand when protocol is detected as HTTP/2)
    struct THttp2State {
        std::unique_ptr<NHttp2::TSession> Session;

        struct TStreamState {
            THttpIncomingRequestPtr Request;
            uint32_t StreamId = 0;
        };
        THashMap<uint32_t, TStreamState> StreamStates;
        TVector<TEvHttpProxy::TEvSubscribeForCancel::TPtr> CancelSubscribers;

        TString OutputBuffer;
        size_t OutputOffset = 0;
        size_t PendingWriteSize = 0; // for SSL_write retry with same length
        bool Error = false;
    };
    std::unique_ptr<THttp2State> H2;

    THPTimer InactivityTimer;
    TEvPollerReady* InactivityEvent = nullptr;

    TPollerToken::TPtr PollerToken;
    TEvHttpProxy::TEvSubscribeForCancel::TPtr CancelSubscriber;

    TString MTlsClientCertificate;

    // Protocol detection buffer (used only during StateDetecting for h2c)
    char DetectionBuffer[24]; // NHttp2::CONNECTION_PREFACE.size()
    size_t DetectionSize = 0;

    TIncomingConnectionActor(
            std::shared_ptr<TPrivateEndpointInfo> endpoint,
            TIntrusivePtr<TSocketDescriptor> socket,
            SocketAddressType address,
            THttpIncomingRequestPtr recycledRequest = nullptr)
        : TBase(&TIncomingConnectionActor::StateAccepting)
        , TSocketImpl(std::move(socket))
        , Endpoint(std::move(endpoint))
        , Address(address)
    {
        if (recycledRequest != nullptr) {
            RecycledRequests.emplace_back(std::move(recycledRequest));
        }
        TSocketImpl::SetNonBlock();
    }

    static constexpr char ActorName[] = "IN_CONNECTION_ACTOR";

    void CleanupRequest(THttpIncomingRequestPtr& request) {
        if (RecycleRequests) {
            request->Clear();
            RecycledRequests.push_back(std::move(request));
        } else {
            request = nullptr;
        }
    }

    void CleanupResponse(THttpOutgoingResponsePtr& response) {
        CleanupRequest(response->Request);
        // TODO: maybe recycle too?
        response = nullptr;
    }

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parent) override {
        return new IEventHandle(self, parent, new TEvents::TEvBootstrap());
    }

    void PassAway() override {
        if (CancelSubscriber) {
            Send(CancelSubscriber->Sender, new TEvHttpProxy::TEvRequestCancelled(), 0, CancelSubscriber->Cookie);
        }
        if (H2) {
            if (H2->Session && !H2->Error) {
                H2->Session->SendGoaway();
                FlushHttp2Output(); // best-effort, may fail silently
            }
            for (auto& subscriber : H2->CancelSubscribers) {
                Send(subscriber->Sender, new TEvHttpProxy::TEvRequestCancelled(), 0, subscriber->Cookie);
            }
            H2->CancelSubscribers.clear();
            H2->StreamStates.clear();
        }
        Send(Endpoint->Owner, new TEvHttpProxy::TEvHttpIncomingConnectionClosed(SelfId(), std::move(RecycledRequests)));
        TSocketImpl::Shutdown();
        TBase::PassAway();
    }

protected:
    void Bootstrap() {
        InactivityTimer.Reset();
        Schedule(Endpoint->InactivityTimeout, InactivityEvent = new TEvPollerReady(nullptr, false, false));
        ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") incoming connection opened");
        OnAccept();
    }

    void OnAccept() {
        int res;
        bool read = false, write = false;
        for (;;) {
            if ((res = TSocketImpl::OnAccept(Endpoint, read, write)) != 1) {
                if (-res == EAGAIN) {
                    if (PollerToken && PollerToken->RequestReadNotificationAfterWouldBlock()) {
                        continue;
                    }
                    return; // wait for further notifications
                } else {
                    ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - error in Accept: " << strerror(-res));
                    return PassAway();
                }
            }
            break;
        }
        ExtractClientCertificate();
        // Check for HTTP/2 via ALPN negotiation after TLS handshake
        if constexpr (std::is_same_v<TSocketImpl, TSecureSocketImpl>) {
            if (Endpoint->AllowHttp2 && this->GetAlpnProtocol() == "h2") {
                ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") ALPN negotiated h2, switching to HTTP/2");
                SwitchToHttp2();
                return;
            }
        }
        // For plain connections with HTTP/2 enabled, detect h2c preface
        if constexpr (std::is_same_v<TSocketImpl, TPlainSocketImpl>) {
            if (Endpoint->AllowHttp2) {
                Become(&TIncomingConnectionActor::StateDetecting);
                TryDetectProtocol();
                return;
            }
        }
        Become(&TIncomingConnectionActor::StateConnected);
        Send(SelfId(), new TEvPollerReady(nullptr, true, true));
    }

    void ExtractClientCertificate() {
        if constexpr (std::is_base_of_v<TSecureSocketImpl, TSocketImpl>) {
            if (Endpoint->Secure && this->Ssl) {
                if (TSslHelpers::TSslHolder<X509> peerCert{SSL_get_peer_certificate(this->Ssl.Get())}) {
                    TSslHelpers::TSslHolder<BIO> bio(BIO_new(BIO_s_mem()));
                    if (bio && PEM_write_bio_X509(bio.Get(), peerCert.Get()) > 0) {
                        char* pemData = nullptr;
                        size_t pemLen = BIO_get_mem_data(bio.Get(), &pemData);
                        if (pemData && pemLen > 0) {
                            MTlsClientCertificate = TString(pemData, pemLen);
                        }
                    }
                } // else: client did not provide certificate; that's OK
            }
        }
    }

    void TryDetectProtocol() {
        for (;;) {
            size_t toRead = sizeof(DetectionBuffer) - DetectionSize;
            bool read = false, write = false;
            ssize_t res = TSocketImpl::Recv(DetectionBuffer + DetectionSize, toRead, read, write);
            if (res > 0) {
                InactivityTimer.Reset();
                DetectionSize += static_cast<size_t>(res);
                if (DetectionSize >= 3) {
                    TStringBuf peek(DetectionBuffer, DetectionSize);
                    if (peek.StartsWith(NHttp2::CONNECTION_PREFACE.Head(DetectionSize))) {
                        if (DetectionSize >= NHttp2::CONNECTION_PREFACE.size()) {
                            // Full preface matched -> HTTP/2
                            SwitchToHttp2();
                            // Feed the already-read preface to the session
                            H2->Session->Feed(DetectionBuffer, DetectionSize);
                            return;
                        }
                        // Partial match, need more data
                        if (PollerToken) {
                            if (!read && !write) {
                                read = true;
                            }
                            if (PollerToken->RequestNotificationAfterWouldBlock(read, write)) {
                                continue;
                            }
                        }
                        return;
                    }
                    // Not HTTP/2 preface -> HTTP/1.1
                    SwitchToHttp1();
                    return;
                }
                // Less than 3 bytes, need more
                if (PollerToken) {
                    if (!read && !write) {
                        read = true;
                    }
                    if (PollerToken->RequestNotificationAfterWouldBlock(read, write)) {
                        continue;
                    }
                }
                return;
            } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                if (PollerToken) {
                    if (!read && !write) {
                        read = true;
                    }
                    if (PollerToken->RequestReadNotificationAfterWouldBlock()) {
                        continue;
                    }
                }
                return;
            } else if (-res == EINTR) {
                continue;
            } else {
                PassAway();
                return;
            }
        }
    }

    void SwitchToHttp1() {
        // Feed detection bytes into CurrentRequest before switching to HTTP/1.1
        if (RecycleRequests && !RecycledRequests.empty()) {
            CurrentRequest = std::move(RecycledRequests.front());
            RecycledRequests.pop_front();
            CurrentRequest->Address = Address;
            CurrentRequest->Endpoint = Endpoint;
            CurrentRequest->MTlsClientCertificate = MTlsClientCertificate;
        } else {
            CurrentRequest = new THttpIncomingRequest(Endpoint, Address, MTlsClientCertificate);
        }
        CurrentRequest->EnsureEnoughSpaceAvailable();
        std::memcpy(CurrentRequest->Pos(), DetectionBuffer, DetectionSize);
        CurrentRequest->Advance(DetectionSize);
        Become(&TIncomingConnectionActor::StateConnected);
        Send(SelfId(), new TEvPollerReady(nullptr, true, true));
    }

    void SwitchToHttp2() {
        ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") switching to HTTP/2");
        InitHttp2Session();
        Become(&TIncomingConnectionActor::StateConnectedHttp2);
        Send(SelfId(), new TEvPollerReady(nullptr, true, true));
    }

    // RFC 7540 §3.2: HTTP/1.1 Upgrade to h2c
    // Returns true if upgrade was initiated (caller should return immediately)
    bool TryUpgradeToHttp2(THttpIncomingRequestPtr& request) {
        if constexpr (!std::is_same_v<TSocketImpl, TPlainSocketImpl>) {
            return false; // h2c upgrade only on plain connections
        }
        if (!Endpoint->AllowHttp2) {
            return false;
        }
        THeaders headers(request->Headers);
        TStringBuf upgrade = headers.Get("Upgrade");
        if (upgrade != "h2c") {
            return false;
        }

        ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") HTTP/1.1 -> h2c upgrade");

        // Clean up HTTP/1.1 request pipeline state
        THttpIncomingRequestPtr savedRequest = request;
        Requests.pop_back(); // remove the request we just pushed
        CurrentRequest = nullptr;

        // Initialize HTTP/2 session (generates SETTINGS in H2->OutputBuffer)
        InitHttp2Session();

        // Prepend 101 Switching Protocols before the SETTINGS frame
        static const TString response101 =
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Connection: Upgrade\r\n"
            "Upgrade: h2c\r\n"
            "\r\n";
        H2->OutputBuffer.insert(H2->OutputBuffer.begin(), response101.begin(), response101.end());

        if (!FlushHttp2Output()) { // send 101 + server connection preface (SETTINGS)
            PassAway();
            return true;
        }

        // Deliver the original request as HTTP/2 stream 1 (per RFC 7540 §3.2)
        // Stream 1 is implicitly half-closed (remote) since the request is complete
        const uint32_t upgradeStreamId = 1;
        H2->Session->RegisterUpgradeStream(savedRequest->URL, savedRequest->Host);
        typename THttp2State::TStreamState state;
        state.Request = savedRequest;
        state.StreamId = upgradeStreamId;
        H2->StreamStates[upgradeStreamId] = std::move(state);

        ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") HTTP/2 stream " << upgradeStreamId << " -> (" << savedRequest->Method << " " << savedRequest->URL << ") (upgrade)");
        Send(Endpoint->Proxy, new TEvHttpProxy::TEvHttpIncomingRequest(savedRequest));

        Become(&TIncomingConnectionActor::StateConnectedHttp2);
        Send(SelfId(), new TEvPollerReady(nullptr, true, true));
        return true;
    }

    void InitHttp2Session() {
        H2 = std::make_unique<THttp2State>();
        NHttp2::TSessionCallbacks callbacks;
        callbacks.OnRequest = [this](uint32_t streamId, NHttp2::TStream& stream) {
            OnHttp2Request(streamId, stream);
        };
        callbacks.OnSend = [this](TString data) {
            H2->OutputBuffer.append(data);
        };
        callbacks.OnError = [this](TString error) {
            ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") HTTP/2 error: " << error);
            H2->Error = true;
        };
        callbacks.OnGoaway = [this](uint32_t lastStreamId, NHttp2::EErrorCode errorCode) {
            ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") GOAWAY received, lastStreamId=" << lastStreamId << " error=" << static_cast<uint32_t>(errorCode));
        };
        H2->Session = std::make_unique<NHttp2::TSession>(NHttp2::TSession::ERole::Server, std::move(callbacks));
        H2->Session->Initialize();
    }

    void OnHttp2Request(uint32_t streamId, NHttp2::TStream& stream) {
        THttpIncomingRequestPtr request = NHttp2::StreamToIncomingRequest(stream, Endpoint, Address);
        request->MTlsClientCertificate = MTlsClientCertificate;

        typename THttp2State::TStreamState state;
        state.Request = request;
        state.StreamId = streamId;
        H2->StreamStates[streamId] = std::move(state);

        if (Endpoint->RateLimiter.Check(TActivationContext::Now())) {
            ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") HTTP/2 stream " << streamId << " -> (" << request->Method << " " << request->URL << ")");
            Send(Endpoint->Proxy, new TEvHttpProxy::TEvHttpIncomingRequest(request));
        } else {
            auto response = request->CreateResponseTooManyRequests();
            SendHttp2Response(streamId, response);
            H2->StreamStates.erase(streamId);
        }
    }

    void SendHttp2Response(uint32_t streamId, THttpOutgoingResponsePtr response, bool endStream = true) {
        TString status;
        TVector<std::pair<TString, TString>> headers;
        TString body;
        NHttp2::OutgoingResponseToStream(response, status, headers, body);
        H2->Session->SendResponse(streamId, status, headers, body, endStream);
    }

    static constexpr size_t READ_BUFFER_SIZE = 64 * 1024;

    void ReadHttp2Input() {
        char readBuffer[READ_BUFFER_SIZE];
        for (;;) {
            bool read = false, write = false;
            ssize_t res = TSocketImpl::Recv(readBuffer, READ_BUFFER_SIZE, read, write);
            if (res > 0) {
                InactivityTimer.Reset();
                H2->Session->Feed(readBuffer, static_cast<size_t>(res));
                if (H2->Error) {
                    FlushHttp2Output();
                    return PassAway();
                }
                if (!FlushHttp2Output()) {
                    return PassAway();
                }
            } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                if (PollerToken) {
                    if (!read && !write) {
                        read = true;
                    }
                    if (PollerToken->RequestNotificationAfterWouldBlock(read, write)) {
                        continue;
                    }
                }
                break;
            } else if (-res == EINTR) {
                continue;
            } else if (!res) {
                ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") HTTP/2 connection closed");
                return PassAway();
            } else {
                ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") HTTP/2 connection closed - error in Receive: " << strerror(-res));
                return PassAway();
            }
        }
    }

    bool FlushHttp2Output() {
        while (H2->OutputOffset < H2->OutputBuffer.size()) {
            size_t size = H2->PendingWriteSize ? H2->PendingWriteSize : (H2->OutputBuffer.size() - H2->OutputOffset);
            bool read = false, write = false;
            ssize_t res = TSocketImpl::Send(H2->OutputBuffer.data() + H2->OutputOffset, size, read, write);
            if (res > 0) {
                InactivityTimer.Reset();
                H2->OutputOffset += res;
                H2->PendingWriteSize = 0;
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
                ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") HTTP/2 connection closed - error in FlushOutput: " << strerror(-res));
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

    void HandleDetecting(TEvPollerReady::TPtr& event) {
        if (event->Get() == InactivityEvent) {
            const TDuration passed = TDuration::Seconds(std::abs(InactivityTimer.Passed()));
            if (passed >= Endpoint->InactivityTimeout) {
                ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed by inactivity timeout during detection");
                return PassAway();
            } else {
                Schedule(Endpoint->InactivityTimeout - passed, InactivityEvent = new TEvPollerReady(nullptr, false, false));
            }
            return;
        }
        TryDetectProtocol();
    }

    void HandleDetecting(TEvPollerRegisterResult::TPtr& ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        TryDetectProtocol();
    }

    void HandleHttp2(TEvPollerReady::TPtr& event) {
        if (event->Get()->Read) {
            ReadHttp2Input();
        }
        if (event->Get() == InactivityEvent) {
            const TDuration passed = TDuration::Seconds(std::abs(InactivityTimer.Passed()));
            if (passed >= Endpoint->InactivityTimeout) {
                ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") HTTP/2 connection closed by inactivity timeout");
                if (H2 && H2->Session) {
                    H2->Session->SendGoaway();
                }
                FlushHttp2Output(); // best-effort
                return PassAway();
            } else {
                Schedule(Endpoint->InactivityTimeout - passed, InactivityEvent = new TEvPollerReady(nullptr, false, false));
            }
        }
        if (event->Get()->Write) {
            if (!FlushHttp2Output()) {
                return PassAway();
            }
        }
    }

    void HandleHttp2(TEvPollerRegisterResult::TPtr& ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        PollerToken->Request(true, true);
    }

    void HandleHttp2(TEvHttpProxy::TEvHttpOutgoingResponse::TPtr& event) {
        auto response = event->Get()->Response;
        THttpIncomingRequestPtr request = response->GetRequest();

        uint32_t streamId = 0;
        for (auto& [sid, state] : H2->StreamStates) {
            if (state.Request == request) {
                streamId = sid;
                break;
            }
        }

        if (streamId == 0) {
            ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") HTTP/2 response for unknown request");
            return;
        }

        ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") HTTP/2 stream " << streamId
            << " <- (" << response->Status << " " << response->Message << ")");

        THolder<TEvHttpProxy::TEvReportSensors> sensors(BuildIncomingRequestSensors(request, response));
        Send(Endpoint->Owner, sensors.Release());

        bool endStream = response->IsDone();
        SendHttp2Response(streamId, response, endStream);
        if (!FlushHttp2Output()) {
            return PassAway();
        }

        if (endStream) {
            H2->StreamStates.erase(streamId);
        }
    }

    void HandleHttp2(TEvHttpProxy::TEvHttpOutgoingDataChunk::TPtr& event) {
        if (event->Get()->Error) {
            ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") HTTP/2 DataChunk error: " << event->Get()->Error);
            return PassAway();
        }

        auto dataChunk = event->Get()->DataChunk;
        THttpIncomingRequestPtr request = dataChunk->GetRequest();

        uint32_t streamId = 0;
        for (auto& [sid, state] : H2->StreamStates) {
            if (state.Request == request) {
                streamId = sid;
                break;
            }
        }

        if (streamId == 0) {
            ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") HTTP/2 data chunk for unknown stream");
            return;
        }

        uint8_t flags = dataChunk->IsEndOfData() ? NHttp2::NFrameFlag::END_STREAM : NHttp2::NFrameFlag::NONE;
        TStringBuf data(dataChunk->Data(), dataChunk->Size());
        if (!data.empty() || dataChunk->IsEndOfData()) {
            H2->OutputBuffer.append(NHttp2::TFrameBuilder::BuildData(streamId, flags, data));
            if (!FlushHttp2Output()) {
                return PassAway();
            }
        }

        if (dataChunk->IsEndOfData()) {
            H2->StreamStates.erase(streamId);
        }
    }

    void HandleHttp2(TEvHttpProxy::TEvSubscribeForCancel::TPtr& event) {
        H2->CancelSubscribers.push_back(std::move(event));
    }

    void HandleAccepting(TEvPollerRegisterResult::TPtr& ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        OnAccept();
    }

    void HandleAccepting(NActors::TEvPollerReady::TPtr&) {
        OnAccept();
    }

    TString GetRequestDebugText() {
        TStringBuilder text;
        if (CurrentRequest) {
            text << CurrentRequest->Method << " " << CurrentRequest->URL;
            if (CurrentRequest->Body) {
                text << ", " << CurrentRequest->Body.Size() << " bytes";
            }
        }
        return text;
    }

    void HandleConnected(TEvPollerReady::TPtr& event) {
        if (event->Get()->Read) {
            for (;;) {
                if (CurrentRequest == nullptr) {
                    if (RecycleRequests && !RecycledRequests.empty()) {
                        CurrentRequest = std::move(RecycledRequests.front());
                        RecycledRequests.pop_front();
                        CurrentRequest->Address = Address;
                        CurrentRequest->Endpoint = Endpoint;
                        CurrentRequest->MTlsClientCertificate = MTlsClientCertificate;
                    }  else {
                        CurrentRequest = new THttpIncomingRequest(Endpoint, Address, MTlsClientCertificate);
                    }
                }
                if (!CurrentRequest->EnsureEnoughSpaceAvailable()) {
                    ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - not enough space available");
                    return PassAway();
                }
                ssize_t need = CurrentRequest->Avail();
                bool read = false, write = false;
                ssize_t res = TSocketImpl::Recv(CurrentRequest->Pos(), need, read, write);
                if (res > 0) {
                    InactivityTimer.Reset();
                    CurrentRequest->Advance(res);
                    if (CurrentRequest->IsDone()) {
                        Requests.emplace_back(CurrentRequest);
                        CurrentRequest->Timer.Reset();
                        if (CurrentRequest->IsReady()) {
                            if (TryUpgradeToHttp2(CurrentRequest)) {
                                return;
                            }
                            if (Endpoint->RateLimiter.Check(TActivationContext::Now())) {
                                ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") -> (" << GetRequestDebugText() << ")");
                                ALOG_TRACE(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") Request:\n" << CurrentRequest->GetObfuscatedData());
                                Send(Endpoint->Proxy, new TEvHttpProxy::TEvHttpIncomingRequest(CurrentRequest));
                                CurrentRequest = nullptr;
                            } else {
                                bool success = Respond({
                                    .Response = CurrentRequest->CreateResponseTooManyRequests()
                                });
                                if (!success) {
                                    return;
                                }
                                CleanupRequest(CurrentRequest);
                            }
                        } else if (CurrentRequest->IsError()) {
                            ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") -! (" << GetRequestDebugText() << ")");
                            bool success = Respond({
                                .Response = CurrentRequest->CreateResponseBadRequest()
                            });
                            if (!success) {
                                return;
                            }
                            CleanupRequest(CurrentRequest);
                        }
                    }
                } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                    if (PollerToken) {
                        if (!read && !write) {
                            read = true;
                        }
                        if (PollerToken->RequestNotificationAfterWouldBlock(read, write)) {
                            continue;
                        }
                    }
                    break;
                } else if (-res == EINTR) {
                    continue;
                } else if (!res) {
                    // connection closed
                    ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed");
                    return PassAway();
                } else {
                    ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - error in Receive: " << strerror(-res));
                    return PassAway();
                }
            }
        }
        if (event->Get() == InactivityEvent) {
            const TDuration passed = TDuration::Seconds(std::abs(InactivityTimer.Passed()));
            if (passed >= Endpoint->InactivityTimeout) {
                ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed by inactivity timeout");
                return PassAway(); // timeout
            } else {
                Schedule(Endpoint->InactivityTimeout - passed, InactivityEvent = new TEvPollerReady(nullptr, false, false));
            }
        }
        if (event->Get()->Write) {
            FlushOutput();
        }
    }

    void HandleConnected(TEvPollerRegisterResult::TPtr& ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        PollerToken->Request(true, true);
    }

    void HandleConnected(TEvHttpProxy::TEvHttpOutgoingResponse::TPtr& event) {
        if (event->Get()->Response->IsDone()) {
            CancelSubscriber = nullptr;
        }
        Respond({
            .Response = event->Get()->Response,
            .ProgressNotificationActor = event->Get()->ProgressNotificationBytes > 0 ? event->Sender : TActorId(),
            .ProgressNotificationCookie = event->Cookie,
            .ProgressNotificationBytes = event->Get()->ProgressNotificationBytes,
        });
    }

    static TString GetChunkDebugText(THttpOutgoingDataChunkPtr chunk) {
        TStringBuilder text;
        if (chunk->DataSize) {
            text << "data chunk " << chunk->DataSize << " bytes";
        }
        if (chunk->DataSize && chunk->IsEndOfData()) {
            text << ", ";
        }
        if (chunk->IsEndOfData()) {
            text << "end of stream";
        }
        return text;
    }

    static TString GetResponseDebugText(THttpOutgoingResponsePtr response) {
        TStringBuilder text;
        if (response) {
            text << response->Status << " " << response->Message;
            if (response->Body) {
                text << ", " << response->Body.Size() << " bytes";
            }
        }
        return text;
    }

    void HandleConnected(TEvHttpProxy::TEvHttpOutgoingDataChunk::TPtr& event) {
        if (event->Get()->Error) {
            ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - DataChunk error: " << event->Get()->Error);
            return PassAway();
        }
        if (CurrentResponse && CurrentResponse.Response == event->Get()->DataChunk->GetResponse()) {
            CurrentResponse.Response->AddDataChunk(event->Get()->DataChunk);
        } else {
            auto itResponse = Responses.find(event->Get()->DataChunk->GetRequest());
            if (itResponse != Responses.end() && itResponse->second.Response == event->Get()->DataChunk->GetResponse()) {
                itResponse->second.Response->AddDataChunk(event->Get()->DataChunk);
            } else {
                ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - DataChunk request not found");
                return PassAway();
            }
        }
        ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") <- (" << GetChunkDebugText(event->Get()->DataChunk) << ")");
        if (CurrentResponse.Response->CompressContext) {
            ALOG_TRACE(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") DataChunk:\n(compressed data)");
        } else {
            ALOG_TRACE(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") DataChunk:\n" << event->Get()->DataChunk->AsString());
        }
        if (event->Get()->DataChunk->IsEndOfData()) {
            CancelSubscriber = nullptr;
        }
        FlushOutput();
    }

    void HandleConnected(TEvHttpProxy::TEvSubscribeForCancel::TPtr& event) {
        CancelSubscriber = std::move(event);
    }

    bool Respond(TResponseState state) {
        THttpOutgoingResponsePtr response = state.Response;
        THttpIncomingRequestPtr request = response->GetRequest();
        ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") <- ("
            << GetResponseDebugText(response) << (response->IsDone() ? ")" : ") (incomplete)"));
        if (!response->Status.StartsWith('2') && !response->Status.StartsWith('3') && response->Status != "404") {
            static constexpr size_t MAX_LOGGED_SIZE = 1024;
            ALOG_DEBUG(HttpLog,
                        "(#"
                        << TSocketImpl::GetRawSocket()
                        << ","
                        << Address
                        << ") Request: "
                        << request->GetObfuscatedData().substr(0, MAX_LOGGED_SIZE));
            ALOG_DEBUG(HttpLog,
                        "(#"
                        << TSocketImpl::GetRawSocket()
                        << ","
                        << Address
                        << ") Response: "
                        << response->GetObfuscatedData().substr(0, MAX_LOGGED_SIZE));
        } else {
            ALOG_TRACE(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") Response:\n" << response->GetObfuscatedData());
        }
        THolder<TEvHttpProxy::TEvReportSensors> sensors(BuildIncomingRequestSensors(request, response));
        Send(Endpoint->Owner, sensors.Release());
        if (Requests.empty()) {
            ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - no request found for response");
            PassAway();
            return false; // no request to respond to
        }
        if (request == Requests.front() && !CurrentResponse) {
            CurrentResponse = std::move(state);
            return FlushOutput();
        } else {
            // we are ahead of our pipeline
            Responses.emplace(request, std::move(state));
            return true;
        }
    }

    bool FlushOutput() {
        while (CurrentResponse) {
            auto& response = CurrentResponse.Response;
            auto* buffer = response->GetActiveBuffer();
            size_t size = buffer->Size();
            if (size == 0) {
                if (response->IsDone()) {
                    Y_ABORT_UNLESS(Requests.front() == response->GetRequest());
                    bool close = response->IsConnectionClose();
                    Requests.pop_front();
                    CleanupResponse(response);
                    if (!Requests.empty()) {
                        auto it = Responses.find(Requests.front());
                        if (it != Responses.end()) {
                            CurrentResponse = std::move(it->second);
                            Responses.erase(it);
                            continue;
                        } else {
                            ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - FlushOutput request not found");
                            PassAway();
                            return false;
                        }
                    } else {
                        if (close) {
                            ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed");
                            PassAway();
                            return false;
                        } else {
                            continue;
                        }
                    }
                } else {
                    return true; // we don't have data to send, but the response is not done yet
                }
            }
            bool read = false, write = false;
            ssize_t res = TSocketImpl::Send(buffer->Data(), size, read, write);
            if (res > 0) {
                InactivityTimer.Reset();
                buffer->ChopHead(res);
                if (CurrentResponse.ProgressNotificationBytes > 0) {
                    bool needToNotify = false;
                    ui64 progressBefore = CurrentResponse.ProgressBytes / CurrentResponse.ProgressNotificationBytes;
                    CurrentResponse.ProgressBytes += res;
                    if (buffer->Size() == 0) { // end of headers / chunk
                        needToNotify = true;
                        if (buffer != response) {
                            // If buffer != response, this means we are at the end of a data chunk (not headers).
                            // This comparison distinguishes between the headers buffer (buffer == response)
                            // and data chunk buffers (buffer != response).
                            CurrentResponse.ProgressChunks++;
                        }
                    } else {
                        ui64 progressAfter = CurrentResponse.ProgressBytes / CurrentResponse.ProgressNotificationBytes;
                        if (progressAfter != progressBefore) {
                            needToNotify = true;
                        }
                    }
                    if (needToNotify) {
                        Send(CurrentResponse.ProgressNotificationActor,
                            new TEvHttpProxy::TEvHttpOutgoingResponseProgress(CurrentResponse.ProgressBytes, CurrentResponse.ProgressChunks),
                            0, CurrentResponse.ProgressNotificationCookie);
                    }
                }
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
                CleanupResponse(response);
                ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - error in FlushOutput: " << strerror(-res));
                PassAway();
                return false;
            }
        }
        return true;
    }

    STATEFN(StateAccepting) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvBootstrap::EventType, Bootstrap);
            hFunc(TEvPollerReady, HandleAccepting);
            hFunc(TEvPollerRegisterResult, HandleAccepting);
        }
    }

    STATEFN(StateConnected) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPollerReady, HandleConnected);
            hFunc(TEvHttpProxy::TEvHttpOutgoingResponse, HandleConnected);
            hFunc(TEvHttpProxy::TEvHttpOutgoingDataChunk, HandleConnected);
            hFunc(TEvHttpProxy::TEvSubscribeForCancel, HandleConnected);
            hFunc(TEvPollerRegisterResult, HandleConnected);
        }
    }

    STATEFN(StateDetecting) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPollerReady, HandleDetecting);
            hFunc(TEvPollerRegisterResult, HandleDetecting);
        }
    }

    STATEFN(StateConnectedHttp2) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPollerReady, HandleHttp2);
            hFunc(TEvHttpProxy::TEvHttpOutgoingResponse, HandleHttp2);
            hFunc(TEvHttpProxy::TEvHttpOutgoingDataChunk, HandleHttp2);
            hFunc(TEvHttpProxy::TEvSubscribeForCancel, HandleHttp2);
            hFunc(TEvPollerRegisterResult, HandleHttp2);
        }
    }
};

IActor* CreateIncomingConnectionActor(
        std::shared_ptr<TPrivateEndpointInfo> endpoint,
        TIntrusivePtr<TSocketDescriptor> socket,
        THttpConfig::SocketAddressType address,
        THttpIncomingRequestPtr recycledRequest) {
    if (endpoint->Secure) {
        return new TIncomingConnectionActor<TSecureSocketImpl>(std::move(endpoint), std::move(socket), address, std::move(recycledRequest));
    } else {
        return new TIncomingConnectionActor<TPlainSocketImpl>(std::move(endpoint), std::move(socket), address, std::move(recycledRequest));
    }
}

}
