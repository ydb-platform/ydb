#include "http_proxy.h"
#include "http_proxy_sock_impl.h"

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
    THashMap<THttpIncomingRequestPtr, THttpOutgoingResponsePtr> Responses;
    THttpIncomingRequestPtr CurrentRequest;
    THttpOutgoingResponsePtr CurrentResponse;
    TDeque<THttpIncomingRequestPtr> RecycledRequests;

    THPTimer InactivityTimer;
    TEvPollerReady* InactivityEvent = nullptr;

    TPollerToken::TPtr PollerToken;
    TEvHttpProxy::TEvSubscribeForCancel::TPtr CancelSubscriber;

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
        Become(&TIncomingConnectionActor::StateConnected);
        Send(SelfId(), new TEvPollerReady(nullptr, true, true));
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
                    }  else {
                        CurrentRequest = new THttpIncomingRequest(Endpoint, Address);
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
                            if (Endpoint->RateLimiter.Check(TActivationContext::Now())) {
                                ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") -> (" << GetRequestDebugText() << ")");
                                ALOG_TRACE(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") Request:\n" << CurrentRequest->GetObfuscatedData());
                                Send(Endpoint->Proxy, new TEvHttpProxy::TEvHttpIncomingRequest(CurrentRequest));
                                CurrentRequest = nullptr;
                            } else {
                                bool success = Respond(CurrentRequest->CreateResponseTooManyRequests());
                                if (!success) {
                                    return;
                                }
                                CleanupRequest(CurrentRequest);
                            }
                        } else if (CurrentRequest->IsError()) {
                            ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") -! (" << GetRequestDebugText() << ")");
                            bool success = Respond(CurrentRequest->CreateResponseBadRequest());
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
        Respond(event->Get()->Response);
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
        if (CurrentResponse != nullptr && CurrentResponse == event->Get()->DataChunk->GetResponse()) {
            CurrentResponse->AddDataChunk(event->Get()->DataChunk);
        } else {
            auto itResponse = Responses.find(event->Get()->DataChunk->GetRequest());
            if (itResponse != Responses.end() && itResponse->second == event->Get()->DataChunk->GetResponse()) {
                itResponse->second->AddDataChunk(event->Get()->DataChunk);
            } else {
                ALOG_ERROR(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - DataChunk request not found");
                return PassAway();
            }
        }
        ALOG_DEBUG(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") <- (" << GetChunkDebugText(event->Get()->DataChunk) << ")");
        ALOG_TRACE(HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") DataChunk:\n" << event->Get()->DataChunk->AsString());
        if (event->Get()->DataChunk->IsEndOfData()) {
            CancelSubscriber = nullptr;
        }
        FlushOutput();
    }

    void HandleConnected(TEvHttpProxy::TEvSubscribeForCancel::TPtr& event) {
        CancelSubscriber = std::move(event);
    }

    bool Respond(THttpOutgoingResponsePtr response) {
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
        if (request == Requests.front() && CurrentResponse == nullptr) {
            CurrentResponse = response;
            return FlushOutput();
        } else {
            // we are ahead of our pipeline
            Responses.emplace(request, response);
            return true;
        }
    }

    bool FlushOutput() {
        while (CurrentResponse != nullptr) {
            auto* buffer = CurrentResponse->GetActiveBuffer();
            size_t size = buffer->Size();
            if (size == 0) {
                if (CurrentResponse->IsDone()) {
                    Y_ABORT_UNLESS(Requests.front() == CurrentResponse->GetRequest());
                    bool close = CurrentResponse->IsConnectionClose();
                    Requests.pop_front();
                    CleanupResponse(CurrentResponse);
                    if (!Requests.empty()) {
                        auto it = Responses.find(Requests.front());
                        if (it != Responses.end()) {
                            CurrentResponse = it->second;
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
                CleanupResponse(CurrentResponse);
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
