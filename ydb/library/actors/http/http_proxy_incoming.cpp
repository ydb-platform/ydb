#include "http_proxy.h"
#include "http_proxy_sock_impl.h"

namespace NHttp {

using namespace NActors;

template <typename TSocketImpl>
class TIncomingConnectionActor : public TActor<TIncomingConnectionActor<TSocketImpl>>, public TSocketImpl, virtual public THttpConfig {
public:
    using TBase = TActor<TIncomingConnectionActor<TSocketImpl>>;
    static constexpr bool RecycleRequests = true;

    std::shared_ptr<TPrivateEndpointInfo> Endpoint;
    SocketAddressType Address;
    TList<THttpIncomingRequestPtr> Requests;
    THashMap<THttpIncomingRequestPtr, THttpOutgoingResponsePtr> Responses;
    THttpIncomingRequestPtr CurrentRequest;
    THttpOutgoingResponsePtr CurrentResponse;
    TDeque<THttpIncomingRequestPtr> RecycledRequests;

    THPTimer InactivityTimer;
    static constexpr TDuration InactivityTimeout = TDuration::Minutes(2);
    TEvPollerReady* InactivityEvent = nullptr;

    TPollerToken::TPtr PollerToken;

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

    void Die(const TActorContext& ctx) override {
        ctx.Send(Endpoint->Owner, new TEvHttpProxy::TEvHttpConnectionClosed(ctx.SelfID, std::move(RecycledRequests)));
        TSocketImpl::Shutdown();
        TBase::Die(ctx);
    }

protected:
    void Bootstrap(const TActorContext& ctx) {
        InactivityTimer.Reset();
        ctx.Schedule(InactivityTimeout, InactivityEvent = new TEvPollerReady(nullptr, false, false));
        LOG_DEBUG_S(ctx, HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") incoming connection opened");
        OnAccept(ctx);
    }

    void OnAccept(const NActors::TActorContext& ctx) {
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
                    LOG_ERROR_S(ctx, HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - error in Accept: " << strerror(-res));
                    return Die(ctx);
                }
            }
            break;
        }
        TBase::Become(&TIncomingConnectionActor::StateConnected);
        ctx.Send(ctx.SelfID, new TEvPollerReady(nullptr, true, true));
    }

    void HandleAccepting(TEvPollerRegisterResult::TPtr ev, const NActors::TActorContext& ctx) {
        PollerToken = std::move(ev->Get()->PollerToken);
        OnAccept(ctx);
    }

    void HandleAccepting(NActors::TEvPollerReady::TPtr, const NActors::TActorContext& ctx) {
        OnAccept(ctx);
    }

    void HandleConnected(TEvPollerReady::TPtr event, const TActorContext& ctx) {
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
                    LOG_DEBUG_S(ctx, HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - not enough space available");
                    return Die(ctx);
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
                            LOG_DEBUG_S(ctx, HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") -> (" << CurrentRequest->Method << " " << CurrentRequest->URL << ")");
                            ctx.Send(Endpoint->Proxy, new TEvHttpProxy::TEvHttpIncomingRequest(CurrentRequest));
                            CurrentRequest = nullptr;
                        } else if (CurrentRequest->IsError()) {
                            LOG_DEBUG_S(ctx, HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") -! (" << CurrentRequest->Method << " " << CurrentRequest->URL << ")");
                            bool success = Respond(CurrentRequest->CreateResponseBadRequest(), ctx);
                            if (!success) {
                                return;
                            }
                            CurrentRequest = nullptr;
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
                    LOG_DEBUG_S(ctx, HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed");
                    return Die(ctx);
                } else {
                    LOG_ERROR_S(ctx, HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - error in Receive: " << strerror(-res));
                    return Die(ctx);
                }
            }
        }
        if (event->Get() == InactivityEvent) {
            const TDuration passed = TDuration::Seconds(std::abs(InactivityTimer.Passed()));
            if (passed >= InactivityTimeout) {
                LOG_DEBUG_S(ctx, HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed by inactivity timeout");
                return Die(ctx); // timeout
            } else {
                ctx.Schedule(InactivityTimeout - passed, InactivityEvent = new TEvPollerReady(nullptr, false, false));
            }
        }
        if (event->Get()->Write) {
            FlushOutput(ctx);
        }
    }

    void HandleConnected(TEvPollerRegisterResult::TPtr ev, const TActorContext& /*ctx*/) {
        PollerToken = std::move(ev->Get()->PollerToken);
        PollerToken->Request(true, true);
    }

    void HandleConnected(TEvHttpProxy::TEvHttpOutgoingResponse::TPtr event, const TActorContext& ctx) {
        Respond(event->Get()->Response, ctx);
    }

    bool Respond(THttpOutgoingResponsePtr response, const TActorContext& ctx) {
        THttpIncomingRequestPtr request = response->GetRequest();
        response->Finish();
        LOG_DEBUG_S(ctx, HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") <- (" << response->Status << " " << response->Message << ")");
        if (!response->Status.StartsWith('2') && response->Status != "404") {
            static constexpr size_t MAX_LOGGED_SIZE = 1024;
            LOG_DEBUG_S(ctx, HttpLog,
                        "(#"
                        << TSocketImpl::GetRawSocket()
                        << ","
                        << Address
                        << ") Request: "
                        << request->GetObfuscatedData().substr(0, MAX_LOGGED_SIZE));
            LOG_DEBUG_S(ctx, HttpLog,
                        "(#"
                        << TSocketImpl::GetRawSocket()
                        << ","
                        << Address
                        << ") Response: "
                        << response->GetObfuscatedData().substr(0, MAX_LOGGED_SIZE));
        }
        THolder<TEvHttpProxy::TEvReportSensors> sensors(BuildIncomingRequestSensors(request, response));
        ctx.Send(Endpoint->Owner, sensors.Release());
        if (request == Requests.front() && CurrentResponse == nullptr) {
            CurrentResponse = response;
            return FlushOutput(ctx);
        } else {
            // we are ahead of our pipeline
            Responses.emplace(request, response);
            return true;
        }
    }

    bool FlushOutput(const TActorContext& ctx) {
        while (CurrentResponse != nullptr) {
            size_t size = CurrentResponse->Size();
            if (size == 0) {
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
                        LOG_ERROR_S(ctx, HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - FlushOutput request not found");
                        Die(ctx);
                        return false;
                    }
                } else {
                    if (close) {
                        LOG_DEBUG_S(ctx, HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed");
                        Die(ctx);
                        return false;
                    } else {
                        continue;
                    }
                }
            }
            bool read = false, write = false;
            ssize_t res = TSocketImpl::Send(CurrentResponse->Data(), size, read, write);
            if (res > 0) {
                CurrentResponse->ChopHead(res);
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
                LOG_ERROR_S(ctx, HttpLog, "(#" << TSocketImpl::GetRawSocket() << "," << Address << ") connection closed - error in FlushOutput: " << strerror(-res));
                Die(ctx);
                return false;
            }
        }
        return true;
    }

    STFUNC(StateAccepting) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TEvBootstrap::EventType, Bootstrap);
            HFunc(TEvPollerReady, HandleAccepting);
            HFunc(TEvPollerRegisterResult, HandleAccepting);
        }
    }

    STFUNC(StateConnected) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPollerReady, HandleConnected);
            HFunc(TEvHttpProxy::TEvHttpOutgoingResponse, HandleConnected);
            HFunc(TEvPollerRegisterResult, HandleConnected);
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
