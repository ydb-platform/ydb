#include <library/cpp/actors/core/events.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include "http_proxy.h"

namespace NHttp {

class THttpProxy : public NActors::TActorBootstrapped<THttpProxy>, public THttpConfig {
public:
    IActor* AddListeningPort(TEvHttpProxy::TEvAddListeningPort::TPtr event, const NActors::TActorContext& ctx) {
        IActor* listeningSocket = CreateHttpAcceptorActor(ctx.SelfID, Poller);
        TActorId acceptorId = ctx.Register(listeningSocket);
        ctx.Send(event->Forward(acceptorId));
        Acceptors.emplace_back(acceptorId);
        return listeningSocket;
    }

    IActor* AddOutgoingConnection(const TString& address, bool secure, const NActors::TActorContext& ctx) {
        IActor* connectionSocket = CreateOutgoingConnectionActor(ctx.SelfID, address, secure, Poller);
        TActorId connectionId = ctx.Register(connectionSocket);
        Connections.emplace(connectionId);
        return connectionSocket;
    }

    void Bootstrap(const NActors::TActorContext& ctx) {
        Poller = ctx.Register(NActors::CreatePollerActor());
        Become(&THttpProxy::StateWork);
    }

    THttpProxy(NMonitoring::TMetricRegistry& sensors)
        : Sensors(sensors)
    {}

protected:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHttpProxy::TEvAddListeningPort, Handle);
            HFunc(TEvHttpProxy::TEvRegisterHandler, Handle);
            HFunc(TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            HFunc(TEvHttpProxy::TEvHttpOutgoingRequest, Handle);
            HFunc(TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvHttpProxy::TEvHttpOutgoingResponse, Handle);
            HFunc(TEvHttpProxy::TEvHttpAcceptorClosed, Handle);
            HFunc(TEvHttpProxy::TEvHttpConnectionClosed, Handle);
            HFunc(TEvHttpProxy::TEvResolveHostRequest, Handle);
            HFunc(TEvHttpProxy::TEvReportSensors, Handle);
            HFunc(NActors::TEvents::TEvPoison, Handle);
        }
    }

    void PassAway() override {
        Send(Poller, new NActors::TEvents::TEvPoisonPill());
        for (const NActors::TActorId& connection : Connections) {
            Send(connection, new NActors::TEvents::TEvPoisonPill());
        }
        for (const NActors::TActorId& acceptor : Acceptors) {
            Send(acceptor, new NActors::TEvents::TEvPoisonPill());
        }
        NActors::TActorBootstrapped<THttpProxy>::PassAway();
    }

    void Handle(TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        TStringBuf url = event->Get()->Request->URL.Before('?');
        THashMap<TString, TActorId>::iterator it;
        while (!url.empty()) {
            it = Handlers.find(url);
            if (it != Handlers.end()) {
                ctx.Send(event->Forward(it->second));
                return;
            } else {
                if (url.EndsWith('/')) {
                    url.Trunc(url.size() - 1);
                }
                size_t pos = url.rfind('/');
                if (pos == TStringBuf::npos) {
                    break;
                } else {
                    url = url.substr(0, pos + 1);
                }
            }
        }
        ctx.Send(event->Sender, new TEvHttpProxy::TEvHttpOutgoingResponse(event->Get()->Request->CreateResponseNotFound()));
    }

    void Handle(TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        Y_UNUSED(event);
        Y_UNUSED(ctx);
        Y_FAIL("This event shouldn't be there, it should go to the http connection owner directly");
    }

    void Handle(TEvHttpProxy::TEvHttpOutgoingResponse::TPtr event, const NActors::TActorContext& ctx) {
        Y_UNUSED(event);
        Y_UNUSED(ctx);
        Y_FAIL("This event shouldn't be there, it should go to the http connection directly");
    }

    void Handle(TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event, const NActors::TActorContext& ctx) {
        TStringBuf host(event->Get()->Request->Host);
        bool secure(event->Get()->Request->Secure);
        NActors::IActor* actor = AddOutgoingConnection(TString(host), secure, ctx);
        ctx.Send(event->Forward(actor->SelfId()));
    }

    void Handle(TEvHttpProxy::TEvAddListeningPort::TPtr event, const NActors::TActorContext& ctx) {
        AddListeningPort(event, ctx);
    }

    void Handle(TEvHttpProxy::TEvHttpAcceptorClosed::TPtr event, const NActors::TActorContext&) {
        for (auto it = Acceptors.begin(); it != Acceptors.end(); ++it) {
            if (*it == event->Get()->ConnectionID) {
                Acceptors.erase(it);
                break;
            }
        }
    }

    void Handle(TEvHttpProxy::TEvHttpConnectionClosed::TPtr event, const NActors::TActorContext&) {
        Connections.erase(event->Get()->ConnectionID);
    }

    void Handle(TEvHttpProxy::TEvRegisterHandler::TPtr event, const NActors::TActorContext&) {
        Handlers[event->Get()->Path] = event->Get()->Handler;
    }

    void Handle(TEvHttpProxy::TEvResolveHostRequest::TPtr event, const NActors::TActorContext& ctx) {
        const TString& host(event->Get()->Host);
        auto it = Hosts.find(host);
        if (it == Hosts.end() || it->second.DeadlineTime > ctx.Now()) {
            TString addressPart;
            TIpPort portPart = 0;
            CrackAddress(host, addressPart, portPart);
            if (IsIPv6(addressPart)) {
                TSockAddrInet6 address(addressPart.c_str(), portPart);
                if (it == Hosts.end()) {
                    it = Hosts.emplace(host, THostEntry()).first;
                }
                it->second.Address = address;
                it->second.DeadlineTime = ctx.Now() + HostsTimeToLive;
            } else {
                // TODO(xenoxeno): move to another, possible blocking actor
                try {
                    const NDns::TResolvedHost* result = NDns::CachedResolve(NDns::TResolveInfo(addressPart, portPart));
                    if (result != nullptr) {
                        auto pAddr = result->Addr.Begin();
                        while (pAddr != result->Addr.End() && pAddr->ai_family != AF_INET6) {
                            ++pAddr;
                        }
                        if (pAddr == result->Addr.End()) {
                            ctx.Send(event->Sender, new TEvHttpProxy::TEvResolveHostResponse("Invalid address family resolved"));
                            return;
                        }
                        TSockAddrInet6 address = {};
                        static_cast<sockaddr_in6&>(address) = *reinterpret_cast<sockaddr_in6*>(pAddr->ai_addr);
                        LOG_DEBUG_S(ctx, HttpLog, "Host " << host << " resolved to " << address.ToString());
                        if (it == Hosts.end()) {
                            it = Hosts.emplace(host, THostEntry()).first;
                        }
                        it->second.Address = address;
                        it->second.DeadlineTime = ctx.Now() + HostsTimeToLive;
                    } else {
                        ctx.Send(event->Sender, new TEvHttpProxy::TEvResolveHostResponse("Error resolving host"));
                        return;
                    }
                }
                catch (const yexception& e) {
                    ctx.Send(event->Sender, new TEvHttpProxy::TEvResolveHostResponse(e.what()));
                    return;
                }
            }
        }
        ctx.Send(event->Sender, new TEvHttpProxy::TEvResolveHostResponse(it->first, it->second.Address));
    }

    void Handle(TEvHttpProxy::TEvReportSensors::TPtr event, const NActors::TActorContext&) {
        const TEvHttpProxy::TEvReportSensors& sensors(*event->Get());
        const static TString urlNotFound = "not-found";
        const TString& url = (sensors.Status == "404" ? urlNotFound : sensors.Url);

        Sensors.Rate({
                         {"sensor", "count"},
                         {"direction", sensors.Direction},
                         {"peer", sensors.Host},
                         {"url", url},
                         {"status", sensors.Status}
                     })->Inc();
        Sensors.HistogramRate({
                                  {"sensor", "time_us"},
                                  {"direction", sensors.Direction},
                                  {"peer", sensors.Host},
                                  {"url", url},
                                  {"status", sensors.Status}
                              },
                              NMonitoring::ExplicitHistogram({1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 30000, 60000}))->Record(sensors.Time.MicroSeconds());
        Sensors.HistogramRate({
                                  {"sensor", "time_ms"},
                                  {"direction", sensors.Direction},
                                  {"peer", sensors.Host},
                                  {"url", url},
                                  {"status", sensors.Status}
                              },
                              NMonitoring::ExplicitHistogram({1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 30000, 60000}))->Record(sensors.Time.MilliSeconds());
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr, const NActors::TActorContext&) {
        PassAway();
    }

    NActors::TActorId Poller;
    TVector<TActorId> Acceptors;

    struct THostEntry {
        TSockAddrInet6 Address;
        TInstant DeadlineTime;
    };

    static constexpr TDuration HostsTimeToLive = TDuration::Seconds(60);

    THashMap<TString, THostEntry> Hosts;
    THashMap<TString, TActorId> Handlers;
    THashSet<TActorId> Connections; // outgoing
    NMonitoring::TMetricRegistry& Sensors;
};

TEvHttpProxy::TEvReportSensors* BuildOutgoingRequestSensors(const THttpOutgoingRequestPtr& request, const THttpIncomingResponsePtr& response) {
    return new TEvHttpProxy::TEvReportSensors(
            "out",
            request->Host,
            request->URL.Before('?'),
            response ? response->Status : "504",
            TDuration::Seconds(std::abs(request->Timer.Passed())) 
    );
}

TEvHttpProxy::TEvReportSensors* BuildIncomingRequestSensors(const THttpIncomingRequestPtr& request, const THttpOutgoingResponsePtr& response) {
    return new TEvHttpProxy::TEvReportSensors(
            "in",
            request->Host,
            request->URL.Before('?'),
            response->Status,
            TDuration::Seconds(std::abs(request->Timer.Passed())) 
    );
}

NActors::IActor* CreateHttpProxy(NMonitoring::TMetricRegistry& sensors) {
    return new THttpProxy(sensors);
}

bool IsIPv6(const TString& host) {
    return host.find_first_not_of(":0123456789abcdef") == TString::npos;
}

bool CrackURL(TStringBuf url, TStringBuf& scheme, TStringBuf& host, TStringBuf& uri) {
    url.TrySplit("://", scheme, url);
    auto pos = url.find('/');
    if (pos == TStringBuf::npos) {
        host = url;
    } else {
        host = url.substr(0, pos);
        uri = url.substr(pos);
    }
    return true;
}

void CrackAddress(const TString& address, TString& hostname, TIpPort& port) {
    size_t first_colon_pos = address.find(':');
    if (first_colon_pos != TString::npos) {
        size_t last_colon_pos = address.rfind(':');
        if (last_colon_pos == first_colon_pos) {
            // only one colon, simple case
            port = FromStringWithDefault<TIpPort>(address.substr(first_colon_pos + 1), 0);
            hostname = address.substr(0, first_colon_pos);
        } else {
            // ipv6?
            size_t closing_bracket_pos = address.rfind(']');
            if (closing_bracket_pos == TString::npos || closing_bracket_pos > last_colon_pos) {
                // whole address is ipv6 host
                hostname = address;
            } else {
                port = FromStringWithDefault<TIpPort>(address.substr(last_colon_pos + 1), 0);
                hostname = address.substr(0, last_colon_pos);
            }
            if (hostname.StartsWith('[') && hostname.EndsWith(']')) {
                hostname = hostname.substr(1, hostname.size() - 2);
            }
        }
    } else {
        hostname = address;
    }
}


void TrimBegin(TStringBuf& target, char delim) {
    while (!target.empty() && *target.begin() == delim) {
        target.Skip(1);
    }
}

void TrimEnd(TStringBuf& target, char delim) {
    while (!target.empty() && target.back() == delim) {
        target.Trunc(target.size() - 1);
    }
}

void Trim(TStringBuf& target, char delim) {
    TrimBegin(target, delim);
    TrimEnd(target, delim);
}

void TrimEnd(TString& target, char delim) {
    while (!target.empty() && target.back() == delim) {
        target.resize(target.size() - 1);
    }
}

}
