#include "ybus.h"

#include <util/generic/cast.h>

using namespace NBus;

namespace NBus {
    TBusSession::TBusSession() {
    }

    ////////////////////////////////////////////////////////////////////
    /// \brief Adds peer of connection into connection list

    int CompareByHost(const IRemoteAddr& l, const IRemoteAddr& r) noexcept {
        if (l.Addr()->sa_family != r.Addr()->sa_family) {
            return l.Addr()->sa_family < r.Addr()->sa_family ? -1 : +1;
        }

        switch (l.Addr()->sa_family) {
            case AF_INET: {
                return memcmp(&(((const sockaddr_in*)l.Addr())->sin_addr), &(((const sockaddr_in*)r.Addr())->sin_addr), sizeof(in_addr));
            }

            case AF_INET6: {
                return memcmp(&(((const sockaddr_in6*)l.Addr())->sin6_addr), &(((const sockaddr_in6*)r.Addr())->sin6_addr), sizeof(in6_addr));
            }
        }

        return memcmp(l.Addr(), r.Addr(), Min<size_t>(l.Len(), r.Len()));
    }

    bool operator<(const TNetAddr& a1, const TNetAddr& a2) {
        return CompareByHost(a1, a2) < 0;
    }

    size_t TBusSession::GetInFlight(const TNetAddr& addr) const {
        size_t r;
        GetInFlightBulk({addr}, MakeArrayRef(&r, 1));
        return r;
    }

    size_t TBusSession::GetConnectSyscallsNumForTest(const TNetAddr& addr) const {
        size_t r;
        GetConnectSyscallsNumBulkForTest({addr}, MakeArrayRef(&r, 1));
        return r;
    }

    // Split 'host' into name and port taking into account that host can be specified
    // as ipv6 address ('[<ipv6 address]:port' notion).
    bool SplitHost(const TString& host, TString* hostName, TString* portNum) {
        hostName->clear();
        portNum->clear();

        // Simple check that we have to deal with ipv6 address specification or
        // just host name or ipv4 address.
        if (!host.empty() && (host[0] == '[')) {
            size_t pos = host.find(']');
            if (pos < 2 || pos == TString::npos) {
                // '[]' and '[<address>' are errors.
                return false;
            }

            *hostName = host.substr(1, pos - 1);

            pos++;
            if (pos != host.length()) {
                if (host[pos] != ':') {
                    // Do not allow '[...]a' but '[...]:' is ok (as for ipv4 before
                    return false;
                }

                *portNum = host.substr(pos + 1);
            }
        } else {
            size_t pos = host.find(':');
            if (pos != TString::npos) {
                if (pos == 0) {
                    // Treat ':<port>' as errors but allow or '<host>:' for compatibility.
                    return false;
                }

                *portNum = host.substr(pos + 1);
            }

            *hostName = host.substr(0, pos);
        }

        return true;
    }

    /// registers external session on host:port with locator service
    int TBusSession::RegisterService(const char* host, TBusKey start /*= YBUS_KEYMIN*/, TBusKey end /*= YBUS_KEYMAX*/, EIpVersion ipVersion) {
        TString hostName;
        TString port;
        int portNum;

        if (!SplitHost(host, &hostName, &port)) {
            hostName = host;
        }

        if (port.empty()) {
            portNum = GetProto()->GetPort();
        } else {
            try {
                portNum = FromString<int>(port);
            } catch (const TFromStringException&) {
                return -1;
            }
        }

        TBusService service = GetProto()->GetService();
        return GetQueue()->GetLocator()->Register(service, hostName.data(), portNum, start, end, ipVersion);
    }

    TBusSession::~TBusSession() {
    }

}

TBusClientSessionPtr TBusClientSession::Create(TBusProtocol* proto, IBusClientHandler* handler, const TBusClientSessionConfig& config, TBusMessageQueuePtr queue) {
    return queue->CreateSource(proto, handler, config);
}

TBusServerSessionPtr TBusServerSession::Create(TBusProtocol* proto, IBusServerHandler* handler, const TBusServerSessionConfig& config, TBusMessageQueuePtr queue) {
    return queue->CreateDestination(proto, handler, config);
}

TBusServerSessionPtr TBusServerSession::Create(TBusProtocol* proto, IBusServerHandler* handler, const TBusServerSessionConfig& config, TBusMessageQueuePtr queue, const TVector<TBindResult>& bindTo) {
    return queue->CreateDestination(proto, handler, config, bindTo);
}
