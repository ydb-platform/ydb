#include "net.h"

#include <library/cpp/ipv6_address/ipv6_address.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/network/address.h>
#include <util/network/ip.h>
#include <util/string/cast.h>

namespace NKikimr::NSecurity {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf IPV6_PREFIX = "ipv6:";
constexpr TStringBuf IPV4_PREFIX = "ipv4:";

struct TIpWithPort {
    TIpv6Address Address;
    TIpPort Port;

    TIpWithPort(TIpv6Address address, TIpPort port)
        : Address(std::move(address))
        , Port(port)
    {}
};

////////////////////////////////////////////////////////////////////////////////

TMaybe<TIpv6Address> GetAddress(TStringBuf address) {
    bool isValid = false;
    const auto ip = TIpv6Address::FromString(address, isValid);
    return isValid ? MakeMaybe(ip) : Nothing();
}

NAddr::IRemoteAddrPtr ParseAddress(const TIpWithPort& addr) {
    struct sockaddr_in sa4;
    struct sockaddr_in6 sa6;
    const sockaddr* sa;
    socklen_t len;
    addr.Address.ToSockaddrAndSocklen(sa4, sa6, sa, len, addr.Port);

    if (addr.Address.IsIpv6()) {
        return MakeHolder<NAddr::TIPv6Addr>(sa6);
    } else {
        return MakeHolder<NAddr::TIPv4Addr>(TIpAddress(sa4));
    }
}

////////////////////////////////////////////////////////////////////////////////

TMaybe<TIpWithPort> ParseIPv4(TStringBuf peername) {
    if (peername.empty()) {
        return Nothing();
    }

    const auto colonPos = peername.find(':');
    if (colonPos == TStringBuf::npos) {
        return GetAddress(peername)
            .AndThen([](TIpv6Address&& ip) {
                return ip.IsIpv6() ? Nothing() : MakeMaybe<TIpWithPort>(std::move(ip), 0);
            });
    }

    if (colonPos == 0 || peername.length() <= colonPos + 1) {
        return Nothing();
    }

    const TIpPort port = FromStringWithDefault<TIpPort>(peername.substr(colonPos + 1), 0);
    if (port == 0) {
        return Nothing();
    }

    return GetAddress(peername.substr(0, colonPos))
        .AndThen([&](TIpv6Address&& ip) {
            return ip.IsIpv6() ? Nothing() : MakeMaybe<TIpWithPort>(std::move(ip), port);
        });
}

TMaybe<TIpWithPort> ParseIPv6(TStringBuf peername) {
    if (peername.empty()) {
        return Nothing();
    }

    if (peername[0] != '[') {
        return GetAddress(peername)
            .AndThen([](TIpv6Address&& ip) {
                return ip.IsIpv6() ? MakeMaybe<TIpWithPort>(std::move(ip), 0) : Nothing();
            });
    }

    const auto lastClosedBracketPos = peername.find_last_of(']');
    if (lastClosedBracketPos == TStringBuf::npos || lastClosedBracketPos < 2) {
        return Nothing();
    }

    if (peername.length() <= lastClosedBracketPos + 2 || peername[lastClosedBracketPos + 1] != ':') {
        return Nothing();
    }

    const TIpPort port = FromStringWithDefault<TIpPort>(peername.substr(lastClosedBracketPos + 2), 0);
    if (port == 0) {
        return Nothing();
    }

    return GetAddress(peername.substr(1, lastClosedBracketPos - 1))
        .AndThen([&](TIpv6Address&& ip) {
            return ip.IsIpv6() ? MakeMaybe<TIpWithPort>(std::move(ip), port) : Nothing();
        });
}

////////////////////////////////////////////////////////////////////////////////

TMaybe<TIpWithPort> TryParsePeername(TStringBuf peername) {
    // ipv6:<ipv6> / ipv6:[<ipv6>]:<port>
    if (peername.starts_with(IPV6_PREFIX)) {
        return ParseIPv6(peername.Skip(IPV6_PREFIX.length()));
    }

    // ipv4:<ipv4> / ipv4:<ipv4>:<port>
    if (peername.starts_with(IPV4_PREFIX)) {
        return ParseIPv4(peername.Skip(IPV4_PREFIX.length()));
    }

    // <ipv6> / [<ipv6>]:<port> / <ipv4> / <ipv4>:<port>
    return ParseIPv6(peername).Or([&]() { return ParseIPv4(peername); });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsIPv4(TStringBuf address) {
    return GetAddress(address)
        .Transform([](const TIpv6Address& ip) { return !ip.IsIpv6(); })
        .GetOrElse(false);
}

////////////////////////////////////////////////////////////////////////////////

bool IsIPv6(TStringBuf address) {
    return GetAddress(address)
        .Transform([](const TIpv6Address& ip) { return ip.IsIpv6(); })
        .GetOrElse(false);
}

////////////////////////////////////////////////////////////////////////////////

bool IsGoodPeernameFormat(TStringBuf peername) {
    return TryParsePeername(peername).Defined();
}

NAddr::IRemoteAddrPtr ParsePeername(TStringBuf peername) {
    return TryParsePeername(peername)
        .Transform([](const TIpWithPort& addr) { return ParseAddress(addr); })
        .GetOrElse(nullptr);
}

} // namespace NKikimr::NSecurity
