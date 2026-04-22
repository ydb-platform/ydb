#include "net.h"

#include <library/cpp/ipv6_address/ipv6_address.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/network/address.h>

#include <tuple>

namespace NKikimr::NSecurity {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf IPV6_PREFIX = "ipv6:";
constexpr TStringBuf IPV4_PREFIX = "ipv4:";

////////////////////////////////////////////////////////////////////////////////

TMaybe<TIpv6Address> GetAddress(TStringBuf address) {
    bool isValid = false;
    const auto ip = TIpv6Address::FromString(address, isValid);
    return isValid ? MakeMaybe(ip) : Nothing();
}

////////////////////////////////////////////////////////////////////////////////

TMaybe<THostAddressAndPort> TryParsePeername(TStringBuf peername) {
    const auto parsePeername = [](
        TStringBuf normalizedPeername,
        TMaybe<TIpv6Address::TIpType> target) -> TMaybe<THostAddressAndPort> {
        bool isOk = false;
        auto [addrPort, host, port] =
            ParseHostAndMayBePortFromString(normalizedPeername, 0, isOk);
        if (!isOk) {
            return Nothing();
        }

        if (target.Defined()) {
            return (addrPort.Ip.Type() == target.GetRef())
                ? MakeMaybe<THostAddressAndPort>(std::move(addrPort))
                : Nothing();
        } else {
            return (addrPort.Ip.Type() == TIpv6Address::TIpType::Ipv6
                    || addrPort.Ip.Type() == TIpv6Address::TIpType::Ipv4)
                ? MakeMaybe<THostAddressAndPort>(std::move(addrPort))
                : Nothing();
        }
    };

    // ipv6:<ipv6> / ipv6:[<ipv6>] / ipv6:[<ipv6>]:<port>
    if (peername.SkipPrefix(IPV6_PREFIX)) {
        return parsePeername(peername, TIpv6Address::TIpType::Ipv6);
    }

    // ipv4:<ipv4> / ipv4:<ipv4>:<port>
    if (peername.SkipPrefix(IPV4_PREFIX)) {
        return parsePeername(peername, TIpv6Address::TIpType::Ipv4);
    }

    // <ipv6> / [<ipv6>] / [<ipv6>]:<port> / <ipv4> / <ipv4>:<port>
    return parsePeername(peername, Nothing());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsIPv4(TStringBuf address) {
    return GetAddress(address)
        .Transform([](const TIpv6Address& ip) {
            return ip.Type() == TIpv6Address::TIpType::Ipv4;
        })
        .GetOrElse(false);
}

bool IsIPv6(TStringBuf address) {
    return GetAddress(address)
        .Transform([](const TIpv6Address& ip) {
            return ip.Type() == TIpv6Address::TIpType::Ipv6;
        })
        .GetOrElse(false);
}

////////////////////////////////////////////////////////////////////////////////

bool IsGoodPeernameFormat(TStringBuf peername) {
    return TryParsePeername(peername).Defined();
}

NAddr::IRemoteAddrPtr ParsePeername(TStringBuf peername) {
    return TryParsePeername(peername)
        .Transform([](const THostAddressAndPort& addrWithPort) {
            return THolder{ToIRemoteAddr(addrWithPort.Ip, addrWithPort.Port)};
        })
        .GetOrElse(nullptr);
}

} // namespace NKikimr::NSecurity
