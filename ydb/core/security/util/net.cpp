#include "net.h"

#include <library/cpp/ipv6_address/ipv6_address.h>

#include <util/generic/strbuf.h>
#include <util/string/cast.h>

namespace NKikimr::NSecurity {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf IPV4_PREFIX = "ipv4:";

bool IsGoodIPv4Part2(TStringBuf peername) {
    const auto colonPos = peername.find(':');
    if (colonPos == TStringBuf::npos ||
        colonPos == 0 || peername.length() <= colonPos + 1) {
        return false;
    }

    const TIpPort port = FromStringWithDefault<TIpPort>(peername.substr(colonPos + 1), 0);
    return (port != 0) && IsIPv4(peername.substr(0, colonPos));
}

bool IsGoodIPv4Part3(TStringBuf peername) {
    return peername.starts_with(IPV4_PREFIX) &&
           IsIPv4(peername.Skip(IPV4_PREFIX.length()));
}

bool IsGoodIPv4Part4(TStringBuf peername) {
    return peername.starts_with(IPV4_PREFIX) &&
           IsGoodIPv4Part2(peername.Skip(IPV4_PREFIX.size()));
}

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf IPV6_PREFIX = "ipv6:";

bool IsGoodIPv6Part2(TStringBuf peername) {
    if (peername.length() < 1 || peername[0] != '[') {
        return false;
    }

    const auto lastClosedBracketPos = peername.find_last_of(']');
    if (lastClosedBracketPos == TStringBuf::npos || lastClosedBracketPos < 2) {
        return false;
    }

    if (peername.length() <= lastClosedBracketPos + 2 || peername[lastClosedBracketPos + 1] != ':') {
        return false;
    }

    const TIpPort port = FromStringWithDefault<TIpPort>(peername.substr(lastClosedBracketPos + 2), 0);
    return (port != 0) && IsIPv6(peername.substr(1, lastClosedBracketPos - 1));
}

bool IsGoodIPv6Part3(TStringBuf peername) {
    return peername.starts_with(IPV6_PREFIX) &&
           IsIPv6(peername.Skip(IPV6_PREFIX.length()));
}

bool IsGoodIPv6Part4(TStringBuf peername) {
    return peername.starts_with(IPV6_PREFIX) &&
           IsGoodIPv6Part2(peername.Skip(IPV6_PREFIX.length()));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsIPv4(TStringBuf address) {
    bool isValid = false;
    const auto ip = TIpv6Address::FromString(address, isValid);
    return isValid && !ip.IsIpv6();
}

bool IsIPv6(TStringBuf address) {
    bool isValid = false;
    const auto ip = TIpv6Address::FromString(address, isValid);
    return isValid && ip.IsIpv6();
}

bool IsGoodPeernameFormat(TStringBuf peername) {
    return IsGoodIPv6Part4(peername) || IsGoodIPv4Part4(peername)
        || IsGoodIPv6Part3(peername) || IsGoodIPv4Part3(peername)
        || IsGoodIPv6Part2(peername) || IsGoodIPv4Part2(peername)
        || IsIPv6(peername) || IsIPv4(peername);
}

} // namespace NKikimr::NSecurity
