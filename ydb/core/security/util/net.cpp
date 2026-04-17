#include "net.h"

#include <library/cpp/ipv6_address/ipv6_address.h>

#include <util/generic/strbuf.h>

#include <regex>

namespace NKikimr::NSecurity {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf IPV4_PREFIX = "ipv4:";

bool IsGoodIPv4Part1(TStringBuf peername) {
    return IsIPv4(peername);
}

bool IsGoodIPv4Part2(TStringBuf peername) {
    static const std::regex pattern{R"((.+):(\d+))"};
    if (std::smatch match;
        std::regex_match(peername.begin(), peername.end(), match, pattern) &&
        match.ready() &&
        match.size() == 3) {
        const TString address = match.str(1);
        const TIpPort port = FromStringWithDefault<TIpPort>(match.str(2), 0);
        return (port != 0) && IsIPv4(address);
    }
    return false;
}

bool IsGoodIPv4Part3(TStringBuf peername) {
    return peername.starts_with(IPV4_PREFIX) &&
           IsGoodIPv4Part1(peername.Skip(IPV4_PREFIX.length()));
}

bool IsGoodIPv4Part4(TStringBuf peername) {
    return peername.starts_with(IPV4_PREFIX) &&
           IsGoodIPv4Part2(peername.Skip(IPV4_PREFIX.size()));
}

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf IPV6_PREFIX = "ipv6:";

bool IsGoodIPv6Part1(TStringBuf peername) {
    return IsIPv6(peername);
}

bool IsGoodIPv6Part2(TStringBuf peername) {
    static const std::regex pattern{R"(\[(.+)\]:(\d+))"};
    if (std::smatch match;
        std::regex_match(peername.begin(), peername.end(), match, pattern) &&
        match.ready() &&
        match.size() == 3) {
        const TString address = match.str(1);
        const TIpPort port = FromStringWithDefault<TIpPort>(match.str(2), 0);
        return (port != 0) && IsIPv6(address);
    }
    return false;
}

bool IsGoodIPv6Part3(TStringBuf peername) {
    return peername.starts_with(IPV6_PREFIX) &&
           IsGoodIPv6Part1(peername.Skip(IPV6_PREFIX.length()));
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
    return IsGoodIPv6Part4(peername) || IsGoodIPv4Part4(peername) ||
           IsGoodIPv6Part3(peername) || IsGoodIPv4Part3(peername) ||
           IsGoodIPv6Part2(peername) || IsGoodIPv4Part2(peername) ||
           IsGoodIPv6Part1(peername) || IsGoodIPv4Part1(peername);
}

} // namespace NKikimr::NSecurity