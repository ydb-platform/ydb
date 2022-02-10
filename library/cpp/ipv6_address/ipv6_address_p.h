#pragma once

#include "ipv6_address.h"

inline TIpv6Address::TIpType FigureOutType(TStringBuf srcStr) noexcept {
    return srcStr.Contains(':') ? TIpv6Address::Ipv6 : TIpv6Address::Ipv4;
}
