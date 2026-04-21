#pragma once

#include <util/generic/fwd.h>
#include <util/network/address.h>

namespace NKikimr::NSecurity {

////////////////////////////////////////////////////////////////////////////////

bool IsIPv4(TStringBuf address);
bool IsIPv6(TStringBuf address);

// Supported peername formats:
//   <port> is a decimal port number in the inclusive range [0, 65535].
//   Port 0 is intentionally allowed and represents an unspecified/ephemeral
//   port in the parsed peername.
//
//   IPv4 part:
//     1. <ipv4>
//     2. <ipv4>:<port>
//     3. ipv4:<ipv4>
//     4. ipv4:<ipv4>:<port>
//   IPv6 part:
//     1. <ipv6>
//     2. [<ipv6>]
//     3. [<ipv6>]:<port>
//     4. ipv6:<ipv6>
//     5. ipv6:[<ipv6>]
//     6. ipv6:[<ipv6>]:<port>
bool IsGoodPeernameFormat(TStringBuf peername);
NAddr::IRemoteAddrPtr ParsePeername(TStringBuf peername);

} // namespace NKikimr::NSecurity
