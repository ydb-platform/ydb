#pragma once

#include <util/generic/fwd.h>

namespace NKikimr::NSecurity {

////////////////////////////////////////////////////////////////////////////////

bool IsIPv4(TStringBuf address);
bool IsIPv6(TStringBuf address);

// Supported peername formats:
//   IPv4 part:
//     1. <ipv4>
//     2. <ipv4>:<port>
//     3. ipv4:<ipv4>
//     4. ipv4:<ipv4>:<port>
//   IPv6 part:
//     1. <ipv6>
//     2. [<ipv6>]:<port>
//     3. ipv6:<ipv6>
//     4. ipv6:[<ipv6>]:<port>
bool IsGoodPeernameFormat(TStringBuf peername);

} // namespace NKikimr::NSecurity
