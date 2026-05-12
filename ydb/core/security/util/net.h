#pragma once

#include <util/generic/fwd.h>
#include <util/network/address.h>

// TicketParser receives requests in which peername is a TString.
// Accordingly, you can write anything in TString, so validating the correctness
// of this string is critically important from a security point of view.
// We decided to implement validation and parsing functions in this folder for the following reasons:
//
// 1. We don't want to work with addresses like with an array of bytes.
//    This leads to mistakes, incorrect behavior, and is extremely inconvenient.
// 2. Validating addresses using standard tools is more expensive than writing
//    simple validation functions. Therefore, the implementation of a unified
//    validation mechanism is met with resistance from other teams.
// 3. Different sources provide different data formats (e.g. <ipv6> vs ipv6:<ipv6>).
//    Some sources provide invalid formats (for example, [<ipv6>]), which still
//    must be successfully parsed by us. Currently, there is no single
//    non-self-written function capable of parsing such a variety of peername.
//
// For these reasons, it was decided to fix the contract for the accepted peername
// in this file and use already proven utilities for parsing such diverse formats.
// Also, these functions now return an opaque type, which will store a valid address
// in machine format

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
