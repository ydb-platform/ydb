#pragma once

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimr::NHttp {

// First occurrence of each RFC 7239 parameter, left to right (Section 4).
// Port is taken from host= (Host ABNF: uri-host [ ":" port ]); there is no port= parameter.
struct TRfc7239Forwarded {
    TString Host;
    TMaybe<ui16> Port;
    TString Proto;
    TString For;
    TString By;
};

TRfc7239Forwarded ParseRfc7239Forwarded(TStringBuf header);

} // namespace NKikimr::NHttp
