#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NFq {
TString HmacSha1(const TStringBuf data, const TStringBuf secret);
TString HmacSha1Base64(const TStringBuf data, const TStringBuf secret);
}   // namespace NFq
