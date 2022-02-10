#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NYq {
    TString HmacSha1(const TStringBuf data, const TStringBuf secret);
    TString HmacSha1Base64(const TStringBuf data, const TStringBuf secret);
}
