#pragma once

#include <util/generic/strbuf.h>

namespace NTld {
    const char* const* GetTlds();

    // Note that FindTld() returns empty string when @host is single domain label (without '.').
    // If you need whole @host for such case, you can use GetZone() from library/cpp/string_utils/url/url.h
    inline TStringBuf FindTld(const TStringBuf& host) {
        size_t p = host.rfind('.');
        return p != TStringBuf::npos ? host.SubStr(p + 1) : TStringBuf();
    }

    bool IsTld(const TStringBuf& tld);

    inline bool InTld(const TStringBuf& host) {
        return IsTld(FindTld(host));
    }

    // check if @s belongs to a "good" subset of reliable TLDs, defined in tld.cpp
    bool IsVeryGoodTld(const TStringBuf& tld);

    inline bool InVeryGoodTld(const TStringBuf& host) {
        return IsVeryGoodTld(FindTld(host));
    }

}
