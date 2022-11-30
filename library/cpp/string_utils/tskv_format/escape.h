#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NTskvFormat {
    TString& Escape(const TStringBuf& src, TString& dst);
    TString& Unescape(const TStringBuf& src, TString& dst);

}
