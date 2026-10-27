#pragma once

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NLogParsing {

struct TTskvParseResult {
    bool Successed = false;
    THashMap<TString, TString> Fields;
    TString Error;
};

//! Parse TSKV into string→string map (API A). Values kept as-is; last key wins.
TTskvParseResult ParseTskv(TStringBuf raw);

} // namespace NLogParsing
