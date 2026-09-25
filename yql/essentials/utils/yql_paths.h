#pragma once

#include <util/generic/strbuf.h>

#include <functional>

namespace NYql {

// Applies normalizePath once to the raw path, before adding the prefix.
TString BuildTablePath(TStringBuf prefixPath, TStringBuf path,
                       const std::function<TString(TStringBuf)>& normalizePath = {});

} // namespace NYql
