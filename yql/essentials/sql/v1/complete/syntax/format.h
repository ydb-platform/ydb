#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    TString FormatKeywords(const TVector<TString>& seq);
    TString Quoted(TString content);
    TString Unquoted(TString content);

} // namespace NSQLComplete
