#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    TString FormatKeywords(const TVector<TString>& seq);

    bool IsPlain(TStringBuf content);

    bool IsQuoted(TStringBuf content);

    TString Quoted(TString content);

    TStringBuf Unquoted(TStringBuf content);

    bool IsBinding(TStringBuf content);

    TStringBuf Unbinded(TStringBuf content);

} // namespace NSQLComplete
