#pragma once

#include <util/generic/string.h>

namespace NSQLPureAST {

bool IsQuoted(TStringBuf content);

TString Quoted(TString content);

TStringBuf Unquoted(TStringBuf content);

} // namespace NSQLPureAST
