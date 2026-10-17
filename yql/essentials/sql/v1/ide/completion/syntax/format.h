#pragma once

#include <yql/essentials/sql/v1/ide/core/format.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

using NSQLPureAST::IsQuoted;
using NSQLPureAST::Quoted;
using NSQLPureAST::Unquoted;

TString FormatKeywords(const TVector<TString>& seq);

bool IsPlain(TStringBuf content);

bool IsBinding(TStringBuf content);

TStringBuf Unbinded(TStringBuf content);

} // namespace NSQLComplete
