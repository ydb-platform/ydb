#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <cstddef>

namespace NKikimr::NKqp::NOpt {

TString GetOlapColumnName(TStringBuf columnName, bool stripAliasPrefix);
TVector<TString> BuildReadRangeDescriptions(const NYql::TExprNode::TPtr& ranges, const TVector<TString>& keyColumns, size_t usedPrefixLen);

} // namespace NKikimr::NKqp::NOpt
