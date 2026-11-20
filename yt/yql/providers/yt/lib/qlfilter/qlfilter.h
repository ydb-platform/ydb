#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <util/system/types.h>

namespace NYql {

TExprNode::TPtr ExtractQLFilterPredicate(
    const TExprNode::TPtr& predicate,
    const TExprNode::TPtr& rowArg,
    const TExprNode::TPtr& newRowArg,
    TPositionHandle pos,
    TExprContext& ctx);

TMaybe<TString> ConvertFloatingValueForQL(TStringBuf value, bool isFloat);
TMaybe<TString> GenerateInputQuery(const TExprNode::TPtr& qlFilterNode, ui32 depthLimit);

} // namespace NYql
