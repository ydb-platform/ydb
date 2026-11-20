#pragma once

#include "../eval/kqp_eval.h"

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <optional>

namespace NKikimr::NKqp {

// Share one context across a plan's operators, result sets and output views.
class TExplainContext {
public:
    TExplainContext();
    ~TExplainContext();
    NEval::TSession& Evaluation() noexcept;
    TString DescribeOpaqueExpression(const NYql::TExprNode::TPtr& node);

    TString UnknownExpressionName(const NYql::TExprNode::TPtr& node);
    TString DescribeUnknownExpression(const NYql::TExprNode::TPtr& node);

private:
    struct TUnknownExpression {
        TString Name;
        std::optional<TString> Description;
    };

    // Retain nodes so their addresses cannot be reused during this render.
    THashMap<NYql::TExprNode::TPtr, TUnknownExpression> UnknownExpressions_;
    NEval::TSession Evaluation_;
    size_t RemainingNodes_ = 64 * 1024;
};

} // namespace NKikimr::NKqp
