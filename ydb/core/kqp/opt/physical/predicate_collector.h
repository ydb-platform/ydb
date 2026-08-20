#pragma once
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;

struct TOLAPPredicateNode {
    TExprNode::TPtr ExprNode;
    std::vector<TOLAPPredicateNode> Children;
    bool CanBePushed = false;
    bool CanBePushedApply = false;

    bool IsValid() const {
        return ExprNode && std::all_of(Children.cbegin(), Children.cend(), std::bind(&TOLAPPredicateNode::IsValid, std::placeholders::_1));
    }
};

struct TPushdownOptions {
    TPushdownOptions(bool allowOlapApply, bool pushdownSubstring, bool stripAliasPrefixFromColName = false,
                     bool pushdownRegexp = false, bool fastAsciiIgnoreCaseContains = false)
        : AllowOlapApply(allowOlapApply)
        , PushdownSubstring(pushdownSubstring)
        , StripAliasPrefixFromColName(stripAliasPrefixFromColName)
        , PushdownRegexp(pushdownRegexp)
        , FastAsciiIgnoreCaseContains(fastAsciiIgnoreCaseContains) {
    }

    TPushdownOptions WithAllowOlapApply(bool allow) const {
        TPushdownOptions copy = *this;
        copy.AllowOlapApply = allow;
        return copy;
    }

    bool AllowOlapApply{false};
    bool PushdownSubstring{false};
    bool StripAliasPrefixFromColName{false};
    bool PushdownRegexp{false};
    bool FastAsciiIgnoreCaseContains{false};
};

extern THashMap<TString, TString> IgnoreCaseSubstringMatchFunctions;

void CollectPredicates(const NNodes::TExprBase& predicate, TOLAPPredicateNode& predicateTree, const TExprNode* lambdaArg, const TTypeAnnotationNode* inputType,
                       const TPushdownOptions& options);

} // namespace NKikimr::NKqp::NOpt
