#pragma once
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
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

    TPushdownOptions WithExternalArgs(const TNodeOnNodeOwnedMap* externalArgs) const {
        TPushdownOptions copy = *this;
        copy.ExternalArgs = externalArgs;
        return copy;
    }

    // Returns OLAP expression which stands for the given free lambda argument or nullptr if it is not an external argument.
    TExprNode::TPtr FindExternalArg(const TExprNode& node) const {
        if (!ExternalArgs || !node.IsArgument()) {
            return nullptr;
        }
        const auto it = ExternalArgs->find(&node);
        return it == ExternalArgs->end() ? nullptr : it->second;
    }

    bool IsExternalArg(const TExprNode& node) const {
        return FindExternalArg(node) != nullptr;
    }

    bool AllowOlapApply{false};
    bool PushdownSubstring{false};
    bool StripAliasPrefixFromColName{false};
    bool PushdownRegexp{false};
    bool FastAsciiIgnoreCaseContains{false};
    const TNodeOnNodeOwnedMap* ExternalArgs{nullptr};
};

extern THashMap<TString, TString> IgnoreCaseSubstringMatchFunctions;

// Whether `JsonValue` can be computed by the column shard as `KqpOlapJsonValue` with exactly the same semantics:
// JSON_VALUE over a column with a constant path, without RETURNING / PASSING and with default `NULL ON EMPTY` / `NULL ON ERROR`.
bool CanBePushedAsOlapJsonValue(const NNodes::TCoJsonValue& jsonValue);

void CollectPredicates(const NNodes::TExprBase& predicate, TOLAPPredicateNode& predicateTree, const TExprNode* lambdaArg, const TTypeAnnotationNode* inputType,
                       const TPushdownOptions& options);

} // namespace NKikimr::NKqp::NOpt
