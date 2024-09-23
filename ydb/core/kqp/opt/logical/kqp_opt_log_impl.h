#pragma once

#include <ydb/core/kqp/opt/kqp_opt_impl.h>

namespace NKikimr::NKqp::NOpt {

struct TKqpMatchReadResult {
    NYql::NNodes::TExprBase Read;
    NYql::NNodes::TMaybeNode<NYql::NNodes::TCoExtractMembers> ExtractMembers;
    NYql::NNodes::TMaybeNode<NYql::NNodes::TCoFilterNullMembers> FilterNullMembers;
    NYql::NNodes::TMaybeNode<NYql::NNodes::TCoSkipNullMembers> SkipNullMembers;
    NYql::NNodes::TMaybeNode<NYql::NNodes::TCoFlatMap> FlatMap;

    NYql::NNodes::TExprBase BuildProcessNodes(NYql::NNodes::TExprBase input, NYql::TExprContext& ctx) const;
};

TMaybe<TKqpMatchReadResult> MatchRead(NYql::NNodes::TExprBase node,
    std::function<bool(NYql::NNodes::TExprBase)> matchFunc);

template<typename TRead>
TMaybe<TKqpMatchReadResult> MatchRead(NYql::NNodes::TExprBase node) {
    return MatchRead(node, [] (NYql::NNodes::TExprBase node) { return node.Maybe<TRead>().IsValid(); });
}

NYql::NNodes::TMaybeNode<NYql::NNodes::TKqlKeyInc> GetRightTableKeyPrefix(const NYql::NNodes::TKqlKeyRange& range);

NYql::NNodes::TCoLambda MakeFilterForRange(NYql::NNodes::TKqlKeyRange range, NYql::TExprContext& ctx, NYql::TPositionHandle pos, TVector<TString> keyColumns);

bool ExtractUsedFields(const NYql::TExprNode::TPtr& start, const NYql::TExprNode& arg, TSet<TString>& usedFields, const NYql::TParentsMap& parentsMap, bool allowDependsOn);

struct TPrefixLookup {
    NYql::NNodes::TCoAtomList LookupColumns;
    NYql::NNodes::TCoAtomList ResultColumns;

    NYql::NNodes::TMaybeNode<NYql::NNodes::TCoLambda> Filter;
    TMaybe<TSet<TString>> FilterUsedColumnsHint;

    size_t PrefixSize;
    NYql::NNodes::TExprBase PrefixExpr;

    TString LookupTableName;

    NYql::NNodes::TKqpTable MainTable;
    TString IndexName;
};

// Try to rewrite arbitrary table read to (ExtractMembers (Filter (Lookup LookupColumns) ResultColumns)
TMaybe<TPrefixLookup> RewriteReadToPrefixLookup(NYql::NNodes::TExprBase read, NYql::TExprContext& ctx, const TKqpOptimizeContext& kqpCtx, TMaybe<size_t> maxKeys);

} // NKikimr::NKqp::NOpt



