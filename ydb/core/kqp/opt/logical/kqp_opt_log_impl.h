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

} // NKikimr::NKqp::NOpt



