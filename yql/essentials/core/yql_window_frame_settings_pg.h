#pragma once

#include <yql/essentials/core/yql_expr_type_annotation_pg.h>
#include <yql/essentials/core/yql_node_transform.h>
#include <yql/essentials/parser/pg_wrapper/interface/compare.h>
#include <yql/essentials/parser/pg_wrapper/interface/sign.h>

#include <compare>
#include <expected>

namespace NYql {

struct TInRangeCasts {
    TMaybe<TNodeTransform> ColumnCast;
    TMaybe<TNodeTransform> OffsetCast;
    ui32 ProcId;
};

std::expected<TInRangeCasts, TString> LookupInRangeCasts(
    const TTypeAnnotationNode* columnType,
    const TTypeAnnotationNode* offsetType,
    TPositionHandle pos,
    TExprContext& ctx);

std::expected<std::strong_ordering, TString> PgSign(const TExprNode::TPtr& value);

std::expected<bool, TString> PgCompareWithCasts(
    const TExprNode::TPtr& lhs,
    const TExprNode::TPtr& rhs,
    NKikimr::NMiniKQL::EPgCompareType cmpType);

} // namespace NYql
