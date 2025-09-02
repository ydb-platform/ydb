#pragma once

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/ast/yql_expr.h>

#include <util/datetime/base.h>

namespace NYql::NHopping {

struct THoppingTraits {
    TString Column;
    NYql::NNodes::TCoHoppingTraits Traits;
    ui64 Hop;
    ui64 Interval;
    ui64 Delay;
};

struct TKeysDescription {
    TVector<TString> PickleKeys;
    TVector<TString> MemberKeys;
    TVector<TString> FakeKeys;

    TKeysDescription(const TStructExprType& rowType, const NYql::NNodes::TCoAtomList& keys, const TString& hoppingColumn);

    TExprNode::TPtr BuildPickleLambda(TExprContext& ctx, TPositionHandle pos) const;

    TExprNode::TPtr BuildUnpickleLambda(TExprContext& ctx, TPositionHandle pos, const TStructExprType& rowType);

    TVector<NYql::NNodes::TCoAtom> GetKeysList(TExprContext& ctx, TPositionHandle pos) const;

    TVector<TString> GetActualGroupKeys() const;

    bool NeedPickle() const;

    TExprNode::TPtr GetKeySelector(TExprContext& ctx, TPositionHandle pos, const TStructExprType* rowType);
};

TString BuildColumnName(const NYql::NNodes::TExprBase& column);

bool IsLegacyHopping(const TExprNode::TPtr& hoppingSetting);

void EnsureNotDistinct(const NYql::NNodes::TCoAggregate& aggregate);

TMaybe<THoppingTraits> ExtractHopTraits(const NYql::NNodes::TCoAggregate& aggregate, TExprContext& ctx, bool analyticsMode);

TExprNode::TPtr BuildTimeExtractor(const NYql::NNodes::TCoHoppingTraits& hoppingTraits, TExprContext& ctx);

TExprNode::TPtr BuildInitHopLambda(const NYql::NNodes::TCoAggregate& aggregate, TExprContext& ctx);

TExprNode::TPtr BuildUpdateHopLambda(const NYql::NNodes::TCoAggregate& aggregate, TExprContext& ctx);

TExprNode::TPtr BuildMergeHopLambda(const NYql::NNodes::TCoAggregate& aggregate, TExprContext& ctx);

TExprNode::TPtr BuildFinishHopLambda(
    const NYql::NNodes::TCoAggregate& aggregate,
    const TVector<TString>& actualGroupKeys,
    const TString& hoppingColumn,
    TExprContext& ctx);

TExprNode::TPtr BuildSaveHopLambda(const NYql::NNodes::TCoAggregate& aggregate, TExprContext& ctx);

TExprNode::TPtr BuildLoadHopLambda(const NYql::NNodes::TCoAggregate& aggregate, TExprContext& ctx);

} // namespace NYql::NHopping
