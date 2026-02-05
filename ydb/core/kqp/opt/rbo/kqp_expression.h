#pragma once

#include "kqp_info_unit.h"
#include "kqp_rbo_context.h"
#include "kqp_plan_props.h"
#include <ydb/core/kqp/common/kqp_yql.h>


namespace NKikimr {
namespace NKqp {

using namespace NYql;

class TExpression {
  public:

    TExpression(TExprNode::TPtr node, const TExprContext* ctx, const TPlanProps* props = nullptr) :
      Node(node),
      Ctx(ctx),
      PlanProps(props) {}

    TExpression() = default;
    ~TExpression() = default;

    TVector<TExpression> SplitConjunct() const;

    bool IsColumnAccess() const;
    bool IsSingleCallable(THashSet<TString> allowedCallables) const;
    bool IsCast() const;
    bool IsConstantExpr() const;
    bool MaybeJoinCondition(bool includeExpression = false, bool equiJoinOnly = true) const;

    TExprNode::TPtr GetLambda() const;
    TExprNode::TPtr GetExpressionBody() const;

    TVector<TInfoUnit> GetInputIUs(bool includeSubplanVars = false, bool includeCorrelatedDeps = false) const;

    TExpression ApplyRenames(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap);
    TExpression ApplyReplaceMap(TNodeOnNodeOwnedMap map, TRBOContext& ctx);
    TExpression PruneCast();

    TString ToString() const;

    TExprNode::TPtr Node;
    const TExprContext* Ctx;
    const TPlanProps* PlanProps;
};

class TJoinCondition {
  public:
    TJoinCondition(TExpression expr);
    TInfoUnit GetLeftIU();
    TInfoUnit GetRightIU();
    void ExtractExpressions(TNodeOnNodeOwnedMap& map);

    TExpression Expr;
    TVector<TInfoUnit> LeftIUs;
    TVector<TInfoUnit> RightIUs;

    bool PairCondition = true;
    bool IncludesExpressions = false;
    bool EquiJoin = true;
};

TExpression MakeColumnAccess(TInfoUnit column, const TExprContext* ctx, const TPlanProps* props = nullptr);
TExpression MakeConstant(TString type, TString value, const TExprContext* ctx);
TExpression MakeNothing(TPositionHandle pos, const TTypeAnnotationNode* type, const TExprContext* ctx);
TExpression MakeConjunct(TVector<TExpression> vec, bool pgSyntax = false);
TExpression MakeBinaryPredicate(TString callable, TExpression left, TExpression right);

void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit> &IUs);
}
}