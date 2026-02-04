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

    TVector<TExpression> SplitConjunct();

    bool IsRename();
    bool IsSingleCallable(THashSet<TString> allowedCallables);
    bool IsCast();
    bool IsConstantExpr();
    bool MaybeJoinConidition();

    std::pair<TInfoUnit, TInfoUnit> GetRename();
    TExprNode::TPtr GetLambda();
    TExprNode::TPtr GetExpressionBody();

    TVector<TInfoUnit> GetInputIUs(bool includeSubplanVars = false, bool includeCorrelatedDeps = false);

    Expression ApplyRenames(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap);
    Expression ApplyReplaceMap(TNodeOnNodeOwnedMap map, TRBOContext& ctx);
    TExpression PruneCast();

    TExprNode::TPtr Node;
    const TExprContext* Ctx;
    const TPlanProps* PlanProps;
};

TExpression MakeRename(TInfoUnit from, TInfoUnit to, const TExprContext* ctx, const TPlanProps* props = nullptr);
TExpression MakeConstant(TString type, TString value, const TExprContext* ctx);
TExpression MakeConjunct(TVector<TExpression> vec);

}
}