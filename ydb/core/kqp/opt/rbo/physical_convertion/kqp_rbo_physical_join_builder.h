#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalJoinBuilder: public TPhysicalBinaryOpBuilder {
public:
    TPhysicalJoinBuilder(TIntrusivePtr<TOpJoin> join, TExprContext& ctx, TPositionHandle pos)
        : TPhysicalBinaryOpBuilder(ctx, pos)
        , Join(join) {
    }

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput) override;

private:
    TExprNode::TPtr BuildGraceJoinCore(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput);
    TExprNode::TPtr BuildCrossJoin(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput);
    TString GetValidJoinKind(const TString& joinKind);

    TIntrusivePtr<TOpJoin> Join;
};
