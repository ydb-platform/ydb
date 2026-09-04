#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalTableEffectBuilder: public TPhysicalUnaryOpBuilder {
public:
    TPhysicalTableEffectBuilder(TIntrusivePtr<TOpTableEffect> tableEffect, TExprContext& ctx, TPositionHandle pos)
        : TPhysicalUnaryOpBuilder(ctx, pos), TableEffect(tableEffect) {}

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr input) override;

private:
    TIntrusivePtr<TOpTableEffect> TableEffect;
};
