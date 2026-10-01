#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalTableEffectBuilder: public TPhysicalUnaryOpBuilder {
public:
    TPhysicalTableEffectBuilder(TOpTableEffect& tableEffect, TExprContext& ctx, TPositionHandle pos, const TPhysicalNames& names)
        : TPhysicalUnaryOpBuilder(ctx, pos, names), TableEffect(tableEffect) {}

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr input) override;

private:
    TOpTableEffect& TableEffect;
};
