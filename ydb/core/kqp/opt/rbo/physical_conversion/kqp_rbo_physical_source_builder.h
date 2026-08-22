#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalSourceBuilder: public TPhysicalNullaryOpBuilder {
public:
    TPhysicalSourceBuilder(TIntrusivePtr<TOpRead> read, TExprContext& ctx, TPositionHandle pos)
        : TPhysicalNullaryOpBuilder(ctx, pos), Read(read) {}

    TExprNode::TPtr BuildPhysicalOp() override;

private:
    TIntrusivePtr<TOpRead> Read;
};
