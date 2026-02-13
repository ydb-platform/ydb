#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalFilterBuilder: public TPhysicalUnaryOpBuilder {
public:
    TPhysicalFilterBuilder(std::shared_ptr<TOpFilter> filter, TExprContext& ctx, TPositionHandle pos)
        : TPhysicalUnaryOpBuilder(ctx, pos)
        , Filter(filter) {
    }

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr input) override;

private:
    std::shared_ptr<TOpFilter> Filter;
};
