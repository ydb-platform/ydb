#pragma once
#include "kqp_rbo.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalFilterBuilder {
public:
    TPhysicalFilterBuilder(std::shared_ptr<TOpFilter> filter, TExprContext& ctx, TPositionHandle pos)
        : Filter(filter)
        , Ctx(ctx)
        , Pos(pos) {
    }

    TPhysicalFilterBuilder() = delete;
    TPhysicalFilterBuilder(const TPhysicalFilterBuilder&) = delete;
    TPhysicalFilterBuilder& operator=(const TPhysicalFilterBuilder&) = delete;
    TPhysicalFilterBuilder(const TPhysicalFilterBuilder&&) = delete;
    TPhysicalFilterBuilder& operator=(const TPhysicalFilterBuilder&&) = delete;
    ~TPhysicalFilterBuilder() = default;

    TExprNode::TPtr BuildPhysicalFilter(TExprNode::TPtr input);

private:
    std::shared_ptr<TOpFilter> Filter;
    TExprContext& Ctx;
    TPositionHandle Pos;
};
