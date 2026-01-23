#pragma once
#include "kqp_rbo.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalMapBuilder {
public:
    TPhysicalMapBuilder(std::shared_ptr<TOpMap> map, TExprContext& ctx, TPositionHandle pos)
        : Map(map)
        , Ctx(ctx)
        , Pos(pos) {
    }

    TPhysicalMapBuilder() = delete;
    TPhysicalMapBuilder(const TPhysicalMapBuilder&) = delete;
    TPhysicalMapBuilder& operator=(const TPhysicalMapBuilder&) = delete;
    TPhysicalMapBuilder(const TPhysicalMapBuilder&&) = delete;
    TPhysicalMapBuilder& operator=(const TPhysicalMapBuilder&&) = delete;
    ~TPhysicalMapBuilder() = default;

    TExprNode::TPtr BuildPhysicalMap(TExprNode::TPtr input);

private:
    std::shared_ptr<TOpMap> Map;
    TExprContext& Ctx;
    TPositionHandle Pos;
};
