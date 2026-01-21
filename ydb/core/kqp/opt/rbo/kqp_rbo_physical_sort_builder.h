#pragma once
#include "kqp_rbo.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalSortBuilder {
public:
    TPhysicalSortBuilder(std::shared_ptr<TOpSort> sort, TExprContext& ctx, TPositionHandle pos)
        : Sort(sort)
        , Ctx(ctx)
        , Pos(pos) {
    }

    TPhysicalSortBuilder() = delete;
    TPhysicalSortBuilder(const TPhysicalSortBuilder&) = delete;
    TPhysicalSortBuilder& operator=(const TPhysicalSortBuilder&) = delete;
    TPhysicalSortBuilder(const TPhysicalSortBuilder&&) = delete;
    TPhysicalSortBuilder& operator=(const TPhysicalSortBuilder&&) = delete;
    ~TPhysicalSortBuilder() = default;

    TExprNode::TPtr BuildPhysicalSort(TExprNode::TPtr input);

private:
    TVector<TExprNode::TPtr> BuildSortKeysForWideSort(const TVector<TInfoUnit>& inputs, const TVector<TSortElement>& sortElements);
    TExprNode::TPtr BuildSort(TExprNode::TPtr input, TOrderEnforcer& enforcer);
    std::pair<TExprNode::TPtr, TVector<TExprNode::TPtr>> BuildSortKeySelector(const TVector<TSortElement>& sortElements);

    std::shared_ptr<TOpSort> Sort;
    TExprContext& Ctx;
    TPositionHandle Pos;
};
