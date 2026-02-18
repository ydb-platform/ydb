#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalSortBuilder: public TPhysicalUnaryOpBuilder {
public:
    TPhysicalSortBuilder(TIntrusivePtr<TOpSort> sort, TExprContext& ctx, TPositionHandle pos)
        : TPhysicalUnaryOpBuilder(ctx, pos)
        , Sort(sort) {
    }

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr input) override;

private:
    TVector<TExprNode::TPtr> BuildSortKeysForWideSort(const TVector<TInfoUnit>& inputs, const TVector<TSortElement>& sortElements);
    TExprNode::TPtr BuildSort(TExprNode::TPtr input, TOrderEnforcer& enforcer);
    std::pair<TExprNode::TPtr, TVector<TExprNode::TPtr>> BuildSortKeySelector(const TVector<TSortElement>& sortElements);

    TIntrusivePtr<TOpSort> Sort;
};
