#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalSortBuilder: public TPhysicalWideUnaryOpBuilder {
public:
    TPhysicalSortBuilder(TOpSort& sort, TExprContext& ctx, TPositionHandle pos, const TPhysicalNames& names, const NYql::TTypeAnnotationContext& types)
        : TPhysicalWideUnaryOpBuilder(ctx, pos, names)
        , Sort(sort)
        , Types(types) {
    }

    NPhysicalConvertionUtils::TStageBody BuildPhysicalOp(const NPhysicalConvertionUtils::TStageBody& input) override;

private:
    TExprNode::TPtr BuildSort(TExprNode::TPtr input, TOrderEnforcer& enforcer);
    std::pair<TExprNode::TPtr, TVector<TExprNode::TPtr>> BuildSortKeySelector(const TSortIUs& sortElements);
    TVector<TExprNode::TPtr> BuildSortKeysForWideSort(const TVector<TInfoUnitId>& inputs, const TSortIUs& sortElements);
    TExprNode::TPtr BuildNarrowTopSort(TExprNode::TPtr wideInput, const TVector<TInfoUnitId>& inputs, const TSortIUs& sortElements, TExprNode::TPtr count);

    TOpSort& Sort;
    const NYql::TTypeAnnotationContext& Types;
};
