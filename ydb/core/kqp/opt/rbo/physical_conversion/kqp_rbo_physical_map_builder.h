#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalMapBuilder: public TPhysicalUnaryOpBuilder {
public:
    TPhysicalMapBuilder(TIntrusivePtr<TOpMap> map, TExprContext& ctx, TPositionHandle pos, const TInfoUnitSet* liveOut = nullptr)
        : TPhysicalUnaryOpBuilder(ctx, pos)
        , Map(map)
        , LiveOut(liveOut) {
    }

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr input) override;

private:
    TVector<TInfoUnit> GetPhysicalOutputIUs() const;

    TIntrusivePtr<TOpMap> Map;
    const TInfoUnitSet* LiveOut = nullptr;
};
