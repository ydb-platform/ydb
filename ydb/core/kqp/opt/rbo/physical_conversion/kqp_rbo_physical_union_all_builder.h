#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalUnionAllBuilder: public TPhysicalVariadicOpBuilder {
public:
    TPhysicalUnionAllBuilder(TOpUnionAll& unionAll, TExprContext& ctx, TPositionHandle pos, const TPhysicalNames& names)
        : TPhysicalVariadicOpBuilder(ctx, pos, names)
        , UnionAll(unionAll) {
    }

    TExprNode::TPtr BuildPhysicalOp(const TVector<TExprNode::TPtr>& inputs) override;

private:
    TExprNode::TPtr ProjectInput(TExprNode::TPtr input, ui32 childIndex) const;

    TOpUnionAll& UnionAll;
};
