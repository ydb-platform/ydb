#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalUnionAllBuilder: public TPhysicalBinaryOpBuilder {
public:
    TPhysicalUnionAllBuilder(TIntrusivePtr<TOpUnionAll> unionAll, TExprContext& ctx, TPositionHandle pos)
        : TPhysicalBinaryOpBuilder(ctx, pos)
        , UnionAll(unionAll) {
    }

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput) override;

private:
    TExprNode::TPtr ProjectInput(TExprNode::TPtr input, ui32 childIndex) const;

    TIntrusivePtr<TOpUnionAll> UnionAll;
};
