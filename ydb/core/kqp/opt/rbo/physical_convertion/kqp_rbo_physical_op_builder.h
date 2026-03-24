#pragma once
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalOpBuilder {
public:
    TPhysicalOpBuilder(TExprContext& ctx, TPositionHandle pos)
        : Ctx(ctx)
        , Pos(pos) {
    }

    TPhysicalOpBuilder() = delete;
    TPhysicalOpBuilder(const TPhysicalOpBuilder&) = delete;
    TPhysicalOpBuilder& operator=(const TPhysicalOpBuilder&) = delete;
    TPhysicalOpBuilder(const TPhysicalOpBuilder&&) = delete;
    TPhysicalOpBuilder& operator=(const TPhysicalOpBuilder&&) = delete;
    virtual ~TPhysicalOpBuilder() = default;

protected:
    TExprContext& Ctx;
    TPositionHandle Pos;
};

class TPhysicalNullaryOpBuilder: public TPhysicalOpBuilder {
public:
    TPhysicalNullaryOpBuilder(TExprContext& ctx, TPositionHandle pos)
        : TPhysicalOpBuilder(ctx, pos) {
    }

    virtual TExprNode::TPtr BuildPhysicalOp() = 0;
};

class TPhysicalUnaryOpBuilder: public TPhysicalOpBuilder {
public:
    TPhysicalUnaryOpBuilder(TExprContext& ctx, TPositionHandle pos)
        : TPhysicalOpBuilder(ctx, pos) {
    }

    virtual TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr input) = 0;
};

class TPhysicalBinaryOpBuilder: public TPhysicalOpBuilder {
public:
    TPhysicalBinaryOpBuilder(TExprContext& ctx, TPositionHandle pos)
        : TPhysicalOpBuilder(ctx, pos) {
    }

    virtual TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput) = 0;
};

template <typename TPhysicalBuilder, typename TOperator, typename... Args>
TExprNode::TPtr Build(TIntrusivePtr<TOperator> op, TExprContext& ctx, TPositionHandle pos, Args... args) {
    return TPhysicalBuilder(op, ctx, pos).BuildPhysicalOp(args...);
}
