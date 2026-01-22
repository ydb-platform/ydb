#pragma once
#include "kqp_rbo.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalJoinBuilder {
public:
    TPhysicalJoinBuilder(std::shared_ptr<TOpJoin> join, TExprContext& ctx, TPositionHandle pos)
        : Join(join)
        , Ctx(ctx)
        , Pos(pos) {
    }

    TPhysicalJoinBuilder() = delete;
    TPhysicalJoinBuilder(const TPhysicalJoinBuilder&) = delete;
    TPhysicalJoinBuilder& operator=(const TPhysicalJoinBuilder&) = delete;
    TPhysicalJoinBuilder(const TPhysicalJoinBuilder&&) = delete;
    TPhysicalJoinBuilder& operator=(const TPhysicalJoinBuilder&&) = delete;
    ~TPhysicalJoinBuilder() = default;

    TExprNode::TPtr BuildPhysicalJoin(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput);

private:
    TExprNode::TPtr BuildGraceJoinCore(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput);
    TExprNode::TPtr BuildCrossJoin(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput);
    TString GetValidJoinKind(const TString& joinKind);

    std::shared_ptr<TOpJoin> Join;
    TExprContext& Ctx;
    TPositionHandle Pos;
};
