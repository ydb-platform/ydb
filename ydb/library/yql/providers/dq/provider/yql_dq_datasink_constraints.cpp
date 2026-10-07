#include "yql_dq_datasink_constraints.h"

#include <ydb/library/yql/dq/constraints/dq_constraints.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>

#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/ast/yql_constraint.h>

namespace NYql {

using namespace NNodes;

namespace {

class TDqDataSinkConstraintTransformer : public TVisitorTransformerBase {
public:
    explicit TDqDataSinkConstraintTransformer(bool processSortConstraint)
        : TVisitorTransformerBase(true)
        , ProcessSortConstraint_(processSortConstraint)
    {
        AddHandler({TDqStage::CallableName(), TDqPhyStage::CallableName()}, Hndl(&TDqDataSinkConstraintTransformer::HandleDqStage));
        AddHandler({TDqOutput::CallableName()}, Hndl(&TDqDataSinkConstraintTransformer::HandleDqOutput));
        AddHandler({
            TDqCnUnionAll::CallableName(),
            TDqCnBroadcast::CallableName(),
            TDqCnMap::CallableName(),
            TDqCnStreamLookup::CallableName(),
            TDqCnHashShuffle::CallableName(),
            TDqCnResult::CallableName(),
            TDqCnValue::CallableName()
            }, Hndl(&TDqDataSinkConstraintTransformer::HandleDqConnection));
        AddHandler({TDqCnMerge::CallableName()}, Hndl(&NDq::ConstraintDqCnMerge));
        AddHandler({TDqReplicate::CallableName()}, Hndl(&NDq::ConstraintDqReplicate));
        AddHandler({
            TDqJoin::CallableName(),
            TDqPhyGraceJoin::CallableName(),
            TDqPhyMapJoin::CallableName(),
            TDqPhyCrossJoin::CallableName(),
            TDqPhyJoinDict::CallableName(),
        }, Hndl(&NDq::ConstraintDqJoin));
        AddHandler({
            TDqSink::CallableName(),
            TDqWrite::CallableName(),
            TDqQuery::CallableName(),
            TDqPrecompute::CallableName(),
            TDqPhyPrecompute::CallableName(),
            TDqTransform::CallableName()
        }, Hndl(&TDqDataSinkConstraintTransformer::HandleDefault));
    }

private:
    TStatus HandleDqStage(const TExprNode::TPtr& input, TExprContext& ctx) {
        return NDq::ConstraintDqStage(input, ctx, ProcessSortConstraint_);
    }

    TStatus HandleDqOutput(const TExprNode::TPtr& input, TExprContext& ctx) {
        return NDq::ConstraintDqOutput(input, ctx, ProcessSortConstraint_);
    }

    TStatus HandleDqConnection(const TExprNode::TPtr& input, TExprContext& ctx) {
        return NDq::ConstraintDqConnection(input, ctx, ProcessSortConstraint_);
    }

    TStatus HandleDefault(TExprBase, TExprContext&) {
        return TStatus::Ok;
    }

private:
    const bool ProcessSortConstraint_;
};

} // anonymous namespace

THolder<IGraphTransformer> CreateDqDataSinkConstraintTransformer(bool processSortConstraint) {
    return THolder<IGraphTransformer>(new TDqDataSinkConstraintTransformer(processSortConstraint));
}

} // namespace NYql
