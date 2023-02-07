#include "yql_dq_state.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/transform/yql_visit.h>
#include <ydb/library/yql/core/yql_expr_constraint.h>
#include <ydb/library/yql/ast/yql_constraint.h>

namespace NYql {

using namespace NNodes;

namespace {

template <class... Other>
struct TCopyConstraint;

template <>
struct TCopyConstraint<> {
    static void Do(const TExprNode&, const TExprNode::TPtr&) {}
};

template <class TConstraint, class... Other>
struct TCopyConstraint<TConstraint, Other...> {
    static void Do(const TExprNode& from, const TExprNode::TPtr& to) {
        if (const auto c = from.GetConstraint<TConstraint>())
            to->AddConstraint(c);
        TCopyConstraint<Other...>::Do(from, to);
    }
};

class TDqDataSinkConstraintTransformer : public TVisitorTransformerBase {
public:
    TDqDataSinkConstraintTransformer()
        : TVisitorTransformerBase(true)
    {
        AddHandler({TDqStage::CallableName(), TDqPhyStage::CallableName()}, Hndl(&TDqDataSinkConstraintTransformer::HandleStage));
        AddHandler({TDqOutput::CallableName()}, Hndl(&TDqDataSinkConstraintTransformer::HandleOutput));
        AddHandler({
            TDqCnUnionAll::CallableName(),
            TDqCnBroadcast::CallableName(),
            TDqCnMap::CallableName(),
            TDqCnHashShuffle::CallableName(),
            TDqCnResult::CallableName(),
            TDqCnValue::CallableName()
            }, Hndl(&TDqDataSinkConstraintTransformer::HandleConnection));
        AddHandler({TDqCnMerge::CallableName()}, Hndl(&TDqDataSinkConstraintTransformer::HandleMerge));
        AddHandler({TDqReplicate::CallableName()}, Hndl(&TDqDataSinkConstraintTransformer::HandleReplicate));
        AddHandler({
            TDqJoin::CallableName(),
            TDqPhyMapJoin::CallableName(),
            TDqPhyCrossJoin::CallableName(),
            TDqPhyJoinDict::CallableName(),
            TDqSink::CallableName(),
            TDqWrite::CallableName(),
            TDqQuery::CallableName(),
            TDqPrecompute::CallableName(),
            TDqPhyPrecompute::CallableName(),
            TDqTransform::CallableName()
        }, Hndl(&TDqDataSinkConstraintTransformer::HandleDefault));
    }

    TStatus HandleDefault(TExprBase, TExprContext&) {
        return TStatus::Ok;
    }

    TStatus HandleStage(TExprBase input, TExprContext& ctx) {
        const auto stage = input.Cast<TDqStageBase>();
        TSmallVec<TConstraintNode::TListType> argConstraints(stage.Inputs().Size());
        for (auto i = 0U; i < argConstraints.size(); ++i)
            argConstraints[i] = stage.Inputs().Item(i).Ref().GetAllConstraints();
        return UpdateLambdaConstraints(stage.Ptr()->ChildRef(TDqStageBase::idx_Program), ctx, argConstraints);
    }

    TStatus HandleOutput(TExprBase input, TExprContext&) {
        const auto output = input.Cast<TDqOutput>();
        if (const auto multi = output.Stage().Program().Body().Ref().GetConstraint<TMultiConstraintNode>()) {
            if (const auto set = multi->GetItem(FromString<ui32>(output.Index().Value())))
                input.Ptr()->SetConstraints(*set);
        } else
            input.Ptr()->CopyConstraints(output.Stage().Program().Body().Ref());
        return TStatus::Ok;
    }

    TStatus HandleConnection(TExprBase input, TExprContext&) {
        const auto output = input.Cast<TDqConnection>().Output();
        TCopyConstraint<TUniqueConstraintNode, TDistinctConstraintNode, TEmptyConstraintNode>::Do(output.Ref(), input.Ptr());
        return TStatus::Ok;
    }

    TStatus HandleMerge(TExprBase input, TExprContext& ctx) {
        const auto output = input.Cast<TDqCnMerge>().Output();
        if (const auto outSorted = output.Ref().GetConstraint<TSortedConstraintNode>())
            input.Ptr()->AddConstraint(outSorted);
        else {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), "Expected sorted constraint on stage output."));
            return TStatus::Error;
        }
        TCopyConstraint<TUniqueConstraintNode, TDistinctConstraintNode, TEmptyConstraintNode>::Do(output.Ref(), input.Ptr());
        return TStatus::Ok;
    }

    TStatus HandleReplicate(TExprBase input, TExprContext& ctx) {
        const auto replicate = input.Cast<TDqReplicate>();
        TSmallVec<TConstraintNode::TListType> argConstraints(1U, replicate.Input().Ref().GetAllConstraints());
        TStatus status = TStatus::Ok;

        for (auto i = 1U; i < replicate.Ref().ChildrenSize(); ++i)
            status = status.Combine(UpdateLambdaConstraints(replicate.Ptr()->ChildRef(i), ctx, argConstraints));

        if (status != TStatus::Ok)
            return status;

        TMultiConstraintNode::TMapType map;
        map.reserve(replicate.Ref().ChildrenSize() - 1U);

        for (auto i = 1U; i < replicate.Ref().ChildrenSize(); ++i)
            if (auto constraints = replicate.Ref().Child(i)->Tail().GetConstraintSet())
                map.insert_unique(std::make_pair(i - 1U, std::move(constraints)));

        if (!map.empty())
            input.Ptr()->AddConstraint(ctx.MakeConstraint<TMultiConstraintNode>(std::move(map)));

        return TStatus::Ok;
    }
};

}

THolder<IGraphTransformer> CreateDqDataSinkConstraintTransformer() {
    return THolder<IGraphTransformer>(new TDqDataSinkConstraintTransformer());
}

}

