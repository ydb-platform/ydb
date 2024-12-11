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
            TDqCnStreamLookup::CallableName(),
            TDqCnHashShuffle::CallableName(),
            TDqCnResult::CallableName(),
            TDqCnValue::CallableName()
            }, Hndl(&TDqDataSinkConstraintTransformer::HandleConnection));
        AddHandler({TDqCnMerge::CallableName()}, Hndl(&TDqDataSinkConstraintTransformer::HandleMerge));
        AddHandler({TDqReplicate::CallableName()}, Hndl(&TDqDataSinkConstraintTransformer::HandleReplicate));
        AddHandler({
            TDqJoin::CallableName(),
            TDqPhyGraceJoin::CallableName(),
            TDqPhyMapJoin::CallableName(),
            TDqPhyCrossJoin::CallableName(),
            TDqPhyJoinDict::CallableName(),
        }, Hndl(&TDqDataSinkConstraintTransformer::HandleJoin));
        AddHandler({
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

    TStatus HandleJoin(TExprBase input, TExprContext& ctx) {
        const auto join = input.Cast<TDqJoinBase>();

        const auto& joinType = join.JoinType().Ref();
        if (const auto lEmpty = join.LeftInput().Ref().GetConstraint<TEmptyConstraintNode>(), rEmpty = join.RightInput().Ref().GetConstraint<TEmptyConstraintNode>(); lEmpty && rEmpty) {
            input.Ptr()->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
        } else if (lEmpty && joinType.Content().starts_with("Left")) {
            input.Ptr()->AddConstraint(lEmpty);
        } else if (rEmpty && joinType.Content().starts_with("Right")) {
            input.Ptr()->AddConstraint(rEmpty);
        } else if ((lEmpty || rEmpty) && (joinType.IsAtom("Inner") || joinType.Content().ends_with("Semi"))) {
            input.Ptr()->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
        }

        bool leftAny = false, rightAny = false;
        if (const auto maybeJoin = join.Maybe<TDqJoin>()) {
            if (const auto maybeFlags = maybeJoin.Cast().Flags()) {
                maybeFlags.Cast().Ref().ForEachChild([&](const TExprNode& flag) {
                    if (flag.IsAtom("LeftAny"))
                        leftAny = true;
                    else if (flag.IsAtom("RightAny"))
                        rightAny = true;
                });
            }
        }

        const auto lUnique = join.LeftInput().Ref().GetConstraint<TUniqueConstraintNode>();
        const auto rUnique = join.RightInput().Ref().GetConstraint<TUniqueConstraintNode>();

        if (lUnique || rUnique) {
            std::vector<std::string_view> leftJoinKeys, rightJoinKeys;
            const auto size = join.JoinKeys().Size();
            leftJoinKeys.reserve(size);
            rightJoinKeys.reserve(size);

            const auto fullColumnName = [&ctx](const std::string_view& table, const std::string_view& column) -> std::string_view {
                return table.empty() ? column : ctx.AppendString(TStringBuilder() << table << '.' << column);
            };

            for (const auto& keyTuple : join.JoinKeys()) {
                const auto leftLabel = keyTuple.LeftLabel().Value();
                const auto rightLabel = keyTuple.RightLabel().Value();

                leftJoinKeys.emplace_back(
                    join.LeftLabel().Maybe<TCoAtom>() || keyTuple.LeftColumn().Value().starts_with("_yql_dq_key_left_") ?
                        keyTuple.LeftColumn().Value() : fullColumnName(leftLabel, keyTuple.LeftColumn().Value()));

                rightJoinKeys.emplace_back(
                    join.RightLabel().Maybe<TCoAtom>() || keyTuple.RightColumn().Value().starts_with("_yql_dq_key_right_") ?
                        keyTuple.RightColumn().Value() : fullColumnName(rightLabel, keyTuple.RightColumn().Value()));

            }

            const TUniqueConstraintNode* unique = nullptr;
            const TDistinctConstraintNode* distinct = nullptr;

            const bool singleSide = joinType.Content().ends_with("Semi") || joinType.Content().ends_with("Only");
            const bool leftSide = joinType.Content().starts_with("Left");
            const bool rightSide = joinType.Content().starts_with("Right");

            const bool lOneRow = leftAny || lUnique && lUnique->ContainsCompleteSet(leftJoinKeys);
            const bool rOneRow = rightAny || rUnique && rUnique->ContainsCompleteSet(rightJoinKeys);

            const auto makeRename = [&ctx](const TExprBase& label) -> TPartOfConstraintBase::TPathReduce {
                if (label.Ref().IsAtom()) {
                    const auto table = label.Cast<TCoAtom>().Value();
                    return [table, &ctx](const TPartOfConstraintBase::TPathType& path) -> std::vector<TPartOfConstraintBase::TPathType> {
                        if (path.empty())
                            return {path};
                        auto out = path;
                        out.front() = ctx.AppendString(TStringBuilder() << table << '.' << out.front());
                        return {out};
                    };
                }
                return {};
            };

            const auto leftRename = makeRename(join.LeftLabel());
            const auto rightRename = makeRename(join.RightLabel());

            if (singleSide) {
                if (leftSide && lUnique)
                    unique = leftRename ? lUnique->RenameFields(ctx, leftRename) : lUnique;
                else if (rightSide && rUnique)
                    unique = rightRename ? rUnique->RenameFields(ctx, rightRename) : rUnique;
            } else {
                const bool exclusion = joinType.IsAtom("Exclusion");
                const bool useLeft = lUnique && (rOneRow || exclusion);
                const bool useRight = rUnique && (lOneRow || exclusion);

                if (useLeft && !useRight)
                    unique = leftRename ? lUnique->RenameFields(ctx, leftRename) : lUnique;
                else if (useRight && !useLeft)
                    unique = rightRename ? rUnique->RenameFields(ctx, rightRename) : rUnique;
                else if (useRight && useLeft)
                    unique = TUniqueConstraintNode::Merge(leftRename ? lUnique->RenameFields(ctx, leftRename) : lUnique, rightRename ? rUnique->RenameFields(ctx, rightRename) : rUnique, ctx);
            }

            const auto lDistinct = join.LeftInput().Ref().GetConstraint<TDistinctConstraintNode>();
            const auto rDistinct = join.RightInput().Ref().GetConstraint<TDistinctConstraintNode>();

            if (singleSide) {
                if (leftSide && lDistinct)
                    distinct = leftRename ? lDistinct->RenameFields(ctx, leftRename) : lDistinct;
                else if (rightSide && rDistinct)
                    distinct = rightRename ? rDistinct->RenameFields(ctx, rightRename) : rDistinct;
            } else {
                const bool inner = joinType.IsAtom("Inner");
                const bool useLeft = lDistinct && rOneRow && (inner || leftSide);
                const bool useRight = rDistinct && lOneRow && (inner || rightSide);

                if (useLeft && !useRight)
                    distinct = leftRename ? lDistinct->RenameFields(ctx, leftRename) : lDistinct;
                else if (useRight && !useLeft)
                    distinct = rightRename ? rDistinct->RenameFields(ctx, rightRename) : rDistinct;
                else if (useLeft && useRight)
                    distinct = TDistinctConstraintNode::Merge(leftRename ? lDistinct->RenameFields(ctx, leftRename) : lDistinct, rightRename ? rDistinct->RenameFields(ctx, rightRename) : rDistinct, ctx);
            }

            if (unique)
                input.Ptr()->AddConstraint(unique);
            if (distinct)
                input.Ptr()->AddConstraint(distinct);
        }

        return TStatus::Ok;
    }
};

}

THolder<IGraphTransformer> CreateDqDataSinkConstraintTransformer() {
    return THolder<IGraphTransformer>(new TDqDataSinkConstraintTransformer());
}

}

