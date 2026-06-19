#include "dq_constraints.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

namespace NYql::NDq {

namespace {

using namespace NYql::NNodes;
using TStatus = NYql::IGraphTransformer::TStatus;

template <class... TOther>
struct TCopyConstraint;

template <>
struct TCopyConstraint<> {
    static void Do(const TExprNode& from, const TExprNode::TPtr& to) {
        Y_UNUSED(from, to);
    }
};

template <class TConstraint, class... TOther>
struct TCopyConstraint<TConstraint, TOther...> {
    static void Do(const TExprNode& from, const TExprNode::TPtr& to) {
        if (const auto* constraint = from.GetConstraint<TConstraint>()) {
            to->AddConstraint(constraint);
        }
        TCopyConstraint<TOther...>::Do(from, to);
    }
};

TStatus ConstraintDqCnValue(const TExprNode::TPtr& input, TExprContext& ctx) {
    const auto output = TDqConnection(input).Output();
    if (output.Ref().GetConstraint<TStreamingConstraintNode>()) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Converting streaming input to value is not supported"));
        return TStatus::Error;
    }

    TCopyConstraint<TUniqueConstraintNode, TDistinctConstraintNode, TEmptyConstraintNode>::Do(output.Ref(), input);
    return TStatus::Ok;
}

TStatus ConstraintDqCnMerge(const TExprNode::TPtr& input, TExprContext& ctx, bool disableCheck) {
    const auto output = TDqConnection(input).Output();
    if (output.Ref().GetConstraint<TStreamingConstraintNode>()) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Streaming input is not supported for merge connection"));
        return TStatus::Error;
    }

    if (const auto outSorted = output.Ref().GetConstraint<TSortedConstraintNode>()) {
        input->AddConstraint(outSorted);
    } else if (!disableCheck) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Expected sorted constraint on stage output."));
        return TStatus::Error;
    }

    TCopyConstraint<TUniqueConstraintNode, TDistinctConstraintNode, TEmptyConstraintNode>::Do(output.Ref(), input);
    return TStatus::Ok;
}

TStatus ConstraintDqBlockHashJoin(const TExprNode::TPtr& input, TExprContext& ctx) {
    const auto join = TDqJoinBase(input);
    if (join.LeftInput().Ref().GetConstraint<TStreamingConstraintNode>() || join.RightInput().Ref().GetConstraint<TStreamingConstraintNode>()) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Streaming inputs are not supported for BlockHashJoin"));
        return TStatus::Error;
    }
    return TStatus::Ok;
}

TStatus ConstraintDqBlockHashJoinCore(const TExprNode::TPtr& input, TExprContext& ctx) {
    const auto& leftInputNode = *input->Child(0);
    const auto& rightInputNode = *input->Child(1);
    if (leftInputNode.GetConstraint<TStreamingConstraintNode>() || rightInputNode.GetConstraint<TStreamingConstraintNode>()) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Streaming inputs are not supported for BlockHashJoinCore"));
        return TStatus::Error;
    }
    return TStatus::Ok;
}

TStatus ConstraintDqPrecompute(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (input->Head().GetConstraint<TStreamingConstraintNode>()) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Streaming input is not supported for DqPrecompute"));
        return TStatus::Error;
    }
    return TStatus::Ok;
}

TStatus ConstraintDqPhyLength(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (input->Head().GetConstraint<TStreamingConstraintNode>()) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Streaming input is not supported for DqPhyLength"));
        return TStatus::Error;
    }
    return TStatus::Ok;
}

TStatus ConstraintDqPhyHashCombine(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (const auto status = UpdateAllChildLambdasConstraints(*input); status != TStatus::Ok) {
        return status;
    }
    if (input->Head().GetConstraint<TStreamingConstraintNode>()) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Streaming input is not supported for DqPhyHashCombine"));
        return TStatus::Error;
    }
    return TStatus::Ok;
}

TStatus ConstrainDqWatermarkGenerator(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);

    if (const auto status = UpdateAllChildLambdasConstraints(*input); status != TStatus::Ok) {
        return status;
    }

    TCopyConstraint<TStreamingConstraintNode>::Do(input->Head(), input);
    return TStatus::Ok;
}

class TDqConstraintsTransformer final : public TVisitorTransformerBase {
public:
    explicit TDqConstraintsTransformer(bool disableChecks)
        : TVisitorTransformerBase(/* failOnUnknown */ true)
        , DisableChecks(disableChecks)
    {
        AddHandler({
            TDqStage::CallableName(),
            TDqPhyStage::CallableName(),
        }, Hndl(&ConstraintDqStage));
        AddHandler({TDqOutput::CallableName()}, Hndl(&ConstraintDqOutput));
        AddHandler({
            TDqCnUnionAll::CallableName(),
            TDqCnParallelUnionAll::CallableName(),
            TDqCnBroadcast::CallableName(),
            TDqCnMap::CallableName(),
            TDqCnStreamLookup::CallableName(),
            TDqCnHashShuffle::CallableName(),
            TDqCnResult::CallableName(),
        }, Hndl(&ConstraintDqConnection));
        AddHandler({TDqCnValue::CallableName()}, Hndl(&ConstraintDqCnValue));
        AddHandler({TDqCnMerge::CallableName()}, HndlInt(&ConstraintDqCnMerge));
        AddHandler({TDqReplicate::CallableName()}, Hndl(&ConstraintDqReplicate));
        AddHandler({
            TDqJoin::CallableName(),
            TDqPhyMapJoin::CallableName(),
            TDqPhyCrossJoin::CallableName(),
            TDqPhyJoinDict::CallableName(),
        }, Hndl(&ConstraintDqJoin));
        AddHandler({TDqPhyGraceJoin::CallableName()}, Hndl(&ConstraintDqJoin));
        AddHandler({TDqPhyBlockHashJoin::CallableName()}, Hndl(&ConstraintDqBlockHashJoin));
        AddHandler({TDqBlockHashJoinCore::CallableName()}, Hndl(&ConstraintDqBlockHashJoinCore));
        AddHandler({
            TDqPrecompute::CallableName(),
            TDqPhyPrecompute::CallableName(),
        }, Hndl(&ConstraintDqPrecompute));
        AddHandler({TDqPhyLength::CallableName()}, Hndl(&ConstraintDqPhyLength));
        AddHandler({TDqPhyHashCombine::CallableName()}, Hndl(&ConstraintDqPhyHashCombine));
        AddHandler({TDqPhyWatermarkGenerator::CallableName()}, Hndl(&ConstrainDqWatermarkGenerator));
    }

private:
    THandler HndlInt(TStatus (*handler)(const TExprNode::TPtr&, TExprContext&, bool disableCheck)) {
        return [handler, disableCheck = DisableChecks](TExprNode::TPtr input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
            return handler(input, ctx, disableCheck);
        };
    }

    const bool DisableChecks = false;
};

} // anonymous namespace

TStatus ConstraintDqStage(const TExprNode::TPtr& input, TExprContext& ctx) {
    const TDqStageBase stage(input);
    const auto& inputs = stage.Inputs();
    TSmallVec<TConstraintNode::TListType> argConstraints(inputs.Size());
    for (size_t i = 0; i < inputs.Size(); ++i) {
        argConstraints[i] = inputs.Item(i).Ref().GetAllConstraints();
    }

    if (const auto status = UpdateLambdaConstraints(stage.Ptr()->ChildRef(TDqStageBase::idx_Program), ctx, argConstraints); status != TStatus::Ok) {
        return status;
    }

    TCopyConstraint<TStreamingConstraintNode>::Do(stage.Program().Ref(), stage.Ptr());
    return TStatus::Ok;
}

TStatus ConstraintDqOutput(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);

    const TDqOutput output(input);
    const auto& programBody = output.Stage().Program().Body().Ref();
    if (const auto* multi = programBody.GetConstraint<TMultiConstraintNode>()) {
        if (const auto* constraints = multi->GetItem(FromString<ui32>(output.Index().Value()))) {
            input->SetConstraints(*constraints);
        }
        TCopyConstraint<TStreamingConstraintNode>::Do(output.Stage().Ref(), input);
    } else {
        input->CopyConstraints(programBody);
    }

    return TStatus::Ok;
}

TStatus ConstraintDqConnection(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    TCopyConstraint<TUniqueConstraintNode, TDistinctConstraintNode, TEmptyConstraintNode, TStreamingConstraintNode>::Do(TDqConnection(input).Output().Ref(), input);
    return TStatus::Ok;
}

TStatus ConstraintDqCnMerge(const TExprNode::TPtr& input, TExprContext& ctx) {
    return ConstraintDqCnMerge(input, ctx, /* disableCheck */ false);
}

TStatus ConstraintDqReplicate(const TExprNode::TPtr& input, TExprContext& ctx) {
    const TDqReplicate replicate(input);
    TSmallVec<TConstraintNode::TListType> argConstraints(1, replicate.Input().Ref().GetAllConstraints());
    TStatus status = TStatus::Ok;
    for (size_t i = 1; i < replicate.Ref().ChildrenSize(); ++i) {
        status = status.Combine(UpdateLambdaConstraints(replicate.Ptr()->ChildRef(i), ctx, argConstraints));
    }

    if (status != TStatus::Ok) {
        return status;
    }

    TMultiConstraintNode::TMapType map;
    map.reserve(replicate.Ref().ChildrenSize() - 1);

    for (size_t i = 1; i < replicate.Ref().ChildrenSize(); ++i) {
        map.insert_unique(std::make_pair(i - 1, replicate.Ref().Child(i)->Tail().GetConstraintSet()));
    }

    if (!map.empty()) {
        input->AddConstraint(ctx.MakeConstraint<TMultiConstraintNode>(std::move(map)));
    }

    return TStatus::Ok;
}

TStatus ConstraintDqJoin(const TExprNode::TPtr& input, TExprContext& ctx) {
    const TDqJoinBase join(input);

    const auto& joinType = join.JoinType().Ref();
    if (const auto lEmpty = join.LeftInput().Ref().GetConstraint<TEmptyConstraintNode>(), rEmpty = join.RightInput().Ref().GetConstraint<TEmptyConstraintNode>(); lEmpty && rEmpty) {
        input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
    } else if (lEmpty && joinType.Content().starts_with("Left")) {
        input->AddConstraint(lEmpty);
    } else if (rEmpty && joinType.Content().starts_with("Right")) {
        input->AddConstraint(rEmpty);
    } else if ((lEmpty || rEmpty) && (joinType.IsAtom("Inner") || joinType.Content().ends_with("Semi"))) {
        input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
    }

    bool leftAny = false;
    bool rightAny = false;
    if (const auto maybeJoin = join.Maybe<TDqJoin>()) {
        if (const auto maybeFlags = maybeJoin.Cast().Flags()) {
            maybeFlags.Cast().Ref().ForEachChild([&](const TExprNode& flag) {
                if (flag.IsAtom("LeftAny")) {
                    leftAny = true;
                } else if (flag.IsAtom("RightAny")) {
                    rightAny = true;
                }
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
                    if (path.empty()) {
                        return {path};
                    }
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
            if (leftSide && lUnique) {
                unique = leftRename ? lUnique->RenameFields(ctx, leftRename) : lUnique;
            } else if (rightSide && rUnique) {
                unique = rightRename ? rUnique->RenameFields(ctx, rightRename) : rUnique;
            }
        } else {
            const bool exclusion = joinType.IsAtom("Exclusion");
            const bool useLeft = lUnique && (rOneRow || exclusion);
            const bool useRight = rUnique && (lOneRow || exclusion);

            if (useLeft && !useRight) {
                unique = leftRename ? lUnique->RenameFields(ctx, leftRename) : lUnique;
            } else if (useRight && !useLeft) {
                unique = rightRename ? rUnique->RenameFields(ctx, rightRename) : rUnique;
            } else if (useRight && useLeft) {
                unique = TUniqueConstraintNode::Merge(leftRename ? lUnique->RenameFields(ctx, leftRename) : lUnique, rightRename ? rUnique->RenameFields(ctx, rightRename) : rUnique, ctx);
            }
        }

        const auto lDistinct = join.LeftInput().Ref().GetConstraint<TDistinctConstraintNode>();
        const auto rDistinct = join.RightInput().Ref().GetConstraint<TDistinctConstraintNode>();

        if (singleSide) {
            if (leftSide && lDistinct) {
                distinct = leftRename ? lDistinct->RenameFields(ctx, leftRename) : lDistinct;
            } else if (rightSide && rDistinct) {
                distinct = rightRename ? rDistinct->RenameFields(ctx, rightRename) : rDistinct;
            }
        } else {
            const bool inner = joinType.IsAtom("Inner");
            const bool useLeft = lDistinct && rOneRow && (inner || leftSide);
            const bool useRight = rDistinct && lOneRow && (inner || rightSide);

            if (useLeft && !useRight) {
                distinct = leftRename ? lDistinct->RenameFields(ctx, leftRename) : lDistinct;
            } else if (useRight && !useLeft) {
                distinct = rightRename ? rDistinct->RenameFields(ctx, rightRename) : rDistinct;
            } else if (useLeft && useRight) {
                distinct = TDistinctConstraintNode::Merge(leftRename ? lDistinct->RenameFields(ctx, leftRename) : lDistinct, rightRename ? rDistinct->RenameFields(ctx, rightRename) : rDistinct, ctx);
            }
        }

        if (unique) {
            input->AddConstraint(unique);
        }
        if (distinct) {
            input->AddConstraint(distinct);
        }
    }

    const auto lStreaming = join.LeftInput().Ref().GetConstraint<TStreamingConstraintNode>();
    const auto rStreaming = join.RightInput().Ref().GetConstraint<TStreamingConstraintNode>();

    if (lStreaming || rStreaming) {
        if (lStreaming && (joinType.IsAtom("Right") || joinType.IsAtom("RightOnly"))) {
            ctx.AddError(TIssue(ctx.GetPosition(join.Pos()), TStringBuilder() << "Left streaming input is not supported for DqJoin " << joinType.Content()));
            return IGraphTransformer::TStatus::Error;
        }

        if (rStreaming && (joinType.IsAtom("Left") || joinType.IsAtom("LeftOnly"))) {
            ctx.AddError(TIssue(ctx.GetPosition(join.Pos()), TStringBuilder() << "Right streaming input is not supported for DqJoin " << joinType.Content()));
            return IGraphTransformer::TStatus::Error;
        }

        if (joinType.IsAtom("Full") || joinType.IsAtom("Exclusion")) {
            ctx.AddError(TIssue(ctx.GetPosition(join.Pos()), TStringBuilder() << "Streaming inputs are not supported for DqJoin " << joinType.Content()));
            return IGraphTransformer::TStatus::Error;
        }

        input->AddConstraint(ctx.MakeConstraint<TStreamingConstraintNode>());
    }

    return TStatus::Ok;
}

std::unique_ptr<TVisitorTransformerBase> CreateDqConstraintsTransformer(bool disableCheck) {
    return std::make_unique<TDqConstraintsTransformer>(disableCheck);
}

} // namespace NYql::NDq
