#include "dq_constraints.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

#include <yql/essentials/core/yql_expr_constraint_streaming.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

namespace NYql::NDq {

// Internal DQ-only constraint: the stage runs as a single task consuming all input data.
// Mirrors the scheduler planning one task for such a stage (see NYql::NDq::CommonBuildTasks()).
class TDqConsolidateConstraintNode final: public TConstraintNode {
protected:
    friend struct NYql::TExprContext;

    explicit TDqConsolidateConstraintNode(TExprContext& ctx)
        : TConstraintNode(ctx, Name())
    {
    }

    TDqConsolidateConstraintNode(TDqConsolidateConstraintNode&& constr)
        : TConstraintNode(std::move(static_cast<TConstraintNode&>(constr)))
    {
    }

public:
    static constexpr std::string_view Name() {
        return "DqConsolidate";
    }

    bool Equals(const TConstraintNode& node) const override {
        if (this == &node) {
            return true;
        }
        if (GetHash() != node.GetHash()) {
            return false;
        }
        return GetName() == node.GetName();
    }

    void ToJson(NJson::TJsonWriter& out) const override {
        out.Write(true);
    }

    NYT::TNode ToYson() const override {
        return NYT::TNode::CreateEntity();
    }
};

namespace {

using namespace NYql::NNodes;
using TStatus = NYql::IGraphTransformer::TStatus;

struct TCopyConstraint {
    template <class TConstraint>
    static void Do(const TExprNode& from, TExprNode& to) {
        if (const auto* constraint = from.GetConstraint<TConstraint>()) {
            to.AddConstraint(constraint);
        }
    }

    template <class TConstraint>
    static void Do(const TConstraintSet& from, TExprNode& to) {
        if (const auto* constraint = from.GetConstraint<TConstraint>()) {
            to.AddConstraint(constraint);
        }
    }
};

struct TCopyConstraints {
    template <class... TConstraint>
    static void Do(const TExprNode& from, TExprNode& to) {
        (TCopyConstraint::Do<TConstraint>(from, to), ...);
    }

    template <class... TConstraint>
    static void Do(const TConstraintSet& from, TExprNode& to) {
        (TCopyConstraint::Do<TConstraint>(from, to), ...);
    }
};

const TStructExprType* GetWideChannelStructType(const TTypeAnnotationNode& internalType, const TTypeAnnotationNode& externalType) {
    const auto* internalItemType = GetSeqItemType(&internalType);
    if (!internalItemType || internalItemType->GetKind() != ETypeAnnotationKind::Multi) {
        return nullptr;
    }

    const auto* externalItemType = GetSeqItemType(&externalType);
    if (!externalItemType || externalItemType->GetKind() != ETypeAnnotationKind::Struct) {
        return nullptr;
    }
    return externalItemType->Cast<TStructExprType>();
}

TPartOfConstraintBase::TPathReduce MakeStructToMultiPathRename(const TStructExprType& structType, TExprContext& ctx) {
    return [&structType, &ctx](const TPartOfConstraintBase::TPathType& path) {
        if (path.empty()) {
            return std::vector<TPartOfConstraintBase::TPathType>{path};
        }

        const auto index = structType.FindItem(path.front());
        if (!index) {
            return std::vector<TPartOfConstraintBase::TPathType>{};
        }

        auto renamed = path;
        renamed.front() = ctx.GetIndexAsString(*index);
        return std::vector<TPartOfConstraintBase::TPathType>{std::move(renamed)};
    };
}

TPartOfConstraintBase::TPathReduce MakeMultiToStructPathRename(const TStructExprType& structType) {
    return [&structType](const TPartOfConstraintBase::TPathType& path) {
        if (path.empty()) {
            return std::vector<TPartOfConstraintBase::TPathType>{path};
        }

        const auto index = TryFromString<ui32>(TStringBuf(path.front()));
        if (!index || *index >= structType.GetSize()) {
            return std::vector<TPartOfConstraintBase::TPathType>{};
        }

        auto renamed = path;
        renamed.front() = structType.GetItems()[*index]->GetName();
        return std::vector<TPartOfConstraintBase::TPathType>{std::move(renamed)};
    };
}

template <class... TConstraint>
TConstraintSet RenameConstraints(TConstraintSet constraints, TExprContext& ctx, const TPartOfConstraintBase::TPathReduce& reduce) {
    ([&] {
        if (const auto* constraint = constraints.RemoveConstraint<TConstraint>()) {
            constraints.AddConstraint(constraint->RenameFields(ctx, reduce));
        }
    }(), ...);
    return constraints;
}

template <class... TConstraint>
TConstraintSet OverlayConstraints(TConstraintSet constraints, const TConstraintSet& overlay) {
    ([&] {
        constraints.RemoveConstraint<TConstraint>();
        if (const auto* constraint = overlay.GetConstraint<TConstraint>()) {
            constraints.AddConstraint(constraint);
        }
    }(), ...);
    return constraints;
}

TStatus ConstraintDqCnValue(const TExprNode::TPtr& input, TExprContext& ctx) {
    const TDqConnection connection(input);
    if (connection.Output().Ref().GetConstraint<TStreamingConstraintNode>()) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Converting streaming input to value is not supported"));
        return TStatus::Error;
    }

    TCopyConstraints::Do<TUniqueConstraintNode, TDistinctConstraintNode, TEmptyConstraintNode>(connection.Output().Ref(), connection.MutableRef());
    return TStatus::Ok;
}

TStatus ConstraintDqCnMerge(const TExprNode::TPtr& input, TExprContext& ctx, bool disableCheck) {
    const TDqConnection connection(input);

    if (connection.Output().Ref().GetConstraint<TStreamingConstraintNode>()) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Streaming input is not supported for merge connection"));
        return TStatus::Error;
    }

    if (const auto outSorted = connection.Output().Ref().GetConstraint<TSortedConstraintNode>();
        !outSorted && !disableCheck) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Expected sorted constraint on stage output."));
        return TStatus::Error;
    }

    TCopyConstraints::Do<TSortedConstraintNode, TEmptyConstraintNode, TUniqueConstraintNode, TDistinctConstraintNode>(connection.Output().Ref(), connection.MutableRef());
    return TStatus::Ok;
}

TStatus ConstraintDqPhyGraceJoin(const TExprNode::TPtr& input, TExprContext& ctx) {
    const TDqJoinBase join(input);
    if (join.LeftInput().Ref().GetConstraint<TStreamingConstraintNode>() || join.RightInput().Ref().GetConstraint<TStreamingConstraintNode>()) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Streaming inputs are not supported for PhyGraceJoin"));
        return TStatus::Error;
    }

    return ConstraintDqJoin(input, ctx);
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
    if (const auto status = UpdateAllChildLambdasConstraints(*input); status != TStatus::Ok) {
        return status;
    }

    const auto& leftInputNode = *input->Child(0);
    const auto& rightInputNode = *input->Child(1);
    if (leftInputNode.GetConstraint<TStreamingConstraintNode>() || rightInputNode.GetConstraint<TStreamingConstraintNode>()) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Streaming inputs are not supported for BlockHashJoinCore"));
        return TStatus::Error;
    }
    return TStatus::Ok;
}

TStatus ConstraintDqScalarHashJoin(const TExprNode::TPtr& input, TExprContext& ctx) {
    const auto& leftInputNode = *input->Child(0);
    const auto& rightInputNode = *input->Child(1);
    if (leftInputNode.GetConstraint<TStreamingConstraintNode>() || rightInputNode.GetConstraint<TStreamingConstraintNode>()) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Streaming inputs are not supported for " << input->Content()));
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
    const auto* streaming = input->Head().GetConstraint<TStreamingConstraintNode>();
    if (!streaming) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Watermark generator requires streaming input"));
        return TStatus::Error;
    }
    if (streaming->GetEventTime().Defined()) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Event time is already assigned for streaming input"));
        return TStatus::Error;
    }

    auto& extractor = input->ChildRef(TDqPhyWatermarkGenerator::idx_WatermarkExtractor);
    TMaybe<TPartOfConstraintBase::TPathType> eventTime;
    if (const auto status = TryExtractEventTime(
            extractor,
            *extractor->Child(TCoLambda::idx_Body),
            ctx,
            eventTime
        );
        status != TStatus::Ok
    ) {
        return status;
    }
    if (!eventTime) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Event time expression must be materialized into a Timestamp column before assigning a watermark"));
        return TStatus::Error;
    }

    if (const auto status = UpdateLambdaConstraints(*input->Child(TDqPhyWatermarkGenerator::idx_PartitionKeyExtractor));
        status != TStatus::Ok) {
        return status;
    }

    if (const auto status = UpdateLambdaConstraints(*input->Child(TDqPhyWatermarkGenerator::idx_WriteTimeExtractor));
        status != TStatus::Ok) {
        return status;
    }

    input->AddConstraint(ctx.MakeConstraint<TStreamingConstraintNode>(std::move(*eventTime)));
    return TStatus::Ok;
}

class TDqConstraintsTransformer final : public TVisitorTransformerBase {
public:
    TDqConstraintsTransformer(bool disableChecks, bool processSortConstraint)
        : TVisitorTransformerBase(/* failOnUnknown */ true)
        , DisableChecks(disableChecks)
        , ProcessSortConstraint(processSortConstraint)
    {
        AddHandler({
            TDqStage::CallableName(),
            TDqPhyStage::CallableName(),
        }, Hndl(&TDqConstraintsTransformer::HandleDqStage));
        AddHandler({TDqOutput::CallableName()}, Hndl(&TDqConstraintsTransformer::HandleDqOutput));
        AddHandler({
            TDqCnUnionAll::CallableName(),
            TDqCnParallelUnionAll::CallableName(),
            TDqCnBroadcast::CallableName(),
            TDqCnMap::CallableName(),
            TDqCnStreamLookup::CallableName(),
            TDqCnHashShuffle::CallableName(),
            TDqCnResult::CallableName(),
        }, Hndl(&TDqConstraintsTransformer::HandleDqConnection));
        AddHandler({TDqCnValue::CallableName()}, Hndl(&ConstraintDqCnValue));
        AddHandler({TDqCnMerge::CallableName()}, Hndl(&TDqConstraintsTransformer::HandleDqCnMerge));
        AddHandler({TDqReplicate::CallableName()}, Hndl(&ConstraintDqReplicate));
        AddHandler({
            TDqJoin::CallableName(),
            TDqPhyMapJoin::CallableName(),
            TDqPhyCrossJoin::CallableName(),
            TDqPhyJoinDict::CallableName(),
        }, Hndl(&ConstraintDqJoin));
        AddHandler({TDqPhyGraceJoin::CallableName()}, Hndl(&ConstraintDqPhyGraceJoin));
        AddHandler({TDqPhyBlockHashJoin::CallableName()}, Hndl(&ConstraintDqBlockHashJoin));
        AddHandler({TDqBlockHashJoinCore::CallableName()}, Hndl(&ConstraintDqBlockHashJoinCore));
        AddHandler({
            TDqPhyScalarHashJoin::CallableName(),
            TDqScalarHashJoinCore::CallableName(),
        }, Hndl(&ConstraintDqScalarHashJoin));
        AddHandler({
            TDqPrecompute::CallableName(),
            TDqPhyPrecompute::CallableName(),
        }, Hndl(&ConstraintDqPrecompute));
        AddHandler({TDqPhyLength::CallableName()}, Hndl(&ConstraintDqPhyLength));
        AddHandler({TDqPhyHashCombine::CallableName()}, Hndl(&ConstraintDqPhyHashCombine));
        AddHandler({TDqPhyWatermarkGenerator::CallableName()}, Hndl(&ConstrainDqWatermarkGenerator));
    }

private:
    TStatus HandleDqCnMerge(const TExprNode::TPtr& input, TExprContext& ctx) {
        return ConstraintDqCnMerge(input, ctx, DisableChecks);
    }

    TStatus HandleDqStage(const TExprNode::TPtr& input, TExprContext& ctx) {
        return ConstraintDqStage(input, ctx, ProcessSortConstraint);
    }

    TStatus HandleDqOutput(const TExprNode::TPtr& input, TExprContext& ctx) {
        return ConstraintDqOutput(input, ctx, ProcessSortConstraint);
    }

    TStatus HandleDqConnection(const TExprNode::TPtr& input, TExprContext& ctx) {
        return ConstraintDqConnection(input, ctx, ProcessSortConstraint);
    }

    const bool DisableChecks = false;
    const bool ProcessSortConstraint = false;
};

} // anonymous namespace

TStatus ConstraintDqStage(const TExprNode::TPtr& input, TExprContext& ctx, bool processSortConstraint) {
    const TDqStageBase stage(input);
    const auto& inputs = stage.Inputs();
    TSmallVec<TConstraintNode::TListType> constraints(inputs.Size());
    for (size_t i = 0; i < inputs.Size(); ++i) {
        const auto& stageInput = inputs.Item(i);
        constraints[i] = stageInput.Ref().GetAllConstraints();
        if (!stageInput.Maybe<TDqConnection>()) {
            continue;
        }

        const auto& programArg = stage.Program().Args().Arg(i).Ref();
        YQL_ENSURE(programArg.GetTypeAnn() && stageInput.Ref().GetTypeAnn());
        const auto* structType = GetWideChannelStructType(*programArg.GetTypeAnn(), *stageInput.Ref().GetTypeAnn());
        if (!structType) {
            continue;
        }

        const auto renamed = RenameConstraints<TStreamingConstraintNode, TPartOfStreamingConstraintNode>(
            stageInput.Ref().GetConstraintSet(), ctx, MakeStructToMultiPathRename(*structType, ctx));
        constraints[i] = renamed.GetAllConstraints();
    }

    if (const auto status = UpdateLambdaConstraints(stage.Ptr()->ChildRef(TDqStageBase::idx_Program), ctx, constraints); status != TStatus::Ok) {
        return status;
    }

    YQL_ENSURE(stage.Program().Ref().GetTypeAnn() && stage.Ref().GetTypeAnn());
    const auto stageType = stage.Ref().GetTypeAnn()->Cast<TTupleExprType>();
    const auto* structType = stageType->GetSize() == 1U
        ? GetWideChannelStructType(*stage.Program().Ref().GetTypeAnn(), *stageType->GetItems().front())
        : nullptr;
    if (structType) {
        const auto renamed = RenameConstraints<TStreamingConstraintNode, TPartOfStreamingConstraintNode>(
            stage.Program().Ref().GetConstraintSet(), ctx, MakeMultiToStructPathRename(*structType));
        TCopyConstraints::Do<TStreamingConstraintNode, TPartOfStreamingConstraintNode>(renamed, stage.MutableRef());
    } else {
        TCopyConstraints::Do<TStreamingConstraintNode, TPartOfStreamingConstraintNode>(stage.Program().Ref(), stage.MutableRef());
    }

    if (processSortConstraint) {
        TCopyConstraints::Do<TSortedConstraintNode>(stage.Program().Ref(), stage.MutableRef());

        if (TDqStageSettings::Parse(stage).PartitionMode == TDqStageSettings::EPartitionMode::Single
            || AnyOf(inputs, [](const TExprBase& stageInput) { return stageInput.Maybe<TDqCnUnionAll>() || stageInput.Maybe<TDqCnMerge>(); }))
        {
            input->AddConstraint(ctx.MakeConstraint<TDqConsolidateConstraintNode>());
        }
    }

    return TStatus::Ok;
}

TStatus ConstraintDqOutput(const TExprNode::TPtr& input, TExprContext& ctx, bool processSortConstraint) {
    Y_UNUSED(ctx);

    const TDqOutput output(input);
    const auto& programBody = output.Stage().Program().Body().Ref();
    if (const auto* multi = programBody.GetConstraint<TMultiConstraintNode>()) {
        if (const auto* constraints = multi->GetItem(FromString<ui32>(output.Index().Value()))) {
            output.MutableRef().SetConstraints(*constraints);
        }
    } else {
        output.MutableRef().SetConstraints(OverlayConstraints<TStreamingConstraintNode, TPartOfStreamingConstraintNode>(
            programBody.GetConstraintSet(), output.Stage().Ref().GetConstraintSet()));
    }
    if (processSortConstraint) {
        TCopyConstraints::Do<TDqConsolidateConstraintNode>(output.Stage().Ref(), *input);
    }

    return TStatus::Ok;
}

TStatus ConstraintDqConnection(const TExprNode::TPtr& input, TExprContext& ctx, bool processSortConstraint) {
    const auto cn = TDqConnection(input);

    TCopyConstraints::Do<TEmptyConstraintNode>(cn.Output().Ref(), *input);

    if (const auto lookup = cn.Maybe<TDqCnStreamLookup>()) {
        const auto label = lookup.Cast().LeftLabel().Maybe<TCoAtom>();
        const auto rename = [&](const TPartOfConstraintBase::TPathType& path) {
            auto out = path;
            if (!out.empty() && label && !label.Cast().Value().empty()) {
                out.front() = ctx.AppendString(TStringBuilder() << label.Cast().Value() << '.' << out.front());
            }
            return std::vector<TPartOfConstraintBase::TPathType>{std::move(out)};
        };
        const auto renamed = RenameConstraints<TStreamingConstraintNode, TPartOfStreamingConstraintNode>(
            cn.Output().Ref().GetConstraintSet(), ctx, rename);
        TCopyConstraints::Do<TStreamingConstraintNode, TPartOfStreamingConstraintNode>(renamed, *input);

        const auto multiMatches = lookup.Cast().IsMultiMatches();
        if (!multiMatches || !FromString<bool>(multiMatches.Cast().Value())) {
            if (const auto* unique = cn.Output().Ref().GetConstraint<TUniqueConstraintNode>()) {
                input->AddConstraint(unique->RenameFields(ctx, rename));
            }
            if (const auto* distinct = cn.Output().Ref().GetConstraint<TDistinctConstraintNode>()) {
                input->AddConstraint(distinct->RenameFields(ctx, rename));
            }
        }
    } else {
        TCopyConstraints::Do<TUniqueConstraintNode, TDistinctConstraintNode, TStreamingConstraintNode, TPartOfStreamingConstraintNode>(cn.Output().Ref(), *input);
    }

    if (processSortConstraint && TDqCnUnionAll::Match(input.Get()) && cn.Output().Ref().GetConstraint<TDqConsolidateConstraintNode>()) {
        TCopyConstraints::Do<TSortedConstraintNode>(cn.Output().Ref(), *input);
    }

    return TStatus::Ok;
}

TStatus ConstraintDqCnMerge(const TExprNode::TPtr& input, TExprContext& ctx) {
    return ConstraintDqCnMerge(input, ctx, /* disableCheck */ false);
}

TStatus ConstraintDqReplicate(const TExprNode::TPtr& input, TExprContext& ctx) {
    const TDqReplicate replicate(input);
    TSmallVec<TConstraintNode::TListType> constraints(1, replicate.Input().Ref().GetAllConstraints());
    TStatus status = TStatus::Ok;
    for (size_t i = 1; i < replicate.Ref().ChildrenSize(); ++i) {
        status = status.Combine(UpdateLambdaConstraints(replicate.Ptr()->ChildRef(i), ctx, constraints));
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

        if (lStreaming && rStreaming) {
            ctx.AddError(TIssue(ctx.GetPosition(join.Pos()), "Stream X Stream Join is unsupported"));
            return IGraphTransformer::TStatus::Error;
        } else if (lStreaming) {
            const auto* renamed = leftRename ? lStreaming->RenameFields(ctx, leftRename) : lStreaming;
            input->AddConstraint(renamed->GetSimplifiedForType(*input->GetTypeAnn(), ctx));
        } else if (rStreaming) {
            const auto* renamed = rightRename ? rStreaming->RenameFields(ctx, rightRename) : rStreaming;
            input->AddConstraint(renamed->GetSimplifiedForType(*input->GetTypeAnn(), ctx));
        }
    }

    return TStatus::Ok;
}

std::unique_ptr<TVisitorTransformerBase> CreateDqConstraintsTransformer(bool disableCheck, bool processSortConstraint) {
    return std::make_unique<TDqConstraintsTransformer>(disableCheck, processSortConstraint);
}

} // namespace NYql::NDq

template <>
void Out<NYql::NDq::TDqConsolidateConstraintNode>(IOutputStream& out, const NYql::NDq::TDqConsolidateConstraintNode& value) {
    value.Out(out);
}
