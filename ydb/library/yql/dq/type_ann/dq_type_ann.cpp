#include "dq_type_ann.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NYql::NDq {

using namespace NYql::NNodes;
using TStatus = NYql::IGraphTransformer::TStatus;

namespace {

const TTypeAnnotationNode* GetDqOutputType(const TDqOutput& output, TExprContext& ctx) {
    auto stageResultTuple = output.Stage().Ref().GetTypeAnn()->Cast<TTupleExprType>();

    ui32 resultIndex;
    if (!TryFromString(output.Index().Value(), resultIndex)) {
        ctx.AddError(TIssue(ctx.GetPosition(output.Pos()),
            TStringBuilder() << "Failed to convert to integer: " << output.Index().Value()));
        return nullptr;
    }

    if (stageResultTuple->GetSize() == 0) {
        ctx.AddError(TIssue(ctx.GetPosition(output.Pos()), "Stage result is empty"));
        return nullptr;
    }

    if (resultIndex >= stageResultTuple->GetSize()) {
        ctx.AddError(TIssue(ctx.GetPosition(output.Pos()),
            TStringBuilder() << "Stage result index out of bounds: " << resultIndex));
        return nullptr;
    }

    auto outputType = stageResultTuple->GetItems()[resultIndex];
    if (!EnsureListType(output.Pos(), *outputType, ctx)) {
        return nullptr;
    }

    return outputType;
}

const TTypeAnnotationNode* GetDqConnectionType(const TDqConnection& node, TExprContext& ctx) {
    return GetDqOutputType(node.Output(), ctx);
}

const TTypeAnnotationNode* GetColumnType(const TDqConnection& node, const TStructExprType& structType, TStringBuf name, TPositionHandle pos, TExprContext& ctx) {
    TDqStageSettings settings = TDqStageSettings::Parse(node.Output().Stage());
    if (settings.WideChannels) {
        auto multiType = node.Output().Stage().Program().Ref().GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()->Cast<TMultiExprType>();
        ui32 idx;
        if (!TryFromString(name, idx)) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Expecting integer as column name, but got '" << name << "'"));
            return nullptr;
        }
        const bool isBlock = AnyOf(multiType->GetItems(), [](const TTypeAnnotationNode* item) { return item->IsBlockOrScalar(); });
        const ui32 width = isBlock ? (multiType->GetSize() - 1) : multiType->GetSize();
        if (idx >= width) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Column index too big: " << name << " >= " << width));
            return nullptr;
        }

        auto itemType = multiType->GetItems()[idx];
        if (isBlock) {
            itemType = itemType->IsBlock() ? itemType->Cast<TBlockExprType>()->GetItemType() :
                                             itemType->Cast<TScalarExprType>()->GetItemType();
        }
        return itemType;
    }

    auto result = structType.FindItemType(name);
    if (!result) {
        ctx.AddError(TIssue(ctx.GetPosition(pos),
            TStringBuilder() << "Missing column '" << name << "'"));
        return nullptr;
    }

    return result;
}

template <typename TStage>
TStatus AnnotateStage(const TExprNode::TPtr& stage, TExprContext& ctx) {
    if (!EnsureMinMaxArgsCount(*stage, 3, 4, ctx)) {
        return TStatus::Error;
    }

    auto* inputsTuple = stage->Child(TDqStageBase::idx_Inputs);
    auto& programLambda = stage->ChildRef(TDqStageBase::idx_Program);

    if (!EnsureTuple(*inputsTuple, ctx)) {
        return TStatus::Error;
    }

    if (!TDqStageSettings::Validate(*stage, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureLambda(*programLambda, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureArgsCount(programLambda->Head(), inputsTuple->ChildrenSize(), ctx)) {
        return TStatus::Error;
    }

    TVector<const TTypeAnnotationNode*> argTypes;
    argTypes.reserve(inputsTuple->ChildrenSize());

    for (const auto& input: inputsTuple->Children()) {
        if (!TDqPhyPrecompute::Match(input.Get()) &&
            !(TDqConnection::Match(input.Get()) && !TDqCnValue::Match(input.Get())) &&
            !TDqSource::Match(input.Get()) &&
            !(input->Content() == "KqpTxResultBinding"sv))
        {
            ctx.AddError(TIssue(TStringBuilder() << "Unexpected stage input " << input->Content()));
            return TStatus::Error;
        }

        auto* argType = input->GetTypeAnn();
        if constexpr (std::is_same_v<TStage, TDqPhyStage>) {
            if (TDqConnection::Match(input.Get()) && argType->GetKind() == ETypeAnnotationKind::List) {
                auto* itemType = argType->Cast<TListExprType>()->GetItemType();
                if (!itemType->IsPersistable()) {
                    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                                        << "Expected persistable data, but got: "
                                        << *itemType));
                    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Persistable required. Atom, type, key, world, datasink, datasource, callable, resource, stream and lambda are not persistable"));
                    return TStatus::Error;
                }
            }

            if (TDqConnection::Match(input.Get())) {
                TDqConnection conn(input);
                if (TDqStageSettings::Parse(conn.Output().Stage()).WideChannels) {
                    if (TDqCnStreamLookup::Match(input.Get())) {
                        auto narrowType = GetSequenceItemType(input->Pos(), input->GetTypeAnn(), false, ctx);
                        YQL_ENSURE(narrowType->GetKind() == ETypeAnnotationKind::Struct);
                        TTypeAnnotationNode::TListType items;
                        for(const auto& item: narrowType->Cast<TStructExprType>()->GetItems()) {
                            items.push_back(item->GetItemType());
                        }
                        argType = ctx.MakeType<TStreamExprType>(ctx.MakeType<TMultiExprType>(items));
                    } else {
                        argType = conn.Output().Stage().Program().Ref().GetTypeAnn();
                    }
                }
            }
        }

        if (!TDqPhyPrecompute::Match(input.Get()) && input->Content() != "KqpTxResultBinding") {
            if (argType->GetKind() == ETypeAnnotationKind::List) {
                auto* listItemType = argType->Cast<TListExprType>()->GetItemType();
                if constexpr (std::is_same_v<TStage, TDqPhyStage>) {
                    argType = ctx.MakeType<TStreamExprType>(listItemType);
                } else {
                    argType = ctx.MakeType<TFlowExprType>(listItemType);
                }
            }
        }
        argTypes.emplace_back(argType);
    }

    if (!UpdateLambdaAllArgumentsTypes(programLambda, argTypes, ctx)) {
        return TStatus::Error;
    }

    auto* resultType = programLambda->GetTypeAnn();
    if (!resultType) {
        return TStatus::Repeat;
    }

    TVector<const TTypeAnnotationNode*> programResultTypesTuple;
    if (resultType->GetKind() == ETypeAnnotationKind::Void) {
        // do nothing, return empty tuple as program result
    } else {
        const TTypeAnnotationNode* itemType = nullptr;
        if (resultType->GetKind() == ETypeAnnotationKind::Flow) {
            itemType = resultType->template Cast<TFlowExprType>()->GetItemType();
        } else if (resultType->GetKind() == ETypeAnnotationKind::Stream) {
            itemType = resultType->template Cast<TStreamExprType>()->GetItemType();
        }
        if (itemType) {
            if (itemType->GetKind() == ETypeAnnotationKind::Variant) {
                auto variantType = itemType->Cast<TVariantExprType>()->GetUnderlyingType();
                YQL_ENSURE(variantType->GetKind() == ETypeAnnotationKind::Tuple);
                const auto& items = variantType->Cast<TTupleExprType>()->GetItems();
                programResultTypesTuple.reserve(items.size());
                for (const auto* branchType : items) {
                    programResultTypesTuple.emplace_back(ctx.MakeType<TListExprType>(branchType));
                }
            } else {
                programResultTypesTuple.emplace_back(ctx.MakeType<TListExprType>(itemType));
            }
        } else {
            YQL_ENSURE(resultType->GetKind() != ETypeAnnotationKind::List, "stage: " << stage->Dump());
            programResultTypesTuple.emplace_back(resultType);
        }
    }

    const TDqStageSettings settings = TDqStageSettings::Parse(TDqStageBase(stage));
    if (settings.WideChannels) {
        if (!EnsureWideStreamType(*programLambda, ctx)) {
            ctx.AddError(TIssue(ctx.GetPosition(programLambda->Pos()),TStringBuilder() << "Wide channel stage requires exactly one output, but got " << programResultTypesTuple.size()));
            return TStatus::Error;
        }
        YQL_ENSURE(programResultTypesTuple.size() == 1);
        auto multiType = programLambda->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()->Cast<TMultiExprType>();
        const bool isBlock = AnyOf(multiType->GetItems(), [](const TTypeAnnotationNode* item) { return item->IsBlockOrScalar(); });
        TTypeAnnotationNode::TListType blockItemTypes;
        if (isBlock && !EnsureWideStreamBlockType(*programLambda, blockItemTypes, ctx)) {
            return TStatus::Error;
        }

        const ui32 width = isBlock ? (blockItemTypes.size() - 1) : multiType->GetSize();
        if (width != settings.OutputNarrowType->GetSize()) {
            ctx.AddError(TIssue(ctx.GetPosition(programLambda->Pos()),TStringBuilder() << "Wide/narrow types has different number of items: " <<
                width << " vs " << settings.OutputNarrowType->GetSize()));
            return TStatus::Error;
        }

        for (size_t i = 0; i < settings.OutputNarrowType->GetSize(); ++i) {
            auto structItem = settings.OutputNarrowType->GetItems()[i];
            auto wideItem = isBlock ? blockItemTypes[i] : multiType->GetItems()[i];
            if (!IsSameAnnotation(*structItem->GetItemType(), *wideItem)) {
                ctx.AddError(TIssue(ctx.GetPosition(programLambda->Pos()),TStringBuilder() << "Wide/narrow types mismatch for column '" <<
                    structItem->GetName() << "' : " << *wideItem << " vs " << *structItem->GetItemType()));
                return TStatus::Error;
            }
        }

        programResultTypesTuple[0] = ctx.MakeType<TListExprType>(settings.OutputNarrowType);
    }

    TVector<const TTypeAnnotationNode*> stageResultTypes;
    if (TDqStageBase::idx_Outputs < stage->ChildrenSize()) {
        YQL_ENSURE(stage->Child(TDqStageBase::idx_Outputs)->ChildrenSize() != 0, "Stage.Outputs list exists but empty, stage: " << stage->Dump());

        if (settings.WideChannels) {
            ctx.AddError(TIssue(ctx.GetPosition(programLambda->Pos()),TStringBuilder() << "Wide channel stage is incompatible with Sink/Transform"));
            return TStatus::Error;
        }

        auto outputsNumber = programResultTypesTuple.size();
        TVector<TExprNode::TPtr> transforms;
        TVector<TExprNode::TPtr> sinks;
        transforms.reserve(outputsNumber);
        sinks.reserve(outputsNumber);
        for (const auto& output: stage->Child(TDqStageBase::idx_Outputs)->Children()) {
            const ui64 index = FromString(output->Child(TDqOutputAnnotationBase::idx_Index)->Content());
            if (index >= outputsNumber) {
                ctx.AddError(TIssue(ctx.GetPosition(stage->Pos()), TStringBuilder()
                    << "Sink/Transform try to process un-existing lambda's output"));
                return TStatus::Error;
            }

            if (output->IsCallable(TDqSink::CallableName())) {
                sinks.push_back(output);
            } else if (output->IsCallable(TDqTransform::CallableName())) {
                transforms.push_back(output);
            } else {
                YQL_ENSURE(false, "Unknown stage output type " << output->Content());
            }
        }

        if (!sinks.empty()) {
            stageResultTypes.assign(programResultTypesTuple.begin(), programResultTypesTuple.end());
        } else {
            for (auto transform : transforms) {
                auto* type = transform->GetTypeAnn();
                if (!EnsureListType(transform->Pos(), *type, ctx)) {
                    return TStatus::Error;
                }
                stageResultTypes.emplace_back(type);
            }
        }
    } else {
        stageResultTypes.assign(programResultTypesTuple.begin(), programResultTypesTuple.end());
    }

    stage->SetTypeAnn(ctx.MakeType<TTupleExprType>(stageResultTypes));
    return TStatus::Ok;
}

THashMap<TStringBuf, THashMap<TStringBuf, const TTypeAnnotationNode*>>
ParseJoinInputType(const TStructExprType& rowType, TStringBuf tableLabel, TExprContext& ctx, bool optional) {
    THashMap<TStringBuf, THashMap<TStringBuf, const TTypeAnnotationNode*>> result;
    for (auto member : rowType.GetItems()) {
        TStringBuf label, column;
        if (member->GetName().Contains('.')) {
            SplitTableName(member->GetName(), label, column);
        } else {
            column = member->GetName();
        }
        const bool isSystemKeyColumn = column.starts_with("_yql_dq_key_");
        if (label.empty() && tableLabel.empty() && !isSystemKeyColumn) {
            ctx.AddError(TIssue(TStringBuilder() << "Invalid join input type " << FormatType(&rowType)));
            result.clear();
            return result;
        }
        auto memberType = member->GetItemType();
        if (optional && !memberType->IsOptionalOrNull()) {
            memberType = ctx.MakeType<TOptionalExprType>(memberType);
        }
        if (!tableLabel.empty()) {
            result[tableLabel][member->GetName()] = memberType;
        } else {
            result[label][column] = memberType;
        }
    }
    return result;
}

template <bool IsMapJoin>
const TStructExprType* GetDqJoinResultType(TPositionHandle pos, const TStructExprType& leftRowType,
    const TStringBuf& leftLabel, const TStructExprType& rightRowType, const TStringBuf& rightLabel,
    const TStringBuf& joinType, const TDqJoinKeyTupleList& joinKeys, TExprContext& ctx)
{
    // check left
    bool isLeftOptional = IsLeftJoinSideOptional(joinType);
    auto leftType = ParseJoinInputType(leftRowType, leftLabel, ctx, isLeftOptional);
    if (leftType.empty() && joinType != "Cross") {
        TStringStream str; str << "Cannot parse left join input type: ";
        leftRowType.Out(str);
        ctx.AddError(TIssue(ctx.GetPosition(pos), str.Str()));
        return nullptr;
    }

    // check right
    bool isRightOptional = IsRightJoinSideOptional(joinType);
    auto rightType = ParseJoinInputType(rightRowType, rightLabel, ctx, isRightOptional);
    if (rightType.empty() && joinType != "Cross") {
        TStringStream str; str << "Cannot parse right join input type: ";
        rightRowType.Out(str);
        ctx.AddError(TIssue(ctx.GetPosition(pos), str.Str()));
        return nullptr;
    }

    if constexpr (IsMapJoin) {
        if (joinType.StartsWith("Right") || joinType == "Cross") {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder()
                << "Unsupported map join type: " << joinType));
            return nullptr;
        }
    }

    // check join keys
    if (joinKeys.Empty() && joinType != "Cross") {
        ctx.AddError(TIssue(ctx.GetPosition(pos), "No join keys"));
        return nullptr;
    }

    for (const auto& key : joinKeys) {
        auto leftKeyLabel = key.LeftLabel().Value();
        auto leftKeyColumn = key.LeftColumn().Value();
        auto rightKeyLabel = key.RightLabel().Value();
        auto rightKeyColumn = key.RightColumn().Value();

        if (leftLabel && leftLabel != leftKeyLabel) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "different labels for left table"));
            return nullptr;
        }
        if (rightLabel && rightLabel != rightKeyLabel) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "different labels for right table"));
            return nullptr;
        }

        auto maybeLeftKeyType = leftType[leftKeyLabel].FindPtr(leftKeyColumn);
        if (!maybeLeftKeyType && leftKeyColumn.starts_with("_yql_dq_key_left"))
            maybeLeftKeyType = leftType[""].FindPtr(leftKeyColumn);
        if (!maybeLeftKeyType) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder()
                << "Left key " << leftKeyLabel << "." << leftKeyColumn << " not found"));
            return nullptr;
        }

        auto maybeRightKeyType = rightType[rightKeyLabel].FindPtr(rightKeyColumn);
        if (!maybeRightKeyType && rightKeyColumn.starts_with("_yql_dq_key_right"))
            maybeRightKeyType = rightType[""].FindPtr(rightKeyColumn);
        if (!maybeRightKeyType) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder()
                << "Right key " << rightKeyLabel << "." << rightKeyColumn << " not found"));
            return nullptr;
        }

        auto comparable = CanCompare<true>(*maybeLeftKeyType, *maybeRightKeyType);
        if (comparable != ECompareOptions::Comparable && comparable != ECompareOptions::Optional) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder()
                << "Not comparable keys: " << leftKeyLabel << "." << leftKeyColumn
                << " and " << rightKeyLabel << "." << rightKeyColumn << ", "
                << FormatType(*maybeLeftKeyType) << " != " << FormatType(*maybeRightKeyType)));
            return nullptr;
        }
    }

    auto addAllMembersFrom = [&ctx](const THashMap<TStringBuf, THashMap<TStringBuf, const TTypeAnnotationNode*>>& type,
        TVector<const TItemExprType*>* result, bool makeOptional = false)
    {
        for (const auto& it : type) {
            for (const auto& it2 : it.second) {
                const auto memberName = it.first.empty() ? TString(it2.first) : FullColumnName(it.first, it2.first);
                if (makeOptional && !it2.second->IsOptionalOrNull()) {
                    result->emplace_back(ctx.MakeType<TItemExprType>(memberName, ctx.MakeType<TOptionalExprType>(it2.second)));
                } else {
                    result->emplace_back(ctx.MakeType<TItemExprType>(memberName, it2.second));
                }
            }
        }
    };

    TVector<const TItemExprType*> resultStructItems;
    if (joinType != "RightOnly" && joinType != "RightSemi") {
        addAllMembersFrom(leftType, &resultStructItems, joinType == "Right");
    }
    if (joinType != "LeftOnly" && joinType != "LeftSemi") {
        addAllMembersFrom(rightType, &resultStructItems, joinType == "Left");
    }

    auto rowType = ctx.MakeType<TStructExprType>(resultStructItems);
    return rowType;
}

template <bool IsMapJoin>
const TStructExprType* GetDqJoinResultType(const TExprNode::TPtr& input, bool stream, TExprContext& ctx) {
    if (!EnsureMinMaxArgsCount(*input, 8, 10, ctx)) {
        return nullptr;
    }

    if (!input->Child(TDqJoin::idx_LeftLabel)->IsCallable("Void")) {
        if (!EnsureAtom(*input->Child(TDqJoin::idx_LeftLabel), ctx)) {
            return nullptr;
        }
    }

    if (!input->Child(TDqJoin::idx_RightLabel)->IsCallable("Void")) {
        if (!EnsureAtom(*input->Child(TDqJoin::idx_RightLabel), ctx)) {
            return nullptr;
        }
    }

    if (!EnsureAtom(*input->Child(TDqJoin::idx_JoinType), ctx)) {
        return nullptr;
    }

    if (!EnsureTuple(*input->Child(TDqJoin::idx_JoinKeys), ctx)) {
        return nullptr;
    }

    for (auto& child: input->Child(TDqJoin::idx_JoinKeys)->Children()) {
        if (!EnsureTupleSize(*child, 4, ctx)) {
            return nullptr;
        }
        for (auto& subChild: child->Children()) {
            if (!EnsureAtom(*subChild, ctx)) {
                return nullptr;
            }
        }
    }

    auto join = TDqJoinBase(input);

    auto leftInputType = join.LeftInput().Ref().GetTypeAnn();
    auto rightInputType = join.RightInput().Ref().GetTypeAnn();

    if (stream) {
        if (!EnsureNewSeqType<false, false, true>(join.Pos(), *leftInputType, ctx)) {
            return nullptr;
        }
        if (!EnsureNewSeqType<false, false, true>(join.Pos(), *rightInputType, ctx)) {
            return nullptr;
        }
    } else {
        if (!EnsureNewSeqType<false, true, false>(join.Pos(), *leftInputType, ctx)) {
            return nullptr;
        }
        if (!EnsureNewSeqType<false, true, false>(join.Pos(), *rightInputType, ctx)) {
            return nullptr;
        }
    }

    const auto& leftInputItemType = GetSeqItemType(*leftInputType);
    if (!EnsureStructType(join.Pos(), leftInputItemType, ctx)) {
        return nullptr;
    }
    auto leftStructType = leftInputItemType.Cast<TStructExprType>();
    auto leftTableLabel = join.LeftLabel().Maybe<TCoAtom>()
        ? join.LeftLabel().Cast<TCoAtom>().Value()
        : TStringBuf("");

    const auto& rightInputItemType = GetSeqItemType(*rightInputType);
    if (!EnsureStructType(join.Pos(), rightInputItemType, ctx)) {
        return nullptr;
    }
    auto rightStructType = rightInputItemType.Cast<TStructExprType>();
    auto rightTableLabel = join.RightLabel().Maybe<TCoAtom>()
        ? join.RightLabel().Cast<TCoAtom>().Value()
        : TStringBuf("");

    if (input->ChildrenSize() > 9U) {
        for (auto i = 0U; i < input->Tail().ChildrenSize(); ++i) {
            if (const auto& flag = *input->Tail().Child(i); !flag.IsAtom({"LeftAny", "RightAny"})) {
                ctx.AddError(TIssue(ctx.GetPosition(flag.Pos()), TStringBuilder() << "Unsupported DQ join option: " << flag.Content()));
                return nullptr;
            }
        }
    }

    return GetDqJoinResultType<IsMapJoin>(join.Pos(), *leftStructType, leftTableLabel, *rightStructType,
        rightTableLabel, join.JoinType(), join.JoinKeys(), ctx);
}

} // unnamed

TStatus AnnotateDqStage(const TExprNode::TPtr& input, TExprContext& ctx) {
    return AnnotateStage<TDqStage>(input, ctx);
}

TStatus AnnotateDqPhyStage(const TExprNode::TPtr& input, TExprContext& ctx) {
    return AnnotateStage<TDqPhyStage>(input, ctx);
}

TStatus AnnotateDqOutput(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (!EnsureArgsCount(*input, 2, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureCallable(*input->Child(TDqOutput::idx_Stage), ctx)) {
        return TStatus::Error;
    }

    if (!TDqStageBase::Match(input->Child(TDqOutput::idx_Stage))) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Child(TDqOutput::idx_Stage)->Pos()), TStringBuilder() << "Expected " << TDqStage::CallableName() << " or " << TDqPhyStage::CallableName()));
        return TStatus::Error;
    }

    if (!EnsureAtom(*input->Child(TDqOutput::idx_Index), ctx)) {
        return TStatus::Error;
    }

    auto resultType = GetDqOutputType(TDqOutput(input), ctx);
    if (!resultType) {
        return TStatus::Error;
    }

    input->SetTypeAnn(resultType);
    return TStatus::Ok;
}

TStatus AnnotateDqConnection(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (!EnsureArgsCount(*input, 1, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureCallable(*input->Child(TDqConnection::idx_Output), ctx)) {
        return TStatus::Error;
    }

    if (!TDqOutput::Match(input->Child(TDqConnection::idx_Output))) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Child(TDqConnection::idx_Output)->Pos()), TStringBuilder() << "Expected " << TDqOutput::CallableName()));
        return TStatus::Error;
    }

    auto resultType = GetDqConnectionType(TDqConnection(input), ctx);
    if (!resultType) {
        return TStatus::Error;
    }

    input->SetTypeAnn(resultType);
    return TStatus::Ok;
}

TStatus AnnotateDqCnStreamLookup(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto cnStreamLookup = TDqCnStreamLookup(input);
    auto leftInputType = GetDqConnectionType(TDqConnection(input), ctx);
    if (!leftInputType) {
        return TStatus::Error;
    }
    const auto leftRowType = GetSeqItemType(leftInputType);
    const auto rightRowType = GetSeqItemType(cnStreamLookup.RightInput().Raw()->GetTypeAnn());
    const auto outputRowType = GetDqJoinResultType<true>(
        input->Pos(),
        *leftRowType->Cast<TStructExprType>(),
        cnStreamLookup.LeftLabel().Cast<TCoAtom>().StringValue(),
        *rightRowType->Cast<TStructExprType>(),
        cnStreamLookup.RightLabel().StringValue(),
        cnStreamLookup.JoinType().StringValue(),
        cnStreamLookup.JoinKeys(),
        ctx
    );
    //TODO (YQ-2068) verify lookup parameters
    input->SetTypeAnn(ctx.MakeType<TStreamExprType>(outputRowType));
    return TStatus::Ok;
}

TStatus AnnotateDqCnMerge(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureCallable(*node->Child(TDqCnMerge::idx_Output), ctx)) {
        return TStatus::Error;
    }

    if (!TDqOutput::Match(node->Child(TDqCnMerge::idx_Output))) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Child(TDqCnMerge::idx_Output)->Pos()), TStringBuilder() << "Expected " << TDqOutput::CallableName()));
        return TStatus::Error;
    }

    auto cnMerge = TDqCnMerge(node);

    if (!EnsureTupleMinSize(*cnMerge.SortColumns().Ptr(), 1, ctx)) {
        return TStatus::Error;
    }

    auto outputType = GetDqConnectionType(TDqConnection(node), ctx);
    if (!outputType) {
        return TStatus::Error;
    }

    auto itemType = outputType->Cast<TListExprType>()->GetItemType();
    if (!EnsureStructType(node->Pos(), *itemType, ctx)) {
        return TStatus::Error;
    }

    auto structType = itemType->Cast<TStructExprType>();
    for (const auto& column : cnMerge.SortColumns()) {
        if (!EnsureTuple(*column.Ptr(), ctx)) {
            return TStatus::Error;
        }
        if (column.Column().StringValue().empty())
        {
            return TStatus::Error;
        }

        auto colType = GetColumnType(TDqConnection(node), *structType, column.Column().Value(), column.Pos(), ctx);
        if (!colType) {
            return TStatus::Error;
        }

        if (colType->GetKind() == ETypeAnnotationKind::Optional) {
            colType = colType->Cast<TOptionalExprType>()->GetItemType();
        }
        if (!EnsureDataType(column.Pos(), *colType, ctx)) {
            ctx.AddError(TIssue(ctx.GetPosition(column.Pos()),
                TStringBuilder() << "For Merge connection column should be Data Expression: " << column.Column().StringValue()));
            return TStatus::Error;
        }
        if (!IsTypeSupportedInMergeCn(colType->Cast<TDataExprType>())) {
            ctx.AddError(TIssue(ctx.GetPosition(column.Pos()),
                TStringBuilder() << "Unsupported type " << colType->Cast<TDataExprType>()->GetName()
                << " for column '" << column.Column().StringValue() << "' in Merge connection."));
            return TStatus::Error;
        }
    }

    node->SetTypeAnn(outputType);
    return TStatus::Ok;
}

TStatus AnnotateDqCnHashShuffle(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (!EnsureArgsCount(*input, 2, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureCallable(*input->Child(TDqCnHashShuffle::idx_Output), ctx)) {
        return TStatus::Error;
    }

    if (!TDqOutput::Match(input->Child(TDqCnHashShuffle::idx_Output))) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Child(TDqCnHashShuffle::idx_Output)->Pos()), TStringBuilder() << "Expected " << TDqOutput::CallableName()));
        return TStatus::Error;
    }

    if (!EnsureTupleMinSize(*input->Child(TDqCnHashShuffle::idx_KeyColumns), 1, ctx)) {
        return TStatus::Error;
    }

    auto outputType = GetDqConnectionType(TDqConnection(input), ctx);
    if (!outputType) {
        return TStatus::Error;
    }

    auto itemType = outputType->Cast<TListExprType>()->GetItemType();
    if (!EnsureStructType(input->Pos(), *itemType, ctx)) {
        return TStatus::Error;
    }

    auto structType = itemType->Cast<TStructExprType>();
    for (const auto& column: input->Child(TDqCnHashShuffle::idx_KeyColumns)->Children()) {
        if (!EnsureAtom(*column, ctx)) {
            return TStatus::Error;
        }
        auto ty = GetColumnType(TDqConnection(input), *structType, column->Content(), column->Pos(), ctx);
        if (!ty) {
            return TStatus::Error;
        }

        if (!ty->IsHashable()) {
            ctx.AddError(TIssue(ctx.GetPosition(column->Pos()),
                TStringBuilder() << "Non-hashable key column: " << column->Content()));
            return TStatus::Error;
        }
    }

    input->SetTypeAnn(outputType);
    return TStatus::Ok;
}

TStatus AnnotateDqCnValue(const TExprNode::TPtr& cnValue, TExprContext& ctx) {
    if (!EnsureArgsCount(*cnValue, 1, ctx)) {
        return TStatus::Error;
    }

    auto& output = cnValue->ChildRef(TDqCnValue::idx_Output);
    if (!TDqOutput::Match(output.Get())) {
        ctx.AddError(TIssue(ctx.GetPosition(output->Pos()), TStringBuilder() << "Expected " << TDqOutput::CallableName()
            << ", got " << output->Content()));
        return TStatus::Error;
    }

    auto* resultType = GetDqOutputType(TDqOutput(output), ctx);
    if (!resultType) {
        return TStatus::Error;
    }

    const TTypeAnnotationNode* outputType = resultType;
    if (resultType->GetKind() == ETypeAnnotationKind::List) {
        outputType = resultType->Cast<TListExprType>()->GetItemType();
    }

    cnValue->SetTypeAnn(outputType);
    return TStatus::Ok;
}

TStatus AnnotateDqCnResult(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (!EnsureArgsCount(*input, 2, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureCallable(*input->Child(TDqCnResult::idx_Output), ctx)) {
        return TStatus::Error;
    }

    if (!TDqOutput::Match(input->Child(TDqCnResult::idx_Output))) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Child(TDqCnResult::idx_Output)->Pos()), TStringBuilder() << "Expected " << TDqOutput::CallableName()));
        return TStatus::Error;
    }

    if (!EnsureTuple(*input->Child(TDqCnResult::idx_ColumnHints), ctx)) {
        return TStatus::Error;
    }

    for (const auto& column: input->Child(TDqCnResult::idx_ColumnHints)->Children()) {
        if (!EnsureAtom(*column, ctx)) {
            return TStatus::Error;
        }
    }

    auto outputType = GetDqConnectionType(TDqConnection(input), ctx);
    if (!outputType) {
        return TStatus::Error;
    }

    input->SetTypeAnn(outputType);
    return TStatus::Ok;
}

TStatus AnnotateDqReplicate(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (!EnsureMinArgsCount(*input, 3, ctx)) {
        return TStatus::Error;
    }
    auto replicateInput = input->Child(TDqReplicate::idx_Input);
    if (!EnsureFlowType(*replicateInput, ctx)) {
        return TStatus::Error;
    }
    auto inputItemType = replicateInput->GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
    if (!EnsurePersistableType(replicateInput->Pos(), *inputItemType, ctx)) {
        return TStatus::Error;
    }

    if (inputItemType->GetKind() == ETypeAnnotationKind::Tuple) {
        if (!EnsureTupleTypeSize(replicateInput->Pos(), inputItemType, 2, ctx)) {
            return TStatus::Error;
        }

        auto inputTupleType = inputItemType->Cast<TTupleExprType>();
        bool isOptional = false;
        const TStructExprType* structType = nullptr;

        if (!EnsureStructOrOptionalStructType(replicateInput->Pos(), *inputTupleType->GetItems()[0], isOptional, structType, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureStructOrOptionalStructType(replicateInput->Pos(), *inputTupleType->GetItems()[1], isOptional, structType, ctx)) {
            return TStatus::Error;
        }
    } else if (!EnsureStructType(replicateInput->Pos(), *inputItemType, ctx)) {
        return TStatus::Error;
    }
    const TTypeAnnotationNode* lambdaInputFlowType = ctx.MakeType<TFlowExprType>(inputItemType);
    TVector<const TTypeAnnotationNode*> outputFlowItems;
    for (ui32 i = 1; i < input->ChildrenSize(); ++i) {
        auto& lambda = input->ChildRef(i);
        if (!EnsureLambda(*lambda, ctx)) {
            return TStatus::Error;
        }
        if (!EnsureArgsCount(lambda->Head(), 1, ctx)) {
            return TStatus::Error;
        }
        if (!UpdateLambdaAllArgumentsTypes(lambda, {lambdaInputFlowType}, ctx)) {
            return TStatus::Error;
        }
        if (!lambda->GetTypeAnn()) {
            return TStatus::Repeat;
        }
        if (!EnsureFlowType(*lambda, ctx)) {
            return TStatus::Error;
        }
        const TTypeAnnotationNode* lambdaItemType = lambda->GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
        if (!EnsurePersistableType(lambda->Pos(), *lambdaItemType, ctx)) {
            return TStatus::Error;
        }
        outputFlowItems.push_back(lambdaItemType);
    }
    auto resultItemType = ctx.MakeType<TVariantExprType>(ctx.MakeType<TTupleExprType>(outputFlowItems));
    input->SetTypeAnn(ctx.MakeType<TFlowExprType>(resultItemType));
    return TStatus::Ok;
}

TStatus AnnotateDqJoin(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto resultRowType = GetDqJoinResultType<false>(input, false, ctx);
    if (!resultRowType) {
        return TStatus::Error;
    }

    input->SetTypeAnn(ctx.MakeType<TListExprType>(resultRowType));
    return TStatus::Ok;
}

TStatus AnnotateDqMapOrDictJoin(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto resultRowType = GetDqJoinResultType<true>(input, true, ctx);
    if (!resultRowType) {
        return TStatus::Error;
    }

    input->SetTypeAnn(ctx.MakeType<TFlowExprType>(resultRowType));
    return TStatus::Ok;
}

TStatus AnnotateDqCrossJoin(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto resultRowType = GetDqJoinResultType<false>(input, true, ctx);
    if (!resultRowType) {
        return TStatus::Error;
    }

    auto join = TDqPhyCrossJoin(input);
    if (join.JoinType().Value() != "Cross") {
        ctx.AddError(TIssue(ctx.GetPosition(join.Pos()), TStringBuilder()
            << "Unexpected join type: " << join.JoinType().Value()));
        return TStatus::Error;
    }

    if (!join.JoinKeys().Empty()) {
        ctx.AddError(TIssue(ctx.GetPosition(join.Pos()), TStringBuilder()
            << "Expected empty join keys for cross join"));
        return TStatus::Error;
    }

    input->SetTypeAnn(ctx.MakeType<TFlowExprType>(resultRowType));
    return TStatus::Ok;
}

TStatus AnnotateDqSource(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (!EnsureArgsCount(*input, 2, ctx)) {
        return TStatus::Error;
    }

    const TExprNode* dataSourceChild = input->Child(TDqSource::idx_DataSource);
    if (!EnsureDataSource(*dataSourceChild, ctx)) {
        return TStatus::Error;
    }

    const TExprNode* settingsChild = input->Child(TDqSource::idx_Settings);
    if (!EnsureCallable(*settingsChild, ctx)) {
        return TStatus::Error;
    }

    input->SetTypeAnn(settingsChild->GetTypeAnn());
    return TStatus::Ok;
}

TStatus AnnotateDqSink(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (!EnsureArgsCount(*input, 3, ctx)) {
        return TStatus::Error;
    }

    const TExprNode* dataSinkChild = input->Child(TDqSink::idx_DataSink);
    if (!EnsureDataSink(*dataSinkChild, ctx)) {
        return TStatus::Error;
    }

    const TExprNode* settingsChild = input->Child(TDqSink::idx_Settings);
    if (!EnsureCallable(*settingsChild, ctx)) {
        return TStatus::Error;
    }

    const TExprNode* indexChild = input->Child(TDqSink::idx_Index);
    if (!EnsureAtom(*indexChild, ctx)) {
        return TStatus::Error;
    }

    input->SetTypeAnn(settingsChild->GetTypeAnn());
    return TStatus::Ok;
}

TStatus AnnotateDqQuery(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (!EnsureArgsCount(*input, 2, ctx)) {
        return TStatus::Error;
    }

    TDqQuery query(input);
    input->SetTypeAnn(query.World().Ref().GetTypeAnn());
    return TStatus::Ok;
}

TStatus AnnotateDqTransform(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (!EnsureArgsCount(*input, 6U, ctx)) {
        return TStatus::Error;
    }

    const TExprNode* outputArg = input->Child(TDqTransform::idx_OutputType);
    if (!EnsureTypeWithStructType(*outputArg, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }
    const TTypeAnnotationNode* outputType = outputArg->GetTypeAnn()->Cast<TTypeExprType>()->GetType();

    const TExprNode* inputType = input->Child(TDqTransform::idx_InputType);
    if (!EnsureTypeWithStructType(*inputType, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.MakeType<TListExprType>(outputType));
    return TStatus::Ok;
}

TStatus AnnotateDqPrecompute(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 1, ctx)) {
        return TStatus::Error;
    }

    node->SetTypeAnn(node->Child(TDqPrecompute::idx_Input)->GetTypeAnn());
    return TStatus::Ok;
}

TStatus AnnotateDqPhyPrecompute(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 1, ctx)) {
        return TStatus::Error;
    }

    auto* cn = node->Child(TDqPhyPrecompute::idx_Connection);
    if (!TDqConnection::Match(cn)) {
        ctx.AddError(TIssue(ctx.GetPosition(cn->Pos()), TStringBuilder() << "Expected DqConnection, got " << cn->Content()));
        return TStatus::Error;
    }

    node->SetTypeAnn(cn->GetTypeAnn());
    return TStatus::Ok;
}

TStatus AnnotateDqPhyLength(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2, ctx)) {
        return TStatus::Error;
    }
    auto* input = node->Child(TDqPhyLength::idx_Input);
    auto* aggName = node->Child(TDqPhyLength::idx_Name);

    TVector<const TItemExprType*> aggTypes;
    if (!EnsureAtom(*aggName, ctx)) {
        return TStatus::Error;
    }

    TVector<const TItemExprType*> structItems;
    structItems.push_back(ctx.MakeType<TItemExprType>(aggName->Content(), ctx.MakeType<TDataExprType>(EDataSlot::Uint64)));

    node->SetTypeAnn(MakeSequenceType(input->GetTypeAnn()->GetKind(), *ctx.MakeType<TStructExprType>(structItems), ctx));
    return TStatus::Ok;
}

THolder<IGraphTransformer> CreateDqTypeAnnotationTransformer(TTypeAnnotationContext& typesCtx) {
    auto coreTransformer = CreateExtCallableTypeAnnotationTransformer(typesCtx);

    return CreateFunctorTransformer(
        [coreTransformer](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            output = input;
            TIssueScopeGuard issueScope(ctx.IssueManager, [&input, &ctx] {
                return MakeIntrusive<TIssue>(ctx.GetPosition(input->Pos()),
                    TStringBuilder() << "At function: " << input->Content());
            });

            if (TDqStage::Match(input.Get())) {
                return AnnotateDqStage(input, ctx);
            }

            if (TDqPhyStage::Match(input.Get())) {
                return AnnotateDqPhyStage(input, ctx);
            }

            if (TDqOutput::Match(input.Get())) {
                return AnnotateDqOutput(input, ctx);
            }

            if (TDqCnUnionAll::Match(input.Get())) {
                return AnnotateDqConnection(input, ctx);
            }

            if (TDqCnHashShuffle::Match(input.Get())) {
                return AnnotateDqCnHashShuffle(input, ctx);
            }

            if (TDqCnMap::Match(input.Get())) {
                return AnnotateDqConnection(input, ctx);
            }
            if (TDqCnStreamLookup::Match(input.Get())) {
                return AnnotateDqCnStreamLookup(input, ctx);
            }

            if (TDqCnBroadcast::Match(input.Get())) {
                return AnnotateDqConnection(input, ctx);
            }

            if (TDqCnResult::Match(input.Get())) {
                return AnnotateDqCnResult(input, ctx);
            }

            if (TDqCnValue::Match(input.Get())) {
                return AnnotateDqCnValue(input, ctx);
            }

            if (TDqCnMerge::Match(input.Get())) {
                return AnnotateDqCnMerge(input, ctx);
            }
            
            if (TDqReplicate::Match(input.Get())) {
                return AnnotateDqReplicate(input, ctx);
            }

            if (TDqJoin::Match(input.Get())) {
                return AnnotateDqJoin(input, ctx);
            }

            if (TDqPhyGraceJoin::Match(input.Get())) {
                return AnnotateDqMapOrDictJoin(input, ctx);
            }

            if (TDqPhyMapJoin::Match(input.Get())) {
                return AnnotateDqMapOrDictJoin(input, ctx);
            }

            if (TDqPhyJoinDict::Match(input.Get())) {
                return AnnotateDqMapOrDictJoin(input, ctx);
            }

            if (TDqPhyCrossJoin::Match(input.Get())) {
                return AnnotateDqCrossJoin(input, ctx);
            }

            if (TDqSource::Match(input.Get())) {
                return AnnotateDqSource(input, ctx);
            }

            if (TDqSink::Match(input.Get())) {
                return AnnotateDqSink(input, ctx);
            }

            if (TDqTransform::Match(input.Get())) {
                return AnnotateDqTransform(input, ctx);
            }

            if (TDqQuery::Match(input.Get())) {
                return AnnotateDqQuery(input, ctx);
            }

            if (TDqPrecompute::Match(input.Get())) {
                return AnnotateDqPrecompute(input, ctx);
            }

            if (TDqPhyPrecompute::Match(input.Get())) {
                return AnnotateDqPhyPrecompute(input, ctx);
            }

            if (TDqPhyLength::Match(input.Get())) {
                return AnnotateDqPhyLength(input, ctx);
            }

            return coreTransformer->Transform(input, output, ctx);
        });
}

bool IsTypeSupportedInMergeCn(EDataSlot type) {
    switch (type) {
        case EDataSlot::Bool:
        case EDataSlot::Int8:
        case EDataSlot::Uint8:
        case EDataSlot::Int16:
        case EDataSlot::Uint16:
        case EDataSlot::Int32:
        case EDataSlot::Uint32:
        case EDataSlot::Int64:
        case EDataSlot::Uint64:
        case EDataSlot::Double:
        case EDataSlot::Float:
        case EDataSlot::String:
        case EDataSlot::Utf8:
        case EDataSlot::Uuid:
        case EDataSlot::Date:
        case EDataSlot::Datetime:
        case EDataSlot::Timestamp:
        case EDataSlot::Interval:
        case EDataSlot::Decimal:
        case EDataSlot::DyNumber:
            // Supported
            return true;
        case EDataSlot::Yson:
        case EDataSlot::Json:
        case EDataSlot::TzDate:
        case EDataSlot::TzDatetime:
        case EDataSlot::TzTimestamp:
        case EDataSlot::JsonDocument:
        case EDataSlot::Date32:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
        case EDataSlot::Interval64:
        case EDataSlot::TzDate32:
        case EDataSlot::TzDatetime64:
        case EDataSlot::TzTimestamp64:
            return false;
    }
    return false;
}

bool IsTypeSupportedInMergeCn(const TDataExprType* dataType) {
   return IsTypeSupportedInMergeCn(dataType->GetSlot());
}

bool IsMergeConnectionApplicable(const TVector<const TTypeAnnotationNode*>& sortKeyTypes) {
    for (auto sortKeyType : sortKeyTypes) {
        if (sortKeyType->GetKind() == ETypeAnnotationKind::Optional) {
            sortKeyType = sortKeyType->Cast<TOptionalExprType>()->GetItemType();
        }
        if (sortKeyType->GetKind() != ETypeAnnotationKind::Data
            || !IsTypeSupportedInMergeCn(sortKeyType->Cast<TDataExprType>())) {
            return false;
        }
    }
    return true;
}

TDqStageSettings TDqStageSettings::Parse(const TDqStageBase& node) {
    TDqStageSettings settings{};

    for (const auto& tuple : node.Settings()) {
        if (const auto name = tuple.Name().Value(); name == IdSettingName) {
            YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
            settings.Id = tuple.Value().Cast<TCoAtom>().Value();
        } else if (name == LogicalIdSettingName) {
            YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
            settings.LogicalId = FromString<ui64>(tuple.Value().Cast<TCoAtom>().Value());
        } else if (name == PartitionModeSettingName) {
            YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
            settings.PartitionMode = FromString<EPartitionMode>(tuple.Value().Cast<TCoAtom>().Value());
        } else if (name == WideChannelsSettingName) {
            settings.WideChannels = true;
            settings.OutputNarrowType = tuple.Value().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
        } else if (name == BlockStatusSettingName) {
            YQL_ENSURE(tuple.Value().Maybe<TCoAtom>());
            settings.BlockStatus = FromString<EBlockStatus>(tuple.Value().Cast<TCoAtom>().Value());
        }
    }

    return settings;
}

bool TDqStageSettings::Validate(const TExprNode& stage, TExprContext& ctx) {
    auto& settings = *stage.Child(TDqStageBase::idx_Settings);
    if (!EnsureTuple(settings, ctx)) {
        return false;
    }

    for (auto& setting: settings.Children()) {
        if (!EnsureTupleMinSize(*setting, 1, ctx)) {
            return false;
        }

        if (!EnsureAtom(*setting->Child(0), ctx)) {
            return false;
        }

        TStringBuf name = setting->Head().Content();
        if (name == IdSettingName || name == LogicalIdSettingName || name == BlockStatusSettingName || name == PartitionModeSettingName) {
            if (setting->ChildrenSize() != 2) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Pos()), TStringBuilder() << "Setting " << name << " should contain single value"));
                return false;
            }
            auto value = setting->Child(1);
            if (!EnsureAtom(*value, ctx)) {
                return false;
            }

            if (name == LogicalIdSettingName && !TryFromString<ui64>(value->Content())) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Pos()), TStringBuilder() << "Setting " << name << " should contain ui64 value, but got: " << value->Content()));
                return false;
            }
            if (name == BlockStatusSettingName && !TryFromString<EBlockStatus>(value->Content())) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Pos()), TStringBuilder() << "Unsupported " << name << " value: " << value->Content()));
                return false;
            }
            if (name == PartitionModeSettingName && !TryFromString<EPartitionMode>(value->Content())) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Pos()), TStringBuilder() << "Unsupported " << name << " value: " << value->Content()));
                return false;
            }
        } else if (name == WideChannelsSettingName) {
            if (setting->ChildrenSize() != 2) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Pos()), TStringBuilder() << "Setting " << name << " should contain single value"));
                return false;
            }
            auto value = setting->Child(1);
            if (!EnsureType(*value, ctx)) {
                return false;
            }

            auto valueType  = value->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            if (!EnsureStructType(value->Pos(), *valueType, ctx)) {
                return false;
            }
        }
    }

    return true;
}

TDqStageSettings TDqStageSettings::New(const NNodes::TDqStageBase& node) {
    auto settings = Parse(node);

    if (!settings.Id) {
        settings.Id = CreateGuidAsString();
    }

    return settings;
}

TDqStageSettings TDqStageSettings::New() {
    TDqStageSettings s;
    s.Id = CreateGuidAsString();
    return s;
}

NNodes::TCoNameValueTupleList TDqStageSettings::BuildNode(TExprContext& ctx, TPositionHandle pos) const {
    TVector<TCoNameValueTuple> settings;
    auto logicalId = LogicalId;
    if (!logicalId) {
        logicalId = ctx.NextUniqueId;
    }

    settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
        .Name().Build(LogicalIdSettingName)
        .Value<TCoAtom>().Build(logicalId)
        .Done());

    if (Id) {
        settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(IdSettingName)
            .Value<TCoAtom>().Build(Id)
            .Done());
    }

    if (PartitionMode != EPartitionMode::Default) {
        settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(PartitionModeSettingName)
            .Value<TCoAtom>().Build(ToString(PartitionMode))
            .Done());
    }

    if (WideChannels) {
        YQL_ENSURE(OutputNarrowType);
        settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(WideChannelsSettingName)
            .Value(ExpandType(pos, *OutputNarrowType, ctx))
            .Done());
    }

    if (BlockStatus.Defined()) {
        settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(BlockStatusSettingName)
            .Value<TCoAtom>().Build(ToString(*BlockStatus))
            .Done());
    }

    return Build<TCoNameValueTupleList>(ctx, pos)
        .Add(settings)
        .Done();
}


TString PrintDqStageOnly(const TDqStageBase& stage, TExprContext& ctx) {
    if (stage.Inputs().Empty()) {
        return NCommon::ExprToPrettyString(ctx, stage.Ref());
    }

    TNodeOnNodeOwnedMap replaces;
    for (ui64 i = 0; i < stage.Inputs().Size(); ++i) {
        auto input = stage.Inputs().Item(i);
        auto param = Build<NNodes::TCoParameter>(ctx, input.Pos())
            .Name().Build(TStringBuilder() << "stage_input_" << i)
            .Type(ExpandType(input.Pos(), *input.Ref().GetTypeAnn(), ctx))
            .Done();

        replaces[input.Raw()] = param.Ptr();
    }

    auto newStage = ctx.ReplaceNodes(stage.Ptr(), replaces);
    return NCommon::ExprToPrettyString(ctx, *newStage);
}

} // namespace NYql::NDq
