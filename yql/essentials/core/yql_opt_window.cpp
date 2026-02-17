#include "yql_opt_window.h"
#include "yql_opt_utils.h"
#include "yql_expr_type_annotation.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_window_features.h>

#include <yql/essentials/core/sql_types/window_frame_bounds.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/core/sql_types/window_frames_collector_params.h>
#include <yql/essentials/core/yql_window_frames_collector_params_serializer.h>

#include <expected>

namespace NYql {

using namespace NNodes;

using NWindow::TCoreWinFrameCollectorBounds;
using NWindow::TNumberAndDirection;
using NWindow::EDirection;
using NWindow::TInputRow;
using NWindow::TInputRowWindowFrame;
using NWindow::TCoreWinFramesCollectorParams;

using THandle = TExprNodeCoreWinFrameCollectorBounds::THandle;

namespace {

constexpr TStringBuf SessionStartMemberName = "_yql_window_session_start";
constexpr TStringBuf SessionParamsMemberName = "_yql_window_session_params";

const TItemExprType* GetSortedColumnType(const TExprNode::TPtr& sortTraits, TExprContext& ctx) {
    YQL_ENSURE(sortTraits->IsCallable("SortTraits"));

    auto sortKeyLambda = sortTraits->ChildPtr(2);
    YQL_ENSURE(sortKeyLambda->IsLambda());
    const TTypeAnnotationNode* sortKeyType = sortKeyLambda->GetTypeAnn();
    YQL_ENSURE(sortKeyType);
    return ctx.MakeType<TItemExprType>(SortedColumnMemberName, sortKeyType);
}

bool ShouldAddSortedColumn(ESortOrder sortOrder) {
    return sortOrder != ESortOrder::Unimportant;
}

TExprNode::TPtr PushSortedColumnInsideStream(const TExprNode::TPtr& partitionsByKeys, TExprContext& ctx) {
    YQL_ENSURE(partitionsByKeys->IsCallable("PartitionsByKeys"));
    YQL_ENSURE(partitionsByKeys->ChildrenSize() == 5);

    auto pos = partitionsByKeys->Pos();
    auto stream = partitionsByKeys->ChildPtr(0);
    auto keySelector = partitionsByKeys->ChildPtr(1);
    auto sortDirection = partitionsByKeys->ChildPtr(2);
    auto sortKeySelector = partitionsByKeys->ChildPtr(3);
    auto handler = partitionsByKeys->ChildPtr(4);

    // If sortKeySelector is Void, nothing to do.
    if (sortKeySelector->IsCallable("Void")) {
        return partitionsByKeys;
    }

    // Add sorted column to the input stream using Map.
    auto rowArg = ctx.NewArgument(pos, "row");
    auto addMemberBody = ctx.Builder(pos)
        .Callable("AddMember")
            .Add(0, rowArg)
            .Atom(1, SortedColumnMemberName)
            .Apply(2, ctx.DeepCopyLambda(*sortKeySelector))
                .With(0, rowArg)
            .Seal()
        .Seal()
        .Build();

    auto addMemberLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {rowArg}), std::move(addMemberBody));

    auto streamWithSortedColumn = ctx.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, stream)
            .Add(1, addMemberLambda)
        .Seal()
        .Build();

#if 0  // TODO(atarasov5): Decide what to do with double lambda computation here and in non numeric range pipeline.
    // Create new sortKeySelector that just extracts the sorted column.
    auto newSortKeySelector = ctx.Builder(pos)
        .Lambda()
            .Param("item")
            .Callable("Member")
                .Arg(0, "item")
                .Atom(1, SortedColumnMemberName)
            .Seal()
        .Seal()
        .Build();
#else // #if 0
    auto newSortKeySelector = sortKeySelector;
#endif // #if 0

    // Build new PartitionsByKeys with modified arguments.
    return ctx.Builder(pos)
        .Callable("PartitionsByKeys")
            .Add(0, streamWithSortedColumn)
            .Add(1, keySelector)
            .Add(2, sortDirection)
            .Add(3, newSortKeySelector)
            .Add(4, handler)
        .Seal()
        .Build();
}

EFrameBoundsType FrameBoundsType(const TWindowFrameSettings::TRowFrame& settings) {
    auto first = settings.first;
    auto last = settings.second;

    if (CheckRowFrameIsAlwaysEmpty(settings)) {
        return EFrameBoundsType::EMPTY;
    }

    if (!first.Defined()) {
        if (!last.Defined()) {
            return EFrameBoundsType::FULL;
        }
        if (*last < 0) {
            return EFrameBoundsType::LAGGING;
        }

        return *last > 0 ? EFrameBoundsType::LEADING : EFrameBoundsType::CURRENT;
    }

    return EFrameBoundsType::GENERIC;
}

TExprNode::TPtr ReplaceLastLambdaArgWithUnsignedLiteral(const TExprNode& lambda, ui32 literal, TExprContext& ctx) {
    YQL_ENSURE(lambda.IsLambda());
    TExprNodeList args = lambda.ChildPtr(0)->ChildrenList();
    YQL_ENSURE(!args.empty());

    auto literalNode = ctx.Builder(lambda.Pos())
        .Callable("Uint32")
            .Atom(0, literal)
        .Seal()
        .Build();
    auto newBody = ctx.ReplaceNodes(lambda.ChildPtr(1), {{args.back().Get(), literalNode}});
    args.pop_back();
    return ctx.NewLambda(lambda.Pos(), ctx.NewArguments(lambda.Pos(), std::move(args)), std::move(newBody));
}

TExprNode::TPtr ReplaceFirstLambdaArgWithCastStruct(const TExprNode& lambda, const TTypeAnnotationNode& targetType, TExprContext& ctx) {
    YQL_ENSURE(lambda.IsLambda());
    YQL_ENSURE(targetType.GetKind() == ETypeAnnotationKind::Struct);
    TExprNodeList args = lambda.ChildPtr(0)->ChildrenList();
    YQL_ENSURE(!args.empty());

    auto newArg = ctx.NewArgument(lambda.Pos(), "row");

    auto cast = ctx.Builder(lambda.Pos())
        .Callable("MatchType")
            .Add(0, newArg)
            .Atom(1, "Optional", TNodeFlags::Default)
            .Lambda(2)
                .Param("row")
                .Callable("Map")
                    .Arg(0, "row")
                    .Lambda(1)
                        .Param("unwrapped")
                        .Callable("CastStruct")
                            .Arg(0, "unwrapped")
                            .Add(1, ExpandType(lambda.Pos(), targetType, ctx))
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(3)
                .Param("row")
                .Callable("CastStruct")
                    .Arg(0, "row")
                    .Add(1, ExpandType(lambda.Pos(), targetType, ctx))
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto newBody = ctx.ReplaceNodes(lambda.ChildPtr(1), {{args.front().Get(), cast}});
    args[0] = newArg;
    return ctx.NewLambda(lambda.Pos(), ctx.NewArguments(lambda.Pos(), std::move(args)), std::move(newBody));
}

TExprNode::TPtr AddOptionalIfNotAlreadyOptionalOrNull(const TExprNode::TPtr& lambda, TExprContext& ctx) {
    YQL_ENSURE(lambda->IsLambda());
    YQL_ENSURE(lambda->ChildPtr(0)->ChildrenSize() == 1);

    auto identity = MakeIdentityLambda(lambda->Pos(), ctx);
    return ctx.Builder(lambda->Pos())
        .Lambda()
            .Param("arg")
            .Callable("MatchType")
                .Apply(0, lambda)
                    .With(0, "arg")
                .Seal()
                .Atom(1, "Optional", TNodeFlags::Default)
                .Add(2, identity)
                .Atom(3, "Null", TNodeFlags::Default)
                .Add(4, identity)
                .Atom(5, "Pg", TNodeFlags::Default)
                .Add(6, identity)
                .Lambda(7)
                    .Param("result")
                    .Callable("Just")
                        .Arg(0, "result")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

struct TRawTrait {
    TPositionHandle Pos;

    // Init/Update/Default are set only for aggregations
    TExprNode::TPtr InitLambda;
    TExprNode::TPtr UpdateLambda;
    TExprNode::TPtr DefaultValue;

    TExprNode::TPtr CalculateLambda;
    TMaybe<i64> CalculateLambdaLead; // lead/lag for input to CalculateLambda;
    TVector<TExprNode::TPtr> Params; // NTile

    const TTypeAnnotationNode* OutputType = nullptr;

    TWindowFrameSettings FrameSettings;
};

struct TQueueParamsFromTraits {
    ui64 MaxDataOutpace = 0;
    ui64 MaxDataLag = 0;
    ui64 MaxUnboundedPrecedingLag = 0;
};

struct TCalcOverWindowTraits {
    TMap<TStringBuf, TRawTrait> RawTraits;
    TQueueParamsFromTraits QueueParams;
    const TTypeAnnotationNode* LagQueueItemType = nullptr;
};

TExprNode::TPtr ApplyDistinctForInitLambda(TExprNode::TPtr initLambda, const TStringBuf& distinctKey, const TTypeAnnotationNode& distinctKeyType, const TTypeAnnotationNode& distinctKeyOrigType, TExprContext& ctx) {
    bool hasParent = initLambda->Child(0)->ChildrenSize() == 2;
    bool distinctKeyIsStruct = distinctKeyOrigType.GetKind() == ETypeAnnotationKind::Struct;

    auto expandedDistinctKeyType = ExpandType(initLambda->Pos(), distinctKeyType, ctx);
    auto expandedDistinctKeyOrigType = ExpandType(initLambda->Pos(), distinctKeyOrigType, ctx);

    auto setCreateUdf = ctx.Builder(initLambda->Pos())
        .Callable("Udf")
            .Atom(0, "Set.Create")
            .Callable(1, "Void").Seal()
            .Callable(2, "TupleType")
                .Callable(0, "VoidType").Seal()
                .Callable(1, "VoidType").Seal()
                .Add(2, expandedDistinctKeyOrigType)
            .Seal()
        .Seal()
        .Build();

    auto setCreateLambda = ctx.Builder(initLambda->Pos())
        .Lambda()
            .Param("value")
            .Param("parent")
            .Callable("NamedApply")
                .Add(0, setCreateUdf)
                .List(1)
                    .Arg(0, "value")
                    .Callable(1, "Uint32")
                        .Atom(0, 0)
                    .Seal()
                .Seal()
                .Callable(2, "AsStruct").Seal()
                .Callable(3, "DependsOn")
                    .Arg(0, "parent")
                .Seal()
            .Seal()
        .Seal()
        .Build();

    initLambda = ctx.Builder(initLambda->Pos())
        .Lambda()
            .Param("value")
            .Param("parent")
            .List()
                // aggregation state
                .Apply(0, initLambda)
                    .Do([&](TExprNodeReplaceBuilder& builder) -> TExprNodeReplaceBuilder& {
                        if (distinctKeyIsStruct) {
                            return builder
                                .With(0)
                                    .Callable("CastStruct")
                                        .Arg(0, "value")
                                        .Add(1, expandedDistinctKeyType)
                                    .Seal()
                                .Done();
                        } else {
                            return builder.With(0, "value");
                        }
                    })
                    .Do([&](TExprNodeReplaceBuilder& builder) -> TExprNodeReplaceBuilder& {
                        return hasParent ? builder.With(1, "parent") : builder;
                    })
                .Seal()
                // distinct set state
                .Apply(1, setCreateLambda)
                    .With(0, "value")
                    .With(1, "parent")
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return ctx.Builder(initLambda->Pos())
        .Lambda()
            .Param("row")
            .Param("parent")
            .Apply(initLambda)
                .With(0)
                    .Callable("Member")
                        .Arg(0, "row")
                        .Atom(1, distinctKey)
                    .Seal()
                .Done()
                .With(1, "parent")
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr ApplyDistinctForUpdateLambda(TExprNode::TPtr updateLambda, const TStringBuf& distinctKey, const TTypeAnnotationNode& distinctKeyType, const TTypeAnnotationNode& distinctKeyOrigType, TExprContext& ctx) {
    bool hasParent = updateLambda->Child(0)->ChildrenSize() == 3;
    bool distinctKeyIsStruct = distinctKeyOrigType.GetKind() == ETypeAnnotationKind::Struct;

    auto expandedDistinctKeyType = ExpandType(updateLambda->Pos(), distinctKeyType, ctx);
    auto expandedDistinctKeyOrigType = ExpandType(updateLambda->Pos(), distinctKeyOrigType, ctx);

    auto setAddValueUdf = ctx.Builder(updateLambda->Pos())
        .Callable("Udf")
            .Atom(0, "Set.AddValue")
            .Callable(1, "Void").Seal()
            .Callable(2, "TupleType")
                .Callable(0, "VoidType").Seal()
                .Callable(1, "VoidType").Seal()
                .Add(2, expandedDistinctKeyOrigType)
            .Seal()
        .Seal()
        .Build();

    auto setWasChangedUdf = ctx.Builder(updateLambda->Pos())
        .Callable("Udf")
            .Atom(0, "Set.WasChanged")
            .Callable(1, "Void").Seal()
            .Callable(2, "TupleType")
                .Callable(0, "VoidType").Seal()
                .Callable(1, "VoidType").Seal()
                .Add(2, expandedDistinctKeyOrigType)
            .Seal()
        .Seal()
        .Build();

    auto setInsertLambda = ctx.Builder(updateLambda->Pos())
        .Lambda()
            .Param("set")
            .Param("value")
            .Param("parent")
            .Callable("NamedApply")
                .Add(0, setAddValueUdf)
                .List(1)
                    .Arg(0, "set")
                    .Arg(1, "value")
                .Seal()
                .Callable(2, "AsStruct").Seal()
                .Callable(3, "DependsOn")
                    .Arg(0, "parent")
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto setWasChangedLambda = ctx.Builder(updateLambda->Pos())
        .Lambda()
            .Param("set")
            .Param("parent")
            .Callable("NamedApply")
                .Add(0, setWasChangedUdf)
                .List(1)
                    .Arg(0, "set")
                .Seal()
                .Callable(2, "AsStruct").Seal()
                .Callable(3, "DependsOn")
                    .Arg(0, "parent")
                .Seal()
            .Seal()
        .Seal()
        .Build();

    updateLambda = ctx.Builder(updateLambda->Pos())
        .Lambda()
            .Param("value")
            .Param("state")
            .Param("parent")
            .Callable("If")
                // condition
                .Apply(0, setWasChangedLambda)
                    .With(0)
                        .Apply(setInsertLambda)
                            .With(0)
                                .Callable("Nth")
                                    .Arg(0, "state")
                                    .Atom(1, 1)
                                .Seal()
                            .Done()
                            .With(1, "value")
                            .With(2, "parent")
                        .Seal()
                    .Done()
                    .With(1, "parent")
                .Seal()
                // new state
                .List(1)
                    // aggregation state
                    .Apply(0, updateLambda)
                        .Do([&](TExprNodeReplaceBuilder& builder) -> TExprNodeReplaceBuilder& {
                            if (distinctKeyIsStruct) {
                                return builder
                                    .With(0)
                                        .Callable("CastStruct")
                                            .Arg(0, "value")
                                            .Add(1, expandedDistinctKeyType)
                                        .Seal()
                                    .Done();
                            } else {
                                return builder.With(0, "value");
                            }
                        })
                        .With(1)
                            .Callable("Nth")
                                .Arg(0, "state")
                                .Atom(1, 0)
                            .Seal()
                        .Done()
                        .Do([&](TExprNodeReplaceBuilder& builder) -> TExprNodeReplaceBuilder& {
                            return hasParent ? builder.With(2, "parent") : builder;
                        })
                    .Seal()
                    // distinct set state
                    .Apply(1, setInsertLambda)
                        .With(0)
                            .Callable("Nth")
                                .Arg(0, "state")
                                .Atom(1, 1)
                            .Seal()
                        .Done()
                        .With(1, "value")
                        .With(2, "parent")
                    .Seal()
                .Seal()
                // old state
                .Arg(2, "state")
            .Seal()
        .Seal()
        .Build();

    return ctx.Builder(updateLambda->Pos())
        .Lambda()
            .Param("row")
            .Param("state")
            .Param("parent")
            .Apply(updateLambda)
                .With(0)
                    .Callable("Member")
                        .Arg(0, "row")
                        .Atom(1, distinctKey)
                    .Seal()
                .Done()
                .With(1, "state")
                .With(2, "parent")
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr ApplyDistinctForCalculateLambda(TExprNode::TPtr calculateLambda, TExprContext& ctx) {
    return ctx.Builder(calculateLambda->Pos())
        .Lambda()
            .Param("state")
            .Apply(calculateLambda)
                .With(0)
                    .Callable("Nth")
                        .Arg(0, "state")
                        .Atom(1, 0)
                    .Seal()
                .Done()
            .Seal()
        .Seal()
        .Build();
}

TInputRow FromSettingsNumbers(i64 number) {
    if (number >= 0) {
        return TInputRow{static_cast<ui64>(number), EDirection::Following};
    } else {
        return TInputRow{static_cast<ui64>(-number), EDirection::Preceding};
    }
}

TInputRow FromSettingsNumbers(TMaybe<i32> number, EDirection directionIfInf) {
    if (!number) {
        return TInputRow::Inf(directionIfInf);
    }
    return FromSettingsNumbers(*number);
}

TCalcOverWindowTraits ExtractCalcOverWindowTraits(const TExprNode::TPtr& frames, const TStructExprType& rowType, TExprContext& ctx) {
    TCalcOverWindowTraits result;

    TVector<const TItemExprType*> lagQueueStructItems;
    for (auto& winOn : frames->ChildrenList()) {
        TWindowFrameSettings frameSettings = TWindowFrameSettings::Parse(*winOn, ctx);

        ui64 frameOutpace = 0;
        ui64 frameLag = 0;

        const EFrameType ft = frameSettings.GetFrameType();
        if (ft == EFrameType::FrameByRows) {
            const auto frameFirst = frameSettings.GetRowFrame().first;
            const auto frameLast = frameSettings.GetRowFrame().second;

            if (!frameSettings.IsAlwaysEmpty()) {
                if (!frameLast.Defined() || *frameLast > 0) {
                    frameOutpace = frameLast.Defined() ? ui64(*frameLast) : Max<ui64>();
                }

                if (frameFirst.Defined() && *frameFirst < 0) {
                    frameLag = ui64(0 - *frameFirst);
                }
            }
        }
        const auto& winOnChildren = winOn->ChildrenList();
        YQL_ENSURE(winOnChildren.size() > 1);
        for (size_t i = 1; i < winOnChildren.size(); ++i) {
            auto item = winOnChildren[i];
            YQL_ENSURE(item->IsList());

            auto nameNode = item->Child(0);
            YQL_ENSURE(nameNode->IsAtom());

            TStringBuf name = nameNode->Content();

            YQL_ENSURE(!result.RawTraits.contains(name));

            auto traits = item->Child(1);
            result.RawTraits.insert({name, TRawTrait{.FrameSettings = frameSettings}});
            auto& rawTraits = result.RawTraits.find(name)->second;
            rawTraits.Pos = traits->Pos();

            YQL_ENSURE(traits->IsCallable({"WindowTraits","CumeDist"}) || ft == EFrameType::FrameByRows, "Non-canonical frame for window functions");
            if (traits->IsCallable("WindowTraits")) {
                result.QueueParams.MaxDataOutpace = Max(result.QueueParams.MaxDataOutpace, frameOutpace);
                result.QueueParams.MaxDataLag = Max(result.QueueParams.MaxDataLag, frameLag);

                auto initLambda = traits->ChildPtr(1);
                auto updateLambda = traits->ChildPtr(2);
                auto calculateLambda = traits->ChildPtr(4);

                rawTraits.OutputType = calculateLambda->GetTypeAnn();
                YQL_ENSURE(rawTraits.OutputType);

                auto lambdaInputType = traits->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();

                if (item->ChildrenSize() == 3) {
                    auto distinctKey = item->Child(2)->Content();

                    auto distinctKeyOrigType = rowType.FindItemType(distinctKey);
                    YQL_ENSURE(distinctKeyOrigType);

                    initLambda = ApplyDistinctForInitLambda(initLambda, distinctKey, *lambdaInputType, *distinctKeyOrigType, ctx);
                    updateLambda = ApplyDistinctForUpdateLambda(updateLambda, distinctKey, *lambdaInputType, *distinctKeyOrigType, ctx);
                    calculateLambda = ApplyDistinctForCalculateLambda(calculateLambda, ctx);
                } else {
                    initLambda = ReplaceFirstLambdaArgWithCastStruct(*initLambda, *lambdaInputType, ctx);
                    updateLambda = ReplaceFirstLambdaArgWithCastStruct(*updateLambda, *lambdaInputType, ctx);
                }

                if (initLambda->Child(0)->ChildrenSize() == 2) {
                    initLambda = ReplaceLastLambdaArgWithUnsignedLiteral(*initLambda, i, ctx);
                }

                if (updateLambda->Child(0)->ChildrenSize() == 3) {
                    updateLambda = ReplaceLastLambdaArgWithUnsignedLiteral(*updateLambda, i, ctx);
                }

                rawTraits.InitLambda = initLambda;
                rawTraits.UpdateLambda = updateLambda;
                rawTraits.CalculateLambda = calculateLambda;
                rawTraits.DefaultValue = traits->ChildPtr(5);

                if (ft == EFrameType::FrameByRows) {
                    const EFrameBoundsType frameType = FrameBoundsType(frameSettings.GetRowFrame());
                    const auto frameLast = frameSettings.GetRowFrame().second;
                    if (frameType == EFrameBoundsType::LAGGING) {
                        result.QueueParams.MaxUnboundedPrecedingLag = Max(result.QueueParams.MaxUnboundedPrecedingLag, ui64(abs(*frameLast)));
                        lagQueueStructItems.push_back(ctx.MakeType<TItemExprType>(name, rawTraits.OutputType));
                    }
                }
            } else if (traits->IsCallable({"Lead", "Lag"})) {
                i64 lead = 1;
                if (traits->ChildrenSize() == 3) {
                    YQL_ENSURE(traits->Child(2)->IsCallable("Int64"));
                    lead = FromString<i64>(traits->Child(2)->Child(0)->Content());
                }

                if (traits->IsCallable("Lag")) {
                    lead = -lead;
                }

                if (lead < 0) {
                    result.QueueParams.MaxDataLag = Max(result.QueueParams.MaxDataLag, ui64(abs(lead)));
                } else {
                    result.QueueParams.MaxDataOutpace = Max<ui64>(result.QueueParams.MaxDataOutpace, lead);
                }

                auto lambdaInputType =
                    traits->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TListExprType>()->GetItemType();

                rawTraits.CalculateLambda = ReplaceFirstLambdaArgWithCastStruct(*traits->Child(1), *lambdaInputType, ctx);
                rawTraits.CalculateLambdaLead = lead;
                rawTraits.OutputType = traits->Child(1)->GetTypeAnn();
                YQL_ENSURE(rawTraits.OutputType);
            } else if (traits->IsCallable({"Rank", "DenseRank", "PercentRank"})) {
                rawTraits.OutputType = traits->Child(1)->GetTypeAnn();
                auto lambdaInputType =
                    traits->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TListExprType>()->GetItemType();
                auto lambda = ReplaceFirstLambdaArgWithCastStruct(*traits->Child(1), *lambdaInputType, ctx);
                rawTraits.CalculateLambda = ctx.ChangeChild(*traits, 1, std::move(lambda));
            } else {
                YQL_ENSURE(traits->IsCallable({"RowNumber","CumeDist","NTile"}));
                rawTraits.CalculateLambda = traits;
                rawTraits.OutputType = traits->GetTypeAnn();
                for (ui32 i = 1; i < traits->ChildrenSize(); ++i) {
                    rawTraits.Params.push_back(traits->ChildPtr(i));
                }
            }
        }
    }

    result.LagQueueItemType = ctx.MakeType<TStructExprType>(lagQueueStructItems);

    return result;
}

TExprNode::TPtr BuildUint64(TPositionHandle pos, ui64 value, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("Uint64")
            .Atom(0, ToString(value))
        .Seal()
        .Build();
}

TExprNode::TPtr BuildDouble(TPositionHandle pos, double value, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("Double")
            .Atom(0, ToString(value))
        .Seal()
        .Build();
}

TExprNode::TPtr BuildQueuePeek(TPositionHandle pos,
                               const TExprNode::TPtr& queue,
                               ui64 index,
                               const TExprNode::TPtr& dependsOn,
                               TExprContext& ctx)
{
    return ctx.Builder(pos)
        .Callable("QueuePeek")
            .Add(0, queue)
            .Add(1, BuildUint64(pos, index, ctx))
            .Callable(2, "DependsOn")
                .Add(0, dependsOn)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildQueueRange(TPositionHandle pos, const TExprNode::TPtr& queue, ui64 begin, ui64 end,
                                const TExprNode::TPtr& dependsOn, TExprContext& ctx)
{
    return ctx.Builder(pos)
        .Callable("FlatMap")
            .Callable(0, "QueueRange")
                .Add(0, queue)
                .Add(1, BuildUint64(pos, begin, ctx))
                .Add(2, BuildUint64(pos, end, ctx))
                .Callable(3, "DependsOn")
                    .Add(0, dependsOn)
                .Seal()
            .Seal()
            .Lambda(1)
                .Param("item")
                .Arg("item")
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildWinFrame(TPositionHandle pos,
                                 const TExprNode::TPtr& queue,
                                 THandle handle,
                                 const TExprNode::TPtr& dependsOn,
                                 TExprContext& ctx,
                                 bool isSingleElement)
{
    auto queueData = ctx.Builder(pos)
        .Callable("WinFrame")
            .Add(0, queue)
            .Add(1, BuildUint64(pos, handle.Index(), ctx))
            .Add(2, MakeBool(pos, handle.IsIncremental(), ctx))
            .Add(3, MakeBool(pos, handle.IsRange(), ctx))
            .Add(4, MakeBool(pos, isSingleElement, ctx))
            .Callable(5, "DependsOn")
                .Add(0, dependsOn)
            .Seal()
        .Seal()
        .Build();

    if (!isSingleElement) {
        return ctx.Builder(pos)
                    .Callable("OrderedMap")
                        .Add(0, queueData)
                        .Lambda(1)
                            .Param("item")
                            .Arg("item")
                        .Seal()
                    .Seal()
                    .Build();
    }

    return queueData;
}

struct TWinFramesCollectorBuildResult {
    TExprNodePtr Queue;
    TExprNodePtr WinFramesCollector;
};

TWinFramesCollectorBuildResult BuildWinFramesCollector(TPositionHandle pos,
                                                       TExprNode::TPtr stream,
                                                       TExprNode::TPtr itemType,
                                                       const TExprNodeCoreWinFrameCollectorParams& params,
                                                       TExprNode::TPtr dependsOn,
                                                       TExprContext& ctx) {
    auto unboundedQueue = ctx.Builder(pos)
        .Callable("QueueCreate")
            .Add(0, itemType)
            .Add(1, ctx.NewCallable(pos, "Void", {}))
            .Add(2, BuildUint64(pos, 0, ctx))
            .Callable(3, "DependsOn")
                .Add(0, dependsOn)
            .Seal()
        .Seal()
        .Build();

    auto winFramesCollector = ctx.Builder(pos)
        .Callable("WinFramesCollector")
            .Add(0, stream)
            .Add(1, unboundedQueue)
            .Add(2, SerializeWindowAggregatorParamsToExpr(params, pos, ctx))
        .Seal()
        .Build();

    return {.Queue = std::move(unboundedQueue), .WinFramesCollector = std::move(winFramesCollector)};
}

TWinFramesCollectorBuildResult BuildWinFramesCollector(TPositionHandle pos,
                                                       TExprNode::TPtr stream,
                                                       const TTypeAnnotationNode& itemType,
                                                       const TExprNodeCoreWinFrameCollectorParams& params,
                                                       TExprNode::TPtr dependsOn,
                                                       TExprContext& ctx) {
    return BuildWinFramesCollector(pos, stream, ExpandType(pos, itemType, ctx), params, dependsOn, ctx);
}

TExprNode::TPtr BuildQueue(TPositionHandle pos,
                           const TExprNode::TPtr& itemType,
                           ui64 queueSize,
                           ui64 initSize,
                           const TExprNode::TPtr& dependsOn,
                           TExprContext& ctx) {
    TExprNode::TPtr size;
    if (queueSize == Max<ui64>()) {
        size = ctx.NewCallable(pos, "Void", {});
    } else {
        size = BuildUint64(pos, queueSize, ctx);
    }

    return ctx.Builder(pos)
        .Callable("QueueCreate")
            .Add(0, itemType)
            .Add(1, size)
            .Add(2, BuildUint64(pos, initSize, ctx))
            .Callable(3, "DependsOn")
                .Add(0, dependsOn)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildQueue(TPositionHandle pos, const TTypeAnnotationNode& itemType, ui64 queueSize, ui64 initSize,
    const TExprNode::TPtr& dependsOn, TExprContext& ctx)
{
    return BuildQueue(pos, ExpandType(pos, itemType, ctx), queueSize, initSize, dependsOn, ctx);
}

TExprNode::TPtr CoalesceQueueOutput(TPositionHandle pos, const TExprNode::TPtr& output, bool rawOutputIsOptional,
    const TExprNode::TPtr& defaultValue, TExprContext& ctx)
{
    // Output has type Optional<RawOutputType>.
    if (!rawOutputIsOptional) {
        return ctx.Builder(pos)
            .Callable("Coalesce")
                .Add(0, output)
                .Add(1, defaultValue)
            .Seal()
            .Build();
    }

    return ctx.Builder(pos)
        .Callable("IfPresent")
            .Add(0, output)
            .Lambda(1)
                .Param("item")
                .Callable("Coalesce")
                    .Arg(0, "item")
                    .Add(1, defaultValue)
                .Seal()
            .Seal()
            .Add(2, defaultValue)
        .Seal()
        .Build();
}

TExprNode::TPtr WrapWithWinContext(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (HasContextFuncs(*input)) {
        return ctx.Builder(input->Pos())
            .Callable("WithContext")
                .Add(0, input)
                .Atom(1, "WinAgg", TNodeFlags::Default)
            .Seal()
            .Build();
    }
    return input;
}

TExprNode::TPtr BuildInitLambdaForChain1Map(TPositionHandle pos, const TExprNode::TPtr& initStateLambda,
    const TExprNode::TPtr& calculateLambda, TExprContext& ctx)
{
    return ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .List()
                .Do([&](TExprNodeBuilder& parent)->TExprNodeBuilder& {
                    if (calculateLambda->Head().ChildrenSize() == 1) {
                        parent.Apply(0, calculateLambda)
                            .With(0)
                                .Apply(initStateLambda)
                                    .With(0, "row")
                                .Seal()
                            .Done()
                        .Seal();
                    } else {
                        parent.Apply(0, calculateLambda)
                            .With(0)
                                .Apply(initStateLambda)
                                    .With(0, "row")
                                .Seal()
                            .Done()
                            .With(1, "row")
                        .Seal();
                    }

                    return parent;
                })
                .Apply(1, initStateLambda)
                    .With(0, "row")
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr Unwrap(TPositionHandle pos, TExprNode::TPtr output, TExprNode::TPtr calculate, TExprNode::TPtr originalInit, TExprNode::TPtr rowArg, TExprContext& ctx) {
    // Output is always non-empty optional in this case
    // we do IfPresent with some fake output value to remove optional
    // this will have exactly the same result as Unwrap(output).
    return ctx.Builder(pos)
        .Callable("IfPresent")
            .Add(0, output)
            .Lambda(1)
                .Param("unwrapped")
                .Arg("unwrapped")
            .Seal()
            .Apply(2, calculate)
                .With(0)
                    .Apply(originalInit)
                        .With(0, rowArg)
                    .Seal()
                .Done()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildUpdateLambdaForChain1Map(TPositionHandle pos, const TExprNode::TPtr& updateStateLambda,
    const TExprNode::TPtr& calculateLambda, TExprContext& ctx)
{
    return ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .Param("state")
            .List()
                .Do([&](TExprNodeBuilder& parent)->TExprNodeBuilder& {
                    if (calculateLambda->Head().ChildrenSize() == 1) {
                        parent.Apply(0, calculateLambda)
                            .With(0)
                                .Apply(updateStateLambda)
                                    .With(0, "row")
                                    .With(1, "state")
                                .Seal()
                            .Done()
                        .Seal();
                    } else {
                        parent.Apply(0, calculateLambda)
                            .With(0)
                                .Apply(updateStateLambda)
                                    .With(0, "row")
                                    .With(1, "state")
                                .Seal()
                            .Done()
                            .With(1, "row")
                        .Seal();
                    }

                    return parent;
                })
                .Apply(1, updateStateLambda)
                    .With(0, "row")
                    .With(1, "state")
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr ExtractShiftNonEmpty(TPositionHandle pos,
                                     const TExprNode::TPtr& queue,
                                     const TExprNode::TPtr& dependsOn,
                                     const TStringBuf name,
                                     THandle handle,
                                     TExprContext& ctx) {
    return ctx.Builder(pos)
            .Callable("Member")
                .Callable(0, "Unwrap")
                    .Add(0, ::NYql::BuildWinFrame(pos, queue, handle, dependsOn, ctx, /*isSingleElement=*/true))
                .Seal()
                .Atom(1, name)
            .Seal()
            .Build();
}
class TChain1MapTraits : public TThrRefBase, public TNonCopyable {
public:
    using TPtr = TIntrusivePtr<TChain1MapTraits>;

    TChain1MapTraits(TStringBuf name, TPositionHandle pos)
      : Name_(name)
      , Pos_(pos)
    {
    }

    TStringBuf GetName() const {
        return Name_;
    }

    TPositionHandle GetPos() const {
        return Pos_;
    }

    // Lambda(row) -> AsTuple(output, state)
    virtual TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const = 0;

    // Lambda(row, state) -> AsTuple(output, state)
    virtual TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const = 0;

    virtual TExprNode::TPtr ExtractLaggingOutput(const TExprNode::TPtr& lagQueue,
                                                 const TExprNode::TPtr& dependsOn,
                                                 TExprContext& ctx) const {
        Y_UNUSED(lagQueue);
        Y_UNUSED(dependsOn);
        Y_UNUSED(ctx);
        return {};
    }

    virtual TExprNode::TPtr ExtractShiftedOutput(const TExprNode::TPtr& lagQueue,
                                                 const TExprNode::TPtr& dependsOn,
                                                 TExprContext& ctx) const {
        Y_UNUSED(lagQueue);
        Y_UNUSED(dependsOn);
        Y_UNUSED(ctx);
        return {};
    }


    ~TChain1MapTraits() override = default;
private:
    const TStringBuf Name_;
    const TPositionHandle Pos_;
};

class TChain1MapTraitsLagLead : public TChain1MapTraits {
public:
    using TQueueParam = std::variant<ui64, THandle>;

    TChain1MapTraitsLagLead(TStringBuf name, const TRawTrait& raw, TMaybe<TQueueParam> queueOffset)
        : TChain1MapTraits(name, raw.Pos)
        , QueueOffset_(queueOffset)
        , LeadLagLambda_(raw.CalculateLambda)
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .List()
                    .Apply(0, CalculateOutputLambda(dataQueue, ctx))
                        .With(0, "row")
                    .Seal()
                    .Callable(1, "Void")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .List()
                    .Apply(0, CalculateOutputLambda(dataQueue, ctx))
                        .With(0, "row")
                    .Seal()
                    .Arg(1, "state")
                .Seal()
            .Seal()
            .Build();
    }

private:
    TExprNode::TPtr CalculateOutputLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const {
        if (!QueueOffset_.Defined()) {
            return AddOptionalIfNotAlreadyOptionalOrNull(LeadLagLambda_, ctx);
        }

        YQL_ENSURE(dataQueue);

        auto rowArg = ctx.NewArgument(GetPos(), "row");

        auto body = ctx.Builder(GetPos())
            .Callable("IfPresent")
                .Add(0, GetSingleElement(dataQueue, rowArg, ctx))
                .Add(1, AddOptionalIfNotAlreadyOptionalOrNull(LeadLagLambda_, ctx))
                .Callable(2, "Null")
                .Seal()
            .Seal()
            .Build();

        return ctx.NewLambda(GetPos(), ctx.NewArguments(GetPos(), {rowArg}), std::move(body));
    }

    TExprNode::TPtr GetSingleElement(TExprNode::TPtr dataQueue, TExprNode::TPtr rowArg, TExprContext& ctx) const {
        if (std::holds_alternative<THandle>(*QueueOffset_)) {
            return ::NYql::BuildWinFrame(GetPos(), dataQueue, std::get<THandle>(*QueueOffset_), rowArg, ctx, /*isSingleElement=*/true);
        } else {
            return BuildQueuePeek(GetPos(), dataQueue, std::get<ui64>(*QueueOffset_), rowArg, ctx);
        }
    }

    const TMaybe<TQueueParam> QueueOffset_;
    const TExprNode::TPtr LeadLagLambda_;
};

class TChain1MapTraitsRowNumber : public TChain1MapTraits {
public:
    TChain1MapTraitsRowNumber(TStringBuf name, const TRawTrait& raw)
        : TChain1MapTraits(name, raw.Pos)
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .List()
                    .Add(0, BuildUint64(GetPos(), 1, ctx))
                    .Add(1, BuildUint64(GetPos(), 1, ctx))
                .Seal()
            .Seal()
            .Build();
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .List()
                    .Callable(0, "Inc")
                        .Arg(0, "state")
                    .Seal()
                    .Callable(1, "Inc")
                        .Arg(0, "state")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }
};

class TChain1MapTraitsCumeDist : public TChain1MapTraits {
public:
    TChain1MapTraitsCumeDist(TStringBuf name, const TRawTrait& raw, TMaybe<THandle> handle, const TString& partitionRowsColumn)
        : TChain1MapTraits(name, raw.Pos)
        , PartitionRowsColumn_(partitionRowsColumn)
        , Handle_(handle)
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .List()
                    .Callable(0, "/")
                        .Add(0, BuildDouble(GetPos(), 1.0, ctx))
                        .Callable(1, "Member")
                            .Arg(0, "row")
                            .Atom(1, PartitionRowsColumn_)
                        .Seal()
                    .Seal()
                    .Add(1, BuildUint64(GetPos(), 1, ctx))
                .Seal()
            .Seal()
            .Build();
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .List()
                    .Callable(0, "/")
                        .Callable(0, "SafeCast")
                            .Callable(0, "Inc")
                                .Arg(0, "state")
                            .Seal()
                            .Atom(1, "Double")
                        .Seal()
                        .Callable(1, "Member")
                            .Arg(0, "row")
                            .Atom(1, PartitionRowsColumn_)
                        .Seal()
                    .Seal()
                    .Callable(1, "Inc")
                        .Arg(0, "state")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    TExprNode::TPtr ExtractShiftedOutput(const TExprNode::TPtr& queue,
                                         const TExprNode::TPtr& dependsOn,
                                         TExprContext& ctx) const override {
        if (!Handle_.Defined()) {
            return {};
        }
        return ExtractShiftNonEmpty(GetPos(), queue, dependsOn, GetName(), *Handle_, ctx);
    }

private:
    const TString PartitionRowsColumn_;
    TMaybe<THandle> Handle_;
};

class TChain1MapTraitsNTile : public TChain1MapTraits {
public:
    TChain1MapTraitsNTile(TStringBuf name, const TRawTrait& raw, const TString& partitionRowsColumn)
        : TChain1MapTraits(name, raw.Pos)
        , PartitionRowsColumn_(partitionRowsColumn)
    {
        YQL_ENSURE(raw.Params.size() == 1);
        Param_ = raw.Params[0];
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .List()
                    .Add(0, BuildUint64(GetPos(), 1, ctx))
                    .Add(1, BuildUint64(GetPos(), 1, ctx))
                .Seal()
            .Seal()
            .Build();
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .List()
                    .Callable(0, "Inc")
                        .Callable(0, "Unwrap")
                            .Callable(0, "/")
                                .Callable(0, "*")
                                    .Callable(0, "SafeCast")
                                        .Add(0, Param_)
                                        .Atom(1, "Uint64")
                                    .Seal()
                                    .Arg(1, "state")
                                .Seal()
                                .Callable(1, "Member")
                                    .Arg(0, "row")
                                    .Atom(1, PartitionRowsColumn_)
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                    .Callable(1, "Inc")
                        .Arg(0, "state")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

private:
    const TString PartitionRowsColumn_;
    TExprNode::TPtr Param_;
};

class TChain1MapTraitsRankBase : public TChain1MapTraits {
public:
    TChain1MapTraitsRankBase(TStringBuf name, const TRawTrait& raw)
        : TChain1MapTraits(name, raw.Pos)
        , ExtractForCompareLambda_(raw.CalculateLambda->ChildPtr(1))
        , Ansi_(HasSetting(*raw.CalculateLambda->Child(2), "ansi"))
        , KeyType_(raw.OutputType)
    {
    }

    virtual TExprNode::TPtr BuildCalculateLambda(TExprContext& ctx) const {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("state")
                .Callable("Nth")
                    .Arg(0, "state")
                    .Atom(1, "0")
                .Seal()
            .Seal()
            .Build();
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const final {
        Y_UNUSED(dataQueue);


        auto initKeyLambda = BuildRawInitLambda(ctx);
        if (!Ansi_ && KeyType_->GetKind() == ETypeAnnotationKind::Optional) {
            auto stateType = GetStateType(KeyType_->Cast<TOptionalExprType>()->GetItemType(), ctx);
            initKeyLambda = BuildOptKeyInitLambda(initKeyLambda, stateType, ctx);
        }

        auto initRowLambda = ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Apply(initKeyLambda)
                    .With(0)
                        .Apply(ExtractForCompareLambda_)
                            .With(0, "row")
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
            .Build();

        return BuildInitLambdaForChain1Map(GetPos(), initRowLambda, BuildCalculateLambda(ctx), ctx);
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const final {
        Y_UNUSED(dataQueue);

        bool useAggrEquals = Ansi_;
        auto updateKeyLambda = BuildRawUpdateLambda(useAggrEquals, ctx);

        if (!Ansi_ && KeyType_->GetKind() == ETypeAnnotationKind::Optional) {
            auto stateType = GetStateType(KeyType_->Cast<TOptionalExprType>()->GetItemType(), ctx);
            updateKeyLambda = ctx.Builder(GetPos())
                .Lambda()
                    .Param("key")
                    .Param("state")
                    .Callable("IfPresent")
                        .Arg(0, "state")
                        .Lambda(1)
                            .Param("unwrappedState")
                            .Callable("IfPresent")
                                .Arg(0, "key")
                                .Lambda(1)
                                    .Param("unwrappedKey")
                                    .Callable("Just")
                                        .Apply(0, updateKeyLambda)
                                            .With(0, "unwrappedKey")
                                            .With(1, "unwrappedState")
                                        .Seal()
                                    .Seal()
                                .Seal()
                                .Callable(2, "Just")
                                    .Arg(0, "unwrappedState")
                                .Seal()
                            .Seal()
                        .Seal()
                        .Apply(2, BuildOptKeyInitLambda(BuildRawInitLambda(ctx), stateType, ctx))
                            .With(0, "key")
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        auto updateRowLambda = ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .Apply(updateKeyLambda)
                    .With(0)
                        .Apply(ExtractForCompareLambda_)
                            .With(0, "row")
                        .Seal()
                    .Done()
                    .With(1, "state")
                .Seal()
            .Seal()
            .Build();

        return BuildUpdateLambdaForChain1Map(GetPos(), updateRowLambda, BuildCalculateLambda(ctx), ctx);
    }

    virtual TExprNode::TPtr BuildRawInitLambda(TExprContext& ctx) const = 0;
    virtual TExprNode::TPtr BuildRawUpdateLambda(bool useAggrEquals, TExprContext& ctx) const = 0;
    virtual const TTypeAnnotationNode* GetStateType(const TTypeAnnotationNode* keyType, TExprContext& ctx) const = 0;

private:
    TExprNode::TPtr BuildOptKeyInitLambda(const TExprNode::TPtr& rawInitKeyLambda,
        const TTypeAnnotationNode* stateType, TExprContext& ctx) const
    {
        auto optStateType = ctx.MakeType<TOptionalExprType>(stateType);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("key")
                .Callable("IfPresent")
                    .Arg(0, "key")
                    .Lambda(1)
                        .Param("unwrapped")
                        .Callable("Just")
                            .Apply(0, rawInitKeyLambda)
                                .With(0, "unwrapped")
                            .Seal()
                        .Seal()
                    .Seal()
                    .Callable(2, "Nothing")
                        .Add(0, ExpandType(GetPos(), *optStateType, ctx))
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    const TExprNode::TPtr ExtractForCompareLambda_;
    const bool Ansi_;
    const TTypeAnnotationNode* const KeyType_;
};

class TChain1MapTraitsRank : public TChain1MapTraitsRankBase {
public:
    TChain1MapTraitsRank(TStringBuf name, const TRawTrait& raw)
        : TChain1MapTraitsRankBase(name, raw)
    {
    }

    TExprNode::TPtr BuildRawInitLambda(TExprContext& ctx) const final {
        auto one = BuildUint64(GetPos(), 1, ctx);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("key")
                .List()
                    .Add(0, one)
                    .Add(1, one)
                    .Arg(2, "key")
                .Seal()
            .Seal()
            .Build();
    }

    TExprNode::TPtr BuildRawUpdateLambda(bool useAggrEquals, TExprContext& ctx) const final {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("key")
                .Param("state")
                .List()
                    .Callable(0, "If")
                        .Callable(0, useAggrEquals ? "AggrEquals" : "==")
                            .Arg(0, "key")
                            .Callable(1, "Nth")
                                .Arg(0, "state")
                                .Atom(1, "2")
                            .Seal()
                        .Seal()
                        .Callable(1, "Nth")
                            .Arg(0, "state")
                            .Atom(1, "0")
                        .Seal()
                        .Callable(2, "Inc")
                            .Callable(0, "Nth")
                                .Arg(0, "state")
                                .Atom(1, "1")
                            .Seal()
                        .Seal()
                    .Seal()
                    .Callable(1, "Inc")
                        .Callable(0, "Nth")
                            .Arg(0, "state")
                            .Atom(1, "1")
                        .Seal()
                    .Seal()
                    .Arg(2, "key")
                .Seal()
            .Seal()
            .Build();
    }

    const TTypeAnnotationNode* GetStateType(const TTypeAnnotationNode* keyType, TExprContext& ctx) const final {
        return ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            ctx.MakeType<TDataExprType>(EDataSlot::Uint64),
            ctx.MakeType<TDataExprType>(EDataSlot::Uint64),
            keyType
        });
    }
};

class TChain1MapTraitsPercentRank : public TChain1MapTraitsRank {
public:
    TChain1MapTraitsPercentRank(TStringBuf name, const TRawTrait& raw, const TString& partitionRowsColumn)
        : TChain1MapTraitsRank(name, raw)
        , PartitionRowsColumn_(partitionRowsColumn)
    {
    }

  TExprNode::TPtr BuildCalculateLambda(TExprContext& ctx) const override {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("state")
                .Param("row")
                .Callable("/")
                    .Callable(0, "SafeCast")
                        .Callable(0, "Dec")
                            .Callable(0, "Nth")
                                .Arg(0, "state")
                                .Atom(1, "0")
                            .Seal()
                        .Seal()
                        .Atom(1, "Double")
                    .Seal()
                    .Callable(1, "Dec")
                        .Callable(0, "Member")
                            .Arg(0, "row")
                            .Atom(1, PartitionRowsColumn_)
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

private:
    const TString PartitionRowsColumn_;
};

class TChain1MapTraitsDenseRank : public TChain1MapTraitsRankBase {
public:
    TChain1MapTraitsDenseRank(TStringBuf name, const TRawTrait& raw)
        : TChain1MapTraitsRankBase(name, raw)
    {
    }

    TExprNode::TPtr BuildRawInitLambda(TExprContext& ctx) const final {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("key")
                .List()
                    .Add(0, BuildUint64(GetPos(), 1, ctx))
                    .Arg(1, "key")
                .Seal()
            .Seal()
            .Build();
    }

    TExprNode::TPtr BuildRawUpdateLambda(bool useAggrEquals, TExprContext& ctx) const final {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("key")
                .Param("state")
                .List()
                    .Callable(0, "If")
                        .Callable(0, useAggrEquals ? "AggrEquals" : "==")
                            .Arg(0, "key")
                            .Callable(1, "Nth")
                                .Arg(0, "state")
                                .Atom(1, "1")
                            .Seal()
                        .Seal()
                        .Callable(1, "Nth")
                            .Arg(0, "state")
                            .Atom(1, "0")
                        .Seal()
                        .Callable(2, "Inc")
                            .Callable(0, "Nth")
                                .Arg(0, "state")
                                .Atom(1, "0")
                            .Seal()
                        .Seal()
                    .Seal()
                    .Arg(1, "key")
                .Seal()
            .Seal()
            .Build();
    }

    const TTypeAnnotationNode* GetStateType(const TTypeAnnotationNode* keyType, TExprContext& ctx) const final {
        return ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            ctx.MakeType<TDataExprType>(EDataSlot::Uint64),
            keyType
        });
    }
};

class TChain1MapTraitsStateBase : public TChain1MapTraits {
public:
    TChain1MapTraitsStateBase(TStringBuf name, const TRawTrait& raw)
        : TChain1MapTraits(name, raw.Pos)
        , FrameNeverEmpty_(raw.FrameSettings.IsNonEmpty())
        , InitLambda_(raw.InitLambda)
        , UpdateLambda_(raw.UpdateLambda)
        , CalculateLambda_(raw.CalculateLambda)
        , DefaultValue_(raw.DefaultValue)
    {
        YQL_ENSURE(InitLambda_);
        YQL_ENSURE(UpdateLambda_);
        YQL_ENSURE(CalculateLambda_);
        YQL_ENSURE(DefaultValue_);
    }

protected:
    TExprNode::TPtr GetInitLambda() const {
        return InitLambda_;
    }

    TExprNode::TPtr GetUpdateLambda() const {
        return UpdateLambda_;
    }

    TExprNode::TPtr GetCalculateLambda() const {
        return CalculateLambda_;
    }

    TExprNode::TPtr GetDefaultValue() const {
        return DefaultValue_;
    }

    const bool FrameNeverEmpty_;

private:
    const TExprNode::TPtr InitLambda_;
    const TExprNode::TPtr UpdateLambda_;
    const TExprNode::TPtr CalculateLambda_;
    const TExprNode::TPtr DefaultValue_;
};

class TChain1MapTraitsCurrentOrLagging : public TChain1MapTraitsStateBase {
public:
    TChain1MapTraitsCurrentOrLagging(TStringBuf name, const TRawTrait& raw, TMaybe<ui64> lagQueueIndex)
        : TChain1MapTraitsStateBase(name, raw)
        , LaggingQueueIndex_(lagQueueIndex)
        , OutputIsOptional_(raw.OutputType->IsOptionalOrNull())
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return BuildInitLambdaForChain1Map(GetPos(), GetInitLambda(), GetCalculateLambda(), ctx);
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return BuildUpdateLambdaForChain1Map(GetPos(), GetUpdateLambda(), GetCalculateLambda(), ctx);
    }

    TExprNode::TPtr ExtractLaggingOutput(const TExprNode::TPtr& lagQueue,
        const TExprNode::TPtr& dependsOn, TExprContext& ctx) const override
    {
        if (!LaggingQueueIndex_.Defined()) {
            return {};
        }

        YQL_ENSURE(!FrameNeverEmpty_);
        auto output = ctx.Builder(GetPos())
            .Callable("Map")
                .Add(0, BuildQueuePeek(GetPos(), lagQueue, *LaggingQueueIndex_, dependsOn, ctx))
                .Lambda(1)
                    .Param("struct")
                    .Callable("Member")
                        .Arg(0, "struct")
                        .Atom(1, GetName())
                    .Seal()
                .Seal()
            .Seal()
            .Build();
        return CoalesceQueueOutput(GetPos(), output, OutputIsOptional_, GetDefaultValue(), ctx);
    }

private:
    const TMaybe<ui64> LaggingQueueIndex_;
    const bool OutputIsOptional_;
};

class TChain1MapTraitsLeading : public TChain1MapTraitsStateBase {
public:
    TChain1MapTraitsLeading(TStringBuf name, const TRawTrait& raw, ui64 currentRowIndex, ui64 lastRowIndex)
        : TChain1MapTraitsStateBase(name, raw)
        , QueueBegin_(currentRowIndex + 1)
        , QueueEnd_(lastRowIndex + 1)
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        YQL_ENSURE(dataQueue);
        auto originalInit = GetInitLambda();
        auto originalUpdate = GetUpdateLambda();
        auto calculate = GetCalculateLambda();

        auto rowArg = ctx.NewArgument(GetPos(), "row");
        auto state = ctx.Builder(GetPos())
            .Callable("Fold")
                .Add(0, BuildQueueRange(GetPos(), dataQueue, QueueBegin_, QueueEnd_, rowArg, ctx))
                .Apply(1, originalInit)
                    .With(0, rowArg)
                .Seal()
                .Add(2, ctx.DeepCopyLambda(*originalUpdate))
            .Seal()
            .Build();

        state = WrapWithWinContext(state, ctx);

        auto initBody = ctx.Builder(GetPos())
            .List()
                .Apply(0, calculate)
                    .With(0, state)
                .Seal()
                .Apply(1, originalInit)
                    .With(0, rowArg)
                .Seal()
            .Seal()
            .Build();

        return ctx.NewLambda(GetPos(), ctx.NewArguments(GetPos(), {rowArg}), std::move(initBody));
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        YQL_ENSURE(dataQueue);
        auto originalInit = GetInitLambda();
        auto originalUpdate = GetUpdateLambda();
        auto calculate = GetCalculateLambda();

        auto rowArg = ctx.NewArgument(GetPos(), "row");
        auto stateArg = ctx.NewArgument(GetPos(), "state");

        auto state = ctx.Builder(GetPos())
            .Callable("Fold")
                .Add(0, BuildQueueRange(GetPos(), dataQueue, QueueBegin_, QueueEnd_, rowArg, ctx))
                .Apply(1, originalUpdate)
                    .With(0, rowArg)
                    .With(1, stateArg)
                .Seal()
                .Add(2, ctx.DeepCopyLambda(*originalUpdate))
            .Seal()
            .Build();

        state = WrapWithWinContext(state, ctx);

        auto updateBody = ctx.Builder(GetPos())
            .List()
                .Apply(0, calculate)
                    .With(0, state)
                .Seal()
                .Apply(1, originalUpdate)
                    .With(0, rowArg)
                    .With(1, stateArg)
                .Seal()
            .Seal()
            .Build();

        return ctx.NewLambda(GetPos(), ctx.NewArguments(GetPos(), {rowArg, stateArg}), std::move(updateBody));
    }

private:
    const ui64 QueueBegin_;
    const ui64 QueueEnd_;
};

class TChain1MapTraitsFull : public TChain1MapTraitsStateBase {
public:
    using TQueueParam = std::variant<ui64, THandle>;

    TChain1MapTraitsFull(TStringBuf name, const TRawTrait& raw, TQueueParam queueParam)
        : TChain1MapTraitsStateBase(name, raw)
        , QueueParam_(queueParam)
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    // state == output
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        auto originalInit = GetInitLambda();
        auto originalUpdate = GetUpdateLambda();
        auto calculate = GetCalculateLambda();

        auto rowArg = ctx.NewArgument(GetPos(), "row");
        auto state = ctx.Builder(GetPos())
            .Callable("Fold")
                .Add(0, BuildQueueRange(dataQueue, rowArg, ctx))
                .Apply(1, originalInit)
                    .With(0, rowArg)
                .Seal()
                .Add(2, ctx.DeepCopyLambda(*originalUpdate))
            .Seal()
            .Build();

        state = WrapWithWinContext(state, ctx);

        auto initBody = ctx.Builder(GetPos())
            .List()
                .Apply(0, calculate)
                    .With(0, state)
                .Seal()
                .Apply(1, calculate)
                    .With(0, state)
                .Seal()
            .Seal()
            .Build();

        return ctx.NewLambda(GetPos(), ctx.NewArguments(GetPos(), {rowArg}), std::move(initBody));
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .List()
                    .Arg(0, "state")
                    .Arg(1, "state")
                .Seal()
            .Seal()
            .Build();
    }

private:
    TExprNode::TPtr BuildQueueRange(const TExprNode::TPtr& queue,
                                    const TExprNode::TPtr& dependsOn,
                                    TExprContext& ctx) const {
        if (std::holds_alternative<ui64>(QueueParam_)) {
            return ::NYql::BuildQueueRange(GetPos(), queue, std::get<ui64>(QueueParam_), Max<ui64>(), dependsOn, ctx);
        } else {
            return ctx.Builder(GetPos())
                .Callable("ListSkip")
                    .Add(0, ::NYql::BuildWinFrame(GetPos(), queue, std::get<THandle>(QueueParam_), dependsOn, ctx, /*isSingleElement=*/false))
                    .Add(1, BuildUint64(GetPos(), 1, ctx))
                .Seal()
                .Build();
        }
    }

    const TQueueParam QueueParam_;
};

class TChain1MapTraitsIncremental : public TChain1MapTraitsStateBase {
public:
    TChain1MapTraitsIncremental(TStringBuf name, const TRawTrait& raw, TMaybe<THandle> handle)
        : TChain1MapTraitsStateBase(name, raw)
        , Handle_(handle)
        , OutputIsOptional_(raw.OutputType->IsOptionalOrNull())
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return BuildInitLambdaForChain1Map(GetPos(), GetInitLambda(), GetCalculateLambda(), ctx);
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return BuildUpdateLambdaForChain1Map(GetPos(), GetUpdateLambda(), GetCalculateLambda(), ctx);
    }

    TExprNode::TPtr ExtractShiftedOutput(const TExprNode::TPtr& queue,
                                         const TExprNode::TPtr& dependsOn,
                                         TExprContext& ctx) const override
    {
        if (!Handle_.Defined()) {
            return {};
        }

        if (FrameNeverEmpty_) {
            return ExtractShiftNonEmpty(GetPos(), queue, dependsOn, GetName(), *Handle_, ctx);
        }

        auto output = ctx.Builder(GetPos())
            .Callable("Map")
                .Add(0, ::NYql::BuildWinFrame(GetPos(), queue, *Handle_, dependsOn, ctx, /*isSingleElement=*/true))
                .Lambda(1)
                    .Param("struct")
                    .Callable("Member")
                        .Arg(0, "struct")
                        .Atom(1, GetName())
                    .Seal()
                .Seal()
            .Seal()
            .Build();
        return CoalesceQueueOutput(GetPos(), output, OutputIsOptional_, GetDefaultValue(), ctx);
    }

private:
    const TMaybe<THandle> Handle_;
    const bool OutputIsOptional_;
};

class TChain1MapTraitsGeneric : public TChain1MapTraitsStateBase {
public:
    struct TFixedQueueRange {
        ui64 QueueBegin;
        ui64 QueueEnd;
    };

    using TInputQueueRange = std::variant<THandle, TFixedQueueRange>;

    TChain1MapTraitsGeneric(TStringBuf name, const TRawTrait& raw, TInputQueueRange queueParam)
        : TChain1MapTraitsStateBase(name, raw)
        , QueueParam_(queueParam)
        , OutputIsOptional_(raw.OutputType->IsOptionalOrNull())
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        auto rowArg = ctx.NewArgument(GetPos(), "row");
        auto body = ctx.Builder(GetPos())
            .List()
                .Add(0, BuildFinalOutput(rowArg, dataQueue, ctx))
                .Callable(1, "Void")
                .Seal()
            .Seal()
            .Build();
        return ctx.NewLambda(GetPos(), ctx.NewArguments(GetPos(), {rowArg}), std::move(body));
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        auto rowArg = ctx.NewArgument(GetPos(), "row");
        auto stateArg = ctx.NewArgument(GetPos(), "state");
        auto body = ctx.Builder(GetPos())
            .List()
                .Add(0, BuildFinalOutput(rowArg, dataQueue, ctx))
                .Add(1, stateArg)
            .Seal()
            .Build();
        return ctx.NewLambda(GetPos(), ctx.NewArguments(GetPos(), {rowArg, stateArg}), std::move(body));
    }

private:
    TExprNode::TPtr BuildQueueRange(TPositionHandle pos, const TExprNode::TPtr& queue,
                                    const TExprNode::TPtr& dependsOn, TExprContext& ctx) const {
        if (std::holds_alternative<TFixedQueueRange>(QueueParam_)) {
            auto [from, to] = std::get<TFixedQueueRange>(QueueParam_);
            return ::NYql::BuildQueueRange(pos, queue, from, to, dependsOn, ctx);
        } else {
            auto handle = std::get<THandle>(QueueParam_);
            return ::NYql::BuildWinFrame(pos, queue, handle, dependsOn, ctx, /*isSingleElement=*/false);
        }
        return nullptr;
    }

    TExprNode::TPtr BuildFinalOutput(const TExprNode::TPtr& rowArg, const TExprNode::TPtr& dataQueue, TExprContext& ctx) const {
        YQL_ENSURE(dataQueue);
        auto originalInit = GetInitLambda();
        auto originalUpdate = GetUpdateLambda();
        auto calculate = GetCalculateLambda();

        auto fold1 = ctx.Builder(GetPos())
            .Callable("Fold1")
                .Add(0, BuildQueueRange(GetPos(), dataQueue, rowArg, ctx))
                .Add(1, ctx.DeepCopyLambda(*originalInit))
                .Add(2, ctx.DeepCopyLambda(*originalUpdate))
            .Seal()
            .Build();

        fold1 = WrapWithWinContext(fold1, ctx);

        auto output = ctx.Builder(GetPos())
            .Callable("Map")
                .Add(0, fold1)
                .Add(1, ctx.DeepCopyLambda(*calculate))
            .Seal()
            .Build();

        if (FrameNeverEmpty_) {
            return Unwrap(GetPos(), /*output=*/output, /*calculate=*/calculate, /*originalInit=*/originalInit, /*rowArg=*/rowArg, ctx);
        }

        return CoalesceQueueOutput(GetPos(), output, OutputIsOptional_, GetDefaultValue(), ctx);
    }

    TInputQueueRange QueueParam_;
    const bool OutputIsOptional_;
};

class TChain1MapTraitsEmpty : public TChain1MapTraitsStateBase {
public:
    TChain1MapTraitsEmpty(TStringBuf name, const TRawTrait& raw)
        : TChain1MapTraitsStateBase(name, raw)
        , RawOutputType_(raw.OutputType)
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .List()
                    .Add(0, BuildFinalOutput(ctx))
                    .Callable(1, "Void")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .List()
                    .Add(0, BuildFinalOutput(ctx))
                    .Arg(1, "state")
                .Seal()
            .Seal()
            .Build();
    }

private:
    TExprNode::TPtr BuildFinalOutput(TExprContext& ctx) const {
        const auto defaultValue = GetDefaultValue();
        YQL_ENSURE(!FrameNeverEmpty_);

        if (defaultValue->IsCallable("Null")) {
            auto resultingType = RawOutputType_;
            if (!resultingType->IsOptionalOrNull()) {
                resultingType = ctx.MakeType<TOptionalExprType>(resultingType);
            }

            return ctx.Builder(GetPos())
                .Callable("Nothing")
                    .Add(0, ExpandType(GetPos(), *resultingType, ctx))
                .Seal()
                .Build();
        }
        return defaultValue;
    }

    const TTypeAnnotationNode* const RawOutputType_;
};

struct TQueueParams {
    ui64 DataOutpace = 0;
    ui64 DataLag = 0;
    bool DataQueueNeeded = false;
    ui64 LagQueueSize = 0;
    const TTypeAnnotationNode* LagQueueItemType = nullptr;
};

TChain1MapTraits::TPtr ProcessRowFrameAggregateTraitNewPipeline(const TRawTrait& trait,
                                                                TStringBuf name,
                                                                TExprNodeCoreWinFrameCollectorBounds& bounds,
                                                                TExprNodeCoreWinFrameCollectorBounds& incrementalBounds) {
    switch (GetFrameTypeNew(trait.FrameSettings)) {
        case EFrameBoundsNewType::INCREMENTAL: {
            auto last = trait.FrameSettings.GetRowFrame().second;
            MKQL_ENSURE(last.Defined(), "Last offset required.");
            auto getIncrementalHandle = [&]() -> TMaybe<THandle> {
                if (*last == 0) {
                    return TMaybe<THandle>();
                }
                return incrementalBounds.AddRowIncremental(FromSettingsNumbers(*last));

            };
            TMaybe<THandle> handle = getIncrementalHandle();
            return new TChain1MapTraitsIncremental(name, trait, handle);
        }
        case EFrameBoundsNewType::FULL: {
            auto handle = bounds.AddRow(TInputRowWindowFrame(TInputRow::Inf(EDirection::Preceding), TInputRow::Inf(EDirection::Following)));
            return new TChain1MapTraitsFull(name, trait, handle);
        }
        case EFrameBoundsNewType::GENERIC: {
            auto first = trait.FrameSettings.GetRowFrame().first;
            auto last = trait.FrameSettings.GetRowFrame().second;
            YQL_ENSURE(first, "First offset must be defined.");
            auto handle = bounds.AddRow({FromSettingsNumbers(*first), FromSettingsNumbers(last, EDirection::Following)});
            return new TChain1MapTraitsGeneric(name, trait, handle);
        }
        case EFrameBoundsNewType::EMPTY: {
            return new TChain1MapTraitsEmpty(name, trait);
        }
    }
}

TChain1MapTraits::TPtr ProcessRangeFrameAggregateTraitNewPipeline(const TRawTrait& trait,
                                                                  TStringBuf name,
                                                                  TExprNodeCoreWinFrameCollectorBounds& bounds,
                                                                  TExprNodeCoreWinFrameCollectorBounds& incrementalBounds) {
    switch (GetFrameTypeNew(trait.FrameSettings)) {
        case EFrameBoundsNewType::INCREMENTAL: {
            auto last = trait.FrameSettings.GetRangeFrame().GetLast();
            MKQL_ENSURE(!last.IsInf(), "Last offset required.");
            auto getIncrementalHandle = [&]() -> TMaybe<THandle> {
                return incrementalBounds.AddRangeIncremental(last);
            };
            TMaybe<THandle> handle = getIncrementalHandle();
            return new TChain1MapTraitsIncremental(name, trait, handle);
        }
        case EFrameBoundsNewType::FULL: {
            // Note: AddRow since UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING are the same for all frame types.
            auto handle = bounds.AddRow(TInputRowWindowFrame(TInputRow::Inf(EDirection::Preceding), TInputRow::Inf(EDirection::Following)));
            return new TChain1MapTraitsFull(name, trait, handle);
        }
        case EFrameBoundsNewType::GENERIC: {
            auto first = trait.FrameSettings.GetRangeFrame().GetFirst();
            auto last = trait.FrameSettings.GetRangeFrame().GetLast();
            YQL_ENSURE(!first.IsInf(), "First offset must be defined.");
            auto handle = bounds.AddRange({first, last});
            return new TChain1MapTraitsGeneric(name, trait, handle);
        }
        case EFrameBoundsNewType::EMPTY: {
            return new TChain1MapTraitsEmpty(name, trait);
        }
    }
}

TChain1MapTraits::TPtr ProcessRowFrameAggregateTraitOldPipeline(TQueueParams& queueParams,
                                                                const TRawTrait& trait,
                                                                TStringBuf name,
                                                                ui64 currentRowIndex) {
    auto first = trait.FrameSettings.GetRowFrame().first;
    auto last = trait.FrameSettings.GetRowFrame().second;
    switch (FrameBoundsType(trait.FrameSettings.GetRowFrame())) {
        case EFrameBoundsType::CURRENT:
        case EFrameBoundsType::LAGGING: {
            TMaybe<ui64> lagQueueIndex;
            auto end = *last;
            YQL_ENSURE(end <= 0);
            if (end < 0) {
                YQL_ENSURE(queueParams.LagQueueSize >= ui64(0 - end));
                lagQueueIndex = queueParams.LagQueueSize + end;
            }
            return new TChain1MapTraitsCurrentOrLagging(name, trait, lagQueueIndex);
        }
        case EFrameBoundsType::LEADING: {
            YQL_ENSURE(last, "Last offset must be specified.");
            auto end = *last;
            YQL_ENSURE(end > 0);
            ui64 lastRowIndex = currentRowIndex + ui64(end);
            return new TChain1MapTraitsLeading(name, trait, currentRowIndex, lastRowIndex);
        }
        case EFrameBoundsType::FULL: {
            return new TChain1MapTraitsFull(name, trait, currentRowIndex + 1);
        }
        case EFrameBoundsType::GENERIC: {
            queueParams.DataQueueNeeded = true;
            YQL_ENSURE(first.Defined());
            ui64 beginIndex = currentRowIndex + *first;
            ui64 endIndex = last.Defined() ? (currentRowIndex + *last + 1) : Max<ui64>();
            return new TChain1MapTraitsGeneric(name, trait, TChain1MapTraitsGeneric::TFixedQueueRange{beginIndex, endIndex});
        }
        case EFrameBoundsType::EMPTY: {
            return new TChain1MapTraitsEmpty(name, trait);
        }
    }
}

TChain1MapTraits::TPtr ProcessLeadLag(const TRawTrait& trait,
                                      TStringBuf name,
                                      TExprNodeCoreWinFrameCollectorBounds& bounds,
                                      ui64 currentRowIndex,
                                      TTypeAnnotationContext& types) {
    YQL_ENSURE(!trait.UpdateLambda);
    YQL_ENSURE(!trait.DefaultValue);
    if (!IsWindowNewPipelineEnabled(types)) {
        TMaybe<ui64> queueOffset;
        if (*trait.CalculateLambdaLead != 0) {
            queueOffset = currentRowIndex + *trait.CalculateLambdaLead;
        }
        return new TChain1MapTraitsLagLead(name, trait, queueOffset);
    } else {
        if (*trait.CalculateLambdaLead == 0) {
            return new TChain1MapTraitsLagLead(name, trait, {});
        } else {
            auto handle = bounds.AddRow({FromSettingsNumbers(*trait.CalculateLambdaLead), FromSettingsNumbers(*trait.CalculateLambdaLead)});
            return new TChain1MapTraitsLagLead(name, trait, handle);
        }
    }
}

TChain1MapTraits::TPtr ProcessPartitionBaseTraits(const TRawTrait& trait,
                                                  TStringBuf name,
                                                  const TMaybe<TString>& partitionRowsColumn,
                                                  TExprNodeCoreWinFrameCollectorBounds* incrementalBounds) {
    YQL_ENSURE(!trait.UpdateLambda);
    YQL_ENSURE(!trait.DefaultValue);

    auto handle = [&]() -> TMaybe<THandle> {
        if (!incrementalBounds) {
            return {};
        }
        if (trait.FrameSettings.GetFrameType() != EFrameType::FrameByRange) {
            return {};
        }
        YQL_ENSURE(trait.FrameSettings.IsLeftInf() && trait.FrameSettings.IsRightCurrent());
        return incrementalBounds->AddRangeIncremental(trait.FrameSettings.GetRangeFrame().GetLast());
    };

    if (trait.CalculateLambda->IsCallable("RowNumber")) {
        return new TChain1MapTraitsRowNumber(name, trait);
    } else if (trait.CalculateLambda->IsCallable("Rank")) {
        return new TChain1MapTraitsRank(name, trait);
    } else if (trait.CalculateLambda->IsCallable("CumeDist")) {
        YQL_ENSURE(trait.FrameSettings.GetFrameType() == EFrameType::FrameByRange || trait.FrameSettings.GetFrameType() == EFrameType::FrameByRows);
        return new TChain1MapTraitsCumeDist(name, trait, handle(), *partitionRowsColumn);
    } else if (trait.CalculateLambda->IsCallable("NTile")) {
        return new TChain1MapTraitsNTile(name, trait, *partitionRowsColumn);
    } else if (trait.CalculateLambda->IsCallable("PercentRank")) {
        return new TChain1MapTraitsPercentRank(name, trait, *partitionRowsColumn);
    } else {
        YQL_ENSURE(trait.CalculateLambda->IsCallable("DenseRank"));
        return new TChain1MapTraitsDenseRank(name, trait);
    }
}

TChain1MapTraits::TPtr ProcessFrameIndependedTraits(const TRawTrait& trait,
                                                    TStringBuf name,
                                                    TExprNodeCoreWinFrameCollectorBounds& bounds,
                                                    TExprNodeCoreWinFrameCollectorBounds& incrementalBounds,
                                                    const TMaybe<TString>& partitionRowsColumn,
                                                    ui64 currentRowIndex,
                                                    TTypeAnnotationContext& types) {
    YQL_ENSURE(!trait.UpdateLambda);
    YQL_ENSURE(!trait.DefaultValue);
    if (trait.CalculateLambdaLead.Defined()) {
        return ProcessLeadLag(trait, name, bounds, currentRowIndex, types);
    } else {
        return ProcessPartitionBaseTraits(trait, name, partitionRowsColumn, &incrementalBounds);
    }
}

TVector<TChain1MapTraits::TPtr> BuildFoldMapTraitsForNonNumericRange(const TExprNode::TPtr& frames,
                                                                     const TStructExprType& rowType,
                                                                     const TMaybe<TString>& partitionRowsColumn,
                                                                     TExprContext& ctx) {
    TVector<TChain1MapTraits::TPtr> result;
    TCalcOverWindowTraits traits = ExtractCalcOverWindowTraits(frames, rowType, ctx);
    for (const auto& item : traits.RawTraits) {
        TStringBuf name = item.first;
        const TRawTrait& trait = item.second;
        if (!trait.InitLambda) {
            result.push_back(ProcessPartitionBaseTraits(trait, name, partitionRowsColumn, nullptr));
            continue;
        }
        YQL_ENSURE(trait.FrameSettings.GetFrameType() == EFrameType::FrameByRange);
        YQL_ENSURE(trait.FrameSettings.IsLeftInf() && trait.FrameSettings.IsRightCurrent());
        result.push_back(new TChain1MapTraitsIncremental(name, trait, {}));
    }
    return result;
}

TVector<TChain1MapTraits::TPtr> BuildFoldMapTraitsForRowsAndNumericRanges(TQueueParams& queueParams,
                                                                          TExprNodeCoreWinFrameCollectorBounds& bounds,
                                                                          TExprNodeCoreWinFrameCollectorBounds& incrementalBounds,
                                                                          const TExprNode::TPtr& frames,
                                                                          const TMaybe<TString>& partitionRowsColumn,
                                                                          const TStructExprType& rowType,
                                                                          TExprContext& ctx,
                                                                          TTypeAnnotationContext& typeCtx) {
    queueParams = {};

    TVector<TChain1MapTraits::TPtr> result;

    TCalcOverWindowTraits traits = ExtractCalcOverWindowTraits(frames, rowType, ctx);

    if (traits.LagQueueItemType->Cast<TStructExprType>()->GetSize()) {
        YQL_ENSURE(traits.QueueParams.MaxUnboundedPrecedingLag > 0);
        queueParams.LagQueueSize = traits.QueueParams.MaxUnboundedPrecedingLag;
        queueParams.LagQueueItemType = traits.LagQueueItemType;
    }

    ui64 currentRowIndex = 0;
    if (traits.QueueParams.MaxDataOutpace || traits.QueueParams.MaxDataLag) {
        queueParams.DataOutpace = traits.QueueParams.MaxDataOutpace;
        queueParams.DataLag = traits.QueueParams.MaxDataLag;
        currentRowIndex = queueParams.DataLag;
        queueParams.DataQueueNeeded = true;
    }

    for (const auto& item : traits.RawTraits) {
        TStringBuf name = item.first;
        const TRawTrait& trait = item.second;

        if (!trait.InitLambda) {
            result.push_back(ProcessFrameIndependedTraits(trait, name, bounds, incrementalBounds, partitionRowsColumn, currentRowIndex, typeCtx));
            continue;
        }

        if (IsWindowNewPipelineEnabled(typeCtx)) {
            if (trait.FrameSettings.GetFrameType() == EFrameType::FrameByRows) {
                result.push_back(ProcessRowFrameAggregateTraitNewPipeline(trait, name, bounds, incrementalBounds));
            } else {
                YQL_ENSURE(trait.FrameSettings.GetFrameType() == EFrameType::FrameByRange);
                YQL_ENSURE(IsRangeWindowFrameEnabled(typeCtx));
                result.push_back(ProcessRangeFrameAggregateTraitNewPipeline(trait, name, bounds, incrementalBounds));
            }
        } else {
            YQL_ENSURE(trait.FrameSettings.GetFrameType() == EFrameType::FrameByRows);
            result.push_back(ProcessRowFrameAggregateTraitOldPipeline(queueParams, trait, name, currentRowIndex));
            continue;
        }
    }

    return result;
}

TExprNode::TPtr ConvertStructOfTuplesToTupleOfStructs(TPositionHandle pos, const TExprNode::TPtr& input, TExprContext& ctx) {
    return ctx.Builder(pos)
        .List()
            .Callable(0, "StaticMap")
                .Add(0, input)
                .Lambda(1)
                    .Param("tuple")
                    .Callable("Nth")
                        .Arg(0, "tuple")
                        .Atom(1, "0")
                    .Seal()
                .Seal()
            .Seal()
            .Callable(1, "StaticMap")
                .Add(0, input)
                .Lambda(1)
                    .Param("tuple")
                    .Callable("Nth")
                        .Arg(0, "tuple")
                        .Atom(1, "1")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr AddInputMembersToOutput(TPositionHandle pos, const TExprNode::TPtr& tupleOfOutputStructAndStateStruct,
    const TExprNode::TPtr& rowArg, TExprContext& ctx)
{
    return ctx.Builder(pos)
        .List()
            .Callable(0, "FlattenMembers")
                .List(0)
                    .Atom(0, "")
                    .Callable(1, "Nth")
                        .Add(0, tupleOfOutputStructAndStateStruct)
                        .Atom(1, "0")
                    .Seal()
                .Seal()
                .List(1)
                    .Atom(0, "")
                    .Add(1, rowArg)
                .Seal()
            .Seal()
            .Callable(1, "Nth")
                .Add(0, tupleOfOutputStructAndStateStruct)
                .Atom(1, "1")
            .Seal()
        .Seal()
        .Build();
}

template<typename T>
TExprNode::TPtr SelectMembers(TPositionHandle pos, const T& members, const TExprNode::TPtr& structNode, TExprContext& ctx) {
    TExprNodeList structItems;
    for (auto& name : members) {
        structItems.push_back(
            ctx.Builder(pos)
                .List()
                    .Atom(0, name)
                    .Callable(1, "Member")
                        .Add(0, structNode)
                        .Atom(1, name)
                    .Seal()
                .Seal()
                .Build()
        );
    }
    return ctx.NewCallable(pos, "AsStruct", std::move(structItems));
}

template<typename T>
TExprNode::TPtr RemoveMembers(TPositionHandle pos, const T& members, const TExprNode::TPtr& structNode, TExprContext& ctx) {
    return ctx.Builder(pos)
            .Callable("RemoveMembers")
                .Add(0, structNode)
                .List(1)
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        size_t i = 0;
                        for (auto name : members) {
                            parent.Atom(i++, name);
                        }
                        return parent;
                    })
                .Seal()
            .Seal()
            .Build();
}

TExprNode::TPtr HandleLaggingItems(TPositionHandle pos,
                                   const TExprNode::TPtr& rowArg,
                                   const TExprNode::TPtr& tupleOfOutputAndState,
                                   const TVector<TChain1MapTraits::TPtr>& traits,
                                   const TExprNode::TPtr& lagQueue,
                                   TExprContext& ctx,
                                   TTypeAnnotationContext& typeCtx)
{
    TExprNodeList laggingStructItems;
    TSet<TStringBuf> laggingNames;
    TSet<TStringBuf> otherNames;
    for (auto& trait : traits) {
        auto name = trait->GetName();
        auto laggingOutput = trait->ExtractLaggingOutput(lagQueue, rowArg, ctx);
        if (laggingOutput) {
            laggingNames.insert(name);
            laggingStructItems.push_back(
                ctx.Builder(pos)
                    .List()
                        .Atom(0, name)
                        .Add(1, laggingOutput)
                    .Seal()
                    .Build()
            );
        } else {
            otherNames.insert(trait->GetName());
        }
    }

    if (laggingStructItems.empty()) {
        return tupleOfOutputAndState;
    }
    YQL_ENSURE(!IsWindowNewPipelineEnabled(typeCtx));
    YQL_ENSURE(lagQueue);

    auto output = ctx.NewCallable(pos, "Nth", { tupleOfOutputAndState, ctx.NewAtom(pos, "0")});
    auto state  = ctx.NewCallable(pos, "Nth", { tupleOfOutputAndState, ctx.NewAtom(pos, "1")});

    auto leadingOutput = SelectMembers(pos, laggingNames, output, ctx);
    auto otherOutput = SelectMembers(pos, otherNames, output, ctx);
    auto laggingOutput = ctx.NewCallable(pos, "AsStruct", std::move(laggingStructItems));

    output = ctx.Builder(pos)
        .Callable("FlattenMembers")
            .List(0)
                .Atom(0, "")
                .Add(1, laggingOutput)
            .Seal()
            .List(1)
                .Atom(0, "")
                .Add(1, otherOutput)
            .Seal()
        .Seal()
        .Build();


    return ctx.Builder(pos)
        .List()
            .Add(0, output)
            .Callable(1, "Seq")
                .Add(0, output)
                .Add(1, state)
                .Add(2, leadingOutput)
                .List(3)
                    .Add(0, state)
                    .Callable(1, "QueuePush")
                        .Callable(0, "QueuePop")
                            .Add(0, lagQueue)
                        .Seal()
                        .Add(1, leadingOutput)
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}
TExprNode::TPtr ReplaceAllShiftedElements(TPositionHandle pos, const TExprNode::TPtr& rowArg,  const TExprNodeList& laggingStructItems, TSet<TStringBuf> laggingNames, TExprContext& ctx) {
    auto otherOutput = RemoveMembers(pos, laggingNames, rowArg, ctx);
    auto laggingOutput = ctx.NewCallable(pos, "AsStruct", TExprNodeList(laggingStructItems));
    return ctx.Builder(pos)
        .Callable("FlattenMembers")
            .List(0)
                .Atom(0, "")
                .Add(1, laggingOutput)
            .Seal()
            .List(1)
                .Atom(0, "")
                .Add(1, otherOutput)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr HandleIncrementalOutput(TPositionHandle pos,
                                        const TExprNode::TPtr& rowArg,
                                        const TVector<TChain1MapTraits::TPtr>& traits,
                                        const TExprNode::TPtr& dataQueue,
                                        TExprContext& ctx)
{
    TExprNodeList laggingStructItems;
    TSet<TStringBuf> laggingNames;
    for (auto& trait : traits) {
        auto name = trait->GetName();
        auto laggingOutput = trait->ExtractShiftedOutput(dataQueue, rowArg, ctx);
        if (laggingOutput) {
            laggingNames.insert(name);
            laggingStructItems.push_back(
                ctx.Builder(pos)
                    .List()
                        .Atom(0, name)
                        .Add(1, laggingOutput)
                    .Seal()
                    .Build()
            );
        }
    }

    YQL_ENSURE(!laggingStructItems.empty());
    YQL_ENSURE(dataQueue);
    return ReplaceAllShiftedElements(pos, rowArg, laggingStructItems, laggingNames, ctx);;
}

TExprNode::TPtr BuildChain1MapInitLambda(TPositionHandle pos,
                                         const TVector<TChain1MapTraits::TPtr>& traits,
                                         const TExprNode::TPtr& dataQueue,
                                         ui64 lagQueueSize,
                                         const TTypeAnnotationNode* lagQueueItemType,
                                         TExprContext& ctx,
                                         TTypeAnnotationContext& typeCtx)
{
    auto rowArg = ctx.NewArgument(pos, "row");

    TExprNode::TPtr lagQueue;
    if (lagQueueSize) {
        YQL_ENSURE(lagQueueItemType);
        lagQueue = BuildQueue(pos, *lagQueueItemType, lagQueueSize, lagQueueSize, rowArg, ctx);
    }

    TExprNodeList structItems;
    for (auto& trait : traits) {
        structItems.push_back(
            ctx.Builder(pos)
                .List()
                    .Atom(0, trait->GetName())
                    .Apply(1, trait->BuildInitLambda(dataQueue, ctx))
                        .With(0, rowArg)
                    .Seal()
                .Seal()
                .Build()
        );
    }

    auto asStruct = ctx.NewCallable(pos, "AsStruct", std::move(structItems));
    auto tupleOfOutputAndState = ConvertStructOfTuplesToTupleOfStructs(pos, asStruct, ctx);

    tupleOfOutputAndState = HandleLaggingItems(pos, rowArg, tupleOfOutputAndState, traits, lagQueue, ctx, typeCtx);

    auto finalBody = AddInputMembersToOutput(pos, tupleOfOutputAndState, rowArg, ctx);
    return ctx.NewLambda(pos, ctx.NewArguments(pos, {rowArg}), std::move(finalBody));
}

TExprNode::TPtr BuildChain1MapUpdateLambda(TPositionHandle pos,
                                           const TVector<TChain1MapTraits::TPtr>& traits,
                                           const TExprNode::TPtr& dataQueue,
                                           bool haveLagQueue,
                                           TExprContext& ctx,
                                           TTypeAnnotationContext& typeCtx)
{
    const auto rowArg = ctx.NewArgument(pos, "row");
    const auto stateArg = ctx.NewArgument(pos, "state");
    auto state = ctx.Builder(pos)
        .Callable("Nth")
            .Add(0, stateArg)
            .Atom(1, "1", TNodeFlags::Default)
        .Seal()
        .Build();

    TExprNode::TPtr lagQueue;
    if (haveLagQueue) {
        lagQueue = ctx.Builder(pos)
            .Callable("Nth")
                .Add(0, state)
                .Atom(1, "1", TNodeFlags::Default)
            .Seal()
            .Build();
        state = ctx.Builder(pos)
            .Callable("Nth")
                .Add(0, std::move(state))
                .Atom(1, "0", TNodeFlags::Default)
            .Seal()
            .Build();
    }

    TExprNodeList structItems;
    for (auto& trait : traits) {
        structItems.push_back(
            ctx.Builder(pos)
                .List()
                    .Atom(0, trait->GetName())
                    .Apply(1, trait->BuildUpdateLambda(dataQueue, ctx))
                        .With(0, rowArg)
                        .With(1)
                            .Callable("Member")
                                .Add(0, state)
                                .Atom(1, trait->GetName())
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
                .Build()
        );
    }

    auto asStruct = ctx.NewCallable(pos, "AsStruct", std::move(structItems));
    auto tupleOfOutputAndState = ConvertStructOfTuplesToTupleOfStructs(pos, asStruct, ctx);

    tupleOfOutputAndState = HandleLaggingItems(pos, rowArg, tupleOfOutputAndState, traits, lagQueue, ctx, typeCtx);

    auto finalBody = AddInputMembersToOutput(pos, tupleOfOutputAndState, rowArg, ctx);
    return ctx.NewLambda(pos, ctx.NewArguments(pos, {rowArg, stateArg}), std::move(finalBody));
}

bool IsNonCompactFullFrame(const TExprNode& winOnRows, TExprContext& ctx) {
    TWindowFrameSettings frameSettings = TWindowFrameSettings::Parse(winOnRows, ctx);
    return frameSettings.IsFullPartition() && !frameSettings.IsCompact();
}

TExprNode::TPtr DeduceCompatibleSort(const TExprNode::TPtr& traitsOne, const TExprNode::TPtr& traitsTwo) {
    YQL_ENSURE(traitsOne->IsCallable({"Void", "SortTraits"}));
    YQL_ENSURE(traitsTwo->IsCallable({"Void", "SortTraits"}));

    if (traitsOne->IsCallable("Void")) {
        return traitsTwo;
    }

    if (traitsTwo->IsCallable("Void")) {
        return traitsOne;
    }

    // TODO: need more advanced logic here
    if (traitsOne == traitsTwo) {
        return traitsOne;
    }

    return {};
}

TExprNode::TPtr BuildPartitionsByKeys(TPositionHandle pos, const TExprNode::TPtr& input, const TExprNode::TPtr& keySelector,
    const TExprNode::TPtr& sortOrder, const TExprNode::TPtr& sortKey, const TExprNode::TPtr& streamProcessingLambda,
    const TExprNode::TPtr& sessionKey, const TExprNode::TPtr& sessionInit, const TExprNode::TPtr& sessionUpdate,
    const TExprNode::TPtr& sessionColumns, TExprContext& ctx)
{
    TExprNode::TPtr preprocessLambda;
    TExprNode::TPtr chopperKeySelector;
    const TExprNode::TPtr addSessionColumnsArg = ctx.NewArgument(pos, "row");
    TExprNode::TPtr addSessionColumnsBody = addSessionColumnsArg;
    if (sessionUpdate) {
        YQL_ENSURE(sessionKey);
        YQL_ENSURE(sessionInit);
        preprocessLambda =
            AddSessionParamsMemberLambda(pos, SessionStartMemberName, SessionParamsMemberName, keySelector, sessionKey, sessionInit, sessionUpdate, ctx);

        chopperKeySelector = ctx.Builder(pos)
            .Lambda()
                .Param("item")
                .List()
                    .Apply(0, keySelector)
                        .With(0, "item")
                    .Seal()
                    .Callable(1, "Member")
                        .Arg(0, "item")
                        .Atom(1, SessionStartMemberName)
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    } else {
        YQL_ENSURE(!sessionKey);
        preprocessLambda = MakeIdentityLambda(pos, ctx);
        chopperKeySelector = keySelector;
    }

    for (auto& column : sessionColumns->ChildrenList()) {
        addSessionColumnsBody = ctx.Builder(pos)
            .Callable("AddMember")
                .Add(0, addSessionColumnsBody)
                .Add(1, column)
                .Callable(2, "Member")
                    .Add(0, addSessionColumnsArg)
                    .Atom(1, SessionParamsMemberName)
                .Seal()
            .Seal()
            .Build();
    }

    addSessionColumnsBody = ctx.Builder(pos)
        .Callable("ForceRemoveMember")
            .Callable(0, "ForceRemoveMember")
                .Add(0, addSessionColumnsBody)
                .Atom(1, SessionStartMemberName)
            .Seal()
            .Atom(1, SessionParamsMemberName)
        .Seal()
        .Build();

    auto addSessionColumnsLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { addSessionColumnsArg }), std::move(addSessionColumnsBody));

    auto groupSwitchLambda = ctx.Builder(pos)
        .Lambda()
            .Param("prevKey")
            .Param("item")
            .Callable("AggrNotEquals")
                .Arg(0, "prevKey")
                .Apply(1, chopperKeySelector)
                    .With(0, "item")
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return ctx.Builder(pos)
        .Callable("PartitionsByKeys")
            .Add(0, input)
            .Add(1, keySelector)
            .Add(2, sortOrder)
            .Add(3, sortKey)
            .Lambda(4)
                .Param("partitionedStream")
                .Callable("ForwardList")
                    .Callable(0, "Chopper")
                        .Callable(0, "ToStream")
                            .Apply(0, preprocessLambda)
                                .With(0, "partitionedStream")
                            .Seal()
                        .Seal()
                        .Add(1, chopperKeySelector)
                        .Add(2, groupSwitchLambda)
                        .Lambda(3)
                            .Param("key")
                            .Param("singlePartition")
                            .Callable("Map")
                                .Apply(0, streamProcessingLambda)
                                    .With(0, "singlePartition")
                                .Seal()
                                .Add(1, addSessionColumnsLambda)
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

enum EFold1LambdaKind {
    INIT,
    UPDATE,
    CALCULATE,
};

TExprNode::TPtr BuildFold1Lambda(TPositionHandle pos, const TExprNode::TPtr& frames, EFold1LambdaKind kind,
    const TExprNodeList& keyColumns, const TStructExprType& rowType, TExprContext& ctx)
{
    TExprNode::TPtr arg1 = ctx.NewArgument(pos, "arg1");
    TExprNodeList args = { arg1 };

    TExprNode::TPtr arg2;
    if (kind == EFold1LambdaKind::UPDATE) {
        arg2 = ctx.NewArgument(pos, "arg2");
        args.push_back(arg2);
    }

    TExprNodeList structItems;
    for (auto& winOn : frames->ChildrenList()) {
        YQL_ENSURE(IsNonCompactFullFrame(*winOn, ctx));
        for (ui32 i = 1; i < winOn->ChildrenSize(); ++i) {
            YQL_ENSURE(winOn->Child(i)->IsList());
            YQL_ENSURE(winOn->Child(i)->Child(0)->IsAtom());
            YQL_ENSURE(winOn->Child(i)->Child(1)->IsCallable("WindowTraits"));
            YQL_ENSURE(2 <= winOn->Child(i)->ChildrenSize() && winOn->Child(i)->ChildrenSize() <= 3);

            auto column = winOn->Child(i)->ChildPtr(0);
            auto traits = winOn->Child(i)->ChildPtr(1);
            auto traitsInputType = traits->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();

            TStringBuf distinctKey;
            const TTypeAnnotationNode* distinctKeyOrigType = nullptr;
            if (winOn->Child(i)->ChildrenSize() == 3) {
                auto distinctKeyNode = winOn->Child(i)->Child(2);
                YQL_ENSURE(distinctKeyNode->IsAtom());
                distinctKey = distinctKeyNode->Content();

                distinctKeyOrigType = rowType.FindItemType(distinctKey);
                YQL_ENSURE(distinctKeyOrigType);
            }

            TExprNode::TPtr applied;
            switch (kind) {
                case EFold1LambdaKind::INIT: {
                    auto lambda = traits->ChildPtr(1);
                    if (distinctKeyOrigType) {
                        lambda = ApplyDistinctForInitLambda(lambda, distinctKey, *traitsInputType, *distinctKeyOrigType, ctx);
                    } else {
                        lambda = ReplaceFirstLambdaArgWithCastStruct(*lambda, *traitsInputType, ctx);
                    }

                    if (lambda->Child(0)->ChildrenSize() == 2) {
                        lambda = ReplaceLastLambdaArgWithUnsignedLiteral(*lambda, i, ctx);
                    }
                    YQL_ENSURE(lambda->Child(0)->ChildrenSize() == 1);

                    applied = ctx.Builder(pos)
                        .Apply(lambda)
                            .With(0, arg1)
                        .Seal()
                        .Build();
                    break;
                }
                case EFold1LambdaKind::CALCULATE: {
                    auto lambda = traits->ChildPtr(4);
                    YQL_ENSURE(lambda->Child(0)->ChildrenSize() == 1);

                    if (distinctKeyOrigType) {
                        lambda = ApplyDistinctForCalculateLambda(lambda, ctx);
                    }

                    applied = ctx.Builder(pos)
                        .Apply(lambda)
                            .With(0)
                                .Callable("Member")
                                    .Add(0, arg1)
                                    .Add(1, column)
                                .Seal()
                            .Done()
                        .Seal()
                        .Build();
                    break;
                }
                case EFold1LambdaKind::UPDATE: {
                    auto lambda = traits->ChildPtr(2);
                    if (distinctKeyOrigType) {
                        lambda = ApplyDistinctForUpdateLambda(lambda, distinctKey, *traitsInputType, *distinctKeyOrigType, ctx);
                    } else {
                        lambda = ReplaceFirstLambdaArgWithCastStruct(*lambda, *traitsInputType, ctx);
                    }

                    if (lambda->Child(0)->ChildrenSize() == 3) {
                        lambda = ReplaceLastLambdaArgWithUnsignedLiteral(*lambda, i, ctx);
                    }
                    YQL_ENSURE(lambda->Child(0)->ChildrenSize() == 2);

                    applied = ctx.Builder(pos)
                        .Apply(lambda)
                            .With(0, arg1)
                            .With(1)
                                .Callable("Member")
                                    .Add(0, arg2)
                                    .Add(1, column)
                                .Seal()
                            .Done()
                        .Seal()
                        .Build();
                    break;
                }
            }

            structItems.push_back(ctx.NewList(pos, {column, applied}));
        }
    }

    // pass key columns as-is
    for (auto& keyColumn : keyColumns) {
        YQL_ENSURE(keyColumn->IsAtom());
        structItems.push_back(
            ctx.Builder(pos)
                .List()
                    .Add(0, keyColumn)
                    .Callable(1, "Member")
                        .Add(0, arg1)
                        .Add(1, keyColumn)
                    .Seal()
                .Seal()
                .Build()
        );
    }
    return ctx.NewLambda(pos, ctx.NewArguments(pos, std::move(args)), ctx.NewCallable(pos, "AsStruct", std::move(structItems)));
}

TExprNode::TPtr ExpandNonCompactFullFrames(TPositionHandle pos, const TExprNode::TPtr& inputList,
    const TExprNode::TPtr& originalKeyColumns, const TExprNode::TPtr& sortTraits, const TExprNode::TPtr& frames,
    const TExprNode::TPtr& sessionTraits, const TExprNode::TPtr& sessionColumns, TExprContext& ctx)
{
    TExprNode::TPtr sessionKey;
    TExprNode::TPtr sessionInit;
    TExprNode::TPtr sessionUpdate;
    TExprNode::TPtr sessionSortTraits;
    const TTypeAnnotationNode* sessionKeyType = nullptr;
    const TTypeAnnotationNode* sessionParamsType = nullptr;
    ExtractSessionWindowParams(pos, sessionTraits, sessionKey, sessionKeyType, sessionParamsType, sessionSortTraits, sessionInit, sessionUpdate, ctx);

    TExprNode::TPtr sortKey;
    TExprNode::TPtr sortOrder;
    TExprNode::TPtr input = inputList;
    if (input->IsCallable("ForwardList")) {
        // full frame strategy uses input 2 times (for grouping and join)
        // TODO: better way to detect "single use input"
        input = ctx.NewCallable(pos, "Collect", { input });
    }

    const auto rowType = inputList->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    TVector<const TItemExprType*> rowItems = rowType->GetItems();
    TExprNodeList originalKeysWithSession = originalKeyColumns->ChildrenList();

    TExprNodeList addedColumns;
    const auto commonSortTraits = DeduceCompatibleSort(sortTraits, sessionSortTraits);
    ExtractSortKeyAndOrder(pos, commonSortTraits ? commonSortTraits : sortTraits, sortKey, sortOrder, ctx);
    if (!commonSortTraits) {
        YQL_ENSURE(sessionKey);
        YQL_ENSURE(sessionInit);
        YQL_ENSURE(sessionUpdate);
        TExprNode::TPtr sessionSortKey;
        TExprNode::TPtr sessionSortOrder;
        ExtractSortKeyAndOrder(pos, sessionSortTraits, sessionSortKey, sessionSortOrder, ctx);
        const auto keySelector = BuildKeySelector(pos, *rowType, originalKeyColumns, ctx);
        input = ctx.Builder(pos)
            .Callable("PartitionsByKeys")
                .Add(0, input)
                .Add(1, keySelector)
                .Add(2, sessionSortOrder)
                .Add(3, sessionSortKey)
                .Lambda(4)
                    .Param("partitionedStream")
                    .Apply(AddSessionParamsMemberLambda(pos, SessionStartMemberName, SessionParamsMemberName, keySelector, sessionKey, sessionInit, sessionUpdate, ctx))
                        .With(0, "partitionedStream")
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        rowItems.push_back(ctx.MakeType<TItemExprType>(SessionParamsMemberName, sessionParamsType));
        addedColumns.push_back(ctx.NewAtom(pos, SessionParamsMemberName));

        originalKeysWithSession.push_back(ctx.NewAtom(pos, SessionStartMemberName));
        addedColumns.push_back(originalKeysWithSession.back());
        rowItems.push_back(ctx.MakeType<TItemExprType>(SessionStartMemberName, sessionKeyType));
        sessionKey = sessionInit = sessionUpdate = {};
    }

    TExprNodeList keyColumns;

    auto rowArg = ctx.NewArgument(pos, "row");
    auto addMembersBody = rowArg;

    static const TStringBuf KeyColumnNamePrefix = "_yql_CalcOverWindowJoinKey";

    const TStructExprType* rowTypeWithSession = ctx.MakeType<TStructExprType>(rowItems);
    for (auto& keyColumn : originalKeysWithSession) {
        YQL_ENSURE(keyColumn->IsAtom());
        auto columnName = keyColumn->Content();
        const TTypeAnnotationNode* columnType =
            rowTypeWithSession->GetItems()[*rowTypeWithSession->FindItem(columnName)]->GetItemType();
        if (columnType->HasOptionalOrNull()) {
            addedColumns.push_back(ctx.NewAtom(pos, TStringBuilder() << KeyColumnNamePrefix << addedColumns.size()));
            keyColumns.push_back(addedColumns.back());

            TStringBuf newName = addedColumns.back()->Content();
            const TTypeAnnotationNode* newType = ctx.MakeType<TDataExprType>(EDataSlot::String);
            rowItems.push_back(ctx.MakeType<TItemExprType>(newName, newType));

            addMembersBody = ctx.Builder(pos)
                .Callable("AddMember")
                    .Add(0, addMembersBody)
                    .Atom(1, newName)
                    .Callable(2, "StablePickle")
                        .Callable(0, "Member")
                            .Add(0, rowArg)
                            .Add(1, keyColumn)
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        } else {
            keyColumns.push_back(keyColumn);
        }
    }

    input = ctx.Builder(pos)
        .Callable("Map")
            .Add(0, input)
            .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, { rowArg }), std::move(addMembersBody)))
        .Seal()
        .Build();

    auto keySelector = BuildKeySelector(pos, *ctx.MakeType<TStructExprType>(rowItems),
        ctx.NewList(pos, TExprNodeList{keyColumns}), ctx);

    TExprNode::TPtr preprocessLambda;
    TExprNode::TPtr groupKeySelector;
    TExprNode::TPtr condenseSwitch;
    if (sessionUpdate) {
        YQL_ENSURE(sessionKey);
        YQL_ENSURE(sessionInit);
        YQL_ENSURE(sessionKeyType);
        YQL_ENSURE(commonSortTraits);

        preprocessLambda =
            AddSessionParamsMemberLambda(pos, SessionStartMemberName, SessionParamsMemberName, keySelector, sessionKey, sessionInit, sessionUpdate, ctx);
        rowItems.push_back(ctx.MakeType<TItemExprType>(SessionStartMemberName, sessionKeyType));
        rowItems.push_back(ctx.MakeType<TItemExprType>(SessionParamsMemberName, sessionParamsType));

        addedColumns.push_back(ctx.NewAtom(pos, SessionStartMemberName));
        addedColumns.push_back(ctx.NewAtom(pos, SessionParamsMemberName));

        if (sessionKeyType->HasOptionalOrNull()) {
            addedColumns.push_back(ctx.NewAtom(pos, TStringBuilder() << KeyColumnNamePrefix << addedColumns.size()));
            preprocessLambda = ctx.Builder(pos)
                .Lambda()
                    .Param("stream")
                    .Callable("OrderedMap")
                        .Apply(0, preprocessLambda)
                            .With(0, "stream")
                        .Seal()
                        .Lambda(1)
                            .Param("item")
                            .Callable("AddMember")
                                .Arg(0, "item")
                                .Add(1, addedColumns.back())
                                .Callable(2, "StablePickle")
                                    .Callable(0, "Member")
                                        .Arg(0, "item")
                                        .Atom(1, SessionStartMemberName)
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Build();

            TStringBuf newName = addedColumns.back()->Content();
            const TTypeAnnotationNode* newType = ctx.MakeType<TDataExprType>(EDataSlot::String);
            rowItems.push_back(ctx.MakeType<TItemExprType>(newName, newType));
        }

        keyColumns.push_back(addedColumns.back());

        auto groupKeySelector = BuildKeySelector(pos, *ctx.MakeType<TStructExprType>(rowItems),
            ctx.NewList(pos, TExprNodeList{keyColumns}), ctx);

        condenseSwitch = ctx.Builder(pos)
            .Lambda()
                .Param("row")
                .Param("state")
                .Callable("AggrNotEquals")
                    .Apply(0, groupKeySelector)
                        .With(0, "row")
                    .Seal()
                    .Apply(1, groupKeySelector)
                        .With(0, "state")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    } else {
        YQL_ENSURE(!sessionKey);
        preprocessLambda = MakeIdentityLambda(pos, ctx);
        auto groupKeySelector = keySelector;

        condenseSwitch = ctx.Builder(pos)
            .Lambda()
                .Param("row")
                .Param("state")
                .Callable("IsKeySwitch")
                    .Arg(0, "row")
                    .Arg(1, "state")
                    .Add(2, groupKeySelector)
                    .Add(3, groupKeySelector)
                .Seal()
            .Seal()
            .Build();
    }

    auto partitionByKeysLambda = ctx.Builder(pos)
        .Lambda()
            .Param("stream")
            .Callable("Map")
                .Callable(0, "Condense1")
                    .Apply(0, preprocessLambda)
                        .With(0, "stream")
                    .Seal()
                    .Add(1, BuildFold1Lambda(pos, frames, EFold1LambdaKind::INIT, keyColumns, *rowType, ctx))
                    .Add(2, condenseSwitch)
                    .Add(3, BuildFold1Lambda(pos, frames, EFold1LambdaKind::UPDATE, keyColumns, *rowType, ctx))
                .Seal()
                .Add(1, BuildFold1Lambda(pos, frames, EFold1LambdaKind::CALCULATE, keyColumns, *rowType, ctx))
            .Seal()
        .Seal()
        .Build();

    if (HasContextFuncs(*partitionByKeysLambda)) {
        partitionByKeysLambda = ctx.Builder(pos)
            .Lambda()
                .Param("stream")
                .Callable("WithContext")
                    .Apply(0, partitionByKeysLambda)
                        .With(0, "stream")
                    .Seal()
                    .Atom(1, "WinAgg", TNodeFlags::Default)
                .Seal()
            .Seal()
            .Build();
    }

    auto aggregated = ctx.Builder(pos)
        .Callable("PartitionsByKeys")
            .Add(0, input)
            .Add(1, keySelector)
            .Add(2, sortOrder)
            .Add(3, sortKey)
            .Add(4, partitionByKeysLambda)
        .Seal().Build();

    if (sessionUpdate) {
        // preprocess input without aggregation
        input = ctx.Builder(pos)
                .Callable("PartitionsByKeys")
                    .Add(0, input)
                    .Add(1, ctx.DeepCopyLambda(*keySelector))
                    .Add(2, sortOrder)
                    .Add(3, ctx.DeepCopyLambda(*sortKey))
                    .Lambda(4)
                        .Param("stream")
                        .Apply(preprocessLambda)
                            .With(0, "stream")
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
    }

    TExprNode::TPtr joined;
    if (!keyColumns.empty()) {
        // SELECT * FROM input AS a JOIN aggregated AS b USING(keyColumns)
        auto buildJoinKeysTuple = [&](TStringBuf side) {
            TExprNodeList items;
            for (const auto& keyColumn : keyColumns) {
                items.push_back(ctx.NewAtom(pos, side));
                items.push_back(keyColumn);
            }
            return ctx.NewList(pos, std::move(items));
        };

        joined = ctx.Builder(pos)
            .Callable("EquiJoin")
                .List(0)
                    .Add(0, input)
                    .Atom(1, "a", TNodeFlags::Default)
                .Seal()
                .List(1)
                    .Add(0, aggregated)
                    .Atom(1, "b", TNodeFlags::Default)
                .Seal()
                .List(2)
                    .Atom(0, "Inner", TNodeFlags::Default)
                    .Atom(1, "a", TNodeFlags::Default)
                    .Atom(2, "b", TNodeFlags::Default)
                    .Add(3, buildJoinKeysTuple("a"))
                    .Add(4, buildJoinKeysTuple("b"))
                    .List(5)
                        .List(0)
                            .Atom(0, "right", TNodeFlags::Default)
                            .Atom(1, "any", TNodeFlags::Default)
                        .Seal()
                    .Seal()
                .Seal()
                .List(3).Seal()
            .Seal()
            .Build();

        // remove b.keys*
        auto rowArg = ctx.NewArgument(pos, "row");

        TExprNode::TPtr removed = rowArg;

        auto removeSide = [&](const TString& side, const TExprNodeList& keys) {
            for (const auto& keyColumn : keys) {
                YQL_ENSURE(keyColumn->IsAtom());
                TString toRemove = side + keyColumn->Content();
                removed = ctx.Builder(pos)
                    .Callable("RemoveMember")
                        .Add(0, removed)
                        .Atom(1, toRemove)
                    .Seal()
                    .Build();
            }
        };

        removeSide("b.", keyColumns);

        // add session columns
        for (auto column : sessionColumns->ChildrenList()) {
            removed = ctx.Builder(pos)
                .Callable("AddMember")
                    .Add(0, removed)
                    .Add(1, column)
                    .Callable(2, "Member")
                        .Add(0, rowArg)
                        .Atom(1, TString("a.") + SessionParamsMemberName)
                    .Seal()
                .Seal()
                .Build();
        }

        removeSide("a.", addedColumns);

        joined = ctx.Builder(pos)
            .Callable("Map")
                .Add(0, joined)
                .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, {rowArg}), std::move(removed)))
            .Seal()
            .Build();
    } else {
        // SELECT * FROM input AS a CROSS JOIN aggregated AS b
        joined = ctx.Builder(pos)
            .Callable("EquiJoin")
                .List(0)
                    .Add(0, input)
                    .Atom(1, "a", TNodeFlags::Default)
                .Seal()
                .List(1)
                    .Add(0, aggregated)
                    .Atom(1, "b", TNodeFlags::Default)
                .Seal()
                .List(2)
                    .Atom(0, "Cross", TNodeFlags::Default)
                    .Atom(1, "a", TNodeFlags::Default)
                    .Atom(2, "b", TNodeFlags::Default)
                    .List(3).Seal()
                    .List(4).Seal()
                    .List(5).Seal()
                .Seal()
                .List(3).Seal()
            .Seal()
            .Build();
    }

    return ctx.Builder(pos)
        .Callable("Map")
            .Add(0, joined)
            .Lambda(1)
                .Param("row")
                .Callable("DivePrefixMembers")
                    .Arg(0, "row")
                    .List(1)
                        .Atom(0, "a.")
                        .Atom(1, "b.")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr TryExpandNonCompactFullFrames(TPositionHandle pos, const TExprNode::TPtr& inputList, const TExprNode::TPtr& keyColumns,
    const TExprNode::TPtr& sortTraits, const TExprNode::TPtr& frames, const TExprNode::TPtr& sessionTraits,
    const TExprNode::TPtr& sessionColumns, TExprContext& ctx)
{
    TExprNodeList nonCompactAggregatingFullFrames;
    TExprNodeList otherFrames;

    for (auto& winOn : frames->ChildrenList()) {
        if (!IsNonCompactFullFrame(*winOn, ctx)) {
            otherFrames.push_back(winOn);
            continue;
        }

        YQL_ENSURE(TCoWinOnBase::Match(winOn.Get()));

        TExprNodeList nonAggregates = { winOn->ChildPtr(0) };
        TExprNodeList aggregates = { winOn->ChildPtr(0) };

        for (ui32 i = 1; i < winOn->ChildrenSize(); ++i) {
            auto item = winOn->Child(i)->Child(1);
            if (item->IsCallable("WindowTraits")) {
                aggregates.push_back(winOn->ChildPtr(i));
            } else {
                nonAggregates.push_back(winOn->ChildPtr(i));
            }
        }

        if (aggregates.size() == 1) {
            otherFrames.push_back(winOn);
            continue;
        }

        nonCompactAggregatingFullFrames.push_back(ctx.ChangeChildren(*winOn, std::move(aggregates)));
        if (nonAggregates.size() > 1) {
            otherFrames.push_back(ctx.ChangeChildren(*winOn, std::move(nonAggregates)));
        }
    }

    if (nonCompactAggregatingFullFrames.empty()) {
        return {};
    }

    auto fullFrames = ctx.NewList(pos, std::move(nonCompactAggregatingFullFrames));
    auto nonFullFrames = ctx.NewList(pos, std::move(otherFrames));
    auto expanded = ExpandNonCompactFullFrames(pos, inputList, keyColumns, sortTraits, fullFrames, sessionTraits, sessionColumns, ctx);

    if (sessionTraits && !sessionTraits->IsCallable("Void")) {
        return Build<TCoCalcOverSessionWindow>(ctx, pos)
            .Input(expanded)
            .Keys(keyColumns)
            .SortSpec(sortTraits)
            .Frames(nonFullFrames)
            .SessionSpec(sessionTraits)
            .SessionColumns(sessionColumns)
            .Done().Ptr();
    }
    YQL_ENSURE(sessionColumns->ChildrenSize() == 0);
    return Build<TCoCalcOverWindow>(ctx, pos)
        .Input(expanded)
        .Keys(keyColumns)
        .SortSpec(sortTraits)
        .Frames(nonFullFrames)
        .Done().Ptr();
}

struct TSplitResult {
    TExprNode::TPtr Rows;
    TExprNode::TPtr NonNumericRanges;
    TExprNode::TPtr NumericRangesAndRows;
};

TSplitResult SplitFramesByType(const TExprNode::TPtr& frames, TExprContext& ctx, TTypeAnnotationContext& typeCtx) {
    TExprNodeList nonNumericRanges;
    TExprNodeList numbericRangesAndRows;
    for (auto& winOn : frames->ChildrenList()) {
        if (TCoWinOnRows::Match(winOn.Get())) {
            numbericRangesAndRows.push_back(std::move(winOn));
        } else if (TCoWinOnRange::Match(winOn.Get())) {
            auto settings = TWindowFrameSettings::Parse(*winOn, ctx);
            if (settings.GetRangeFrame().IsNumeric() && IsRangeWindowFrameEnabled(typeCtx)) {
                numbericRangesAndRows.push_back(std::move(winOn));
            } else {
                nonNumericRanges.push_back(std::move(winOn));
            }
        } else {
            YQL_ENSURE(TCoWinOnGroups::Match(winOn.Get()));
            YQL_ENSURE(0, "Unexpected WinOnGroups.");
        }
    }

    return TSplitResult {
        .NonNumericRanges = ctx.NewList(frames->Pos(), std::move(nonNumericRanges)),
        .NumericRangesAndRows = ctx.NewList(frames->Pos(), std::move(numbericRangesAndRows))
    };
}

ESortOrder ExtractAndVerifyRangeSortOrder(const TExprNode::TPtr& frames, TExprContext& ctx) {
    TMaybe<ESortOrder> sortOrder;
    for (auto& winOn : frames->ChildrenList()) {
        if (TCoWinOnRange::Match(winOn.Get())) {
            auto settings = TWindowFrameSettings::Parse(*winOn, ctx);
            if (settings.GetFrameType() != EFrameType::FrameByRange) {
                continue;
            }
            auto currentSortOrder = settings.GetRangeFrame().GetSortOrder();
            if (!sortOrder) {
                sortOrder = currentSortOrder;
            } else {
                YQL_ENSURE(*sortOrder == currentSortOrder, "All Range frames must have the same SortOrder");
            }
        }
    }

    return sortOrder.GetOrElse(ESortOrder::Unimportant);
}

const TStructExprType* ApplyFramesToType(const TStructExprType& inputType, const TStructExprType& finalOutputType, const TExprNode& frames, TExprContext& ctx) {
    TVector<const TItemExprType*> resultItems = inputType.GetItems();
    for (auto& frame : frames.ChildrenList()) {
        YQL_ENSURE(TCoWinOnBase::Match(frame.Get()));
        for (size_t i = 1; i < frame->ChildrenSize(); ++i) {
            YQL_ENSURE(frame->Child(i)->IsList());
            YQL_ENSURE(frame->Child(i)->Head().IsAtom());
            TStringBuf column = frame->Child(i)->Head().Content();

            const TTypeAnnotationNode* type = finalOutputType.FindItemType(column);
            YQL_ENSURE(type);

            resultItems.push_back(ctx.MakeType<TItemExprType>(column, type));
        }
    }

    return ctx.MakeType<TStructExprType>(resultItems);
}

bool NeedPartitionRows(const TExprNode::TPtr& frames, const TStructExprType& rowType, TExprContext& ctx) {
    if (frames->ChildrenSize() == 0) {
        return false;
    }

    TCalcOverWindowTraits traits = ExtractCalcOverWindowTraits(frames, rowType, ctx);
    for (const auto& item : traits.RawTraits) {
        const TRawTrait& trait = item.second;
        if (trait.CalculateLambda->IsCallable({"CumeDist","NTile","PercentRank"})) {
            return true;
        }
    }

    return false;
}

TString AllocatePartitionRowsColumn(const TStructExprType& rowType) {
    ui64 index = 0;
    for (;;) {
        auto name = "_yql_partition_rows_" + ToString(index);
        if (!rowType.FindItemType(name)) {
            return name;
        }

        ++index;
    }
}

TExprNode::TPtr AddPartitionRowsColumn(TPositionHandle pos, const TExprNode::TPtr& input, const TExprNode::TPtr& keyColumns,
    const TString& columnName, TExprContext& ctx, TTypeAnnotationContext& types) {
    auto exportsPtr = types.Modules->GetModule("/lib/yql/window.yql");
    YQL_ENSURE(exportsPtr);
    const auto& exports = exportsPtr->Symbols();
    const auto ex = exports.find("count_traits_factory");
    YQL_ENSURE(exports.cend() != ex);
    TNodeOnNodeOwnedMap deepClones;
    auto lambda = ctx.DeepCopy(*ex->second, exportsPtr->ExprCtx(), deepClones, true, false);
    auto listTypeNode = ctx.NewCallable(pos, "TypeOf", {input});
    auto extractor = ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .Callable("Void")
            .Seal()
        .Seal()
        .Build();

    auto traits = ctx.ReplaceNodes(lambda->TailPtr(), {
        {lambda->Head().Child(0), listTypeNode},
        {lambda->Head().Child(1), extractor}
    });

    ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
    auto status = ExpandApplyNoRepeat(traits, traits, ctx);
    YQL_ENSURE(status != IGraphTransformer::TStatus::Error);

    return ctx.Builder(pos)
        .Callable("CalcOverWindow")
            .Add(0, input)
            .Add(1, keyColumns)
            .Callable(2, "Void")
            .Seal()
            .List(3)
                .Callable(0, "WinOnRows")
                    .List(0)
                        .List(0)
                            .Atom(0, "begin")
                            .Callable(1, "Void")
                            .Seal()
                        .Seal()
                        .List(1)
                            .Atom(0, "end")
                            .Callable(1, "Void")
                            .Seal()
                        .Seal()
                        .List(2)
                            .Atom(0, "sortSpec")
                            .Callable(1, "Void")
                            .Seal()
                        .Seal()
                    .Seal()
                    .List(1)
                        .Atom(0, columnName)
                        .Add(1, traits)
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr RemoveRowsColumn(TPositionHandle pos, const TExprNode::TPtr& input, const TString& columnName, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("Map")
            .Add(0, input)
            .Lambda(1)
                .Param("row")
                .Callable("RemoveMember")
                    .Arg(0, "row")
                    .Atom(1, columnName)
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr ProccessAllIncrementalShifts(TPositionHandle pos,
                                             const TExprNode::TPtr& stream,
                                             const TExprNodeCoreWinFrameCollectorBounds& incrementalBounds,
                                             TExprNode::TPtr streamDependsOn,
                                             const TVector<TChain1MapTraits::TPtr>& traits,
                                             TMaybe<ESortOrder> sortOrder,
                                             TExprContext& ctx) {
    TExprNodeCoreWinFrameCollectorParams params(incrementalBounds.AsBase(), sortOrder.GetOrElse(ESortOrder::Unimportant), TString(SortedColumnMemberName));
    auto processedItemType = ctx.Builder(pos)
        .Callable("StreamItemType")
            .Callable(0, "TypeOf")
                .Add(0, stream)
            .Seal()
        .Seal()
        .Build();
    auto WinFramesCollectorResult = BuildWinFramesCollector(pos, stream, processedItemType, params, streamDependsOn, ctx);
    auto arg = ctx.NewArgument(pos, "row");

    auto body = HandleIncrementalOutput(pos, arg, traits, WinFramesCollectorResult.Queue, ctx);
    auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {arg}), std::move(body));

    return ctx.Builder(pos)
                    .Callable("OrderedMap")
                        .Add(0, WinFramesCollectorResult.WinFramesCollector)
                        .Add(1, lambda)
                    .Seal()
                    .Build();
}

TExprNode::TPtr ProcessRowsAndNumericRangeFrames(TPositionHandle pos,
                                                 const TExprNode::TPtr& input,
                                                 const TStructExprType& rowType,
                                                 const TExprNode::TPtr& dependsOn,
                                                 const TExprNode::TPtr& frames,
                                                 const TMaybe<TString>& partitionRowsColumn,
                                                 TMaybe<ESortOrder> sortOrder,
                                                 TExprContext& ctx,
                                                 TTypeAnnotationContext& typeCtx)
{
    if (frames->ChildrenSize() == 0) {
        return input;
    }
    TExprNode::TPtr processed = input;
    TExprNode::TPtr dataQueue;
    TQueueParams queueParams;
    // Deduplicate all same bounds.
    TExprNodeCoreWinFrameCollectorBounds bounds(/*dedup=*/true);
    TExprNodeCoreWinFrameCollectorBounds incrementalBounds(/*dedup=*/true);
    TVector<TChain1MapTraits::TPtr> traits = BuildFoldMapTraitsForRowsAndNumericRanges(queueParams, bounds, incrementalBounds, frames, partitionRowsColumn, rowType, ctx, typeCtx);

    if (IsWindowNewPipelineEnabled(typeCtx)) {
        if (!bounds.Empty()) {
            TExprNodeCoreWinFrameCollectorParams params(bounds.AsBase(), sortOrder.GetOrElse(ESortOrder::Unimportant), TString(SortedColumnMemberName));
            auto WinFramesCollectorResult = BuildWinFramesCollector(pos, processed, rowType, params, dependsOn, ctx);
            dataQueue = WinFramesCollectorResult.Queue;
            processed = WinFramesCollectorResult.WinFramesCollector;
        }
    } else {
        YQL_ENSURE(bounds.Empty(), "Bounds should be filled only inside new pipeline.");
        if (queueParams.DataQueueNeeded) {
            ui64 queueSize = (queueParams.DataOutpace == Max<ui64>()) ? Max<ui64>() : (queueParams.DataOutpace + queueParams.DataLag + 2);
            dataQueue = BuildQueue(pos, rowType, queueSize, queueParams.DataLag, dependsOn, ctx);
            processed = ctx.Builder(pos)
                .Callable("PreserveStream")
                    .Add(0, processed)
                    .Add(1, dataQueue)
                    .Add(2, BuildUint64(pos, queueParams.DataOutpace, ctx))
                .Seal()
                .Build();
        }
    }

    bool haveLagQueue = !IsWindowNewPipelineEnabled(typeCtx) && queueParams.LagQueueSize != 0;
    ui64 lagQueueSize = IsWindowNewPipelineEnabled(typeCtx) ? 0: queueParams.LagQueueSize;

    processed = ctx.Builder(pos)
        .Callable("OrderedMap")
            .Callable(0, "Chain1Map")
                .Add(0, std::move(processed))
                .Add(1, BuildChain1MapInitLambda(pos, traits, dataQueue, lagQueueSize, queueParams.LagQueueItemType, ctx, typeCtx))
            .Add(2, BuildChain1MapUpdateLambda(pos, traits, dataQueue, haveLagQueue, ctx, typeCtx))
            .Seal()
            .Lambda(1)
                .Param("pair")
                .Callable("Nth")
                    .Arg(0, "pair")
                    .Atom(1, "0", TNodeFlags::Default)
                .Seal()
            .Seal()
        .Seal()
        .Build();

    if (IsWindowNewPipelineEnabled(typeCtx)) {
        if (!incrementalBounds.Empty()) {
            processed = ProccessAllIncrementalShifts(pos, processed, incrementalBounds, dependsOn, traits, sortOrder, ctx);
        }
    } else {
        YQL_ENSURE(incrementalBounds.Empty(), "Incremental bounds should be filled only inside new pipeline.");
    }

    return WrapWithWinContext(processed, ctx);
}

TExprNode::TPtr ProcessRangeNonNumericFrames(TPositionHandle pos,
                                             const TExprNode::TPtr& input,
                                             const TStructExprType& rowType,
                                             const TExprNode::TPtr& sortKey,
                                             const TExprNode::TPtr& frames,
                                             const TMaybe<TString>& partitionRowsColumn,
                                             TExprContext& ctx,
                                             TTypeAnnotationContext& typeCtx) {
    if (frames->ChildrenSize() == 0) {
        return input;
    }

    TExprNode::TPtr processed = input;
    TVector<TChain1MapTraits::TPtr> traits = BuildFoldMapTraitsForNonNumericRange(frames, rowType,  partitionRowsColumn, ctx);

    // same processing as in WinOnRows
    processed = ctx.Builder(pos)
        .Callable("OrderedMap")
            .Callable(0, "Chain1Map")
                .Add(0, std::move(processed))
                .Add(1, BuildChain1MapInitLambda(pos, traits, nullptr, 0, nullptr, ctx, typeCtx))
                .Add(2, BuildChain1MapUpdateLambda(pos, traits, nullptr, false, ctx, typeCtx))
            .Seal()
            .Lambda(1)
                .Param("pair")
                .Callable("Nth")
                    .Arg(0, "pair")
                    .Atom(1, "0", TNodeFlags::Default)
                .Seal()
            .Seal()
        .Seal()
        .Build();
    processed = WrapWithWinContext(processed, ctx);

    TExprNode::TPtr sortKeyLambda = sortKey;
    if (sortKey->IsCallable("Void")) {
        sortKeyLambda = ctx.Builder(sortKey->Pos())
            .Lambda()
                .Param("row")
                .Callable("Void")
                .Seal()
            .Seal()
            .Build();
    }

    auto processedItemType = ctx.Builder(pos)
        .Callable("StreamItemType")
            .Callable(0, "TypeOf")
                .Add(0, processed)
            .Seal()
        .Seal()
        .Build();

    auto variantType = ctx.Builder(pos)
        .Callable("VariantType")
            .Callable(0, "StructType")
                .List(0)
                    .Atom(0, "singleRow", TNodeFlags::Default)
                    .Add(1, processedItemType)
                .Seal()
                .List(1)
                    .Atom(0, "group", TNodeFlags::Default)
                    .Callable(1, "ListType")
                        .Add(0, processedItemType)
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    // split rows by groups with equal sortKey
    processed = ctx.Builder(pos)
        .Callable("Condense1")
            .Add(0, processed)
            .Lambda(1)
                .Param("row")
                .List()
                    .Apply(0, sortKeyLambda)
                        .With(0, "row")
                    .Seal()
                    .Callable(1, "Variant")
                        .Arg(0, "row")
                        .Atom(1, "singleRow", TNodeFlags::Default)
                        .Add(2, variantType)
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(2)
                .Param("row")
                .Param("state")
                .Callable(0, "AggrNotEquals")
                    .Apply(0, sortKeyLambda)
                        .With(0, "row")
                    .Seal()
                    .Callable(1, "Nth")
                        .Arg(0, "state")
                        .Atom(1, "0", TNodeFlags::Default)
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(3)
                .Param("row")
                .Param("state")
                .List()
                    .Callable(0, "Nth")
                        .Arg(0, "state")
                        .Atom(1, "0", TNodeFlags::Default)
                    .Seal()
                    .Callable(1, "Visit")
                        .Callable(0, "Nth")
                            .Arg(0, "state")
                            .Atom(1, "1", TNodeFlags::Default)
                        .Seal()
                        .Atom(1, "singleRow", TNodeFlags::Default)
                        .Lambda(2)
                            .Param("singleRow")
                            .Callable(0, "Variant")
                                .Callable(0, "AsList")
                                    .Arg(0, "singleRow")
                                    .Arg(1, "row")
                                .Seal()
                                .Atom(1, "group", TNodeFlags::Default)
                                .Add(2, variantType)
                            .Seal()
                        .Seal()
                        .Atom(3, "group", TNodeFlags::Default)
                        .Lambda(4)
                            .Param("group")
                            .Callable(0, "Variant")
                                .Callable(0, "Insert")
                                    .Arg(0, "group")
                                    .Arg(1, "row")
                                .Seal()
                                .Atom(1, "group", TNodeFlags::Default)
                                .Add(2, variantType)
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    processed = ctx.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, processed)
            .Lambda(1)
                .Param("item")
                .Callable(0, "Nth")
                    .Arg(0, "item")
                    .Atom(1, "1", TNodeFlags::Default)
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto lastRowArg = ctx.NewArgument(pos, "lastRow");
    auto currentRowArg = ctx.NewArgument(pos, "currentRow");
    auto currentRow = currentRowArg;

    for (auto& trait : traits) {
        TStringBuf name = trait->GetName();
        currentRow = ctx.Builder(pos)
            .Callable("AddMember")
                .Callable(0, "RemoveMember")
                    .Add(0, currentRow)
                    .Atom(1, name)
                .Seal()
                .Atom(1, name)
                .Callable(2, "Member")
                    .Add(0, lastRowArg)
                    .Atom(1, name)
                .Seal()
            .Seal()
            .Build();
    }

    auto overwriteWithLastRowLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { currentRowArg, lastRowArg }), std::move(currentRow));

    // processed is currently stream of groups (=Variant<row, List<row>>>) with equal sort keys
    processed = ctx.Builder(pos)
        .Callable("OrderedFlatMap")
            .Add(0, processed)
            .Lambda(1)
                .Param("item")
                .Callable("Visit")
                    .Arg(0, "item")
                    .Atom(1, "singleRow", TNodeFlags::Default)
                    .Lambda(2)
                        .Param("singleRow")
                        .Callable(0, "AsList")
                            .Arg(0, "singleRow")
                        .Seal()
                    .Seal()
                    .Atom(3, "group", TNodeFlags::Default)
                    .Lambda(4)
                        .Param("group")
                        .Callable("Coalesce")
                            .Callable(0, "Map")
                                .Callable(0, "Last")
                                    .Arg(0, "group")
                                .Seal()
                                .Lambda(1)
                                    .Param("lastRow")
                                    .Callable("OrderedMap")
                                        .Arg(0, "group")
                                        .Lambda(1)
                                            .Param("currentRow")
                                            .Apply(overwriteWithLastRowLambda)
                                                .With(0, "currentRow")
                                                .With(1, "lastRow")
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Callable(1, "EmptyList")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return processed;
}

TExprNode::TPtr ExpandSingleCalcOverWindow(TPositionHandle pos,
                                           const TExprNode::TPtr& inputList,
                                           const TExprNode::TPtr& keyColumns,
                                           const TExprNode::TPtr& sortTraits,
                                           const TExprNode::TPtr& frames,
                                           const TExprNode::TPtr& sessionTraits,
                                           const TExprNode::TPtr& sessionColumns,
                                           const TStructExprType& outputRowType,
                                           TExprContext& ctx,
                                           TTypeAnnotationContext& types) {
    if (auto expanded = TryExpandNonCompactFullFrames(pos, inputList, keyColumns, sortTraits, frames, sessionTraits, sessionColumns, ctx)) {
        YQL_CLOG(INFO, Core) << "Expanded non-compact CalcOverWindow";
        return expanded;
    }

    TExprNode::TPtr sessionKey;
    TExprNode::TPtr sessionSortTraits;
    const TTypeAnnotationNode* sessionKeyType = nullptr;
    const TTypeAnnotationNode* sessionParamsType = nullptr;
    TExprNode::TPtr sessionInit;
    TExprNode::TPtr sessionUpdate;
    ExtractSessionWindowParams(pos, sessionTraits, sessionKey, sessionKeyType, sessionParamsType, sessionSortTraits, sessionInit, sessionUpdate, ctx);
    auto splitResult = SplitFramesByType(frames, ctx, types);
    auto sortOrderForNumeric = ExtractAndVerifyRangeSortOrder(splitResult.NumericRangesAndRows, ctx);
    const auto originalRowType = inputList->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    TVector<const TItemExprType*> rowItems = originalRowType->GetItems();
    if (sessionKeyType) {
        YQL_ENSURE(sessionParamsType);
        rowItems.push_back(ctx.MakeType<TItemExprType>(SessionStartMemberName, sessionKeyType));
        rowItems.push_back(ctx.MakeType<TItemExprType>(SessionParamsMemberName, sessionParamsType));
    }
    if (ShouldAddSortedColumn(sortOrderForNumeric)) {
        rowItems.push_back(GetSortedColumnType(sortTraits, ctx));
    }
    auto rowType = ctx.MakeType<TStructExprType>(rowItems);

    auto keySelector = BuildKeySelector(pos, *rowType->Cast<TStructExprType>(), keyColumns, ctx);

    TExprNode::TPtr sortKey;
    TExprNode::TPtr sortOrder;
    ExtractSortKeyAndOrder(pos, sortTraits, sortKey, sortOrder, ctx);
    const TExprNode::TPtr originalSortKey = sortKey;
    TExprNode::TPtr input = inputList;

    const auto commonSortTraits = DeduceCompatibleSort(sortTraits, sessionSortTraits);
    ExtractSortKeyAndOrder(pos, commonSortTraits ? commonSortTraits : sortTraits, sortKey, sortOrder, ctx);
    auto fullKeyColumns = keyColumns;
    if (!commonSortTraits) {
        YQL_ENSURE(sessionKey);
        YQL_ENSURE(sessionInit);
        YQL_ENSURE(sessionUpdate);
        TExprNode::TPtr sessionSortKey;
        TExprNode::TPtr sessionSortOrder;
        ExtractSortKeyAndOrder(pos, sessionSortTraits, sessionSortKey, sessionSortOrder, ctx);
        input = ctx.Builder(pos)
            .Callable("PartitionsByKeys")
                .Add(0, input)
                .Add(1, keySelector)
                .Add(2, sessionSortOrder)
                .Add(3, sessionSortKey)
                .Lambda(4)
                    .Param("partitionedStream")
                    .Apply(AddSessionParamsMemberLambda(pos, SessionStartMemberName, SessionParamsMemberName, keySelector, sessionKey, sessionInit, sessionUpdate, ctx))
                        .With(0, "partitionedStream")
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        TExprNodeList keyColumnsList = keyColumns->ChildrenList();
        keyColumnsList.push_back(ctx.NewAtom(pos, SessionStartMemberName));

        auto keyColumnsWithSessionStart = ctx.NewList(pos, std::move(keyColumnsList));
        fullKeyColumns = keyColumnsWithSessionStart;

        keySelector = BuildKeySelector(pos, *rowType, keyColumnsWithSessionStart, ctx);
        sessionKey = sessionInit = sessionUpdate = {};
    }

    auto topLevelStreamArg = ctx.NewArgument(pos, "stream");
    TExprNode::TPtr processed = topLevelStreamArg;

    TMaybe<TString> partitionRowsColumn;
    if (NeedPartitionRows(frames, *rowType, ctx)) {
        partitionRowsColumn = AllocatePartitionRowsColumn(outputRowType);
        input = AddPartitionRowsColumn(pos, input, fullKeyColumns, *partitionRowsColumn, ctx, types);
    }

    // All RANGE frames (even simplest RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    // will require additional memory to store TableRow()'s - so we want to start with minimum size of row
    // (i.e. process range frames first)
    processed = ProcessRangeNonNumericFrames(pos, processed, *rowType, originalSortKey, splitResult.NonNumericRanges, partitionRowsColumn, ctx, types);
    rowType = ApplyFramesToType(*rowType, outputRowType, *splitResult.NonNumericRanges, ctx);
    processed = ProcessRowsAndNumericRangeFrames(pos, processed, *rowType, topLevelStreamArg, splitResult.NumericRangesAndRows, partitionRowsColumn, sortOrderForNumeric, ctx, types);

    auto topLevelStreamProcessingLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {topLevelStreamArg}), std::move(processed));

    YQL_CLOG(INFO, Core) << "Expanded compact CalcOverWindow";
    auto res = BuildPartitionsByKeys(pos, input, keySelector, sortOrder, sortKey, topLevelStreamProcessingLambda, sessionKey,
        sessionInit, sessionUpdate, sessionColumns, ctx);


    if (ShouldAddSortedColumn(sortOrderForNumeric)) {
        res = PushSortedColumnInsideStream(res, ctx);
        res = RemoveRowsColumn(pos, res, TString(SortedColumnMemberName), ctx);
    }

    if (partitionRowsColumn) {
        res = RemoveRowsColumn(pos, res, *partitionRowsColumn, ctx);
    }

    return res;
}

} // namespace

TExprNode::TPtr ExpandCalcOverWindow(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    YQL_ENSURE(node->IsCallable({"CalcOverWindow", "CalcOverSessionWindow", "CalcOverWindowGroup"}));

    auto input = node->ChildPtr(0);
    auto calcs = ExtractCalcsOverWindow(node, ctx);
    if (calcs.empty()) {
        return input;
    }

    if (input->GetTypeAnn()->HasErrors()) {
        TErrorTypeVisitor errorVisitor(ctx);
        input->GetTypeAnn()->Accept(errorVisitor);
        return nullptr;
    }

    TCoCalcOverWindowTuple calc(calcs.front());
    if (calc.Frames().Size() != 0 || calc.SessionColumns().Size() != 0) {
        const TStructExprType& outputRowType = *node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        input = ExpandSingleCalcOverWindow(node->Pos(), input, calc.Keys().Ptr(), calc.SortSpec().Ptr(), calc.Frames().Ptr(),
            calc.SessionSpec().Ptr(), calc.SessionColumns().Ptr(), outputRowType, ctx, types);
    }

    calcs.erase(calcs.begin());
    return RebuildCalcOverWindowGroup(node->Pos(), input, calcs, ctx);
}

TExprNodeList ExtractCalcsOverWindow(const TExprNodePtr& node, TExprContext& ctx) {
    TExprNodeList result;
    if (auto maybeBase = TMaybeNode<TCoCalcOverWindowBase>(node)) {
        TCoCalcOverWindowBase self(maybeBase.Cast());
        TExprNode::TPtr sessionSpec;
        TExprNode::TPtr sessionColumns;
        if (auto session = TMaybeNode<TCoCalcOverSessionWindow>(node)) {
            sessionSpec = session.Cast().SessionSpec().Ptr();
            sessionColumns = session.Cast().SessionColumns().Ptr();
        } else {
            sessionSpec = ctx.NewCallable(node->Pos(), "Void", {});
            sessionColumns = ctx.NewList(node->Pos(), {});
        }
        result.emplace_back(
            Build<TCoCalcOverWindowTuple>(ctx, node->Pos())
                .Keys(self.Keys())
                .SortSpec(self.SortSpec())
                .Frames(self.Frames())
                .SessionSpec(sessionSpec)
                .SessionColumns(sessionColumns)
                .Done().Ptr()
        );
    } else {
        result = TMaybeNode<TCoCalcOverWindowGroup>(node).Cast().Calcs().Ref().ChildrenList();
    }
    return result;
}

TExprNode::TPtr RebuildCalcOverWindowGroup(TPositionHandle pos, const TExprNode::TPtr& input, const TExprNodeList& calcs, TExprContext& ctx) {
    auto inputType = ctx.Builder(input->Pos())
        .Callable("TypeOf")
            .Add(0, input)
        .Seal()
        .Build();

    auto inputItemType = ctx.Builder(input->Pos())
        .Callable("ListItemType")
            .Add(0, inputType)
        .Seal()
        .Build();

    TExprNodeList fixedCalcs;
    for (auto calcNode : calcs) {
        TCoCalcOverWindowTuple calc(calcNode);
        auto sortSpec = calc.SortSpec().Ptr();
        if (sortSpec->IsCallable("SortTraits")) {
            sortSpec = ctx.Builder(sortSpec->Pos())
                .Callable("SortTraits")
                    .Add(0, inputType)
                    .Add(1, sortSpec->ChildPtr(1))
                    .Add(2, ctx.DeepCopyLambda(*sortSpec->Child(2)))
                .Seal()
                .Build();
        } else {
            YQL_ENSURE(sortSpec->IsCallable("Void"));
        }

        auto sessionSpec = calc.SessionSpec().Ptr();
        if (sessionSpec->IsCallable("SessionWindowTraits")) {
            TCoSessionWindowTraits traits(sessionSpec);
            auto sessionSortSpec = traits.SortSpec().Ptr();
            if (auto maybeSort = TMaybeNode<TCoSortTraits>(sessionSortSpec)) {
                sessionSortSpec = Build<TCoSortTraits>(ctx, sessionSortSpec->Pos())
                    .ListType(inputType)
                    .SortDirections(maybeSort.Cast().SortDirections())
                    .SortKeySelectorLambda(ctx.DeepCopyLambda(maybeSort.Cast().SortKeySelectorLambda().Ref()))
                    .Done().Ptr();
            } else {
                YQL_ENSURE(sessionSortSpec->IsCallable("Void"));
            }

            sessionSpec = Build<TCoSessionWindowTraits>(ctx, traits.Pos())
                .ListType(inputType)
                .SortSpec(sessionSortSpec)
                .InitState(ctx.DeepCopyLambda(traits.InitState().Ref()))
                .UpdateState(ctx.DeepCopyLambda(traits.UpdateState().Ref()))
                .Calculate(ctx.DeepCopyLambda(traits.Calculate().Ref()))
                .Done().Ptr();
        } else {
            YQL_ENSURE(sessionSpec->IsCallable("Void"));
        }

        auto sessionColumns = calc.SessionColumns().Ptr();

        TExprNodeList newFrames;
        for (auto frameNode : calc.Frames().Ref().Children()) {
            YQL_ENSURE(TCoWinOnBase::Match(frameNode.Get()));
            TExprNodeList winOnArgs = { frameNode->ChildPtr(0) };
            for (ui32 i = 1; i < frameNode->ChildrenSize(); ++i) {
                auto kvTuple = frameNode->ChildPtr(i);
                YQL_ENSURE(kvTuple->IsList());
                YQL_ENSURE(2 <= kvTuple->ChildrenSize() && kvTuple->ChildrenSize() <= 3);

                auto columnName = kvTuple->ChildPtr(0);

                auto traits = kvTuple->ChildPtr(1);
                YQL_ENSURE(traits->IsCallable({"Lag", "Lead", "RowNumber", "Rank", "DenseRank", "WindowTraits", "PercentRank", "CumeDist", "NTile"}));
                if (traits->IsCallable("WindowTraits")) {
                    bool isDistinct = kvTuple->ChildrenSize() == 3;
                    if (!isDistinct) {
                        YQL_ENSURE(traits->Head().GetTypeAnn());
                        const TTypeAnnotationNode& oldItemType = *traits->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                        traits = ctx.Builder(traits->Pos())
                            .Callable(traits->Content())
                                .Add(0, inputItemType)
                                .Add(1, ctx.DeepCopyLambda(*ReplaceFirstLambdaArgWithCastStruct(*traits->Child(1), oldItemType, ctx)))
                                .Add(2, ctx.DeepCopyLambda(*ReplaceFirstLambdaArgWithCastStruct(*traits->Child(2), oldItemType, ctx)))
                                .Add(3, ctx.DeepCopyLambda(*ReplaceFirstLambdaArgWithCastStruct(*traits->Child(3), oldItemType, ctx)))
                                .Add(4, ctx.DeepCopyLambda(*traits->Child(4)))
                                .Add(5, traits->Child(5)->IsLambda() ? ctx.DeepCopyLambda(*traits->Child(5)) : traits->ChildPtr(5))
                            .Seal()
                            .Build();
                    }
                } else if (traits->IsCallable({"Lag", "Lead", "Rank", "DenseRank", "PercentRank"})) {
                    YQL_ENSURE(traits->Head().GetTypeAnn());
                    const TTypeAnnotationNode& oldItemType = *traits->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType()
                        ->Cast<TListExprType>()->GetItemType();
                    traits = ctx.ChangeChild(*traits, 1, ctx.DeepCopyLambda(*ReplaceFirstLambdaArgWithCastStruct(*traits->Child(1), oldItemType, ctx)));
                }
                winOnArgs.push_back(ctx.ChangeChild(*kvTuple, 1, std::move(traits)));
            }
            newFrames.push_back(ctx.ChangeChildren(*frameNode, std::move(winOnArgs)));
        }

        fixedCalcs.push_back(
            Build<TCoCalcOverWindowTuple>(ctx, calc.Pos())
                .Keys(calc.Keys())
                .SortSpec(sortSpec)
                .Frames(ctx.NewList(calc.Frames().Pos(), std::move(newFrames)))
                .SessionSpec(sessionSpec)
                .SessionColumns(sessionColumns)
                .Done().Ptr()
        );
    }

    return Build<TCoCalcOverWindowGroup>(ctx, pos)
        .Input(input)
        .Calcs(ctx.NewList(pos, std::move(fixedCalcs)))
        .Done().Ptr();
}

bool IsUnbounded(const NNodes::TCoFrameBound& bound) {
    if (bound.Ref().ChildrenSize() < 2) {
        return false;
    }
    if (auto maybeAtom = bound.Bound().Maybe<TCoAtom>()) {
        return maybeAtom.Cast().Value() == "unbounded";
    }
    return false;
}

TExprNode::TPtr ZipWithSessionParamsLambda(TPositionHandle pos, const TExprNode::TPtr& partitionKeySelector,
    const TExprNode::TPtr& sessionKeySelector, const TExprNode::TPtr& sessionInit,
    const TExprNode::TPtr& sessionUpdate, TExprContext& ctx)
{
    auto extractTupleItem = [&](ui32 idx) {
        return ctx.Builder(pos)
            .Lambda()
                .Param("tuple")
                .Callable("Nth")
                    .Arg(0, "tuple")
                    .Atom(1, ToString(idx), TNodeFlags::Default)
                .Seal()
            .Seal()
            .Build();
    };

    auto initLambda = ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .List() // row, sessionKey, sessionState, partitionKey
                .Arg(0, "row")
                .Apply(1, sessionKeySelector)
                    .With(0, "row")
                    .With(1)
                        .Apply(sessionInit)
                            .With(0, "row")
                        .Seal()
                    .Done()
                .Seal()
                .Apply(2, sessionInit)
                    .With(0, "row")
                .Seal()
                .Apply(3, partitionKeySelector)
                    .With(0, "row")
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto newPartitionLambda = ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .Param("prevBigState")
            .Callable("AggrNotEquals")
                .Apply(0, partitionKeySelector)
                    .With(0, "row")
                .Seal()
                .Apply(1, partitionKeySelector)
                    .With(0)
                        .Apply(extractTupleItem(0))
                            .With(0, "prevBigState")
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto newSessionOrUpdatedStateLambda = [&](bool newSession) {
        return ctx.Builder(pos)
            .Lambda()
                .Param("row")
                .Param("prevBigState")
                .Apply(extractTupleItem(newSession ? 0 : 1))
                    .With(0)
                        .Apply(sessionUpdate)
                            .With(0, "row")
                            .With(1)
                                .Apply(extractTupleItem(2))
                                    .With(0, "prevBigState")
                                .Seal()
                            .Done()
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
            .Build();
    };

    return ctx.Builder(pos)
        .Lambda()
            .Param("input")
            .Callable("Chain1Map")
                .Arg(0, "input")
                .Add(1, initLambda)
                .Lambda(2)
                    .Param("row")
                    .Param("prevBigState")
                    .Callable("If")
                        .Apply(0, newPartitionLambda)
                            .With(0, "row")
                            .With(1, "prevBigState")
                        .Seal()
                        .Apply(1, initLambda)
                            .With(0, "row")
                        .Seal()
                        .List(2)
                            .Arg(0, "row")
                            .Callable(1, "If")
                                .Apply(0, newSessionOrUpdatedStateLambda(/* newSession = */ true))
                                    .With(0, "row")
                                    .With(1, "prevBigState")
                                .Seal()
                                .Apply(1, sessionKeySelector)
                                    .With(0, "row")
                                    .With(1)
                                        .Apply(newSessionOrUpdatedStateLambda(/* newSession = */ false))
                                            .With(0, "row")
                                            .With(1, "prevBigState")
                                        .Seal()
                                    .Done()
                                .Seal()
                                .Apply(2, extractTupleItem(1))
                                    .With(0, "prevBigState")
                                .Seal()
                            .Seal()
                            .Apply(2, newSessionOrUpdatedStateLambda(/* newSession = */ false))
                                .With(0, "row")
                                .With(1, "prevBigState")
                            .Seal()
                            .Apply(3, partitionKeySelector)
                                .With(0, "row")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr AddSessionParamsMemberLambda(TPositionHandle pos,
    TStringBuf sessionStartMemberName, const TExprNode::TPtr& partitionKeySelector,
    const TSessionWindowParams& sessionWindowParams, TExprContext& ctx)
{
    return AddSessionParamsMemberLambda(pos, sessionStartMemberName, "", partitionKeySelector,
        sessionWindowParams.Key, sessionWindowParams.Init, sessionWindowParams.Update, ctx);
}

TExprNode::TPtr AddSessionParamsMemberLambda(TPositionHandle pos,
    TStringBuf sessionStartMemberName, TStringBuf sessionParamsMemberName,
    const TExprNode::TPtr& partitionKeySelector,
    const TExprNode::TPtr& sessionKeySelector, const TExprNode::TPtr& sessionInit,
    const TExprNode::TPtr& sessionUpdate, TExprContext& ctx)
{
    YQL_ENSURE(sessionStartMemberName);
    TExprNode::TPtr addLambda = ctx.Builder(pos)
        .Lambda()
            .Param("tupleOfItemAndSessionParams")
            .Callable("AddMember")
                .Callable(0, "Nth")
                    .Arg(0, "tupleOfItemAndSessionParams")
                    .Atom(1, "0", TNodeFlags::Default)
                .Seal()
                .Atom(1, sessionStartMemberName)
                .Callable(2, "Nth")
                    .Arg(0, "tupleOfItemAndSessionParams")
                    .Atom(1, "1", TNodeFlags::Default)
                .Seal()
            .Seal()
        .Seal()
        .Build();

    if (sessionParamsMemberName) {
        addLambda = ctx.Builder(pos)
            .Lambda()
                .Param("tupleOfItemAndSessionParams")
                .Callable("AddMember")
                    .Apply(0, addLambda)
                        .With(0, "tupleOfItemAndSessionParams")
                    .Seal()
                    .Atom(1,  sessionParamsMemberName)
                    .Callable(2, "AsStruct")
                        .List(0)
                            .Atom(0, "start", TNodeFlags::Default)
                            .Callable(1, "Nth")
                                .Arg(0, "tupleOfItemAndSessionParams")
                                .Atom(1, "1", TNodeFlags::Default)
                            .Seal()
                        .Seal()
                        .List(1)
                            .Atom(0, "state", TNodeFlags::Default)
                            .Callable(1, "Nth")
                                .Arg(0, "tupleOfItemAndSessionParams")
                                .Atom(1, "2", TNodeFlags::Default)
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    return ctx.Builder(pos)
        .Lambda()
            .Param("input")
            .Callable("OrderedMap")
                .Apply(0, ZipWithSessionParamsLambda(pos, partitionKeySelector, sessionKeySelector, sessionInit, sessionUpdate, ctx))
                    .With(0, "input")
                .Seal()
                .Add(1, addLambda)
            .Seal()
        .Seal()
        .Build();
}

void TSessionWindowParams::Reset()
{
    Traits = {};
    Key = {};
    KeyType = nullptr;
    ParamsType = nullptr;
    Init = {};
    Update = {};
    SortTraits = {};
}

}
