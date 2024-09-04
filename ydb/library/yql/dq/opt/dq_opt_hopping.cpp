#include "dq_opt_hopping.h"

#include <ydb/library/yql/core/yql_aggregate_expander.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt_join.h>
#include <ydb/library/yql/dq/integration/yql_dq_optimization.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/optimizer.h>

#include <util/generic/bitmap.h>

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

struct THoppingTraits {
    TString Column;
    TCoHoppingTraits Traits;
    ui64 Hop;
    ui64 Interval;
    ui64 Delay;
};

 struct TKeysDescription {
    TVector<TString> PickleKeys;
    TVector<TString> MemberKeys;
    TVector<TString> FakeKeys;

    TKeysDescription(const TStructExprType& rowType, const TCoAtomList& keys, const TString& hoppingColumn) {
        for (const auto& key : keys) {
            if (key.StringValue() == hoppingColumn) {
                FakeKeys.emplace_back(key.StringValue());
                continue;
            }

            const auto index = rowType.FindItem(key.StringValue());
            Y_ENSURE(index);

            auto itemType = rowType.GetItems()[*index]->GetItemType();
            if (RemoveOptionalType(itemType)->GetKind() == ETypeAnnotationKind::Data) {
                MemberKeys.emplace_back(key.StringValue());
                continue;
            }

            PickleKeys.emplace_back(key.StringValue());
        }
    }

    TExprNode::TPtr BuildPickleLambda(TExprContext& ctx, TPositionHandle pos) const {
        TCoArgument arg = Build<TCoArgument>(ctx, pos)
            .Name("item")
            .Done();

        TExprBase body = arg;

        for (const auto& key : PickleKeys) {
            const auto member = Build<TCoMember>(ctx, pos)
                    .Name().Build(key)
                    .Struct(arg)
                .Done()
                .Ptr();

            body = Build<TCoReplaceMember>(ctx, pos)
                .Struct(body)
                .Name().Build(key)
                .Item(ctx.NewCallable(pos, "StablePickle", { member }))
                .Done();
        }

        return Build<TCoLambda>(ctx, pos)
            .Args({arg})
            .Body(body)
            .Done()
            .Ptr();
    }

    TExprNode::TPtr BuildUnpickleLambda(TExprContext& ctx, TPositionHandle pos, const TStructExprType& rowType) {
        TCoArgument arg = Build<TCoArgument>(ctx, pos)
            .Name("item")
            .Done();

        TExprBase body = arg;

        for (const auto& key : PickleKeys) {
            const auto index = rowType.FindItem(key);
            Y_ENSURE(index);

            auto itemType = rowType.GetItems().at(*index)->GetItemType();
            const auto member = Build<TCoMember>(ctx, pos)
                    .Name().Build(key)
                    .Struct(arg)
                .Done()
                .Ptr();

            body = Build<TCoReplaceMember>(ctx, pos)
                .Struct(body)
                .Name().Build(key)
                .Item(ctx.NewCallable(pos, "Unpickle", { ExpandType(pos, *itemType, ctx), member }))
                .Done();
        }

        return Build<TCoLambda>(ctx, pos)
            .Args({arg})
            .Body(body)
            .Done()
            .Ptr();
    }

    TVector<TCoAtom> GetKeysList(TExprContext& ctx, TPositionHandle pos) const {
        TVector<TCoAtom> res;
        res.reserve(PickleKeys.size() + MemberKeys.size());

        for (const auto& pickleKey : PickleKeys) {
            res.emplace_back(Build<TCoAtom>(ctx, pos).Value(pickleKey).Done());
        }
        for (const auto& memberKey : MemberKeys) {
            res.emplace_back(Build<TCoAtom>(ctx, pos).Value(memberKey).Done());
        }
        return res;
    }

    TVector<TString> GetActualGroupKeys() {
        TVector<TString> result;
        result.reserve(PickleKeys.size() + MemberKeys.size());
        result.insert(result.end(), PickleKeys.begin(), PickleKeys.end());
        result.insert(result.end(), MemberKeys.begin(), MemberKeys.end());
        return result;
    }

    bool NeedPickle() const {
        return !PickleKeys.empty();
    }

    TExprNode::TPtr GetKeySelector(TExprContext& ctx, TPositionHandle pos, const TStructExprType* rowType) {
        auto builder = Build<TCoAtomList>(ctx, pos);
        for (auto key : GetKeysList(ctx, pos)) {
            builder.Add(std::move(key));
        }
        return BuildKeySelector(pos, *rowType, builder.Build().Value().Ptr(), ctx);
    }
};

TString BuildColumnName(const TExprBase& column) {
    if (const auto columnName = column.Maybe<TCoAtom>()) {
        return columnName.Cast().StringValue();
    }

    if (const auto columnNames = column.Maybe<TCoAtomList>()) {
        TStringBuilder columnNameBuilder;
        for (const auto columnName : columnNames.Cast()) {
            columnNameBuilder.append(columnName.StringValue());
            columnNameBuilder.append("_");
        }
        return columnNameBuilder;
    }

    YQL_ENSURE(false, "Invalid node. Expected Atom or AtomList, but received: "
        << column.Ptr()->Dump());
}

bool IsLegacyHopping(const TExprNode::TPtr& hoppingSetting) {
    return !hoppingSetting->Child(1)->IsList();
}

void EnsureNotDistinct(const TCoAggregate& aggregate) {
    const auto& aggregateHandlers = aggregate.Handlers();

    YQL_ENSURE(
        AllOf(aggregateHandlers, [](const auto& t){ return !t.DistinctName(); }),
        "Distinct is not supported for aggregation with hop");
}

TMaybe<THoppingTraits> ExtractHopTraits(const TCoAggregate& aggregate, TExprContext& ctx, bool analyticsMode) {
    const auto pos = aggregate.Pos();

    const auto hopSetting = GetSetting(aggregate.Settings().Ref(), "hopping");
    if (!hopSetting) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), "Aggregate over stream must have 'hopping' setting"));
        return Nothing();
    }

    const auto hoppingColumn = IsLegacyHopping(hopSetting)
        ? "_yql_time"
        : TString(hopSetting->Child(1)->Child(0)->Content());

    const auto traitsNode = IsLegacyHopping(hopSetting)
        ? hopSetting->Child(1)
        : hopSetting->Child(1)->Child(1);

    const auto maybeTraits = TMaybeNode<TCoHoppingTraits>(traitsNode);
    if (!maybeTraits) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), "Invalid 'hopping' setting in Aggregate"));
        return Nothing();
    }

    const auto traits = maybeTraits.Cast();

    const auto checkIntervalParam = [&] (TExprBase param) -> ui64 {
        if (param.Maybe<TCoJust>()) {
            param = param.Cast<TCoJust>().Input();
        }
        if (!param.Maybe<TCoInterval>()) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "Not an interval data ctor"));
            return 0;
        }
        auto value = FromString<i64>(param.Cast<TCoInterval>().Literal().Value());
        if (value <= 0) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "Interval value must be positive"));
            return 0;
        }
        return (ui64)value;
    };

    const auto hop = checkIntervalParam(traits.Hop());
    if (!hop) {
        return Nothing();
    }
    const auto interval = checkIntervalParam(traits.Interval());
    if (!interval) {
        return Nothing();
    }
    const auto delay = checkIntervalParam(traits.Delay());
    if (!delay) {
        return Nothing();
    }

    if (interval < hop) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), "Interval must be greater or equal then hop"));
        return Nothing();
    }
    if (delay < hop) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), "Delay must be greater or equal then hop"));
        return Nothing();
    }

    const auto newTraits = Build<TCoHoppingTraits>(ctx, aggregate.Pos())
        .InitFrom(traits)
        .DataWatermarks(analyticsMode
            ? ctx.NewAtom(aggregate.Pos(), "false")
            : traits.DataWatermarks().Ptr())
        .Done();

    return THoppingTraits {
        hoppingColumn,
        newTraits,
        hop,
        interval,
        delay
    };
}

TExprNode::TPtr BuildTimeExtractor(const TCoHoppingTraits& hoppingTraits, TExprContext& ctx) {
    const auto pos = hoppingTraits.Pos();

    if (hoppingTraits.ItemType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>()->GetSize() == 0) {
        // The case when no fields are used in lambda. F.e. when it has only DependsOn.
        return ctx.DeepCopyLambda(hoppingTraits.TimeExtractor().Ref());
    }

    return Build<TCoLambda>(ctx, pos)
        .Args({"item"})
        .Body<TExprApplier>()
            .Apply(hoppingTraits.TimeExtractor())
            .With<TCoSafeCast>(0)
                .Type(hoppingTraits.ItemType())
                .Value("item")
                .Build()
            .Build()
        .Done()
        .Ptr();
}

TExprNode::TPtr BuildInitHopLambda(const TCoAggregate& aggregate, TExprContext& ctx) {
    const auto pos = aggregate.Pos();
    const auto& aggregateHandlers = aggregate.Handlers();

    const auto initItemArg = Build<TCoArgument>(ctx, pos).Name("item").Done();

    TVector<TExprBase> structItems;
    structItems.reserve(aggregateHandlers.Size());

    ui32 index = 0;
    for (const auto& handler : aggregateHandlers) {
        const auto tuple = handler.Cast<TCoAggregateTuple>();

        TMaybeNode<TExprBase> applier;
        if (tuple.Trait().Cast<TCoAggregationTraits>().InitHandler().Args().Size() == 1) {
            applier = Build<TExprApplier>(ctx, pos)
                .Apply(tuple.Trait().Cast<TCoAggregationTraits>().InitHandler())
                .With(0, initItemArg)
                .Done();
        } else {
            applier = Build<TExprApplier>(ctx, pos)
                .Apply(tuple.Trait().Cast<TCoAggregationTraits>().InitHandler())
                .With(0, initItemArg)
                .With<TCoUint32>(1)
                    .Literal().Build(ToString(index))
                    .Build()
                .Done();
        }

        structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(BuildColumnName(tuple.ColumnName()))
            .Value(applier)
            .Done());
        ++index;
    }

    return Build<TCoLambda>(ctx, pos)
        .Args({initItemArg})
        .Body<TCoAsStruct>()
            .Add(structItems)
            .Build()
        .Done()
        .Ptr();
}

TExprNode::TPtr BuildUpdateHopLambda(const TCoAggregate& aggregate, TExprContext& ctx) {
    const auto pos = aggregate.Pos();
    const auto aggregateHandlers = aggregate.Handlers();

    const auto updateItemArg = Build<TCoArgument>(ctx, pos).Name("item").Done();
    const auto updateStateArg = Build<TCoArgument>(ctx, pos).Name("state").Done();

    TVector<TExprBase> structItems;
    structItems.reserve(aggregateHandlers.Size());

    i32 index = 0;
    for (const auto& handler : aggregateHandlers) {
        const auto tuple = handler.Cast<TCoAggregateTuple>();
        const TString columnName = BuildColumnName(tuple.ColumnName());

        const auto member = Build<TCoMember>(ctx, pos)
            .Struct(updateStateArg)
            .Name().Build(columnName)
            .Done();

        TMaybeNode<TExprBase> applier;
        if (tuple.Trait().Cast<TCoAggregationTraits>().UpdateHandler().Args().Size() == 2) {
            applier = Build<TExprApplier>(ctx, pos)
                .Apply(tuple.Trait().Cast<TCoAggregationTraits>().UpdateHandler())
                .With(0, updateItemArg)
                .With(1, member)
                .Done();
        } else {
            applier = Build<TExprApplier>(ctx, pos)
                .Apply(tuple.Trait().Cast<TCoAggregationTraits>().UpdateHandler())
                .With(0, updateItemArg)
                .With(1, member)
                .With<TCoUint32>(2)
                    .Literal().Build(ToString(index))
                    .Build()
                .Done();
        }

        structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(columnName)
            .Value(applier)
            .Done());
        ++index;
    }

    return Build<TCoLambda>(ctx, pos)
        .Args({updateItemArg, updateStateArg})
        .Body<TCoAsStruct>()
            .Add(structItems)
            .Build()
        .Done()
        .Ptr();
}  

TExprNode::TPtr WrapToShuffle(
    const TKeysDescription& keysDescription,
    const TCoAggregate& aggregate,
    const TDqConnection& input,
    TExprContext& ctx)
{
    auto pos = aggregate.Pos();

    TDqStageBase mappedInput = input.Output().Stage();
    if (keysDescription.NeedPickle()) {
        mappedInput = Build<TDqStage>(ctx, pos)
            .Inputs()
                .Add<TDqCnMap>()
                    .Output()
                        .Stage(input.Output().Stage())
                        .Index(input.Output().Index())
                        .Build()
                    .Build()
                .Build()
            .Program()
                .Args({"stream"})
                .Body<TCoMap>()
                    .Input("stream")
                    .Lambda(keysDescription.BuildPickleLambda(ctx, pos))
                .Build()
            .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, pos))
            .Done();
    }

    return Build<TDqCnHashShuffle>(ctx, pos)
        .Output()
            .Stage(mappedInput)
            .Index().Value("0").Build()
            .Build()
        .KeyColumns()
            .Add(keysDescription.GetKeysList(ctx, pos))
            .Build()
        .Done()
        .Ptr();
}

TExprNode::TPtr BuildMergeHopLambda(const TCoAggregate& aggregate, TExprContext& ctx) {
    const auto pos = aggregate.Pos();
    const auto& aggregateHandlers = aggregate.Handlers();

    const auto mergeState1Arg = Build<TCoArgument>(ctx, pos).Name("state1").Done();
    const auto mergeState2Arg = Build<TCoArgument>(ctx, pos).Name("state2").Done();

    TVector<TExprBase> structItems;
    structItems.reserve(aggregateHandlers.Size());

    for (const auto& handler : aggregateHandlers) {
        const auto tuple = handler.Cast<TCoAggregateTuple>();
        const TString columnName = BuildColumnName(tuple.ColumnName());

        const auto member1 = Build<TCoMember>(ctx, pos)
            .Struct(mergeState1Arg)
            .Name().Build(columnName)
            .Done();
        const auto member2 = Build<TCoMember>(ctx, pos)
            .Struct(mergeState2Arg)
            .Name().Build(columnName)
            .Done();

        structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(columnName)
            .Value<TExprApplier>()
                .Apply(tuple.Trait().Cast<TCoAggregationTraits>().MergeHandler())
                .With(0, member1)
                .With(1, member2)
                .Build()
            .Done());
    }

    return Build<TCoLambda>(ctx, pos)
        .Args({mergeState1Arg, mergeState2Arg})
        .Body<TCoAsStruct>()
            .Add(structItems)
            .Build()
        .Done()
        .Ptr();
}

TExprNode::TPtr BuildFinishHopLambda(
    const TCoAggregate& aggregate,
    const TVector<TString>& actualGroupKeys,
    const TString& hoppingColumn,
    TExprContext& ctx)
{
    const auto pos = aggregate.Pos();
    const auto aggregateHandlers = aggregate.Handlers();

    const auto finishKeyArg = Build<TCoArgument>(ctx, pos).Name("key").Done();
    const auto finishStateArg = Build<TCoArgument>(ctx, pos).Name("state").Done();
    const auto finishTimeArg = Build<TCoArgument>(ctx, pos).Name("time").Done();

    TVector<TExprBase> structItems;
    structItems.reserve(actualGroupKeys.size() + aggregateHandlers.Size() + 1);

    if (actualGroupKeys.size() == 1) {
        structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(actualGroupKeys[0])
            .Value(finishKeyArg)
            .Done());
    } else {
        for (size_t i = 0; i < actualGroupKeys.size(); ++i) {
            structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(actualGroupKeys[i])
                .Value<TCoNth>()
                    .Tuple(finishKeyArg)
                    .Index<TCoAtom>()
                        .Value(ToString(i))
                        .Build()
                    .Build()
                .Done());
        }
    }

    for (const auto& handler : aggregateHandlers) {
        const auto tuple = handler.Cast<TCoAggregateTuple>();
        const TString compoundColumnName = BuildColumnName(tuple.ColumnName());

        const auto member = Build<TCoMember>(ctx, pos)
            .Struct(finishStateArg)
            .Name().Build(compoundColumnName)
            .Done();

        if (tuple.ColumnName().Maybe<TCoAtom>()) {
            structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(compoundColumnName)
                .Value<TExprApplier>()
                    .Apply(tuple.Trait().Cast<TCoAggregationTraits>().FinishHandler())
                    .With(0, member)
                    .Build()
                .Done());

            continue;
        }

        if (const auto namesList = tuple.ColumnName().Maybe<TCoAtomList>()) {
            const auto expApplier = Build<TExprApplier>(ctx, pos)
                .Apply(tuple.Trait().Cast<TCoAggregationTraits>().FinishHandler())
                .With(0, member)
                .Done();

            int index = 0;
            for (const auto columnName : namesList.Cast()) {
                const auto extracter = Build<TCoNth>(ctx, pos)
                    .Tuple(expApplier)
                    .Index<TCoAtom>().Build(index++)
                    .Done();

                structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                    .Name(columnName)
                    .Value(extracter)
                    .Done());
            }

            continue;
        }

        YQL_ENSURE(false, "Invalid node. Expected Atom or AtomList, but received: "
            << tuple.ColumnName().Ptr()->Dump());
    }

    structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
        .Name().Build(hoppingColumn)
        .Value(finishTimeArg)
        .Done());

    return Build<TCoLambda>(ctx, pos)
        .Args({finishKeyArg, finishStateArg, finishTimeArg})
        .Body<TCoAsStruct>()
            .Add(structItems)
            .Build()
        .Done()
        .Ptr();
}

TExprNode::TPtr BuildSaveHopLambda(const TCoAggregate& aggregate, TExprContext& ctx) {
    const auto pos = aggregate.Pos();
    const auto aggregateHandlers = aggregate.Handlers();

    const auto saveStateArg = Build<TCoArgument>(ctx, pos).Name("state").Done();

    TVector<TExprBase> structItems;
    structItems.reserve(aggregateHandlers.Size());

    for (const auto& handler : aggregateHandlers) {
        const auto tuple = handler.Cast<TCoAggregateTuple>();
        const TString columnName = BuildColumnName(tuple.ColumnName());

        const auto member = Build<TCoMember>(ctx, pos)
            .Struct(saveStateArg)
            .Name().Build(columnName)
            .Done();

        structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(columnName)
            .Value<TExprApplier>()
                .Apply(tuple.Trait().Cast<TCoAggregationTraits>().SaveHandler())
                .With(0, member)
                .Build()
            .Done());
    }

    return Build<TCoLambda>(ctx, pos)
        .Args({saveStateArg})
        .Body<TCoAsStruct>()
            .Add(structItems)
            .Build()
        .Done()
        .Ptr();
}

TExprNode::TPtr BuildLoadHopLambda(const TCoAggregate& aggregate, TExprContext& ctx) {
    const auto pos = aggregate.Pos();
    const auto aggregateHandlers = aggregate.Handlers();

    TCoArgument loadStateArg = Build<TCoArgument>(ctx, pos).Name("state").Done();

    TVector<TExprBase> structItems;
    structItems.reserve(aggregateHandlers.Size());

    for (const auto& handler : aggregateHandlers) {
        const auto tuple = handler.Cast<TCoAggregateTuple>();
        const TString columnName = BuildColumnName(tuple.ColumnName());

        const auto member = Build<TCoMember>(ctx, pos)
            .Struct(loadStateArg)
            .Name().Build(columnName)
            .Done();

        structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(columnName)
            .Value<TExprApplier>()
                .Apply(tuple.Trait().Cast<TCoAggregationTraits>().LoadHandler())
                .With(0, member)
                .Build()
            .Done());
    }

    return Build<TCoLambda>(ctx, pos)
        .Args({loadStateArg})
        .Body<TCoAsStruct>()
            .Add(structItems)
            .Build()
        .Done()
        .Ptr();
}

TMaybe<bool> BuildWatermarkMode(
    const TCoAggregate& aggregate,
    const TCoHoppingTraits& hoppingTraits,
    TExprContext& ctx,
    bool analyticsMode,
    bool defaultWatermarksMode,
    bool syncActor)
{
    const bool enableWatermarks = !analyticsMode &&
        defaultWatermarksMode &&
        hoppingTraits.Version().Cast<TCoAtom>().StringValue() == "v2";
    if (enableWatermarks && syncActor) {
        ctx.AddError(TIssue(ctx.GetPosition(aggregate.Pos()), "Watermarks should be used only with async compute actor"));
        return Nothing();
    }

    if (hoppingTraits.Version().Cast<TCoAtom>().StringValue() == "v2" && !enableWatermarks) {
        ctx.AddError(TIssue(
            ctx.GetPosition(aggregate.Pos()),
            "HoppingWindow requires watermarks to be enabled. If you don't want to do that, you can use HOP instead."));
        return Nothing();
    }

    return enableWatermarks;
}

TMaybeNode<TExprBase> RewriteAsHoppingWindowFullOutput(
    const TExprBase node,
    TExprContext& ctx,
    const TDqConnection& input,
    bool analyticsMode,
    TDuration lateArrivalDelay,
    bool defaultWatermarksMode,
    bool syncActor) {
    const auto aggregate = node.Cast<TCoAggregate>();
    const auto pos = aggregate.Pos();

    YQL_CLOG(DEBUG, ProviderDq) << "OptimizeStreamingAggregate";

    EnsureNotDistinct(aggregate);

    const auto maybeHopTraits = ExtractHopTraits(aggregate, ctx, analyticsMode);
    if (!maybeHopTraits) {
        return nullptr;
    }
    const auto hopTraits = *maybeHopTraits;

    const auto aggregateInputType = GetSeqItemType(*node.Ptr()->Head().GetTypeAnn()).Cast<TStructExprType>();
    TKeysDescription keysDescription(*aggregateInputType, aggregate.Keys(), hopTraits.Column);

    if (keysDescription.NeedPickle()) {
        return Build<TCoMap>(ctx, pos)
            .Lambda(keysDescription.BuildUnpickleLambda(ctx, pos, *aggregateInputType))
            .Input<TCoAggregate>()
                .InitFrom(aggregate)
                .Input<TCoMap>()
                    .Lambda(keysDescription.BuildPickleLambda(ctx, pos))
                    .Input(input)
                .Build()
                .Settings(RemoveSetting(aggregate.Settings().Ref(), "output_columns", ctx))
            .Build()
            .Done();
    }

    const auto keyLambda = keysDescription.GetKeySelector(ctx, pos, aggregateInputType);
    const auto timeExtractorLambda = BuildTimeExtractor(hopTraits.Traits, ctx);
    const auto initLambda = BuildInitHopLambda(aggregate, ctx);
    const auto updateLambda = BuildUpdateHopLambda(aggregate, ctx);
    const auto saveLambda = BuildSaveHopLambda(aggregate, ctx);
    const auto loadLambda = BuildLoadHopLambda(aggregate, ctx);
    const auto mergeLambda = BuildMergeHopLambda(aggregate, ctx);
    const auto finishLambda = BuildFinishHopLambda(aggregate, keysDescription.GetActualGroupKeys(), hopTraits.Column, ctx);
    const auto enableWatermarks = BuildWatermarkMode(aggregate, hopTraits.Traits, ctx, analyticsMode, defaultWatermarksMode, syncActor);
    if (!enableWatermarks) {
        return nullptr;
    }

    const auto streamArg = Build<TCoArgument>(ctx, pos).Name("stream").Done();
    auto multiHoppingCoreBuilder = Build<TCoMultiHoppingCore>(ctx, pos)
        .KeyExtractor(keyLambda)
        .TimeExtractor(timeExtractorLambda)
        .Hop(hopTraits.Traits.Hop())
        .Interval(hopTraits.Traits.Interval())
        .DataWatermarks(hopTraits.Traits.DataWatermarks())
        .InitHandler(initLambda)
        .UpdateHandler(updateLambda)
        .MergeHandler(mergeLambda)
        .FinishHandler(finishLambda)
        .SaveHandler(saveLambda)
        .LoadHandler(loadLambda)
        .template WatermarkMode<TCoAtom>().Build(ToString(*enableWatermarks));

    if (*enableWatermarks) {
        const auto hop = TDuration::MicroSeconds(hopTraits.Hop);
        multiHoppingCoreBuilder.template Delay<TCoInterval>()
            .Literal().Build(ToString(Max(hop, lateArrivalDelay).MicroSeconds()))
            .Build();
    } else {
        multiHoppingCoreBuilder.Delay(hopTraits.Traits.Delay());
    }

    if (analyticsMode) {
        return Build<TCoPartitionsByKeys>(ctx, node.Pos())
            .Input(input.Ptr())
            .KeySelectorLambda(keyLambda)
            .SortDirections<TCoBool>()
                    .Literal()
                    .Value("true")
                    .Build()
                .Build()
            .SortKeySelectorLambda(timeExtractorLambda)
            .ListHandlerLambda()
                .Args(streamArg)
                .template Body<TCoForwardList>()
                    .Stream(multiHoppingCoreBuilder
                        .template Input<TCoIterator>()
                            .List(streamArg)
                            .Build()
                        .Done())
                    .Build()
                .Build()
            .Done();
    } else {
        auto wrappedInput = input.Ptr();
        if (!keysDescription.MemberKeys.empty()) {
            // Shuffle input connection by keys
            wrappedInput = WrapToShuffle(keysDescription, aggregate, input, ctx);
            if (!wrappedInput) {
                return nullptr;
            }
        }

        const auto stage = Build<TDqStage>(ctx, node.Pos())
            .Inputs()
                .Add(wrappedInput)
                .Build()
            .Program()
                .Args(streamArg)
                .Body<TCoMap>()
                    .Input(multiHoppingCoreBuilder
                        .template Input<TCoFromFlow>()
                            .Input(streamArg)
                            .Build()
                        .Done())
                    .Lambda(keysDescription.BuildUnpickleLambda(ctx, pos, *aggregateInputType))
                    .Build()
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
            .Done();

        return Build<TDqCnUnionAll>(ctx, node.Pos())
            .Output()
                .Stage(stage)
                .Index().Build(0)
                .Build()
            .Done();
    }
}

} // namespace

namespace NYql::NDq::NHopping {

TMaybeNode<TExprBase> RewriteAsHoppingWindow(
    const TExprBase node,
    TExprContext& ctx,
    const TDqConnection& input,
    bool analyticsMode,
    TDuration lateArrivalDelay,
    bool defaultWatermarksMode,
    bool syncActor)
{
    auto result = RewriteAsHoppingWindowFullOutput(node, ctx, input, analyticsMode, lateArrivalDelay, defaultWatermarksMode, syncActor);
    if (!result) {
        return result;
    }

    const auto aggregate = node.Cast<TCoAggregate>();
    auto outputColumnSetting = GetSetting(aggregate.Settings().Ref(), "output_columns");
    if (!outputColumnSetting) {
        return result;
    }

    return Build<TCoExtractMembers>(ctx, node.Pos())
        .Input(result.Cast())
        .Members(outputColumnSetting->ChildPtr(1))
        .Done();
}

} // NYql::NDq::NHopping
