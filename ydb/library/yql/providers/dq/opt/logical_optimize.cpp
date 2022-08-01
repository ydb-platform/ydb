#include "logical_optimize.h"
#include "dqs_opt.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql::NDqs {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

constexpr TStringBuf YQL_TIME = "_yql_time";

TString BuildColumnName(const TExprBase column) {
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

}

class TDqsLogicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TDqsLogicalOptProposalTransformer(TTypeAnnotationContext* typeCtx, const TDqConfiguration::TPtr& config)
        : TOptimizeTransformerBase(typeCtx, NLog::EComponent::ProviderDq, {})
        , Config(config)
    {
#define HNDL(name) "DqsLogical-"#name, Hndl(&TDqsLogicalOptProposalTransformer::name)
        AddHandler(0, &TCoUnorderedBase::Match, HNDL(SkipUnordered));
        AddHandler(0, &TCoAggregate::Match, HNDL(RewriteAggregate));
        AddHandler(0, &TCoTake::Match, HNDL(RewriteTakeSortToTopSort));
        AddHandler(0, &TCoEquiJoin::Match, HNDL(RewriteEquiJoin));
        AddHandler(0, &TCoCalcOverWindowBase::Match, HNDL(ExpandWindowFunctions));
        AddHandler(0, &TCoCalcOverWindowGroup::Match, HNDL(ExpandWindowFunctions));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(FlatMapOverExtend));
        AddHandler(0, &TDqQuery::Match, HNDL(MergeQueriesWithSinks));
        AddHandler(0, &TDqStageBase::Match, HNDL(UnorderedInStage));
        AddHandler(0, &TCoSqlIn::Match, HNDL(SqlInDropCompact));
#undef HNDL
    }

protected:
    TMaybeNode<TExprBase> SkipUnordered(TExprBase node, TExprContext& ctx) {
        Y_UNUSED(ctx);
        auto unordered = node.Cast<TCoUnorderedBase>();
        if (unordered.Input().Maybe<TDqConnection>()) {
            return unordered.Input();
        }
        return node;
    }

    TMaybeNode<TExprBase> FlatMapOverExtend(TExprBase node, TExprContext& ctx) {
        return DqFlatMapOverExtend(node, ctx);
    }

    TMaybeNode<TExprBase> RewriteAggregate(TExprBase node, TExprContext& ctx) {
        auto aggregate = node.Cast<TCoAggregate>();
        auto input = aggregate.Input().Maybe<TDqConnection>();

        auto hopSetting = GetSetting(aggregate.Settings().Ref(), "hopping");
        if (input) {
            if (hopSetting) {
                return RewriteAsHoppingWindow(node, ctx, input.Cast()).Cast();
            } else {
                return DqRewriteAggregate(node, ctx, *Types);
            }
        }
        return node;
    }

    TMaybeNode<TExprBase> RewriteTakeSortToTopSort(TExprBase node, TExprContext& ctx, const TGetParents& getParents) {
        if (node.Maybe<TCoTake>().Input().Maybe<TCoSort>().Input().Maybe<TDqConnection>()) {
            return DqRewriteTakeSortToTopSort(node, ctx, *getParents());
        }
        return node;
    }

    TMaybeNode<TExprBase> RewriteEquiJoin(TExprBase node, TExprContext& ctx) {
        auto equiJoin = node.Cast<TCoEquiJoin>();
        bool hasDqConnections = false;
        for (size_t i = 0; i + 2 < equiJoin.ArgCount(); ++i) {
            auto list = equiJoin.Arg(i).Cast<TCoEquiJoinInput>().List();
            if (auto maybeExtractMembers = list.Maybe<TCoExtractMembers>()) {
                list = maybeExtractMembers.Cast().Input();
            }
            if (auto maybeFlatMap = list.Maybe<TCoFlatMapBase>()) {
                list = maybeFlatMap.Cast().Input();
            }
            hasDqConnections |= !!list.Maybe<TDqConnection>();
        }

        return hasDqConnections ? DqRewriteEquiJoin(node, ctx) : node;
    }

    TMaybeNode<TExprBase> ExpandWindowFunctions(TExprBase node, TExprContext& ctx) {
        if (node.Cast<TCoInputBase>().Input().Maybe<TDqConnection>()) {
            return DqExpandWindowFunctions(node, ctx, true);
        }
        return node;
    }

    TMaybeNode<TExprBase> MergeQueriesWithSinks(TExprBase node, TExprContext& ctx) {
        return DqMergeQueriesWithSinks(node, ctx);
    }

    TMaybeNode<TExprBase> UnorderedInStage(TExprBase node, TExprContext& ctx) const {
        return DqUnorderedInStage(node, TDqReadWrapBase::Match, ctx, Types);
    }

    TMaybeNode<TExprBase> SqlInDropCompact(TExprBase node, TExprContext& ctx) const {
        return DqSqlInDropCompact(node, ctx);
    }

private:
    TMaybeNode<TExprBase> RewriteAsHoppingWindow(const TExprBase node, TExprContext& ctx, const TDqConnection& input) {
        const auto aggregate = node.Cast<TCoAggregate>();
        const auto pos = aggregate.Pos();

        YQL_CLOG(DEBUG, ProviderDq) << "OptimizeStreamingAggregate";

        EnsureNotDistinct(aggregate);

        const auto aggregateInputType = GetSeqItemType(node.Ptr()->Head().GetTypeAnn())->Cast<TStructExprType>();
        TKeysDescription keysDescription(*aggregateInputType, aggregate.Keys());

        if (keysDescription.NeedPickle()) {
            return Build<TCoMap>(ctx, pos)
                .Lambda(keysDescription.BuildUnpickleLambda(ctx, pos, *aggregateInputType))
                .Input<TCoAggregate>()
                    .InitFrom(aggregate)
                    .Input<TCoMap>()
                        .Lambda(keysDescription.BuildPickleLambda(ctx, pos))
                        .Input(input)
                    .Build()
                .Build()
                .Done();
        }

        const auto maybeHopTraits = ExtractHopTraits(aggregate, ctx);
        if (!maybeHopTraits) {
            return nullptr;
        }
        const auto hopTraits = maybeHopTraits.Cast();

        const auto keyLambda = BuildKeySelector(pos, *aggregateInputType, aggregate.Keys().Ptr(), ctx);
        const auto timeExtractorLambda = BuildTimeExtractor(hopTraits, ctx);
        const auto initLambda = BuildInitHopLambda(aggregate, ctx);
        const auto updateLambda = BuildUpdateHopLambda(aggregate, ctx);
        const auto saveLambda = BuildSaveHopLambda(aggregate, ctx);
        const auto loadLambda = BuildLoadHopLambda(aggregate, ctx);
        const auto mergeLambda = BuildMergeHopLambda(aggregate, ctx);
        const auto finishLambda = BuildFinishHopLambda(aggregate, ctx);

        const auto streamArg = Build<TCoArgument>(ctx, pos).Name("stream").Done();
        auto multiHoppingCoreBuilder = Build<TCoMultiHoppingCore>(ctx, pos)
            .KeyExtractor(keyLambda)
            .TimeExtractor(timeExtractorLambda)
            .Hop(hopTraits.Hop())
            .Interval(hopTraits.Interval())
            .Delay(hopTraits.Delay())
            .DataWatermarks(hopTraits.DataWatermarks())
            .InitHandler(initLambda)
            .UpdateHandler(updateLambda)
            .MergeHandler(mergeLambda)
            .FinishHandler(finishLambda)
            .SaveHandler(saveLambda)
            .LoadHandler(loadLambda);

        if (Config->AnalyticsHopping.Get().GetOrElse(false)) {
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
                    .Body<TCoForwardList>()
                        .Stream(multiHoppingCoreBuilder
                            .Input<TCoIterator>()
                                .List(streamArg)
                                .Build()
                            .Done())
                        .Build()
                    .Build()
                .Done();
        } else {
            auto wrappedInput = input.Ptr();
            if (!aggregate.Keys().Empty()) {
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
                            .Input<TCoFromFlow>()
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

    TMaybeNode<TCoHoppingTraits> ExtractHopTraits(const TCoAggregate& aggregate, TExprContext& ctx) {
        const auto pos = aggregate.Pos();

        const auto hopSetting = GetSetting(aggregate.Settings().Ref(), "hopping");
        if (!hopSetting) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "Aggregate over stream must have 'hopping' setting"));
            return TMaybeNode<TCoHoppingTraits>();
        }

        const auto maybeTraits = TMaybeNode<TCoHoppingTraits>(hopSetting->Child(1));
        if (!maybeTraits) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "Invalid 'hopping' setting in Aggregate"));
            return TMaybeNode<TCoHoppingTraits>();
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
            return TMaybeNode<TCoHoppingTraits>();
        }
        const auto interval = checkIntervalParam(traits.Interval());
        if (!interval) {
            return TMaybeNode<TCoHoppingTraits>();
        }
        const auto delay = checkIntervalParam(traits.Delay());
        if (!delay) {
            return TMaybeNode<TCoHoppingTraits>();
        }

        if (interval < hop) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "Interval must be greater or equal then hop"));
            return TMaybeNode<TCoHoppingTraits>();
        }
        if (delay < hop) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "Delay must be greater or equal then hop"));
            return TMaybeNode<TCoHoppingTraits>();
        }

        return Build<TCoHoppingTraits>(ctx, aggregate.Pos())
            .InitFrom(traits)
            .DataWatermarks(Config->AnalyticsHopping.Get().GetOrElse(false)
                ? ctx.NewAtom(aggregate.Pos(), "false")
                : traits.DataWatermarks().Ptr())
            .Done();
    }

    struct TKeysDescription {
        TVector<TString> PickleKeys;
        TVector<TString> MemberKeys;

        explicit TKeysDescription(const TStructExprType& rowType, const TCoAtomList& keys) {
            for (const auto& key : keys) {
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

        bool NeedPickle() const {
            return !PickleKeys.empty();
        }
    };

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

    void EnsureNotDistinct(const TCoAggregate& aggregate) {
        const auto& aggregateHandlers = aggregate.Handlers();

        YQL_ENSURE(
            AllOf(aggregateHandlers, [](const auto& t){ return !t.DistinctName(); }),
            "Distinct is not supported for aggregation with hop");
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
            if (tuple.Trait().InitHandler().Args().Size() == 1) {
                applier = Build<TExprApplier>(ctx, pos)
                    .Apply(tuple.Trait().InitHandler())
                    .With(0, initItemArg)
                    .Done();
            } else {
                applier = Build<TExprApplier>(ctx, pos)
                    .Apply(tuple.Trait().InitHandler())
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
            if (tuple.Trait().UpdateHandler().Args().Size() == 2) {
                applier = Build<TExprApplier>(ctx, pos)
                    .Apply(tuple.Trait().UpdateHandler())
                    .With(0, updateItemArg)
                    .With(1, member)
                    .Done();
            } else {
                applier = Build<TExprApplier>(ctx, pos)
                    .Apply(tuple.Trait().UpdateHandler())
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
                    .Apply(tuple.Trait().MergeHandler())
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

    TExprNode::TPtr BuildFinishHopLambda(const TCoAggregate& aggregate, TExprContext& ctx) {
        const auto pos = aggregate.Pos();
        const auto keyColumns = aggregate.Keys();
        const auto aggregateHandlers = aggregate.Handlers();

        const auto finishKeyArg = Build<TCoArgument>(ctx, pos).Name("key").Done();
        const auto finishStateArg = Build<TCoArgument>(ctx, pos).Name("state").Done();
        const auto finishTimeArg = Build<TCoArgument>(ctx, pos).Name("time").Done();

        TVector<TExprBase> structItems;
        structItems.reserve(keyColumns.Size() + aggregateHandlers.Size() + 1);

        if (keyColumns.Size() == 1) {
            structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name(keyColumns.Item(0))
                .Value(finishKeyArg)
                .Done());
        } else {
            for (size_t i = 0; i < keyColumns.Size(); ++i) {
                structItems.push_back(Build<TCoNameValueTuple>(ctx, pos)
                    .Name(keyColumns.Item(i))
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
                        .Apply(tuple.Trait().FinishHandler())
                        .With(0, member)
                        .Build()
                    .Done());

                continue;
            }

            if (const auto namesList = tuple.ColumnName().Maybe<TCoAtomList>()) {
                const auto expApplier = Build<TExprApplier>(ctx, pos)
                    .Apply(tuple.Trait().FinishHandler())
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
            .Name().Build(YQL_TIME)
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
                    .Apply(tuple.Trait().SaveHandler())
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
                    .Apply(tuple.Trait().LoadHandler())
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

private:
    TDqConfiguration::TPtr Config;
};

THolder<IGraphTransformer> CreateDqsLogOptTransformer(TTypeAnnotationContext* typeCtx, const TDqConfiguration::TPtr& config) {
    return THolder(new TDqsLogicalOptProposalTransformer(typeCtx, config));
}

} // NYql::NDqs
