#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/time_provider.h>

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <algorithm>

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYql;

struct TRuleTestContext {
    TRuleTestContext()
        : FuncRegistry(NKikimr::NMiniKQL::CreateFunctionRegistry(
              NKikimr::NMiniKQL::CreateBuiltinRegistry()))
        , Config(MakeIntrusive<TKikimrConfiguration>())
        , QueryCtx(MakeIntrusive<TKikimrQueryContext>(
              FuncRegistry.Get(), CreateDefaultTimeProvider(), CreateDefaultRandomProvider()))
        , Tables(MakeIntrusive<TKikimrTablesData>())
        , UserRequestContext(MakeIntrusive<TUserRequestContext>())
        , KqpCtx("ut", Config, QueryCtx, Tables, UserRequestContext)
        , RboCtx(KqpCtx, ExprCtx, TypeCtx, TypeAnnTransformer, *FuncRegistry)
    {
    }

    TExprContext ExprCtx;
    TTypeAnnotationContext TypeCtx;
    TNullTransformer TypeAnnTransformer;
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FuncRegistry;
    TIntrusivePtr<TKikimrConfiguration> Config;
    TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    TIntrusivePtr<TKikimrTablesData> Tables;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    NOpt::TKqpOptimizeContext KqpCtx;
    TRBOContext RboCtx;
    TPlanProps PlanProps;
};

TIntrusivePtr<TOpRead> MakeRead(
    TPositionHandle pos,
    const TVector<TInfoUnit>& columns)
{
    TVector<TString> columnNames;
    columnNames.reserve(columns.size());
    for (const auto& column : columns) {
        columnNames.push_back(column.GetFullName());
    }
    return MakeIntrusive<TOpRead>(
        "",
        columnNames,
        columns,
        NYql::EStorageType::ColumnStorage,
        nullptr,
        nullptr,
        nullptr,
        std::nullopt,
        std::nullopt,
        ESortDir::None,
        TPhysicalOpProps{},
        pos);
}

TExprNode::TPtr MakeWindowDefinition(
    TExprContext& ctx,
    TPositionHandle pos,
    TStringBuf name,
    const TVector<TInfoUnit>& partitions)
{
    TExprNode::TListType groups;
    groups.reserve(partitions.size());
    for (const auto& partition : partitions) {
        auto row = ctx.NewArgument(pos, "window_row");
        auto groupRef = ctx.NewCallable(
            pos,
            "YqlGroupRef",
            {
                row,
                ctx.NewAtom(pos, "type"),
                ctx.NewAtom(pos, "3"),
                ctx.NewAtom(pos, partition.GetFullName()),
            });
        groups.push_back(ctx.NewCallable(
            pos,
            "YqlGroup",
            {
                ctx.NewAtom(pos, "row_type"),
                ctx.NewLambda(
                    pos,
                    ctx.NewArguments(pos, {row}),
                    std::move(groupRef)),
            }));
    }

    return ctx.NewCallable(
        pos,
        "YqlWindow",
        {
            ctx.NewAtom(pos, name),
            ctx.NewAtom(pos, ""),
            ctx.NewList(pos, std::move(groups)),
            ctx.NewList(pos, {}),
            ctx.NewList(pos, {}),
        });
}

TExpression MakeWindowExpression(
    TRuleTestContext& ctx,
    const TInfoUnit& input,
    const TVector<TInfoUnit>& partitions,
    TStringBuf callName = "_window",
    TStringBuf definitionName = "_window",
    ui32 callCount = 1)
{
    const auto pos = TPositionHandle();
    const auto access = MakeColumnAccess(
        input,
        pos,
        &ctx.ExprCtx,
        &ctx.PlanProps);
    const auto lambda = access.GetLambda();

    TExprNode::TListType calls;
    calls.reserve(callCount);
    for (ui32 i = 0; i < callCount; ++i) {
        calls.push_back(ctx.ExprCtx.NewCallable(
            pos,
            "YqlAggWin",
            {
                ctx.ExprCtx.NewAtom(pos, "factory"),
                ctx.ExprCtx.NewAtom(pos, callName),
                ctx.ExprCtx.NewList(pos, {}),
                ctx.ExprCtx.NewAtom(pos, "type"),
                lambda->ChildPtr(1),
            }));
    }
    TExprNode::TPtr body = callCount == 1
        ? calls.front()
        : ctx.ExprCtx.NewCallable(pos, "AsTuple", std::move(calls));

    return TExpression(
        ctx.ExprCtx.NewLambda(
            pos,
            lambda->ChildPtr(0),
            std::move(body)),
        &ctx.ExprCtx,
        &ctx.PlanProps,
        MakeWindowDefinition(
            ctx.ExprCtx,
            pos,
            definitionName,
            partitions));
}

TExpression MakeUntrackedWindowExpression(
    TRuleTestContext& ctx,
    const TInfoUnit& input)
{
    auto expression = MakeWindowExpression(
        ctx,
        input,
        TVector<TInfoUnit>{input});
    return TExpression(
        expression.GetLambda(),
        &ctx.ExprCtx,
        &ctx.PlanProps);
}

TExprNode::TPtr MakeGlobalRankDefinition(
    TExprContext& ctx,
    TPositionHandle pos,
    TStringBuf name,
    const TInfoUnit& orderBy)
{
    auto row = ctx.NewArgument(pos, "window_row");
    auto decimalType = ctx.NewCallable(
        pos,
        "DataType",
        {
            ctx.NewAtom(pos, "Decimal"),
            ctx.NewAtom(pos, "15"),
            ctx.NewAtom(pos, "4"),
        });
    auto sort = ctx.NewCallable(
        pos,
        "YqlSort",
        {
            ctx.NewCallable(
                pos,
                "StructType",
                {ctx.NewList(
                    pos,
                    {
                        ctx.NewAtom(pos, orderBy.GetFullName()),
                        decimalType,
                    })}),
            ctx.NewLambda(
                pos,
                ctx.NewArguments(pos, {row}),
                ctx.NewCallable(
                    pos,
                    "Member",
                    {row, ctx.NewAtom(pos, orderBy.GetFullName())})),
            ctx.NewAtom(pos, "asc"),
            ctx.NewAtom(pos, "first"),
        });
    return ctx.NewCallable(
        pos,
        "YqlWindow",
        {
            ctx.NewAtom(pos, name),
            ctx.NewAtom(pos, ""),
            ctx.NewList(pos, {}),
            ctx.NewList(pos, {std::move(sort)}),
            ctx.NewList(
                pos,
                {
                    ctx.NewList(
                        pos,
                        {ctx.NewAtom(pos, "type"), ctx.NewAtom(pos, "rows")}),
                    ctx.NewList(
                        pos,
                        {ctx.NewAtom(pos, "from"), ctx.NewAtom(pos, "up")}),
                    ctx.NewList(
                        pos,
                        {ctx.NewAtom(pos, "to"), ctx.NewAtom(pos, "f")}),
                    ctx.NewList(
                        pos,
                        {
                            ctx.NewAtom(pos, "to_value"),
                            ctx.NewCallable(
                                pos,
                                "Int32",
                                {ctx.NewAtom(pos, "0")}),
                        }),
                }),
        });
}

TExpression MakeGlobalRankExpression(
    TRuleTestContext& ctx,
    const TInfoUnit& orderBy,
    TStringBuf name = "_rank_window")
{
    const auto pos = TPositionHandle();
    auto row = ctx.ExprCtx.NewArgument(pos, "rank_row");
    return TExpression(
        ctx.ExprCtx.NewLambda(
            pos,
            ctx.ExprCtx.NewArguments(pos, {row}),
            ctx.ExprCtx.NewCallable(
                pos,
                "YqlWin",
                {
                    ctx.ExprCtx.NewAtom(pos, "rank"),
                    ctx.ExprCtx.NewAtom(pos, name),
                    ctx.ExprCtx.NewList(pos, {}),
                    ctx.ExprCtx.NewCallable(
                        pos,
                        "DataType",
                        {ctx.ExprCtx.NewAtom(pos, "Uint64")}),
                })),
        &ctx.ExprCtx,
        &ctx.PlanProps,
        MakeGlobalRankDefinition(ctx.ExprCtx, pos, name, orderBy));
}

TExpression MakeUntrackedRankExpression(
    TRuleTestContext& ctx,
    const TInfoUnit& orderBy)
{
    const auto tracked = MakeGlobalRankExpression(ctx, orderBy);
    return TExpression(
        tracked.GetLambda(),
        &ctx.ExprCtx,
        &ctx.PlanProps);
}

TExprNode::TPtr MakeStageDataType(
    TExprContext& ctx,
    TPositionHandle pos,
    TStringBuf name)
{
    return ctx.NewCallable(
        pos,
        "DataType",
        {ctx.NewAtom(pos, name)});
}

TExprNode::TPtr MakeStageOptionalDataType(
    TExprContext& ctx,
    TPositionHandle pos,
    TStringBuf name)
{
    return ctx.NewCallable(
        pos,
        "OptionalType",
        {MakeStageDataType(ctx, pos, name)});
}

TExprNode::TPtr MakeQ51WindowKeyReference(
    TExprContext& ctx,
    TPositionHandle pos,
    const TExprNode::TPtr& row,
    const TExprNode::TPtr& type,
    const TInfoUnit& column,
    TStringBuf index,
    bool groupRef)
{
    if (groupRef) {
        return ctx.NewCallable(
            pos,
            "YqlGroupRef",
            {
                row,
                type,
                ctx.NewAtom(pos, index),
                ctx.NewAtom(pos, column.GetFullName()),
            });
    }
    return ctx.NewCallable(
        pos,
        "Member",
        {row, ctx.NewAtom(pos, column.GetFullName())});
}

TExprNode::TPtr MakeQ51WindowDefinition(
    TExprContext& ctx,
    TPositionHandle pos,
    TStringBuf name,
    const TInfoUnit& partitionBy,
    const TInfoUnit& orderBy,
    bool groupRefs)
{
    auto partitionType = groupRefs
        ? MakeStageDataType(ctx, pos, "Int64")
        : MakeStageOptionalDataType(ctx, pos, "Int64");
    auto partitionRow = ctx.NewArgument(pos, "partition_row");
    auto partition = ctx.NewCallable(
        pos,
        "YqlGroup",
        {
            ctx.NewCallable(
                pos,
                "StructType",
                {ctx.NewList(
                    pos,
                    {
                        ctx.NewAtom(pos, partitionBy.GetFullName()),
                        partitionType,
                    })}),
            ctx.NewLambda(
                pos,
                ctx.NewArguments(pos, {partitionRow}),
                MakeQ51WindowKeyReference(
                    ctx,
                    pos,
                    partitionRow,
                    partitionType,
                    partitionBy,
                    "0",
                    groupRefs)),
        });

    auto orderType = MakeStageOptionalDataType(ctx, pos, "Date");
    auto orderRow = ctx.NewArgument(pos, "order_row");
    auto sort = ctx.NewCallable(
        pos,
        "YqlSort",
        {
            ctx.NewCallable(
                pos,
                "StructType",
                {ctx.NewList(
                    pos,
                    {
                        ctx.NewAtom(pos, orderBy.GetFullName()),
                        orderType,
                    })}),
            ctx.NewLambda(
                pos,
                ctx.NewArguments(pos, {orderRow}),
                MakeQ51WindowKeyReference(
                    ctx,
                    pos,
                    orderRow,
                    orderType,
                    orderBy,
                    "1",
                    groupRefs)),
            ctx.NewAtom(pos, "asc"),
            ctx.NewAtom(pos, "first"),
        });
    return ctx.NewCallable(
        pos,
        "YqlWindow",
        {
            ctx.NewAtom(pos, name),
            ctx.NewAtom(pos, ""),
            ctx.NewList(pos, {std::move(partition)}),
            ctx.NewList(pos, {std::move(sort)}),
            ctx.NewList(
                pos,
                {
                    ctx.NewList(
                        pos,
                        {ctx.NewAtom(pos, "type"), ctx.NewAtom(pos, "rows")}),
                    ctx.NewList(
                        pos,
                        {ctx.NewAtom(pos, "from"), ctx.NewAtom(pos, "up")}),
                    ctx.NewList(
                        pos,
                        {ctx.NewAtom(pos, "to"), ctx.NewAtom(pos, "f")}),
                    ctx.NewList(
                        pos,
                        {
                            ctx.NewAtom(pos, "to_value"),
                            ctx.NewCallable(
                                pos,
                                "Int32",
                                {ctx.NewAtom(pos, "0")}),
                        }),
                }),
        });
}

TExpression MakeQ51WindowExpression(
    TRuleTestContext& ctx,
    const TInfoUnit& input,
    const TInfoUnit& partitionBy,
    const TInfoUnit& orderBy,
    TStringBuf name,
    bool groupRefs)
{
    const auto pos = TPositionHandle();
    auto row = ctx.ExprCtx.NewArgument(pos, "window_row");
    auto decimalType = ctx.ExprCtx.NewCallable(
        pos,
        "OptionalType",
        {ctx.ExprCtx.NewCallable(
            pos,
            "DataType",
            {
                ctx.ExprCtx.NewAtom(pos, "Decimal"),
                ctx.ExprCtx.NewAtom(pos, "35"),
                ctx.ExprCtx.NewAtom(pos, "2"),
            })});
    return TExpression(
        ctx.ExprCtx.NewLambda(
            pos,
            ctx.ExprCtx.NewArguments(pos, {row}),
            ctx.ExprCtx.NewCallable(
                pos,
                "YqlAggWin",
                {
                    ctx.ExprCtx.NewCallable(
                        pos,
                        "YqlWinFactory",
                        {ctx.ExprCtx.NewAtom(
                            pos,
                            groupRefs ? TStringBuf("sum") : TStringBuf("max"))}),
                    ctx.ExprCtx.NewAtom(pos, name),
                    ctx.ExprCtx.NewList(pos, {}),
                    decimalType,
                    ctx.ExprCtx.NewCallable(
                        pos,
                        "Member",
                        {row, ctx.ExprCtx.NewAtom(pos, input.GetFullName())}),
                })),
        &ctx.ExprCtx,
        &ctx.PlanProps,
        MakeQ51WindowDefinition(
            ctx.ExprCtx,
            pos,
            name,
            partitionBy,
            orderBy,
            groupRefs));
}

int AssignSourceStage(
    TRuleTestContext& ctx,
    const TIntrusivePtr<TOpRead>& read)
{
    const auto stage = ctx.PlanProps.StageGraph.AddSourceStage(
        NYql::EStorageType::ColumnStorage);
    read->Props.StageId = stage;
    return stage;
}

void AssignStage(
    TRuleTestContext& ctx,
    const TIntrusivePtr<IOperator>& op)
{
    TIntrusivePtr<IOperator> input = op;
    TAssignStagesRule rule;
    UNIT_ASSERT(rule.MatchAndApply(input, ctx.RboCtx, ctx.PlanProps));
    UNIT_ASSERT_VALUES_EQUAL(input.Get(), op.Get());
}

const TConnection* GetOnlyConnection(
    const TPlanProps& props,
    ui32 from,
    ui32 to)
{
    const auto& connections = props.StageGraph.GetConnections(from, to);
    UNIT_ASSERT_VALUES_EQUAL(connections.size(), 1);
    return connections.front().Get();
}

void AssertSerialWindowConnection(
    const TPlanProps& props,
    ui32 from,
    ui32 to)
{
    const auto* connection = dynamic_cast<const TUnionAllConnection*>(
        GetOnlyConnection(props, from, to));
    UNIT_ASSERT(connection);
    UNIT_ASSERT(!connection->IsParallel());
}

Y_UNIT_TEST_SUITE(KqpRboStageAssignmentRules) {
    Y_UNIT_TEST(PlainMapRemainsFusedWithSource) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit value("value");
        auto read = MakeRead(pos, {value});
        const auto sourceStage = AssignSourceStage(ctx, read);
        auto map = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("projected"),
                MakeColumnAccess(
                    value,
                    pos,
                    &ctx.ExprCtx,
                    &ctx.PlanProps))});

        AssignStage(ctx, map);

        UNIT_ASSERT_VALUES_EQUAL(*map->Props.StageId, sourceStage);
        UNIT_ASSERT(ctx.PlanProps.StageGraph.Connections.empty());
    }

    Y_UNIT_TEST(TrackedWindowUsesCommonAvailablePartitionHash) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit itemClass("i_class");
        const TInfoUnit item("i_item_id");
        const TInfoUnit value("sum_value");
        auto read = MakeRead(pos, {itemClass, item, value});
        const auto sourceStage = AssignSourceStage(ctx, read);
        auto map = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{
                TMapElement(
                    TInfoUnit("first_window"),
                    MakeWindowExpression(
                        ctx,
                        value,
                        {itemClass, item})),
                TMapElement(
                    TInfoUnit("second_window"),
                    MakeWindowExpression(
                        ctx,
                        value,
                        {itemClass, value})),
            });

        AssignStage(ctx, map);

        UNIT_ASSERT(*map->Props.StageId != sourceStage);
        const auto* shuffle = dynamic_cast<const TShuffleConnection*>(
            GetOnlyConnection(
                ctx.PlanProps,
                sourceStage,
                *map->Props.StageId));
        UNIT_ASSERT(shuffle);
        UNIT_ASSERT_VALUES_EQUAL(shuffle->Keys.size(), 1);
        UNIT_ASSERT(shuffle->Keys.front() == itemClass);
    }

    Y_UNIT_TEST(UntrackedWindowGathersSerially) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit itemClass("i_class");
        const TInfoUnit value("sum_value");
        auto read = MakeRead(pos, {itemClass, value});
        const auto sourceStage = AssignSourceStage(ctx, read);
        auto map = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("window"),
                MakeUntrackedWindowExpression(ctx, value))});

        AssignStage(ctx, map);

        UNIT_ASSERT(*map->Props.StageId != sourceStage);
        AssertSerialWindowConnection(
            ctx.PlanProps,
            sourceStage,
            *map->Props.StageId);
    }

    Y_UNIT_TEST(GlobalWindowGathersSerially) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit value("sum_value");
        auto read = MakeRead(pos, {value});
        const auto sourceStage = AssignSourceStage(ctx, read);
        auto map = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("window"),
                MakeWindowExpression(ctx, value, {}))});

        AssignStage(ctx, map);

        AssertSerialWindowConnection(
            ctx.PlanProps,
            sourceStage,
            *map->Props.StageId);
    }

    Y_UNIT_TEST(ExactGlobalRankGathersSerially) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit ratio("currency_ratio");
        auto read = MakeRead(pos, {ratio});
        const auto sourceStage = AssignSourceStage(ctx, read);
        auto map = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("currency_rank"),
                MakeGlobalRankExpression(ctx, ratio))});

        AssignStage(ctx, map);

        UNIT_ASSERT(*map->Props.StageId != sourceStage);
        AssertSerialWindowConnection(
            ctx.PlanProps,
            sourceStage,
            *map->Props.StageId);
    }

    Y_UNIT_TEST(GlobalRankOrderDependencyComposesRenameBatches) {
        TRuleTestContext ctx;
        const TInfoUnit sourceRatio("source.currency_ratio");
        const TInfoUnit middleRatio("middle.currency_ratio");
        const TInfoUnit finalRatio("currency_ratio");
        auto expression = MakeGlobalRankExpression(ctx, sourceRatio);

        TExpression::TRenameMap firstBatch;
        firstBatch.emplace(sourceRatio, middleRatio);
        firstBatch.emplace(middleRatio, finalRatio);
        expression = expression.ApplyRenames(firstBatch);
        UNIT_ASSERT_VALUES_EQUAL(expression.GetWindowOrderBy().size(), 1);
        UNIT_ASSERT(expression.GetWindowOrderBy().front() == middleRatio);
        UNIT_ASSERT_VALUES_EQUAL(expression.GetInputIUs().size(), 1);
        UNIT_ASSERT(expression.GetInputIUs().front() == middleRatio);

        TExpression::TRenameMap secondBatch;
        secondBatch.emplace(middleRatio, finalRatio);
        expression = expression.ApplyRenames(secondBatch);
        UNIT_ASSERT_VALUES_EQUAL(expression.GetWindowOrderBy().size(), 1);
        UNIT_ASSERT(expression.GetWindowOrderBy().front() == finalRatio);
        UNIT_ASSERT_VALUES_EQUAL(expression.GetInputIUs().size(), 1);
        UNIT_ASSERT(expression.GetInputIUs().front() == finalRatio);
    }

    Y_UNIT_TEST(Q51InnerSumKeepsOrderLiveAndHashesOnlyPartition) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit item("item_sk");
        const TInfoUnit date("d_date");
        const TInfoUnit sum("sum_sales");
        const TInfoUnit cumulative("cume_sales");
        auto read = MakeRead(pos, {item, date, sum});
        const auto sourceStage = AssignSourceStage(ctx, read);
        auto expression = MakeQ51WindowExpression(
            ctx,
            sum,
            item,
            date,
            "_yql_anonymous_window0",
            true);

        UNIT_ASSERT_VALUES_EQUAL(expression.GetWindowPartitionBy().size(), 1);
        UNIT_ASSERT(expression.GetWindowPartitionBy().front() == item);
        UNIT_ASSERT_VALUES_EQUAL(expression.GetWindowOrderBy().size(), 1);
        UNIT_ASSERT(expression.GetWindowOrderBy().front() == date);
        const auto& dependencies = expression.GetInputIUs();
        UNIT_ASSERT_VALUES_EQUAL(dependencies.size(), 3);
        UNIT_ASSERT(std::find(dependencies.begin(), dependencies.end(), sum) != dependencies.end());
        UNIT_ASSERT(std::find(dependencies.begin(), dependencies.end(), item) != dependencies.end());
        UNIT_ASSERT(std::find(dependencies.begin(), dependencies.end(), date) != dependencies.end());

        auto map = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{TMapElement(cumulative, expression)});
        TOpRoot root(map, pos, {cumulative.GetFullName()});
        root.RecomputeOutputIUsSubtree();
        root.ComputeParents();
        ComputePlanLiveness(root);
        const auto& live = GetLiveOut(read.Get());
        UNIT_ASSERT_VALUES_EQUAL(live.size(), 3);
        UNIT_ASSERT(live.contains(sum));
        UNIT_ASSERT(live.contains(item));
        UNIT_ASSERT(live.contains(date));

        AssignStage(ctx, map);
        const auto* shuffle = dynamic_cast<const TShuffleConnection*>(
            GetOnlyConnection(
                ctx.PlanProps,
                sourceStage,
                *map->Props.StageId));
        UNIT_ASSERT(shuffle);
        UNIT_ASSERT_VALUES_EQUAL(shuffle->Keys.size(), 1);
        UNIT_ASSERT(shuffle->Keys.front() == item);
    }

    Y_UNIT_TEST(Q51OuterMaxWindowsSharePartitionOnlyHash) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit item("item_sk");
        const TInfoUnit date("d_date");
        const TInfoUnit webSales("web_sales");
        const TInfoUnit storeSales("store_sales");
        auto read = MakeRead(pos, {item, date, webSales, storeSales});
        const auto sourceStage = AssignSourceStage(ctx, read);
        auto web = MakeQ51WindowExpression(
            ctx,
            webSales,
            item,
            date,
            "_yql_anonymous_window2",
            false);
        auto store = MakeQ51WindowExpression(
            ctx,
            storeSales,
            item,
            date,
            "_yql_anonymous_window3",
            false);

        for (const auto* expression : {&web, &store}) {
            UNIT_ASSERT_VALUES_EQUAL(
                expression->GetWindowPartitionBy().size(),
                1);
            UNIT_ASSERT(expression->GetWindowPartitionBy().front() == item);
            UNIT_ASSERT_VALUES_EQUAL(expression->GetWindowOrderBy().size(), 1);
            UNIT_ASSERT(expression->GetWindowOrderBy().front() == date);
        }

        auto map = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{
                TMapElement(TInfoUnit("web_cumulative"), web),
                TMapElement(TInfoUnit("store_cumulative"), store),
            });
        AssignStage(ctx, map);
        const auto* shuffle = dynamic_cast<const TShuffleConnection*>(
            GetOnlyConnection(
                ctx.PlanProps,
                sourceStage,
                *map->Props.StageId));
        UNIT_ASSERT(shuffle);
        UNIT_ASSERT_VALUES_EQUAL(shuffle->Keys.size(), 1);
        UNIT_ASSERT(shuffle->Keys.front() == item);
    }

    Y_UNIT_TEST(UntrackedRawRankIsAConservativeSerialBarrier) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit ratio("currency_ratio");
        auto read = MakeRead(pos, {ratio});
        const auto sourceStage = AssignSourceStage(ctx, read);
        auto map = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("currency_rank"),
                MakeUntrackedRankExpression(ctx, ratio))});

        UNIT_ASSERT(!map->MapElements.front()
            .GetExpression()
            .GetWindowMetadata());
        UNIT_ASSERT(map->MapElements.front()
            .GetExpression()
            .HasWindowSemantics());
        AssignStage(ctx, map);
        AssertSerialWindowConnection(
            ctx.PlanProps,
            sourceStage,
            *map->Props.StageId);
    }

    Y_UNIT_TEST(UntrackedRawRankKeepsEveryMapInputLive) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit ratio("currency_ratio");
        const TInfoUnit hiddenOrderInput("hidden_order_input");
        const TInfoUnit rank("currency_rank");
        auto read = MakeRead(pos, {ratio, hiddenOrderInput});
        auto map = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{TMapElement(
                rank,
                MakeUntrackedRankExpression(ctx, ratio))});
        TOpRoot root(map, pos, {rank.GetFullName()});

        root.RecomputeOutputIUsSubtree();
        root.ComputeParents();
        ComputePlanLiveness(root);

        const auto& live = GetLiveOut(read.Get());
        UNIT_ASSERT_VALUES_EQUAL(live.size(), 2);
        UNIT_ASSERT(live.contains(ratio));
        UNIT_ASSERT(live.contains(hiddenOrderInput));
        const auto used = map->GetUsedIUs(root.PlanProps);
        UNIT_ASSERT_VALUES_EQUAL(used.size(), 2);
        UNIT_ASSERT(std::find(used.begin(), used.end(), ratio) != used.end());
        UNIT_ASSERT(
            std::find(used.begin(), used.end(), hiddenOrderInput) !=
            used.end());
    }

    Y_UNIT_TEST(UntrackedRawRankBlocksPreferredAliasRewrite) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit source("source_ratio");
        const TInfoUnit ratio("currency_ratio");
        const TInfoUnit rank("currency_rank");
        auto read = MakeRead(pos, {source});
        auto aliases = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{TMapElement(
                ratio,
                MakeColumnAccess(
                    source,
                    pos,
                    &ctx.ExprCtx,
                    &ctx.PlanProps))});
        auto ranks = MakeIntrusive<TOpMap>(
            aliases,
            pos,
            TVector<TMapElement>{TMapElement(
                rank,
                MakeUntrackedRankExpression(ctx, ratio))});
        TOpRoot root(ranks, pos, {rank.GetFullName()});
        root.RecomputeOutputIUsSubtree();
        root.ComputeParents();
        ComputePlanAliases(root);

        const auto* candidates = GetAliases(aliases.Get(), ratio);
        UNIT_ASSERT(candidates);
        UNIT_ASSERT_VALUES_EQUAL(candidates->size(), 2);
        const auto expression =
            ranks->MapElements.front().GetExpression().GetLambda();
        TIntrusivePtr<IOperator> input = ranks;
        TRewriteExpressionsToPreferredAliasesRule rule;

        UNIT_ASSERT(!rule.MatchAndApply(
            input,
            ctx.RboCtx,
            root.PlanProps));
        UNIT_ASSERT_VALUES_EQUAL(input.Get(), ranks.Get());
        UNIT_ASSERT_VALUES_EQUAL(
            ranks->MapElements.front().GetExpression().GetLambda().Get(),
            expression.Get());
    }

    Y_UNIT_TEST(UntrackedRawRankDoesNotMoveAcrossAnotherMap) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit source("source");
        const TInfoUnit ratio("currency_ratio");
        const TInfoUnit rank("currency_rank");
        auto read = MakeRead(pos, {source});
        auto ratios = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{TMapElement(
                ratio,
                MakeColumnAccess(
                    source,
                    pos,
                    &ctx.ExprCtx,
                    &ctx.PlanProps))});
        auto ranks = MakeIntrusive<TOpMap>(
            ratios,
            pos,
            TVector<TMapElement>{TMapElement(
                rank,
                MakeUntrackedRankExpression(ctx, ratio))});
        TOpRoot root(ranks, pos, {rank.GetFullName()});
        root.RecomputeOutputIUsSubtree();
        root.ComputeParents();

        TPushMapElementsIntoMapRule rule;
        const auto result = rule.SimpleMatchAndApply(
            ranks,
            ctx.RboCtx,
            root.PlanProps);

        UNIT_ASSERT_VALUES_EQUAL(result.Get(), ranks.Get());
        UNIT_ASSERT_VALUES_EQUAL(ranks->GetInput().Get(), ratios.Get());
    }

    Y_UNIT_TEST(GlobalRankDependenciesPreventRatioAndTraitPruning) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit item("item");
        const TInfoUnit quantity1("quantity_1");
        const TInfoUnit quantity2("quantity_2");
        const TInfoUnit amount1("amount_1");
        const TInfoUnit amount2("amount_2");
        const TInfoUnit sumQuantity1("sum_quantity_1");
        const TInfoUnit sumQuantity2("sum_quantity_2");
        const TInfoUnit sumAmount1("sum_amount_1");
        const TInfoUnit sumAmount2("sum_amount_2");
        const TInfoUnit returnRatio("return_ratio");
        const TInfoUnit currencyRatio("currency_ratio");
        const TInfoUnit returnRank("return_rank");
        const TInfoUnit currencyRank("currency_rank");

        auto read = MakeRead(
            pos,
            {item, quantity1, quantity2, amount1, amount2});
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{
                TOpAggregationTraits(quantity1, "sum", sumQuantity1),
                TOpAggregationTraits(quantity2, "sum", sumQuantity2),
                TOpAggregationTraits(amount1, "sum", sumAmount1),
                TOpAggregationTraits(amount2, "sum", sumAmount2),
            },
            TVector<TInfoUnit>{item},
            EOpPhase::Final,
            false,
            pos);
        auto ratios = MakeIntrusive<TOpMap>(
            aggregate,
            pos,
            TVector<TMapElement>{
                TMapElement(
                    returnRatio,
                    MakeBinaryPredicate(
                        "+",
                        MakeColumnAccess(
                            sumQuantity1,
                            pos,
                            &ctx.ExprCtx,
                            &ctx.PlanProps),
                        MakeColumnAccess(
                            sumQuantity2,
                            pos,
                            &ctx.ExprCtx,
                            &ctx.PlanProps))),
                TMapElement(
                    currencyRatio,
                    MakeBinaryPredicate(
                        "+",
                        MakeColumnAccess(
                            sumAmount1,
                            pos,
                            &ctx.ExprCtx,
                            &ctx.PlanProps),
                        MakeColumnAccess(
                            sumAmount2,
                            pos,
                            &ctx.ExprCtx,
                            &ctx.PlanProps))),
            });
        auto ranks = MakeIntrusive<TOpMap>(
            ratios,
            pos,
            TVector<TMapElement>{
                TMapElement(
                    returnRank,
                    MakeGlobalRankExpression(
                        ctx, returnRatio, "_return_rank")),
                TMapElement(
                    currencyRank,
                    MakeGlobalRankExpression(
                        ctx, currencyRatio, "_currency_rank")),
            });
        TOpRoot root(
            ranks,
            pos,
            {returnRank.GetFullName(), currencyRank.GetFullName()});

        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TPushMapElementsIntoMapRule>());
        rules.emplace_back(std::make_unique<TPruneDeadMapElementsRule>());
        rules.emplace_back(std::make_unique<TPruneDeadAggregateTraitsRule>());
        TRuleBasedStage pruning(
            "Focused global rank liveness",
            std::move(rules));
        pruning.RunStage(root, ctx.RboCtx);

        UNIT_ASSERT_VALUES_EQUAL(root.GetInput().Get(), ranks.Get());
        UNIT_ASSERT_VALUES_EQUAL(ranks->GetInput().Get(), ratios.Get());
        UNIT_ASSERT_VALUES_EQUAL(ratios->MapElements.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(
            aggregate->AggregationTraitsList.size(),
            4);
    }

    Y_UNIT_TEST(RenamedPartitionUsesCurrentInputHash) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit sourceClass("source_class");
        const TInfoUnit currentClass("i_class");
        const TInfoUnit value("sum_value");
        auto read = MakeRead(pos, {currentClass, value});
        const auto sourceStage = AssignSourceStage(ctx, read);
        auto expression = MakeWindowExpression(
            ctx,
            value,
            {sourceClass});
        TExpression::TRenameMap renames;
        renames.emplace(sourceClass, currentClass);
        expression = expression.ApplyRenames(renames);
        auto map = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("window"),
                expression)});

        AssignStage(ctx, map);

        const auto* shuffle = dynamic_cast<const TShuffleConnection*>(
            GetOnlyConnection(
                ctx.PlanProps,
                sourceStage,
                *map->Props.StageId));
        UNIT_ASSERT(shuffle);
        UNIT_ASSERT_VALUES_EQUAL(shuffle->Keys.size(), 1);
        UNIT_ASSERT(shuffle->Keys.front() == currentClass);
    }

    Y_UNIT_TEST(DisjointWindowPartitionsGatherSerially) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit itemClass("i_class");
        const TInfoUnit item("i_item_id");
        const TInfoUnit value("sum_value");
        auto read = MakeRead(pos, {itemClass, item, value});
        const auto sourceStage = AssignSourceStage(ctx, read);
        auto map = MakeIntrusive<TOpMap>(
            read,
            pos,
            TVector<TMapElement>{
                TMapElement(
                    TInfoUnit("first_window"),
                    MakeWindowExpression(ctx, value, {itemClass})),
                TMapElement(
                    TInfoUnit("second_window"),
                    MakeWindowExpression(ctx, value, {item})),
            });

        AssignStage(ctx, map);

        AssertSerialWindowConnection(
            ctx.PlanProps,
            sourceStage,
            *map->Props.StageId);
    }

    Y_UNIT_TEST(StaleAndDuplicateWindowMetadataGatherSerially) {
        const auto assertFallback = [](
            TStringBuf callName,
            TStringBuf definitionName,
            ui32 callCount)
        {
            TRuleTestContext ctx;
            const auto pos = TPositionHandle();
            const TInfoUnit itemClass("i_class");
            const TInfoUnit value("sum_value");
            auto read = MakeRead(pos, {itemClass, value});
            const auto sourceStage = AssignSourceStage(ctx, read);
            auto map = MakeIntrusive<TOpMap>(
                read,
                pos,
                TVector<TMapElement>{TMapElement(
                    TInfoUnit("window"),
                    MakeWindowExpression(
                        ctx,
                        value,
                        {itemClass},
                        callName,
                        definitionName,
                        callCount))});

            AssignStage(ctx, map);
            AssertSerialWindowConnection(
                ctx.PlanProps,
                sourceStage,
                *map->Props.StageId);
        };

        assertFallback("_other", "_window", 1);
        assertFallback("_window", "_window", 2);
    }

    Y_UNIT_TEST(AggregateThenWindowHasTwoShuffleBoundaries) {
        TRuleTestContext ctx;
        const auto pos = TPositionHandle();
        const TInfoUnit itemClass("i_class");
        const TInfoUnit item("i_item_id");
        const TInfoUnit value("ext_sales_price");
        const TInfoUnit sum("sum_value");
        auto read = MakeRead(pos, {itemClass, item, value});
        const auto sourceStage = AssignSourceStage(ctx, read);
        auto aggregate = MakeIntrusive<TOpAggregate>(
            read,
            TVector<TOpAggregationTraits>{TOpAggregationTraits(
                value,
                "sum",
                sum)},
            TVector<TInfoUnit>{itemClass, item},
            EOpPhase::Final,
            false,
            pos);

        AssignStage(ctx, aggregate);
        const auto aggregateStage = *aggregate->Props.StageId;
        const auto* aggregateShuffle =
            dynamic_cast<const TShuffleConnection*>(GetOnlyConnection(
                ctx.PlanProps,
                sourceStage,
                aggregateStage));
        UNIT_ASSERT(aggregateShuffle);
        UNIT_ASSERT_VALUES_EQUAL(aggregateShuffle->Keys.size(), 2);
        UNIT_ASSERT(aggregateShuffle->Keys[0] == itemClass);
        UNIT_ASSERT(aggregateShuffle->Keys[1] == item);

        auto window = MakeIntrusive<TOpMap>(
            aggregate,
            pos,
            TVector<TMapElement>{TMapElement(
                TInfoUnit("ratio"),
                MakeWindowExpression(ctx, sum, {itemClass}))});
        AssignStage(ctx, window);

        const auto windowStage = *window->Props.StageId;
        UNIT_ASSERT(windowStage != aggregateStage);
        const auto* windowShuffle =
            dynamic_cast<const TShuffleConnection*>(GetOnlyConnection(
                ctx.PlanProps,
                aggregateStage,
                windowStage));
        UNIT_ASSERT(windowShuffle);
        UNIT_ASSERT_VALUES_EQUAL(windowShuffle->Keys.size(), 1);
        UNIT_ASSERT(windowShuffle->Keys.front() == itemClass);
    }
}

} // anonymous namespace
