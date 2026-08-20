#include <ydb/core/kqp/opt/rbo/kqp_window_transport.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

#include <initializer_list>
#include <iterator>

namespace {

using namespace NKikimr::NKqp;
using namespace NYql;

TExprNode::TPtr DataType(
    TExprContext& ctx,
    TPositionHandle pos,
    std::initializer_list<TStringBuf> parameters)
{
    TExprNode::TListType atoms;
    atoms.reserve(parameters.size());
    for (const auto parameter : parameters) {
        atoms.push_back(ctx.NewAtom(pos, parameter));
    }
    return ctx.NewCallable(pos, "DataType", std::move(atoms));
}

TExprNode::TPtr RankDefinition(
    TExprContext& ctx,
    TPositionHandle pos,
    TStringBuf windowName,
    TStringBuf orderBy)
{
    auto row = ctx.NewArgument(pos, "window_row");
    auto orderLambda = ctx.NewLambda(
        pos,
        ctx.NewArguments(pos, {row}),
        ctx.NewCallable(
            pos,
            "Member",
            {row, ctx.NewAtom(pos, orderBy)}));
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
                        ctx.NewAtom(pos, orderBy),
                        DataType(ctx, pos, {"Decimal", "15", "4"}),
                    })}),
            std::move(orderLambda),
            ctx.NewAtom(pos, "asc"),
            ctx.NewAtom(pos, "first"),
        });

    return ctx.NewCallable(
        pos,
        "YqlWindow",
        {
            ctx.NewAtom(pos, windowName),
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

TExprNode::TPtr RankCall(
    TExprContext& ctx,
    TPositionHandle pos,
    TStringBuf windowName)
{
    return ctx.NewCallable(
        pos,
        "YqlWin",
        {
            ctx.NewAtom(pos, "rank"),
            ctx.NewAtom(pos, windowName),
            ctx.NewList(pos, {}),
            DataType(ctx, pos, {"Uint64"}),
        });
}

TExprNode::TPtr WindowSetting(
    TExprContext& ctx,
    TPositionHandle pos,
    TExprNode::TListType definitions)
{
    return ctx.NewList(
        pos,
        {
            ctx.NewAtom(pos, "window"),
            ctx.NewList(pos, std::move(definitions)),
        });
}

TExprNode::TPtr OrderedAggregateWindowDefinition(
    TExprContext& ctx,
    TPositionHandle pos)
{
    auto row = ctx.NewArgument(pos, "window_row");
    auto optionalString = ctx.NewCallable(
        pos,
        "OptionalType",
        {DataType(ctx, pos, {"String"})});
    auto group = ctx.NewCallable(
        pos,
        "YqlGroup",
        {
            ctx.NewCallable(
                pos,
                "StructType",
                {ctx.NewList(
                    pos,
                    {ctx.NewAtom(pos, "item"), optionalString})}),
            ctx.NewLambda(
                pos,
                ctx.NewArguments(pos, {row}),
                ctx.NewCallable(
                    pos,
                    "YqlGroupRef",
                    {
                        row,
                        optionalString,
                        ctx.NewAtom(pos, "0"),
                        ctx.NewAtom(pos, "item"),
                    })),
        });
    return ctx.NewCallable(
        pos,
        "YqlWindow",
        {
            ctx.NewAtom(pos, "_window"),
            ctx.NewAtom(pos, ""),
            ctx.NewList(pos, {std::move(group)}),
            ctx.NewList(pos, {ctx.NewAtom(pos, "ordered")}),
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
                        {ctx.NewAtom(pos, "to"), ctx.NewAtom(pos, "uf")}),
                }),
        });
}

Y_UNIT_TEST_SUITE(KqpRboWindowTransport) {
    Y_UNIT_TEST(TransportsExactTpcdsQuery49RankDefinitions) {
        TExprContext ctx;
        const auto pos = TPositionHandle();
        const TStringBuf orderColumns[] = {
            "_alias_in_web.return_ratio",
            "_alias_in_web.currency_ratio",
            "_alias_in_cat.return_ratio",
            "_alias_in_cat.currency_ratio",
            "_alias_in_store.return_ratio",
            "_alias_in_store.currency_ratio",
        };

        for (size_t index = 0; index < std::size(orderColumns); ++index) {
            const TString windowName = TStringBuilder()
                << "_yql_anonymous_window" << index;
            auto definition = RankDefinition(
                ctx, pos, windowName, orderColumns[index]);
            auto setting = WindowSetting(ctx, pos, {definition});
            const auto transported =
                NWindowTransport::FindTransportSafeWindowDefinition(
                    RankCall(ctx, pos, windowName), setting);
            UNIT_ASSERT_VALUES_EQUAL(transported.Get(), definition.Get());
        }
    }

    Y_UNIT_TEST(RejectsMalformedWindowCallsWithoutReadingMissingChildren) {
        TExprContext ctx;
        const auto pos = TPositionHandle();
        auto definition = RankDefinition(
            ctx, pos, "_window", "currency_ratio");
        auto setting = WindowSetting(ctx, pos, {definition});

        for (auto call : {
                 ctx.NewCallable(pos, "YqlWin", {}),
                 ctx.NewCallable(
                     pos,
                     "YqlWin",
                     {ctx.NewAtom(pos, "rank")}),
                 ctx.NewCallable(pos, "YqlAggWin", {}),
             })
        {
            UNIT_ASSERT(!NWindowTransport::
                FindTransportSafeWindowDefinition(call, setting));
        }
    }

    Y_UNIT_TEST(RejectsOrderedAggregateWindowMetadata) {
        TExprContext ctx;
        const auto pos = TPositionHandle();
        auto definition = OrderedAggregateWindowDefinition(ctx, pos);
        auto setting = WindowSetting(ctx, pos, {definition});
        auto aggregateCall = ctx.NewCallable(
            pos,
            "YqlAggWin",
            {
                ctx.NewAtom(pos, "factory"),
                ctx.NewAtom(pos, "_window"),
                ctx.NewList(pos, {}),
                ctx.NewAtom(pos, "type"),
                ctx.NewAtom(pos, "input"),
            });

        UNIT_ASSERT(!NWindowTransport::FindTransportSafeWindowDefinition(
            aggregateCall, setting));
    }
}

} // anonymous namespace
