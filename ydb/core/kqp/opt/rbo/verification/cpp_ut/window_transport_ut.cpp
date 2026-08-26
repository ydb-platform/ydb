#include <ydb/core/kqp/opt/rbo/kqp_window_transport.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

#include <initializer_list>
#include <iterator>

namespace {

using namespace NKikimr::NKqp;
using namespace NYql;

enum class ERankTransportMutation {
    None,
    WrongFunction,
    NonemptyOptions,
    WrongResultType,
    NonemptyPartition,
    MissingOrder,
    WrongOrderType,
    WrongDirection,
    WrongNullOrder,
    ForeignOrderBinder,
    MismatchedOrderMember,
    WrongFrameEnd,
    WrongCurrentRow,
};

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
    TStringBuf orderBy,
    ERankTransportMutation mutation = ERankTransportMutation::None)
{
    auto row = ctx.NewArgument(pos, "window_row");
    auto memberRow = mutation == ERankTransportMutation::ForeignOrderBinder
        ? ctx.NewArgument(pos, "foreign_window_row")
        : row;
    auto orderLambda = ctx.NewLambda(
        pos,
        ctx.NewArguments(pos, {row}),
        ctx.NewCallable(
            pos,
            "Member",
            {
                memberRow,
                ctx.NewAtom(
                    pos,
                    mutation == ERankTransportMutation::MismatchedOrderMember
                        ? TStringBuf("other_ratio")
                        : orderBy),
            }));
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
                        DataType(
                            ctx,
                            pos,
                            {
                                "Decimal",
                                mutation == ERankTransportMutation::WrongOrderType
                                    ? TStringBuf("16")
                                    : TStringBuf("15"),
                                "4",
                            }),
                    })}),
            std::move(orderLambda),
            ctx.NewAtom(
                pos,
                mutation == ERankTransportMutation::WrongDirection
                    ? TStringBuf("desc")
                    : TStringBuf("asc")),
            ctx.NewAtom(
                pos,
                mutation == ERankTransportMutation::WrongNullOrder
                    ? TStringBuf("last")
                    : TStringBuf("first")),
        });
    TExprNode::TListType partitions;
    if (mutation == ERankTransportMutation::NonemptyPartition) {
        partitions.push_back(ctx.NewAtom(pos, "unexpected_partition"));
    }
    TExprNode::TListType order;
    if (mutation != ERankTransportMutation::MissingOrder) {
        order.push_back(std::move(sort));
    }

    return ctx.NewCallable(
        pos,
        "YqlWindow",
        {
            ctx.NewAtom(pos, windowName),
            ctx.NewAtom(pos, ""),
            ctx.NewList(pos, std::move(partitions)),
            ctx.NewList(pos, std::move(order)),
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
                        {
                            ctx.NewAtom(pos, "to"),
                            ctx.NewAtom(
                                pos,
                                mutation == ERankTransportMutation::WrongFrameEnd
                                    ? TStringBuf("uf")
                                    : TStringBuf("f")),
                        }),
                    ctx.NewList(
                        pos,
                        {
                            ctx.NewAtom(pos, "to_value"),
                            ctx.NewCallable(
                                pos,
                                "Int32",
                                {ctx.NewAtom(
                                    pos,
                                    mutation == ERankTransportMutation::WrongCurrentRow
                                        ? TStringBuf("1")
                                        : TStringBuf("0"))}),
                        }),
                }),
        });
}

TExprNode::TPtr RankCall(
    TExprContext& ctx,
    TPositionHandle pos,
    TStringBuf windowName,
    ERankTransportMutation mutation = ERankTransportMutation::None)
{
    TExprNode::TListType options;
    if (mutation == ERankTransportMutation::NonemptyOptions) {
        options.push_back(ctx.NewAtom(pos, "ansi"));
    }
    return ctx.NewCallable(
        pos,
        "YqlWin",
        {
            ctx.NewAtom(
                pos,
                mutation == ERankTransportMutation::WrongFunction
                    ? TStringBuf("dense_rank")
                    : TStringBuf("rank")),
            ctx.NewAtom(pos, windowName),
            ctx.NewList(pos, std::move(options)),
            DataType(
                ctx,
                pos,
                {
                    mutation == ERankTransportMutation::WrongResultType
                        ? TStringBuf("Int64")
                        : TStringBuf("Uint64"),
                }),
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

    Y_UNIT_TEST(RankTransportGrammarFailsClosedForNearMisses) {
        struct TCase {
            ERankTransportMutation Mutation;
            TStringBuf Name;
        };
        const TCase cases[] = {
            {ERankTransportMutation::WrongFunction, "wrong function"},
            {ERankTransportMutation::NonemptyOptions, "nonempty options"},
            {ERankTransportMutation::WrongResultType, "wrong result type"},
            {ERankTransportMutation::NonemptyPartition, "nonempty partition"},
            {ERankTransportMutation::MissingOrder, "missing order"},
            {ERankTransportMutation::WrongOrderType, "wrong order type"},
            {ERankTransportMutation::WrongDirection, "wrong direction"},
            {ERankTransportMutation::WrongNullOrder, "wrong null order"},
            {ERankTransportMutation::ForeignOrderBinder, "foreign order binder"},
            {ERankTransportMutation::MismatchedOrderMember, "mismatched order member"},
            {ERankTransportMutation::WrongFrameEnd, "wrong frame end"},
            {ERankTransportMutation::WrongCurrentRow, "wrong current row"},
        };

        for (const auto& test : cases) {
            TExprContext ctx;
            const auto pos = TPositionHandle();
            const TStringBuf windowName = "_window";
            auto definition = RankDefinition(
                ctx,
                pos,
                windowName,
                "currency_ratio",
                test.Mutation);
            auto setting = WindowSetting(ctx, pos, {definition});
            auto call = RankCall(ctx, pos, windowName, test.Mutation);

            UNIT_ASSERT_C(
                !NWindowTransport::FindTransportSafeWindowDefinition(
                    call,
                    setting),
                TStringBuilder()
                    << "near miss unexpectedly transported: " << test.Name);
        }
    }

    Y_UNIT_TEST(RankTransportRejectsMissingDuplicateAndMismatchedDefinitions) {
        enum class EDefinitionMutation {
            Missing,
            Duplicate,
            MismatchedName,
            MultipleCalls,
        };
        struct TCase {
            EDefinitionMutation Mutation;
            TStringBuf Name;
        };
        const TCase cases[] = {
            {EDefinitionMutation::Missing, "missing definition"},
            {EDefinitionMutation::Duplicate, "duplicate definition"},
            {EDefinitionMutation::MismatchedName, "mismatched definition name"},
            {EDefinitionMutation::MultipleCalls, "multiple window calls"},
        };

        for (const auto& test : cases) {
            TExprContext ctx;
            const auto pos = TPositionHandle();
            const TStringBuf windowName = "_window";
            TExprNode::TListType definitions;
            if (test.Mutation != EDefinitionMutation::Missing) {
                definitions.push_back(RankDefinition(
                    ctx,
                    pos,
                    test.Mutation == EDefinitionMutation::MismatchedName
                        ? TStringBuf("_other_window")
                        : windowName,
                    "currency_ratio"));
            }
            if (test.Mutation == EDefinitionMutation::Duplicate) {
                definitions.push_back(RankDefinition(
                    ctx,
                    pos,
                    windowName,
                    "currency_ratio"));
            }
            auto setting = WindowSetting(ctx, pos, std::move(definitions));
            auto expression = RankCall(ctx, pos, windowName);
            if (test.Mutation == EDefinitionMutation::MultipleCalls) {
                expression = ctx.NewCallable(
                    pos,
                    "AsTuple",
                    {
                        expression,
                        RankCall(ctx, pos, windowName),
                    });
            }

            UNIT_ASSERT_C(
                !NWindowTransport::FindTransportSafeWindowDefinition(
                    expression,
                    setting),
                TStringBuilder()
                    << "near miss unexpectedly transported: " << test.Name);
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
