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

enum class EQ51WindowFunction {
    Sum,
    Max,
};

enum class EQ51TransportMutation {
    None,
    WrongFunction,
    NonemptyOptions,
    WrongResultType,
    NonMemberInput,
    ForeignInputBinder,
    Inherited,
    MissingPartition,
    WrongPartitionType,
    WrongPartitionReference,
    WrongPartitionIndex,
    MismatchedPartitionName,
    ForeignPartitionBinder,
    MissingOrder,
    WrongOrderType,
    WrongOrderReference,
    WrongOrderIndex,
    MismatchedOrderName,
    ForeignOrderBinder,
    WrongDirection,
    WrongNullOrder,
    WrongFrameEnd,
    WrongCurrentRow,
};

TExprNode::TPtr OptionalType(
    TExprContext& ctx,
    TPositionHandle pos,
    std::initializer_list<TStringBuf> parameters)
{
    return ctx.NewCallable(
        pos,
        "OptionalType",
        {DataType(ctx, pos, parameters)});
}

TExprNode::TPtr Q51WindowReference(
    TExprContext& ctx,
    TPositionHandle pos,
    const TExprNode::TPtr& row,
    const TExprNode::TPtr& type,
    TStringBuf name,
    TStringBuf index,
    bool groupRef,
    bool foreignBinder,
    bool mismatchedName)
{
    auto referenceRow = foreignBinder
        ? ctx.NewArgument(pos, "foreign_window_row")
        : row;
    const TStringBuf referenceName = mismatchedName
        ? TStringBuf("other_column")
        : name;
    if (groupRef) {
        return ctx.NewCallable(
            pos,
            "YqlGroupRef",
            {
                referenceRow,
                type,
                ctx.NewAtom(pos, index),
                ctx.NewAtom(pos, referenceName),
            });
    }
    return ctx.NewCallable(
        pos,
        "Member",
        {
            referenceRow,
            ctx.NewAtom(pos, referenceName),
        });
}

TExprNode::TPtr Q51OrderedAggregateWindowDefinition(
    TExprContext& ctx,
    TPositionHandle pos,
    TStringBuf windowName,
    TStringBuf partitionName,
    TStringBuf orderName,
    EQ51WindowFunction function,
    EQ51TransportMutation mutation = EQ51TransportMutation::None)
{
    const bool sum = function == EQ51WindowFunction::Sum;
    auto partitionType = mutation == EQ51TransportMutation::WrongPartitionType
        ? OptionalType(ctx, pos, {"String"})
        : sum
            ? DataType(ctx, pos, {"Int64"})
            : OptionalType(ctx, pos, {"Int64"});
    auto partitionRow = ctx.NewArgument(pos, "partition_row");
    const bool partitionGroupRef =
        mutation == EQ51TransportMutation::WrongPartitionReference
            ? !sum
            : sum;
    auto partition = ctx.NewCallable(
        pos,
        "YqlGroup",
        {
            ctx.NewCallable(
                pos,
                "StructType",
                {ctx.NewList(
                    pos,
                    {ctx.NewAtom(pos, partitionName), partitionType})}),
            ctx.NewLambda(
                pos,
                ctx.NewArguments(pos, {partitionRow}),
                Q51WindowReference(
                    ctx,
                    pos,
                    partitionRow,
                    partitionType,
                    partitionName,
                    mutation == EQ51TransportMutation::WrongPartitionIndex
                        ? TStringBuf("2")
                        : TStringBuf("0"),
                    partitionGroupRef,
                    mutation == EQ51TransportMutation::ForeignPartitionBinder,
                    mutation == EQ51TransportMutation::MismatchedPartitionName)),
        });

    auto orderType = mutation == EQ51TransportMutation::WrongOrderType
        ? OptionalType(ctx, pos, {"Datetime"})
        : OptionalType(ctx, pos, {"Date"});
    auto orderRow = ctx.NewArgument(pos, "order_row");
    const bool orderGroupRef =
        mutation == EQ51TransportMutation::WrongOrderReference
            ? !sum
            : sum;
    auto order = ctx.NewCallable(
        pos,
        "YqlSort",
        {
            ctx.NewCallable(
                pos,
                "StructType",
                {ctx.NewList(
                    pos,
                    {ctx.NewAtom(pos, orderName), orderType})}),
            ctx.NewLambda(
                pos,
                ctx.NewArguments(pos, {orderRow}),
                Q51WindowReference(
                    ctx,
                    pos,
                    orderRow,
                    orderType,
                    orderName,
                    mutation == EQ51TransportMutation::WrongOrderIndex
                        ? TStringBuf("2")
                        : TStringBuf("1"),
                    orderGroupRef,
                    mutation == EQ51TransportMutation::ForeignOrderBinder,
                    mutation == EQ51TransportMutation::MismatchedOrderName)),
            ctx.NewAtom(
                pos,
                mutation == EQ51TransportMutation::WrongDirection
                    ? TStringBuf("desc")
                    : TStringBuf("asc")),
            ctx.NewAtom(
                pos,
                mutation == EQ51TransportMutation::WrongNullOrder
                    ? TStringBuf("last")
                    : TStringBuf("first")),
        });

    TExprNode::TListType partitions;
    if (mutation != EQ51TransportMutation::MissingPartition) {
        partitions.push_back(std::move(partition));
    }
    TExprNode::TListType orderBy;
    if (mutation != EQ51TransportMutation::MissingOrder) {
        orderBy.push_back(std::move(order));
    }
    return ctx.NewCallable(
        pos,
        "YqlWindow",
        {
            ctx.NewAtom(pos, windowName),
            ctx.NewAtom(
                pos,
                mutation == EQ51TransportMutation::Inherited
                    ? TStringBuf("base_window")
                    : TStringBuf("")),
            ctx.NewList(pos, std::move(partitions)),
            ctx.NewList(pos, std::move(orderBy)),
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
                                mutation == EQ51TransportMutation::WrongFrameEnd
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
                                    mutation == EQ51TransportMutation::WrongCurrentRow
                                        ? TStringBuf("1")
                                        : TStringBuf("0"))}),
                        }),
                }),
        });
}

TExprNode::TPtr Q51OrderedAggregateWindowCall(
    TExprContext& ctx,
    TPositionHandle pos,
    TStringBuf windowName,
    TStringBuf inputName,
    EQ51WindowFunction function,
    EQ51TransportMutation mutation = EQ51TransportMutation::None)
{
    auto row = ctx.NewArgument(pos, "window_input_row");
    auto inputRow = mutation == EQ51TransportMutation::ForeignInputBinder
        ? ctx.NewArgument(pos, "foreign_window_input_row")
        : row;
    TExprNode::TListType options;
    if (mutation == EQ51TransportMutation::NonemptyOptions) {
        options.push_back(ctx.NewAtom(pos, "distinct"));
    }
    auto input = mutation == EQ51TransportMutation::NonMemberInput
        ? ctx.NewAtom(pos, inputName)
        : ctx.NewCallable(
            pos,
            "Member",
            {inputRow, ctx.NewAtom(pos, inputName)});
    auto call = ctx.NewCallable(
        pos,
        "YqlAggWin",
        {
            ctx.NewCallable(
                pos,
                "YqlWinFactory",
                {ctx.NewAtom(
                    pos,
                    mutation == EQ51TransportMutation::WrongFunction
                        ? TStringBuf("avg")
                        : function == EQ51WindowFunction::Sum
                            ? TStringBuf("sum")
                            : TStringBuf("max"))}),
            ctx.NewAtom(pos, windowName),
            ctx.NewList(pos, std::move(options)),
            OptionalType(
                ctx,
                pos,
                {
                    "Decimal",
                    mutation == EQ51TransportMutation::WrongResultType
                        ? TStringBuf("34")
                        : TStringBuf("35"),
                    "2",
                }),
            std::move(input),
        });
    return ctx.NewLambda(
        pos,
        ctx.NewArguments(pos, {row}),
        std::move(call));
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

    Y_UNIT_TEST(TransportsExactTpcdsQuery51OrderedAggregateDefinitions) {
        struct TCase {
            TStringBuf WindowName;
            TStringBuf Partition;
            TStringBuf Order;
            TStringBuf Input;
            EQ51WindowFunction Function;
        };
        const TCase cases[] = {
            {
                "_yql_anonymous_window0",
                "_alias_/Root/test/ds/web_sales.ws_item_sk",
                "_alias_/Root/test/ds/date_dim.d_date",
                "__kqp_agg_result_agg_col_0",
                EQ51WindowFunction::Sum,
            },
            {
                "_yql_anonymous_window1",
                "_alias_/Root/test/ds/store_sales.ss_item_sk",
                "_alias_/Root/test/ds/date_dim.d_date",
                "__kqp_agg_result_agg_col_0",
                EQ51WindowFunction::Sum,
            },
            {
                "_yql_anonymous_window2",
                "_alias_x.item_sk",
                "_alias_x.d_date",
                "x.web_sales",
                EQ51WindowFunction::Max,
            },
            {
                "_yql_anonymous_window3",
                "_alias_x.item_sk",
                "_alias_x.d_date",
                "x.store_sales",
                EQ51WindowFunction::Max,
            },
        };

        for (const auto& test : cases) {
            TExprContext ctx;
            const auto pos = TPositionHandle();
            auto definition = Q51OrderedAggregateWindowDefinition(
                ctx,
                pos,
                test.WindowName,
                test.Partition,
                test.Order,
                test.Function);
            auto setting = WindowSetting(ctx, pos, {definition});
            const auto transported =
                NWindowTransport::FindTransportSafeWindowDefinition(
                    Q51OrderedAggregateWindowCall(
                        ctx,
                        pos,
                        test.WindowName,
                        test.Input,
                        test.Function),
                    setting);
            UNIT_ASSERT_VALUES_EQUAL(transported.Get(), definition.Get());
        }
    }

    Y_UNIT_TEST(Q51OrderedAggregateTransportFailsClosedForNearMisses) {
        struct TCase {
            EQ51WindowFunction Function;
            EQ51TransportMutation Mutation;
            TStringBuf Name;
        };
        const TCase cases[] = {
            {EQ51WindowFunction::Sum, EQ51TransportMutation::WrongFunction, "wrong function"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::NonemptyOptions, "nonempty options"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::WrongResultType, "wrong result type"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::NonMemberInput, "non-Member input"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::ForeignInputBinder, "foreign input binder"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::Inherited, "inherited window"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::MissingPartition, "missing partition"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::WrongPartitionType, "wrong SUM partition type"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::WrongPartitionReference, "SUM Member partition"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::WrongPartitionIndex, "wrong partition index"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::MismatchedPartitionName, "mismatched partition name"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::ForeignPartitionBinder, "foreign partition binder"},
            {EQ51WindowFunction::Max, EQ51TransportMutation::WrongPartitionType, "wrong MAX partition type"},
            {EQ51WindowFunction::Max, EQ51TransportMutation::WrongPartitionReference, "MAX GroupRef partition"},
            {EQ51WindowFunction::Max, EQ51TransportMutation::MismatchedPartitionName, "mismatched MAX partition name"},
            {EQ51WindowFunction::Max, EQ51TransportMutation::ForeignPartitionBinder, "foreign MAX partition binder"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::MissingOrder, "missing order"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::WrongOrderType, "wrong order type"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::WrongOrderReference, "SUM Member order"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::WrongOrderIndex, "wrong order index"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::MismatchedOrderName, "mismatched order name"},
            {EQ51WindowFunction::Sum, EQ51TransportMutation::ForeignOrderBinder, "foreign order binder"},
            {EQ51WindowFunction::Max, EQ51TransportMutation::WrongOrderReference, "MAX GroupRef order"},
            {EQ51WindowFunction::Max, EQ51TransportMutation::MismatchedOrderName, "mismatched MAX order name"},
            {EQ51WindowFunction::Max, EQ51TransportMutation::ForeignOrderBinder, "foreign MAX order binder"},
            {EQ51WindowFunction::Max, EQ51TransportMutation::WrongDirection, "wrong direction"},
            {EQ51WindowFunction::Max, EQ51TransportMutation::WrongNullOrder, "wrong NULL order"},
            {EQ51WindowFunction::Max, EQ51TransportMutation::WrongFrameEnd, "wrong frame end"},
            {EQ51WindowFunction::Max, EQ51TransportMutation::WrongCurrentRow, "wrong current row"},
        };

        for (const auto& test : cases) {
            TExprContext ctx;
            const auto pos = TPositionHandle();
            const TStringBuf windowName = "_window";
            auto definition = Q51OrderedAggregateWindowDefinition(
                ctx,
                pos,
                windowName,
                "item_sk",
                "d_date",
                test.Function,
                test.Mutation);
            auto setting = WindowSetting(ctx, pos, {definition});
            auto call = Q51OrderedAggregateWindowCall(
                ctx,
                pos,
                windowName,
                "sales",
                test.Function,
                test.Mutation);
            UNIT_ASSERT_C(
                !NWindowTransport::FindTransportSafeWindowDefinition(
                    call,
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
