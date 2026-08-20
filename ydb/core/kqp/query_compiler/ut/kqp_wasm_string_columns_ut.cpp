#include <ydb/core/kqp/query_compiler/kqp_wasm_string_columns.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

using namespace NKikimr::NKqp;
using namespace NYql;
using namespace NYql::NNodes;

namespace {

TExprNode::TPtr MakeAtomList(TExprContext& ctx, TPositionHandle pos, const TVector<TString>& items) {
    TExprNode::TListType atoms;
    for (const auto& item : items) {
        atoms.push_back(ctx.NewAtom(pos, item));
    }
    return ctx.NewList(pos, std::move(atoms));
}

TExprNode::TPtr MakeKqpTable(TExprContext& ctx, TPositionHandle pos) {
    return ctx.NewCallable(pos, "KqpTable", {
        ctx.NewAtom(pos, "/Root/table"),
        ctx.NewAtom(pos, "1"),
        ctx.NewAtom(pos, ""),
        ctx.NewAtom(pos, "0"),
    });
}

//! DqSource over a datashard row read: the only source honoring PreferWasm.
TExprNode::TPtr MakeRowsSource(TExprContext& ctx, TPositionHandle pos, const TVector<TString>& columns) {
    auto settings = ctx.NewCallable(pos, "KqpRowsSourceSettings", {
        MakeKqpTable(ctx, pos),
        MakeAtomList(ctx, pos, columns),
        ctx.NewList(pos, TExprNode::TListType{}),
        ctx.NewCallable(pos, "Void", {}),
    });
    auto dataSource = ctx.NewCallable(pos, "DataSource", {
        ctx.NewAtom(pos, "kikimr"),
        ctx.NewAtom(pos, "db"),
    });
    return ctx.NewCallable(pos, "DqSource", {dataSource, settings});
}

TExprNode::TPtr MakeWideRead(TExprContext& ctx, TPositionHandle pos, const TVector<TString>& columns) {
    return ctx.NewCallable(pos, "KqpWideReadTable", {
        MakeKqpTable(ctx, pos),
        ctx.NewCallable(pos, "Void", {}),
        MakeAtomList(ctx, pos, columns),
        ctx.NewList(pos, TExprNode::TListType{}),
    });
}

TExprNode::TPtr MakeUdf(TExprContext& ctx, TPositionHandle pos) {
    return ctx.NewCallable(pos, "Udf", {ctx.NewAtom(pos, "WasmMod.Func")});
}

TExprNode::TPtr MakeApplyUdf(TExprContext& ctx, TPositionHandle pos, const TExprNode::TPtr& arg) {
    return ctx.NewCallable(pos, "Apply", {MakeUdf(ctx, pos), arg});
}

TExprNode::TPtr MakeMember(TExprContext& ctx, TPositionHandle pos, const TExprNode::TPtr& row, TStringBuf name) {
    return ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, name)});
}

TExprNode::TPtr MakeStage(
    TExprContext& ctx,
    TPositionHandle pos,
    TExprNode::TListType inputs,
    const TExprNode::TPtr& program)
{
    return ctx.NewCallable(pos, "DqPhyStage", {
        ctx.NewList(pos, std::move(inputs)),
        program,
        ctx.NewList(pos, TExprNode::TListType{}),
    });
}

TVector<TString> Collect(
    TExprContext& ctx,
    TPositionHandle pos,
    TExprNode::TListType inputs,
    const TExprNode::TPtr& program)
{
    auto stage = MakeStage(ctx, pos, std::move(inputs), program);
    const auto result = CollectWasmUdfStringColumns(TDqPhyStage(stage));
    TVector<TString> columns(result.Columns.begin(), result.Columns.end());
    Sort(columns);
    return columns;
}

const TVector<TString> SourceColumns = {"id", "blob", "unused_blob"};

} // namespace

Y_UNIT_TEST_SUITE(KqpWasmStringColumns) {

Y_UNIT_TEST(RowSourceMemberArg) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto input = ctx.NewArgument(pos, "input");
    auto row = ctx.NewArgument(pos, "row");
    auto rowLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}),
        MakeApplyUdf(ctx, pos, MakeMember(ctx, pos, row, "blob")));
    auto body = ctx.NewCallable(pos, "Map", {
        ctx.NewCallable(pos, "ToFlow", {input}),
        rowLambda,
    });
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {input}), std::move(body));

    const auto columns = Collect(ctx, pos, {MakeRowsSource(ctx, pos, SourceColumns)}, program);
    UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(columns[0], "blob");
}

Y_UNIT_TEST(RowSourceAutoMapUnwrapsOptionalArg) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto input = ctx.NewArgument(pos, "input");
    auto row = ctx.NewArgument(pos, "row");
    auto value = ctx.NewArgument(pos, "value");

    // AutoMap rewrite of an optional argument:
    // FlatMap(Member(row, blob), λ(value) → Just(Apply(Udf, value)))
    auto valueLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {value}),
        ctx.NewCallable(pos, "Just", {MakeApplyUdf(ctx, pos, value)}));
    auto rowLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}),
        ctx.NewCallable(pos, "FlatMap", {MakeMember(ctx, pos, row, "blob"), valueLambda}));
    auto body = ctx.NewCallable(pos, "Map", {
        ctx.NewCallable(pos, "ToFlow", {input}),
        rowLambda,
    });
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {input}), std::move(body));

    const auto columns = Collect(ctx, pos, {MakeRowsSource(ctx, pos, SourceColumns)}, program);
    UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(columns[0], "blob");
}

//! The shape KQP actually builds for SELECT id, Udf(blob) FROM t ORDER BY id:
//! the Apply sits right inside the ExpandMap lambda of the source stage.
Y_UNIT_TEST(RowSourceApplyInsideExpandMap) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto input = ctx.NewArgument(pos, "input");
    auto row = ctx.NewArgument(pos, "row");
    auto expandLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), TExprNode::TListType{
        MakeMember(ctx, pos, row, "id"),
        MakeApplyUdf(ctx, pos, MakeMember(ctx, pos, row, "blob")),
    });
    auto body = ctx.NewCallable(pos, "FromFlow", {
        ctx.NewCallable(pos, "ExpandMap", {ctx.NewCallable(pos, "ToFlow", {input}), expandLambda}),
    });
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {input}), std::move(body));

    const auto columns = Collect(ctx, pos, {MakeRowsSource(ctx, pos, SourceColumns)}, program);
    UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(columns[0], "blob");
}

Y_UNIT_TEST(RowSourceExpandMapThenWideMap) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto input = ctx.NewArgument(pos, "input");
    auto row = ctx.NewArgument(pos, "row");

    auto expandLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), TExprNode::TListType{
        MakeMember(ctx, pos, row, "id"),
        MakeMember(ctx, pos, row, "blob"),
    });
    auto expandMap = ctx.NewCallable(pos, "ExpandMap", {
        ctx.NewCallable(pos, "ToFlow", {input}),
        expandLambda,
    });

    auto idArg = ctx.NewArgument(pos, "idArg");
    auto blobArg = ctx.NewArgument(pos, "blobArg");
    auto wideLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {idArg, blobArg}), TExprNode::TListType{
        idArg,
        MakeApplyUdf(ctx, pos, blobArg),
    });
    auto body = ctx.NewCallable(pos, "FromFlow", {
        ctx.NewCallable(pos, "WideMap", {expandMap, wideLambda}),
    });
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {input}), std::move(body));

    const auto columns = Collect(ctx, pos, {MakeRowsSource(ctx, pos, SourceColumns)}, program);
    UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(columns[0], "blob");
}

//! The shape KQP builds for SELECT SUM(Udf(blob)) FROM t: no ExpandMap of the
//! row, the UDF sits inside the Condense1 handlers instead.
Y_UNIT_TEST(RowSourceApplyInsideCondense1) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto input = ctx.NewArgument(pos, "input");
    auto initItem = ctx.NewArgument(pos, "initItem");
    auto switchItem = ctx.NewArgument(pos, "switchItem");
    auto switchState = ctx.NewArgument(pos, "switchState");
    auto updateItem = ctx.NewArgument(pos, "updateItem");
    auto updateState = ctx.NewArgument(pos, "updateState");

    auto identity = ctx.NewArgument(pos, "identity");
    auto condense = ctx.NewCallable(pos, "Condense1", {
        ctx.NewCallable(pos, "ToFlow", {input}),
        ctx.NewLambda(pos, ctx.NewArguments(pos, {initItem}),
            MakeApplyUdf(ctx, pos, MakeMember(ctx, pos, initItem, "blob"))),
        ctx.NewLambda(pos, ctx.NewArguments(pos, {switchItem, switchState}),
            ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, "false")})),
        ctx.NewLambda(pos, ctx.NewArguments(pos, {updateItem, updateState}),
            ctx.NewCallable(pos, "AggrAdd", {
                MakeApplyUdf(ctx, pos, MakeMember(ctx, pos, updateItem, "blob")),
                updateState,
            })),
    });
    auto body = ctx.NewCallable(pos, "FromFlow", {
        ctx.NewCallable(pos, "ExpandMap", {
            condense,
            ctx.NewLambda(pos, ctx.NewArguments(pos, {identity}), TExprNode::TPtr(identity)),
        }),
    });
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {input}), std::move(body));

    const auto columns = Collect(ctx, pos, {MakeRowsSource(ctx, pos, SourceColumns)}, program);
    UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(columns[0], "blob");
}

//! GROUP BY: the item is the second argument of the init and update handlers,
//! the first one being the key.
Y_UNIT_TEST(RowSourceApplyInsideCombineCore) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto input = ctx.NewArgument(pos, "input");
    auto keyItem = ctx.NewArgument(pos, "keyItem");
    auto initKey = ctx.NewArgument(pos, "initKey");
    auto initItem = ctx.NewArgument(pos, "initItem");
    auto updateKey = ctx.NewArgument(pos, "updateKey");
    auto updateItem = ctx.NewArgument(pos, "updateItem");
    auto updateState = ctx.NewArgument(pos, "updateState");
    auto finishKey = ctx.NewArgument(pos, "finishKey");
    auto finishState = ctx.NewArgument(pos, "finishState");

    auto body = ctx.NewCallable(pos, "FromFlow", {
        ctx.NewCallable(pos, "CombineCore", {
            ctx.NewCallable(pos, "ToFlow", {input}),
            ctx.NewLambda(pos, ctx.NewArguments(pos, {keyItem}),
                MakeMember(ctx, pos, keyItem, "id")),
            ctx.NewLambda(pos, ctx.NewArguments(pos, {initKey, initItem}),
                MakeApplyUdf(ctx, pos, MakeMember(ctx, pos, initItem, "blob"))),
            ctx.NewLambda(pos, ctx.NewArguments(pos, {updateKey, updateItem, updateState}),
                ctx.NewCallable(pos, "AggrAdd", {
                    MakeApplyUdf(ctx, pos, MakeMember(ctx, pos, updateItem, "blob")),
                    updateState,
                })),
            ctx.NewLambda(pos, ctx.NewArguments(pos, {finishKey, finishState}), TExprNode::TPtr(finishState)),
            ctx.NewAtom(pos, "0"),
        }),
    });
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {input}), std::move(body));

    const auto columns = Collect(ctx, pos, {MakeRowsSource(ctx, pos, SourceColumns)}, program);
    UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(columns[0], "blob");
}

//! The condense state is a value of its own: whatever the handler put there is
//! no longer the buffer the read produced, so its members stay unmarked.
Y_UNIT_TEST(Condense1StateIsNotARow) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto input = ctx.NewArgument(pos, "input");
    auto initItem = ctx.NewArgument(pos, "initItem");
    auto switchItem = ctx.NewArgument(pos, "switchItem");
    auto switchState = ctx.NewArgument(pos, "switchState");
    auto updateItem = ctx.NewArgument(pos, "updateItem");
    auto updateState = ctx.NewArgument(pos, "updateState");

    auto body = ctx.NewCallable(pos, "FromFlow", {
        ctx.NewCallable(pos, "Condense1", {
            ctx.NewCallable(pos, "ToFlow", {input}),
            ctx.NewLambda(pos, ctx.NewArguments(pos, {initItem}), TExprNode::TPtr(initItem)),
            ctx.NewLambda(pos, ctx.NewArguments(pos, {switchItem, switchState}),
                ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, "false")})),
            ctx.NewLambda(pos, ctx.NewArguments(pos, {updateItem, updateState}),
                MakeApplyUdf(ctx, pos, MakeMember(ctx, pos, updateState, "blob"))),
        }),
    });
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {input}), std::move(body));

    UNIT_ASSERT(Collect(ctx, pos, {MakeRowsSource(ctx, pos, SourceColumns)}, program).empty());
}

Y_UNIT_TEST(WideReadInProgram) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto blobArg = ctx.NewArgument(pos, "blobArg");
    auto unusedArg = ctx.NewArgument(pos, "unusedArg");
    auto wideLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {blobArg, unusedArg}), TExprNode::TListType{
        MakeApplyUdf(ctx, pos, blobArg),
    });
    auto body = ctx.NewCallable(pos, "FromFlow", {
        ctx.NewCallable(pos, "WideMap", {
            ctx.NewCallable(pos, "ToFlow", {MakeWideRead(ctx, pos, {"blob", "unused_blob"})}),
            wideLambda,
        }),
    });
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {}), std::move(body));

    const auto columns = Collect(ctx, pos, {}, program);
    UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(columns[0], "blob");
}

Y_UNIT_TEST(SeveralArgsFromSeveralColumns) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto input = ctx.NewArgument(pos, "input");
    auto row = ctx.NewArgument(pos, "row");
    auto apply = ctx.NewCallable(pos, "Apply", {
        MakeUdf(ctx, pos),
        MakeMember(ctx, pos, row, "blob"),
        MakeMember(ctx, pos, row, "unused_blob"),
    });
    auto rowLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(apply));
    auto body = ctx.NewCallable(pos, "Map", {ctx.NewCallable(pos, "ToFlow", {input}), rowLambda});
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {input}), std::move(body));

    const auto columns = Collect(ctx, pos, {MakeRowsSource(ctx, pos, SourceColumns)}, program);
    UNIT_ASSERT_VALUES_EQUAL(columns.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(columns[0], "blob");
    UNIT_ASSERT_VALUES_EQUAL(columns[1], "unused_blob");
}

Y_UNIT_TEST(NoUdfInStage) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto input = ctx.NewArgument(pos, "input");
    auto row = ctx.NewArgument(pos, "row");
    auto rowLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), MakeMember(ctx, pos, row, "blob"));
    auto body = ctx.NewCallable(pos, "Map", {ctx.NewCallable(pos, "ToFlow", {input}), rowLambda});
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {input}), std::move(body));

    UNIT_ASSERT(Collect(ctx, pos, {MakeRowsSource(ctx, pos, SourceColumns)}, program).empty());
}

Y_UNIT_TEST(NoTableReadInStage) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    // Stage input is a channel: names here belong to the upstream stage output,
    // and a wasm resident buffer would not survive the channel anyway.
    auto input = ctx.NewArgument(pos, "input");
    auto row = ctx.NewArgument(pos, "row");
    auto rowLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}),
        MakeApplyUdf(ctx, pos, MakeMember(ctx, pos, row, "blob")));
    auto body = ctx.NewCallable(pos, "Map", {ctx.NewCallable(pos, "ToFlow", {input}), rowLambda});
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {input}), std::move(body));

    auto connection = ctx.NewCallable(pos, "DqCnUnionAll", {ctx.NewCallable(pos, "Void", {})});
    UNIT_ASSERT(Collect(ctx, pos, {connection}, program).empty());
}

Y_UNIT_TEST(AliasedColumnNameIsNotPhysical) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto input = ctx.NewArgument(pos, "input");
    auto row = ctx.NewArgument(pos, "row");
    auto rowLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}),
        MakeApplyUdf(ctx, pos, MakeMember(ctx, pos, row, "alias.blob")));
    auto body = ctx.NewCallable(pos, "Map", {ctx.NewCallable(pos, "ToFlow", {input}), rowLambda});
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {input}), std::move(body));

    UNIT_ASSERT(Collect(ctx, pos, {MakeRowsSource(ctx, pos, SourceColumns)}, program).empty());
}

Y_UNIT_TEST(NonStringColumnIsNotMarked) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto input = ctx.NewArgument(pos, "input");
    auto row = ctx.NewArgument(pos, "row");
    auto member = MakeMember(ctx, pos, row, "id");
    member->SetTypeAnn(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64));

    auto rowLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), MakeApplyUdf(ctx, pos, member));
    auto body = ctx.NewCallable(pos, "Map", {ctx.NewCallable(pos, "ToFlow", {input}), rowLambda});
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {input}), std::move(body));

    UNIT_ASSERT(Collect(ctx, pos, {MakeRowsSource(ctx, pos, SourceColumns)}, program).empty());
}

Y_UNIT_TEST(CapturedCallableIsNotFollowed) {
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});

    auto input = ctx.NewArgument(pos, "input");
    auto row = ctx.NewArgument(pos, "row");
    auto captured = ctx.NewArgument(pos, "captured");

    // The stage does contain a Udf, but this Apply calls a captured callable:
    // we cannot tell whether it is the wasm one, so the column is not marked.
    auto body = ctx.NewCallable(pos, "Map", {
        ctx.NewCallable(pos, "ToFlow", {input}),
        ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), ctx.NewList(pos, {
            ctx.NewCallable(pos, "Apply", {captured, MakeMember(ctx, pos, row, "blob")}),
            MakeApplyUdf(ctx, pos, ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "literal")})),
        })),
    });
    auto program = ctx.NewLambda(pos, ctx.NewArguments(pos, {input}), std::move(body));

    UNIT_ASSERT(Collect(ctx, pos, {MakeRowsSource(ctx, pos, SourceColumns)}, program).empty());
}

} // Y_UNIT_TEST_SUITE
