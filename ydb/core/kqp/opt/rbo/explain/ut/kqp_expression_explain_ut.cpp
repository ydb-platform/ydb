#include <ydb/core/kqp/opt/rbo/kqp_expression.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/minikql/mkql_type_ops.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>
#include <util/string/builder.h>

namespace NKikimr::NKqp {

namespace {

// Columns a, b, c and the struct column s get IDs 0 to 3. ID 4 is the binding of
// subquery1, an IN subquery over a.
class TPrinterTestContext {
public:
    TPrinterTestContext() {
        TUnorderedIUs live;
        for (const auto* name : {"a", "b", "c", "s"}) {
            live.Add(Props.InfoUnitRegistry.Add(TInfoUnit(name)));
        }
        const auto subquery = Props.InfoUnitRegistry.AddGenerated("subquery");
        live.Add(subquery);
        TOrderedIUs<> tuple;
        tuple.Append(0);
        Props.Subplans.Add(subquery, MakeIntrusive<TOpEmptySource>(Pos), ESubplanType::IN_SUBPLAN, std::move(tuple),
            Props.InfoUnitRegistry.Add(TInfoUnit("result")));
        Props.InfoUnitRegistry.FinalizeDisplayNames(live);
    }

    // The body is an s-expression over the row argument `row`.
    TString Print(TStringBuf body) {
        const TString program = TStringBuilder() << "(\n(return (lambda '(row) " << body << "))\n)";
        const auto ast = ParseAst(program);
        UNIT_ASSERT_C(ast.IsOk(), program);
        TExprNode::TPtr lambda;
        UNIT_ASSERT_C(CompileExpr(*ast.Root, lambda, Ctx, nullptr, nullptr), program);
        return Print(lambda);
    }

    TString Print(TExprNode::TPtr lambda) {
        return TExpression(std::move(lambda), &Ctx, &Props).ToExplainString(Props.InfoUnitRegistry);
    }

    // The column `id` added to itself `depth` times. As a tree it has 2^depth leaves.
    TExprNode::TPtr SharedSum(const TExprNode::TPtr& row, TStringBuf id, int depth) {
        auto node = Ctx.NewCallable(Pos, "Member", {row, Ctx.NewAtom(Pos, id)});
        for (int i = 0; i < depth; ++i) {
            node = Ctx.NewCallable(Pos, "+", {node, node});
        }
        return node;
    }

    TExprNode::TPtr Lambda(const TExprNode::TPtr& row, TExprNode::TPtr body) {
        return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {row}), std::move(body));
    }

    TExprNode::TPtr OptionalColumn(const TExprNode::TPtr& row, TStringBuf id) {
        auto column = Ctx.NewCallable(Pos, "Member", {row, Ctx.NewAtom(Pos, id)});
        column->SetTypeAnn(Ctx.MakeType<TOptionalExprType>(Ctx.MakeType<TDataExprType>(EDataSlot::Int32)));
        return column;
    }

    const TPositionHandle Pos;
    TExprContext Ctx;
    TPlanProps Props;
};

void AssertPrinted(std::initializer_list<std::pair<TStringBuf, TStringBuf>> cases) {
    TPrinterTestContext context;
    for (const auto& [body, expected] : cases) {
        UNIT_ASSERT_VALUES_EQUAL_C(context.Print(body), TString(expected), body);
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpRboExpressionPrinter) {
    Y_UNIT_TEST(Columns) {
        AssertPrinted({
            {"(Member row '0)", "a"},
            {"(Member (Member row '3) 'f)", "s.f"},
            {"(Nth (Member row '3) '1)", "s.1"},
            {"(Member row '4)", "subquery1"},
        });
    }

    Y_UNIT_TEST(Literals) {
        AssertPrinted({
            {"(Int32 '10)", "10"},
            {"(Double '1.5)", "1.5"},
            {"(Bool 'true)", "true"},
            {"(Decimal '\"1.25\" '10 '2)", "1.25"},
            {R"((String '"a\"b"))", R"("a\"b")"},
            {R"((Utf8 '"x"))", R"("x")"},
            {R"((Date '"2020-01-01"))", R"(Date("2020-01-01"))"},
            // Constant folding leaves dates and times as their internal values.
            {"(Date '18262)", R"(Date("2020-01-01"))"},
            {"(Datetime '0)", R"(Datetime("1970-01-01T00:00:00Z"))"},
            {"(Interval '86400000000)", R"(Interval("P1D"))"},
            {"(Nothing (OptionalType (DataType 'Int32)))", "NULL"},
            {"(Null)", "NULL"},
            {R"((Parameter '"$p" (DataType 'Int32)))", "$p"},
        });
    }

    Y_UNIT_TEST(LogicalPrecedence) {
        AssertPrinted({
            {"(And (Or (> (Member row '0) (Int32 '1)) (< (Member row '1) (Int32 '2))) (== (Member row '2) (Int32 '3)))",
                "(a > 1 OR b < 2) AND c == 3"},
            {"(Or (And (> (Member row '0) (Int32 '1)) (< (Member row '1) (Int32 '2))) (== (Member row '2) (Int32 '3)))",
                "(a > 1 AND b < 2) OR c == 3"},
            {"(And (And (> (Member row '0) (Int32 '1)) (> (Member row '1) (Int32 '1))) (> (Member row '2) (Int32 '1)))",
                "a > 1 AND b > 1 AND c > 1"},
            {"(Xor (Member row '0) (Or (Member row '1) (Member row '2)))", "a XOR (b OR c)"},
            {"(Not (> (Member row '0) (Int32 '1)))", "NOT (a > 1)"},
            {"(Not (Member row '0))", "NOT a"},
            {"(Not (Not (Member row '0)))", "NOT (NOT a)"},
            {"(== (< (Member row '0) (Member row '1)) (Bool 'true))", "(a < b) == true"},
            {"(== (And (Member row '0) (Member row '1)) (Member row '2))", "(a AND b) == c"},
            // Degenerate conjunctions are printed as written.
            {"(And (Member row '0))", "And(a)"},
            {"(And)", "And()"},
        });
    }

    Y_UNIT_TEST(UuidAndTimezoneLiterals) {
        AssertPrinted({
            {R"((Uuid '"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))",
                R"(Uuid("00000000-0000-0000-0000-000000000000"))"},
            {R"((TzDate '"18262,GMT"))", R"(TzDate("2020-01-01,GMT"))"},
            {R"((TzDatetime '"0,GMT"))", R"(TzDatetime("1970-01-01T00:00:00,GMT"))"},
            {R"((TzTimestamp '"1,GMT"))", R"(TzTimestamp("1970-01-01T00:00:00.000001,GMT"))"},
            {R"((TzDate32 '"-1,GMT"))", R"(TzDate32("1969-12-31,GMT"))"},
            {R"((TzDatetime64 '"-1,GMT"))", R"(TzDatetime64("1969-12-31T23:59:59,GMT"))"},
            {R"((TzTimestamp64 '"-1,GMT"))", R"(TzTimestamp64("1969-12-31T23:59:59.999999,GMT"))"},
        });
        TPrinterTestContext context;
        TStringStream bytes;
        NMiniKQL::SerializeTzDatetime(0, NMiniKQL::GetTimezoneId("GMT"), bytes);
        auto row = context.Ctx.NewArgument(context.Pos, "row");
        auto literal = context.Ctx.NewCallable(context.Pos, "TzDatetime", {
            context.Ctx.NewAtom(context.Pos, bytes.Str(), TNodeFlags::BinaryContent)});
        UNIT_ASSERT_VALUES_EQUAL(context.Print(context.Lambda(row, literal)), R"(TzDatetime("1970-01-01T00:00:00,GMT"))");
    }

    Y_UNIT_TEST(MeaningfulWrappersStayVisible) {
        AssertPrinted({
            {"(Convert (Int32 '-1) 'Uint8)", "Convert(-1, Uint8)"},
            {"(Convert (Member row '0) (DataType 'Int64))", "Convert(a, Int64)"},
            {"(Exists (Just (Nothing (OptionalType (DataType 'Int32)))))", "Just(NULL) IS NOT NULL"},
            {"(Coalesce (Unwrap (Member row '0)) (Int32 '0))", "Coalesce(Unwrap(a), 0)"},
        });
    }

    Y_UNIT_TEST(FieldNamesAreQuoted) {
        AssertPrinted({
            {R"((Member (Member row '3) '"a.b"))", "s.`a.b`"},
            {R"((Member (Member row '3) '"a`b"))", "s.`a``b`"},
            {R"((Member (Member row '3) '"1"))", "s.`1`"},
            {R"((AsStruct '('"a+b" (Member row '0))))", "<|`a+b`: a|>"},
        });
        TExprContext ctx;
        TPlanProps props;
        const auto id = props.InfoUnitRegistry.Add(TInfoUnit("t.part", "a+b"));
        TUnorderedIUs live;
        live.Add(id);
        props.InfoUnitRegistry.FinalizeDisplayNames(live);
        const TPositionHandle pos;
        auto row = ctx.NewArgument(pos, "row");
        auto body = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, ToString(id))});
        auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(body));
        UNIT_ASSERT_VALUES_EQUAL(TExpression(lambda, &ctx, &props).ToExplainString(props.InfoUnitRegistry), "`t.part`.`a+b`");
    }

    Y_UNIT_TEST(ReservedIdentifiersAreQuoted) {
        const struct {
            TStringBuf Alias;
            TStringBuf Column;
            TStringBuf Expected;
        } cases[] = {
            {"not", "a", "`not`.a"},
            {"SeLeCt", "a", "`SeLeCt`.a"},
            {"", "where", "`where`"},
            {"", "ANY", "`ANY`"},
            {"", "distinct", "`distinct`"},
            {"", "compact", "`compact`"},
            {"", "true", "`true`"},
            {"t", "value", "t.value"},
        };
        for (const auto& test : cases) {
            TExprContext ctx;
            TPlanProps props;
            const TPositionHandle pos;
            auto& registry = props.InfoUnitRegistry;
            const auto id = registry.Add(TInfoUnit(TString(test.Alias), TString(test.Column)));
            TUnorderedIUs live;
            live.Add(id);
            registry.FinalizeDisplayNames(live);
            auto row = ctx.NewArgument(pos, "row");
            auto column = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, ToString(id))});
            auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(column));
            UNIT_ASSERT_VALUES_EQUAL(TExpression(lambda, &ctx, &props).ToExplainString(registry), test.Expected);
        }
    }

    Y_UNIT_TEST(NestedLambdaArgumentsAreDistinct) {
        TPrinterTestContext context;
        auto& ctx = context.Ctx;
        const auto pos = context.Pos;
        auto row = ctx.NewArgument(pos, "row");
        auto outer = ctx.NewArgument(pos, "x");
        auto inner = ctx.NewArgument(pos, "x");
        auto column = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, "3")});
        auto body = ctx.NewCallable(pos, "+", {outer, inner});
        auto innerMap = ctx.NewCallable(pos, "Map", {column, context.Lambda(inner, body)});
        auto outerMap = ctx.NewCallable(pos, "Map", {column, context.Lambda(outer, innerMap)});
        UNIT_ASSERT_VALUES_EQUAL(context.Print(context.Lambda(row, outerMap)),
            "Map(s, ($x) -> (Map(s, ($x_2) -> ($x + $x_2))))");
    }

    Y_UNIT_TEST(CutLambdaBodiesKeepTheirArguments) {
        TPrinterTestContext context;
        auto& ctx = context.Ctx;
        const auto pos = context.Pos;
        auto row = ctx.NewArgument(pos, "row");
        auto item = ctx.NewArgument(pos, "item");
        auto sum = item;
        for (int i = 0; i < 20; ++i) {
            sum = ctx.NewCallable(pos, "+", {sum, sum});
        }
        const auto printed = context.Print(context.Lambda(row, context.Lambda(item, sum)));
        UNIT_ASSERT_C(printed.Contains("f($item)"), printed);
    }

    Y_UNIT_TEST(ParametersAreNotShadowedByLambdaArguments) {
        AssertPrinted({
            {R"((Map (Member row '3) (lambda '(x) (+ x (Parameter '$x (DataType 'Int32))))))",
                "Map(s, ($x_2) -> ($x_2 + $x))"},
        });
    }

    Y_UNIT_TEST(SharedCompoundTypesStayBounded) {
        TPrinterTestContext context;
        auto& ctx = context.Ctx;
        const auto pos = context.Pos;
        auto row = ctx.NewArgument(pos, "row");
        auto typeNode = ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, "Int32")});
        const TTypeAnnotationNode* type = ctx.MakeType<TDataExprType>(EDataSlot::Int32);
        for (int i = 0; i < 20; ++i) {
            typeNode = ctx.NewCallable(pos, "TupleType", {typeNode, typeNode});
            type = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{type, type});
            typeNode->SetTypeAnn(ctx.MakeType<TTypeExprType>(type));
        }
        auto column = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, "0")});
        column->SetTypeAnn(type);
        auto cast = ctx.NewCallable(pos, "SafeCast", {column, typeNode});
        const auto printed = context.Print(context.Lambda(row, cast));
        UNIT_ASSERT_C(printed.size() < 20000, printed.size());
        UNIT_ASSERT_C(printed.Contains("..."), printed);
    }

    Y_UNIT_TEST(DeepWrappersAndTypesAreCut) {
        TPrinterTestContext context;
        auto& ctx = context.Ctx;
        const auto pos = context.Pos;
        auto row = ctx.NewArgument(pos, "row");
        auto column = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, "0")});
        auto value = column;
        auto type = ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, "Int32")});
        for (int i = 0; i < 1000; ++i) {
            value = ctx.NewCallable(pos, "Just", {value});
            type = ctx.NewCallable(pos, "OptionalType", {type});
        }
        UNIT_ASSERT_C(context.Print(context.Lambda(row, value)).Contains("f(a)"), "Wrappers bypassed the budget");
        auto cast = ctx.NewCallable(pos, "SafeCast", {column, type});
        UNIT_ASSERT_C(context.Print(context.Lambda(row, cast)).Contains("f("), "Type bypassed the budget");
    }

    Y_UNIT_TEST(WideLambdaArgumentsAreCut) {
        TPrinterTestContext context;
        auto& ctx = context.Ctx;
        const auto pos = context.Pos;
        auto row = ctx.NewArgument(pos, "row");
        TExprNode::TListType args;
        for (int i = 0; i < 20; ++i) {
            args.push_back(ctx.NewArgument(pos, TStringBuilder() << "x" << i));
        }
        auto body = args.front();
        auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, std::move(args)), std::move(body));
        const auto printed = context.Print(context.Lambda(row, lambda));
        UNIT_ASSERT_C(printed.EndsWith("... (4 more)) -> ($x0)"), printed);
    }

    Y_UNIT_TEST(ArithmeticPrecedence) {
        AssertPrinted({
            {"(* (+ (Member row '0) (Member row '1)) (Member row '2))", "(a + b) * c"},
            {"(+ (Member row '0) (* (Member row '1) (Member row '2)))", "a + b * c"},
            {"(- (- (Member row '0) (Member row '1)) (Member row '2))", "a - b - c"},
            {"(- (Member row '0) (- (Member row '1) (Member row '2)))", "a - (b - c)"},
            {"(/ (Member row '0) (* (Member row '1) (Member row '2)))", "a / (b * c)"},
            {"(> (+ (Member row '0) (Int32 '1)) (Member row '1))", "a + 1 > b"},
            {"(+ (BitAnd (Member row '0) (Member row '1)) (Member row '2))", "(a & b) + c"},
            {"(ShiftLeft (Member row '0) (+ (Member row '1) (Int32 '1)))", "a << (b + 1)"},
            {"(Concat (Concat (Member row '1) (String '\"x\")) (Member row '1))", "b || \"x\" || b"},
            {"(* (Concat (Member row '0) (Member row '1)) (Member row '2))", "(a || b) * c"},
            {"(DecimalMul (DecimalAdd (Member row '0) (Member row '1)) (Member row '2))", "(a + b) * c"},
        });
    }

    Y_UNIT_TEST(UnaryOperators) {
        AssertPrinted({
            {"(Minus (Member row '0))", "-a"},
            {"(Minus (+ (Member row '0) (Member row '1)))", "-(a + b)"},
            {"(Minus (Int32 '-1))", "-(-1)"},
            {"(- (Member row '0) (Int32 '-1))", "a - -1"},
            {"(BitNot (Member row '0))", "~a"},
        });
    }

    Y_UNIT_TEST(Predicates) {
        AssertPrinted({
            {"(Exists (Member row '0))", "a IS NOT NULL"},
            {"(Not (Exists (Member row '0)))", "a IS NULL"},
            {"(Not (Not (Exists (Member row '0))))", "a IS NOT NULL"},
            {"(SqlIn (AsList (Int32 '1) (Int32 '2)) (Member row '0) '())", "a IN (1, 2)"},
            {"(Not (SqlIn (AsList (Int32 '1) (Int32 '2)) (Member row '0) '()))", "a NOT IN (1, 2)"},
            {"(SqlIn '((Int32 '1) (Int32 '2)) (Member row '0) '())", "a IN (1, 2)"},
            {"(SqlIn (Member row '3) (+ (Member row '0) (Int32 '1)) '())", "a + 1 IN s"},
            {"(AggrEquals (Member row '0) (Member row '1))", "a IS NOT DISTINCT FROM b"},
            {"(AggrNotEquals (Member row '0) (Member row '1))", "a IS DISTINCT FROM b"},
            {"(IsDistinctFrom (Member row '0) (Member row '1))", "a IS DISTINCT FROM b"},
            {"(IsNotDistinctFrom (Member row '0) (Member row '1))", "a IS NOT DISTINCT FROM b"},
            {"(And (Exists (Member row '0)) (Member row '1))", "a IS NOT NULL AND b"},
        });
    }

    Y_UNIT_TEST(Calls) {
        AssertPrinted({
            {"(< (Coalesce (Member row '0) (Int32 '0)) (Int32 '5))", "Coalesce(a, 0) < 5"},
            {"(And (> (Member row '0) (Int32 '1)) (StartsWith (Member row '1) (String '\"ab\")))",
                "a > 1 AND StartsWith(b, \"ab\")"},
            {"(Apply (Udf 'String.Contains) (Member row '1) (String '\"x\"))", "String::Contains(b, \"x\")"},
            {"(Apply (Udf 'Foo) (Member row '1))", "Foo(b)"},
            {"(If (Member row '0) (Member row '1) (Member row '2))", "If(a, b, c)"},
            {"(If (Coalesce (> (Member row '0) (Int32 '0)) (Bool 'false)) (Member row '1) (Member row '2))", "If(a > 0, b, c)"},
            {"(If (Coalesce (Member row '0) (Bool 'true)) (Member row '1) (Member row '2))", "If(Coalesce(a, true), b, c)"},
            {"(Map (Member row '3) (lambda '(x) (+ x (Int32 '1))))", "Map(s, ($x) -> ($x + 1))"},
            {"(Fold (Member row '3) (Int32 '0) (lambda '(x y) (+ x y)))", "Fold(s, 0, ($x, $y) -> ($x + $y))"},
            {"(AsList (Member row '0) (Int32 '1))", "[a, 1]"},
            {"(AsStruct '('x (Member row '0)) '('y (Int32 '1)))", "<|x: a, y: 1|>"},
        });
    }

    Y_UNIT_TEST(ConfiguredUdfsStayVisible) {
        AssertPrinted({
            {R"((Apply (Udf 'Re2.Match '((String '"a.*") (AsStruct '('CaseSensitive (Bool 'false))))) (Member row '1)))",
                R"(Apply(Udf(Re2.Match, ("a.*", <|CaseSensitive: false|>)), b))"},
            {"(Apply (Udf 'Foo (Member row '0)) (Member row '1))", "Apply(Udf(Foo, a), b)"},
            {R"((Apply (Udf 'Foo (Void) (VoidType) '"mode") (Member row '1)))",
                "Apply(Udf(Foo, Void(), VoidType(), mode), b)"},
            {R"((Apply (Udf 'Foo (Void) (VoidType) '"") (Member row '1)))", "Foo(b)"},
        });
    }

    Y_UNIT_TEST(Wrappers) {
        AssertPrinted({
            {"(Just (Convert (Member row '0) (DataType 'Int64)))", "Just(Convert(a, Int64))"},
            {"(> (Unwrap (Member row '0)) (Int32 '1))", "Unwrap(a) > 1"},
            {"(SafeCast (Member row '0) (OptionalType (DataType 'Int64)))", "CAST(a AS Int64?)"},
            {"(SafeCast (Member row '0) (DataType 'Decimal '10 '2))", "CAST(a AS Decimal(10,2))"},
            // A type that can't be named keeps the call as written.
            {"(SafeCast (Member row '0) (StructType))", "SafeCast(a, StructType())"},
        });
    }

    Y_UNIT_TEST(OptionalToStringConversionsAreCompact) {
        TPrinterTestContext context;
        auto& ctx = context.Ctx;
        const auto pos = context.Pos;
        auto row = ctx.NewArgument(pos, "row");
        auto column = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, "1")});
        column->SetTypeAnn(ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Utf8)));
        auto item = ctx.NewArgument(pos, "item");
        auto toString = ctx.NewLambda(pos, ctx.NewArguments(pos, {item}), ctx.NewCallable(pos, "ToString", {item}));
        auto body = ctx.NewCallable(pos, "StartsWith", {
            ctx.NewCallable(pos, "Map", {column, toString}),
            ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "ab")}),
        });
        UNIT_ASSERT_VALUES_EQUAL(context.Print(context.Lambda(row, std::move(body))), R"(StartsWith(ToString(b), "ab"))");

        auto flatMap = ctx.NewCallable(pos, "FlatMap", {column,
            context.Lambda(item, ctx.NewCallable(pos, "Just", {ctx.NewCallable(pos, "ToString", {item})}))});
        UNIT_ASSERT_VALUES_EQUAL(context.Print(context.Lambda(row, std::move(flatMap))), "ToString(b)");
    }

    Y_UNIT_TEST(SharedOptionalConversionLambdaKeepsItsScope) {
        TPrinterTestContext context;
        auto& ctx = context.Ctx;
        const auto pos = context.Pos;
        auto row = ctx.NewArgument(pos, "row");
        auto item = ctx.NewArgument(pos, "item");
        auto toString = context.Lambda(item, ctx.NewCallable(pos, "ToString", {item}));
        auto list = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, "3")});
        list->SetTypeAnn(ctx.MakeType<TListExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Int32)));
        auto body = ctx.NewCallable(pos, "AsTuple", {
            ctx.NewCallable(pos, "Map", {context.OptionalColumn(row, "0"), toString}),
            ctx.NewCallable(pos, "Map", {context.OptionalColumn(row, "1"), toString}),
            // Reuse the same lambda over a list, where its argument must stay local.
            ctx.NewCallable(pos, "Map", {list, toString}),
        });
        UNIT_ASSERT_VALUES_EQUAL(context.Print(context.Lambda(row, std::move(body))),
            "AsTuple(ToString(a), ToString(b), Map(s, ($item) -> (ToString($item))))");
    }

    Y_UNIT_TEST(OptionalMapsPreserveNullGuard) {
        TPrinterTestContext context;
        auto& ctx = context.Ctx;
        const auto pos = context.Pos;
        auto row = ctx.NewArgument(pos, "row");
        auto column = context.OptionalColumn(row, "0");
        auto item = ctx.NewArgument(pos, "item");
        auto one = ctx.NewCallable(pos, "Int32", {ctx.NewAtom(pos, "1")});
        for (const auto* callable : {"Map", "FlatMap"}) {
            auto value = TStringBuf(callable) == "Map" ? one : ctx.NewCallable(pos, "Just", {one});
            auto mapped = ctx.NewCallable(pos, callable, {column, context.Lambda(item, value)});
            const TString expected = TStringBuilder() << callable << "(a, ($item) -> (1))";
            UNIT_ASSERT_VALUES_EQUAL(context.Print(context.Lambda(row, mapped)), expected);
            auto exists = ctx.NewCallable(pos, "Exists", {mapped});
            UNIT_ASSERT_VALUES_EQUAL(context.Print(context.Lambda(row, std::move(exists))), expected + " IS NOT NULL");
        }
    }

    Y_UNIT_TEST(OptionalMapsKeepNonPropagatingBodies) {
        TPrinterTestContext context;
        auto& ctx = context.Ctx;
        const auto pos = context.Pos;
        auto row = ctx.NewArgument(pos, "row");
        auto column = context.OptionalColumn(row, "0");
        // The lambda receives an optional itself. Coalesce must not consume the
        // missing outer optional, and ToString must not flatten the two layers.
        column->SetTypeAnn(ctx.MakeType<TOptionalExprType>(column->GetTypeAnn().Get()));
        auto item = ctx.NewArgument(pos, "item");
        auto fallback = ctx.NewCallable(pos, "Int32", {ctx.NewAtom(pos, "1")});
        auto coalesce = ctx.NewCallable(pos, "Coalesce", {item, fallback});
        auto mapped = ctx.NewCallable(pos, "Map", {column, context.Lambda(item, coalesce)});
        UNIT_ASSERT_VALUES_EQUAL(context.Print(context.Lambda(row, std::move(mapped))),
            "Map(a, ($item) -> (Coalesce($item, 1)))");
        auto toString = ctx.NewCallable(pos, "ToString", {item});
        mapped = ctx.NewCallable(pos, "Map", {column, context.Lambda(item, toString)});
        UNIT_ASSERT_VALUES_EQUAL(context.Print(context.Lambda(row, std::move(mapped))),
            "Map(a, ($item) -> (ToString($item)))");
    }

    Y_UNIT_TEST(NestedOptionalMapsStayBounded) {
        TPrinterTestContext context;
        auto& ctx = context.Ctx;
        const auto pos = context.Pos;
        auto row = ctx.NewArgument(pos, "row");
        auto node = context.OptionalColumn(row, "0");
        const auto* type = node->GetTypeAnn().Get();
        // Substituting rendered arguments doubles the text at every level,
        // producing megabytes from a small expression under the node budget.
        for (int i = 0; i < 20; ++i) {
            auto item = ctx.NewArgument(pos, "item");
            auto sum = ctx.NewCallable(pos, "+", {item, item});
            node = ctx.NewCallable(pos, "Map", {node, context.Lambda(item, sum)});
            node->SetTypeAnn(type);
        }
        const auto printed = context.Print(context.Lambda(row, std::move(node)));
        UNIT_ASSERT_C(printed.size() < 20000, printed.size());
        UNIT_ASSERT_C(printed.Contains("a"), printed);
    }

    Y_UNIT_TEST(LongListsAreElided) {
        TStringBuilder values;
        for (int i = 0; i < 20; ++i) {
            values << " (Int32 '" << i << ")";
        }
        TPrinterTestContext context;
        UNIT_ASSERT_VALUES_EQUAL(context.Print(TStringBuilder() << "(SqlIn (AsList" << values << ") (Member row '0) '())"),
            "a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, ... (4 more))");
    }

    Y_UNIT_TEST(WideConjunctionsAreCut) {
        TStringBuilder conjuncts;
        for (int i = 0; i < 300; ++i) {
            conjuncts << " (== (Member row '0) (Int32 '" << i << "))";
        }
        TPrinterTestContext context;
        const auto printed = context.Print(TStringBuilder() << "(And" << conjuncts << ")");
        UNIT_ASSERT_C(printed.StartsWith("a == 0 AND a == 1 AND "), printed);
        UNIT_ASSERT_C(printed.EndsWith(" AND ... (215 more)"), printed);
    }

    Y_UNIT_TEST(SharedSubtreesAreCut) {
        TPrinterTestContext context;
        auto row = context.Ctx.NewArgument(context.Pos, "row");
        const auto printed = context.Print(context.Lambda(row, context.SharedSum(row, "0", 40)));
        UNIT_ASSERT_C(printed.Contains("f(a)"), printed);
        UNIT_ASSERT_C(printed.size() < 20000, printed);
    }

    Y_UNIT_TEST(CutArgumentsAreCounted) {
        TPrinterTestContext context;
        auto row = context.Ctx.NewArgument(context.Pos, "row");
        // The first argument alone is over the budget, so the other two are elided.
        auto sum = context.SharedSum(row, "0", 9);
        const auto printed = context.Print(context.Lambda(row, context.Ctx.NewCallable(context.Pos, "Foo", {sum, sum, sum})));
        UNIT_ASSERT_C(printed.StartsWith("Foo(") && printed.EndsWith(", ... (2 more))"), printed);
    }

    Y_UNIT_TEST(CutSubqueriesShowTheirArguments) {
        TPrinterTestContext context;
        auto row = context.Ctx.NewArgument(context.Pos, "row");
        const auto printed = context.Print(context.Lambda(row, context.SharedSum(row, "4", 40)));
        UNIT_ASSERT_C(printed.Contains("f(a, subquery1)"), printed);
    }
}

} // namespace NKikimr::NKqp
