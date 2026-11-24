#include <yt/yql/providers/yt/lib/qlfilter/qlfilter.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <yql/essentials/ast/yql_expr.h>

#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>
#include <util/string/join.h>

#include <algorithm>
#include <cmath>
#include <limits>

namespace NYql {
namespace {

namespace QL = NYT::NQueryClient;
namespace QLAst = NYT::NQueryClient::NAst;

bool SameExpression(const TExprNode* actual, const TExprNode* expected) {
    return CompareExprTrees(actual, expected);
}

struct TQLFilterTestContext {
    TExprContext Expr;
    const TPositionHandle Pos = Expr.AppendPosition({});
    const TExprNode::TPtr Row = Expr.NewArgument(Pos, "row");
    const TExprNode::TPtr FilterRow = Expr.NewArgument(Pos, "filterRow");
    TVector<const TItemExprType*> Columns;

    TExprNode::TPtr Call(TStringBuf name, TExprNode::TListType args) {
        return Expr.NewCallable(Pos, name, std::move(args));
    }

    TExprNode::TPtr Column(TStringBuf name, EDataSlot slot, bool optional = false) {
        auto member = Call("Member", {Row, Expr.NewAtom(Pos, name)});
        const TTypeAnnotationNode* type = Expr.MakeType<TDataExprType>(slot);
        if (optional) {
            type = Expr.MakeType<TOptionalExprType>(type);
        }
        member->SetTypeAnn(type);
        Columns.push_back(Expr.MakeType<TItemExprType>(name, type));
        return member;
    }

    TExprNode::TPtr Literal(EDataSlot slot, TStringBuf value) {
        auto literal = Call(NKikimr::NUdf::GetDataTypeInfo(slot).Name, {Expr.NewAtom(Pos, value)});
        literal->SetTypeAnn(Expr.MakeType<TDataExprType>(slot));
        return literal;
    }

    TExprNode::TPtr Compare(const TExprNode::TPtr& left, TStringBuf op, const TExprNode::TPtr& right) {
        return Call(op, {left, right});
    }

    TExprNode::TPtr And(TExprNode::TListType operands) {
        return Call("And", std::move(operands));
    }

    TExprNode::TPtr Or(TExprNode::TListType operands) {
        return Call("Or", std::move(operands));
    }

    TExprNode::TPtr Not(const TExprNode::TPtr& operand) {
        return Call("Not", {operand});
    }

    TExprNode::TPtr Coalesce(const TExprNode::TPtr& operand, bool onNull) {
        return Call("Coalesce", {operand, Literal(EDataSlot::Bool, onNull ? "true" : "false")});
    }

    TExprNode::TPtr Extract(const TExprNode::TPtr& predicate) {
        Row->SetTypeAnn(Expr.MakeType<TStructExprType>(Columns));
        return ExtractQLFilterPredicate(predicate, Row, FilterRow, Pos, Expr);
    }

    TExprNode::TPtr WrapFilter(TExprNode::TPtr predicate, const TExprNode::TPtr& row) {
        const auto* rowType = Expr.MakeType<TStructExprType>(Columns);
        row->SetTypeAnn(rowType);
        auto args = Expr.NewArguments(Pos, {row});
        auto lambda = Expr.NewLambda(Pos, std::move(args), std::move(predicate));
        return Call("YtQLFilter", {ExpandType(Pos, *rowType, Expr), std::move(lambda)});
    }

    TMaybe<TString> Generate(TExprNode::TPtr predicate, ui32 depthLimit = 0) {
        return GenerateInputQuery(WrapFilter(std::move(predicate), Row), depthLimit);
    }
};

TExprNode::TPtr ExpectExtractedPredicate(
    TQLFilterTestContext& ctx,
    const TExprNode::TPtr& input,
    TExprNode::TPtr expected)
{
    // Compare the expected shape on the filter argument. Free arguments are
    // compared by identity, so a Member still bound to Row fails this check.
    const auto rebound = ctx.Expr.ReplaceNode(std::move(expected), *ctx.Row, ctx.FilterRow);
    const auto actual = ctx.Extract(input);
    const auto describe = [&] () -> TString {
        return TStringBuilder() << "Input:\n" << input->Dump()
            << "\nExpected extraction:\n" << rebound->Dump()
            << "\nActual extraction:\n" << (actual ? actual->Dump() : TString("<none>"));
    };
    UNIT_ASSERT_C(actual, describe());

    if (rebound->IsCallable({"And", "Or"})) {
        UNIT_ASSERT_C(actual->IsCallable(rebound->Content()), describe());
        const auto actualOperands = actual->Children();
        const auto expectedOperands = rebound->Children();
        UNIT_ASSERT_C(std::is_permutation(
            actualOperands.begin(), actualOperands.end(), expectedOperands.begin(), expectedOperands.end(),
            [] (const auto& left, const auto& right) { return SameExpression(left.Get(), right.Get()); }),
            describe());
    } else {
        UNIT_ASSERT_C(SameExpression(actual.Get(), rebound.Get()), describe());
    }
    return actual;
}

void ExpectQuery(TQLFilterTestContext& ctx, const TExprNode::TPtr& predicate, TStringBuf expected, ui32 depthLimit = 0) {
    const auto query = ctx.Generate(predicate, depthLimit);
    UNIT_ASSERT_C(query, TStringBuilder() << "Expected " << expected << "\nPredicate:\n" << predicate->Dump());
    UNIT_ASSERT_VALUES_EQUAL_C(*query, expected, predicate->Dump());
}

void ExpectQueryWithMinDepth(
    TQLFilterTestContext& ctx,
    const TExprNode::TPtr& predicate,
    TStringBuf expected,
    ui32 minDepth)
{
    if (minDepth > 1) {
        UNIT_ASSERT_C(!ctx.Generate(predicate, minDepth - 1), predicate->Dump());
    }
    ExpectQuery(ctx, predicate, expected, minDepth);
}

const QLAst::TExpression& WherePredicate(const QL::TParsedSource& source) {
    const auto& query = std::get<QLAst::TQuery>(source.AstHead.Ast);
    UNIT_ASSERT_C(!query.SelectExprs, source.Source);
    UNIT_ASSERT_C(query.WherePredicate && query.WherePredicate->size() == 1, source.Source);
    return *query.WherePredicate->front();
}

double FloatingValue(const QLAst::TExpression& expression, TStringBuf source) {
    if (const auto* sign = dynamic_cast<const QLAst::TUnaryOpExpression*>(&expression)) {
        UNIT_ASSERT_C(sign->Opcode == QL::EUnaryOp::Minus || sign->Opcode == QL::EUnaryOp::Plus, source);
        UNIT_ASSERT_VALUES_EQUAL_C(sign->Operand.size(), 1, source);
        const double value = FloatingValue(*sign->Operand.front(), source);
        return sign->Opcode == QL::EUnaryOp::Minus ? -value : value;
    }
    const auto* literal = dynamic_cast<const QLAst::TLiteralExpression*>(&expression);
    UNIT_ASSERT_C(literal && std::holds_alternative<double>(literal->Value), source);
    return std::get<double>(literal->Value);
}

struct TLogicalShape {
    TStringBuf Separator;
    TVector<TString> Operands;
    ui32 Depth;

    TString Format() const {
        if (Separator.empty()) {
            return Operands.front();
        }
        return TStringBuilder() << "(" << JoinSeq(Separator, Operands) << ")";
    }
};

TLogicalShape InspectLogicalShape(const QLAst::TExpression& expression, TStringBuf source) {
    if (const auto* column = dynamic_cast<const QLAst::TReferenceExpression*>(&expression)) {
        return {{}, {TString(QLAst::FormatReference(column->Reference))}, 1};
    }
    if (const auto* literal = dynamic_cast<const QLAst::TLiteralExpression*>(&expression)) {
        const auto* value = std::get_if<bool>(&literal->Value);
        UNIT_ASSERT_C(value, source);
        return {{}, {TString(*value ? "TRUE" : "FALSE")}, 1};
    }

    const auto* op = dynamic_cast<const QLAst::TBinaryOpExpression*>(&expression);
    UNIT_ASSERT_C(op && (op->Opcode == QL::EBinaryOp::And || op->Opcode == QL::EBinaryOp::Or), source);
    UNIT_ASSERT_C(op->Lhs.size() == 1 && op->Rhs.size() == 1, source);
    const auto left = InspectLogicalShape(*op->Lhs.front(), source);
    const auto right = InspectLogicalShape(*op->Rhs.front(), source);
    const TStringBuf sep = op->Opcode == QL::EBinaryOp::And ? " AND " : " OR ";
    TLogicalShape result{sep, {}, 1 + std::max(left.Depth, right.Depth)};
    // Ignore association and order within the same operator. Keep mixed subtrees intact.
    for (const auto* child : {&left, &right}) {
        if (child->Separator == sep) {
            result.Operands.insert(result.Operands.end(), child->Operands.begin(), child->Operands.end());
        } else {
            result.Operands.push_back(child->Format());
        }
    }
    std::sort(result.Operands.begin(), result.Operands.end());
    return result;
}

void ExpectLogicalQueryWithMinDepth(
    TQLFilterTestContext& ctx,
    const TExprNode::TPtr& predicate,
    TStringBuf expectedLogicalForm,
    ui32 minDepth)
{
    UNIT_ASSERT_C(!ctx.Generate(predicate, minDepth - 1), expectedLogicalForm);
    const auto query = ctx.Generate(predicate, minDepth);
    UNIT_ASSERT_C(query, expectedLogicalForm);

    const auto parsed = QL::ParseSource(*query, QL::EParseMode::JobQuery);
    const auto expected = QL::ParseSource(expectedLogicalForm, QL::EParseMode::JobQuery);
    const auto actualShape = InspectLogicalShape(WherePredicate(*parsed), *query);
    const auto expectedShape = InspectLogicalShape(WherePredicate(*expected), expectedLogicalForm);
    UNIT_ASSERT_VALUES_EQUAL_C(actualShape.Depth, minDepth, *query);
    UNIT_ASSERT_VALUES_EQUAL_C(actualShape.Format(), expectedShape.Format(), *query);
}

Y_UNIT_TEST_SUITE(TExtractQLFilterPredicateTest) {
    Y_UNIT_TEST(RequiresARowIndependentComparisonOperand) {
        TQLFilterTestContext ctx;
        const auto x = ctx.Column("x", EDataSlot::Int32);
        const auto y = ctx.Column("y", EDataSlot::Int32);
        const auto two = ctx.Literal(EDataSlot::Int32, "2");

        // x > (40 + 2): extract the comparison and keep the unevaluated sum.
        const auto sum = ctx.Call("+", {ctx.Literal(EDataSlot::Int32, "40"), two});
        sum->SetTypeAnn(two->GetTypeAnn());
        const auto constantComparison = ctx.Compare(x, ">", sum);
        const auto withSum = ExpectExtractedPredicate(ctx, constantComparison, constantComparison);
        // CalculateNodes remaps this shared node in both the filter and the mapper.
        UNIT_ASSERT(withSum->TailPtr() == sum);

        // 42 < x: the column can be on either side.
        const auto reversedComparison = ctx.Compare(ctx.Literal(EDataSlot::Int32, "42"), "<", x);
        ExpectExtractedPredicate(ctx, reversedComparison, reversedComparison);

        // x > (y + 2): the right side depends on the input row.
        const auto rowDependentSum = ctx.Call("+", {y, two});
        rowDependentSum->SetTypeAnn(two->GetTypeAnn());
        UNIT_ASSERT(!ctx.Extract(ctx.Compare(x, ">", rowDependentSum)));
    }

    Y_UNIT_TEST(ExtractsPartialConjunctionOnlyAtTheRoot) {
        TQLFilterTestContext ctx;
        const auto x = ctx.Column("x", EDataSlot::Int32);
        const auto y = ctx.Column("y", EDataSlot::Int32);
        const auto lowerBound = ctx.Compare(x, ">", ctx.Literal(EDataSlot::Int32, "0"));
        const auto upperBound = ctx.Compare(x, "<", ctx.Literal(EDataSlot::Int32, "10"));
        const auto rowComparison = ctx.Compare(x, "!=", y);

        // (x > 0 AND x != y AND x < 10) -> x > 0 AND x < 10.
        const auto conjunction = ctx.And({lowerBound, rowComparison, upperBound});
        ExpectExtractedPredicate(ctx, conjunction, ctx.And({lowerBound, upperBound}));

        // NOT (x > 0 AND x != y AND x < 10): dropping a conjunct is unsafe here.
        UNIT_ASSERT(!ctx.Extract(ctx.Not(conjunction)));
    }

    Y_UNIT_TEST(RequiresAllDisjunctsToBeCompatible) {
        TQLFilterTestContext ctx;
        const auto x = ctx.Column("x", EDataSlot::Int32);
        const auto y = ctx.Column("y", EDataSlot::Int32);
        const auto belowZero = ctx.Compare(x, "<", ctx.Literal(EDataSlot::Int32, "0"));
        const auto aboveTen = ctx.Compare(x, ">", ctx.Literal(EDataSlot::Int32, "10"));
        const auto rowComparison = ctx.Compare(x, "==", y);

        // x < 0 OR x > 10: both comparisons must survive.
        const auto disjunction = ctx.Or({belowZero, aboveTen});
        ExpectExtractedPredicate(ctx, disjunction, disjunction);

        // x < 0 OR x == y: dropping a disjunct could exclude matching rows.
        UNIT_ASSERT(!ctx.Extract(ctx.Or({belowZero, rowComparison})));
    }

    Y_UNIT_TEST(SelectsSupportedScalarComparisons) {
        struct TCase {
            EDataSlot ColumnType;
            TStringBuf Op;
            EDataSlot LiteralType;
            TStringBuf Value;
            bool Compatible;
        };
        for (const auto& test : {
            TCase{EDataSlot::Int32, ">=", EDataSlot::Uint32, "7", true},
            TCase{EDataSlot::Uint64, "<", EDataSlot::Int32, "7", true},
            TCase{EDataSlot::Float, ">", EDataSlot::Double, "0.5", true},
            TCase{EDataSlot::Double, "<=", EDataSlot::Float, "0.5", true},
            TCase{EDataSlot::String, "==", EDataSlot::Utf8, "abc", true},
            TCase{EDataSlot::Utf8, "!=", EDataSlot::String, "abc", true},
            TCase{EDataSlot::Date, "==", EDataSlot::Date, "42", false},
        }) {
            TQLFilterTestContext ctx;
            const auto predicate = ctx.Compare(
                ctx.Column("x", test.ColumnType), test.Op, ctx.Literal(test.LiteralType, test.Value));
            UNIT_ASSERT_VALUES_EQUAL_C(bool(ctx.Extract(predicate)), test.Compatible,
                TStringBuilder() << "x:" << test.ColumnType << " " << test.Op << " "
                    << test.LiteralType << "(" << test.Value << ")");
        }
    }

    Y_UNIT_TEST(RejectsComparisonsOnSystemColumns) {
        TQLFilterTestContext ctx;
        const auto path = ctx.Literal(EDataSlot::String, "//tmp/t");
        const auto pathColumn = ctx.Column("path", EDataSlot::String);
        // TablePath() reads a virtual column supplied by YQL.
        const auto tablePath = ctx.Column("_yql_sys_tablepath", EDataSlot::String);

        const auto comparison = ctx.Compare(pathColumn, "==", path);
        ExpectExtractedPredicate(ctx, comparison, comparison);
        UNIT_ASSERT(!ctx.Extract(ctx.Compare(tablePath, "==", path)));
    }

    Y_UNIT_TEST(ExtractsNullChecksOnDirectColumns) {
        TQLFilterTestContext ctx;
        const auto s = ctx.Column("s", EDataSlot::String, /*optional*/ true);
        const auto isNotNull = ctx.Call("Exists", {s});
        const auto isNull = ctx.Not(isNotNull);
        ExpectExtractedPredicate(ctx, isNotNull, isNotNull);
        ExpectExtractedPredicate(ctx, isNull, isNull);

        // CAST(s AS Int64?) can be NULL even when s is present.
        const auto* optionalInt64 = ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int64));
        const auto cast = ctx.Call("SafeCast", {s, ExpandType(ctx.Pos, *optionalInt64, ctx.Expr)});
        cast->SetTypeAnn(optionalInt64);
        UNIT_ASSERT(!ctx.Extract(ctx.Not(ctx.Call("Exists", {cast}))));
    }

    Y_UNIT_TEST(ExtractsNullableComparisonWithBooleanLiteralFallback) {
        TQLFilterTestContext ctx;
        const auto x = ctx.Column("x", EDataSlot::Int8, /*optional*/ true);
        const auto comparison = ctx.Compare(x, "<", ctx.Literal(EDataSlot::Int32, "1"));

        // COALESCE(x < 1, FALSE) is compatible.
        const auto onNullFalse = ctx.Coalesce(comparison, false);
        ExpectExtractedPredicate(ctx, onNullFalse, onNullFalse);

        // COALESCE(x < 1, flag) would need a second column.
        const auto flag = ctx.Column("flag", EDataSlot::Bool);
        UNIT_ASSERT(!ctx.Extract(ctx.Call("Coalesce", {comparison, flag})));
    }
}

Y_UNIT_TEST_SUITE(TQLFilterTest) {
    Y_UNIT_TEST(SerializesExtractedConjunct) {
        TQLFilterTestContext ctx;
        const auto flag = ctx.Column("flag", EDataSlot::Bool);
        const auto otherFlag = ctx.Column("otherFlag", EDataSlot::Bool);

        // NOT flag AND flag == otherFlag -> NOT flag.
        const auto notFlag = ctx.Not(flag);
        const auto predicate = ctx.And({notFlag, ctx.Compare(flag, "==", otherFlag)});
        auto selected = ExpectExtractedPredicate(ctx, predicate, notFlag);

        const auto filter = ctx.WrapFilter(std::move(selected), ctx.FilterRow);
        UNIT_ASSERT(!GenerateInputQuery(filter, /*depthLimit*/ 1));
        const auto query = GenerateInputQuery(filter, /*depthLimit*/ 2);
        UNIT_ASSERT(query);
        UNIT_ASSERT_VALUES_EQUAL(*query, "* WHERE NOT (`flag`)");
    }
}

Y_UNIT_TEST_SUITE(TConvertFloatingValueForQLTest) {
    Y_UNIT_TEST(PreservesValuesAndFloatingPointSyntax) {
        struct TCase {
            TStringBuf Value;
            bool IsFloat;
            double ExpectedValue;
        };
        const double infinity = std::numeric_limits<double>::infinity();
        constexpr double base = 87423.203125;
        for (const auto& test : {
            TCase{"0.0", false, 0.0},
            TCase{"1E2", false, 100.0},
            TCase{"-1e-2", false, -0.01},
            TCase{"1E100", false, 1e100},
            TCase{"0xAA", false, 170.0},
            // 2^63 is exact in Double but exceeds Int64.
            TCase{"9223372036854775808", false, 0x1p63},
            // Float(2^27 + 1) rounds to 2^27; QL must recover that exact value as Double.
            TCase{"134217729", true, 0x1p27},
            // Each pair straddles the midpoint between base and its next representable value.
            TCase{"87423.2031250000001", false, base},
            TCase{"87423.20312500001", false, std::nextafter(base, infinity)},
            TCase{"87423.207", true, base},
            TCase{"87423.208", true, std::nextafter(static_cast<float>(base), std::numeric_limits<float>::infinity())},
            TCase{"+inf", false, infinity},
            TCase{"-inf", false, -infinity},
        }) {
            const auto literal = ConvertFloatingValueForQL(test.Value, test.IsFloat);
            UNIT_ASSERT_C(literal, test.Value);
            const auto parsed = QL::ParseSource(*literal, QL::EParseMode::Expression);
            const auto* expression = std::get<QLAst::TExpressionPtr>(parsed->AstHead.Ast);
            UNIT_ASSERT_VALUES_EQUAL_C(FloatingValue(*expression, *literal), test.ExpectedValue, test.Value);
        }
        for (bool isFloat : {false, true}) {
            UNIT_ASSERT(!ConvertFloatingValueForQL("nan", isFloat));
        }
        UNIT_ASSERT(!ConvertFloatingValueForQL("1e+", false));
    }

    Y_UNIT_TEST(NormalizesEquivalentSpellings) {
        struct TCase {
            TStringBuf First;
            TStringBuf Second;
            bool IsFloat;
        };
        for (const auto& [first, second, isFloat] : {
            TCase{"1E2", "100.0", false},
            // Both inputs represent 2^27 once rounded to Float.
            TCase{"134217729", "134217728", true},
        }) {
            const auto firstLiteral = ConvertFloatingValueForQL(first, isFloat);
            const auto secondLiteral = ConvertFloatingValueForQL(second, isFloat);
            UNIT_ASSERT_C(firstLiteral, first);
            UNIT_ASSERT_C(secondLiteral, second);
            UNIT_ASSERT_VALUES_EQUAL_C(*firstLiteral, *secondLiteral, TStringBuilder() << first << " vs " << second);
        }
    }
}

Y_UNIT_TEST_SUITE(TGenerateInputQueryTest) {
    Y_UNIT_TEST(UsesNullComparisonsForExists) {
        TQLFilterTestContext ctx;
        const auto value = ctx.Column("value", EDataSlot::String, /*optional*/ true);
        const auto exists = ctx.Call("Exists", {value});
        struct TCase {
            TExprNode::TPtr Predicate;
            TStringBuf ExpectedQuery;
        };
        for (const auto& [predicate, expectedQuery] : {
            TCase{exists, "* WHERE (`value`) != NULL"},
            TCase{ctx.Not(exists), "* WHERE (`value`) = NULL"},
        }) {
            ExpectQueryWithMinDepth(ctx, predicate, expectedQuery, 2);
        }
    }

    Y_UNIT_TEST(UsesNullComparisonsForOptionalComparisons) {
        TQLFilterTestContext ctx;
        const auto value = ctx.Column("value", EDataSlot::Int8, /*optional*/ true);
        const auto integer = [&] (TStringBuf literal) {
            return ctx.Literal(EDataSlot::Int32, literal);
        };
        const auto justSeven = ctx.Call("Just", {integer("7")});
        const auto* optionalInt32 = ctx.Expr.MakeType<TOptionalExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int32));
        const auto nothing = ctx.Call("Nothing", {ExpandType(ctx.Pos, *optionalInt32, ctx.Expr)});
        struct TCase {
            TExprNode::TPtr Comparison;
            bool OnNull;
            TStringBuf ExpectedQuery;
            ui32 Depth;
        };
        // COALESCE(comparison, OnNull) -> input_query.
        for (const auto& test : {
            TCase{ctx.Compare(value, "<", integer("1")), false, "* WHERE `value` != NULL AND `value` < 1", 3},
            TCase{ctx.Compare(value, ">", integer("1")), true, "* WHERE `value` = NULL OR `value` > 1", 3},
            TCase{ctx.Compare(value, "<", integer("-1")), false, "* WHERE `value` != NULL AND `value` < -1", 4},
            TCase{ctx.Compare(value, "<", integer("300")), false, "* WHERE `value` != NULL", 2},
            TCase{ctx.Compare(value, ">", integer("300")), true, "* WHERE `value` = NULL", 2},
            TCase{ctx.Compare(value, "<", integer("1")), true, "* WHERE `value` < 1", 2},
            TCase{ctx.Compare(value, ">=", integer("1")), false, "* WHERE `value` >= 1", 2},
            TCase{ctx.Compare(value, "==", justSeven), false, "* WHERE `value` = 7", 2},
            TCase{ctx.Compare(value, "==", nothing), false, "* WHERE FALSE", 1},
            TCase{ctx.Compare(value, "==", nothing), true, "* WHERE TRUE", 1},
        }) {
            ExpectQueryWithMinDepth(ctx, ctx.Coalesce(test.Comparison, test.OnNull), test.ExpectedQuery, test.Depth);
        }
    }

    Y_UNIT_TEST(PreservesIntegerBoundsAndComparisonDirection) {
        TQLFilterTestContext ctx;
        const auto x = ctx.Column("x", EDataSlot::Int8);
        const auto u = ctx.Column("u", EDataSlot::Uint8);
        const auto integer = [&] (TStringBuf value) {
            return ctx.Literal(EDataSlot::Int32, value);
        };

        struct TCase {
            TExprNode::TPtr Predicate;
            TStringBuf ExpectedQuery;
        };
        for (const auto& [predicate, expectedQuery] : {
            TCase{ctx.Compare(x, "==", integer("-128")), "* WHERE `x` = -128"},
            TCase{ctx.Compare(x, "==", integer("127")), "* WHERE `x` = 127"},
            TCase{ctx.Compare(x, "<", integer("-129")), "* WHERE FALSE"},
            TCase{ctx.Compare(x, "!=", integer("128")), "* WHERE TRUE"},
            TCase{ctx.Compare(u, "==", integer("0")), "* WHERE `u` = 0"},
            TCase{ctx.Compare(u, "==", integer("255")), "* WHERE `u` = 255"},
            TCase{ctx.Compare(u, ">=", integer("-1")), "* WHERE TRUE"},
            TCase{ctx.Compare(integer("256"), "<=", u), "* WHERE FALSE"},
            TCase{ctx.Compare(integer("5"), "<", x), "* WHERE `x` > 5"},
            TCase{ctx.Compare(integer("5"), "<=", x), "* WHERE `x` >= 5"},
            TCase{ctx.Compare(integer("5"), ">", x), "* WHERE `x` < 5"},
            TCase{ctx.Compare(integer("5"), ">=", x), "* WHERE `x` <= 5"},
        }) {
            ExpectQuery(ctx, predicate, expectedQuery);
        }
    }

    Y_UNIT_TEST(EscapesColumnNamesAndStringLiterals) {
        constexpr TStringBuf columnName = R"(a\`q)";
        constexpr TStringBuf value = R"(a"b\c)";
        for (const auto type : {EDataSlot::String, EDataSlot::Utf8}) {
            TQLFilterTestContext ctx;
            const auto column = ctx.Column(columnName, type);
            const auto comparison = ctx.Compare(column, "==", ctx.Literal(type, value));
            const auto query = ctx.Generate(comparison);
            UNIT_ASSERT_C(query, type);

            const auto parsed = QL::ParseSource(*query, QL::EParseMode::JobQuery);
            const auto* equality = dynamic_cast<const QLAst::TBinaryOpExpression*>(&WherePredicate(*parsed));
            UNIT_ASSERT_C(equality && equality->Opcode == QL::EBinaryOp::Equal
                && equality->Lhs.size() == 1 && equality->Rhs.size() == 1, *query);
            const auto* field = dynamic_cast<const QLAst::TReferenceExpression*>(equality->Lhs.front());
            const auto* literal = dynamic_cast<const QLAst::TLiteralExpression*>(equality->Rhs.front());
            UNIT_ASSERT_C(field && field->Reference == QLAst::TReference(columnName), *query);
            UNIT_ASSERT_C(literal && std::holds_alternative<std::string>(literal->Value), *query);
            UNIT_ASSERT_VALUES_EQUAL_C(std::get<std::string>(literal->Value), value, *query);
        }
    }

    Y_UNIT_TEST(PreservesNullAndNaNGuardsUnderNot) {
        TQLFilterTestContext ctx;
        const auto x = ctx.Column("x", EDataSlot::Int8, /*optional*/ true);
        const auto d = ctx.Column("d", EDataSlot::Double, /*optional*/ true);

        // NOT COALESCE(x != 1, FALSE) must accept NULL.
        ExpectQueryWithMinDepth(
            ctx,
            ctx.Not(ctx.Coalesce(ctx.Compare(x, "!=", ctx.Literal(EDataSlot::Int32, "1")), false)),
            "* WHERE NOT (`x` != NULL AND `x` != 1)",
            4);

        // NOT COALESCE(d >= 0.5, TRUE) must accept NaN and reject NULL.
        const auto notGreaterOrNull = ctx.Not(ctx.Coalesce(ctx.Compare(d, ">=", ctx.Literal(EDataSlot::Double, "0.5")), true));
        constexpr TStringBuf expected = "* WHERE NOT (`d` = NULL OR (`d` >= 0.5 AND `d` <= inf))";
        ExpectQueryWithMinDepth(ctx, notGreaterOrNull, expected, 5);
        ExpectQuery(ctx, notGreaterOrNull, expected, /*depthLimit*/ 0);
    }

    Y_UNIT_TEST(RejectsDisjunctionWithUnrepresentableNaNLiteral) {
        TQLFilterTestContext ctx;
        const auto d = ctx.Column("d", EDataSlot::Double);
        const auto positive = ctx.Compare(d, ">", ctx.Literal(EDataSlot::Double, "0.0"));
        const auto notEqualToNaN = ctx.Compare(d, "!=", ctx.Literal(EDataSlot::Double, "nan"));

        // d > 0.0 OR d != NaN: keeping just the first disjunct would lose rows such as d = -1.
        UNIT_ASSERT(!ctx.Generate(ctx.Or({positive, notEqualToNaN})));
    }

    Y_UNIT_TEST(BalancesLogicalExpressionsWithinDepthLimit) {
        TQLFilterTestContext ctx;
        const auto a = ctx.Column("a", EDataSlot::Bool);
        const auto b = ctx.Column("b", EDataSlot::Bool);
        const auto c = ctx.Column("c", EDataSlot::Bool);
        const auto d = ctx.Column("d", EDataSlot::Bool);
        const auto e = ctx.Column("e", EDataSlot::Bool);
        const auto f = ctx.Column("f", EDataSlot::Bool);
        const auto trueLiteral = ctx.Literal(EDataSlot::Bool, "true");
        const auto falseLiteral = ctx.Literal(EDataSlot::Bool, "false");
        struct TCase {
            TExprNode::TPtr Predicate;
            TStringBuf ExpectedLogicalForm;
            ui32 MinDepth;
        };
        for (const auto& [predicate, expectedLogicalForm, minDepth] : {
            TCase{ctx.Or({a, trueLiteral, b, falseLiteral}), "* WHERE a OR TRUE OR b OR FALSE", 3},
            TCase{ctx.And({a, b, c, d, e}), "* WHERE a AND b AND c AND d AND e", 4},
            TCase{ctx.Or({a, b, c, ctx.And({d, ctx.Or({e, f})})}), "* WHERE a OR b OR c OR (d AND (e OR f))", 4},
        }) {
            ExpectLogicalQueryWithMinDepth(ctx, predicate, expectedLogicalForm, minDepth);
        }
    }
}

} // namespace
} // namespace NYql
