#include <ydb/core/kqp/host/new_parser/kqp_expr_lowering.h>
#include <ydb/core/kqp/host/new_parser/kqp_new_parser.h>

#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

using namespace NNewYql;
using namespace NYql;

// Exercise scalar construction independently of relational annotation. Tests
// provide column/parameter/callable bindings explicitly, then run the existing
// YQL annotator to check both inferred types and rewritten expression shapes.
struct TFixture {
    TExprContext Ctx;
    TTypeAnnotationContext Types;
    std::unique_ptr<TScript> Ast;
    TExpressionBindings Bindings;
    TExpressionLoweringSettings Settings;

    void Parse(const TString& query) {
        auto parsed = ParseQueryWithNewParser(query, {}, false);
        UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
        Ast = std::move(parsed.Ast);
    }

    const TExpression& Expression(size_t index = 0) const {
        const auto& query = *std::get<TQueryStatement>(Ast->Statements[0].Value).Query;
        return *std::get<TSelect>(query.Body).Columns[index].Expression;
    }

    TExprNode::TPtr Lower(size_t index = 0) {
        return LowerExpression(Expression(index), *Ast, Bindings, Ctx, Settings);
    }

    TExprNode::TPtr Annotate(TExprNode::TPtr root) {
        UNIT_ASSERT_C(root, Ctx.IssueManager.GetIssues().ToString());
        // Each call annotates a new root, including its pre-annotation stages.
        Ctx.Step.Reset();
        UNIT_ASSERT_C(InstantAnnotateTypes(root, Ctx, false, Types), Ctx.IssueManager.GetIssues().ToString());
        UNIT_ASSERT(root->GetTypeAnn());
        return root;
    }

    TExprNode::TPtr Literal(TStringBuf type, TStringBuf value) {
        return Ctx.NewCallable(TPositionHandle{}, type, {Ctx.NewAtom(TPositionHandle{}, value)});
    }
};

// Inspect the annotator-returned root: lowering alone does not assign types,
// and annotation may replace the original callable while coercing operands.
void CheckType(const TString& expression, TStringBuf expected) {
    TFixture f;
    f.Parse("SELECT " + expression + ";");
    const auto node = f.Annotate(f.Lower());
    UNIT_ASSERT_VALUES_EQUAL_C(FormatType(node->GetTypeAnn()), expected, expression);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpNewYqlExpressionLowering) {
    // Run literal callables through the shared annotator to cover suffixes,
    // bases and inferred widths instead of checking callable text alone.
    Y_UNIT_TEST(LiteralsUseExistingTypeAnnotation) {
        for (const auto& [sql, type] : TVector<std::pair<TString, TString>>{
            {"42", "Int32"}, {"2147483648", "Int64"}, {"9223372036854775808", "Uint64"},
            {"18446744073709551615ul", "Uint64"}, {"0x2a", "Int32"}, {"0o52", "Int32"}, {"0b101010", "Int32"},
            {"1u", "Uint32"}, {"1l", "Int64"}, {"127t", "Int8"}, {"255ut", "Uint8"},
            {"32767s", "Int16"}, {"65535us", "Uint16"}, {"1.25", "Double"}, {"1.25f", "Float"},
            {"TRUE", "Bool"}, {"NULL", "Null"}, {"'text'", "String"}, {"'text'u", "Utf8"},
            {"'{}'j", "Json"}, {"'{}'y", "Yson"},
        }) {
            CheckType(sql, type);
        }
    }

    // The minimum signed value has no representable positive signed magnitude.
    Y_UNIT_TEST(NegativeIntegerBoundaries) {
        for (const auto& [sql, type] : TVector<std::pair<TString, TString>>{
            {"-2147483648", "Int32"}, {"-2147483649", "Int64"}, {"-9223372036854775808", "Int64"},
            {"-9223372036854775808l", "Int64"}, {"-128t", "Int8"}, {"-32768s", "Int16"},
        }) {
            CheckType(sql, type);
        }
    }

    // Some errors are caught while decoding, others by existing callable rules;
    // both must produce diagnostics rather than a usable typed expression.
    Y_UNIT_TEST(InvalidLiteralsAreRejected) {
        for (const TString sql : {"18446744073709551616", "-9223372036854775809", "128t", "256ut", "-129t", "'\\xff'u"}) {
            TFixture f;
            f.Parse("SELECT " + sql + ";");
            auto root = f.Lower();
            if (root) {
                UNIT_ASSERT_C(!InstantAnnotateTypes(root, f.Ctx, false, f.Types), sql);
            }
            UNIT_ASSERT_C(!f.Ctx.IssueManager.GetIssues().Empty(), sql);
        }
    }

    // Decoded contents and dialect-sensitive quoting must agree with literal types.
    Y_UNIT_TEST(StringEscapesAndAnsiQuoting) {
        TFixture f;
        f.Parse("SELECT 'a\\n\\x00b', @@one@@@@two@@;");
        const auto first = f.Annotate(f.Lower());
        UNIT_ASSERT_VALUES_EQUAL(first->Head().Content(), TString("a\n\0b", 4));
        const auto second = f.Annotate(f.Lower(1));
        UNIT_ASSERT_VALUES_EQUAL(second->Head().Content(), "one@@two");

        TFixture ansi;
        ansi.Parse("--!ansi_lexer\nSELECT 'it''s';");
        UNIT_ASSERT_VALUES_EQUAL(ansi.Annotate(ansi.Lower())->Head().Content(), "it's");

        TFixture unicode;
        unicode.Settings.UnicodeLiterals = true;
        unicode.Parse("SELECT 'text';");
        UNIT_ASSERT_VALUES_EQUAL(FormatType(unicode.Annotate(unicode.Lower())->GetTypeAnn()), "Utf8");
    }

    // Optional operands must retain SQL NULL semantics through existing YQL rules.
    Y_UNIT_TEST(OperatorsAndNullablePredicates) {
        for (const auto& [sql, type] : TVector<std::pair<TString, TString>>{
            {"1 + 2l * 3 - 4", "Int64"}, {"+1", "Int32"}, {"~1u", "Uint32"},
            {"7u & 3u | 1u", "Uint32"}, {"1u << 2u", "Uint32"},
            {"TRUE AND NOT FALSE OR FALSE XOR TRUE", "Bool"},
            {"1 < 2", "Bool"}, {"1 <= 2 AND 3 >= 2", "Bool"}, {"1 != 2", "Bool"},
            {"NULL IS NULL", "Bool"}, {"NULL IS NOT NULL", "Bool"},
            {"NULL IS DISTINCT FROM 1", "Bool"}, {"NULL IS NOT DISTINCT FROM NULL", "Bool"},
            {"Just(1) = 2", "Optional<Bool>"},
            {"'a' || 'b'", "String"}, {"NULL ?? 1", "Int32"},
        }) {
            CheckType(sql, type);
        }
    }

    // Identical SQL text can require different callable trees under these settings.
    Y_UNIT_TEST(DivisionAndCheckedArithmeticSettings) {
        TFixture f;
        f.Parse("SELECT 4 / 2, 1 + 2;");
        UNIT_ASSERT_VALUES_EQUAL(f.Lower()->Content(), "/MayWarn");
        f.Settings.ClassicDivision = false;
        auto root = f.Lower();
        UNIT_ASSERT(root->Head().IsCallable("SafeCast"));
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Annotate(root)->GetTypeAnn()), "Double");
        f.Settings.ClassicDivision = true;
        f.Settings.CheckedOps = true;
        UNIT_ASSERT(f.Lower()->IsCallable("CheckedDiv"));
        root = f.Lower(1);
        UNIT_ASSERT(root->IsCallable("CheckedAdd"));
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Annotate(root)->GetTypeAnn()), "Optional<Int32>");
    }

    // Delegate cast validation and resulting optionality to the shared type rules.
    Y_UNIT_TEST(CastsUseExistingCoercions) {
        CheckType("CAST(1 AS Int64)", "Int64");
        CheckType("CAST('123' AS Int32)", "Optional<Int32>");
        CheckType("CAST(1 AS Int32?)", "Optional<Int32>");
        CheckType("BITCAST(1 AS Uint32)", "Uint32");
        CheckType("CAST([1, 2] AS List<Int64>)", "List<Int64>");
        CheckType("CAST((1, 'a') AS Tuple<Int64, String>)", "Tuple<Int64,String>");
        CheckType("CAST(1 AS Decimal(12, 2))", "Decimal(12,2)");
    }

    Y_UNIT_TEST(DecimalConstructors) {
        CheckType("Decimal('0.06', 12, 2) - Decimal('0.01', 12, 2)", "Decimal(12,2)");
        for (const TString sql : {"Decimal('bad', 12, 2)", "Decimal('1', 0, 0)", "Decimal('1', 12, 13)"}) {
            TFixture f;
            f.Parse("SELECT " + sql + ";");
            auto root = f.Lower();
            UNIT_ASSERT(root);
            UNIT_ASSERT_C(!InstantAnnotateTypes(root, f.Ctx, false, f.Types), sql);
            UNIT_ASSERT_C(!f.Ctx.IssueManager.GetIssues().Empty(), sql);
        }
    }

    // Structured predicates must lower into callables the existing annotator accepts,
    // including CASE branch coercion and membership options.
    Y_UNIT_TEST(CaseBetweenAndIn) {
        CheckType("CASE WHEN TRUE THEN 1 ELSE 2l END", "Int64");
        CheckType("CASE 1 WHEN 1 THEN 'yes' WHEN 2 THEN 'two' ELSE 'no' END", "String");
        CheckType("CASE WHEN Just(TRUE) THEN 1 ELSE 2 END", "Int32");
        CheckType("CASE WHEN NULL THEN 1 ELSE 2 END", "Int32");
        CheckType("2 BETWEEN 1 AND 3", "Bool");
        CheckType("2 NOT BETWEEN SYMMETRIC 3 AND 1", "Bool");
        CheckType("2 IN (1, 2, 3)", "Bool");
        CheckType("2 IN (2)", "Bool");
        CheckType("2 NOT IN (1)", "Bool");
        CheckType("(1, 2) IN ((1, 2))", "Bool");
        CheckType("2 NOT IN (1, 3)", "Bool");
        TFixture f;
        f.Settings.AnsiIn = true;
        f.Parse("SELECT 2 IN COMPACT (1, NULL);");
        auto root = f.Lower();
        UNIT_ASSERT(root->IsCallable("SqlIn"));
        UNIT_ASSERT_VALUES_EQUAL(root->Tail().ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Annotate(root)->GetTypeAnn()), "Optional<Bool>");
    }

    // Assert the rewritten form, because pre-annotation SQL access callables are
    // not necessarily the expressions relational lowering should consume.
    Y_UNIT_TEST(CollectionsAndAccessAreRewrittenByAnnotator) {
        CheckType("(1, 'a')", "Tuple<Int32,String>");
        CheckType("[1, 2l]", "List<Int64>");
        CheckType("[1, 2][0u]", "Optional<Int32>");
        CheckType("AsTuple(1, 'a')", "Tuple<Int32,String>");
        TFixture f;
        f.Parse("SELECT $record.`some``field`;");
        f.Bindings.Parameters["$record"] = f.Ctx.NewCallable(TPositionHandle{}, "AsStruct", {
            f.Ctx.NewList(TPositionHandle{}, {f.Ctx.NewAtom(TPositionHandle{}, "some`field"), f.Literal("Int32", "42")})});
        auto root = f.Lower();
        UNIT_ASSERT(root->IsCallable("SqlAccess"));
        root = f.Annotate(root);
        UNIT_ASSERT(root->IsCallable("Member"));
        UNIT_ASSERT_VALUES_EQUAL(FormatType(root->GetTypeAnn()), "Int32");
    }

    // SQL name normalization and argument defaults must produce valid YQL calls.
    Y_UNIT_TEST(ScalarFunctionsUseSqlNames) {
        CheckType("aBs(-1)", "Int32");
        CheckType("Length('abc')", "Uint32");
        CheckType("ListLength([1, 2])", "Uint64");
        CheckType("ListHasItems([1])", "Bool");
        CheckType("Starts_With('abc', 'a')", "Bool");
        CheckType("MaxOf(1, 2l)", "Int64");
        CheckType("Coalesce(Just(1), 2)", "Int32");
        CheckType("IF(Just(TRUE), 1, 2)", "Int32");
        CheckType("IF(NULL, 1)", "Optional<Int32>");
        CheckType("Substring('abc', 1u)", "String");
    }

    // Bind by resolved ID and argument identity rather than looking up text again.
    Y_UNIT_TEST(ResolvedColumnsPreserveSelfJoinIdentity) {
        TFixture f;
        f.Parse("SELECT l.id + r.id FROM t AS l JOIN t AS r ON l.id = r.id;");
        auto table = std::make_shared<TTableMetadata>();
        table->Columns.push_back({"id", 1, "Int64", true});
        auto names = ResolveNames(*f.Ast, {"local", "/Root", {}},
            [&](const TString&, const TString&) { return table; });
        UNIT_ASSERT_C(names.IsOk(), names.Issues.ToString());
        f.Bindings.Names = names.Names.get();
        const auto& binary = std::get<TBinary>(f.Expression().Value);
        const auto leftId = names.Names->ColumnReferences.at(binary.Left.get());
        const auto rightId = names.Names->ColumnReferences.at(binary.Right.get());
        UNIT_ASSERT(leftId != rightId);
        const auto left = f.Ctx.NewArgument(TPositionHandle{}, "left");
        const auto right = f.Ctx.NewArgument(TPositionHandle{}, "right");
        f.Bindings.Columns[leftId] = left;
        f.Bindings.Columns[rightId] = right;
        auto root = f.Lower();
        UNIT_ASSERT(root->ChildPtr(0) == left);
        UNIT_ASSERT(root->ChildPtr(1) == right);
        auto lambda = f.Ctx.NewLambda(TPositionHandle{}, f.Ctx.NewArguments(TPositionHandle{}, {left, right}), std::move(root));
        // The caller's actual inputs determine nullability, not base metadata.
        root = f.Ctx.NewCallable(TPositionHandle{}, "Apply", {lambda, f.Literal("Int64", "1"),
            f.Ctx.NewCallable(TPositionHandle{}, "Just", {f.Literal("Int64", "2")})});
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Annotate(root)->GetTypeAnn()), "Optional<Int64>");
    }

    // Caller-provided parameter values and callable nodes belong to the destination
    // context and must be applied without introducing free arguments.
    Y_UNIT_TEST(ParameterAndResolvedCallableBindings) {
        TFixture f;
        f.Parse("SELECT $f($p), Demo::Identity($p);");
        const auto arg = f.Ctx.NewArgument(TPositionHandle{}, "value");
        auto lambda = f.Ctx.NewLambda(TPositionHandle{}, f.Ctx.NewArguments(TPositionHandle{}, {arg}), TExprNode::TPtr(arg));
        f.Bindings.Parameters["$f"] = lambda;
        f.Bindings.Parameters["$p"] = f.Literal("Int64", "7");
        f.Bindings.Callables["Demo::Identity"] = lambda;
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Annotate(f.Lower())->GetTypeAnn()), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Annotate(f.Lower(1))->GetTypeAnn()), "Int64");
    }

    // Reusing YQL callable rules must not lose the original SQL diagnostic position.
    Y_UNIT_TEST(AnnotationReportsTypeErrorsAtSqlPosition) {
        TFixture f;
        f.Parse("SELECT\n  1 + 'bad';");
        auto root = f.Lower();
        UNIT_ASSERT(root);
        UNIT_ASSERT(!root->GetTypeAnn());
        UNIT_ASSERT_VALUES_EQUAL(f.Ctx.GetPosition(root->Pos()).Row, 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Ctx.GetPosition(root->Pos()).Column, 3);
        UNIT_ASSERT(!InstantAnnotateTypes(root, f.Ctx, false, f.Types));
        UNIT_ASSERT(!f.Ctx.IssueManager.GetIssues().Empty());
    }

    // A malformed supported expression is an error, not a reason to select fallback.
    Y_UNIT_TEST(MissingBindingsAndElseProduceDiagnostics) {
        for (const TString sql : {"missing", "$missing", "CASE WHEN TRUE THEN 1 END"}) {
            TFixture f;
            f.Parse("SELECT\n  " + sql + ";");
            UNIT_ASSERT(!f.Lower());
            const auto issues = f.Ctx.IssueManager.GetIssues();
            UNIT_ASSERT(!issues.Empty());
            UNIT_ASSERT_VALUES_EQUAL(issues.begin()->Position.Row, 2);
            UNIT_ASSERT_VALUES_EQUAL(issues.begin()->Position.Column, 3);
        }
    }

    // Reject aggregate/window/subquery semantics before accidentally treating them
    // as scalar callables with a superficially compatible spelling.
    Y_UNIT_TEST(UnsupportedSemanticsRequestFallback) {
        for (const TString sql : {"COUNT(*)", "SUM(1)", "MIN(1)", "SUM(1) OVER ()", "SUM(DISTINCT 1)",
            "(SELECT 1)", "EXISTS (SELECT 1)", "*"}) {
            TFixture f;
            f.Parse("SELECT " + sql + ";");
            UNIT_ASSERT_EXCEPTION(f.Lower(), TNewYqlParserFallback);
            UNIT_ASSERT(f.Ctx.IssueManager.GetIssues().Empty());
        }
    }
}

} // namespace NKikimr::NKqp
