#include <ydb/core/kqp/host/new_parser/kqp_new_parser.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

using namespace NNewYql;

// These tests inspect syntax shape directly. Acceptance here does not imply
// that resolution or native operator lowering supports the feature. The helper
// returns only the owning AST, after the ANTLR parse tree has been destroyed.
std::unique_ptr<TScript> Parse(const TString& text) {
    auto parsed = ParseQueryWithNewParser(text, {}, false);
    UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
    return std::move(parsed.Ast);
}

const TSelect& Select(const TScript& script, size_t index = 0) {
    return std::get<TSelect>(std::get<TQueryStatement>(script.Statements.at(index).Value).Query->Body);
}

template <typename T>
const T& As(const TExpressionPtr& expression) {
    UNIT_ASSERT(expression);
    UNIT_ASSERT_C(std::holds_alternative<T>(expression->Value), "Unexpected expression kind: " << expression->Value.index());
    return std::get<T>(expression->Value);
}

void LiteralIs(const TExpressionPtr& expression, TStringBuf spelling) {
    UNIT_ASSERT_VALUES_EQUAL(As<TLiteral>(expression).Spelling, spelling);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpNewYqlAst) {
    Y_UNIT_TEST(SetOperationRejectsMisplacedTrailingClauses) {
        for (const TString sql : {
            "SELECT 1 AS n ORDER BY n UNION ALL SELECT 2 AS n;",
            "SELECT 1 AS n LIMIT 1 INTERSECT SELECT 2 AS n;",
            "SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n EXCEPT SELECT 3 AS n;",
            "SELECT 1 AS n UNION ALL SELECT 2 AS n ASSUME ORDER BY n;"}) {
            UNIT_ASSERT_EXCEPTION(Parse(sql), TNewYqlParserFallback);
        }
    }

    Y_UNIT_TEST(SetOperationTrailingClauses) {
        auto ast = Parse("SELECT 3 AS n UNION ALL SELECT 2 AS n INTERSECT SELECT 1 AS n ORDER BY n DESC LIMIT 2 OFFSET 1;");
        const auto& query = *std::get<TQueryStatement>(ast->Statements[0].Value).Query;
        UNIT_ASSERT_VALUES_EQUAL(query.OrderBy.size(), 1);
        LiteralIs(query.Limit, "2");
        LiteralIs(query.Offset, "1");
        const auto& right = *std::get<TSetOperation>(query.Body).Right;
        UNIT_ASSERT(right.OrderBy.empty() && !right.Limit);
        const auto& last = *std::get<TSetOperation>(right.Body).Right;
        UNIT_ASSERT(std::get<TSelect>(last.Body).OrderBy.empty() && !last.Limit);

        auto local = Parse("(SELECT 3 AS n ORDER BY n LIMIT 1) UNION ALL (SELECT 2 AS n ORDER BY n DESC LIMIT 1);");
        const auto& compound = *std::get<TQueryStatement>(local->Statements[0].Value).Query;
        UNIT_ASSERT(compound.OrderBy.empty() && !compound.Limit);
        const auto& set = std::get<TSetOperation>(compound.Body);
        for (const auto* branch : {set.Left.get(), set.Right.get()}) {
            UNIT_ASSERT_VALUES_EQUAL(std::get<TSelect>(branch->Body).OrderBy.size(), 1);
            LiteralIs(branch->Limit, "1");
        }
    }

    // Check the tree, not evaluated constants: wrong associativity can still
    // produce the same value for convenient literal inputs.
    Y_UNIT_TEST(PrecedenceAndAssociativity) {
        auto ast = Parse("SELECT 10 - 3 - 2 * 4, a OR b AND c, x ?? y ?? z, -x * y;");
        const auto& columns = Select(*ast).Columns;
        const auto& subtract = As<TBinary>(columns[0].Expression);
        UNIT_ASSERT(subtract.Op == EBinaryOp::Subtract);
        UNIT_ASSERT(As<TBinary>(subtract.Left).Op == EBinaryOp::Subtract);
        UNIT_ASSERT(As<TBinary>(subtract.Right).Op == EBinaryOp::Multiply);
        LiteralIs(As<TBinary>(subtract.Left).Left, "10");
        LiteralIs(As<TBinary>(subtract.Right).Right, "4");
        const auto& logical = As<TBinary>(columns[1].Expression);
        UNIT_ASSERT(logical.Op == EBinaryOp::Or);
        UNIT_ASSERT(As<TBinary>(logical.Right).Op == EBinaryOp::And);
        const auto& coalesce = As<TBinary>(columns[2].Expression);
        UNIT_ASSERT(coalesce.Op == EBinaryOp::Coalesce);
        UNIT_ASSERT(As<TBinary>(coalesce.Right).Op == EBinaryOp::Coalesce);
        UNIT_ASSERT(As<TUnary>(As<TBinary>(columns[3].Expression).Left).Op == EUnaryOp::Minus);
    }

    // Assert clause placement and join inputs together so a nested grammar
    // wrapper cannot cause the builder to pick the wrong expression.
    Y_UNIT_TEST(SelectClausesAndJoin) {
        auto ast = Parse(R"(
            SELECT DISTINCT t.id AS id, COUNT(DISTINCT u.value) AS n
            FROM plato.`/Root/t` AS t LEFT JOIN `/Root/u` AS u ON t.id = u.id
            WHERE t.id > $min GROUP BY t.id HAVING COUNT(*) > 1
            ORDER BY n DESC, id ASC LIMIT 10 OFFSET 2;
        )");
        const auto& query = *std::get<TQueryStatement>(ast->Statements[0].Value).Query;
        const auto& select = Select(*ast);
        UNIT_ASSERT(select.Quantifier == ESetQuantifier::Distinct);
        UNIT_ASSERT_VALUES_EQUAL(select.Columns[0].Alias->Spelling, "id");
        UNIT_ASSERT(As<TCall>(select.Columns[1].Expression).Quantifier == ESetQuantifier::Distinct);
        const auto& join = std::get<TJoin>(select.From->Value);
        UNIT_ASSERT(join.Kind == EJoinKind::Left);
        const auto& table = std::get<TTable>(join.Left->Value);
        UNIT_ASSERT_VALUES_EQUAL(table.Name.Spelling, "`/Root/t`");
        UNIT_ASSERT_VALUES_EQUAL(table.Cluster->Spelling, "plato");
        UNIT_ASSERT_VALUES_EQUAL(join.Left->Alias->Spelling, "t");
        UNIT_ASSERT(As<TBinary>(join.On).Op == EBinaryOp::Equal);
        UNIT_ASSERT_VALUES_EQUAL(As<TParameter>(As<TBinary>(select.Where).Right).Spelling, "$min");
        UNIT_ASSERT_VALUES_EQUAL(select.GroupBy.size(), 1);
        UNIT_ASSERT(As<TCall>(As<TBinary>(select.Having).Left).StarArgument);
        UNIT_ASSERT_VALUES_EQUAL(select.OrderBy.size(), 2);
        UNIT_ASSERT(select.OrderBy[0].Direction == ESortDirection::Descending);
        UNIT_ASSERT(select.OrderBy[1].Direction == ESortDirection::Ascending);
        LiteralIs(query.Limit, "10");
        LiteralIs(query.Offset, "2");
    }

    // Keep syntax modifiers even when their later semantic phases require
    // fallback; parsing must not silently simplify the query.
    Y_UNIT_TEST(AnyUsingAndQualifiedStar) {
        auto ast = Parse("SELECT l.*, r.v FROM ANY t AS l INNER JOIN ANY u AS r USING(id);");
        const auto& select = Select(*ast);
        UNIT_ASSERT_VALUES_EQUAL(As<TStar>(select.Columns[0].Expression).Qualifier->Spelling, "l");
        const auto& join = std::get<TJoin>(select.From->Value);
        UNIT_ASSERT(join.LeftAny && join.RightAny);
        UNIT_ASSERT_VALUES_EQUAL(join.Using[0].Spelling, "id");
        UNIT_ASSERT(!join.On);
    }

    // ANY on one input must not mark the other input, including across the
    // outer/semi/only join spellings accepted by the grammar.
    Y_UNIT_TEST(AnyJoinInputsAreIndependent) {
        for (bool ansi : {false, true}) {
            for (bool leftAny : {false, true}) {
                for (bool rightAny : {false, true}) {
                    TString query = ansi ? "--!ansi_lexer\n" : "";
                    query += "SELECT * FROM ";
                    query += leftAny ? "ANY " : "";
                    query += "t AS l LEFT JOIN ";
                    query += rightAny ? "ANY " : "";
                    query += "u AS r ON l.id = r.id;";
                    auto ast = Parse(query);
                    const auto& source = *Select(*ast).From;
                    const auto& join = std::get<TJoin>(source.Value);
                    UNIT_ASSERT(join.Kind == EJoinKind::Left);
                    UNIT_ASSERT_VALUES_EQUAL(join.LeftAny, leftAny);
                    UNIT_ASSERT_VALUES_EQUAL(join.RightAny, rightAny);
                    UNIT_ASSERT_VALUES_EQUAL(join.Left->Alias->Spelling, "l");
                    UNIT_ASSERT_VALUES_EQUAL(join.Right->Alias->Spelling, "r");
                    UNIT_ASSERT(As<TBinary>(join.On).Op == EBinaryOp::Equal);
                }
            }
        }
    }

    // The accumulated left relation of a later join does not inherit an
    // earlier input's ANY flag; nested sources have their own modifier state.
    Y_UNIT_TEST(AnyDoesNotLeakAcrossJoinChainOrSubquery) {
        auto ast = Parse(R"(SELECT * FROM ANY t AS l
            JOIN ANY (SELECT * FROM u JOIN w ON u.id = w.id) WITH infer AS r SAMPLE 0.5 ON l.id = r.id
            JOIN v AS s ON r.id = s.id;)");
        const auto& source = *Select(*ast).From;
        const auto& outer = std::get<TJoin>(source.Value);
        UNIT_ASSERT(!outer.LeftAny && !outer.RightAny);
        const auto& inner = std::get<TJoin>(outer.Left->Value);
        UNIT_ASSERT(inner.LeftAny && inner.RightAny);
        UNIT_ASSERT_VALUES_EQUAL(inner.Right->Alias->Spelling, "r");
        UNIT_ASSERT_VALUES_EQUAL(inner.Right->Hints.size(), 1);
        LiteralIs(inner.Right->Sampling->Rate, "0.5");
        const auto& derived = std::get<TDerivedTable>(inner.Right->Value);
        const auto& nested = std::get<TJoin>(std::get<TSelect>(derived.Query->Body).From->Value);
        UNIT_ASSERT(!nested.LeftAny && !nested.RightAny);
        UNIT_ASSERT_VALUES_EQUAL(outer.Right->Alias->Spelling, "s");
    }

    // Normalize the two LIMIT syntaxes into count/offset slots while retaining
    // the derived query as a separate subtree.
    Y_UNIT_TEST(DerivedTableAndCommaLimit) {
        auto ast = Parse("SELECT d.id FROM (SELECT id FROM t LIMIT 2, 5) AS d;");
        const auto& from = *Select(*ast).From;
        UNIT_ASSERT_VALUES_EQUAL(from.Alias->Spelling, "d");
        const auto& inner = *std::get<TDerivedTable>(from.Value).Query;
        LiteralIs(inner.Limit, "5");
        LiteralIs(inner.Offset, "2");
    }

    // Predicate-specific operands and flags must survive construction rather
    // than collapsing into an incomplete binary expression.
    Y_UNIT_TEST(Predicates) {
        auto ast = Parse(R"(SELECT x NOT IN (1, 2), x NOT BETWEEN SYMMETRIC 1 AND 3,
            x IS NOT NULL, x IS NOT DISTINCT FROM y, name NOT LIKE 'a%' ESCAPE '!';)");
        const auto& columns = Select(*ast).Columns;
        const auto& in = As<TIn>(columns[0].Expression);
        UNIT_ASSERT(in.Negated);
        UNIT_ASSERT_VALUES_EQUAL(As<TTuple>(in.Collection).Elements.size(), 2);
        const auto& between = As<TBetween>(columns[1].Expression);
        UNIT_ASSERT(between.Negated && between.Symmetric);
        LiteralIs(between.Lower, "1");
        LiteralIs(between.Upper, "3");
        UNIT_ASSERT(As<TUnary>(columns[2].Expression).Op == EUnaryOp::IsNotNull);
        UNIT_ASSERT(As<TBinary>(columns[3].Expression).Op == EBinaryOp::IsNotDistinctFrom);
        const auto& like = As<TPatternMatch>(columns[4].Expression);
        UNIT_ASSERT(like.Negated && like.Op == EPatternOp::Like);
        LiteralIs(like.Escape, "'!'");
    }

    Y_UNIT_TEST(InSingletonCollectionsPreserveTheirOuterTuple) {
        auto ast = Parse("SELECT x IN (1), x IN ((1, 2)), x IN $items, x IN (SELECT id FROM t), (1), "
            "x IN ((SELECT id FROM t));");
        const auto& columns = Select(*ast).Columns;
        const auto& singleton = As<TTuple>(As<TIn>(columns[0].Expression).Collection);
        UNIT_ASSERT_VALUES_EQUAL(singleton.Elements.size(), 1);
        LiteralIs(singleton.Elements[0].Expression, "1");
        const auto& tuples = As<TTuple>(As<TIn>(columns[1].Expression).Collection);
        UNIT_ASSERT_VALUES_EQUAL(tuples.Elements.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(As<TTuple>(tuples.Elements[0].Expression).Elements.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(As<TParameter>(As<TIn>(columns[2].Expression).Collection).Spelling, "$items");
        As<TSubquery>(As<TIn>(columns[3].Expression).Collection);
        LiteralIs(columns[4].Expression, "1");
        As<TSubquery>(As<TIn>(columns[5].Expression).Collection);
    }

    // Distinguish grouping from tuples/lists and relational subqueries even
    // though their grammar shares parenthesized alternatives.
    Y_UNIT_TEST(ParenthesesCollectionsAndSubqueries) {
        auto ast = Parse("SELECT (1 + 2), (1,), (), [1, 2], EXISTS (SELECT 1 FROM t), (SELECT MAX(x) FROM u);");
        const auto& columns = Select(*ast).Columns;
        UNIT_ASSERT(As<TBinary>(columns[0].Expression).Op == EBinaryOp::Add);
        UNIT_ASSERT_VALUES_EQUAL(As<TTuple>(columns[1].Expression).Elements.size(), 1);
        UNIT_ASSERT(As<TTuple>(columns[2].Expression).Elements.empty());
        UNIT_ASSERT_VALUES_EQUAL(As<TList>(columns[3].Expression).Elements.size(), 2);
        UNIT_ASSERT(As<TExists>(columns[4].Expression).Query);
        UNIT_ASSERT(As<TSubquery>(columns[5].Expression).Query);
    }

    // Preserve CASE branches and structured cast targets before lowering can
    // choose callables or apply type coercions.
    Y_UNIT_TEST(CaseAndCast) {
        auto ast = Parse(R"(SELECT CASE x WHEN 1 THEN 'one' ELSE 'many' END,
            CASE WHEN x > 0 THEN 1 END, CAST($x AS Int64?), CAST(1 AS Decimal(22, 9)),
            CAST($items AS List<Int32>);)");
        const auto& columns = Select(*ast).Columns;
        const auto& simple = As<TCase>(columns[0].Expression);
        UNIT_ASSERT(simple.Operand);
        LiteralIs(simple.Branches[0].When, "1");
        LiteralIs(simple.Else, "'many'");
        const auto& searched = As<TCase>(columns[1].Expression);
        UNIT_ASSERT(!searched.Operand && !searched.Else);
        const auto& optional = As<TCast>(columns[2].Expression);
        UNIT_ASSERT_VALUES_EQUAL(optional.Type.Name.Spelling, "Int64");
        UNIT_ASSERT_VALUES_EQUAL(optional.Type.OptionalDepth, 1);
        const auto& decimal = As<TCast>(columns[3].Expression).Type;
        UNIT_ASSERT_VALUES_EQUAL(decimal.Parameters[0], "22");
        UNIT_ASSERT_VALUES_EQUAL(decimal.Parameters[1], "9");
        UNIT_ASSERT_VALUES_EQUAL(As<TCast>(columns[4].Expression).Type.Arguments[0].Name.Spelling, "Int32");
    }

    // A left fold at only one precedence level would misgroup INTERSECT with
    // UNION/EXCEPT; assert the nested set-operation tree explicitly.
    Y_UNIT_TEST(CteAndSetOperationPrecedence) {
        auto ast = Parse("WITH c(id) AS (SELECT 1) SELECT id FROM c UNION ALL SELECT 2 INTERSECT SELECT 3 EXCEPT SELECT 4;");
        const auto& query = *std::get<TQueryStatement>(ast->Statements[0].Value).Query;
        UNIT_ASSERT_VALUES_EQUAL(query.With.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(query.With[0].Name.Spelling, "c");
        UNIT_ASSERT_VALUES_EQUAL(query.With[0].Columns[0].Spelling, "id");
        const auto& except = std::get<TSetOperation>(query.Body);
        UNIT_ASSERT(except.Op == ESetOperation::Except);
        const auto& unionAll = std::get<TSetOperation>(except.Left->Body);
        UNIT_ASSERT(unionAll.Op == ESetOperation::Union && unionAll.Quantifier == ESetQuantifier::All);
        UNIT_ASSERT(std::get<TSetOperation>(unionAll.Right->Body).Op == ESetOperation::Intersect);
    }

    // Retain nested grouping syntax instead of expanding combinations during
    // parsing; downstream support must see the original grouping form.
    Y_UNIT_TEST(GroupingSets) {
        auto ast = Parse("SELECT a, COUNT(*) FROM t GROUP BY GROUPING SETS ((), ROLLUP(a, b), CUBE(a, b));");
        const auto& sets = Select(*ast).GroupBy[0];
        UNIT_ASSERT(sets.Kind == EGroupingKind::Sets);
        UNIT_ASSERT_VALUES_EQUAL(sets.Elements.size(), 3);
        UNIT_ASSERT(As<TTuple>(sets.Elements[0].Expression.Expression).Elements.empty());
        UNIT_ASSERT(sets.Elements[1].Kind == EGroupingKind::Rollup);
        UNIT_ASSERT(sets.Elements[2].Kind == EGroupingKind::Cube);
    }

    // Suffix chains must attach in source order, and module qualification
    // must remain distinct from table/struct member access.
    Y_UNIT_TEST(MemberIndexAndModuleCall) {
        auto ast = Parse("SELECT t.record.field, $items[0], Math::Abs(-$value);");
        const auto& columns = Select(*ast).Columns;
        const auto& member = As<TMember>(columns[0].Expression);
        UNIT_ASSERT_VALUES_EQUAL(member.Member.Spelling, "field");
        UNIT_ASSERT_VALUES_EQUAL(As<TMember>(member.Object).Member.Spelling, "record");
        LiteralIs(As<TIndex>(columns[1].Expression).Index, "0");
        const auto& name = As<TName>(As<TCall>(columns[2].Expression).Callable);
        UNIT_ASSERT_VALUES_EQUAL(name.Module->Spelling, "Math");
        UNIT_ASSERT_VALUES_EQUAL(name.Name.Spelling, "Abs");
    }

    // Keep quoted spelling and statement boundaries for later dialect-aware
    // name resolution; the builder itself does not decode identifiers.
    Y_UNIT_TEST(AnsiSpellingsAndMultipleStatements) {
        auto ast = Parse("--!ansi_lexer\nEXPLAIN QUERY PLAN SELECT 'a''b' AS \"a\"\"b\"; SELECT 42ul;");
        UNIT_ASSERT(ast->AnsiLexer);
        UNIT_ASSERT_VALUES_EQUAL(ast->Statements.size(), 2);
        UNIT_ASSERT(ast->Statements[0].Explain == EExplainMode::QueryPlan);
        LiteralIs(Select(*ast).Columns[0].Expression, "'a''b'");
        UNIT_ASSERT_VALUES_EQUAL(Select(*ast).Columns[0].Alias->Spelling, "\"a\"\"b\"");
        LiteralIs(Select(*ast, 1).Columns[0].Expression, "42ul");
    }

    // Suffixes and delimiters carry type information that would be lost by
    // converting literals into generic C++ values during parsing.
    Y_UNIT_TEST(LiteralKindsAndSpellings) {
        auto ast = Parse(R"(SELECT NULL, true, 1.25, 42ul, 'a\n';)");
        const auto& columns = Select(*ast).Columns;
        UNIT_ASSERT(As<TLiteral>(columns[0].Expression).Kind == ELiteralKind::Null);
        UNIT_ASSERT(As<TLiteral>(columns[1].Expression).Kind == ELiteralKind::Boolean);
        UNIT_ASSERT(As<TLiteral>(columns[2].Expression).Kind == ELiteralKind::Real);
        UNIT_ASSERT(As<TLiteral>(columns[3].Expression).Kind == ELiteralKind::Integer);
        UNIT_ASSERT(As<TLiteral>(columns[4].Expression).Kind == ELiteralKind::String);
        LiteralIs(columns[3].Expression, "42ul");
        LiteralIs(columns[4].Expression, R"('a\n')");
    }

    // Use the AST after parser/input temporaries have gone away and verify
    // Unicode-aware ranges; copied token text must not be dangling storage.
    Y_UNIT_TEST(SourceRangesAndOwnership) {
        auto ast = Parse("-- comment\nSELECT 'Ж', x + 12;");
        const auto& literal = *Select(*ast).Columns[0].Expression;
        UNIT_ASSERT_VALUES_EQUAL(literal.Range.Line, 2);
        UNIT_ASSERT_VALUES_EQUAL(literal.Range.Column, 7);
        UNIT_ASSERT_VALUES_EQUAL(literal.Range.Begin, 18);
        UNIT_ASSERT_VALUES_EQUAL(literal.Range.End, 21);
        const auto& binary = *Select(*ast).Columns[1].Expression;
        UNIT_ASSERT_VALUES_EQUAL(binary.Range.Begin, 23);
        UNIT_ASSERT_VALUES_EQUAL(binary.Range.End, 29);
        UNIT_ASSERT_VALUES_EQUAL(As<TName>(std::get<TBinary>(binary.Value).Left).Name.Spelling, "x");
    }

    // Window and aggregate modifiers belong to the call and must retain all
    // frame/partition/order detail even before window execution is supported.
    Y_UNIT_TEST(InlineWindowWithFilterAndFrame) {
        auto ast = Parse(R"(SELECT SUM(x) FILTER (WHERE x > 0) OVER (
            PARTITION COMPACT BY department AS dep ORDER BY ts DESC, id ASC
            ROWS BETWEEN $n PRECEDING AND 1 FOLLOWING EXCLUDE TIES) FROM t;)");
        const auto& call = As<TCall>(Select(*ast).Columns[0].Expression);
        UNIT_ASSERT(As<TBinary>(call.Filter).Op == EBinaryOp::Greater);
        UNIT_ASSERT(call.NullTreatment == ENullTreatment::Default);
        const auto& window = std::get<TWindowSpecification>(call.Over->Value);
        UNIT_ASSERT(!window.BaseWindow);
        UNIT_ASSERT(window.CompactPartition);
        UNIT_ASSERT_VALUES_EQUAL(window.PartitionBy[0].Alias->Spelling, "dep");
        UNIT_ASSERT_VALUES_EQUAL(window.OrderBy.size(), 2);
        UNIT_ASSERT(window.OrderBy[0].Direction == ESortDirection::Descending);
        UNIT_ASSERT(window.OrderBy[1].Direction == ESortDirection::Ascending);
        const auto& frame = *window.Frame;
        UNIT_ASSERT(frame.Units == EWindowFrameUnits::Rows);
        UNIT_ASSERT(frame.Begin.Kind == EWindowBoundKind::Preceding);
        UNIT_ASSERT_VALUES_EQUAL(As<TParameter>(frame.Begin.Offset).Spelling, "$n");
        UNIT_ASSERT(frame.End->Kind == EWindowBoundKind::Following);
        LiteralIs(frame.End->Offset, "1");
        UNIT_ASSERT(frame.Exclusion == EWindowExclusion::Ties);
        UNIT_ASSERT(frame.Range.Begin > window.Range.Begin);
        UNIT_ASSERT(frame.Range.End < window.Range.End);
    }

    // OVER name and OVER (name) encode different inheritance rules; preserve
    // that distinction alongside explicit NULL treatment.
    Y_UNIT_TEST(NamedWindowsInheritanceAndNullTreatment) {
        auto ast = Parse(R"(SELECT FIRST_VALUE(x) IGNORE NULLS OVER w,
            LAST_VALUE(x) RESPECT NULLS OVER (w), ROW_NUMBER() OVER (),
            COUNT(*) FILTER (WHERE x > 0)
            FROM t WINDOW base AS (PARTITION BY department),
            w AS (base ORDER BY ts RANGE UNBOUNDED PRECEDING);)");
        const auto& select = Select(*ast);
        const auto& first = As<TCall>(select.Columns[0].Expression);
        UNIT_ASSERT(first.NullTreatment == ENullTreatment::Ignore);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TIdentifier>(first.Over->Value).Spelling, "w");
        const auto& last = As<TCall>(select.Columns[1].Expression);
        UNIT_ASSERT(last.NullTreatment == ENullTreatment::Respect);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TWindowSpecification>(last.Over->Value).BaseWindow->Spelling, "w");
        const auto& empty = std::get<TWindowSpecification>(As<TCall>(select.Columns[2].Expression).Over->Value);
        UNIT_ASSERT(!empty.Frame && !empty.BaseWindow && empty.PartitionBy.empty() && empty.OrderBy.empty());
        UNIT_ASSERT(!As<TCall>(select.Columns[3].Expression).Over);
        UNIT_ASSERT(As<TCall>(select.Columns[3].Expression).Filter);
        UNIT_ASSERT_VALUES_EQUAL(select.Windows.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(select.Windows[0].Name.Spelling, "base");
        UNIT_ASSERT_VALUES_EQUAL(select.Windows[1].Name.Spelling, "w");
        const auto& inherited = select.Windows[1].Specification;
        UNIT_ASSERT_VALUES_EQUAL(inherited.BaseWindow->Spelling, "base");
        UNIT_ASSERT(inherited.Frame->Units == EWindowFrameUnits::Range);
        UNIT_ASSERT(inherited.Frame->Begin.Kind == EWindowBoundKind::UnboundedPreceding);
        UNIT_ASSERT(!inherited.Frame->End && !inherited.Frame->Begin.Offset);
        UNIT_ASSERT(inherited.Frame->Exclusion == EWindowExclusion::Default);
    }

    // Check bounded and unbounded endpoints independently of exclusion mode;
    // omitted endpoints must not be confused with explicit syntax.
    Y_UNIT_TEST(WindowFrameExclusionsAndBounds) {
        auto ast = Parse(R"(SELECT
            SUM(x) OVER (GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING EXCLUDE GROUP),
            SUM(x) OVER (ROWS CURRENT ROW EXCLUDE CURRENT ROW),
            SUM(x) OVER (RANGE BETWEEN 2 PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS);)");
        const auto& columns = Select(*ast).Columns;
        const auto& groups = *std::get<TWindowSpecification>(As<TCall>(columns[0].Expression).Over->Value).Frame;
        UNIT_ASSERT(groups.Units == EWindowFrameUnits::Groups);
        UNIT_ASSERT(groups.Begin.Kind == EWindowBoundKind::CurrentRow);
        UNIT_ASSERT(groups.End->Kind == EWindowBoundKind::UnboundedFollowing);
        UNIT_ASSERT(!groups.Begin.Offset && !groups.End->Offset);
        UNIT_ASSERT(groups.Exclusion == EWindowExclusion::Group);
        const auto& current = *std::get<TWindowSpecification>(As<TCall>(columns[1].Expression).Over->Value).Frame;
        UNIT_ASSERT(current.Exclusion == EWindowExclusion::CurrentRow);
        UNIT_ASSERT(!current.End);
        const auto& range = *std::get<TWindowSpecification>(As<TCall>(columns[2].Expression).Over->Value).Frame;
        UNIT_ASSERT(range.Exclusion == EWindowExclusion::NoOthers);
        UNIT_ASSERT(range.End->Kind == EWindowBoundKind::CurrentRow);
        LiteralIs(range.Begin.Offset, "2");
    }

    // SAMPLE fractions and TABLESAMPLE percentages are different modes, and
    // sampling must remain attached to the intended source occurrence.
    Y_UNIT_TEST(SamplingModesAndSourcePlacement) {
        auto ast = Parse(R"(SELECT * FROM t AS l SAMPLE $fraction
            JOIN (SELECT * FROM u) AS r TABLESAMPLE BERNOULLI(10 + 5) REPEATABLE($seed)
            ON l.id = r.id;
            SELECT * FROM t TABLESAMPLE SYSTEM(25);)");
        const auto& join = std::get<TJoin>(Select(*ast).From->Value);
        UNIT_ASSERT(join.Left->Sampling->Mode == ESamplingMode::Sample);
        UNIT_ASSERT_VALUES_EQUAL(As<TParameter>(join.Left->Sampling->Rate).Spelling, "$fraction");
        UNIT_ASSERT(!join.Left->Sampling->Seed);
        UNIT_ASSERT(std::holds_alternative<TDerivedTable>(join.Right->Value));
        UNIT_ASSERT_VALUES_EQUAL(join.Right->Alias->Spelling, "r");
        UNIT_ASSERT(join.Right->Sampling->Mode == ESamplingMode::Bernoulli);
        UNIT_ASSERT(As<TBinary>(join.Right->Sampling->Rate).Op == EBinaryOp::Add);
        UNIT_ASSERT_VALUES_EQUAL(As<TParameter>(join.Right->Sampling->Seed).Spelling, "$seed");
        const auto& system = *Select(*ast, 1).From->Sampling;
        UNIT_ASSERT(system.Mode == ESamplingMode::System);
        LiteralIs(system.Rate, "25");
        UNIT_ASSERT(!system.Seed);
    }

    // Hint repetition, quoting and scalar/list forms may matter to providers;
    // a map keyed only by hint name would discard valid source information.
    Y_UNIT_TEST(GenericHintsPreserveValuesAndOrder) {
        auto ast = Parse(R"(SELECT * FROM t WITH (infer, format = 'csv',
            flags = (foo, 'bar', $baz), format = json, one = (foo)) AS src SAMPLE 0.5;
            SELECT * FROM u WITH truncate;)");
        const auto& source = *Select(*ast).From;
        const auto& hints = source.Hints;
        UNIT_ASSERT_VALUES_EQUAL(hints.size(), 5);
        const auto& bare = std::get<TGenericTableHint>(hints[0].Value);
        UNIT_ASSERT_VALUES_EQUAL(bare.Name.Spelling, "infer");
        UNIT_ASSERT(bare.ValueForm == EHintValueForm::None && bare.Values.empty());
        const auto& scalar = std::get<TGenericTableHint>(hints[1].Value);
        UNIT_ASSERT(scalar.ValueForm == EHintValueForm::Scalar);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TLiteral>(scalar.Values[0].Value).Spelling, "'csv'");
        const auto& list = std::get<TGenericTableHint>(hints[2].Value);
        UNIT_ASSERT(list.ValueForm == EHintValueForm::List);
        UNIT_ASSERT_VALUES_EQUAL(list.Values.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TIdentifier>(list.Values[0].Value).Spelling, "foo");
        UNIT_ASSERT_VALUES_EQUAL(std::get<TParameter>(list.Values[2].Value).Spelling, "$baz");
        UNIT_ASSERT_VALUES_EQUAL(std::get<TGenericTableHint>(hints[3].Value).Name.Spelling, "format");
        UNIT_ASSERT(std::get<TGenericTableHint>(hints[4].Value).ValueForm == EHintValueForm::List);
        UNIT_ASSERT_VALUES_EQUAL(source.Alias->Spelling, "src");
        LiteralIs(source.Sampling->Rate, "0.5");
        UNIT_ASSERT_VALUES_EQUAL(Select(*ast, 1).From->Hints.size(), 1);
    }

    // Schema hints carry type/nullability structure, while watermark hints
    // carry expressions. Neither can be reduced to an opaque hint name.
    Y_UNIT_TEST(SchemaAndWatermarkHints) {
        auto ast = Parse(R"(SELECT * FROM t WITH (
            SCHEMA = Struct<id:Int64, name:String?>, COLUMNS = $columns,
            WATERMARK = ts - 1);
            SELECT * FROM t WITH SCHEMA (id Int64 NOT NULL, name String NULL, Int32 AS old, raw $type);
            SELECT * FROM t WITH SCHEMA ();)");
        const auto& hints = Select(*ast).From->Hints;
        const auto& schema = std::get<TTypeName>(std::get<TSchemaHint>(hints[0].Value).Value);
        UNIT_ASSERT_VALUES_EQUAL(schema.Name.Spelling, "Struct");
        UNIT_ASSERT_VALUES_EQUAL(schema.Members.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TIdentifier>(schema.Members[0].Name->Value).Spelling, "id");
        UNIT_ASSERT_VALUES_EQUAL(schema.Members[1].Type.OptionalDepth, 1);
        const auto& columns = std::get<TSchemaHint>(hints[1].Value);
        UNIT_ASSERT(columns.Kind == ESchemaHintKind::Columns);
        UNIT_ASSERT(std::get<TTypeName>(columns.Value).Binding);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TTypeName>(columns.Value).Name.Spelling, "$columns");
        UNIT_ASSERT(As<TBinary>(std::get<TWatermarkHint>(hints[2].Value).Expression).Op == EBinaryOp::Subtract);
        const auto& positional = std::get<TVector<TSchemaColumn>>(std::get<TSchemaHint>(Select(*ast, 1).From->Hints[0].Value).Value);
        UNIT_ASSERT_VALUES_EQUAL(positional.size(), 4);
        UNIT_ASSERT(positional[0].Nullability == EColumnNullability::NotNull);
        UNIT_ASSERT(positional[1].Nullability == EColumnNullability::Nullable);
        UNIT_ASSERT(positional[2].Nullability == EColumnNullability::Default);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TIdentifier>(positional[2].Name.Value).Spelling, "old");
        UNIT_ASSERT(positional[3].Type.Binding);
        UNIT_ASSERT(std::get<TVector<TSchemaColumn>>(std::get<TSchemaHint>(Select(*ast, 2).From->Hints[0].Value).Value).empty());
    }

    // Recursive type arguments and field/tag syntax must survive intact for
    // subsequent validation by the supported semantic path.
    Y_UNIT_TEST(NestedSchemaTypes) {
        auto ast = Parse(R"(SELECT * FROM t WITH SCHEMA Struct<
            a:Variant<Int32, String>, b:Variant<x:Int32, y:$type>,
            c:Tagged<List<Int32>, 'tag'>, d:Enum<red, $blue>, e:Resource<'handle'>>;)");
        const auto& members = std::get<TTypeName>(std::get<TSchemaHint>(Select(*ast).From->Hints[0].Value).Value).Members;
        UNIT_ASSERT_VALUES_EQUAL(members[0].Type.Members.size(), 2);
        UNIT_ASSERT(!members[0].Type.Members[0].Name);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TIdentifier>(members[1].Type.Members[0].Name->Value).Spelling, "x");
        UNIT_ASSERT(members[1].Type.Members[1].Type.Binding);
        UNIT_ASSERT_VALUES_EQUAL(members[2].Type.Arguments[0].Arguments[0].Name.Spelling, "Int32");
        UNIT_ASSERT_VALUES_EQUAL(std::get<TLiteral>(members[2].Type.Tags[0].Value).Spelling, "'tag'");
        UNIT_ASSERT_VALUES_EQUAL(std::get<TParameter>(members[3].Type.Tags[1].Value).Spelling, "$blue");
        UNIT_ASSERT_VALUES_EQUAL(std::get<TLiteral>(members[4].Type.Tags[0].Value).Spelling, "'handle'");
    }

    // Pragmas affect later statements, so keep source order and distinguish
    // flag, assignment and list forms without applying settings during parsing.
    Y_UNIT_TEST(PragmasPreserveOrderAndValueForms) {
        auto ast = Parse(R"(PRAGMA OrderedColumns;
            PRAGMA yt.Pool = 'batch'; SELECT 1;
            PRAGMA Custom(-42, +1.5, true, foo, $value, 'a\n');
            PRAGMA yt.Pool = default; SELECT 2;)");
        UNIT_ASSERT_VALUES_EQUAL(ast->Statements.size(), 6);
        const auto& bare = std::get<TPragmaStatement>(ast->Statements[0].Value);
        UNIT_ASSERT_VALUES_EQUAL(bare.Name.Spelling, "OrderedColumns");
        UNIT_ASSERT(!bare.Namespace && bare.Values.empty());
        UNIT_ASSERT(bare.ValueForm == EPragmaValueForm::None);
        const auto& pool = std::get<TPragmaStatement>(ast->Statements[1].Value);
        UNIT_ASSERT_VALUES_EQUAL(pool.Namespace->Spelling, "yt");
        UNIT_ASSERT_VALUES_EQUAL(pool.Name.Spelling, "Pool");
        UNIT_ASSERT(pool.ValueForm == EPragmaValueForm::Assignment);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TLiteral>(pool.Values[0].Value).Spelling, "'batch'");
        LiteralIs(Select(*ast, 2).Columns[0].Expression, "1");
        const auto& custom = std::get<TPragmaStatement>(ast->Statements[3].Value);
        UNIT_ASSERT(custom.ValueForm == EPragmaValueForm::List);
        UNIT_ASSERT_VALUES_EQUAL(custom.Values.size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TLiteral>(custom.Values[0].Value).Spelling, "-42");
        UNIT_ASSERT(std::get<TLiteral>(custom.Values[0].Value).Kind == ELiteralKind::Integer);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TLiteral>(custom.Values[1].Value).Spelling, "+1.5");
        UNIT_ASSERT(std::get<TLiteral>(custom.Values[1].Value).Kind == ELiteralKind::Real);
        UNIT_ASSERT(std::get<TLiteral>(custom.Values[2].Value).Kind == ELiteralKind::Boolean);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TIdentifier>(custom.Values[3].Value).Spelling, "foo");
        UNIT_ASSERT_VALUES_EQUAL(std::get<TParameter>(custom.Values[4].Value).Spelling, "$value");
        UNIT_ASSERT_VALUES_EQUAL(std::get<TLiteral>(custom.Values[5].Value).Spelling, R"('a\n')");
        UNIT_ASSERT_VALUES_EQUAL(std::get<TIdentifier>(std::get<TPragmaStatement>(ast->Statements[4].Value).Values[0].Value).Spelling, "default");
        LiteralIs(Select(*ast, 5).Columns[0].Expression, "2");
    }

    // Exercise the same AST alternatives under ANSI tokenization so the shared
    // builder cannot accidentally depend on default-dialect token behavior.
    Y_UNIT_TEST(AnsiWindowsHintsAndPragmas) {
        auto ast = Parse(R"(--!ansi_lexer
            PRAGMA "ns"."name" = 'a''b';
            SELECT SUM(x) OVER "w" FROM t WITH ("hint" = 'a''b')
            WINDOW "w" AS (ORDER BY x);
        )");
        UNIT_ASSERT(ast->AnsiLexer);
        const auto& pragma = std::get<TPragmaStatement>(ast->Statements[0].Value);
        UNIT_ASSERT_VALUES_EQUAL(pragma.Namespace->Spelling, "\"ns\"");
        UNIT_ASSERT_VALUES_EQUAL(pragma.Name.Spelling, "\"name\"");
        UNIT_ASSERT_VALUES_EQUAL(std::get<TLiteral>(pragma.Values[0].Value).Spelling, "'a''b'");
        const auto& select = Select(*ast, 1);
        UNIT_ASSERT_VALUES_EQUAL(select.Windows[0].Name.Spelling, "\"w\"");
        UNIT_ASSERT_VALUES_EQUAL(std::get<TIdentifier>(As<TCall>(select.Columns[0].Expression).Over->Value).Spelling, "\"w\"");
        const auto& hint = std::get<TGenericTableHint>(select.From->Hints[0].Value);
        UNIT_ASSERT_VALUES_EQUAL(hint.Name.Spelling, "\"hint\"");
        UNIT_ASSERT_VALUES_EQUAL(std::get<TLiteral>(hint.Values[0].Value).Spelling, "'a''b'");
    }

    // Each rejected alternative must request whole-script fallback; returning
    // a partial AST with the unsupported clause removed would change semantics.
    Y_UNIT_TEST(UnsupportedSyntaxDoesNotDisappear) {
        const TVector<TString> queries = {
            "SELECT 1; DELETE FROM t;",
            "SELECT * WITHOUT id FROM t;",
            "SELECT x = y = z;",
            "SELECT * FROM t OUTER JOIN u;",
            "SELECT * FROM t WITH SCHEMA Callable<() -> Int32>;",
            "PRAGMA OrderedColumns; DELETE FROM t;",
            "SELECT * FROM ANY t;",
        };
        for (const auto& query : queries) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(ParseQueryWithNewParser(query, {}, false),
                TNewYqlParserFallback, "New AST does not support");
        }
    }

    // An empty script is a valid owning AST with no statements, not a parse
    // error or an unsupported-feature fallback.
    Y_UNIT_TEST(EmptyScript) {
        UNIT_ASSERT(Parse("-- comment\n")->Statements.empty());
    }

    // Both query-assignment spellings and both lambda-body spellings must own
    // their syntax after the temporary ANTLR parser has been destroyed.
    Y_UNIT_TEST(NamedPreambleAndLambdaBodies) {
        auto ast = Parse("$f = ($x, $y) -> { $n = $x + 1; return $n + $y; }; "
            "$g = () -> (42); $q = SELECT $f(id, 2) AS k FROM t; "
            "$r = (WITH c AS (SELECT k FROM $q) SELECT * FROM c); SELECT * FROM $r;");
        UNIT_ASSERT_VALUES_EQUAL(ast->Statements.size(), 5);
        const auto& f = std::get<TNamedBinding>(ast->Statements[0].Value);
        UNIT_ASSERT_VALUES_EQUAL(f.Name.Spelling, "$f");
        const auto& lambda = std::get<TLambda>(f.Value->Value);
        UNIT_ASSERT_VALUES_EQUAL(lambda.Parameters.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(lambda.Parameters[1].Spelling, "$y");
        UNIT_ASSERT_VALUES_EQUAL(lambda.Bindings[0].Name.Spelling, "$n");
        UNIT_ASSERT(std::holds_alternative<TBinary>(lambda.Body->Value));
        const auto& g = std::get<TNamedBinding>(ast->Statements[1].Value);
        UNIT_ASSERT(std::get<TLambda>(g.Value->Value).Parameters.empty());
        const auto& q = std::get<TNamedBinding>(ast->Statements[2].Value);
        UNIT_ASSERT(std::holds_alternative<TSubquery>(q.Value->Value));
        const auto& r = std::get<TNamedBinding>(ast->Statements[3].Value);
        UNIT_ASSERT_VALUES_EQUAL(std::get<TSubquery>(r.Value->Value).Query->With.size(), 1);
    }

}

} // namespace NKikimr::NKqp
