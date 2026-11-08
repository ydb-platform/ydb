#include <ydb/core/kqp/host/new_parser/kqp_new_parser.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

using namespace NNewYql;

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

    Y_UNIT_TEST(AnyUsingAndQualifiedStar) {
        auto ast = Parse("SELECT l.*, r.v FROM ANY t AS l INNER JOIN ANY u AS r USING(id);");
        const auto& select = Select(*ast);
        UNIT_ASSERT_VALUES_EQUAL(As<TStar>(select.Columns[0].Expression).Qualifier->Spelling, "l");
        const auto& join = std::get<TJoin>(select.From->Value);
        UNIT_ASSERT(join.LeftAny && join.RightAny);
        UNIT_ASSERT_VALUES_EQUAL(join.Using[0].Spelling, "id");
        UNIT_ASSERT(!join.On);
    }

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

    Y_UNIT_TEST(DerivedTableAndCommaLimit) {
        auto ast = Parse("SELECT d.id FROM (SELECT id FROM t LIMIT 2, 5) AS d;");
        const auto& from = *Select(*ast).From;
        UNIT_ASSERT_VALUES_EQUAL(from.Alias->Spelling, "d");
        const auto& inner = *std::get<TDerivedTable>(from.Value).Query;
        LiteralIs(inner.Limit, "5");
        LiteralIs(inner.Offset, "2");
    }

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

    Y_UNIT_TEST(GroupingSets) {
        auto ast = Parse("SELECT a, COUNT(*) FROM t GROUP BY GROUPING SETS ((), ROLLUP(a, b), CUBE(a, b));");
        const auto& sets = Select(*ast).GroupBy[0];
        UNIT_ASSERT(sets.Kind == EGroupingKind::Sets);
        UNIT_ASSERT_VALUES_EQUAL(sets.Elements.size(), 3);
        UNIT_ASSERT(As<TTuple>(sets.Elements[0].Expression.Expression).Elements.empty());
        UNIT_ASSERT(sets.Elements[1].Kind == EGroupingKind::Rollup);
        UNIT_ASSERT(sets.Elements[2].Kind == EGroupingKind::Cube);
    }

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

    Y_UNIT_TEST(AnsiSpellingsAndMultipleStatements) {
        auto ast = Parse("--!ansi_lexer\nEXPLAIN QUERY PLAN SELECT 'a''b' AS \"a\"\"b\"; SELECT 42ul;");
        UNIT_ASSERT(ast->AnsiLexer);
        UNIT_ASSERT_VALUES_EQUAL(ast->Statements.size(), 2);
        UNIT_ASSERT(ast->Statements[0].Explain == EExplainMode::QueryPlan);
        LiteralIs(Select(*ast).Columns[0].Expression, "'a''b'");
        UNIT_ASSERT_VALUES_EQUAL(Select(*ast).Columns[0].Alias->Spelling, "\"a\"\"b\"");
        LiteralIs(Select(*ast, 1).Columns[0].Expression, "42ul");
    }

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

    Y_UNIT_TEST(UnsupportedSyntaxDoesNotDisappear) {
        const TVector<TString> queries = {
            "SELECT 1; DELETE FROM t;",
            "SELECT * WITHOUT id FROM t;",
            "SELECT ($x) -> ($x + 1);",
            "SELECT x = y = z;",
            "SELECT * FROM t OUTER JOIN u;",
            "SELECT SUM(x) OVER (PARTITION BY ($x) -> ($x + 1)) FROM t;",
            "SELECT * FROM t SAMPLE ($x) -> ($x + 1);",
            "SELECT * FROM t WITH SCHEMA Callable<() -> Int32>;",
            "PRAGMA OrderedColumns; DELETE FROM t;",
            "SELECT * FROM ANY t;",
        };
        for (const auto& query : queries) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(ParseQueryWithNewParser(query, {}, false),
                TNewYqlParserFallback, "New AST does not support");
        }
    }

    Y_UNIT_TEST(EmptyScript) {
        UNIT_ASSERT(Parse("-- comment\n")->Statements.empty());
    }
}

} // namespace NKikimr::NKqp
