#include <library/cpp/testing/unittest/registar.h>

#include <ydb/services/sql/parse/parser.h>

using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(ParseSelect) {
    Y_UNIT_TEST(ColumnList) {
        const TParseResult parsed = Parse("SELECT a, b FROM t");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT(!parsed.Select->Star);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Columns.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Columns[0].Name, "a");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Columns[1].Name, "b");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->From.Name, "t");
    }

    Y_UNIT_TEST(FoldsUnquotedIdentifiers) {
        const TParseResult parsed = Parse("SELECT A, B FROM T");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Columns[0].Name, "a");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Columns[1].Name, "b");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->From.Name, "t");
        UNIT_ASSERT(!parsed.Select->Columns[0].Quoted);
    }

    Y_UNIT_TEST(KeepsQuotedIdentifiers) {
        const TParseResult parsed = Parse("SELECT \"A\", \"B\" FROM \"T\"");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Columns[0].Name, "A");
        UNIT_ASSERT(parsed.Select->Columns[0].Quoted);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Columns[1].Name, "B");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->From.Name, "T");
        UNIT_ASSERT(parsed.Select->From.Quoted);
    }

    Y_UNIT_TEST(Star) {
        const TParseResult parsed = Parse("SELECT * FROM t");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT(parsed.Select->Star);
        UNIT_ASSERT(parsed.Select->Columns.empty());
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->From.Name, "t");
    }

    Y_UNIT_TEST(InsertReturning) {
        const TParseResult parsed = Parse("INSERT INTO items (id, note) VALUES (7, 'hello') RETURNING note AS label, extra");
        UNIT_ASSERT_C(parsed.Insert, parsed.SqlState + " " + parsed.Message);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Insert->Returning.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Insert->Returning[0].Column.Name, "note");
        UNIT_ASSERT(parsed.Insert->Returning[0].Alias);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Insert->Returning[0].Alias->Name, "label");
    }

    Y_UNIT_TEST(UnknownCommand) {
        const TParseResult parsed = Parse("GRANT SELECT ON t TO u");
        UNIT_ASSERT(!parsed.Select);
        UNIT_ASSERT_VALUES_EQUAL(parsed.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Message, "grant");
    }

    Y_UNIT_TEST(WhereAndLimit) {
        const TParseResult parsed = Parse("SELECT a, b FROM t WHERE a = 1 AND b = 'x' LIMIT 10");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Where.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Where[0].Column.Name, "a");
        UNIT_ASSERT(parsed.Select->Where[0].Op == ECompareOp::Eq);
        UNIT_ASSERT(parsed.Select->Where[0].Value.Kind == TLiteral::EKind::Integer);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Where[0].Value.Text, "1");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Where[1].Column.Name, "b");
        UNIT_ASSERT(parsed.Select->Where[1].Value.Kind == TLiteral::EKind::String);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Where[1].Value.Text, "x");
        UNIT_ASSERT(parsed.Select->Limit);
        UNIT_ASSERT_VALUES_EQUAL(*parsed.Select->Limit, 10u);
    }

    Y_UNIT_TEST(QualifiedTableName) {
        const TParseResult parsed = Parse("SELECT a FROM app.t");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT(parsed.Select->Schema);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Schema->Name, "app");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->From.Name, "t");
    }

    Y_UNIT_TEST(InnerJoinKeepsBothTables) {
        const TParseResult parsed = Parse("SELECT lefts.note, rights.extra FROM lefts INNER JOIN rights ON lefts.id = rights.id");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT(parsed.Select->Join);
        UNIT_ASSERT(parsed.Select->Join->Kind == EJoinKind::Inner);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->From.Name, "lefts");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Join->Name.Name, "rights");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Columns[0].Qualifier, "lefts");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Columns[0].Name, "note");
    }

    Y_UNIT_TEST(LeftJoinKeepsTheLeftTable) {
        const TParseResult parsed = Parse("SELECT lefts.note FROM lefts LEFT JOIN rights ON lefts.id = rights.id");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT(parsed.Select->Join);
        UNIT_ASSERT(parsed.Select->Join->Kind == EJoinKind::Left);
    }

    Y_UNIT_TEST(RightJoinKeepsTheRightTable) {
        const TParseResult parsed = Parse("SELECT rights.extra FROM lefts RIGHT JOIN rights ON lefts.id = rights.id");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT(parsed.Select->Join);
        UNIT_ASSERT(parsed.Select->Join->Kind == EJoinKind::Right);
    }

    Y_UNIT_TEST(CrossJoinAndCommaFromHaveNoPredicate) {
        const TParseResult cross = Parse("SELECT lefts.note, rights.extra FROM lefts CROSS JOIN rights");
        UNIT_ASSERT(cross.Select);
        UNIT_ASSERT(cross.Select->Join);
        UNIT_ASSERT(cross.Select->Join->Kind == EJoinKind::Cross);
        UNIT_ASSERT_VALUES_EQUAL(cross.Select->Join->Name.Name, "rights");

        const TParseResult comma = Parse("SELECT lefts.note FROM lefts, rights");
        UNIT_ASSERT(comma.Select);
        UNIT_ASSERT(comma.Select->Join);
        UNIT_ASSERT(comma.Select->Join->Kind == EJoinKind::Cross);
        UNIT_ASSERT_VALUES_EQUAL(comma.Select->From.Name, "lefts");
        UNIT_ASSERT_VALUES_EQUAL(comma.Select->Join->Name.Name, "rights");
    }

    Y_UNIT_TEST(FullJoinKeepsBothTables) {
        const TParseResult parsed = Parse("SELECT lefts.note, rights.extra FROM lefts FULL JOIN rights ON lefts.id = rights.id");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT(parsed.Select->Join);
        UNIT_ASSERT(parsed.Select->Join->Kind == EJoinKind::Full);
    }

    Y_UNIT_TEST(OrderByKeepsDirectionAndNulls) {
        const TParseResult parsed = Parse("SELECT note FROM items ORDER BY id DESC NULLS LAST, note ASC");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Order.size(), 2u);
        UNIT_ASSERT(parsed.Select->Order[0].Desc);
        UNIT_ASSERT(!parsed.Select->Order[0].NullsFirst);
        UNIT_ASSERT(!parsed.Select->Order[1].Desc);
        UNIT_ASSERT(!parsed.Select->Order[1].NullsFirst);
    }

    Y_UNIT_TEST(GroupByKeepsCountAndSum) {
        const TParseResult parsed = Parse("SELECT kind, count(*), sum(amount) FROM sales GROUP BY kind");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Group.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Aggs.size(), 2u);
        UNIT_ASSERT(parsed.Select->Aggs[0].Func == EAggFunc::Count);
        UNIT_ASSERT(parsed.Select->Aggs[0].Star);
        UNIT_ASSERT(parsed.Select->Aggs[1].Func == EAggFunc::Sum);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Aggs[1].Argument.Name, "amount");

        const TParseResult avg = Parse("SELECT avg(amount) FROM sales");
        UNIT_ASSERT(!avg.Select);
        UNIT_ASSERT_VALUES_EQUAL(avg.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(avg.Message, "avg");
    }

    Y_UNIT_TEST(HavingKeepsTheComparison) {
        const TParseResult parsed = Parse("SELECT kind, sum(amount) FROM sales GROUP BY kind HAVING sum > 10");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Having.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Having[0].Column.Name, "sum");
        UNIT_ASSERT(parsed.Select->Having[0].Op == ECompareOp::Gt);
    }

    Y_UNIT_TEST(DistinctOnKeepsTheColumns) {
        const TParseResult parsed = Parse("SELECT DISTINCT ON (note) id FROM items");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT(parsed.Select->Distinct);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->DistinctOn.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->DistinctOn[0].Name, "note");
    }

    Y_UNIT_TEST(UnionAllIsSeparateFromUnion) {
        const TParseResult all = Parse("SELECT a FROM t UNION ALL SELECT a FROM u");
        UNIT_ASSERT(all.Select);
        UNIT_ASSERT(all.SelectRight);
        UNIT_ASSERT(all.Combine == ECombine::UnionAll);
        UNIT_ASSERT_VALUES_EQUAL(all.Select->From.Name, "t");
        UNIT_ASSERT_VALUES_EQUAL(all.SelectRight->From.Name, "u");

        const TParseResult once = Parse("SELECT a FROM t UNION SELECT a FROM u");
        UNIT_ASSERT(once.Combine == ECombine::Union);
    }

    Y_UNIT_TEST(IntersectKeepsTheOperator) {
        const TParseResult parsed = Parse("SELECT a FROM t INTERSECT SELECT a FROM u");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT(parsed.Combine == ECombine::Intersect);
        UNIT_ASSERT_VALUES_EQUAL(parsed.SelectRight->From.Name, "u");
    }

    Y_UNIT_TEST(ExceptKeepsTheOperator) {
        const TParseResult parsed = Parse("SELECT a FROM t EXCEPT SELECT a FROM u");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT(parsed.Combine == ECombine::Except);
    }

    Y_UNIT_TEST(SubqueryInFromAndSelectList) {
        const TParseResult scalar = Parse("SELECT id, (SELECT note FROM rights WHERE id = 1) FROM lefts WHERE id = 1");
        UNIT_ASSERT(scalar.Select);
        UNIT_ASSERT_VALUES_EQUAL(scalar.Scalars.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(scalar.Scalars[0].From.Name, "rights");
        UNIT_ASSERT_VALUES_EQUAL(scalar.Select->ScalarOf.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(scalar.Select->ScalarOf[1], 0);

        const TParseResult derived = Parse("SELECT note FROM (SELECT note FROM lefts WHERE id = 1) s");
        UNIT_ASSERT(derived.Select);
        UNIT_ASSERT(derived.FromSubquery);
        UNIT_ASSERT(derived.Select->FromQuery);
        UNIT_ASSERT_VALUES_EQUAL(derived.Select->From.Name, "s");
        UNIT_ASSERT_VALUES_EQUAL(derived.FromSubquery->From.Name, "lefts");
    }

    Y_UNIT_TEST(WithNamesTheQuery) {
        const TParseResult parsed = Parse("WITH s AS (SELECT note FROM items WHERE id = 1) SELECT note FROM s");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT(parsed.Cte);
        UNIT_ASSERT_VALUES_EQUAL(parsed.CteName, "s");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Cte->From.Name, "items");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->From.Name, "s");
    }

    Y_UNIT_TEST(OrIsUnsupported) {
        const TParseResult parsed = Parse("SELECT a FROM t WHERE a = 1 OR b = 2");
        UNIT_ASSERT(!parsed.Select);
        UNIT_ASSERT_VALUES_EQUAL(parsed.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Message, "or");
    }

    Y_UNIT_TEST(DistinctJoinAndOtherStatementsAreUnsupported) {
        const TParseResult window = Parse("SELECT a FROM t WINDOW w AS ()");
        UNIT_ASSERT_VALUES_EQUAL(window.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(window.Message, "window");

        const TParseResult join = Parse("SELECT a FROM t NATURAL JOIN u");
        UNIT_ASSERT_VALUES_EQUAL(join.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(join.Message, "natural");

        const TParseResult grant = Parse("GRANT SELECT ON t TO u");
        UNIT_ASSERT_VALUES_EQUAL(grant.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(grant.Message, "grant");

        const TParseResult all = Parse("SELECT ALL a FROM t");
        UNIT_ASSERT(all.Select);
        UNIT_ASSERT_VALUES_EQUAL(all.Select->Columns[0].Name, "a");
    }

    Y_UNIT_TEST(CommentIsSkipped) {
        const TParseResult parsed = Parse("SELECT a /* cols */ FROM t -- tail\n");
        UNIT_ASSERT(parsed.Select);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->Columns[0].Name, "a");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Select->From.Name, "t");
    }
}
