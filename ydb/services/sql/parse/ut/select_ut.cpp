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

    Y_UNIT_TEST(OrIsUnsupported) {
        const TParseResult parsed = Parse("SELECT a FROM t WHERE a = 1 OR b = 2");
        UNIT_ASSERT(!parsed.Select);
        UNIT_ASSERT_VALUES_EQUAL(parsed.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Message, "or");
    }

    Y_UNIT_TEST(DistinctJoinAndOtherStatementsAreUnsupported) {
        const TParseResult distinct = Parse("SELECT DISTINCT a FROM t");
        UNIT_ASSERT_VALUES_EQUAL(distinct.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(distinct.Message, "distinct");

        const TParseResult join = Parse("SELECT a FROM t FULL JOIN u ON t.id = u.id");
        UNIT_ASSERT_VALUES_EQUAL(join.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(join.Message, "full");

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
