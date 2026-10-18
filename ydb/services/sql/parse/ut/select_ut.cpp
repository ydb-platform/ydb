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

    Y_UNIT_TEST(UnknownCommand) {
        const TParseResult parsed = Parse("INSERT INTO t");
        UNIT_ASSERT(!parsed.Select);
        UNIT_ASSERT_VALUES_EQUAL(parsed.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Message, "insert");
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

    Y_UNIT_TEST(OrIsUnsupported) {
        const TParseResult parsed = Parse("SELECT a FROM t WHERE a = 1 OR b = 2");
        UNIT_ASSERT(!parsed.Select);
        UNIT_ASSERT_VALUES_EQUAL(parsed.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Message, "or");
    }
}
