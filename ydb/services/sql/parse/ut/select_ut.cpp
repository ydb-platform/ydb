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

    Y_UNIT_TEST(UnknownNodeKeepsText) {
        const TParseResult parsed = Parse("SELECT a FROM t WHERE a = 1");
        UNIT_ASSERT(!parsed.Select);
        UNIT_ASSERT_VALUES_EQUAL(parsed.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(parsed.Message, "where");
    }
}
