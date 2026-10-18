#include <library/cpp/testing/unittest/registar.h>

#include <ydb/services/sql/parse/parser.h>

using namespace NKikimr::NSql;

Y_UNIT_TEST_SUITE(ParseCreateTable) {
    Y_UNIT_TEST(ColumnsTypesAndPrimaryKey) {
        const TParseResult parsed = Parse(
            "CREATE TABLE app.t (id int8 PRIMARY KEY, ts timestamp, tiny \"Int8\", note text DEFAULT 'hi')");
        UNIT_ASSERT(parsed.CreateTable);
        UNIT_ASSERT(parsed.CreateTable->Schema);
        UNIT_ASSERT_VALUES_EQUAL(parsed.CreateTable->Schema->Name, "app");
        UNIT_ASSERT_VALUES_EQUAL(parsed.CreateTable->Name.Name, "t");
        UNIT_ASSERT_VALUES_EQUAL(parsed.CreateTable->Columns.size(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(parsed.CreateTable->Columns[0].Type.Name, "int8");
        UNIT_ASSERT(parsed.CreateTable->Columns[0].NotNull);
        UNIT_ASSERT_VALUES_EQUAL(parsed.CreateTable->Columns[1].Type.Name, "timestamp");
        UNIT_ASSERT(parsed.CreateTable->Columns[2].Type.Quoted);
        UNIT_ASSERT_VALUES_EQUAL(parsed.CreateTable->Columns[2].Type.Name, "Int8");
        UNIT_ASSERT(parsed.CreateTable->Columns[3].Default);
        UNIT_ASSERT_VALUES_EQUAL(parsed.CreateTable->Columns[3].Default->Text, "hi");
        UNIT_ASSERT_VALUES_EQUAL(parsed.CreateTable->PrimaryKey.size(), 1u);
    }

    Y_UNIT_TEST(PartitionAndCheckAreRejected) {
        const TParseResult partition = Parse("CREATE TABLE t (id int4 PRIMARY KEY) PARTITION BY RANGE (id)");
        UNIT_ASSERT(!partition.CreateTable);
        UNIT_ASSERT_VALUES_EQUAL(partition.SqlState, "0A000");

        const TParseResult check = Parse("CREATE TABLE t (id int4 PRIMARY KEY, CHECK (id > 0))");
        UNIT_ASSERT(!check.CreateTable);
        UNIT_ASSERT_VALUES_EQUAL(check.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(check.Message, "check");
    }

    Y_UNIT_TEST(UniqueColumnList) {
        const TParseResult parsed = Parse("CREATE TABLE t (id int4 PRIMARY KEY, email text, UNIQUE (email))");
        UNIT_ASSERT(parsed.CreateTable);
        UNIT_ASSERT_VALUES_EQUAL(parsed.CreateTable->Unique.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(parsed.CreateTable->Unique[0].size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(parsed.CreateTable->Unique[0][0].Name, "email");
    }

    Y_UNIT_TEST(AddColumn) {
        const TParseResult parsed = Parse("ALTER TABLE t ADD COLUMN note text");
        UNIT_ASSERT(parsed.AlterTable);
        UNIT_ASSERT_VALUES_EQUAL(parsed.AlterTable->Name.Name, "t");
        UNIT_ASSERT_VALUES_EQUAL(parsed.AlterTable->Column.Name.Name, "note");
        UNIT_ASSERT_VALUES_EQUAL(parsed.AlterTable->Column.Type.Name, "text");

        const TParseResult drop = Parse("ALTER TABLE t DROP COLUMN note");
        UNIT_ASSERT(!drop.AlterTable);
        UNIT_ASSERT_VALUES_EQUAL(drop.SqlState, "0A000");
    }
}
