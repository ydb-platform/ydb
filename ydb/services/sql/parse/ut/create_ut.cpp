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
        UNIT_ASSERT(drop.AlterTable);
        UNIT_ASSERT(drop.AlterTable->Kind == EAlterTableKind::DropColumn);
        UNIT_ASSERT_VALUES_EQUAL(drop.AlterTable->Column.Name.Name, "note");
        UNIT_ASSERT(!drop.AlterTable->IfExists);

        const TParseResult again = Parse("ALTER TABLE app.t DROP COLUMN IF EXISTS note");
        UNIT_ASSERT(again.AlterTable);
        UNIT_ASSERT(again.AlterTable->IfExists);
        UNIT_ASSERT(again.AlterTable->Schema);
        UNIT_ASSERT_VALUES_EQUAL(again.AlterTable->Schema->Name, "app");
    }

    Y_UNIT_TEST(AlterColumnNullAndDefault) {
        const TParseResult setNull = Parse("ALTER TABLE t ALTER COLUMN note SET NOT NULL");
        UNIT_ASSERT(setNull.AlterTable);
        UNIT_ASSERT(setNull.AlterTable->Kind == EAlterTableKind::SetNotNull);

        const TParseResult setDefault = Parse("ALTER TABLE t ALTER note SET DEFAULT 'hi'");
        UNIT_ASSERT(setDefault.AlterTable);
        UNIT_ASSERT(setDefault.AlterTable->Kind == EAlterTableKind::SetDefault);
        UNIT_ASSERT_VALUES_EQUAL(setDefault.AlterTable->Column.Default->Text, "hi");

        const TParseResult dropDefault = Parse("ALTER TABLE t ALTER COLUMN note DROP DEFAULT");
        UNIT_ASSERT(dropDefault.AlterTable);
        UNIT_ASSERT(dropDefault.AlterTable->Kind == EAlterTableKind::DropDefault);

        const TParseResult typed = Parse("ALTER TABLE t ALTER COLUMN note TYPE int4");
        UNIT_ASSERT(!typed.AlterTable);
        UNIT_ASSERT_VALUES_EQUAL(typed.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(typed.Message, "type");
    }

    Y_UNIT_TEST(CreateSequenceOptions) {
        const TParseResult parsed = Parse(
            "CREATE SEQUENCE app.bounds AS integer INCREMENT BY 2 MINVALUE 1 MAXVALUE 20 START WITH 5 CACHE 3 CYCLE");
        UNIT_ASSERT(parsed.CreateSequence);
        UNIT_ASSERT_VALUES_EQUAL(parsed.CreateSequence->DataType, "Int32");
        UNIT_ASSERT_VALUES_EQUAL(*parsed.CreateSequence->Increment, 2);
        UNIT_ASSERT_VALUES_EQUAL(*parsed.CreateSequence->MaxValue, 20);
        UNIT_ASSERT(parsed.CreateSequence->Cycle);

        const TParseResult temporary = Parse("CREATE SEQUENCE seq TEMPORARY");
        UNIT_ASSERT(!temporary.CreateSequence);
        UNIT_ASSERT_VALUES_EQUAL(temporary.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(temporary.Message, "temporary");

        const TParseResult owned = Parse("CREATE SEQUENCE seq OWNED BY t.id");
        UNIT_ASSERT(!owned.CreateSequence);
        UNIT_ASSERT_VALUES_EQUAL(owned.Message, "owned");
    }

    Y_UNIT_TEST(AlterSequenceRestart) {
        const TParseResult parsed = Parse("ALTER SEQUENCE bounds RESTART WITH 9");
        UNIT_ASSERT(parsed.AlterSequence);
        UNIT_ASSERT(parsed.AlterSequence->Restart);
        UNIT_ASSERT_VALUES_EQUAL(*parsed.AlterSequence->RestartWith, 9);

        const TParseResult owned = Parse("ALTER SEQUENCE bounds OWNED BY t.id");
        UNIT_ASSERT(!owned.AlterSequence);
        UNIT_ASSERT_VALUES_EQUAL(owned.SqlState, "0A000");
        UNIT_ASSERT_VALUES_EQUAL(owned.Message, "owned");
    }

    Y_UNIT_TEST(DropTable) {
        const TParseResult parsed = Parse("DROP TABLE IF EXISTS app.doomed");
        UNIT_ASSERT(parsed.Drop);
        UNIT_ASSERT(parsed.Drop->IfExists);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Drop->Name.Name, "doomed");

        const TParseResult cascade = Parse("DROP TABLE doomed CASCADE");
        UNIT_ASSERT(!cascade.Drop);
        UNIT_ASSERT_VALUES_EQUAL(cascade.SqlState, "0A000");
    }

    Y_UNIT_TEST(DropSequence) {
        const TParseResult parsed = Parse("DROP SEQUENCE IF EXISTS app.bounds");
        UNIT_ASSERT(parsed.Drop);
        UNIT_ASSERT(parsed.Drop->Kind == EDropKind::Sequence);
        UNIT_ASSERT(parsed.Drop->IfExists);
        UNIT_ASSERT_VALUES_EQUAL(parsed.Drop->Schema->Name, "app");
    }
}
