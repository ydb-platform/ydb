#include "json_row_parser.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/public/udf/arrow/block_reader.h>

namespace NYql::NJson {

Y_UNIT_TEST_SUITE(TJsonRowParse) {

    Y_UNIT_TEST(SyntaxTopLevel) {
        arrow::SchemaBuilder builder;
        TJsonRowParser parser(builder.Finish().ValueOrDie());

        // not well formed
        parser.ParseNextRow("Blah blah blah"sv);
        UNIT_ASSERT(parser.ErrorFound);
        UNIT_ASSERT_VALUES_EQUAL(parser.ErrorMessage, "invalid syntax at token: 'blah'");

        // field at top level is not allowed
        parser.ParseNextRow("some_field: 0"sv);
        UNIT_ASSERT(parser.ErrorFound);
        UNIT_ASSERT_VALUES_EQUAL(parser.ErrorMessage, "invalid syntax at token: ':', top level json object is expected, starting with '{'");

        // array at top level is not allowed
        parser.ParseNextRow("[0, 1, 2]"sv);
        UNIT_ASSERT(parser.ErrorFound);
        UNIT_ASSERT_VALUES_EQUAL(parser.ErrorMessage, "invalid syntax at token: '[', top level json object is expected, starting with '{'");

        // value at top level is not allowed
        parser.ParseNextRow("0"sv);
        UNIT_ASSERT(parser.ErrorFound);
        UNIT_ASSERT_VALUES_EQUAL(parser.ErrorMessage, "invalid syntax at token: '0', top level json object is expected, starting with '{'");

        // empty string is not allowed
        parser.ParseNextRow("   "sv);
        UNIT_ASSERT(parser.ErrorFound);
        UNIT_ASSERT_VALUES_EQUAL(parser.ErrorMessage, "invalid or truncated");

        // minimal correct and well formed json object
        parser.ParseNextRow("{}"sv);
        UNIT_ASSERT(!parser.ErrorFound);

        // allow comma after object
        parser.ParseNextRow("{},"sv);
        UNIT_ASSERT(!parser.ErrorFound);

        // allow (ignore) any trash after object
        parser.ParseNextRow("{}; -- Blah blah blah"sv);
        UNIT_ASSERT(!parser.ErrorFound);

        // allow unclosed object map
        parser.ParseNextRow("{"sv);
        UNIT_ASSERT(!parser.ErrorFound);
    }

    Y_UNIT_TEST(SyntaxStructure) {
        arrow::SchemaBuilder builder;
        TJsonRowParser parser(builder.Finish().ValueOrDie());

        parser.ParseNextRow("{]}"sv);
        UNIT_ASSERT(parser.ErrorFound);
        UNIT_ASSERT_VALUES_EQUAL(parser.ErrorMessage, "invalid syntax at token: ']', expected '}'");

        parser.ParseNextRow("{[}]"sv);
        UNIT_ASSERT(parser.ErrorFound);
        UNIT_ASSERT_VALUES_EQUAL(parser.ErrorMessage, "invalid syntax at token: '}', expected ']'");

        parser.ParseNextRow("{ blah blah }"sv);
        UNIT_ASSERT(parser.ErrorFound);
        UNIT_ASSERT_VALUES_EQUAL(parser.ErrorMessage, "invalid syntax at token: 'blah'");
    }

    Y_UNIT_TEST(Nullables) {
        arrow::SchemaBuilder builder;
        THROW_ARROW_NOT_OK(builder.AddField(std::make_shared<arrow::Field>(std::string("field1"), arrow::uint32(), false)));
        THROW_ARROW_NOT_OK(builder.AddField(std::make_shared<arrow::Field>(std::string("field2"), arrow::uint32(), true)));
        TJsonRowParser parser(builder.Finish().ValueOrDie());

        parser.ParseNextRow("{ field1: 1 }"sv);
        UNIT_ASSERT(!parser.ErrorFound);

        parser.ParseNextRow("{ field2: 2 }"sv);
        UNIT_ASSERT(parser.ErrorFound);
        UNIT_ASSERT_VALUES_EQUAL(parser.ErrorMessage, "missed mandatory field \"field1\"");
    }

    Y_UNIT_TEST(MixedOrder) {
        arrow::SchemaBuilder builder;
        THROW_ARROW_NOT_OK(builder.AddField(std::make_shared<arrow::Field>(std::string("field1"), arrow::uint32(), false)));
        THROW_ARROW_NOT_OK(builder.AddField(std::make_shared<arrow::Field>(std::string("field2"), arrow::uint32(), false)));
        TJsonRowParser parser(builder.Finish().ValueOrDie());

        parser.ParseNextRow("{ field1: 1, field2: 2 }"sv);
        UNIT_ASSERT(!parser.ErrorFound);
        parser.ParseNextRow("{ field2: 3, field1: 4 }"sv);
        UNIT_ASSERT(!parser.ErrorFound);

        {
            auto batch = parser.TakeBatch();
            ::NYql::NUdf::TFixedSizeBlockReader<i32, true> reader;
            auto column1 = batch->column(0);
            auto column2 = batch->column(1);
            UNIT_ASSERT_VALUES_EQUAL(reader.GetItem(*column1->data(), 0).As<i32>(), 1);
            UNIT_ASSERT_VALUES_EQUAL(reader.GetItem(*column1->data(), 1).As<i32>(), 4);
            UNIT_ASSERT_VALUES_EQUAL(reader.GetItem(*column2->data(), 0).As<i32>(), 2);
            UNIT_ASSERT_VALUES_EQUAL(reader.GetItem(*column2->data(), 1).As<i32>(), 3);
        }

        parser.ParseNextRow("{ field1: 5, field2: 6 }"sv);
        UNIT_ASSERT(!parser.ErrorFound);
        parser.ParseNextRow("{ field2: 7, field1: 8 }"sv);
        UNIT_ASSERT(!parser.ErrorFound);

        {
            auto batch = parser.TakeBatch();
            ::NYql::NUdf::TFixedSizeBlockReader<i32, true> reader;
            auto column1 = batch->column(0);
            auto column2 = batch->column(1);
            UNIT_ASSERT_VALUES_EQUAL(reader.GetItem(*column1->data(), 0).As<i32>(), 5);
            UNIT_ASSERT_VALUES_EQUAL(reader.GetItem(*column1->data(), 1).As<i32>(), 8);
            UNIT_ASSERT_VALUES_EQUAL(reader.GetItem(*column2->data(), 0).As<i32>(), 6);
            UNIT_ASSERT_VALUES_EQUAL(reader.GetItem(*column2->data(), 1).As<i32>(), 7);
        }
    }

    Y_UNIT_TEST(SingleRow) {
        arrow::SchemaBuilder builder;
        THROW_ARROW_NOT_OK(builder.AddField(std::make_shared<arrow::Field>(std::string("field1"), arrow::uint32(), true)));
        THROW_ARROW_NOT_OK(builder.AddField(std::make_shared<arrow::Field>(std::string("field2"), arrow::float64(), true)));
        THROW_ARROW_NOT_OK(builder.AddField(std::make_shared<arrow::Field>(std::string("field3"), arrow::utf8(), true)));
        TJsonRowParser parser(builder.Finish().ValueOrDie());

        TString json = "{ field1: 1, field2: 2.5, field3: \"text\", field4: [1, \"a\", -5], field5: { sub1: 1, sub2: 2 }, field6: \"\\\"QQ\\\"\" },";
        parser.ParseNextRow(json);
    }
}

} // namespace NYql::NJson
