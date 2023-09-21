#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <google/protobuf/text_format.h>

using namespace NYdb;

Y_UNIT_TEST_SUITE(CppGrpcClientResultSetTest) {
    Y_UNIT_TEST(ListResultSet) {
        const TString resultSetString =
            "columns {\n"
            "  name: \"colName\"\n"
            "  type {\n"
            "    list_type {\n"
            "      item {\n"
            "        type_id: INT32\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n"
            "rows {\n"
            "  items {\n"
            "    items {\n"
            "      int32_value: 42\n"
            "    }\n"
            "    items {\n"
            "      int32_value: 43\n"
            "    }\n"
            "    items {\n"
            "      int32_value: 44\n"
            "    }\n"
            "  }\n"
            "}\n"
            "rows {\n"
            "  items {\n"
            "    items {\n"
            "      int32_value: 45\n"
            "    }\n"
            "    items {\n"
            "      int32_value: 46\n"
            "    }\n"
            "    items {\n"
            "      int32_value: 47\n"
            "    }\n"
            "  }\n"
            "}\n";
        Ydb::ResultSet rsProto;
        google::protobuf::TextFormat::ParseFromString(resultSetString, &rsProto);

        NYdb::TResultSet rs(std::move(rsProto));
        NYdb::TResultSetParser rsParser(rs);
        UNIT_ASSERT_EQUAL(rsParser.ColumnsCount(), 1);
        UNIT_ASSERT_EQUAL(rsParser.RowsCount(), 2);
        UNIT_ASSERT_EQUAL(rsParser.ColumnIndex("colName"), 0);
        UNIT_ASSERT_EQUAL(rsParser.ColumnIndex("otherName"), -1);
        int expectedVal = 42;
        auto& column0 = rsParser.ColumnParser(0);
        while (rsParser.TryNextRow()) {
            column0.OpenList();
            while (column0.TryNextListItem()) {
                UNIT_ASSERT_EQUAL(column0.GetInt32(), expectedVal++);
            }
        }
    }

    Y_UNIT_TEST(OptionalDictResultSet) {
        const TString resultSetString =
            "columns {\n"
            "  name: \"colName\"\n"
            "  type {\n"
            "    optional_type {\n"
            "      item {\n"
            "        dict_type {\n"
            "          key {\n"
            "            type_id: INT32\n"
            "          }\n"
            "          payload {\n"
            "            type_id: STRING\n"
            "          }\n"
            "        }\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n"
            "rows {\n"
            "  items {\n"
            "    pairs {\n"
            "      key {\n"
            "        int32_value: 42\n"
            "      }\n"
            "      payload {\n"
            "        bytes_value: \"abc\"\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n"
            "rows {\n"
            "  items {\n"
            "    pairs {\n"
            "      key {\n"
            "        int32_value: 43\n"
            "      }\n"
            "      payload {\n"
            "        bytes_value: \"zxc\"\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n";
        Ydb::ResultSet rsProto;
        google::protobuf::TextFormat::ParseFromString(resultSetString, &rsProto);

        NYdb::TResultSet rs(std::move(rsProto));
        NYdb::TResultSetParser rsParser(rs);
        UNIT_ASSERT_EQUAL(rsParser.ColumnsCount(), 1);
        UNIT_ASSERT_EQUAL(rsParser.RowsCount(), 2);
        UNIT_ASSERT_EQUAL(rsParser.ColumnIndex("colName"), 0);
        UNIT_ASSERT_EQUAL(rsParser.ColumnIndex("otherName"), -1);
        auto& column0 = rsParser.ColumnParser(0);

        UNIT_ASSERT(rsParser.TryNextRow());
        column0.OpenOptional();
        UNIT_ASSERT(!column0.IsNull());
        column0.OpenDict();
        UNIT_ASSERT(column0.TryNextDictItem());
        column0.DictKey();
        UNIT_ASSERT_EQUAL(column0.GetInt32(), 42);
        column0.DictPayload();
        UNIT_ASSERT_EQUAL(column0.GetString(), "abc");

        UNIT_ASSERT(rsParser.TryNextRow());
        column0.OpenOptional();
        UNIT_ASSERT(!column0.IsNull());
        column0.OpenDict();
        UNIT_ASSERT(column0.TryNextDictItem());
        column0.DictKey();
        UNIT_ASSERT_EQUAL(column0.GetInt32(), 43);
        column0.DictPayload();
        UNIT_ASSERT_EQUAL(column0.GetString(), "zxc");
        column0.DictKey();
        column0.DictKey();
        UNIT_ASSERT_EQUAL(column0.GetInt32(), 43);
        UNIT_ASSERT_EQUAL(column0.GetInt32(), 43);
        column0.DictPayload();
        UNIT_ASSERT_EQUAL(column0.GetString(), "zxc");
        UNIT_ASSERT_EQUAL(column0.GetString(), "zxc");
    }

    Y_UNIT_TEST(Utf8OptionalResultSet) {
        const TString resultSetString =
            "columns {\n"
            "  name: \"colName\"\n"
            "  type {\n"
            "    optional_type {\n"
            "      item {\n"
            "        type_id: UTF8\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n"
            "rows {\n"
            "  items {\n"
            "    text_value: \"йцукен\"\n"
            "  }\n"
            "}\n"
            "rows {\n"
            "  items {\n"
            "    null_flag_value: NULL_VALUE\n"
            "  }\n"
            "}\n";
        Ydb::ResultSet rsProto;
        google::protobuf::TextFormat::ParseFromString(resultSetString, &rsProto);

        NYdb::TResultSet rs(std::move(rsProto));
        NYdb::TResultSetParser rsParser(rs);
        UNIT_ASSERT_EQUAL(rsParser.ColumnsCount(), 1);
        UNIT_ASSERT_EQUAL(rsParser.RowsCount(), 2);
        auto& column0 = rsParser.ColumnParser(0);

        int row = 0;
        while (rsParser.TryNextRow()) {
            column0.OpenOptional();

            if (row == 0) {
                UNIT_ASSERT(!column0.IsNull());
                UNIT_ASSERT_EQUAL(column0.GetUtf8(), "йцукен");
            } else {
                UNIT_ASSERT(column0.IsNull());
            }

            row++;
        }
    }

    Y_UNIT_TEST(ListCorruptedResultSet) {
        const TString resultSetString =
            "columns {\n"
            "  name: \"colName1\"\n"
            "  type {\n"
            "    optional_type {\n"
            "      item {\n"
            "        type_id: UTF8\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n"
            "columns {\n"
            "  name: \"colName2\"\n"
            "  type {\n"
            "    optional_type {\n"
            "      item {\n"
            "        type_id: UTF8\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n"
            "rows {\n"
            "  items {\n"
            "    bytes_value: \"test content\"\n"
            "  }\n"
            "}\n";
        Ydb::ResultSet rsProto;
        google::protobuf::TextFormat::ParseFromString(resultSetString, &rsProto);

        NYdb::TResultSet rs(std::move(rsProto));
        NYdb::TResultSetParser rsParser(rs);
        UNIT_ASSERT_EQUAL(rsParser.ColumnsCount(), 2);
        UNIT_ASSERT_EQUAL(rsParser.RowsCount(), 1);
        
        UNIT_ASSERT_EXCEPTION_CONTAINS(rsParser.TryNextRow(), TContractViolation, "Corrupted data: row 0 contains 1 column(s), but metadata contains 2 column(s)");
    }
}
