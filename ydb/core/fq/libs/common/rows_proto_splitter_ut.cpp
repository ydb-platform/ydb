#include "rows_proto_splitter.h"

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/services/ydb/ydb_common_ut.h>

namespace NFq {

namespace {

Ydb::ResultSet MakeProtoResultSet() {
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
    return rsProto;
}

} //namespace

Y_UNIT_TEST_SUITE(SplitterBasic) {
    Y_UNIT_TEST(EqualSplitByMaxBytesLimitPerChunk) {
        Ydb::ResultSet resultSet = MakeProtoResultSet();
        TRowsProtoSplitter splitter(resultSet, 41, 0);
        const auto& result = splitter.Split();
        UNIT_ASSERT_C(result.Success, result.Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.ResultSets.size(), 2);

        int expectedVal = 42;
        for (const auto& rs : result.ResultSets) {
            NYdb::TResultSetParser rsParser(rs);
            UNIT_ASSERT_EQUAL(rsParser.ColumnsCount(), 1);
            UNIT_ASSERT_EQUAL(rsParser.RowsCount(), 1);
            UNIT_ASSERT_EQUAL(rsParser.ColumnIndex("colName"), 0);
            UNIT_ASSERT_EQUAL(rsParser.ColumnIndex("otherName"), -1);
            auto& column0 = rsParser.ColumnParser(0);
            while (rsParser.TryNextRow()) {
                column0.OpenList();
                while (column0.TryNextListItem()) {
                    UNIT_ASSERT_EQUAL(column0.GetInt32(), expectedVal++);
                }
            }
        }
    }

    Y_UNIT_TEST(EqualSplitByMaxRowsLimitPerChunk) {
        Ydb::ResultSet resultSet = MakeProtoResultSet();
        TRowsProtoSplitter splitter(resultSet, 1024 * 1024, 0, 1);
        const auto& result = splitter.Split();
        UNIT_ASSERT_C(result.Success, result.Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.ResultSets.size(), 2);

        int expectedVal = 42;
        for (const auto& rs : result.ResultSets) {
            NYdb::TResultSetParser rsParser(rs);
            UNIT_ASSERT_EQUAL(rsParser.ColumnsCount(), 1);
            UNIT_ASSERT_EQUAL_C(rsParser.RowsCount(), 1, rsParser.RowsCount());
            UNIT_ASSERT_EQUAL(rsParser.ColumnIndex("colName"), 0);
            UNIT_ASSERT_EQUAL(rsParser.ColumnIndex("otherName"), -1);
            auto& column0 = rsParser.ColumnParser(0);
            while (rsParser.TryNextRow()) {
                column0.OpenList();
                while (column0.TryNextListItem()) {
                    UNIT_ASSERT_EQUAL(column0.GetInt32(), expectedVal++);
                }
            }
        }
    }

    Y_UNIT_TEST(LimitExceed) {
        Ydb::ResultSet resultSet = MakeProtoResultSet();
        TRowsProtoSplitter splitter(resultSet, 1, 0);
        const auto& result = splitter.Split();
        const auto& issuesStr = result.Issues.ToString();
        UNIT_ASSERT_C(!result.Success, issuesStr);
        UNIT_ASSERT_VALUES_EQUAL(result.ResultSets.size(), 0);
        UNIT_ASSERT_C(issuesStr.Contains("Can not write Row[0] with size: 41 bytes"), issuesStr);
    }
}

} // NFq
