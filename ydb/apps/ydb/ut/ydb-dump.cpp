#include "run_ydb.h"

#include <util/string/split.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

Y_UNIT_TEST_SUITE(YdbDump) {

Y_UNIT_TEST(NotNullTypeDump) {
    RunYdb({"-v", "yql", "-s",
        R"(CREATE TABLE TableWithNotNullTypeForDump (
            k Uint32 NOT NULL,
            v String NOT NULL,
            ov String,
            PRIMARY KEY(k));
        )"},
        TList<TString>());

    const TString output = RunYdb({"-v", "tools", "dump", "--scheme-only"}, TList<TString>());

    TVector<bool> column_not_null_flag;
    auto fillFlag = [&column_not_null_flag](const TString& str) {
        Ydb::Table::ColumnMeta meta;
        google::protobuf::TextFormat::ParseFromString(str, &meta);
        column_not_null_flag.emplace_back(meta.not_null());
    };

    const TString token = "columns {";
    size_t start = 0;
    while (true) {
        start = output.find(token, start);
        if (start != TString::npos) {
            int scope = 1;
            start += token.size();
            size_t pos = start;
            while (pos < output.size() && scope != 0) {
                if (output[pos] == '{') {
                    scope++;
                } else if (output[pos] == '}') {
                    scope--;
                }
                pos++;
            }
            Y_ABORT_UNLESS(pos > start);
            fillFlag(output.substr(start, pos - start - 1));
            start = pos;
        } else {
            break;
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(column_not_null_flag.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(column_not_null_flag[0], true);
    UNIT_ASSERT_VALUES_EQUAL(column_not_null_flag[1], true);
    UNIT_ASSERT_VALUES_EQUAL(column_not_null_flag[2], false);
}

}
