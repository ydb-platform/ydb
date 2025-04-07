#include "run_ydb.h"

#include <util/string/split.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/stream/file.h>
#include <util/string/printf.h>

#include <google/protobuf/text_format.h>

Y_UNIT_TEST_SUITE(YdbDump) {

Y_UNIT_TEST(NotNullTypeDump) {
    const char* tableName = "TableWithNotNullTypeForDump";
    RunYdb({"-v", "yql", "-s",
        Sprintf(R"(CREATE TABLE %s (
            k Uint32 NOT NULL,
            v String NOT NULL,
            ov String,
            PRIMARY KEY(k));
        )", tableName)},
        TList<TString>());

    const auto dumpPath = GetOutputPath() / "dump";
    RunYdb({"-v", "tools", "dump", "--scheme-only", "--output", dumpPath.GetPath()}, TList<TString>());

    struct TFlags {
        const bool HasNullFlag;
        const bool IsOptionalType;
    };

    TVector<TFlags> column_flags;
    auto fillFlag = [&column_flags](const TString& str) {
        Ydb::Table::ColumnMeta meta;
        google::protobuf::TextFormat::ParseFromString(str, &meta);
        column_flags.emplace_back(TFlags{meta.has_not_null(), meta.type().has_optional_type()});
    };

    const auto output = TFileInput(dumpPath / tableName / "scheme.pb").ReadAll();
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

    // For compatibility reason we do not show not null flag
    UNIT_ASSERT_VALUES_EQUAL(column_flags.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(column_flags[0].HasNullFlag,    false);
    UNIT_ASSERT_VALUES_EQUAL(column_flags[0].IsOptionalType, false);
    UNIT_ASSERT_VALUES_EQUAL(column_flags[1].HasNullFlag,    false);
    UNIT_ASSERT_VALUES_EQUAL(column_flags[1].IsOptionalType, false);
    UNIT_ASSERT_VALUES_EQUAL(column_flags[2].HasNullFlag,    false);
    UNIT_ASSERT_VALUES_EQUAL(column_flags[2].IsOptionalType, true);
}

}
