#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpKv) {
    Y_UNIT_TEST(BulkUpsert) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(true);
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            --!syntax_v1

            CREATE TABLE TestTable (
                Key Int64,
                Data Uint32,
                Value Utf8,
                PRIMARY KEY (Key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 10; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("Key").Int64(i)
                .AddMember("Data").Uint32(i*137)
                .AddMember("Value").Utf8("abcde")
                .EndStruct();
        }
        rows.EndList();

        auto upsertResult = db.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM TestTable ;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);
        auto res = FormatResultSetYson(result.GetResultSet(0));
        Cout << res << Endl;
        CompareYson(R"(
            [[[0u];[0];["abcde"]];[[137u];[1];["abcde"]];[[274u];[2];["abcde"]];[[411u];[3];["abcde"]];[[548u];[4];["abcde"]];[[685u];[5];["abcde"]];[[822u];[6];["abcde"]];[[959u];[7];["abcde"]];[[1096u];[8];["abcde"]];[[1233u];[9];["abcde"]]]
        )", res);
    }

    Y_UNIT_TEST(ReadRows) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(true);
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE TestTable (
                Key Uint64,
                Data Uint32,
                Value Utf8,
                PRIMARY KEY (Key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 1000; ++i) {
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(i * 1921763474923857134ull + 1858343823)
                    .AddMember("Data").Uint32(i)
                    .AddMember("Value").Utf8("abcde")
                .EndStruct();
        }
        rows.EndList();

        auto upsertResult = db.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder keys;
        keys.BeginList();
        for (size_t i = 0; i < 5; ++i) {
            keys.AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(i * 1921763474923857134ull + 1858343823)
                .EndStruct();
        }
        keys.EndList();
        auto selectResult = db.ReadRows("/Root/TestTable", keys.Build()).GetValueSync();
        UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());
        auto res = FormatResultSetYson(selectResult.GetResultSet());
        CompareYson(R"(
            [
                [1858343823u;0u;"abcde"];
                [1921763476782200957u;1u;"abcde"];
                [3843526951706058091u;2u;"abcde"];
                [5765290426629915225u;3u;"abcde"];
                [7687053901553772359u;4u;"abcde"]
            ]
        )", res);
    }
}

} // namespace NKikimr::NKqp
