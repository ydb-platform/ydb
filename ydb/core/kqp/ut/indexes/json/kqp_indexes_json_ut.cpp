#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <library/cpp/json/json_reader.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpJsonIndexes) {

TKikimrRunner Kikimr(NKikimrConfig::TFeatureFlags&& featureFlags) {
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    return TKikimrRunner(settings);
}

TKikimrRunner Kikimr() {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableFulltextIndex(true);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    return TKikimrRunner(settings);
}

void CreateTestTable(NQuery::TQueryClient& db, const char* type = "String") {
    TString query = std::format(R"sql(
        CREATE TABLE `/Root/TestTable` (
            Key Uint64,
            Text {0},
            Data Utf8,
            PRIMARY KEY (Key)
        );
    )sql", type);
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

TResultSet ReadIndex(NQuery::TQueryClient& db, const char* table = "indexImplTable") {
    TString query = Sprintf(R"sql(
        SELECT * FROM `/Root/TestTable/json_idx/%s`;
    )sql", table);
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return result.GetResultSet(0);
}

void DoTestAddJsonIndex(const TString& type, bool nullable, bool covered) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    auto columnType = type + (nullable ? "" : " not null");
    CreateTestTable(db, columnType.c_str());
    {
        TString castStart, castEnd;
        if (type == "JsonDocument") {
            castStart = !nullable ? "unwrap(cast(" : "cast(";
            castEnd = !nullable ? " as JsonDocument))" : " as JsonDocument)";
        }
        TString query = Sprintf(R"sql(
            UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES
                (10, %1$s"\"literal string\""%2$s, "d1"),
                (11, %1$s"0.123"%2$s, "data 2"),
                (12, %1$s"true"%2$s, "very long unit test data 3"),
                (13, %1$s"false"%2$s, "data 4"),
                (14, %1$s"null"%2$s, "data 5"),
                (15, %1$s"[false,\"item 1\",45]"%2$s, "array data 6"),
                (16, %1$s"{\"id\":42042,\"brand\":\"bricks\",\"part_count\":1401,\"price\":null,\"parts\":
                    [{\"id\":32526,\"count\":7,\"name\":\"3x5\"},{\"id\":32523,\"count\":17,\"name\":\"1x3\"}]}"%2$s, "object data 7")
        )sql", castStart.c_str(), castEnd.c_str());
        if (nullable) {
            query += ", (17, NULL, \"null data 8\")";
        }
        if (type == "String") {
            query += ", (18, \"[invalid json\", \"invalid data 9\")";
        }
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    {
        TString query = R"sql(
            ALTER TABLE `/Root/TestTable` ADD INDEX json_idx
                GLOBAL USING json ON (Text)
        )sql";
        if (covered) {
            query += " COVER (Data)";
        }
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    auto index = ReadIndex(db);
    if (covered) {
        CompareYson(R"([
            [["data 4"];[13u];"\0\0"];
            [["array data 6"];[15u];"\0\0"];
            [["very long unit test data 3"];[12u];"\0\1"];
            [["data 5"];[14u];"\0\2"];
            [["array data 6"];[15u];"\0\3item 1"];
            [["d1"];[10u];"\0\3literal string"];
            [["array data 6"];[15u];"\0\4\0\0\0\0\0\200F@"];
            [["data 2"];[11u];"\0\4\xB0rh\x91\xED|\xBF?"];
            [["object data 7"];[16u];"\2id\0\4\0\0\0\0@\x87\xE4@"];
            [["object data 7"];[16u];"\5brand\0\3bricks"];
            [["object data 7"];[16u];"\5parts\2id\0\4\0\0\0\0\x80\xC3\xDF@"];
            [["object data 7"];[16u];"\5parts\2id\0\4\0\0\0\0\xC0\xC2\xDF@"];
            [["object data 7"];[16u];"\5parts\4name\0\0031x3"];
            [["object data 7"];[16u];"\5parts\4name\0\0033x5"];
            [["object data 7"];[16u];"\5parts\5count\0\4\0\0\0\0\0\0\x1C@"];
            [["object data 7"];[16u];"\5parts\5count\0\4\0\0\0\0\0\0001@"];
            [["object data 7"];[16u];"\5price\0\2"];
            [["object data 7"];[16u];"\npart_count\0\4\0\0\0\0\0\xE4\x95@"]
        ])", NYdb::FormatResultSetYson(index));
    } else {
        CompareYson(R"([
            [[13u];"\0\0"];
            [[15u];"\0\0"];
            [[12u];"\0\1"];
            [[14u];"\0\2"];
            [[15u];"\0\3item 1"];
            [[10u];"\0\3literal string"];
            [[15u];"\0\4\0\0\0\0\0\200F@"];
            [[11u];"\0\4\xB0rh\x91\xED|\xBF?"];
            [[16u];"\2id\0\4\0\0\0\0@\x87\xE4@"];
            [[16u];"\5brand\0\3bricks"];
            [[16u];"\5parts\2id\0\4\0\0\0\0\x80\xC3\xDF@"];
            [[16u];"\5parts\2id\0\4\0\0\0\0\xC0\xC2\xDF@"];
            [[16u];"\5parts\4name\0\0031x3"];
            [[16u];"\5parts\4name\0\0033x5"];
            [[16u];"\5parts\5count\0\4\0\0\0\0\0\0\x1C@"];
            [[16u];"\5parts\5count\0\4\0\0\0\0\0\0001@"];
            [[16u];"\5price\0\2"];
            [[16u];"\npart_count\0\4\0\0\0\0\0\xE4\x95@"]
        ])", NYdb::FormatResultSetYson(index));
    }
}

Y_UNIT_TEST(AddJsonIndexJson) {
    DoTestAddJsonIndex("Json", true, false);
}

Y_UNIT_TEST(AddJsonIndexJsonDocument) {
    DoTestAddJsonIndex("JsonDocument", true, false);
}

Y_UNIT_TEST(AddJsonIndexString) {
    DoTestAddJsonIndex("String", true, false);
}

Y_UNIT_TEST(AddJsonIndexJsonNotNull) {
    DoTestAddJsonIndex("Json", false, false);
}

Y_UNIT_TEST(AddJsonIndexJsonDocumentNotNull) {
    DoTestAddJsonIndex("JsonDocument", false, false);
}

Y_UNIT_TEST(AddJsonIndexStringNotNull) {
    DoTestAddJsonIndex("String", false, false);
}

Y_UNIT_TEST(AddJsonIndexCoveringJson) {
    DoTestAddJsonIndex("Json", true, true);
}

Y_UNIT_TEST(AddJsonIndexCoveringJsonDocument) {
    DoTestAddJsonIndex("JsonDocument", true, true);
}

Y_UNIT_TEST(AddJsonIndexCoveringString) {
    DoTestAddJsonIndex("String", true, true);
}

Y_UNIT_TEST(AddJsonIndexCoveringJsonNotNull) {
    DoTestAddJsonIndex("Json", false, true);
}

Y_UNIT_TEST(AddJsonIndexCoveringJsonDocumentNotNull) {
    DoTestAddJsonIndex("JsonDocument", false, true);
}

Y_UNIT_TEST(AddJsonIndexCoveringStringNotNull) {
    DoTestAddJsonIndex("String", false, true);
}

}

} // namespace NKikimr::NKqp
