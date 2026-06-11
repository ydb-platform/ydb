#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/kqp/ut/indexes/fulltext/kqp_fulltext_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void ExecuteQuery(NQuery::TQueryClient& db, const TString& query) {
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

TResultSet ReadIndex(NQuery::TQueryClient& db, const char* table = "indexImplTable") {
    TString query = Sprintf(R"sql(
        SELECT * FROM `/Root/Texts/fulltext_idx/%s`;
    )sql", table);
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return result.GetResultSet(0);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpFulltextCompact) {

Y_UNIT_TEST(AddIndexCompact) {
    auto kikimr = KikimrWithCompact();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertTexts(db);
    AddIndex(db, "fulltext_plain");

    auto index = ReadIndex(db);
    CompareYson(R"([
        [%true;4294967295u;100u;"d";"animals"];
        [%true;4294967295u;300u;"ddd";"cats"];
        [%true;4294967295u;200u;"dd";"chase"];
        [%true;4294967295u;400u;"\xC8\1\xC8\1";"dogs"];
        [%true;4294967295u;400u;"\x90\3";"foxes"];
        [%true;4294967295u;400u;"\xAC\2d";"love"];
        [%true;4294967295u;200u;"dd";"small"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST_TWIN(AddIndexCompactRelevance, Covered) {
    auto kikimr = KikimrWithCompact();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertTexts(db);
    if (Covered) {
        AddIndexCovered(db, "fulltext_relevance");
    } else {
        AddIndex(db, "fulltext_relevance");
    }

    auto index = ReadIndex(db);
    CompareYson(R"([
        [%true;4294967295u;100u;"\xA4\1";"animals"];
        [%true;4294967295u;300u;"\xA4\1\xA4\1\xE4\1\2";"cats"];
        [%true;4294967295u;200u;"\xA4\1\xA4\1";"chase"];
        [%true;4294967295u;400u;"\x88\3\x88\3";"dogs"];
        [%true;4294967295u;400u;"\x90\6";"foxes"];
        [%true;4294967295u;400u;"\xAC\4\xA4\1";"love"];
        [%true;4294967295u;200u;"\xA4\1\xA4\1";"small"]
    ])", NYdb::FormatResultSetYson(index));

    index = ReadIndex(db, NTableIndex::NFulltext::DocsTable);
    if (Covered) {
        CompareYson(R"([
            [["cats data"];[100u];4u];
            [["dogs data"];[200u];4u];
            [["cats cats data"];[300u];3u];
            [["foxes data"];[400u];3u]
        ])", NYdb::FormatResultSetYson(index));
    } else {
        CompareYson(R"([
            [[100u];4u];
            [[200u];4u];
            [[300u];3u];
            [[400u];3u]
        ])", NYdb::FormatResultSetYson(index));
    }

    index = ReadIndex(db, NTableIndex::NFulltext::DictTable);
    CompareYson(R"([
        [1u;"animals"];
        [3u;"cats"];
        [2u;"chase"];
        [2u;"dogs"];
        [1u;"foxes"];
        [2u;"love"];
        [2u;"small"]
    ])", NYdb::FormatResultSetYson(index));

    index = ReadIndex(db, NTableIndex::NFulltext::StatsTable);
    CompareYson(R"([
        [4u;0u;14u]
    ])", NYdb::FormatResultSetYson(index));
}

} // Y_UNIT_TEST_SUITE(KqpFulltextCompact)

Y_UNIT_TEST_SUITE(KqpJsonCompact) {

TResultSet ReadIndex(NQuery::TQueryClient& db, const char* table = "indexImplTable") {
    TString query = Sprintf(R"sql(
        SELECT * FROM `/Root/Texts/json_idx/%s`;
    )sql", table);
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return result.GetResultSet(0);
}

Y_UNIT_TEST(AddJsonCompactIndex) {
    auto kikimr = KikimrWithCompact();
    auto db = kikimr.GetQueryClient();

    ExecuteQuery(db, R"sql(
        CREATE TABLE `/Root/Texts` (
            Key Uint64,
            Text Json,
            Data String,
            PRIMARY KEY (Key)
        );
    )sql");

    ExecuteQuery(db, R"sql(
        UPSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
            (100, '"hello world"', "data1"),
            (200, '42', "data2"),
            (300, 'true', "data3"),
            (400, '{"name":"test","value":123}', "data4")
    )sql");

    ExecuteQuery(db, R"sql(
        ALTER TABLE `/Root/Texts` ADD INDEX json_idx
            GLOBAL USING json ON (Text)
    )sql");

    auto index = ReadIndex(db);
    auto indexStr = NYdb::FormatResultSetYson(index);
    Cerr << "json index: " << indexStr << Endl;

    NYdb::TResultSetParser parser(index);
    UNIT_ASSERT(parser.RowsCount() > 0);
}

} // Y_UNIT_TEST_SUITE(KqpJsonCompact)

} // namespace NKikimr::NKqp
