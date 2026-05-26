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

Y_UNIT_TEST_SUITE(KqpFulltextCompact) {

TResultSet ReadIndex(NQuery::TQueryClient& db, const char* table = "indexImplTable") {
    TString query = Sprintf(R"sql(
        SELECT * FROM `/Root/Texts/fulltext_idx/%s`;
    )sql", table);
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return result.GetResultSet(0);
}

Y_UNIT_TEST(AddIndexCompact) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertTexts(db);
    AddIndex(db, "fulltext_compact");

    auto index = ReadIndex(db);
    CompareYson(R"([
        [%true;0u;100u;"d";"animals"];
        [%true;0u;300u;"ddd";"cats"];
        [%true;0u;200u;"dd";"chase"];
        [%true;0u;400u;"\xC8\1\xC8\1";"dogs"];
        [%true;0u;400u;"\x90\3";"foxes"];
        [%true;0u;400u;"\xAC\2d";"love"];
        [%true;0u;200u;"dd";"small"]
    ])", NYdb::FormatResultSetYson(index));
}

Y_UNIT_TEST_TWIN(AddIndexCompactRelevance, Covered) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertTexts(db);
    if (Covered)
        AddIndexCovered(db, "fulltext_compact_relevance");
    else
        AddIndex(db, "fulltext_compact_relevance");

    auto index = ReadIndex(db);
    CompareYson(R"([
        [%true;0u;100u;"\xA4\1";"animals"];
        [%true;0u;300u;"\xA4\1\xA4\1\xE4\1\2";"cats"];
        [%true;0u;200u;"\xA4\1\xA4\1";"chase"];
        [%true;0u;400u;"\x88\3\x88\3";"dogs"];
        [%true;0u;400u;"\x90\6";"foxes"];
        [%true;0u;400u;"\xAC\4\xA4\1";"love"];
        [%true;0u;200u;"\xA4\1\xA4\1";"small"]
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

Y_UNIT_TEST_TWIN(InsertRow, WithRelevance) {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    CreateTexts(db);
    UpsertSomeTexts(db);
    AddIndex(db, WithRelevance ? "fulltext_compact_relevance" : "fulltext_compact");
    auto index = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "indexImplTable: " << index << Endl;
    if (WithRelevance) {
        auto docs = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::DocsTable));
        auto dict = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::DictTable));
        auto stats = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::StatsTable));
        Cerr << "indexImplDocsTable: " << docs << Endl;
        Cerr << "indexImplDictTable: " << dict << Endl;
        Cerr << "indexImplStatsTable: " << stats << Endl;
        CompareYson(R"([
            [%true;18446744073709551615u;18446744073709551615u;"\xE4\1\2";"cats"];
            [%true;18446744073709551615u;18446744073709551615u;"\x88\3";"dogs"];
            [%true;18446744073709551615u;18446744073709551615u;"\x88\3";"foxes"];
            [%true;18446744073709551615u;18446744073709551615u;"\xA4\1\xA4\1";"love"]
        ])", index);
        CompareYson(R"([
            [[100u];3u];
            [[200u];3u]
        ])", docs);
        CompareYson(R"([
            [1u;"cats"];
            [1u;"dogs"];
            [1u;"foxes"];
            [2u;"love"]
        ])", dict);
        CompareYson(R"([
            [2u;0u;6u]
        ])", stats);
    } else {
        CompareYson(R"([
            [%true;18446744073709551615u;18446744073709551615u;"d";"cats"];
            [%true;18446744073709551615u;18446744073709551615u;"\xC8\1";"dogs"];
            [%true;18446744073709551615u;18446744073709551615u;"\xC8\1";"foxes"];
            [%true;18446744073709551615u;18446744073709551615u;"dd";"love"]
        ])", index);
    }

    { // InsertRow
        TString query = R"sql(
            INSERT INTO `/Root/Texts` (Key, Text, Data) VALUES
                (150, "Foxes love cats.", "foxes data")
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    index = NYdb::FormatResultSetYson(ReadIndex(db));
    Cerr << "indexImplTable: " << index << Endl;
    if (WithRelevance) {
        auto docs = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::DocsTable));
        auto dict = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::DictTable));
        auto stats = NYdb::FormatResultSetYson(ReadIndex(db, NTableIndex::NFulltext::StatsTable));
        Cerr << "indexImplDocsTable: " << docs << Endl;
        Cerr << "indexImplDictTable: " << dict << Endl;
        Cerr << "indexImplStatsTable: " << stats << Endl;
        CompareYson(R"([
            [%true;18446744073709551610u;18446744073709551615u;"\x96\2";"cats"];
            [%true;18446744073709551615u;18446744073709551615u;"\xE4\1\2";"cats"];
            [%true;18446744073709551615u;18446744073709551615u;"\x88\3";"dogs"];
            [%true;18446744073709551610u;18446744073709551615u;"\x96\2";"foxes"];
            [%true;18446744073709551615u;18446744073709551615u;"\x88\3";"foxes"];
            [%true;18446744073709551610u;18446744073709551615u;"\x96\2";"love"];
            [%true;18446744073709551615u;18446744073709551615u;"\xA4\1\xA4\1";"love"]
        ])", index);
        CompareYson(R"([
            [[100u];3u];
            [[150u];3u];
            [[200u];3u]
        ])", docs);
        CompareYson(R"([
            [2u;"cats"];
            [1u;"dogs"];
            [2u;"foxes"];
            [3u;"love"]
        ])", dict);
        CompareYson(R"([
            [3u;0u;9u]
        ])", stats);
    } else {
        CompareYson(R"([
            [%true;18446744073709551610u;18446744073709551615u;"\x96\1";"cats"];
            [%true;18446744073709551615u;18446744073709551615u;"d";"cats"];
            [%true;18446744073709551615u;18446744073709551615u;"\xC8\1";"dogs"];
            [%true;18446744073709551610u;18446744073709551615u;"\x96\1";"foxes"];
            [%true;18446744073709551615u;18446744073709551615u;"\xC8\1";"foxes"];
            [%true;18446744073709551610u;18446744073709551615u;"\x96\1";"love"];
            [%true;18446744073709551615u;18446744073709551615u;"dd";"love"]
        ])", index);
    }

    {
        TString query = R"sql(
            SELECT `Key`, `Text`, `Data`
            FROM `/Root/Texts` VIEW `fulltext_idx`
            WHERE FulltextMatch(`Text`, "foxes cats")
            ORDER BY `Key`;
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[150u];["Foxes love cats."];["foxes data"]]
        ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

}

}

} // namespace NKikimr::NKqp
