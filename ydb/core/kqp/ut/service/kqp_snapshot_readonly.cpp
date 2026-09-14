#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NQuery;

namespace {

void CreateColumnTables(NYdb::NTable::TSession& session, NYdb::NQuery::TQueryClient& queryClient) {
    {
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/ColumnTable1` (
                Key Uint32 NOT NULL,
                ColumnText Utf8,
                ColumnAmount Int64,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(Key)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        
        auto insertResult = queryClient.ExecuteQuery(R"(
            INSERT INTO `/Root/ColumnTable1` (Key, ColumnText, ColumnAmount) VALUES
                (1u, "ColumnData1", 1000l),
                (2u, "ColumnData2", 2000l),
                (3u, "ColumnData3", 3000l);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
    }
    
    {
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/ColumnTable2` (
                Key Uint32 NOT NULL,
                Category Utf8,
                Score Double,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(Key)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        
        auto insertResult = queryClient.ExecuteQuery(R"(
            INSERT INTO `/Root/ColumnTable2` (Key, Category, Score) VALUES
                (1u, "Electronics", 95.5),
                (2u, "Books", 87.2),
                (3u, "Clothing", 92.8);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
    }
    
    {
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/ColumnTable3` (
                Key Uint32 NOT NULL,
                Description Utf8,
                Timestamp Timestamp,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(Key)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        
        auto insertResult = queryClient.ExecuteQuery(R"(
            INSERT INTO `/Root/ColumnTable3` (Key, Description, Timestamp) VALUES
                (1u, "First item", Timestamp("2023-01-01T00:00:00.000000Z")),
                (2u, "Second item", Timestamp("2023-01-02T00:00:00.000000Z")),
                (3u, "Third item", Timestamp("2023-01-03T00:00:00.000000Z"));
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
    }
}

void CreateRowTableForJoin(NYdb::NTable::TSession& session) {
    auto tableDesc = TTableBuilder()
        .AddNullableColumn("Key", EPrimitiveType::Uint32)
        .AddNullableColumn("Text", EPrimitiveType::Utf8)
        .AddNullableColumn("Amount", EPrimitiveType::Int64)
        .SetPrimaryKeyColumns({"Key"})
        .Build();
    
    auto result = session.CreateTable("/Root/RowTableForJoin", std::move(tableDesc)).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    
    auto query = R"(
        INSERT INTO `/Root/RowTableForJoin` (Key, Text, Amount) VALUES
            (1u, "RowData1", 1000l),
            (2u, "RowData2", 2000l),
            (3u, "RowData3", 3000l);
    )";
    auto insertResult = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), EStatus::SUCCESS, insertResult.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(KqpSnapshotReadOnly) {

    Y_UNIT_TEST(SnapshotReadOnlyCheckNoLocks) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetQueryClient();
        auto tc = kikimr.GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        
        CreateSampleTablesWithIndex(session);
        CreateColumnTables(session, db);
        CreateRowTableForJoin(session);
        
        TVector<TString> queries = {
            // row table join
            R"(
                SELECT t.Group, t.Name, kv.Value
                FROM `/Root/Test` AS t
                JOIN `/Root/KeyValue` AS kv ON CAST(t.Group AS Uint64) = kv.Key
                ORDER BY t.Group, t.Name;
            )",
            
            // diff shard join
            R"(
                SELECT ts.Key, ts.Value1, es.Text, es.Data
                FROM `/Root/TwoShard` AS ts
                JOIN `/Root/EightShard` AS es ON CAST(ts.Key AS Uint64) = es.Key
                ORDER BY ts.Key;
            )",
            
            // join with index
            R"(
                SELECT sk.Key, sk.Value, sck.Fk1, sck.Fk2
                FROM `/Root/SecondaryKeys` AS sk
                JOIN `/Root/SecondaryComplexKeys` AS sck ON sk.Fk = sck.Fk1
                ORDER BY sk.Key;
            )",
            
            // column and row tables join
            R"(
                SELECT rtj.Key, rtj.Text, ct1.ColumnText, ct1.ColumnAmount
                FROM `/Root/RowTableForJoin` AS rtj
                JOIN `/Root/ColumnTable1` AS ct1 ON rtj.Key = ct1.Key
                ORDER BY rtj.Key;
            )",
            
            // column tables join
            R"(
                SELECT ct1.Key, ct1.ColumnText, ct2.Category, ct3.Description
                FROM `/Root/ColumnTable1` AS ct1
                JOIN `/Root/ColumnTable2` AS ct2 ON ct1.Key = ct2.Key
                JOIN `/Root/ColumnTable3` AS ct3 ON ct1.Key = ct3.Key
                ORDER BY ct1.Key;
            )",
            
            // column tables join with aggregation
            R"(
                SELECT COUNT(*) as total_count, AVG(ct1.ColumnAmount) as avg_amount, AVG(ct2.Score) as avg_score
                FROM `/Root/RowTableForJoin` AS rtj
                JOIN `/Root/ColumnTable1` AS ct1 ON rtj.Key = ct1.Key
                JOIN `/Root/ColumnTable2` AS ct2 ON rtj.Key = ct2.Key
            )",
            
            // read with different keys
            R"(
                SELECT kv1.Key as NumKey, kv1.Value as NumValue, 
                       kv2.Key as StrKey, kv2.Value as StrValue
                FROM `/Root/KeyValue` AS kv1
                CROSS JOIN `/Root/KeyValue2` AS kv2
                ORDER BY kv1.Key, kv2.Key
            )"
        };
        
        auto txSettings = NYdb::NQuery::TTxSettings::SnapshotRO();
        for (auto& query : queries) {
            auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx(txSettings).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, "query: " + query + ", result: " + result.GetIssues().ToString());
        }
    }
}

} // namespace NKqp
} // namespace NKikimr