#pragma once

#include <ydb/core/kqp/ut/rbo/effects/kqp_effects_helpers.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_yql_query_helpers.h>

namespace NKikimr::NKqp::NIndexReturningTests {

using namespace NYdb;
using namespace NYdb::NQuery;

// Ported from ut/indexes/kqp_stream_indexes_ut.cpp.
template <bool NewRbo, bool StreamIndex>
void UpdateDeleteReturningOnlyKey() {
    auto settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(StreamIndex);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo && !StreamIndex);

    TKikimrRunner kikimr(settings);

    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

    const TString createQuery = Sprintf(R"(
        CREATE TABLE `/Root/DataShard` (
            c0 Int64, c1 Int64, c2 Int64,
            PRIMARY KEY (c0, c1),
            INDEX idx0 GLOBAL SYNC ON (c1)
        );
    )");

    auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

    auto client = kikimr.GetQueryClient();

    auto it = client.ExecuteQuery(
        R"(
            INSERT INTO `/Root/DataShard` (c0, c1, c2) VALUES (0, 0, 0);
        )",
        NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    if constexpr (NewRbo && !StreamIndex) {
        UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "Compilation with the new RBO failed");
        UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "New RBO requires sink-maintained indexes");
    }

    {
        auto it = client.StreamExecuteQuery(
            R"(
                UPDATE `/Root/DataShard` ON (c0, c1, c2) VALUES (0, 0, 1)
                RETURNING c1;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        TString output = StreamResultToYson(it);
        CompareYson(output, R"([[[0]]])");
    }

    {
        auto it = client.StreamExecuteQuery(
            R"(
                DELETE FROM `/Root/DataShard` ON (c0, c1) VALUES (0, 0)
                RETURNING c1;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        TString output = StreamResultToYson(it);
        CompareYson(output, R"([[[0]]])");
    }
}

template <bool NewRbo, bool StreamIndex>
void ManyEmptyReturningClauses() {
    auto settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(StreamIndex);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo && !StreamIndex);

    TKikimrRunner kikimr(settings);

    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

    const TString createQuery = R"(
        CREATE TABLE `/Root/DataShard` (
            c1 Bool, c2 Utf8 NOT NULL, c3 Int8 NOT NULL, c4 Uint32 NOT NULL, c5 Int8, c6 Uint8 NOT NULL,
            PRIMARY KEY (c5),
        );
    )";

    auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

    auto client = kikimr.GetQueryClient();

    {
        auto params = TParamsBuilder()
            .AddParam("$p0_rows")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                    .AddMember("c5").Int8(0)
                    .EndStruct()
                .EndList()
                .Build()
            .AddParam("$p1_rows")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                    .AddMember("c5").Int8(1)
                    .EndStruct()
                .EndList()
                .Build()
            .Build();

        auto it = client.ExecuteQuery(
            R"(
                DELETE FROM `/Root/DataShard` ON
                SELECT * FROM AS_TABLE($p0_rows)
                RETURNING c6, c1, c2, c5, c4, c3;

                DELETE FROM `/Root/DataShard` ON
                SELECT * FROM AS_TABLE($p1_rows)
                RETURNING c5;
            )",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        if constexpr (NewRbo && !StreamIndex) {
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "Compilation with the new RBO failed");
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "Returning table sink requires returning-column metadata");
        }
        UNIT_ASSERT_VALUES_EQUAL(it.GetResultSets().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(it.GetResultSet(0).RowsCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(it.GetResultSet(1).RowsCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(it.GetResultSet(0).GetColumnsMeta().size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(it.GetResultSet(1).GetColumnsMeta().size(), 1);
    }
}

template <bool NewRbo, bool StreamIndex>
void UpdateIndexKeyChangeWithReturningVerifyIndex() {
    auto settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(StreamIndex);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo && !StreamIndex);

    TKikimrRunner kikimr(settings);

    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const TString createQuery = R"(
        CREATE TABLE `/Root/T` (
            pk Int64,
            s Int64,
            v Int64,
            PRIMARY KEY (pk),
            INDEX idx GLOBAL SYNC ON (s)
        );
    )";
    auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

    auto client = kikimr.GetQueryClient();

    {
        auto it = client.ExecuteQuery(
            "UPSERT INTO `/Root/T` (pk, s, v) VALUES (1, 10, 100);",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        if constexpr (NewRbo && !StreamIndex) {
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "Compilation with the new RBO failed");
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "New RBO requires sink-maintained indexes");
        }
    }

    // UPDATE changing index key + RETURNING
    {
        auto it = client.StreamExecuteQuery(
            "UPDATE `/Root/T` SET s = 20 WHERE pk = 1 RETURNING pk, s, v;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[1];[20];[100]]])", StreamResultToYson(it));
    }

    // Verify main table
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s, v FROM `/Root/T` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];[20];[100]]])", StreamResultToYson(it));
    }

    // Verify index table reflects the new key
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s FROM `/Root/T/idx/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];[20]]])", StreamResultToYson(it));
    }
}

template <bool NewRbo, bool StreamIndex>
void DefaultOnIndexedColumnWithReturning() {
    auto settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(StreamIndex);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo && !StreamIndex);

    TKikimrRunner kikimr(settings);

    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const TString createQuery = R"(
        CREATE TABLE `/Root/T` (
            pk Int64,
            s Int64 DEFAULT 42,
            v Int64,
            PRIMARY KEY (pk),
            INDEX idx GLOBAL SYNC ON (s)
        );
    )";
    auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

    auto client = kikimr.GetQueryClient();

    // INSERT omitting indexed column (s has DEFAULT 42) + RETURNING s
    {
        const auto failuresBefore = NYqlTests::GetNewRBOCompileCounters(kikimr).second;
        auto it = client.StreamExecuteQuery(
            "INSERT INTO `/Root/T` (pk, v) VALUES (1, 100) RETURNING pk, s, v;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[1];[42];[100]]])", StreamResultToYson(it));
        if constexpr (NewRbo && !StreamIndex) {
            // The initial stream response precedes compilation diagnostics.
            UNIT_ASSERT_C(NYqlTests::GetNewRBOCompileCounters(kikimr).second > failuresBefore,
                "expected a legacy optimizer retry");
        }
    }
    // Verify index table got DEFAULT value
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s FROM `/Root/T/idx/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];[42]]])", StreamResultToYson(it));
    }

    // UPSERT existing row
    {
        auto it = client.StreamExecuteQuery(
            "UPSERT INTO `/Root/T` (pk, s, v) VALUES (1, 100, 200) RETURNING pk, s, v;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[1];[100];[200]]])", StreamResultToYson(it));
    }
    // Verify index table
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s FROM `/Root/T/idx/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];[100]]])", StreamResultToYson(it));
    }

    // UPSERT new row omitting indexed column + RETURNING s
    {
        auto it = client.StreamExecuteQuery(
            "UPSERT INTO `/Root/T` (pk, v) VALUES (2, 200) RETURNING pk, s, v;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[2];[42];[200]]])", StreamResultToYson(it));
    }
    // Verify index table
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s FROM `/Root/T/idx/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];[100]];[[2];[42]]])", StreamResultToYson(it));
    }

    // REPLACE existing row omitting indexed column + RETURNING s (should get DEFAULT)
    {
        auto it = client.StreamExecuteQuery(
            "REPLACE INTO `/Root/T` (pk, v) VALUES (1, 300) RETURNING pk, s, v;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[1];[42];[300]]])", StreamResultToYson(it));
    }
    // Verify index table still has DEFAULT value
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s FROM `/Root/T/idx/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];[42]];[[2];[42]]])", StreamResultToYson(it));
    }
}

template <bool NewRbo, bool StreamIndex>
void UpdateSetIndexedColumnToDefaultWithReturning() {
    auto settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(StreamIndex);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo && !StreamIndex);

    TKikimrRunner kikimr(settings);

    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const TString createQuery = R"(
        CREATE TABLE `/Root/T` (
            pk Int64,
            s Int64 DEFAULT 42,
            v Int64,
            PRIMARY KEY (pk),
            INDEX idx GLOBAL SYNC ON (s)
        );
    )";
    auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

    auto client = kikimr.GetQueryClient();

    // Insert with explicit s=99
    {
        auto it = client.ExecuteQuery(
            "UPSERT INTO `/Root/T` (pk, s, v) VALUES (1, 99, 100);",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        if constexpr (NewRbo && !StreamIndex) {
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "Compilation with the new RBO failed");
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "New RBO requires sink-maintained indexes");
        }
    }

    // UPDATE SET s to explicit DEFAULT value + RETURNING
    {
        auto it = client.ExecuteQuery(
            "UPDATE `/Root/T` SET s = 42 WHERE pk = 1 RETURNING pk, s, v;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[[1];[42];[100]]])", FormatResultSetYson(it.GetResultSet(0)));
    }
    // Verify main table
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s, v FROM `/Root/T` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];[42];[100]]])", StreamResultToYson(it));
    }
    // Verify index table reflects DEFAULT
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s FROM `/Root/T/idx/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];[42]]])", StreamResultToYson(it));
    }
}

} // namespace NKikimr::NKqp::NIndexReturningTests
