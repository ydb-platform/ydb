#pragma once

#include <ydb/core/kqp/ut/rbo/effects/kqp_effects_helpers.h>

namespace NKikimr::NKqp::NIndexWritesTests {

using namespace NYdb;
using namespace NYdb::NQuery;

// Ported from ut/indexes/kqp_stream_indexes_ut.cpp.
template <bool NewRbo, bool StreamIndex>
void UpsertUniqIndexNullKeys() {
    auto settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(StreamIndex);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo && !StreamIndex);

    TKikimrRunner kikimr(settings);

    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const TString createQuery = R"(
        CREATE TABLE `/Root/T` (
            pk Int64,
            u Int64,
            PRIMARY KEY (pk),
            INDEX idx GLOBAL UNIQUE SYNC ON (u)
        );
    )";
    auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

    auto client = kikimr.GetQueryClient();

    {
        auto it = client.ExecuteQuery(R"(
            UPSERT INTO `/Root/T` (pk, u) VALUES
                (1, NULL),
                (2, NULL),
                (3, 100),
                (4, NULL);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        if constexpr (NewRbo && !StreamIndex) {
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "Compilation with the new RBO failed");
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "New RBO requires sink-maintained indexes");
        }
    }

    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, u FROM `/Root/T` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];#];[[2];#];[[3];[100]];[[4];#]])", StreamResultToYson(it));
    }

    {
        auto it = client.ExecuteQuery(
            "UPSERT INTO `/Root/T` (pk, u) VALUES (5, NULL);",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    }

    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, u FROM `/Root/T` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];#];[[2];#];[[3];[100]];[[4];#];[[5];#]])", StreamResultToYson(it));
    }

    {
        auto it = client.ExecuteQuery(
            "UPSERT INTO `/Root/T` (pk, u) VALUES (6, 100);",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::PRECONDITION_FAILED, it.GetIssues().ToString());
    }
}

template <bool NewRbo, bool StreamIndex>
void UpsertUniqIndexCompositePartialNull() {
    auto settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(StreamIndex);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo && !StreamIndex);

    TKikimrRunner kikimr(settings);

    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const TString createQuery = R"(
        CREATE TABLE `/Root/T` (
            pk Int64,
            a Int64,
            b Int64,
            PRIMARY KEY (pk),
            INDEX idx GLOBAL UNIQUE SYNC ON (a, b)
        );
    )";
    auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

    auto client = kikimr.GetQueryClient();

    {
        auto it = client.ExecuteQuery(R"(
            UPSERT INTO `/Root/T` (pk, a, b) VALUES
                (1, 1, NULL),
                (2, 1, NULL),
                (3, NULL, 1),
                (4, NULL, 1),
                (5, 7, 8);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        if constexpr (NewRbo && !StreamIndex) {
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "Compilation with the new RBO failed");
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "New RBO requires sink-maintained indexes");
        }
    }

    {
        auto it = client.StreamExecuteQuery(
            "SELECT a, b, pk FROM `/Root/T` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(
            R"([[[1];#;[1]];[[1];#;[2]];[#;[1];[3]];[#;[1];[4]];[[7];[8];[5]]])",
            StreamResultToYson(it));
    }

    {
        auto it = client.ExecuteQuery(
            "UPSERT INTO `/Root/T` (pk, a, b) VALUES (6, 7, 8);",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::PRECONDITION_FAILED, it.GetIssues().ToString());
    }
}

template <bool NewRbo, bool StreamIndex>
void ReplaceMultipleIndexesEquivalence() {
    auto settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(StreamIndex);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo && !StreamIndex);

    TKikimrRunner kikimr(settings);

    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const TString createQuery = R"(
        CREATE TABLE `/Root/T` (
            pk Int64,
            s1 Int64,
            s2 Int64,
            u Int64,
            data Int64,
            PRIMARY KEY (pk),
            INDEX i1 GLOBAL SYNC ON (s1),
            INDEX i2 GLOBAL SYNC ON (s2) COVER (data),
            INDEX iu GLOBAL UNIQUE SYNC ON (u)
        );
    )";
    auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

    auto client = kikimr.GetQueryClient();

    {
        auto it = client.ExecuteQuery(R"(
            REPLACE INTO `/Root/T` (pk, s1, s2, u, data) VALUES
                (1, 10, 20, 1000, 111),
                (2, 11, 21, 1001, 222);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        if constexpr (NewRbo && !StreamIndex) {
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "Compilation with the new RBO failed");
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "New RBO requires sink-maintained indexes");
        }
    }
    {
        auto it = client.ExecuteQuery(R"(
            REPLACE INTO `/Root/T` (pk, s1, s2, u, data) VALUES
                (1, 99, 98, 9000, 999),
                (3, 12, 22, 1002, 333);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    }

    const TString mainExpected =
        R"([[[999];[1];[99];[98];[9000]];[[222];[2];[11];[21];[1001]];[[333];[3];[12];[22];[1002]]])";

    {
        auto it = client.StreamExecuteQuery(
            "SELECT data, pk, s1, s2, u FROM `/Root/T` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(mainExpected, StreamResultToYson(it));
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT data, pk, s2 FROM `/Root/T/i2/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(
            R"([[[999];[1];[98]];[[222];[2];[21]];[[333];[3];[22]]])",
            StreamResultToYson(it));
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, u FROM `/Root/T/iu/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(
            R"([[[1];[9000]];[[2];[1001]];[[3];[1002]]])",
            StreamResultToYson(it));
    }

    const TString createQuery2 = R"(
        CREATE TABLE `/Root/T2` (
            pk Int64,
            s1 Int64,
            s2 Int64,
            u Int64,
            data Int64,
            PRIMARY KEY (pk),
            INDEX i1 GLOBAL SYNC ON (s1),
            INDEX i2 GLOBAL SYNC ON (s2) COVER (data),
            INDEX iu GLOBAL UNIQUE SYNC ON (u)
        );
    )";
    UNIT_ASSERT_C(session.ExecuteSchemeQuery(createQuery2).GetValueSync().GetStatus() == NYdb::EStatus::SUCCESS, "create T2");
    {
        auto it = client.ExecuteQuery(R"(
            UPSERT INTO `/Root/T2` (pk, s1, s2, u, data) VALUES
                (1, 99, 98, 9000, 999),
                (2, 11, 21, 1001, 222),
                (3, 12, 22, 1002, 333);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT data, pk, s1, s2, u FROM `/Root/T2` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(mainExpected, StreamResultToYson(it));
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT data, pk, s2 FROM `/Root/T2/i2/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(
            R"([[[999];[1];[98]];[[222];[2];[21]];[[333];[3];[22]]])",
            StreamResultToYson(it));
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, u FROM `/Root/T2/iu/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(
            R"([[[1];[9000]];[[2];[1001]];[[3];[1002]]])",
            StreamResultToYson(it));
    }
}

template <bool NewRbo, bool StreamIndex>
void UpsertPartialColumnsWithCoveringIndex() {
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
            data Int64,
            PRIMARY KEY (pk),
            INDEX idx GLOBAL SYNC ON (s) COVER (data)
        );
    )";
    auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

    auto client = kikimr.GetQueryClient();

    {
        auto it = client.ExecuteQuery(
            "UPSERT INTO `/Root/T` (pk, s, data) VALUES (1, 10, 100);",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        if constexpr (NewRbo && !StreamIndex) {
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "Compilation with the new RBO failed");
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "New RBO requires sink-maintained indexes");
        }
    }
    {
        auto it = client.ExecuteQuery(
            "UPSERT INTO `/Root/T` (pk, data) VALUES (1, 200);",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    }

    {
        auto it = client.StreamExecuteQuery(
            "SELECT data, pk, s FROM `/Root/T` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[200];[1];[10]]])", StreamResultToYson(it));
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT data, pk, s FROM `/Root/T/idx/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[200];[1];[10]]])", StreamResultToYson(it));
    }

    {
        auto it = client.ExecuteQuery(
            "UPSERT INTO `/Root/T` (pk, s) VALUES (1, 11);",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT data, pk, s FROM `/Root/T` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[200];[1];[11]]])", StreamResultToYson(it));
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT data, pk, s FROM `/Root/T/idx/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[200];[1];[11]]])", StreamResultToYson(it));
    }
}

template <bool NewRbo, bool StreamIndex>
void UpsertDefaultColumnWithCoveringIndex() {
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
            data Int64 NOT NULL DEFAULT 42,
            PRIMARY KEY (pk),
            INDEX idx GLOBAL SYNC ON (s) COVER (data)
        );
    )";
    auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

    auto client = kikimr.GetQueryClient();

    {
        auto it = client.ExecuteQuery(
            "INSERT INTO `/Root/T` (pk, s) VALUES (1, 10);",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        if constexpr (NewRbo && !StreamIndex) {
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "Compilation with the new RBO failed");
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "New RBO requires sink-maintained indexes");
        }
    }
    {
        auto it = client.ExecuteQuery(
            "UPSERT INTO `/Root/T` (pk, s) VALUES (2, 20);",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    }

    {
        auto it = client.StreamExecuteQuery(
            "SELECT data, pk, s FROM `/Root/T` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[42;[1];[10]];[42;[2];[20]]])", StreamResultToYson(it));
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT data, pk, s FROM `/Root/T/idx/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[42;[1];[10]];[42;[2];[20]]])", StreamResultToYson(it));
    }
}

template <bool NewRbo, bool StreamIndex>
void ConditionalUpdateIndexKeyChangeAndNoChange() {
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
            tag Int64,
            PRIMARY KEY (pk),
            INDEX idx GLOBAL SYNC ON (s)
        );
    )";
    auto result = session.ExecuteSchemeQuery(createQuery).GetValueSync();
    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

    auto client = kikimr.GetQueryClient();

    {
        auto it = client.ExecuteQuery(R"(
            UPSERT INTO `/Root/T` (pk, s, tag) VALUES
                (1, 10, 0),
                (2, 20, 0),
                (3, 30, 1);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        if constexpr (NewRbo && !StreamIndex) {
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "Compilation with the new RBO failed");
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "New RBO requires sink-maintained indexes");
        }
    }
    {
        auto it = client.ExecuteQuery(
            "UPDATE `/Root/T` SET s = s + 100 WHERE tag = 0;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    }

    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s, tag FROM `/Root/T` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];[110];[0]];[[2];[120];[0]];[[3];[30];[1]]])", StreamResultToYson(it));
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s FROM `/Root/T/idx/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];[110]];[[2];[120]];[[3];[30]]])", StreamResultToYson(it));
    }
}

template <bool NewRbo, bool StreamIndex>
void UpdateIndexedColumnToNullAndBack() {
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

    // UPDATE indexed column to NULL
    {
        auto it = client.ExecuteQuery(
            "UPDATE `/Root/T` SET s = NULL WHERE pk = 1;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s, v FROM `/Root/T` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];#;[100]]])", StreamResultToYson(it));
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s FROM `/Root/T/idx/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];#]])", StreamResultToYson(it));
    }

    // UPDATE indexed column back from NULL to a value
    {
        auto it = client.ExecuteQuery(
            "UPDATE `/Root/T` SET s = 20 WHERE pk = 1;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s, v FROM `/Root/T` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];[20];[100]]])", StreamResultToYson(it));
    }
    {
        auto it = client.StreamExecuteQuery(
            "SELECT pk, s FROM `/Root/T/idx/indexImplTable` ORDER BY pk;",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        CompareYson(R"([[[1];[20]]])", StreamResultToYson(it));
    }
}

} // namespace NKikimr::NKqp::NIndexWritesTests
