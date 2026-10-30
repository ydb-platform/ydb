#include <ydb/core/kqp/ut/rbo/effects/kqp_effects_helpers.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

// Ported from ut/indexes/kqp_stream_indexes_ut.cpp. Both variants use sink-maintained indexes.
Y_UNIT_TEST_SUITE(KqpRboIndexEffects) {

    Y_UNIT_TEST_TWIN(UpsertUniqIndexInternalConflictIsAtomic, NewRbo) {
        auto settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

        TKikimrRunner kikimr(settings);

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/T` (
                pk Int64,
                u Int64,
                payload Int64,
                PRIMARY KEY (pk),
                INDEX idx GLOBAL UNIQUE SYNC ON (u)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        {
            auto it = client.ExecuteQuery(R"(
                UPSERT INTO `/Root/T` (pk, u, payload) VALUES
                    (1, 100, 10),
                    (2, 200, 20);
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }

        // The first row updates an existing primary key, while the second row is new.
        // Their duplicate unique key must reject the whole statement, including the update.
        {
            auto it = client.ExecuteQuery(R"(
                UPSERT INTO `/Root/T` (pk, u, payload) VALUES
                    (1, 300, 11),
                    (3, 300, 30);
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::PRECONDITION_FAILED, it.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(it.GetIssues().ToString(), "Duplicated keys found.");
        }

        {
            auto it = client.StreamExecuteQuery(
                "SELECT payload, pk, u FROM `/Root/T` ORDER BY pk;",
                TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(R"([[[10];[1];[100]];[[20];[2];[200]]])", StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteQuery(
                "SELECT pk, u FROM `/Root/T/idx/indexImplTable` ORDER BY u;",
                TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(R"([[[1];[100]];[[2];[200]]])", StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST_TWIN(UpdateUniqIndexConflictIsAtomic, NewRbo) {
        auto settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

        TKikimrRunner kikimr(settings);

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/T` (
                pk Int64,
                u Int64,
                payload Int64,
                PRIMARY KEY (pk),
                INDEX idx GLOBAL UNIQUE SYNC ON (u)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        {
            auto it = client.ExecuteQuery(R"(
                UPSERT INTO `/Root/T` (pk, u, payload) VALUES
                    (1, 100, 10),
                    (2, 200, 20),
                    (3, 300, 30);
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }

        // Both selected rows would acquire the same unique key. No selected row may
        // be updated when validation rejects the statement.
        {
            auto it = client.ExecuteQuery(R"(
                UPDATE `/Root/T` SET u = 400, payload = payload + 1 WHERE pk <= 2;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::PRECONDITION_FAILED, it.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(
                "SELECT payload, pk, u FROM `/Root/T` ORDER BY pk;",
                TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(R"([[[10];[1];[100]];[[20];[2];[200]];[[30];[3];[300]]])", StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteQuery(
                "SELECT pk, u FROM `/Root/T/idx/indexImplTable` ORDER BY u;",
                TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(R"([[[1];[100]];[[2];[200]];[[3];[300]]])", StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST_TWIN(ConditionalUpdateConcurrentDelete, NewRbo) {
        auto settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

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
        }

        auto session1 = client.GetSession().GetValueSync().GetSession();
        auto session2 = client.GetSession().GetValueSync().GetSession();

        auto r1 = session1.ExecuteQuery(R"(
            PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
            UPDATE `/Root/T` SET v = 200 WHERE pk = 1;
        )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SnapshotRW())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(r1.GetStatus(), EStatus::SUCCESS, r1.GetIssues().ToString());
        auto tx1 = r1.GetTransaction();
        UNIT_ASSERT(tx1);

        auto r2 = session2.ExecuteQuery(R"(
            DELETE FROM `/Root/T` WHERE pk = 1;
        )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(r2.GetStatus(), EStatus::SUCCESS, r2.GetIssues().ToString());

        auto c1 = session1.ExecuteQuery(R"(
            SELECT * FROM `/Root/T`;
        )", NYdb::NQuery::TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(c1.GetStatus(), EStatus::ABORTED, c1.GetIssues().ToString());

        {
            auto it = client.StreamExecuteQuery(
                "SELECT * FROM `/Root/T` ORDER BY pk;",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(R"([])", StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteQuery(
                "SELECT * FROM `/Root/T/idx/indexImplTable` ORDER BY pk;",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(R"([])", StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST_TWIN(ConcurrentTransactionsConflictOnUniqueKey, NewRbo) {
        auto settings = TKikimrSettings(NewRboAppConfig()).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

        TKikimrRunner kikimr(settings);

        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto createResult = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/T` (
                pk Int64,
                u Int64,
                payload Utf8,
                PRIMARY KEY (pk),
                INDEX uniq GLOBAL UNIQUE SYNC ON (u)
            ) WITH (
                PARTITION_AT_KEYS = (0)
            );
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        auto session1 = client.GetSession().GetValueSync().GetSession();
        auto session2 = client.GetSession().GetValueSync().GetSession();

        auto first = session1.ExecuteQuery(R"(
            PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
            INSERT INTO `/Root/T` (pk, u, payload) VALUES (-1, 42, "first-aborted"u);
        )", TTxControl::BeginTx(TTxSettings::SnapshotRW())).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(first.GetStatus(), EStatus::SUCCESS, first.GetIssues().ToString());
        auto tx1 = first.GetTransaction();
        UNIT_ASSERT_C(tx1 && tx1->IsActive(), "the first transaction must remain open until validation");

        auto second = session2.ExecuteQuery(R"(
            PRAGMA kikimr.KqpForceImmediateEffectsExecution="true";
            INSERT INTO `/Root/T` (pk, u, payload) VALUES (1, 42, "second-winner"u);
        )", TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(second.GetStatus(), EStatus::SUCCESS, second.GetIssues().ToString());

        auto commit = tx1->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::ABORTED, commit.GetIssues().ToString());

        auto main = client.StreamExecuteQuery(R"(
            SELECT pk, u, payload FROM `/Root/T` ORDER BY pk;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(main.GetStatus(), EStatus::SUCCESS, main.GetIssues().ToString());
        CompareYson(R"([[[1];[42];["second-winner"]]])", StreamResultToYson(main));

        auto index = client.StreamExecuteQuery(R"(
            SELECT u, pk FROM `/Root/T/uniq/indexImplTable` ORDER BY u, pk;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(index.GetStatus(), EStatus::SUCCESS, index.GetIssues().ToString());
        CompareYson(R"([[[42];[1]]])", StreamResultToYson(index));
    }

} // suite

} // namespace NKikimr::NKqp
