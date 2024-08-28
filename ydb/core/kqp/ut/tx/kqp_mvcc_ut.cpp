#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpSnapshotRead) {
    Y_UNIT_TEST_TWIN(TestSnapshotExpiration, withSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(withSink);
        auto settings = TKikimrSettings()
            .SetKeepSnapshotTimeout(TDuration::Seconds(1))
            .SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);

//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_BLOBS_STORAGE, NActors::NLog::PRI_DEBUG);
//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1u OR Key = 4000000001u ORDER BY Key;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["One"];[-1]];
            [[4000000001u];["BigOne"];[-1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        auto tx = result.GetTransaction();

        result = session2.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES (1u, "ChangedOne", 1);
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto deadline = TInstant::Now() + TDuration::Seconds(30);
        auto caught = false;
        do {
            Sleep(TDuration::Seconds(1));
            auto result = session1.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/TwoShard` WHERE Key = 1u OR Key = 4000000001u;
            )"), TTxControl::Tx(*tx)).ExtractValueSync();
            if (result.GetStatus() == EStatus::SUCCESS)
                continue;

            UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR,
                [](const NYql::TIssue& issue){
                    return issue.GetMessage().Contains("has no snapshot at");
                }), result.GetIssues().ToString());

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::ABORTED);

            caught = true;
            break;
        } while (TInstant::Now() < deadline);
        UNIT_ASSERT_C(caught, "Failed to wait for snapshot expiration.");
    }

    Y_UNIT_TEST_TWIN(ReadOnlyTxCommitsOnConcurrentWrite, withSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(withSink);
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(true);
        TKikimrRunner kikimr(TKikimrSettings()
            .SetAppConfig(appConfig)
        );

//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_BLOBS_STORAGE, NActors::NLog::PRI_DEBUG);
//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1u OR Key = 4000000001u ORDER BY Key;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();

        auto tx = result.GetTransaction();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["One"];[-1]];
            [[4000000001u];["BigOne"];[-1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        result = session2.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES (1u, "ChangedOne", 1);
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session2.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1u;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["ChangedOne"];[1]];
        ])", FormatResultSetYson(result.GetResultSet(0)));

        result = session1.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1u;
        )"), TTxControl::Tx(*tx)).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["One"];[-1]];
        ])", FormatResultSetYson(result.GetResultSet(0)));

        result = session1.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 2u OR Key = 4000000002u ORDER BY Key;
        )"), TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
            [[2u];["Two"];[0]];
            [[4000000002u];["BigTwo"];[0]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST_TWIN(ReadOnlyTxWithIndexCommitsOnConcurrentWrite, withSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(withSink);
        TKikimrRunner kikimr(
            TKikimrSettings()
                .SetAppConfig(appConfig)
        );

//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_BLOBS_STORAGE, NActors::NLog::PRI_DEBUG);
//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTablesWithIndex(session1);

        auto result = session1.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/SecondaryWithDataColumns` (Key, Index2, Value) VALUES
                ("Pk1",    "Fk1",    "One"),
                ("Pk2",    "Fk2",    "Two"),
                ("Pk3",    "Fk3",    "Three"),
                ("Pk4",    "Fk4",    "Four"),
                ("Pk5",    "Fk5",    "Five"),
                ("Pk6",    "Fk6",    "Six");
            )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session1.ExecuteDataQuery(Q1_(R"(
            SELECT Value FROM `/Root/SecondaryWithDataColumns` view Index WHERE Index2 = "Fk1";
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();

        auto tx = result.GetTransaction();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["One"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        result = session2.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/SecondaryWithDataColumns` (Key, Index2, Value) VALUES ("Pk1", "Fk1", "ChangedOne");
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session2.ExecuteDataQuery(Q1_(R"(
            SELECT Value FROM `/Root/SecondaryWithDataColumns` view Index WHERE Index2 = "Fk1";
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["ChangedOne"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        result = session1.ExecuteDataQuery(Q1_(R"(
            SELECT Value FROM `/Root/SecondaryWithDataColumns` view Index WHERE Index2 = "Fk1";
        )"), TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [["One"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST_TWIN(ReadWriteTxFailsOnConcurrentWrite1, withSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(withSink);
        TKikimrRunner kikimr(
            TKikimrSettings()
                .SetAppConfig(appConfig)
        );

//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_BLOBS_STORAGE, NActors::NLog::PRI_DEBUG);
//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1u OR Key = 4000000001u ORDER BY Key;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();

        auto tx = result.GetTransaction();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["One"];[-1]];
            [[4000000001u];["BigOne"];[-1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        result = session2.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES (1u, "ChangedOne", 1);
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session1.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES (1u, "TwiceChangedOne", 2);
        )"), TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED), result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(ReadWriteTxFailsOnConcurrentWrite2, withSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(withSink);
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(withSink);
        TKikimrRunner kikimr(
            TKikimrSettings()
                .SetAppConfig(appConfig)
        );

//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_BLOBS_STORAGE, NActors::NLog::PRI_DEBUG);
//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1u OR Key = 4000000001u ORDER BY Key;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();

        auto tx = result.GetTransaction();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["One"];[-1]];
            [[4000000001u];["BigOne"];[-1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        // We need to sleep before the upsert below, otherwise writes
        // might happen in the same step as the snapshot, which would be
        // treated as happening before snapshot and will not break any locks.
        Sleep(TDuration::Seconds(2));

        result = session2.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/EightShard` (Key, Text) VALUES (101u, "SomeText");
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session1.ExecuteDataQuery(Q_(R"(
            UPDATE `/Root/TwoShard` SET Value2 = 2 WHERE Key = 1u;
            UPDATE `/Root/EightShard` SET Text = "AnotherString" WHERE Key = 101u;
        )"), TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED), result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(ReadWriteTxFailsOnConcurrentWrite3, withSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(true);
        TKikimrRunner kikimr(
            TKikimrSettings()
                .SetAppConfig(appConfig)
        );

//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_BLOBS_STORAGE, NActors::NLog::PRI_DEBUG);
//        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        auto db = kikimr.GetTableClient();
        auto session1 = db.CreateSession().GetValueSync().GetSession();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session1.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 1u OR Key = 4000000001u ORDER BY Key;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();

        auto tx = result.GetTransaction();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([
            [[1u];["One"];[-1]];
            [[4000000001u];["BigOne"];[-1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        result = session2.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES (2u, "ChangedTwo", 1);
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session1.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key = 2u OR Key = 4000000002u ORDER BY Key;
        )"), TTxControl::Tx(*tx)).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([
            [[2u];["Two"];[0]];
            [[4000000002u];["BigTwo"];[0]]
        ])", FormatResultSetYson(result.GetResultSet(0)));

        result = session1.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES (2u, "TwiceChangedTwo", 1);
        )"), TTxControl::Tx(*tx).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        UNIT_ASSERT_C(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED), result.GetIssues().ToString());
    }
}

} // namespace NKqp
} // namespace NKikimr
