#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NTopic;

namespace {

TVector<NJson::TJsonValue> ReadCdcMessages(TKikimrRunner& kikimr, const TString& topicPath, size_t expectedCount) {
    TTopicClient topicClient(kikimr.GetDriver());

    TReadSessionSettings readSettings;
    readSettings.ConsumerName("test_consumer");
    TTopicReadSettings topicReadSettings;
    topicReadSettings.Path(topicPath);
    readSettings.AppendTopics(topicReadSettings);

    auto readSession = topicClient.CreateReadSession(readSettings);

    TVector<NJson::TJsonValue> result;
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);

    while (result.size() < expectedCount && TInstant::Now() < deadline) {
        const TDuration remain = deadline - TInstant::Now();
        if (!readSession->WaitEvent().Wait(remain)) {
            break;
        }

        for (auto& ev : readSession->GetEvents(false)) {
            if (auto* startEvent = std::get_if<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&ev)) {
                startEvent->Confirm();
            } else if (auto* stopEvent = std::get_if<NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&ev)) {
                stopEvent->Confirm();
            } else if (auto* data = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&ev)) {
                for (const auto& m : data->GetMessages()) {
                    NJson::TJsonValue json;
                    if (NJson::ReadJsonTree(m.GetData(), &json)) {
                        result.push_back(json);
                    }
                }
                data->Commit();
            } else if (std::get_if<NTopic::TSessionClosedEvent>(&ev)) {
                return result;
            }
        }
    }

    return result;
}

void ExecDdl(NQuery::TSession& session, const std::string& query, const char* what) {
    auto result = session.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
        what << ": " << result.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(KqpTxCommitTimestampCdc) {
    Y_UNIT_TEST(SingleShardWrite) {
        TKikimrSettings settings;
        settings.SetEnableStrictSerializableIsolation(true);
        settings.SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        ExecDdl(session, R"(
            CREATE TABLE `/Root/CdcTest` (
                Key Uint64,
                Value Text,
                PRIMARY KEY (Key)
            );
        )", "CREATE TABLE");

        ExecDdl(session, R"(
            ALTER TABLE `/Root/CdcTest` ADD CHANGEFEED `feed` WITH (
                MODE = 'UPDATES', FORMAT = 'JSON', VIRTUAL_TIMESTAMPS = TRUE
            );
        )", "ADD CHANGEFEED");

        ExecDdl(session, R"(
            ALTER TOPIC `/Root/CdcTest/feed` ADD CONSUMER `test_consumer`;
        )", "ADD CONSUMER");

        auto result = session.ExecuteQuery(R"(
            UPSERT INTO `/Root/CdcTest` (Key, Value) VALUES (1u, "hello");
        )", TTxControl::BeginTx(TTxSettings::StrictSerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetCommitTimestamp().has_value(), "Commit timestamp should be present");
        const auto& commitTs = *result.GetCommitTimestamp();
        UNIT_ASSERT_C(commitTs.PlanStep > 0, "PlanStep should be nonzero");
        UNIT_ASSERT_C(commitTs.TxId > 0, "TxId should be nonzero");

        auto records = ReadCdcMessages(kikimr, "/Root/CdcTest/feed", 1);
        UNIT_ASSERT_VALUES_EQUAL(records.size(), 1);
        UNIT_ASSERT_C(records[0].Has("ts"), "CDC record should have 'ts' field");
        UNIT_ASSERT_VALUES_EQUAL(records[0]["ts"][0].GetUInteger(), commitTs.PlanStep);
        UNIT_ASSERT_VALUES_EQUAL(records[0]["ts"][1].GetUInteger(), commitTs.TxId);
    }

    Y_UNIT_TEST(ThreeInsertsOneShard) {
        TKikimrSettings settings;
        settings.SetEnableStrictSerializableIsolation(true);
        settings.SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        ExecDdl(session, R"(
            CREATE TABLE `/Root/CdcMultiInsert` (
                Key Uint64,
                Value Text,
                PRIMARY KEY (Key)
            ) WITH (UNIFORM_PARTITIONS = 1);
        )", "CREATE TABLE");

        ExecDdl(session, R"(
            ALTER TABLE `/Root/CdcMultiInsert` ADD CHANGEFEED `feed` WITH (
                MODE = 'UPDATES', FORMAT = 'JSON', VIRTUAL_TIMESTAMPS = TRUE
            );
        )", "ADD CHANGEFEED");

        ExecDdl(session, R"(
            ALTER TOPIC `/Root/CdcMultiInsert/feed` ADD CONSUMER `test_consumer`;
        )", "ADD CONSUMER");

        auto beginResult = session.BeginTransaction(TTxSettings::StrictSerializableRW()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(beginResult.GetStatus(), EStatus::SUCCESS, beginResult.GetIssues().ToString());
        auto tx = beginResult.GetTransaction();

        for (ui64 i = 1; i <= 3; ++i) {
            TString query = TStringBuilder()
                << "INSERT INTO `/Root/CdcMultiInsert` (Key, Value) VALUES ("
                << i << "u, \"val" << i << "\");";
            auto execResult = session.ExecuteQuery(query, TTxControl::Tx(tx)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(execResult.GetStatus(), EStatus::SUCCESS, execResult.GetIssues().ToString());
        }

        auto commitResult = tx.Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
        UNIT_ASSERT_C(commitResult.GetCommitTimestamp().has_value(), "Commit timestamp should be present");
        const auto& commitTs = *commitResult.GetCommitTimestamp();
        UNIT_ASSERT_C(commitTs.PlanStep > 0, "PlanStep should be nonzero");
        UNIT_ASSERT_C(commitTs.TxId > 0, "TxId should be nonzero");

        auto records = ReadCdcMessages(kikimr, "/Root/CdcMultiInsert/feed", 3);
        UNIT_ASSERT_VALUES_EQUAL(records.size(), 3);

        for (size_t i = 0; i < records.size(); ++i) {
            UNIT_ASSERT_C(records[i].Has("ts"), TStringBuilder() << "CDC record " << i << " should have 'ts' field");
            UNIT_ASSERT_VALUES_EQUAL_C(
                records[i]["ts"][0].GetUInteger(), commitTs.PlanStep,
                TStringBuilder() << "CDC record " << i << " PlanStep mismatch");
            UNIT_ASSERT_VALUES_EQUAL_C(
                records[i]["ts"][1].GetUInteger(), commitTs.TxId,
                TStringBuilder() << "CDC record " << i << " TxId mismatch");
        }
    }

    Y_UNIT_TEST(ThreeUpsertsMultiShard) {
        TKikimrSettings settings;
        settings.SetEnableStrictSerializableIsolation(true);
        settings.SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        ExecDdl(session, R"(
            CREATE TABLE `/Root/CdcMultiShard` (
                Key Uint64,
                Value Text,
                PRIMARY KEY (Key)
            ) WITH (UNIFORM_PARTITIONS = 4);
        )", "CREATE TABLE");

        ExecDdl(session, R"(
            ALTER TABLE `/Root/CdcMultiShard` ADD CHANGEFEED `feed` WITH (
                MODE = 'UPDATES', FORMAT = 'JSON', VIRTUAL_TIMESTAMPS = TRUE
            );
        )", "ADD CHANGEFEED");

        ExecDdl(session, R"(
            ALTER TOPIC `/Root/CdcMultiShard/feed` ADD CONSUMER `test_consumer`;
        )", "ADD CONSUMER");

        auto beginResult = session.BeginTransaction(TTxSettings::StrictSerializableRW()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(beginResult.GetStatus(), EStatus::SUCCESS, beginResult.GetIssues().ToString());
        auto tx = beginResult.GetTransaction();

        const ui64 keys[] = {1, 1000000, 2000000};
        for (ui64 i = 0; i < 3; ++i) {
            TString query = TStringBuilder()
                << "UPSERT INTO `/Root/CdcMultiShard` (Key, Value) VALUES ("
                << keys[i] << "u, \"val" << (i + 1) << "\");";
            auto execResult = session.ExecuteQuery(query, TTxControl::Tx(tx)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(execResult.GetStatus(), EStatus::SUCCESS, execResult.GetIssues().ToString());
        }

        auto commitResult = tx.Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitResult.GetStatus(), EStatus::SUCCESS, commitResult.GetIssues().ToString());
        UNIT_ASSERT_C(commitResult.GetCommitTimestamp().has_value(), "Commit timestamp should be present");
        const auto& commitTs = *commitResult.GetCommitTimestamp();
        UNIT_ASSERT_C(commitTs.PlanStep > 0, "PlanStep should be nonzero");
        UNIT_ASSERT_C(commitTs.TxId > 0, "TxId should be nonzero");

        auto records = ReadCdcMessages(kikimr, "/Root/CdcMultiShard/feed", 3);
        UNIT_ASSERT_VALUES_EQUAL(records.size(), 3);

        for (size_t i = 0; i < records.size(); ++i) {
            UNIT_ASSERT_C(records[i].Has("ts"), TStringBuilder() << "CDC record " << i << " should have 'ts' field");
            UNIT_ASSERT_VALUES_EQUAL_C(
                records[i]["ts"][0].GetUInteger(), commitTs.PlanStep,
                TStringBuilder() << "CDC record " << i << " PlanStep mismatch");
            UNIT_ASSERT_VALUES_EQUAL_C(
                records[i]["ts"][1].GetUInteger(), commitTs.TxId,
                TStringBuilder() << "CDC record " << i << " TxId mismatch");
        }
    }
}

} // namespace NKikimr::NKqp
