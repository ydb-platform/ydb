#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr::NKqp {
using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(GeneratedStoredAddColumnCdc) {
    Y_UNIT_TEST(ExistingIndexAndChangefeedDuringBackfill) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableGeneratedStored(true);
        config.MutableFeatureFlags()->SetEnableAddGeneratedStoredColumn(true);
        config.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);

        NKikimrPQ::TPQConfig pqConfig;
        pqConfig.SetEnabled(true);
        pqConfig.SetEnableProtoSourceIdInfo(true);
        pqConfig.SetTopicsAreFirstClassCitizen(true);
        pqConfig.SetRequireCredentialsInNewProtocol(false);
        pqConfig.AddClientServiceType()->SetName("data-streams");
        TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false)
            .SetUseRealThreads(false).SetPQConfig(pqConfig));
        auto client = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto exec = [&](const std::string& sql) {
            auto result = kikimr.RunCall([&] {
                return client.ExecuteQuery(sql, TTxControl::NoTx()).GetValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), sql << "\n" << result.GetIssues().ToString());
            return result;
        };
        auto check = [&](const std::string& sql, const TString& expected) {
            const auto result = exec(sql);
            CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
        };

        exec(R"(
            CREATE TABLE t (
                k Int64 NOT NULL,
                a Int64 NOT NULL,
                PRIMARY KEY (k),
                INDEX by_a GLOBAL SYNC ON (a)
            );
        )");
        // Seed before adding the stream: only the two foreground updates below
        // should produce CDC data records. Both rows still require backfill.
        exec("UPSERT INTO t (k, a) VALUES (1, 3), (2, 7);");
        exec("ALTER TABLE t ADD CHANGEFEED feed WITH (MODE = 'NEW_AND_OLD_IMAGES', FORMAT = 'JSON');");
        exec("ALTER TOPIC `/Root/t/feed` ADD CONSUMER test_consumer;");

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        bool capture = true;
        TVector<TAutoPtr<NActors::IEventHandle>> uploads;
        auto uploadObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsRequest>([&](auto& event) {
            if (capture && event->Get()->Record.HasGeneratedColumnFillContext()) {
                uploads.emplace_back(event.Release());
            }
        });
        auto alter = kikimr.RunInThreadPool([&] {
            return client.ExecuteQuery("ALTER TABLE t ADD COLUMN g Int64 GENERATED ALWAYS AS (a + 1) STORED;",
                TTxControl::NoTx()).GetValueSync();
        });
        runtime.WaitFor("generated fill paused before upload", [&] { return !uploads.empty() || alter.HasValue(); });
        UNIT_ASSERT_C(!uploads.empty(), "ALTER finished before backfill upload: "
            << (alter.HasValue() ? alter.GetValueSync().GetIssues().ToString() : std::string()));

        // This update moves an existing index entry and materializes hidden g.
        // Releasing the older snapshot value must preserve that foreground g.
        exec("UPDATE t SET a = 9 WHERE k = 1;");
        check("SELECT k, a FROM t VIEW by_a ORDER BY k;", "[[1;9];[2;7]]");
        check("SELECT k FROM t VIEW by_a WHERE a = 3;", "[]");
        capture = false;
        uploadObserver.Remove();
        for (auto& upload : uploads) {
            runtime.Send(upload);
        }
        const auto completed = runtime.WaitFuture(alter);
        UNIT_ASSERT_C(completed.IsSuccess(), completed.GetIssues().ToString());
        check("SELECT k, a, g FROM t ORDER BY k;", "[[1;9;[10]];[2;7;[8]]]");
        check("SELECT k, a FROM t VIEW by_a ORDER BY k;", "[[1;9];[2;7]]");

        // The second update is an ordered CDC barrier after publication. Any
        // spurious backfill records must precede it in this single-shard stream.
        exec("UPDATE t SET a = 20 WHERE k = 2;");
        check("SELECT k, a, g FROM t ORDER BY k;", "[[1;9;[10]];[2;20;[21]]]");
        check("SELECT k, a FROM t VIEW by_a ORDER BY k;", "[[1;9];[2;20]]");
        check("SELECT k FROM t VIEW by_a WHERE a = 7;", "[]");

        auto topicClient = kikimr.RunCall([&] {
            return NYdb::NTopic::TTopicClient(kikimr.GetDriver());
        });
        NYdb::NTopic::TReadSessionSettings readSettings;
        readSettings.ConsumerName("test_consumer");
        readSettings.AppendTopics(NYdb::NTopic::TTopicReadSettings().Path("/Root/t/feed"));
        auto readSession = kikimr.RunCall([&] { return topicClient.CreateReadSession(readSettings); });
        auto messages = kikimr.RunCall([&] {
            TVector<TString> result;
            bool sawPublishedUpdate = false;
            const auto deadline = TInstant::Now() + TDuration::Seconds(30);
            while (!sawPublishedUpdate && TInstant::Now() < deadline) {
                if (!readSession->WaitEvent().Wait(TDuration::Seconds(1))) {
                    continue;
                }
                for (auto& event : readSession->GetEvents(false)) {
                    if (auto* data = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                        for (auto& message : data->GetMessages()) {
                            result.emplace_back(message.GetData());
                            NJson::TJsonValue json;
                            UNIT_ASSERT_C(NJson::ReadJsonTree(result.back(), &json), result.back());
                            if (json.Has("newImage") && json["newImage"]["a"].GetInteger() == 20) {
                                sawPublishedUpdate = true;
                            }
                        }
                        data->Commit();
                    } else if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
                        start->Confirm();
                    } else if (auto* stop = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&event)) {
                        stop->Confirm();
                    } else if (auto* end = std::get_if<NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent>(&event)) {
                        end->Confirm();
                    } else if (std::get_if<NYdb::NTopic::TSessionClosedEvent>(&event)
                        || std::get_if<NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>(&event)) {
                        UNIT_FAIL("CDC session closed before the post-publication update");
                    }
                }
            }
            UNIT_ASSERT_C(sawPublishedUpdate, "CDC did not deliver the post-publication update");
            return result;
        });
        UNIT_ASSERT_VALUES_EQUAL_C(messages.size(), 2u, JoinSeq("\n", messages));
        for (size_t i = 0; i < messages.size(); ++i) {
            NJson::TJsonValue json;
            UNIT_ASSERT_C(NJson::ReadJsonTree(messages[i], &json), messages[i]);
            UNIT_ASSERT_C(json.Has("update") && json.Has("newImage") && json.Has("oldImage"), messages[i]);
            UNIT_ASSERT_VALUES_EQUAL_C(json["key"][0].GetInteger(), i == 0 ? 1 : 2, messages[i]);
            UNIT_ASSERT_VALUES_EQUAL_C(json["oldImage"]["a"].GetInteger(), i == 0 ? 3 : 7, messages[i]);
            UNIT_ASSERT_VALUES_EQUAL_C(json["newImage"]["a"].GetInteger(), i == 0 ? 9 : 20, messages[i]);
            UNIT_ASSERT_VALUES_EQUAL_C(json["newImage"]["g"].GetInteger(), i == 0 ? 10 : 21, messages[i]);
            if (i == 1) {
                UNIT_ASSERT_VALUES_EQUAL_C(json["oldImage"]["g"].GetInteger(), 8, messages[i]);
            }
        }
        kikimr.RunCall([&] { return readSession->Close(TDuration::Seconds(5)); });
        exec("ALTER TABLE t DROP CHANGEFEED feed;");
    }
}

} // namespace NKikimr::NKqp
