#include "topic_read.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/data_plane_helpers.h>
#include <ydb/services/persqueue_v1/ut/persqueue_test_fixture.h>

namespace NYdb::NConsoleClient {
    class TTopicReaderTests: public NUnitTest::TTestBase {
        using IReadSession = std::shared_ptr<NYdb::NTopic::IReadSession>;

    public:
        UNIT_TEST_SUITE(TTopicReaderTests)
        UNIT_TEST(TestRun_ReadOneMessage);
        UNIT_TEST(TestRun_ReadTwoMessages_With_Limit_1);
        UNIT_TEST(TestRun_ReadMoreMessagesThanLimit_Without_Wait_NewlineDelimited);
        UNIT_TEST(TestRun_ReadMoreMessagesThanLimit_Without_Wait_NoDelimiter);
        UNIT_TEST(TestRun_ReadMessages_Output_Base64);
        UNIT_TEST(TestRun_Read_Less_Messages_Than_Sent);
        UNIT_TEST_SUITE_END();

        void TestRun_ReadOneMessage() {
            RunTest({
                        "some simple message",
                    },
                    {
                        "some simple message",
                    },
                    "", TTopicReaderSettings(Nothing(), false, false, EMessagingFormat::SingleMessage, {}, ETransformBody::None, TDuration::Seconds(1)));
        }

        void TestRun_ReadTwoMessages_With_Limit_1() {
            RunTest({
                        "message1",
                        "message2",
                    },
                    {
                        "message1",
                    },
                    "", TTopicReaderSettings(1, false, false, EMessagingFormat::SingleMessage, {}, ETransformBody::None, TDuration::Seconds(1)));
        }

        void TestRun_ReadMoreMessagesThanLimit_Without_Wait_NewlineDelimited() {
            ui64 limit = 4;
            RunTest(
                {
                    "message1",
                    "message2",
                    "message3",
                },
                {
                    "message1",
                    "message2",
                    "message3",
                },
                "\n", TTopicReaderSettings(limit, false, false, EMessagingFormat::NewlineDelimited, {}, ETransformBody::None, TDuration::Seconds(1)));
        }

        void TestRun_ReadMoreMessagesThanLimit_Without_Wait_NoDelimiter() {
            ui64 limit = 5;
            RunTest(
                {"message1",
                 "message2",
                 "message3",
                 "message4"},
                {
                    "message1message2message3message4",
                },
                "", TTopicReaderSettings(limit, false, false, EMessagingFormat::SingleMessage, {}, ETransformBody::None, TDuration::Seconds(1)));
        }

        void TestRun_ReadMessages_Output_Base64() {
            ui64 limit = 3;
            RunTest(
                {
                    "message1",
                    "message2",
                    "message3",
                },
                {
                    "bWVzc2FnZTE=",
                    "bWVzc2FnZTI=",
                    "bWVzc2FnZTM=",
                },
                "\n", TTopicReaderSettings(limit, false, false, EMessagingFormat::NewlineDelimited, {}, ETransformBody::Base64, TDuration::Seconds(1)));
        }

        void TestRun_Read_Less_Messages_Than_Sent() {
            ui64 limit = 2;
            RunTest(
                {
                    "message1",
                    "message2",
                    "message3",
                },
                {
                    "message1message2",
                },
                "", TTopicReaderSettings(limit, false, false, EMessagingFormat::SingleMessage, {}, ETransformBody::None, TDuration::Seconds(1)));
        }

    private:
        void WriteTestData(NYdb::TDriver* driver, const TString& topicPath, const TVector<TString>& data) {
            auto writer = CreateSimpleWriter(*driver, topicPath, "source1", {}, TString("raw"));
            for (size_t i = 1; i <= data.size(); ++i) {
                UNIT_ASSERT(writer->Write(data[i - 1], i));
            }
            writer->Close();
        }

        void RunTest(
            const TVector<TString>& dataToWrite,
            const TVector<TString>& expected,
            const TString& delimiter,
            TTopicReaderSettings&& settings) {
            Cerr << "=== Starting PQ server\n";
            // NOTE(shmel1k@): old PQ configuration. Waiting for topicservice.
            TPersQueueV1TestServer server;
            Cerr << "=== Started PQ server\n";

            SET_LOCALS;

            const TString topicPath = server.GetTopic();
            auto driver = server.Server->AnnoyingClient->GetDriver();
            server.Server->AnnoyingClient->CreateConsumer("cli");
            NPersQueue::TPersQueueClient persQueueClient(*driver);

            WriteTestData(driver, topicPath, dataToWrite);
            NTopic::TReadSessionSettings readSessionSettings = PrepareReadSessionSettings(topicPath);
            IReadSession sess = CreateTopicReader(*driver, readSessionSettings);
            TTopicReader reader(sess, settings);
            reader.Init();

            TStringStream output;
            int status = reader.Run(output);
            UNIT_ASSERT_EQUAL(status, 0);

            TVector<TString> split;
            Split(output.Str(), delimiter, split);

            UNIT_ASSERT_VALUES_EQUAL(split.size(), expected.size());
            for (size_t i = 0; i < split.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(split[i], expected[i]);
            }
        }

        NTopic::TReadSessionSettings PrepareReadSessionSettings(const TString& topicPath) {
            NTopic::TReadSessionSettings settings;
            settings.ConsumerName("cli");
            settings.AppendTopics(topicPath);

            return settings;
        }

    private:
        IReadSession CreateTopicReader(NYdb::TDriver& driver, NYdb::NTopic::TReadSessionSettings& settings) {
            return NTopic::TTopicClient(driver).CreateReadSession(settings);
        }
    };

    UNIT_TEST_SUITE_REGISTRATION(TTopicReaderTests);
} // namespace NYdb::NConsoleClient
