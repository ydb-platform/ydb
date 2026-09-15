#include "topic_read.h"
#include "topic_metadata_fields.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/data_plane_helpers.h>
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
        UNIT_TEST(TestRun_ReadMessages_With_Offset);
        UNIT_TEST(TestRun_ReadMessages_With_Future_Offset);
        UNIT_TEST(TestRun_ReadMessages_JsonStreamConcat);
        UNIT_TEST(TestRun_ReadMessages_CsvFormat);
        UNIT_TEST(TestRun_ReadMessages_TsvFormat);
        UNIT_TEST(TestRun_ReadMessages_CsvFormat_Unlimited);
        UNIT_TEST_SUITE_END();

        void TestRun_ReadOneMessage() {
            RunTest({
                        "some simple message",
                    },
                    {
                        "some simple message",
                    },
                    "", TTopicReaderSettings(Nothing(), false, false, {}, EMessagingFormat::SingleMessage, {}, ETransformBody::None, TDuration::Seconds(1)));
        }

        void TestRun_ReadTwoMessages_With_Limit_1() {
            RunTest({
                        "message1",
                        "message2",
                    },
                    {
                        "message1",
                    },
                    "", TTopicReaderSettings(1, false, false, {}, EMessagingFormat::SingleMessage, {}, ETransformBody::None, TDuration::Seconds(1)));
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
                "\n", TTopicReaderSettings(limit, false, false, {}, EMessagingFormat::NewlineDelimited, {}, ETransformBody::None, TDuration::Seconds(1)));
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
                "", TTopicReaderSettings(limit, false, false, {}, EMessagingFormat::SingleMessage, {}, ETransformBody::None, TDuration::Seconds(1)));
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
                "\n", TTopicReaderSettings(limit, false, false, {}, EMessagingFormat::NewlineDelimited, {}, ETransformBody::Base64, TDuration::Seconds(1)));
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
                "", TTopicReaderSettings(limit, false, false, {}, EMessagingFormat::SingleMessage, {}, ETransformBody::None, TDuration::Seconds(1)));
        }

        void TestRun_ReadMessages_With_Offset() {
            RunTest({
                        "message1",
                        "message2",
                        "message3",
                        "message4",
                        "message5",
                        "message6",
                    },
                    {
                        "message4",
                        "message5",
                        "message6",

                    },
                    "\n", TTopicReaderSettings({}, false, false, {{0, 3}}, EMessagingFormat::NewlineDelimited, {}, ETransformBody::None, TDuration::Seconds(1)));
        }

         void TestRun_ReadMessages_With_Future_Offset() {
            RunTest({
                        "message1",
                        "message2",
                        "message3",
                        "message4",
                        "message5",
                        "message6",
                    },
                    {
                    },
                    "\n", TTopicReaderSettings({}, false, false, {{0, 10}}, EMessagingFormat::NewlineDelimited, {}, ETransformBody::None, TDuration::Seconds(1)));
        }

        void TestRun_ReadMessages_JsonStreamConcat() {
            // Test JSON stream-concat format - outputs each message as JSON object on its own line
            RunTestWithMetadataFields(
                {
                    "message1",
                    "message2",
                },
                {
                    R"({"body":"message1","offset":0})",
                    R"({"body":"message2","offset":1})",
                },
                "\n",
                TTopicReaderSettings(Nothing(), false, false, {}, EMessagingFormat::JsonStreamConcat,
                    {ETopicMetadataField::Body, ETopicMetadataField::Offset}, ETransformBody::None, TDuration::Seconds(1)));
        }

        void TestRun_ReadMessages_CsvFormat() {
            // Test CSV format - outputs header row followed by data rows
            RunTestWithMetadataFields(
                {
                    "message1",
                    "message2",
                    "message3",
                },
                {
                    "body,offset",  // header
                    "message1,0",
                    "message2,1",
                    "message3,2",
                },
                "\n",
                TTopicReaderSettings(Nothing(), false, false, {}, EMessagingFormat::Csv,
                    {ETopicMetadataField::Body, ETopicMetadataField::Offset}, ETransformBody::None, TDuration::Seconds(1)));
        }

        void TestRun_ReadMessages_TsvFormat() {
            // Test TSV format - tab-separated values
            RunTestWithMetadataFields(
                {
                    "message1",
                    "message2",
                },
                {
                    "body\toffset",  // header
                    "message1\t0",
                    "message2\t1",
                },
                "\n",
                TTopicReaderSettings(Nothing(), false, false, {}, EMessagingFormat::Tsv,
                    {ETopicMetadataField::Body, ETopicMetadataField::Offset}, ETransformBody::None, TDuration::Seconds(1)));
        }

        void TestRun_ReadMessages_CsvFormat_Unlimited() {
            // Test that CSV format reads all messages without a limit (unlimited mode)
            // This tests the fix that removes the 500 line limit for streaming formats
            constexpr size_t numMessages = 501;
            TVector<TString> messages;
            TVector<TString> expected;
            expected.push_back("body");  // header

            for (size_t i = 0; i < numMessages; ++i) {
                TString msg = TStringBuilder() << "msg" << i;
                messages.push_back(msg);
                expected.push_back(msg);
            }

            // With no limit specified, CSV format should read all messages (unlimited)
            RunTestWithMetadataFields(
                messages,
                expected,
                "\n",
                TTopicReaderSettings(Nothing(), false, false, {}, EMessagingFormat::Csv,
                    {ETopicMetadataField::Body}, ETransformBody::None, TDuration::Seconds(2)));
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


            for (size_t i = 0; i < Min(split.size(), expected.size()); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(split[i], expected[i], LabeledOutput(i));
            }
            UNIT_ASSERT_VALUES_EQUAL(split.size(), expected.size());
        }

        void RunTestWithMetadataFields(
            const TVector<TString>& dataToWrite,
            const TVector<TString>& expected,
            const TString& delimiter,
            TTopicReaderSettings&& settings) {
            Cerr << "=== Starting PQ server\n";
            TPersQueueV1TestServer server;
            Cerr << "=== Started PQ server\n";

            SET_LOCALS;

            const TString topicPath = server.GetTopic();
            auto driver = server.Server->AnnoyingClient->GetDriver();
            NPersQueue::TPersQueueClient persQueueClient(*driver);

            WriteTestData(driver, topicPath, dataToWrite);
            NTopic::TReadSessionSettings readSessionSettings = PrepareReadSessionSettings(topicPath);
            IReadSession sess = CreateTopicReader(*driver, readSessionSettings);
            TTopicReader reader(sess, settings);
            reader.Init();

            TStringStream output;
            int status = reader.Run(output);
            UNIT_ASSERT_EQUAL(status, 0);

            // Close to finalize batch formats
            reader.Close(output, TDuration::Seconds(5));

            TVector<TString> split;
            Split(output.Str(), delimiter, split);

            // Remove trailing empty string if present
            while (!split.empty() && split.back().empty()) {
                split.pop_back();
            }

            for (size_t i = 0; i < Min(split.size(), expected.size()); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(split[i], expected[i], LabeledOutput(i));
            }
            UNIT_ASSERT_VALUES_EQUAL(split.size(), expected.size());
        }
        NTopic::TReadSessionSettings PrepareReadSessionSettings(const std::string& topicPath) {
            NTopic::TReadSessionSettings settings;
            settings.ConsumerName("user");
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
