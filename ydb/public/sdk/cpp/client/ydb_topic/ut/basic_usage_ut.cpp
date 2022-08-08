#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/write_session.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/ut_utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

namespace NYdb::NTopic::NTests {

Y_UNIT_TEST_SUITE(BasicUsage) {

    Y_UNIT_TEST(WriteAndReadSomeMessagesWithSyncCompression) {

        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);

        NPersQueue::TWriteSessionSettings writeSettings;
        writeSettings.Path(setup->GetTestTopic()).MessageGroupId("src_id");
        NPersQueue::IExecutor::TPtr executor = new NPersQueue::TSyncExecutor();
        writeSettings.CompressionExecutor(executor);

        ui64 count = 100u;
        TMaybe<bool> shouldCaptureData = {true};

        auto& client = setup->GetPersQueueClient();
        auto session = client.CreateSimpleBlockingWriteSession(writeSettings);
        TString messageBase = "message-";
        TVector<TString> sentMessages;

        for (auto i = 0u; i < count; i++) {
            sentMessages.emplace_back(messageBase * (i+1) + ToString(i));
            auto res = session->Write(sentMessages.back());
            UNIT_ASSERT(res);
        }
        {
            auto sessionAdapter = NPersQueue::NTests::TSimpleWriteSessionTestAdapter(
                    dynamic_cast<NPersQueue::TSimpleBlockingWriteSession *>(session.get()));
            if (shouldCaptureData.Defined()) {
                TStringBuilder msg;
                msg << "Session has captured " << sessionAdapter.GetAcquiredMessagesCount()
                    << " messages, capturing was expected: " << *shouldCaptureData << Endl;
                UNIT_ASSERT_VALUES_EQUAL_C(sessionAdapter.GetAcquiredMessagesCount() > 0, *shouldCaptureData, msg.c_str());
            }
        }
        session->Close();

        std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;

        // Create topic client.
        NYdb::NTopic::TTopicClient topicClient(setup->GetDriver());

        // Create read session.
        NYdb::NTopic::TReadSessionSettings readSettings;
        readSettings
            .ConsumerName(setup->GetTestClient())
            .AppendTopics(setup->GetTestTopic());

        Cerr << "Session was created" << Endl;

        NThreading::TPromise<void> checkedPromise = NThreading::NewPromise<void>();
        auto totalReceived = 0u;
        readSettings.EventHandlers_.SimpleDataHandlers([&](NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& ev) {
            auto& messages = ev.GetMessages();
            for (size_t i = 0u; i < messages.size(); ++i) {
                auto& message = messages[i];
                UNIT_ASSERT_VALUES_EQUAL(message.GetData(), sentMessages[totalReceived]);
                totalReceived++;
            }
            if (totalReceived == sentMessages.size())
                checkedPromise.SetValue();
        });

        ReadSession = topicClient.CreateReadSession(readSettings);

        checkedPromise.GetFuture().GetValueSync();
        ReadSession->Close(TDuration::Zero());
    }
}

}
