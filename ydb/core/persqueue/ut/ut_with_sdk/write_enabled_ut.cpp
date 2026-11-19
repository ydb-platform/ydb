#include <ydb/public/sdk/cpp/src/client/persqueue_public/include/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPersQueueTests {
namespace {

using namespace NYdb::NTopic;

const TString TOPIC = "/Root/write-enabled";
const TString CONSUMER = "reader";

class TWriteEnabledEnv {
public:
    NYdb::NTopic::NTests::TTopicSdkTestSetup Setup;
    NYdb::TDriver Driver;
    TTopicClient Client;
    NYdb::NPersQueue::TPersQueueClient PqClient;

    TWriteEnabledEnv()
        : Setup("WriteEnabled", NYdb::NTopic::NTests::TTopicSdkTestSetup::MakeServerSettings(), false)
        , Driver(Setup.MakeDriverConfig())
        , Client(Driver)
        , PqClient(Driver)
    {
    }

    void Create(bool pqv1, std::optional<bool> enabled = std::nullopt) {
        if (pqv1) {
            auto settings = NYdb::NPersQueue::TCreateTopicSettings()
                .PartitionsCount(1)
                .ReadRules({NYdb::NPersQueue::TReadRuleSettings().ConsumerName(CONSUMER)});
            if (enabled.has_value()) {
                settings.WriteEnabled(*enabled);
            }
            AssertSuccess(PqClient.CreateTopic(TOPIC, settings).GetValueSync());
        } else {
            auto settings = TCreateTopicSettings().PartitioningSettings(1, 1)
                .BeginAddConsumer(CONSUMER).EndAddConsumer();
            if (enabled.has_value()) {
                settings.AddAttribute("_write_enabled", *enabled ? "true" : "false");
            }
            AssertSuccess(Client.CreateTopic(TOPIC, settings).GetValueSync());
        }
    }

    void Alter(bool pqv1, const TString& value) {
        if (pqv1) {
            const auto describe = PqClient.DescribeTopic(TOPIC).GetValueSync();
            AssertSuccess(describe);
            auto settings = NYdb::NPersQueue::TAlterTopicSettings().SetSettings(describe.TopicSettings());
            if (value.empty()) {
                settings.WriteEnabled(std::nullopt);
            } else {
                settings.WriteEnabled(value == "true");
            }
            AssertSuccess(PqClient.AlterTopic(TOPIC, settings).GetValueSync());
        } else {
            auto settings = TAlterTopicSettings();
            settings.BeginAlterAttributes().Alter("_write_enabled", value).EndAlterAttributes();
            AssertSuccess(Client.AlterTopic(TOPIC, settings).GetValueSync());
        }
    }

    void AssertAttribute(std::optional<bool> enabled) {
        const auto pqDescribe = PqClient.DescribeTopic(TOPIC).GetValueSync();
        AssertSuccess(pqDescribe);
        UNIT_ASSERT(pqDescribe.TopicSettings().WriteEnabled() == enabled);
        const auto describe = Client.DescribeTopic(TOPIC).GetValueSync();
        AssertSuccess(describe);
        const auto& attributes = describe.GetTopicDescription().GetAttributes();
        const auto it = attributes.find("_write_enabled");
        if (enabled.has_value()) {
            UNIT_ASSERT(it != attributes.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, *enabled ? "true" : "false");
        } else {
            UNIT_ASSERT(it == attributes.end());
        }
    }

    TWriteSessionSettings WriteSettings() const {
        return TWriteSessionSettings().Path(TOPIC).PartitionId(0)
            .ProducerId("producer").MessageGroupId("producer").Codec(ECodec::RAW)
            .RetryPolicy(IRetryPolicy::GetNoRetryPolicy());
    }

    void Write(ui64 seqNo) {
        auto writer = Client.CreateSimpleBlockingWriteSession(WriteSettings());
        UNIT_ASSERT(writer->Write("message", seqNo, std::nullopt, TDuration::Seconds(10)));
        UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));
    }

    template <class TEvents, class TWriter>
    void AssertRejected(const std::shared_ptr<TWriter>& writer, const TString& data) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        bool sent = false;
        while (TInstant::Now() < deadline) {
            UNIT_ASSERT_C(writer->WaitEvent().Wait(deadline - TInstant::Now()), "Write session did not respond");
            auto event = writer->GetEvent(false);
            UNIT_ASSERT(event);
            if (auto* ready = std::get_if<typename TEvents::TReadyToAcceptEvent>(&*event)) {
                if (!sent) {
                    writer->Write(std::move(ready->ContinuationToken), data, 100);
                    sent = true;
                }
            } else if (auto* acks = std::get_if<typename TEvents::TAcksEvent>(&*event)) {
                for (const auto& ack : acks->Acks) {
                    UNIT_ASSERT_VALUES_EQUAL(ack.State, TEvents::TWriteAck::EES_DISCARDED);
                }
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                UNIT_ASSERT(sent);
                UNIT_ASSERT_VALUES_EQUAL_C(closed->GetStatus(), NYdb::EStatus::UNAVAILABLE, closed->DebugString());
                UNIT_ASSERT_STRING_CONTAINS(closed->GetIssues().ToString(), "Write to topic is disabled");
                return;
            }
        }
        UNIT_FAIL("Write session did not close after a rejected write");
    }

    void RejectWrites(bool pqv1) {
        for (const auto size : {16u, 600u * 1024}) {
            const TString data(size, 'x');
            if (pqv1) {
                auto writer = PqClient.CreateWriteSession(NYdb::NPersQueue::TWriteSessionSettings()
                    .Path(TOPIC).MessageGroupId("producer").Codec(ECodec::RAW)
                    .ClusterDiscoveryMode(NYdb::NPersQueue::EClusterDiscoveryMode::Off)
                    .RetryPolicy(IRetryPolicy::GetNoRetryPolicy()));
                AssertRejected<NYdb::NPersQueue::TWriteSessionEvent>(writer, data);
            } else {
                auto writer = Client.CreateWriteSession(WriteSettings());
                AssertRejected<TWriteSessionEvent>(writer, data);
            }
        }
    }

    void AssertOffsets(ui64 committed, ui64 end) {
        auto reader = Client.CreateReadSession(TReadSessionSettings().ConsumerName(CONSUMER)
            .AppendTopics(TTopicReadSettings(TOPIC)).RetryPolicy(IRetryPolicy::GetNoRetryPolicy()));
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        while (TInstant::Now() < deadline) {
            UNIT_ASSERT(reader->WaitEvent().Wait(deadline - TInstant::Now()));
            auto event = reader->GetEvent(false);
            UNIT_ASSERT(event);
            if (auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                UNIT_ASSERT_VALUES_EQUAL(start->GetCommittedOffset(), committed);
                UNIT_ASSERT_VALUES_EQUAL(start->GetEndOffset(), end);
                start->Confirm();
                UNIT_ASSERT(reader->Close(TDuration::Seconds(10)));
                return;
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(closed->DebugString());
            }
        }
        UNIT_FAIL("Read session did not start");
    }

    static void AssertSuccess(const NYdb::TStatus& status) {
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TTopicWriteEnabled) {
    Y_UNIT_TEST_TWIN(CreateDisabledTopic, pqv1) {
        TWriteEnabledEnv env;
        env.Create(pqv1, false);
        env.AssertAttribute(false);
        env.RejectWrites(pqv1);
        env.AssertOffsets(0, 0);
    }

    Y_UNIT_TEST_TWIN(DisableAndEnableWrites, pqv1) {
        TWriteEnabledEnv env;
        env.Create(pqv1);
        env.AssertAttribute(std::nullopt);
        env.Write(1);
        env.Alter(pqv1, "false");
        env.AssertAttribute(false);

        // A Describe -> SetSettings -> Alter round trip must preserve the restriction.
        const auto describe = env.PqClient.DescribeTopic(TOPIC).GetValueSync();
        env.AssertSuccess(describe);
        env.AssertSuccess(env.PqClient.AlterTopic(TOPIC,
            NYdb::NPersQueue::TAlterTopicSettings().SetSettings(describe.TopicSettings())).GetValueSync());
        env.AssertAttribute(false);
        env.RejectWrites(pqv1);
        env.AssertSuccess(env.Client.CommitOffset(TOPIC, 0, CONSUMER, 1).GetValueSync());
        env.AssertOffsets(1, 1);

        env.Alter(pqv1, "true");
        env.AssertAttribute(true);
        env.Write(2);
        env.Alter(pqv1, "false");
        env.Alter(pqv1, "");
        env.AssertAttribute(std::nullopt);
        env.Write(3);
        env.AssertOffsets(1, 3);
    }

    Y_UNIT_TEST(RejectInvalidWriteEnabled) {
        TWriteEnabledEnv env;
        auto create = env.Client.CreateTopic(TOPIC,
            TCreateTopicSettings().AddAttribute("_write_enabled", "not-bool")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(create.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(create.GetIssues().ToString(), "not bool");

        env.Create(false, false);
        auto settings = TAlterTopicSettings();
        settings.BeginAlterAttributes().Alter("_write_enabled", "not-bool").EndAlterAttributes();
        const auto alter = env.Client.AlterTopic(TOPIC, settings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(alter.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(alter.GetIssues().ToString(), "not bool");
        env.AssertAttribute(false);
        env.RejectWrites(false);
        env.AssertOffsets(0, 0);
    }
}

} // namespace NKikimr::NPersQueueTests
