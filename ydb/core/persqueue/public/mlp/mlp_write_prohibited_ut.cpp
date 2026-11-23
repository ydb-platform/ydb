#include <ydb/core/persqueue/public/mlp/ut/common/common.h>

namespace NKikimr::NPQ::NMLP {

namespace {

const TString Consumer = "mlp-consumer";

void CreateTopicWithWriteMode(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topic, const TString& mode) {
    auto settings = NYdb::NTopic::TCreateTopicSettings()
        .BeginConfigurePartitioningSettings()
            .MinActivePartitions(1)
            .MaxActivePartitions(1)
        .EndConfigurePartitioningSettings()
        .BeginAddSharedConsumer(Consumer)
        .EndAddConsumer()
        .AddAttribute("_write_mode", mode);
    auto result = CreateTopic(setup, topic, settings);
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void AssertChangeRejected(NActors::TTestActorRuntime& runtime, Ydb::StatusIds::StatusCode status, const TString& message) {
    auto response = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(response->Status, status);
    UNIT_ASSERT_VALUES_EQUAL(response->ErrorDescription, message);
    UNIT_ASSERT(response->Messages.empty());
}

void AssertModificationsRejected(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topic, const TString& mode) {
    const auto status = mode == "unavailable" ? Ydb::StatusIds::UNAVAILABLE : Ydb::StatusIds::BAD_REQUEST;
    const TString message = mode == "unavailable"
        ? "Write to topic is temporarily disabled"
        : "Write to topic is prohibited";
    auto& runtime = setup->GetRuntime();

    CreateWriterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Messages = {{
            .Index = 0,
            .MessageBody = "message_body",
        }},
    });
    {
        auto response = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->DescribeStatus, NDescriber::EStatus::Success);
        UNIT_ASSERT_VALUES_EQUAL(response->Status, status);
        UNIT_ASSERT_VALUES_EQUAL(response->ErrorDescription, message);
        UNIT_ASSERT(response->Messages.empty());
    }

    CreateReaderActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Consumer = Consumer,
    });
    {
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(response->Messages.empty());
    }

    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Consumer = Consumer,
        .Messages = {TMessageId(0, 0)},
    });
    AssertChangeRejected(runtime, status, message);

    CreateUnlockerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Consumer = Consumer,
        .Messages = {TMessageId(0, 0)},
    });
    AssertChangeRejected(runtime, status, message);

    CreateMessageDeadlineChangerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Consumer = Consumer,
        .Messages = {TMessageId(0, 0)},
        .Deadlines = {TInstant::Now() + TDuration::Seconds(30)},
    });
    AssertChangeRejected(runtime, status, message);

    CreatePurgerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Consumer = Consumer,
    });
    AssertPurgeError(runtime, status, message);
}

} // namespace

Y_UNIT_TEST_SUITE(TMLPWriteProhibitedTests) {

    Y_UNIT_TEST(ModifyingOperationsFailBeforeTablet) {
        auto setup = CreateSetup();
        CreateTopicWithWriteMode(setup, "/Root/unavailable", "unavailable");
        AssertModificationsRejected(setup, "/Root/unavailable", "unavailable");
        CreateTopicWithWriteMode(setup, "/Root/prohibited", "prohibited");
        AssertModificationsRejected(setup, "/Root/prohibited", "prohibited");
    }

}

} // namespace NKikimr::NPQ::NMLP
