#include <ydb/core/persqueue/public/mlp/ut/common/common.h>

namespace NKikimr::NPQ::NMLP {

namespace {

const TString Topic = "/Root/topic1";
const TString Consumer = "mlp-consumer";
const TString Prohibited = "Write to topic is prohibited";

void CreateProhibitedTopic(std::shared_ptr<TTopicSdkTestSetup>& setup) {
    auto settings = NYdb::NTopic::TCreateTopicSettings()
        .BeginConfigurePartitioningSettings()
            .MinActivePartitions(1)
            .MaxActivePartitions(1)
        .EndConfigurePartitioningSettings()
        .BeginAddSharedConsumer(Consumer)
        .EndAddConsumer()
        .AddAttribute("_write_mode", "prohibited");
    auto result = CreateTopic(setup, Topic, settings);
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void AssertChangeRejected(NActors::TTestActorRuntime& runtime) {
    auto response = GetChangeResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT_VALUES_EQUAL(response->ErrorDescription, Prohibited);
    UNIT_ASSERT(response->Messages.empty());
}

} // namespace

Y_UNIT_TEST_SUITE(TMLPWriteProhibitedTests) {

    Y_UNIT_TEST(ModifyingOperationsFailBeforeTablet) {
        auto setup = CreateSetup();
        CreateProhibitedTopic(setup);
        auto& runtime = setup->GetRuntime();

        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = Topic,
            .Messages = {{
                .Index = 0,
                .MessageBody = "message_body",
            }},
        });
        {
            auto response = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->DescribeStatus, NDescriber::EStatus::Success);
            UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorDescription, Prohibited);
            UNIT_ASSERT(response->Messages.empty());
        }

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = Topic,
            .Consumer = Consumer,
        });
        {
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(response->Messages.empty());
        }

        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = Topic,
            .Consumer = Consumer,
            .Messages = {TMessageId(0, 0)},
        });
        AssertChangeRejected(runtime);

        CreateUnlockerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = Topic,
            .Consumer = Consumer,
            .Messages = {TMessageId(0, 0)},
        });
        AssertChangeRejected(runtime);

        CreateMessageDeadlineChangerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = Topic,
            .Consumer = Consumer,
            .Messages = {TMessageId(0, 0)},
            .Deadlines = {TInstant::Now() + TDuration::Seconds(30)},
        });
        AssertChangeRejected(runtime);

        CreatePurgerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = Topic,
            .Consumer = Consumer,
        });
        AssertPurgeError(runtime, Ydb::StatusIds::BAD_REQUEST, Prohibited);
    }

}

} // namespace NKikimr::NPQ::NMLP
