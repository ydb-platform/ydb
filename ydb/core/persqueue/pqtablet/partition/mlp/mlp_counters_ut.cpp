#include <ydb/core/base/counters.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/testlib/tablet_helpers.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPCountersTests) {

Y_UNIT_TEST(SimpleCounters) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    CreateTopic(setup, "/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .PartitioningSettings(2, 2)
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(10)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer());

    WriteMany(setup, "/Root/topic1", 0, 16, 59);
    WriteMany(setup, "/Root/topic1", 1, 16, 61);

    {
        Cerr << ">>>>> read from first partition" << Endl;
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .VisibilityTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 10
        });
        auto result = GetReadResponse(runtime);

        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
    }
    {
        Cerr << ">>>>> read from second partition" << Endl;
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .VisibilityTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 10
        });
        auto result = GetReadResponse(runtime);

        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 10);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
    }
    {
        Cerr << ">>>>> commit message" << Endl;
        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 1) }
        });
        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }
    {
        Cerr << ">>>>> unlock message" << Endl;
        CreateUnlockerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(1, 0) }
        });
        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    Sleep(TDuration::Seconds(1));

    ReloadPQRBTablet(setup, "/Root", "/Root/topic1");

    Sleep(TDuration::Seconds(2));

    auto counters = setup->GetRuntime().GetAppData(0).Counters;
    auto dbGroup = GetServiceCounters(counters, "topics", false);
    auto group = dbGroup->GetSubgroup("host", "")
                     ->GetSubgroup("database", "/Root")
                     ->GetSubgroup("cloud_id", "")
                     ->GetSubgroup("folder_id", "")
                     ->GetSubgroup("database_id", "")
                     ->GetSubgroup("topic", "topic1")
                     ->GetSubgroup("consumer", "mlp-consumer");

    UNIT_ASSERT(group);

    TStringStream countersStr;
    group->OutputHtml(countersStr);
    Cerr << "COUNTERS: " << countersStr.Str() << "\n";

    auto assertMetric = [&](const auto& name, const auto& value) {
        auto counter = group->FindNamedCounter("name", name);
        UNIT_ASSERT_C(counter, TStringBuilder() << "metric '" << name << "' not found");
        UNIT_ASSERT_VALUES_EQUAL_C(counter->Val(), value, TStringBuilder() << "metric '" << name << "'");
    };

    assertMetric("topic.inflight.locked_messages", 18);
    assertMetric("topic.inflight.committed_messages", 1);
    assertMetric("topic.inflight.delayed_messages", 0);
    assertMetric("topic.inflight.unlocked_messages", 101);
    assertMetric("topic.inflight.scheduled_to_dlq_messages", 0);
    
    assertMetric("topic.committed_messages", 1);
    assertMetric("topic.purged_messages", 0);

    auto assertDeletedMetric = [&](const auto& reason, const auto& value) {
        auto counter = group->GetSubgroup("name", "topic.deleted_messages")->FindNamedCounter("reason", reason);
        UNIT_ASSERT_C(counter, TStringBuilder() << "metric 'topic.deleted_messages/reason." << reason << " not found");
        UNIT_ASSERT_VALUES_EQUAL_C(counter->Val(), value, TStringBuilder() << "metric 'topic.deleted_messages.reason." << reason << "'");

    };

    assertDeletedMetric("delete_policy", 0);
    assertDeletedMetric("move_policy", 0);
    assertDeletedMetric("retention", 0);
}

}

}
