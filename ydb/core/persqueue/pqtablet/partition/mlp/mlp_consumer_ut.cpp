#include "mlp_storage.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/testlib/tablet_helpers.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPConsumerTests) {

Y_UNIT_TEST(ReloadPQTablet) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    CreateTopic(setup, "/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(1)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer());

    // Write many messaes because small snapshot do not write wal
    WriteMany(setup, "/Root/topic1", 0, 16, 113);

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .VisibilityTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .VisibilityTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    Cerr << ">>>>> BEGIN COMMIT" << Endl;

    {
        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) }
        });

        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    Cerr << ">>>>> BEGIN REBOOT " << Endl;
    ReloadPQTablet(setup, "/Root", "/Root/topic1", 0);

    for (size_t i = 0; i < 10; ++i) {
        Sleep(TDuration::Seconds(1));

        auto result = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        if (i < 9 && result->Messages.size() != 2) {
            continue;
        }

        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Offset, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Status, TStorage::EMessageStatus::Locked);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].Offset, 2);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].Status, TStorage::EMessageStatus::Unprocessed);

        break;
    }
}

Y_UNIT_TEST(AlterConsumer) {
    auto setup = CreateSetup();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .RetentionPeriod(TDuration::Seconds(3))
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .DefaultProcessingTimeout(TDuration::Seconds(13))
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(17)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    {
        auto result = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");

        UNIT_ASSERT_VALUES_EQUAL(result->RetentionPeriod.value(), TDuration::Seconds(3));
        UNIT_ASSERT_VALUES_EQUAL(result->Config.GetDefaultProcessingTimeoutSeconds(), 13);
        UNIT_ASSERT_VALUES_EQUAL(result->Config.GetMaxProcessingAttempts(), 17);
        UNIT_ASSERT_VALUES_EQUAL(::NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(result->Config.GetDeadLetterPolicy()),
            ::NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE));
    }

    client.AlterTopic("/Root/topic1", NYdb::NTopic::TAlterTopicSettings()
            .SetRetentionPeriod(TDuration::Seconds(103))
            .BeginAlterConsumer("mlp-consumer")
                .DefaultProcessingTimeout(TDuration::Seconds(113))
                .BeginAlterDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(117)
                    .EndCondition()
                    .SetMoveAction("dlq-queue")
                .EndAlterDeadLetterPolicy()
            .EndAlterConsumer()).GetValueSync();

    {
        auto result = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");

        UNIT_ASSERT_VALUES_EQUAL(result->RetentionPeriod.value(), TDuration::Seconds(103));
        UNIT_ASSERT_VALUES_EQUAL(result->Config.GetDefaultProcessingTimeoutSeconds(), 113);
        UNIT_ASSERT_VALUES_EQUAL(result->Config.GetMaxProcessingAttempts(), 117);
        UNIT_ASSERT_VALUES_EQUAL(::NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(result->Config.GetDeadLetterPolicy()),
            ::NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE));
    }
}

Y_UNIT_TEST(ReloadPQTabletAfterAlterConsumer) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .RetentionPeriod(TDuration::Seconds(3))
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
                .DefaultProcessingTimeout(TDuration::Seconds(13))
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(17)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    // Write many messaes because small snapshot do not write wal
    WriteMany(setup, "/Root/topic1", 0, 16, 17);

    Sleep(TDuration::Seconds(1));

    Cerr << ">>>>> BEGIN COMMIT" << Endl;
    {
        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) }
        });

        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    }

    client.AlterTopic("/Root/topic1", NYdb::NTopic::TAlterTopicSettings()
        .SetRetentionPeriod(TDuration::Seconds(103))
        .BeginAlterConsumer("mlp-consumer")
            .DefaultProcessingTimeout(TDuration::Seconds(113))
            .BeginAlterDeadLetterPolicy()
                .Enable()
                .BeginCondition()
                    .MaxProcessingAttempts(117)
                .EndCondition()
                .SetMoveAction("dlq-queue")
            .EndAlterDeadLetterPolicy()
        .EndAlterConsumer()).GetValueSync();

    Cerr << ">>>>> BEGIN REBOOT " << Endl;
    ReloadPQTablet(setup, "/Root", "/Root/topic1", 0);

    Sleep(TDuration::Seconds(1));

    // Checking that alter consumer do not change consumer generation and snapshot and wal read successfully
    for (size_t i = 0; i < 10; ++i) {
        Sleep(TDuration::Seconds(1));

        auto result = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        if (i < 9 && result->Messages.size() != 16) {
            continue;
        }

        // Message with offset 0 was committed and deleted
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Offset, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Status, TStorage::EMessageStatus::Unprocessed);

        break;
    }
}


Y_UNIT_TEST(RetentionStorage) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .RetentionStorageMb(8)
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
            .EndAddConsumer());

    Sleep(TDuration::Seconds(1));

    WriteMany(setup, "/Root/topic1", 0, 1_MB, 25);

    Sleep(TDuration::Seconds(1));

    {
        // check that message with offset 0 wasn`t removed by retention
        CreateReaderActor(runtime, TReaderSettings{
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 0);
    }
}

Y_UNIT_TEST(RetentionStorageAfterReload) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .RetentionStorageMb(8)
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
            .EndAddConsumer());

    Sleep(TDuration::Seconds(1));

    WriteMany(setup, "/Root/topic1", 0, 1_MB, 25);

    Cerr << ">>>>> BEGIN REBOOT " << Endl;
    ReloadPQTablet(setup, "/Root", "/Root/topic1", 0);

    Sleep(TDuration::Seconds(2));

    {
        // check that message with offset 0 wasn`t removed by retention
        CreateReaderActor(runtime, TReaderSettings{
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 0);
    }
}

}

} // namespace NKikimr::NPQ::NMLP
