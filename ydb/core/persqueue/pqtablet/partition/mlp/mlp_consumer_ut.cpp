#include "mlp_storage.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/core/mon.h>

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

    // Write many messages because small snapshot do not write wal
    WriteMany(setup, "/Root/topic1", 0, 16, 113);

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
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
            .ProcessingTimeout = TDuration::Seconds(30),
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
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Status, static_cast<ui32>(TStorage::EMessageStatus::Locked));
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].Offset, 2);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].Status, static_cast<ui32>(TStorage::EMessageStatus::Unprocessed));

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

Y_UNIT_TEST(RecreateConsumer) {
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
                        .MaxProcessingAttempts(1000)
                    .EndCondition()
                    .DeleteAction()
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    Cerr << ">>>>> Write many messages for creating WAL (if message count is small every will create the snapshot)" << Endl;
    for (size_t i = 0; i < 50; ++i) {
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 0,
                    .MessageBody = "message_body",
                    .MessageGroupId = TStringBuilder() << "message_group_id_" << i
                },
            }
        });

        auto response = GetWriteResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    }

    Cerr << ">>>>> many iteration for creating many WAL records" << Endl;
    for (size_t i = 0; i < 50; ++i) {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });
        GetReadResponse(runtime);

        CreateUnlockerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) }
        });
        GetChangeResponse(runtime);
    }

    Cerr << ">>>>> Commit message" << Endl;
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

    Cerr << ">>>>> drop consumer" << Endl;
    auto result = client.AlterTopic("/Root/topic1", NYdb::NTopic::TAlterTopicSettings()
            .SetRetentionPeriod(TDuration::Seconds(103))
            .AppendDropConsumers("mlp-consumer")
        ).GetValueSync();

    Cerr << ">>>>> add consumer" << Endl;
    client.AlterTopic("/Root/topic1", NYdb::NTopic::TAlterTopicSettings()
            .SetRetentionPeriod(TDuration::Seconds(103))
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
            .EndAddConsumer()
        ).GetValueSync();

    Cerr << ">>>>> read message (write snapshot)" << Endl;
    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
    }

    Cerr << ">>>>> read message (write WAL)" << Endl;
    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 1);
    }

    Cerr << ">>>>> reload pq tablet" << Endl;
    ReloadPQTablet(setup, "/Root", "/Root/topic1", 0);

    Cerr << ">>>>> read message after reload" << Endl;
    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 2);
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

    WriteMany(setup, "/Root/topic1", 0, 16, 113);

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
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Status, static_cast<ui32>(TStorage::EMessageStatus::Unprocessed));

        break;
    }
}

void HtmlApp(std::string_view consumer, size_t partitionId, std::string_view expected) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
                .KeepMessagesOrder(false)
            .EndAddConsumer()).GetValueSync();

    Sleep(TDuration::Seconds(1));

    auto tabletId = GetTabletId(setup, "/Root", "/Root/topic1", 0);
    auto url = TStringBuilder() << "/app?TabletID=" << tabletId
        << "&consumer=" << consumer
        << "&partitionId=" << partitionId;
    runtime.SendToPipe(tabletId, runtime.AllocateEdgeActor(),
        new NMon::TEvRemoteHttpInfo(url, HTTP_METHOD_GET));

    auto response = runtime.GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>();
    UNIT_ASSERT(response);

    Cerr << (TStringBuilder() <<">>>>> " << response->Html << Endl);
    UNIT_ASSERT(response->Html.find(expected) != TString::npos);
}

Y_UNIT_TEST(HtmlApp_Success) {
    HtmlApp("mlp-consumer", 0, "Total metrics");
}

Y_UNIT_TEST(HtmlApp_BadConsumer) {
    HtmlApp("mlp-consumer-not-exists", 0, "MLP consumer 'mlp-consumer-not-exists' not found");
}

Y_UNIT_TEST(HtmlApp_BadPartition) {
    HtmlApp("mlp-consumer", 13, "Tablet info");
}

}

} // namespace NKikimr::NPQ::NMLP
