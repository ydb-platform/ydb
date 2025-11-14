#include <ydb/core/persqueue/pqtablet/partition/mlp/mlp_common.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/testlib/tablet_helpers.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPDLQMoverTests) {

void MoveToDLQ(const TString& msg) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1-dlq", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
            .EndAddConsumer()).GetValueSync();

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(1)
                    .EndCondition()
                    .MoveAction("/Root/topic1-dlq")
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    setup->Write("/Root/topic1", msg, 0);

    Sleep(TDuration::Seconds(2));

    {
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

    {
        CreateUnlockerActor(runtime, TUnlockerSettings{
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = {{0, 0}}
        });

        auto result = GetChangeResponse(runtime);

        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Success, true);
    }


    for (size_t i = 0; i < 10; ++i) {
        Sleep(TDuration::Seconds(1));
        // The message should appear in DLQ
        CreateReaderActor(runtime, TReaderSettings{
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1-dlq",
            .Consumer = "mlp-consumer",
            .UncompressMessages = true
        });
        auto response = GetReadResponse(runtime);
        if (i < 9 && response->Messages.empty()) {
            continue;
        }

        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, msg);

        break;
    }

    for (size_t i = 0; i < 10; ++i) {
        auto result = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        if (i < 9 && result->Messages.size() != 0) {
            Sleep(TDuration::Seconds(1));
            continue;
        }

        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 0);

        break;
    }
}

Y_UNIT_TEST(MoveToDLQ_ShortMessage) {
    MoveToDLQ(NUnitTest::RandomString(1_KB));
}

Y_UNIT_TEST(MoveToDLQ_BigMessage) {
    MoveToDLQ(NUnitTest::RandomString(31_MB));
}

Y_UNIT_TEST(MoveToDLQ_ManyMessages) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1-dlq", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
            .EndAddConsumer()).GetValueSync();

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(1)
                    .EndCondition()
                    .MoveAction("/Root/topic1-dlq")
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    auto msg0 = NUnitTest::RandomString(1_KB);
    auto msg1 = NUnitTest::RandomString(1_KB);
    auto msg2 = NUnitTest::RandomString(1_KB);

    setup->Write("/Root/topic1", msg0, 0);
    setup->Write("/Root/topic1", msg1, 0);
    setup->Write("/Root/topic1", msg2, 0);

    Sleep(TDuration::Seconds(2));

    {
        CreateReaderActor(runtime, TReaderSettings{
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .MaxNumberOfMessage = 3,
            .UncompressMessages = true
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 3);
    }

    auto unlock = [&](const TString& topic, std::vector<ui64> offsets) {
        auto settings = TUnlockerSettings{
            .DatabasePath = "/Root",
            .TopicName = topic,
            .Consumer = "mlp-consumer",
        };

        for (auto& o : offsets) {
            settings.Messages.push_back({0, o});
        }

        CreateUnlockerActor(runtime, std::move(settings));

        auto result = GetChangeResponse(runtime);

        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), offsets.size());
    };

    unlock("/Root/topic1", {2, 0, 1});

    for (size_t i = 0; i < 10; ++i) {
        Sleep(TDuration::Seconds(1));
        // The message should appear in DLQ
        CreateReaderActor(runtime, TReaderSettings{
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1-dlq",
            .Consumer = "mlp-consumer",
            .MaxNumberOfMessage = 10,
            .UncompressMessages = true
        });
        auto response = GetReadResponse(runtime);
        if (i < 9 && response->Messages.size() != 3) {
            Cerr << (TStringBuilder() << ">>>>> i: " << i << " response->Messages.size(): " << response->Messages.size() << Endl);
            for (auto& m : response->Messages) {
                unlock("/Root/topic1-dlq", {m.MessageId.Offset});
            }
            continue;
        }

        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 3);

        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, msg2);

        UNIT_ASSERT_VALUES_EQUAL(response->Messages[1].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[1].MessageId.Offset, 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[1].Data, msg0);

        UNIT_ASSERT_VALUES_EQUAL(response->Messages[2].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[2].MessageId.Offset, 2);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[2].Data, msg1);

        break;
    }

    for (size_t i = 0; i < 10; ++i) {
        auto result = GetConsumerState(setup, "/Root", "/Root/topic1", "mlp-consumer");
        if (i < 9 && result->Messages.size() != 0) {
            Sleep(TDuration::Seconds(1));
            continue;
        }

        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 0);

        break;
    }
}

Y_UNIT_TEST(MoveToDLQ_TopicNotExists) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    client.CreateTopic("/Root/topic1-dlq", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
            .EndAddConsumer()).GetValueSync();

    client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(1)
                    .EndCondition()
                    .MoveAction("/Root/topic1-dlq")
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();

    client.DropTopic("/Root/topic1-dlq").GetValueSync();

    auto msg0 = NUnitTest::RandomString(1_KB);
    setup->Write("/Root/topic1", msg0, 0);

    Sleep(TDuration::Seconds(2));

    {
        CreateReaderActor(runtime, TReaderSettings{
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .MaxNumberOfMessage = 1,
            .UncompressMessages = true
        });
        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    }

    auto unlock = [&](const TString& topic, std::vector<ui64> offsets) {
        auto settings = TUnlockerSettings{
            .DatabasePath = "/Root",
            .TopicName = topic,
            .Consumer = "mlp-consumer",
        };

        for (auto& o : offsets) {
            settings.Messages.push_back({0, o});
        }

        CreateUnlockerActor(runtime, std::move(settings));

        auto result = GetChangeResponse(runtime);

        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), offsets.size());
    };

    unlock("/Root/topic1", { 0});

    // Check that message return to queue if DLQ topic don`t exists
    for (size_t i = 0; i < 10; ++i) {
        Sleep(TDuration::Seconds(1));

        CreateReaderActor(runtime, TReaderSettings{
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .MaxNumberOfMessage = 1,
            .UncompressMessages = true
        });

        auto response = GetReadResponse(runtime);
        if (i < 9 && response->Messages.size() != 1) {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);

        break;
    }

}

}

} // namespace NKikimr::NPQ::NMLP
