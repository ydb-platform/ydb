#include <ydb/core/persqueue/pqtablet/partition/mlp/mlp_common.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/testlib/helpers.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPDLQMoverTests) {

void MoveToDLQ(const TString& msg, bool shortDlqName = false, bool databasePrefixedDlqName = false, bool enableRelativePaths = true) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().FeatureFlags.SetEnableRelativePaths(enableRelativePaths);

    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    const TString dlq = databasePrefixedDlqName && enableRelativePaths ? "/Root/Root/topic1-dlq" : "/Root/topic1-dlq";
    if (databasePrefixedDlqName && enableRelativePaths) {
        setup->GetServer().AnnoyingClient->MkDir("/Root", "Root");
    }
    const auto dlqStatus = client.CreateTopic(dlq, NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
            .EndAddConsumer()).GetValueSync();
    UNIT_ASSERT_C(dlqStatus.IsSuccess(), dlqStatus.GetIssues().ToString());

    const auto sourceStatus = client.CreateTopic("/Root/topic1", NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer("mlp-consumer")
                .BeginDeadLetterPolicy()
                    .Enable()
                    .BeginCondition()
                        .MaxProcessingAttempts(1)
                    .EndCondition()
                    .MoveAction(databasePrefixedDlqName ? "Root/topic1-dlq" : shortDlqName ? "topic1-dlq" : "/Root/topic1-dlq")
                .EndDeadLetterPolicy()
            .EndAddConsumer()).GetValueSync();
    UNIT_ASSERT_C(sourceStatus.IsSuccess(), sourceStatus.GetIssues().ToString());

    setup->Write("/Root/topic1", msg, 0);

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

    const auto dlqReader = runtime.AllocateEdgeActor();
    runtime.Register(CreateReader(dlqReader, TReaderSettings{
        .DatabasePath = "/Root",
        .TopicName = dlq,
        .Consumer = "mlp-consumer",
        .WaitTime = TDuration::Seconds(30),
    }));
    const auto sourceTablet = GetTabletId(setup, "/Root", "/Root/topic1", 0);
    const auto sourceState = runtime.AllocateEdgeActor();
    const auto requestSourceState = [&] {
        ForwardToTablet(runtime, sourceTablet, sourceState,
            new TEvPQ::TEvGetMLPConsumerStateRequest("/Root/topic1", "mlp-consumer", 0));
    };
    requestSourceState();

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
        UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Success);
    }


    {
        auto event = runtime.GrabEdgeEvent<TEvReadResponse>(dlqReader, TDuration::Seconds(35));
        auto* response = event->Get();
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, msg);
    }

    {
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        runtime.GetActorSystem(0)->Schedule(deadline,
            new IEventHandle(sourceState, TActorId(), new TEvents::TEvWakeup()));
        TAutoPtr<IEventHandle> result;
        do {
            runtime.WaitForEdgeEvents([&](auto&, auto& event) {
                result = event;
                return true;
            }, {sourceState});
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Source message was not retired after DLQ delivery");
            UNIT_ASSERT_VALUES_EQUAL(result->GetTypeRewrite(), TEvPQ::TEvGetMLPConsumerStateResponse::EventType);
            if (!result->Get<TEvPQ::TEvGetMLPConsumerStateResponse>()->Messages.empty()) {
                requestSourceState();
            }
        } while (!result->Get<TEvPQ::TEvGetMLPConsumerStateResponse>()->Messages.empty());
        UNIT_ASSERT_VALUES_EQUAL(result->Get<TEvPQ::TEvGetMLPConsumerStateResponse>()->Messages.size(), 0);
    }
}

Y_UNIT_TEST(MoveToDLQ_ShortDlqTopicName) {
    MoveToDLQ(NUnitTest::RandomString(1_KB), true);
}

Y_UNIT_TEST(MoveToDLQ_FullDlqTopicName) {
    MoveToDLQ(NUnitTest::RandomString(1_KB), false);
}

Y_UNIT_TEST_TWIN(MoveToDLQ_DatabasePrefixedDlqTopicName, enableRelativePaths) {
    MoveToDLQ(NUnitTest::RandomString(1_KB), false, true, enableRelativePaths);
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
