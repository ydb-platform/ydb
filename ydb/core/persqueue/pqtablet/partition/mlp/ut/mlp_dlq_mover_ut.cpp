#include <ydb/core/persqueue/pqtablet/partition/mlp/mlp_common.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/testlib/tablet_helpers.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPDLQMoverTests) {

namespace {

constexpr TStringBuf kDatabase = "/Root";
constexpr TStringBuf kSourceTopic = "/Root/topic1";
constexpr TStringBuf kDlqTopic = "/Root/topic1-dlq";
constexpr TStringBuf kConsumer = "mlp-consumer";

void CreateSourceAndDlqTopics(std::shared_ptr<TTopicSdkTestSetup>& setup, bool createDlq = true) {
    auto driver = TDriver(setup->MakeDriverConfig());
    auto client = TTopicClient(driver);

    if (createDlq) {
        auto dlqStatus = client.CreateTopic(TString(kDlqTopic), NYdb::NTopic::TCreateTopicSettings()
                .BeginAddSharedConsumer(TString(kConsumer))
                .EndAddConsumer()).GetValueSync();
        UNIT_ASSERT_C(dlqStatus.IsSuccess(), dlqStatus.GetIssues().ToString());
    }

    auto sourceStatus = client.CreateTopic(TString(kSourceTopic), NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer(TString(kConsumer))
            .EndAddConsumer()).GetValueSync();
    UNIT_ASSERT_C(sourceStatus.IsSuccess(), sourceStatus.GetIssues().ToString());
}

TEvPQ::TEvMLPDLQMoverResponse::TPtr RunDirectMover(
    std::shared_ptr<TTopicSdkTestSetup>& setup,
    TString destinationTopic,
    std::deque<TDLQMessage> messages,
    ui64 consumerGeneration = 1,
    TDuration timeout = TDuration::Seconds(30)
) {
    auto& runtime = setup->GetRuntime();
    const ui64 tabletId = GetTabletId(setup, TString(kDatabase), TString(kSourceTopic), 0);
    const auto parent = runtime.AllocateEdgeActor();

    const auto moverId = runtime.Register(CreateDLQMover({
        .ParentActorId = parent,
        .Database = TString(kDatabase),
        .TabletId = tabletId,
        .PartitionId = 0,
        .ConsumerName = TString(kConsumer),
        .ConsumerGeneration = consumerGeneration,
        .DestinationTopic = std::move(destinationTopic),
        .Messages = std::move(messages),
    }));
    runtime.EnableScheduleForActor(moverId);
    runtime.DispatchEvents();

    auto response = runtime.GrabEdgeEvent<TEvPQ::TEvMLPDLQMoverResponse>(parent, timeout);
    UNIT_ASSERT(response);
    return response;
}

void ExpectDlqContains(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& expectedData, ui32 expectedCount = 1) {
    auto& runtime = setup->GetRuntime();
    for (size_t i = 0; i < 10; ++i) {
        CreateReaderActor(runtime, TReaderSettings{
            .DatabasePath = TString(kDatabase),
            .TopicName = TString(kDlqTopic),
            .Consumer = TString(kConsumer),
            .MaxNumberOfMessage = expectedCount,
        });
        auto response = GetReadResponse(runtime);
        if (i < 9 && response->Messages.size() != expectedCount) {
            Sleep(TDuration::MilliSeconds(200));
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), expectedCount);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, expectedData);
        return;
    }
}

} // namespace

void MoveToDLQ(const TString& msg, bool shortDlqName = false) {
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
                    .MoveAction(shortDlqName ? "topic1-dlq" : "/Root/topic1-dlq")
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
        UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Success);
    }


    for (size_t i = 0; i < 10; ++i) {
        Sleep(TDuration::Seconds(1));
        // The message should appear in DLQ
        CreateReaderActor(runtime, TReaderSettings{
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1-dlq",
            .Consumer = "mlp-consumer",
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

Y_UNIT_TEST(MoveToDLQ_ShortDlqTopicName) {
    MoveToDLQ(NUnitTest::RandomString(1_KB), true);
}

Y_UNIT_TEST(MoveToDLQ_FullDlqTopicName) {
    MoveToDLQ(NUnitTest::RandomString(1_KB), false);
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

Y_UNIT_TEST(DirectMove_Success) {
    auto setup = CreateSetup();
    CreateSourceAndDlqTopics(setup);

    const auto msg = NUnitTest::RandomString(2_KB);
    setup->Write(TString(kSourceTopic), msg, 0);
    Sleep(TDuration::Seconds(1));

    auto response = RunDirectMover(setup, TString(kDlqTopic), {{.Offset = 0, .SeqNo = 1}});
    const auto* result = response->Get();
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages[0].first, 0);
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages[0].second, 1);

    ExpectDlqContains(setup, msg);
}

Y_UNIT_TEST(DirectMove_MultipleMessages) {
    auto setup = CreateSetup();
    CreateSourceAndDlqTopics(setup);

    const auto msg0 = NUnitTest::RandomString(1_KB);
    const auto msg1 = NUnitTest::RandomString(1_KB);
    setup->Write(TString(kSourceTopic), msg0, 0);
    setup->Write(TString(kSourceTopic), msg1, 0);
    Sleep(TDuration::Seconds(1));

    auto response = RunDirectMover(setup, TString(kDlqTopic), {
        {.Offset = 0, .SeqNo = 1},
        {.Offset = 1, .SeqNo = 2},
    });
    const auto* result = response->Get();
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages[0].first, 0);
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages[0].second, 1);
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages[1].first, 1);
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages[1].second, 2);

    ExpectDlqContains(setup, msg0, 2);
}

Y_UNIT_TEST(DirectMove_EmptyQueue) {
    auto setup = CreateSetup();
    CreateSourceAndDlqTopics(setup);

    auto response = RunDirectMover(setup, TString(kDlqTopic), {});
    const auto* result = response->Get();
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages.size(), 0);
}

Y_UNIT_TEST(DirectMove_SkipAlreadyWrittenSeqNo) {
    auto setup = CreateSetup();
    CreateSourceAndDlqTopics(setup);

    const auto msg = NUnitTest::RandomString(1_KB);
    setup->Write(TString(kSourceTopic), msg, 0);
    Sleep(TDuration::Seconds(1));

    {
        auto response = RunDirectMover(setup, TString(kDlqTopic), {{.Offset = 0, .SeqNo = 1}});
        const auto* result = response->Get();
        UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages.size(), 1);
    }

    // Same ProducerId/SeqNo: writer Init reports SeqNo>=1, mover marks message processed without rewrite.
    {
        auto response = RunDirectMover(setup, TString(kDlqTopic), {{.Offset = 0, .SeqNo = 1}});
        const auto* result = response->Get();
        UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages[0].first, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages[0].second, 1);
    }
}

Y_UNIT_TEST(DirectMove_DestinationNotFound) {
    auto setup = CreateSetup();
    CreateSourceAndDlqTopics(setup, /*createDlq=*/false);

    const auto msg = NUnitTest::RandomString(512);
    setup->Write(TString(kSourceTopic), msg, 0);
    Sleep(TDuration::Seconds(1));

    auto response = RunDirectMover(setup, "/Root/missing-dlq", {{.Offset = 0, .SeqNo = 1}});
    const auto* result = response->Get();
    UNIT_ASSERT_VALUES_UNEQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(!result->ErrorDescription.empty());
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages.size(), 0);
}

Y_UNIT_TEST(DirectMove_InvalidSqsDestinationFormat) {
    auto setup = CreateSetup();
    CreateSourceAndDlqTopics(setup);

    auto response = RunDirectMover(setup, "sqs://user/folder-only", {{.Offset = 0, .SeqNo = 1}});
    const auto* result = response->Get();
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::INTERNAL_ERROR);
    UNIT_ASSERT(result->ErrorDescription.Contains("Unexpected SQS destination topic format"));
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages.size(), 0);
}

Y_UNIT_TEST(DirectMove_WrongSourceTablet) {
    auto setup = CreateSetup();
    CreateSourceAndDlqTopics(setup);

    const auto msg = NUnitTest::RandomString(512);
    setup->Write(TString(kSourceTopic), msg, 0);
    Sleep(TDuration::Seconds(1));

    auto& runtime = setup->GetRuntime();
    const auto parent = runtime.AllocateEdgeActor();
    // Non-existent tablet: pipe delivery fails while fetching the source message.
    const auto moverId = runtime.Register(CreateDLQMover({
        .ParentActorId = parent,
        .Database = TString(kDatabase),
        .TabletId = 999999999ull,
        .PartitionId = 0,
        .ConsumerName = TString(kConsumer),
        .ConsumerGeneration = 1,
        .DestinationTopic = TString(kDlqTopic),
        .Messages = {{.Offset = 0, .SeqNo = 1}},
    }));
    runtime.EnableScheduleForActor(moverId);
    runtime.DispatchEvents();

    auto response = runtime.GrabEdgeEvent<TEvPQ::TEvMLPDLQMoverResponse>(parent, TDuration::Seconds(30));
    UNIT_ASSERT(response);
    const auto* result = response->Get();
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::INTERNAL_ERROR);
    UNIT_ASSERT(result->ErrorDescription.Contains("Source topic unavailable")
        || result->ErrorDescription.Contains("Fetch message failed"));
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages.size(), 0);
}

Y_UNIT_TEST(DirectMove_MissingSourceOffset) {
    auto setup = CreateSetup();
    CreateSourceAndDlqTopics(setup);

    // No writes to source topic — read returns an empty result set.
    auto response = RunDirectMover(setup, TString(kDlqTopic), {{.Offset = 0, .SeqNo = 1}});
    const auto* result = response->Get();
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::INTERNAL_ERROR);
    UNIT_ASSERT(result->ErrorDescription.Contains("empty read result")
        || result->ErrorDescription.Contains("Fetch message failed"));
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages.size(), 0);
}

}

} // namespace NKikimr::NPQ::NMLP
