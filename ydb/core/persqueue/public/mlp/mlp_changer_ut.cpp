#include <ydb/core/persqueue/public/mlp/ut/common/common.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPChangerTests) {

Y_UNIT_TEST(TopicNotExists) {
    auto setup = CreateSetup();

    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic_not_exists",
        .Consumer = "consumer_not_exists",
        .Messages = { TMessageId(0, 0) }
    });

    auto result = GetChangeResponse(runtime);

    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SCHEME_ERROR);
}


Y_UNIT_TEST(ConsumerNotExists) {
    auto setup = CreateSetup();

    ExecuteDDL(*setup, "CREATE TOPIC topic1");

    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "consumer_not_exists",
        .Messages = { TMessageId(0, 0) }
    });

    auto result = GetChangeResponse(runtime);

    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SCHEME_ERROR);
}

Y_UNIT_TEST(PartitionNotExists) {
    auto setup = CreateSetup();

    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(13, 17) }
    });

    auto result = GetChangeResponse(runtime);

    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 13);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 17);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Success, false);
}

Y_UNIT_TEST(CommitTest) {
    auto setup = CreateSetup();

    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);
    setup->Write("/Root/topic1", "msg-2", 0);

    Sleep(TDuration::Seconds(2));

    auto& runtime = setup->GetRuntime();
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0) }
    });

    auto result = GetChangeResponse(runtime);

    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Success, true);

    auto describe = setup->DescribeConsumer("/Root/topic1", "mlp-consumer");
    UNIT_ASSERT_VALUES_EQUAL(describe.GetPartitions()[0].GetPartitionConsumerStats()->GetCommittedOffset(), 1);
}

Y_UNIT_TEST(ReadAndReleaseTest) {
    auto setup = CreateSetup();

    CreateTopic(setup, "/Root/topic1", "mlp-consumer");
    setup->Write("/Root/topic1", "msg-1", 0);
    setup->Write("/Root/topic1", "msg-2", 0);
    setup->Write("/Root/topic1", "msg-3", 0);

    Sleep(TDuration::Seconds(2));

    auto& runtime = setup->GetRuntime();

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 2
        });

        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].MessageId.Offset, 1);
    }

    {
        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 1) }
        });

        auto result = GetChangeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Success, true);
    }

    {
        CreateMessageDeadlineChangerActor(runtime, TMessageDeadlineChangerSettings{
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) },
            .Deadlines = {TInstant::Now() - TDuration::Seconds(1), },
        });

        auto result = GetChangeResponse(runtime);

        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].Success, true);
    }

    Sleep(TDuration::Seconds(2));

    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(5),
            .MaxNumberOfMessage = 2
        });

        // You should receive two messages. With offset 0 because his VisibilityDeadline was changed,
        // which expired, and with offset 2, which has not yet been processed.
        // The message from offset 1 has been deleted.
        auto result = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Messages[1].MessageId.Offset, 2);
    }
}

Y_UNIT_TEST(CapacityTest) {
    return;

    auto setup = CreateSetup();

    CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1, true);

    Cerr << (TStringBuilder() << ">>>>> TOPIC WAS CREATED" << Endl);

    struct State {
        size_t ReadSuccess = 0;
        size_t ReadFailed = 0;
        size_t CommitSuccess = 0;
        size_t CommitFailed = 0;
        size_t WriteSuccess = 0;
        size_t WriteAlreadyExists = 0;
        size_t WriteFailed = 0;
    };

    State state;

    struct TestActor : public TActorBootstrapped<TestActor> {

        TestActor(State& state)
            : State(state) {}

        void Bootstrap() {
            Become(&TestActor::StateWork);
            Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup());
            Next();
        }

        void Next() {
            while (InflightWrite < 250) {
                Register(CreateWriter(SelfId(), TWriterSettings{
                    .DatabasePath = "/Root",
                    .TopicName = "/Root/topic1",
                    .Messages = {{
                        .Index = 0,
                        .MessageBody = Body,
                        .MessageGroupId = TStringBuilder() << "message-group-" << RandomNumber<ui64>(100000),
                        .MessageDeduplicationId = TStringBuilder() << "deduplication-id-" << RandomNumber<ui64>(5000000)
                    }}
                }));

                ++InflightWrite;
            }

            while (Inflight < 300) {
                Register(CreateReader(SelfId(), TReaderSettings{
                    .DatabasePath = "/Root",
                    .TopicName = "/Root/topic1",
                    .Consumer = "mlp-consumer",
                    .WaitTime = TDuration::Seconds(1),
                    .ProcessingTimeout = TDuration::Seconds(5),
                    .MaxNumberOfMessage = 1
                }));

                ++Inflight;
            }
        }

        void Handle(NMLP::TEvWriteResponse::TPtr& ev) {
            --InflightWrite;

            auto& messages = ev->Get()->Messages;
            if (messages.size() != 1 || messages[0].Status == Ydb::StatusIds::INTERNAL_ERROR) {
                State.WriteFailed++;
            } else if (messages[0].Status == Ydb::StatusIds::ALREADY_EXISTS) {
                State.WriteAlreadyExists++;
            } else {
                State.WriteSuccess++;
            }

            Next();
        }

        void Handle(NMLP::TEvReadResponse::TPtr& ev) {
            --Inflight;

            if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
                ++State.ReadSuccess;

                if (!ev->Get()->Messages.empty() && RandomNumber<size_t>(10) > 0) {
                    Register(CreateCommitter(SelfId(), TCommitterSettings{
                        .DatabasePath = "/Root",
                        .TopicName = "/Root/topic1",
                        .Consumer = "mlp-consumer",
                        .Messages = { ev->Get()->Messages[0].MessageId }
                    }));

                    ++Inflight;
                }
            } else {
                ++State.ReadFailed;
            }

            Next();
        }

        void Handle(NMLP::TEvChangeResponse::TPtr& ev) {
            --Inflight;

            if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
                ++State.CommitSuccess;
            } else {
                ++State.CommitFailed;
            }

            Next();
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NMLP::TEvWriteResponse, Handle);
                hFunc(NMLP::TEvReadResponse, Handle);
                hFunc(NMLP::TEvChangeResponse, Handle);
                sFunc(TEvents::TEvPoison, PassAway);
                sFunc(TEvents::TEvWakeup, PassAway);
            }
        }

        size_t Inflight = 0;
        size_t InflightWrite = 0;

        TString Body = NUnitTest::RandomString(10_KB);

        State& State;
    };

    auto& runtime = setup->GetRuntime();
    runtime.Register(new TestActor(state));


    Sleep(TDuration::Seconds(65));

    Cerr << "Total:\n  Read success: " << state.ReadSuccess
        << "\n  Read fail: " << state.ReadFailed
        << "\n  Commit success: " << state.CommitSuccess
        << "\n  Commit fail: " << state.CommitFailed
        << "\n  Write success: " << state.WriteSuccess
        << "\n  Write fail: " << state.WriteFailed
        << "\n  Write deduplicated: " << state.WriteAlreadyExists
        << "\n  RPS: " << (state.ReadSuccess + state.CommitSuccess + state.WriteSuccess + state.WriteAlreadyExists) / 60
        << Endl;
}

}

} // namespace NKikimr::NPQ::NMLP
