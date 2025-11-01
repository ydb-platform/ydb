#include <ydb/core/persqueue/public/mlp/ut/common.h>

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
            .VisibilityTimeout = TDuration::Seconds(30),
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
        CreateMessageDeadlineChangerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) },
            .Deadline = TInstant::Now() - TDuration::Seconds(1)
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
            .VisibilityTimeout = TDuration::Seconds(5),
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
    //return;

    auto setup = CreateSetup();

    CreateTopic(setup, "/Root/topic1", "mlp-consumer");

    Cerr << (TStringBuilder() << ">>>>> TOPIC WAS CREATED" << Endl);

    WriteMany(setup, "/Root/topic1", 0, 10000, 50000);

    Cerr << (TStringBuilder() << ">>>>> MESSAGES WAS WRITTEN" << Endl);

    struct State {
        size_t ReadSuccess = 0;
        size_t ReadFailed = 0;
        size_t CommitSuccess = 0;
        size_t CommitFailed = 0;
    };

    State state;

    struct TestActor : public TActorBootstrapped<TestActor> {

        TestActor(State& state)
            : State(state) {}

        void Bootstrap() {
            Become(&TestActor::StateWork);
            Schedule(TDuration::Seconds(30), new TEvents::TEvWakeup());
            Next();
        }

        void Next() {
            while (Infly < 200) {
                Register(CreateReader(SelfId(), TReaderSettings{
                    .DatabasePath = "/Root",
                    .TopicName = "/Root/topic1",
                    .Consumer = "mlp-consumer",
                    .WaitTime = TDuration::Seconds(1),
                    .VisibilityTimeout = TDuration::Seconds(5),
                    .MaxNumberOfMessage = 1
                }));

                ++Infly;
            }
        }

        void Handle(NMLP::TEvReadResponse::TPtr& ev) {
            --Infly;

            if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
                ++State.ReadSuccess;

                if (!ev->Get()->Messages.empty() && RandomNumber<size_t>(10) > 0) {
                    Register(CreateCommitter(SelfId(), TCommitterSettings{
                        .DatabasePath = "/Root",
                        .TopicName = "/Root/topic1",
                        .Consumer = "mlp-consumer",
                        .Messages = { ev->Get()->Messages[0].MessageId }
                    }));

                    ++Infly;
                }
            } else {
                ++State.ReadFailed;
            }

            Next();
        }

        void Handle(NMLP::TEvChangeResponse::TPtr& ev) {
            --Infly;

            if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
                ++State.CommitSuccess;
            } else {
                ++State.CommitFailed;
            }

            Next();
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NMLP::TEvReadResponse, Handle);
                hFunc(NMLP::TEvChangeResponse, Handle);
                sFunc(TEvents::TEvPoison, PassAway);
                sFunc(TEvents::TEvWakeup, PassAway);
            }            
        }

        size_t Infly = 0;

        State& State;
    };

    auto& runtime = setup->GetRuntime();
    runtime.Register(new TestActor(state));


    Sleep(TDuration::Seconds(35));

    Cerr << "Total:\n  Read success: " << state.ReadSuccess
        << "\n  Read fail: " << state.ReadFailed
        << "\n  Commit success: " << state.CommitSuccess
        << "\n  Commit fail: " << state.CommitFailed
        << Endl;
}

}

} // namespace NKikimr::NPQ::NMLP
