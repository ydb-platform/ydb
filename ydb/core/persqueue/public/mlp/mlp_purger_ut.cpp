#include <ydb/core/persqueue/public/mlp/ut/common/common.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPPurgerTests) {

    Y_UNIT_TEST(TopicNotExists) {
        auto setup = CreateSetup();

        auto& runtime = setup->GetRuntime();
        CreatePurgerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic_not_exists",
            .Consumer = "consumer_not_exists"
        });

        AssertPurgeError(runtime, Ydb::StatusIds::SCHEME_ERROR,
            "You do not have access permissions or the '/Root/topic_not_exists' does not exist");
    }

    Y_UNIT_TEST(TopicWithoutConsumer) {
        auto setup = CreateSetup();

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        CreatePurgerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "consumer_not_exists"
        });

        AssertPurgeError(runtime, Ydb::StatusIds::SCHEME_ERROR,
            "Consumer 'consumer_not_exists' does not exist");
    }

    Y_UNIT_TEST(EmptyTopic) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        auto& runtime = setup->GetRuntime();
        CreatePurgerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer"
        });

        AssertPurgeOK(runtime);
    }

    Y_UNIT_TEST(TopicWithData) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        auto& runtime = setup->GetRuntime();

        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 3,
                    .MessageBody = "message_body",
                }
            }
        });

        {
            auto response = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        }

        CreatePurgerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer"
        });
        AssertPurgeOK(runtime);

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(0),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);

        auto describe = setup->DescribeConsumer("/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_VALUES_EQUAL(describe.GetPartitions()[0].GetPartitionConsumerStats()->GetCommittedOffset(), 1);
    }

    Y_UNIT_TEST(PurgeWhileMessagesLocked) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer");
        setup->Write("/Root/topic1", "msg-1", 0);
        setup->Write("/Root/topic1", "msg-2", 0);

        auto& runtime = setup->GetRuntime();

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(1),
                .ProcessingTimeout = TDuration::Seconds(30),
                .MaxNumberOfMessage = 1,
            });
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        }

        CreatePurgerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer"
        });
        AssertPurgeOK(runtime);

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(0),
                .ProcessingTimeout = TDuration::Seconds(30),
                .MaxNumberOfMessage = 10,
            });
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
        }

        auto describe = setup->DescribeConsumer("/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_VALUES_EQUAL(describe.GetPartitions()[0].GetPartitionConsumerStats()->GetCommittedOffset(), 2);
    }

    Y_UNIT_TEST(UnauthorizedPurger) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, "user1@staff");
        ModifyTopicAcl(*setup, "topic1", acl);

        auto& runtime = setup->GetRuntime();
        CreatePurgerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .UserToken = MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{}),
        });

        auto response = GetPurgeResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SCHEME_ERROR);
        UNIT_ASSERT(!response->ErrorDescription.empty());
    }

    Y_UNIT_TEST(TopicWithManyPartitionAndData) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer", 10);

        auto& runtime = setup->GetRuntime();

        for (size_t i = 0; i < 10; ++i) {
            CreateWriterActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Messages = {
                    {
                        .Index = 3,
                        .MessageBody = "message_body",
                    }
                }
            });

            {
                auto response = GetWriteResponse(runtime);
                UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            }
        }

        CreatePurgerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer"
        });
        AssertPurgeOK(runtime);

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(0),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
    }

}

}
