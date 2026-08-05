#include <ydb/core/persqueue/public/mlp/ut/common/common.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPReaderTests) {

    Y_UNIT_TEST(TopicNotExists) {
        auto setup = CreateSetup();

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic_not_exists",
            .Consumer = "consumer_not_exists"
        });

        AssertReadError(runtime, Ydb::StatusIds::SCHEME_ERROR,
            "You do not have access permissions or the '/Root/topic_not_exists' does not exist");
    }

    Y_UNIT_TEST(TopicWithoutConsumer) {
        auto setup = CreateSetup();

        ExecuteDDL(*setup, "CREATE TOPIC topic1");

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "consumer_not_exists"
        });

        AssertReadError(runtime, Ydb::StatusIds::SCHEME_ERROR,
            "Consumer 'consumer_not_exists' does not exist");
    }

    Y_UNIT_TEST(EmptyTopic) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer"
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
    }

    Y_UNIT_TEST(TopicWithData) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer");
        setup->Write("/Root/topic1", "msg-1", 0);

        auto now = TInstant::Now().MilliSeconds();

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(3),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Codec, Ydb::Topic::CODEC_RAW);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].ApproximateReceiveCount, 1);
        UNIT_ASSERT(response->Messages[0].ApproximateFirstReceiveTimestamp.has_value());
        UNIT_ASSERT_GE_C(response->Messages[0].ApproximateFirstReceiveTimestamp->MilliSeconds(), now,
            "ApproximateFirstReceiveTimestamp=" << response->Messages[0].ApproximateFirstReceiveTimestamp->MilliSeconds() << " now=" << now);
    }

    Y_UNIT_TEST(CompressedMessage) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer");
        setup->Write("/Root/topic1", "msg-1", 0, std::nullopt, std::nullopt, NYdb::NTopic::ECodec::GZIP);

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(3),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Codec, Ydb::Topic::CODEC_GZIP);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].ApproximateReceiveCount, 1);

        const NYdb::NTopic::ICodec* codecImpl = NYdb::NTopic::TCodecMap::GetTheCodecMap().GetOrThrow(static_cast<ui32>(Ydb::Topic::CODEC_GZIP));
        auto data = codecImpl->Decompress(response->Messages[0].Data);
        UNIT_ASSERT_VALUES_EQUAL(data, "msg-1");
    }

    Y_UNIT_TEST(TopicWithManyIterationsData) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer");
        setup->Write("/Root/topic1", "msg-1", 0);
        setup->Write("/Root/topic1", "msg-2", 0);
        setup->Write("/Root/topic1", "msg-3", 0);

        auto& runtime = setup->GetRuntime();

        Sleep(TDuration::Seconds(2));

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(1),
                .ProcessingTimeout = TDuration::Seconds(2),
                .MaxNumberOfMessage = 2,
            });

            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].ApproximateReceiveCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[1].Data, "msg-2");
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[1].ApproximateReceiveCount, 1);
        }

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(0),
                .ProcessingTimeout = TDuration::Seconds(5),
                .MaxNumberOfMessage = 10,
            });

            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-3");
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].ApproximateReceiveCount, 1);
        }

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(0),
                .ProcessingTimeout = TDuration::Seconds(2),
                .MaxNumberOfMessage = 2,
            });

            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
        }

        Sleep(TDuration::Seconds(2));

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(5),
                .ProcessingTimeout = TDuration::Seconds(2),
                .MaxNumberOfMessage = 2,
            });

            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].ApproximateReceiveCount, 2);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[1].Data, "msg-2");
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[1].ApproximateReceiveCount, 2);
        }
    }

    Y_UNIT_TEST(TopicWithBigMessage) {
        auto setup = CreateSetup();

        auto bigMessage = NUnitTest::RandomString(1_MB);

        CreateTopic(setup, "/Root/topic1", "mlp-consumer");
        setup->Write("/Root/topic1", bigMessage, 0);

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(3),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, bigMessage);
    }


    Y_UNIT_TEST(TopicWithKeepMessageOrder) {
        auto setup = CreateSetup();
        auto& runtime = setup->GetRuntime();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1, true);

        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 0,
                    .MessageBody = "message_body_1",
                    .MessageGroupId = "message_group_id_1",
                    .MessageDeduplicationId = "deduplication-id-1"
                },
                {
                    .Index = 1,
                    .MessageBody = "message_body_2",
                    .MessageGroupId = "message_group_id_1",
                    .MessageDeduplicationId = "deduplication-id-2"
                },
                {
                    .Index = 2,
                    .MessageBody = "message_body_3",
                    .MessageGroupId = "message_group_id_2",
                    .MessageDeduplicationId = "deduplication-id-3"
                }
            }
        });

        {
            auto response = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 3);
        }

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(3),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });

        {
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 0);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "message_body_1");
        }

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(3),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });

        {
            // message with offset 1 has been skipped because his message group equals message groups of the first message
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, 2);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "message_body_3");
        }
    }

    Y_UNIT_TEST(SkipMessageGroups) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1, true);

        auto& runtime = setup->GetRuntime();
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {
                {
                    .Index = 0,
                    .MessageBody = "message_a",
                    .MessageGroupId = "group_a",
                },
                {
                    .Index = 1,
                    .MessageBody = "message_b",
                    .MessageGroupId = "group_b",
                },
            }
        });
        {
            auto response = GetWriteResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 2);
        }

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 10,
            .SkipMessageGroups = { "group_a" },
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "message_b");
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageGroupId, "group_b");
    }

    Y_UNIT_TEST(ReceiveAttemptIdReplay) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1);
        setup->Write("/Root/topic1", "msg-1", 0);
        setup->Write("/Root/topic1", "msg-2", 0);

        auto& runtime = setup->GetRuntime();
        const TString receiveAttemptId = "attempt-replay";

        TVector<TMessageId> firstIds;
        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(1),
                .ProcessingTimeout = TDuration::Seconds(30),
                .MaxNumberOfMessage = 10,
                .ReceiveAttemptId = receiveAttemptId,
            });
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 2);
            for (const auto& msg : response->Messages) {
                firstIds.push_back(msg.MessageId);
            }
        }

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(0),
                .ProcessingTimeout = TDuration::Seconds(30),
                .MaxNumberOfMessage = 10,
                .ReceiveAttemptId = receiveAttemptId,
            });
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), firstIds.size());
            for (size_t i = 0; i < firstIds.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(response->Messages[i].MessageId.PartitionId, firstIds[i].PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(response->Messages[i].MessageId.Offset, firstIds[i].Offset);
            }
        }
    }

    Y_UNIT_TEST(ReceiveAttemptIdAfterPartialCommit) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1);
        setup->Write("/Root/topic1", "msg-1", 0);
        setup->Write("/Root/topic1", "msg-2", 0);

        auto& runtime = setup->GetRuntime();
        const TString receiveAttemptId = "attempt-partial-commit";

        TVector<TMessageId> firstIds;
        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(1),
                .ProcessingTimeout = TDuration::Seconds(30),
                .MaxNumberOfMessage = 10,
                .ReceiveAttemptId = receiveAttemptId,
            });
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 2);
            for (const auto& msg : response->Messages) {
                firstIds.push_back(msg.MessageId);
            }
        }

        // Commit only the first message — attempt replay must be invalidated.
        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { firstIds[0] },
        });
        {
            auto commit = GetChangeResponse(runtime);
            UNIT_ASSERT(commit);
            UNIT_ASSERT(commit->Messages[0].Status == EOperationResult::Success);
        }

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(0),
                .ProcessingTimeout = TDuration::Seconds(30),
                .MaxNumberOfMessage = 10,
                .ReceiveAttemptId = receiveAttemptId,
            });
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
            // Same attempt id no longer replays; remaining message stays locked.
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
        }

        CreateUnlockerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { firstIds[1] },
        });
        {
            auto unlock = GetChangeResponse(runtime);
            UNIT_ASSERT(unlock);
            UNIT_ASSERT(unlock->Messages[0].Status == EOperationResult::Success);
        }

        {
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(1),
                .ProcessingTimeout = TDuration::Seconds(30),
                .MaxNumberOfMessage = 10,
                .ReceiveAttemptId = "attempt-after-invalidation",
            });
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.Offset, firstIds[1].Offset);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-2");
        }
    }

    Y_UNIT_TEST(MaxNumberOfMessageZero) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer");
        setup->Write("/Root/topic1", "msg-1", 0);

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(0),
            .ProcessingTimeout = TDuration::Seconds(5),
            .MaxNumberOfMessage = 0,
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
    }

    Y_UNIT_TEST(MaxNumberOfMessageLarge) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer");
        setup->Write("/Root/topic1", "msg-1", 0);
        setup->Write("/Root/topic1", "msg-2", 0);
        setup->Write("/Root/topic1", "msg-3", 0);

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1000,
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 3);
    }

    Y_UNIT_TEST(ReadAfterPQRBReboot) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer");
        setup->Write("/Root/topic1", "msg-1", 0);

        ReloadPQRBTablet(setup, "/Root", "/Root/topic1");

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
    }

    Y_UNIT_TEST(UnauthorizedReader) {
        auto setup = CreateSetup();
        CreateTopic(setup, "/Root/topic1", "mlp-consumer");

        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@staff");
        ModifyTopicAcl(*setup, "topic1", acl);

        auto& runtime = setup->GetRuntime();
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .UserToken = MakeIntrusiveConst<NACLib::TUserToken>("bad-user@staff", TVector<TString>{}),
        });

        auto response = GetReadResponse(runtime);
        UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SCHEME_ERROR);
        UNIT_ASSERT(!response->ErrorDescription.empty());
    }

    Y_UNIT_TEST(ReceiveAttemptIdPartitionRouting) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer", 2);
        setup->Write("/Root/topic1", "msg-1", 0);

        auto& runtime = setup->GetRuntime();
        TString successfulAttemptId;

        for (ui32 attempt = 0; attempt < 20; ++attempt) {
            const TString receiveAttemptId = TStringBuilder() << "attempt-" << attempt;
            CreateReaderActor(runtime, {
                .DatabasePath = "/Root",
                .TopicName = "/Root/topic1",
                .Consumer = "mlp-consumer",
                .WaitTime = TDuration::Seconds(0),
                .ProcessingTimeout = TDuration::Seconds(30),
                .MaxNumberOfMessage = 1,
                .ReceiveAttemptId = receiveAttemptId,
            });

            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
            if (response->Messages.size() == 1 && response->Messages[0].Data == "msg-1") {
                successfulAttemptId = receiveAttemptId;
                break;
            }
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 0);
        }
        UNIT_ASSERT(!successfulAttemptId.empty());

        ReloadPQRBTablet(setup, "/Root", "/Root/topic1");

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(0),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
            .ReceiveAttemptId = successfulAttemptId,
        });

        {
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
        }
    }

    Y_UNIT_TEST(ReceiveAttemptIdPartitionRoutingAfterPQReboot) {
        auto setup = CreateSetup();

        CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1);
        setup->Write("/Root/topic1", "msg-1", 0);

        auto& runtime = setup->GetRuntime();
        const TString receiveAttemptId = "attempt-0";

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(0),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
            .ReceiveAttemptId = receiveAttemptId,
        });

        {
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
        }

        ReloadPQTablet(setup, "/Root", "/Root/topic1", 0);

        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(0),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
            .ReceiveAttemptId = receiveAttemptId,
        });

        {
            auto response = GetReadResponse(runtime);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
            UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].MessageId.PartitionId, 0);
        }
    }

}

} // namespace NKikimr::NPQ::NMLP
