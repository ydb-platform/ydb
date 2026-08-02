#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPPipeBreakTests) {

Y_UNIT_TEST(WriterPipeBreakReturnsInternalError) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }

    auto& runtime = setup->GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPersQueue::TEvRequest::EventType });

    CreateWriterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Messages = {
            {
                .Index = 0,
                .MessageBody = "message_body",
            }
        }
    });

    auto response = GetWriteResponse(runtime);
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->DescribeStatus, NDescriber::EStatus::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Status, Ydb::StatusIds::INTERNAL_ERROR);
    UNIT_ASSERT(!response->Messages[0].MessageId.has_value());
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
}

Y_UNIT_TEST(WriterPipeBreakThenSuccess) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }

    auto& runtime = setup->GetRuntime();
    {
        TPipeBreakGuard pipeBreak(runtime, { TEvPersQueue::TEvRequest::EventType }, /*maxBreaks=*/1);
        CreateWriterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Messages = {{.Index = 0, .MessageBody = "first-attempt"}},
        });
        auto response = GetWriteResponse(runtime, TDuration::Seconds(30));
        UNIT_ASSERT(response);
        // Single break may surface as INTERNAL_ERROR or still succeed if writer retries.
        UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
        if (response->Messages.size() == 1 && response->Messages[0].Status == Ydb::StatusIds::SUCCESS) {
            return;
        }
    }

    CreateWriterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Messages = {{.Index = 0, .MessageBody = "retry-body"}},
    });
    auto response = GetWriteResponse(runtime, TDuration::Seconds(30));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Status, Ydb::StatusIds::SUCCESS);
}

Y_UNIT_TEST(ReaderPipeBreakOnSelectPartitionThenSuccess) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, "/Root/topic1", "msg-1");

    auto& runtime = setup->GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvMLPGetPartitionRequest::EventType }, /*maxBreaks=*/1);

    CreateReaderActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .WaitTime = TDuration::Seconds(1),
        .ProcessingTimeout = TDuration::Seconds(30),
        .MaxNumberOfMessage = 1,
    });

    auto response = GetReadResponse(runtime, TDuration::Seconds(30));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
    UNIT_ASSERT_VALUES_EQUAL(pipeBreak.BrokenCount(), 1u);
}

Y_UNIT_TEST(ReaderPipeBreakOnReadThenSuccess) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, "/Root/topic1", "msg-1");

    auto& runtime = setup->GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvMLPReadRequest::EventType }, /*maxBreaks=*/1);

    CreateReaderActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .WaitTime = TDuration::Seconds(1),
        .ProcessingTimeout = TDuration::Seconds(30),
        .MaxNumberOfMessage = 1,
    });

    auto response = GetReadResponse(runtime, TDuration::Seconds(30));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages[0].Data, "msg-1");
    UNIT_ASSERT_VALUES_EQUAL(pipeBreak.BrokenCount(), 1u);
}

Y_UNIT_TEST(ReaderPipeBreakExhausted) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, "/Root/topic1", "msg-1");

    auto& runtime = setup->GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, {
        TEvPQ::TEvMLPGetPartitionRequest::EventType,
        TEvPQ::TEvMLPReadRequest::EventType,
    });

    CreateReaderActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .WaitTime = TDuration::Seconds(0),
        .ProcessingTimeout = TDuration::Seconds(5),
        .MaxNumberOfMessage = 1,
    });

    auto response = GetReadResponse(runtime, TDuration::Seconds(60));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::INTERNAL_ERROR);
    UNIT_ASSERT_VALUES_EQUAL(response->ErrorDescription, "Pipe error");
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 5u);
}

Y_UNIT_TEST(CommitterPipeBreakReturnsFailed) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, "/Root/topic1", "msg-1");

    auto& runtime = setup->GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvMLPCommitRequest::EventType });

    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0) }
    });

    auto result = GetChangeResponse(runtime, TDuration::Seconds(30));
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Failed);
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
}

Y_UNIT_TEST(CommitterPipeBreakThenSuccess) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, "/Root/topic1", "msg-1");

    auto& runtime = setup->GetRuntime();
    {
        TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvMLPCommitRequest::EventType }, /*maxBreaks=*/1);
        CreateCommitterActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { TMessageId(0, 0) }
        });
        auto result = GetChangeResponse(runtime, TDuration::Seconds(30));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Failed);
        UNIT_ASSERT_VALUES_EQUAL(pipeBreak.BrokenCount(), 1u);
    }

    // Changer does not retry; a new actor succeeds after the transient break.
    CreateCommitterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { TMessageId(0, 0) }
    });
    auto result = GetChangeResponse(runtime, TDuration::Seconds(30));
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Success
        || result->Messages[0].Status == EOperationResult::NotFound);
}

Y_UNIT_TEST(UnlockerPipeBreakReturnsFailed) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, "/Root/topic1", "msg-1");

    auto& runtime = setup->GetRuntime();
    TMessageId messageId;
    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime, TDuration::Seconds(30));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        messageId = response->Messages[0].MessageId;
    }

    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvMLPUnlockRequest::EventType });
    CreateUnlockerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { messageId }
    });

    auto result = GetChangeResponse(runtime, TDuration::Seconds(30));
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Failed);
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
}

Y_UNIT_TEST(DeadlineChangerPipeBreakReturnsFailed) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, "/Root/topic1", "msg-1");

    auto& runtime = setup->GetRuntime();
    TMessageId messageId;
    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime, TDuration::Seconds(30));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        messageId = response->Messages[0].MessageId;
    }

    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvMLPChangeMessageDeadlineRequest::EventType });
    CreateMessageDeadlineChangerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { messageId },
        .Deadlines = { TInstant::Now() + TDuration::Minutes(5) },
    });

    auto result = GetChangeResponse(runtime, TDuration::Seconds(30));
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(result->Messages.size(), 1);
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Failed);
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
}

Y_UNIT_TEST(DeadlineChangerPipeBreakThenSuccess) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, "/Root/topic1", "msg-1");

    auto& runtime = setup->GetRuntime();
    TMessageId messageId;
    {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(1),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
        auto response = GetReadResponse(runtime, TDuration::Seconds(30));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        messageId = response->Messages[0].MessageId;
    }

    {
        TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvMLPChangeMessageDeadlineRequest::EventType }, /*maxBreaks=*/1);
        CreateMessageDeadlineChangerActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .Messages = { messageId },
            .Deadlines = { TInstant::Now() + TDuration::Minutes(5) },
        });
        auto result = GetChangeResponse(runtime, TDuration::Seconds(30));
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Failed);
        UNIT_ASSERT_VALUES_EQUAL(pipeBreak.BrokenCount(), 1u);
    }

    CreateMessageDeadlineChangerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer",
        .Messages = { messageId },
        .Deadlines = { TInstant::Now() + TDuration::Minutes(5) },
    });
    auto result = GetChangeResponse(runtime, TDuration::Seconds(30));
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(result->Messages[0].Status == EOperationResult::Success);
}

Y_UNIT_TEST(DescriberPipeBreakThenSuccess) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }

    auto& runtime = setup->GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvMLPGetRuntimeAttributesRequest::EventType }, /*maxBreaks=*/1);

    CreateDescriberActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer"
    });

    auto result = GetDescribeResponse(runtime);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::SUCCESS, result->ErrorDescription);
    UNIT_ASSERT_VALUES_EQUAL(pipeBreak.BrokenCount(), 1u);
}

Y_UNIT_TEST(DescriberPipeBreakExhausted) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }

    auto& runtime = setup->GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvMLPGetRuntimeAttributesRequest::EventType });

    CreateDescriberActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer"
    });

    auto result = GetDescribeResponse(runtime);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::INTERNAL_ERROR);
    UNIT_ASSERT_VALUES_EQUAL(result->ErrorDescription, "Pipe error");
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 5u);
}

Y_UNIT_TEST(PurgerPipeBreakThenSuccess) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, "/Root/topic1", "msg-1");

    auto& runtime = setup->GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvMLPPurgeRequest::EventType }, /*maxBreaks=*/1);

    CreatePurgerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer"
    });

    AssertPurgeOK(runtime, TDuration::Seconds(30));
    UNIT_ASSERT_VALUES_EQUAL(pipeBreak.BrokenCount(), 1u);
}

Y_UNIT_TEST(PurgerPipeBreakExhausted) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, "/Root/topic1", "mlp-consumer");
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, "/Root/topic1", "msg-1");

    auto& runtime = setup->GetRuntime();
    TPipeBreakGuard pipeBreak(runtime, { TEvPQ::TEvMLPPurgeRequest::EventType });

    CreatePurgerActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Consumer = "mlp-consumer"
    });

    auto response = GetPurgeResponse(runtime, TDuration::Seconds(60));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::INTERNAL_ERROR);
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 5u);
}

} // Y_UNIT_TEST_SUITE(TMLPPipeBreakTests)

} // namespace NKikimr::NPQ::NMLP
