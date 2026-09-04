#include <ydb/core/persqueue/pqtablet/partition/mlp/mlp_common.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/protos/pqdata_mlp.pb.h>

#include <unordered_set>

namespace NKikimr::NPQ::NMLP {

namespace {

constexpr TStringBuf kDatabase = "/Root";
constexpr TStringBuf kTopic = "/Root/topic1";
constexpr TStringBuf kDlqTopic = "/Root/topic1-dlq";
constexpr TStringBuf kConsumer = "mlp-consumer";

std::deque<TReadMessage> MakeReadMessages(const std::vector<ui64>& offsets) {
    std::deque<TReadMessage> messages;
    const auto ts = TInstant::Now();
    for (ui64 offset : offsets) {
        messages.push_back(TReadMessage{
            .Offset = offset,
            .ApproximateReceiveCount = 1,
            .ApproximateFirstReceiveTimestamp = ts,
        });
    }
    return messages;
}

} // namespace

Y_UNIT_TEST_SUITE(TMLPEnricherSimThreadsTests) {

Y_UNIT_TEST(PipeBreakOnFetchReturnsShutdown) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, TString(kTopic), TString(kConsumer));
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, TString(kTopic), "enrich-me");

    auto& runtime = setup->GetRuntime();
    const ui64 tabletId = GetTabletId(setup, TString(kDatabase), TString(kTopic), 0);
    const auto edge = runtime.AllocateEdgeActor();

    TPipeBreakGuard pipeBreak(runtime, { TEvPersQueue::TEvRequest::EventType });

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(edge, 42, MakeReadMessages({0})));
    const auto enricherId = runtime.Register(
        CreateMessageEnricher(tabletId, 0, TString(kConsumer), std::move(replies)));
    runtime.EnableScheduleForActor(enricherId);

    auto error = runtime.GrabEdgeEvent<TEvPQ::TEvMLPErrorResponse>(edge, TDuration::Seconds(30));
    UNIT_ASSERT(error);
    UNIT_ASSERT_VALUES_EQUAL(error->Cookie, 42);
    UNIT_ASSERT_VALUES_EQUAL(error->Get()->Record.GetStatus(), Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT(error->Get()->Record.GetErrorMessage().Contains("Shutdown"));
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
}

Y_UNIT_TEST(PipeBreakThenSecondEnricherSucceeds) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, TString(kTopic), TString(kConsumer));
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, TString(kTopic), "after-break");

    auto& runtime = setup->GetRuntime();
    const ui64 tabletId = GetTabletId(setup, TString(kDatabase), TString(kTopic), 0);

    {
        const auto edge = runtime.AllocateEdgeActor();
        TPipeBreakGuard pipeBreak(runtime, { TEvPersQueue::TEvRequest::EventType });
        std::deque<TReadResult> replies;
        replies.push_back(TReadResult(edge, 1, MakeReadMessages({0})));
        const auto enricherId = runtime.Register(
            CreateMessageEnricher(tabletId, 0, TString(kConsumer), std::move(replies)));
        runtime.EnableScheduleForActor(enricherId);
        auto error = runtime.GrabEdgeEvent<TEvPQ::TEvMLPErrorResponse>(edge, TDuration::Seconds(30));
        UNIT_ASSERT(error);
        UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
    }

    {
        const auto edge = runtime.AllocateEdgeActor();
        std::deque<TReadResult> replies;
        replies.push_back(TReadResult(edge, 2, MakeReadMessages({0})));
        const auto enricherId = runtime.Register(
            CreateMessageEnricher(tabletId, 0, TString(kConsumer), std::move(replies)));
        runtime.EnableScheduleForActor(enricherId);
        auto response = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge, TDuration::Seconds(30));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, 2);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.MessageSize(), 1);
        UNIT_ASSERT(response->Get()->Record.GetMessage(0).GetData().Contains("after-break"));
    }
}

} // Y_UNIT_TEST_SUITE(TMLPEnricherSimThreadsTests)

Y_UNIT_TEST_SUITE(TMLPDLQMoverSimThreadsTests) {

Y_UNIT_TEST(PipeBreakOnFetchReturnsSourceUnavailable) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, TString(kDlqTopic), TString(kConsumer));
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    {
        auto status = CreatePipeTopic(setup, TString(kTopic), TString(kConsumer));
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, TString(kTopic), "move-me");

    auto& runtime = setup->GetRuntime();
    const ui64 tabletId = GetTabletId(setup, TString(kDatabase), TString(kTopic), 0);
    const auto parent = runtime.AllocateEdgeActor();

    // Break only source-tablet fetches; DLQ partition writer uses another tablet id.
    TPipeBreakGuard pipeBreak(runtime, { TEvPersQueue::TEvRequest::EventType },
        /*maxBreaks=*/Max<size_t>(), tabletId);

    const auto moverId = runtime.Register(CreateDLQMover({
        .ParentActorId = parent,
        .Database = TString(kDatabase),
        .TabletId = tabletId,
        .PartitionId = 0,
        .ConsumerName = TString(kConsumer),
        .ConsumerGeneration = 1,
        .DestinationTopic = TString(kDlqTopic),
        .Messages = {{.Offset = 0, .SeqNo = 1}},
    }));
    runtime.EnableScheduleForActor(moverId);

    auto response = runtime.GrabEdgeEvent<TEvPQ::TEvMLPDLQMoverResponse>(parent, TDuration::Seconds(60));
    UNIT_ASSERT(response);
    const auto* result = response->Get();
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::INTERNAL_ERROR);
    UNIT_ASSERT(result->ErrorDescription.Contains("Source topic unavailable"));
    UNIT_ASSERT_VALUES_EQUAL(result->MovedMessages.size(), 0);
    UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
}

Y_UNIT_TEST(PipeBreakThenRetrySucceeds) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, TString(kDlqTopic), TString(kConsumer));
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    {
        auto status = CreatePipeTopic(setup, TString(kTopic), TString(kConsumer));
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
    WriteViaMlp(setup, TString(kTopic), "retry-move");

    auto& runtime = setup->GetRuntime();
    const ui64 tabletId = GetTabletId(setup, TString(kDatabase), TString(kTopic), 0);

    {
        const auto parent = runtime.AllocateEdgeActor();
        TPipeBreakGuard pipeBreak(runtime, { TEvPersQueue::TEvRequest::EventType },
            /*maxBreaks=*/Max<size_t>(), tabletId);
        const auto moverId = runtime.Register(CreateDLQMover({
            .ParentActorId = parent,
            .Database = TString(kDatabase),
            .TabletId = tabletId,
            .PartitionId = 0,
            .ConsumerName = TString(kConsumer),
            .ConsumerGeneration = 1,
            .DestinationTopic = TString(kDlqTopic),
            .Messages = {{.Offset = 0, .SeqNo = 1}},
        }));
        runtime.EnableScheduleForActor(moverId);
        auto response = runtime.GrabEdgeEvent<TEvPQ::TEvMLPDLQMoverResponse>(parent, TDuration::Seconds(60));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, Ydb::StatusIds::INTERNAL_ERROR);
        UNIT_ASSERT_GE(pipeBreak.BrokenCount(), 1u);
    }

    {
        const auto parent = runtime.AllocateEdgeActor();
        const auto moverId = runtime.Register(CreateDLQMover({
            .ParentActorId = parent,
            .Database = TString(kDatabase),
            .TabletId = tabletId,
            .PartitionId = 0,
            .ConsumerName = TString(kConsumer),
            .ConsumerGeneration = 1,
            .DestinationTopic = TString(kDlqTopic),
            .Messages = {{.Offset = 0, .SeqNo = 1}},
        }));
        runtime.EnableScheduleForActor(moverId);
        auto response = runtime.GrabEdgeEvent<TEvPQ::TEvMLPDLQMoverResponse>(parent, TDuration::Seconds(60));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Status, Ydb::StatusIds::SUCCESS,
            response->Get()->ErrorDescription);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->MovedMessages.size(), 1);
    }
}

} // Y_UNIT_TEST_SUITE(TMLPDLQMoverSimThreadsTests)

Y_UNIT_TEST_SUITE(TMLPConsumerSimThreadsTests) {

Y_UNIT_TEST(ConcurrentReadsNoDuplicateOffsets) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, TString(kTopic), TString(kConsumer));
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }

    constexpr size_t messageCount = 8;
    for (size_t i = 0; i < messageCount; ++i) {
        WriteViaMlp(setup, TString(kTopic), TStringBuilder() << "race-" << i);
    }

    auto& runtime = setup->GetRuntime();

    // Queue several readers before waiting — under UseRealThreads=false their
    // MLPReadRequests hit the consumer while prior reads are still in-flight.
    constexpr size_t readerCount = 8;
    for (size_t i = 0; i < readerCount; ++i) {
        CreateReaderActor(runtime, {
            .DatabasePath = TString(kDatabase),
            .TopicName = TString(kTopic),
            .Consumer = TString(kConsumer),
            .WaitTime = TDuration::Seconds(5),
            .ProcessingTimeout = TDuration::Seconds(30),
            .MaxNumberOfMessage = 1,
        });
    }

    std::unordered_set<ui64> offsets;
    size_t emptyReads = 0;
    for (size_t i = 0; i < readerCount; ++i) {
        auto response = GetReadResponse(runtime, TDuration::Seconds(60));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        if (response->Messages.empty()) {
            ++emptyReads;
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), 1);
        const ui64 offset = response->Messages[0].MessageId.Offset;
        UNIT_ASSERT_C(offsets.insert(offset).second, "duplicate offset " << offset);
    }

    UNIT_ASSERT_VALUES_EQUAL(offsets.size() + emptyReads, readerCount);
    UNIT_ASSERT_VALUES_EQUAL(offsets.size(), messageCount);
}

Y_UNIT_TEST(ConcurrentBatchReadsPartitionMessages) {
    auto setup = CreatePipeSetup();
    {
        auto status = CreatePipeTopic(setup, TString(kTopic), TString(kConsumer));
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }

    constexpr size_t messageCount = 10;
    for (size_t i = 0; i < messageCount; ++i) {
        WriteViaMlp(setup, TString(kTopic), TStringBuilder() << "batch-" << i);
    }

    auto& runtime = setup->GetRuntime();
    CreateReaderActor(runtime, {
        .DatabasePath = TString(kDatabase),
        .TopicName = TString(kTopic),
        .Consumer = TString(kConsumer),
        .WaitTime = TDuration::Seconds(5),
        .ProcessingTimeout = TDuration::Seconds(30),
        .MaxNumberOfMessage = 5,
    });
    CreateReaderActor(runtime, {
        .DatabasePath = TString(kDatabase),
        .TopicName = TString(kTopic),
        .Consumer = TString(kConsumer),
        .WaitTime = TDuration::Seconds(5),
        .ProcessingTimeout = TDuration::Seconds(30),
        .MaxNumberOfMessage = 5,
    });

    std::unordered_set<ui64> offsets;
    for (size_t i = 0; i < 2; ++i) {
        auto response = GetReadResponse(runtime, TDuration::Seconds(60));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        for (const auto& msg : response->Messages) {
            UNIT_ASSERT_C(offsets.insert(msg.MessageId.Offset).second,
                "duplicate offset " << msg.MessageId.Offset);
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(offsets.size(), messageCount);
}

} // Y_UNIT_TEST_SUITE(TMLPConsumerSimThreadsTests)

} // namespace NKikimr::NPQ::NMLP
