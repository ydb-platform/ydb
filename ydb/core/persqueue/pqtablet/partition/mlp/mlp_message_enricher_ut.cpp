#include "mlp_message_enricher.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/protos/pqdata_mlp.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPEnricherTests) {

namespace {

constexpr TStringBuf kDatabase = "/Root";
constexpr TStringBuf kTopic = "/Root/topic1";
constexpr TStringBuf kConsumer = "mlp-consumer";

std::deque<TReadMessage> MakeReadMessages(
    const std::vector<ui64>& offsets,
    ui32 receiveCount = 1,
    TInstant firstReceive = TInstant::Zero()
) {
    std::deque<TReadMessage> messages;
    const auto ts = firstReceive ? firstReceive : TInstant::Now();
    for (ui64 offset : offsets) {
        messages.push_back(TReadMessage{
            .Offset = offset,
            .ApproximateReceiveCount = receiveCount,
            .ApproximateFirstReceiveTimestamp = ts,
        });
    }
    return messages;
}

void AssertEnrichedResponse(
    const TEvPQ::TEvMLPReadResponse& response,
    const std::vector<std::pair<ui64, TString>>& expected
) {
    UNIT_ASSERT_VALUES_EQUAL(response.Record.MessageSize(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        const auto& msg = response.Record.GetMessage(i);
        UNIT_ASSERT_VALUES_EQUAL_C(msg.GetId().GetOffset(), expected[i].first, i);
        UNIT_ASSERT_C(msg.GetData().Contains(expected[i].second), i);
    }
}

struct TTopicFixture {
    std::shared_ptr<TTopicSdkTestSetup> Setup;
    ui64 TabletId = 0;
    std::vector<TString> Bodies;
    TInstant FirstReceive = TInstant::MilliSeconds(1'700'000'000'000ull);

    explicit TTopicFixture(size_t messageCount = 3) {
        Setup = CreateSetup();
        auto status = CreateTopic(Setup, TString(kTopic), TString(kConsumer));
        UNIT_ASSERT_VALUES_EQUAL_C(status.IsSuccess(), true, status.GetIssues().ToString());

        Bodies.reserve(messageCount);
        std::vector<TWriterSettings::TMessage> writeMessages;
        writeMessages.reserve(messageCount);
        for (size_t i = 0; i < messageCount; ++i) {
            Bodies.push_back(TStringBuilder() << "msg" << i);
            writeMessages.push_back({
                .Index = i,
                .MessageBody = Bodies.back(),
                .MessageGroupId = TStringBuilder() << "group_" << i,
            });
        }
        if (!writeMessages.empty()) {
            CreateWriterActor(Setup->GetRuntime(), {
                .DatabasePath = TString(kDatabase),
                .TopicName = TString(kTopic),
                .Messages = std::move(writeMessages),
            });
            auto writeResponse = GetWriteResponse(Setup->GetRuntime());
            UNIT_ASSERT_VALUES_EQUAL(writeResponse->Messages.size(), messageCount);
        }

        TabletId = GetTabletId(Setup, TString(kDatabase), TString(kTopic), 0);
    }

    NActors::TTestActorRuntime& Runtime() {
        return Setup->GetRuntime();
    }

    TActorId RegisterEnricher(std::deque<TReadResult> replies) {
        const auto enricherId = Runtime().Register(
            CreateMessageEnricher(TabletId, 0, TString(kConsumer), std::move(replies)));
        Runtime().EnableScheduleForActor(enricherId);
        Runtime().DispatchEvents();
        return enricherId;
    }
};

} // namespace

Y_UNIT_TEST(EnrichDuplicateOffsetsAcrossReplies) {
    TTopicFixture fx(3);
    auto& runtime = fx.Runtime();

    const auto edge0 = runtime.AllocateEdgeActor();
    const auto edge1 = runtime.AllocateEdgeActor();

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(edge0, 100, MakeReadMessages({0, 1, 2})));
    replies.push_back(TReadResult(edge1, 200, MakeReadMessages({0, 1, 2})));
    fx.RegisterEnricher(std::move(replies));

    const std::vector<std::pair<ui64, TString>> expected = {
        {0, fx.Bodies[0]},
        {1, fx.Bodies[1]},
        {2, fx.Bodies[2]},
    };

    auto response0 = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge0, TDuration::Seconds(5));
    auto response1 = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge1, TDuration::Seconds(5));
    UNIT_ASSERT(response0);
    UNIT_ASSERT(response1);
    UNIT_ASSERT_VALUES_EQUAL(response0->Cookie, 100);
    UNIT_ASSERT_VALUES_EQUAL(response1->Cookie, 200);

    AssertEnrichedResponse(*response0->Get(), expected);
    AssertEnrichedResponse(*response1->Get(), expected);
}

Y_UNIT_TEST(EnrichSingleReplyWithMetadata) {
    TTopicFixture fx(2);
    auto& runtime = fx.Runtime();
    const auto edge = runtime.AllocateEdgeActor();
    const ui32 receiveCount = 7;

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(edge, 42, MakeReadMessages({0, 1}, receiveCount, fx.FirstReceive)));
    fx.RegisterEnricher(std::move(replies));

    auto response = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Cookie, 42);

    const auto& record = response->Get()->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.MessageSize(), 2);
    for (ui32 i = 0; i < 2; ++i) {
        const auto& msg = record.GetMessage(i);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetId().GetPartitionId(), 0);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetId().GetOffset(), i);
        UNIT_ASSERT(msg.GetData().Contains(fx.Bodies[i]));
        // SourceId/MessageGroupId depends on the write path; only require MLP meta we set ourselves.
        UNIT_ASSERT_VALUES_EQUAL(msg.GetMessageMeta().GetApproximateReceiveCount(), receiveCount);
        UNIT_ASSERT_VALUES_EQUAL(
            msg.GetMessageMeta().GetApproximateFirstReceiveTimestampMilliseconds(),
            fx.FirstReceive.MilliSeconds());
        UNIT_ASSERT(msg.GetMessageMeta().GetSentTimestampMilliseconds() > 0);
    }
}

Y_UNIT_TEST(EnrichUnorderedOffsets) {
    TTopicFixture fx(3);
    auto& runtime = fx.Runtime();
    const auto edge = runtime.AllocateEdgeActor();

    // Requested out of order — enricher sorts and returns by ascending offset.
    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(edge, 1, MakeReadMessages({2, 0, 1})));
    fx.RegisterEnricher(std::move(replies));

    auto response = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(response);
    AssertEnrichedResponse(*response->Get(), {
        {0, fx.Bodies[0]},
        {1, fx.Bodies[1]},
        {2, fx.Bodies[2]},
    });
}

Y_UNIT_TEST(EnrichEmptyReplies) {
    TTopicFixture fx(0);
    auto& runtime = fx.Runtime();
    const auto edge0 = runtime.AllocateEdgeActor();
    const auto edge1 = runtime.AllocateEdgeActor();

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(edge0, 10, {}));
    replies.push_back(TReadResult(edge1, 11, {}));
    fx.RegisterEnricher(std::move(replies));

    auto response0 = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge0, TDuration::Seconds(5));
    auto response1 = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge1, TDuration::Seconds(5));
    UNIT_ASSERT(response0);
    UNIT_ASSERT(response1);
    UNIT_ASSERT_VALUES_EQUAL(response0->Cookie, 10);
    UNIT_ASSERT_VALUES_EQUAL(response1->Cookie, 11);
    UNIT_ASSERT_VALUES_EQUAL(response0->Get()->Record.MessageSize(), 0);
    UNIT_ASSERT_VALUES_EQUAL(response1->Get()->Record.MessageSize(), 0);
}

Y_UNIT_TEST(EnrichAllOffsetsMissing) {
    TTopicFixture fx(1); // only offset 0 exists
    auto& runtime = fx.Runtime();
    const auto edge = runtime.AllocateEdgeActor();

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(edge, 7, MakeReadMessages({5, 6})));
    fx.RegisterEnricher(std::move(replies));

    // Past-end reads may surface as empty results ("Messages were not found") or as a failed
    // fetch that shuts the enricher down with SCHEME_ERROR.
    auto error = runtime.GrabEdgeEvent<TEvPQ::TEvMLPErrorResponse>(edge, TDuration::Seconds(10));
    UNIT_ASSERT(error);
    UNIT_ASSERT_VALUES_EQUAL(error->Cookie, 7);
    const auto status = error->Get()->Record.GetStatus();
    UNIT_ASSERT(status == Ydb::StatusIds::INTERNAL_ERROR || status == Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT(error->Get()->Record.GetErrorMessage().Contains("Messages were not found")
        || error->Get()->Record.GetErrorMessage().Contains("Shutdown"));
}

Y_UNIT_TEST(EnrichGapInOffsets) {
    TTopicFixture fx(3);
    auto& runtime = fx.Runtime();
    const auto edge = runtime.AllocateEdgeActor();

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(edge, 3, MakeReadMessages({0, 2})));
    fx.RegisterEnricher(std::move(replies));

    auto response = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(response);
    AssertEnrichedResponse(*response->Get(), {
        {0, fx.Bodies[0]},
        {2, fx.Bodies[2]},
    });
}

Y_UNIT_TEST(EnrichOverlappingRepliesDifferentSlices) {
    TTopicFixture fx(3);
    auto& runtime = fx.Runtime();
    const auto edge0 = runtime.AllocateEdgeActor();
    const auto edge1 = runtime.AllocateEdgeActor();

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(edge0, 1, MakeReadMessages({0, 1})));
    replies.push_back(TReadResult(edge1, 2, MakeReadMessages({1, 2})));
    fx.RegisterEnricher(std::move(replies));

    auto response0 = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge0, TDuration::Seconds(5));
    auto response1 = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge1, TDuration::Seconds(5));
    UNIT_ASSERT(response0);
    UNIT_ASSERT(response1);

    AssertEnrichedResponse(*response0->Get(), {{0, fx.Bodies[0]}, {1, fx.Bodies[1]}});
    AssertEnrichedResponse(*response1->Get(), {{1, fx.Bodies[1]}, {2, fx.Bodies[2]}});
}

Y_UNIT_TEST(EnrichDeliveryProblemOnWrongTablet) {
    TTopicFixture fx(1);
    auto& runtime = fx.Runtime();
    const auto edge = runtime.AllocateEdgeActor();

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(edge, 55, MakeReadMessages({0})));

    const auto enricherId = runtime.Register(
        CreateMessageEnricher(/*tabletId=*/999999999ull, 0, TString(kConsumer), std::move(replies)));
    runtime.EnableScheduleForActor(enricherId);
    runtime.DispatchEvents();

    auto error = runtime.GrabEdgeEvent<TEvPQ::TEvMLPErrorResponse>(edge, TDuration::Seconds(10));
    UNIT_ASSERT(error);
    UNIT_ASSERT_VALUES_EQUAL(error->Cookie, 55);
    UNIT_ASSERT_VALUES_EQUAL(error->Get()->Record.GetStatus(), Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT(error->Get()->Record.GetErrorMessage().Contains("Shutdown"));
}

Y_UNIT_TEST(EnrichPoisonSendsShutdown) {
    TTopicFixture fx(1);
    auto& runtime = fx.Runtime();
    const auto edge = runtime.AllocateEdgeActor();

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(edge, 77, MakeReadMessages({0})));

    // Register against a non-existent tablet so enrichment stays in-flight, then poison.
    const auto enricherId = runtime.Register(
        CreateMessageEnricher(/*tabletId=*/999999998ull, 0, TString(kConsumer), std::move(replies)));
    runtime.EnableScheduleForActor(enricherId);
    runtime.Send(new IEventHandle(enricherId, edge, new TEvents::TEvPoison()));
    runtime.DispatchEvents();

    auto error = runtime.GrabEdgeEvent<TEvPQ::TEvMLPErrorResponse>(edge, TDuration::Seconds(10));
    UNIT_ASSERT(error);
    UNIT_ASSERT_VALUES_EQUAL(error->Cookie, 77);
    UNIT_ASSERT_VALUES_EQUAL(error->Get()->Record.GetStatus(), Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT(error->Get()->Record.GetErrorMessage().Contains("Shutdown"));
}

Y_UNIT_TEST(EnrichMixedEmptyAndNonEmptyReplies) {
    TTopicFixture fx(2);
    auto& runtime = fx.Runtime();
    const auto emptyEdge = runtime.AllocateEdgeActor();
    const auto dataEdge = runtime.AllocateEdgeActor();

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(emptyEdge, 1, {}));
    replies.push_back(TReadResult(dataEdge, 2, MakeReadMessages({0, 1})));
    fx.RegisterEnricher(std::move(replies));

    auto emptyResponse = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(emptyEdge, TDuration::Seconds(5));
    auto dataResponse = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(dataEdge, TDuration::Seconds(5));
    UNIT_ASSERT(emptyResponse);
    UNIT_ASSERT(dataResponse);
    UNIT_ASSERT_VALUES_EQUAL(emptyResponse->Cookie, 1);
    UNIT_ASSERT_VALUES_EQUAL(emptyResponse->Get()->Record.MessageSize(), 0);
    AssertEnrichedResponse(*dataResponse->Get(), {{0, fx.Bodies[0]}, {1, fx.Bodies[1]}});
}

Y_UNIT_TEST(EnrichDifferentReceiveCounts) {
    TTopicFixture fx(2);
    auto& runtime = fx.Runtime();
    const auto edge = runtime.AllocateEdgeActor();
    const auto ts = fx.FirstReceive;

    std::deque<TReadMessage> messages;
    messages.push_back({.Offset = 0, .ApproximateReceiveCount = 1, .ApproximateFirstReceiveTimestamp = ts});
    messages.push_back({.Offset = 1, .ApproximateReceiveCount = 9, .ApproximateFirstReceiveTimestamp = ts});

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(edge, 3, std::move(messages)));
    fx.RegisterEnricher(std::move(replies));

    auto response = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(response);
    const auto& record = response->Get()->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.MessageSize(), 2);
    UNIT_ASSERT_VALUES_EQUAL(record.GetMessage(0).GetMessageMeta().GetApproximateReceiveCount(), 1);
    UNIT_ASSERT_VALUES_EQUAL(record.GetMessage(1).GetMessageMeta().GetApproximateReceiveCount(), 9);
}

Y_UNIT_TEST(EnrichTripleDuplicateOffset) {
    TTopicFixture fx(1);
    auto& runtime = fx.Runtime();
    const auto e0 = runtime.AllocateEdgeActor();
    const auto e1 = runtime.AllocateEdgeActor();
    const auto e2 = runtime.AllocateEdgeActor();

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(e0, 10, MakeReadMessages({0})));
    replies.push_back(TReadResult(e1, 11, MakeReadMessages({0})));
    replies.push_back(TReadResult(e2, 12, MakeReadMessages({0})));
    fx.RegisterEnricher(std::move(replies));

    for (auto edge : {e0, e1, e2}) {
        auto response = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge, TDuration::Seconds(5));
        UNIT_ASSERT(response);
        AssertEnrichedResponse(*response->Get(), {{0, fx.Bodies[0]}});
    }
}

Y_UNIT_TEST(EnrichSequentialActors) {
    TTopicFixture fx(1);
    auto& runtime = fx.Runtime();

    for (ui64 cookie : {1ull, 2ull}) {
        const auto edge = runtime.AllocateEdgeActor();
        std::deque<TReadResult> replies;
        replies.push_back(TReadResult(edge, cookie, MakeReadMessages({0})));
        fx.RegisterEnricher(std::move(replies));
        auto response = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge, TDuration::Seconds(5));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
        AssertEnrichedResponse(*response->Get(), {{0, fx.Bodies[0]}});
    }
}

Y_UNIT_TEST(EnrichSingleOffset) {
    TTopicFixture fx(1);
    auto& runtime = fx.Runtime();
    const auto edge = runtime.AllocateEdgeActor();

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(edge, 1, MakeReadMessages({0})));
    fx.RegisterEnricher(std::move(replies));

    auto response = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(response);
    AssertEnrichedResponse(*response->Get(), {{0, fx.Bodies[0]}});
}

} // Y_UNIT_TEST_SUITE(TMLPEnricherTests)

} // namespace NKikimr::NPQ::NMLP
