#include "mlp_message_enricher.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/protos/pqdata_mlp.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPEnricherTests) {

static std::deque<TReadMessage> MakeReadMessages(const std::vector<ui64>& offsets) {
    std::deque<TReadMessage> messages;
    const auto now = TInstant::Now();
    for (ui64 offset : offsets) {
        messages.push_back(TReadMessage{
            .Offset = offset,
            .ApproximateReceiveCount = 1,
            .ApproximateFirstReceiveTimestamp = now,
        });
    }
    return messages;
}

static void AssertEnrichedResponse(
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

Y_UNIT_TEST(EnrichDuplicateOffsetsAcrossReplies) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString consumer = "mlp-consumer";

    auto status = CreateTopic(setup, "/Root/topic1", consumer);
    UNIT_ASSERT_VALUES_EQUAL_C(status.IsSuccess(), true, status.GetIssues().ToString());

    const std::vector<TString> bodies = {"msg0", "msg1", "msg2"};
    std::vector<TWriterSettings::TMessage> writeMessages;
    writeMessages.reserve(bodies.size());
    for (size_t i = 0; i < bodies.size(); ++i) {
        writeMessages.push_back({
            .Index = i,
            .MessageBody = bodies[i],
            .MessageGroupId = TStringBuilder() << "group_" << i,
        });
    }
    CreateWriterActor(runtime, {
        .DatabasePath = "/Root",
        .TopicName = "/Root/topic1",
        .Messages = std::move(writeMessages),
    });
    auto writeResponse = GetWriteResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(writeResponse->Messages.size(), 3);

    const ui64 tabletId = GetTabletId(setup, "/Root", "/Root/topic1", 0);

    const auto edge0 = runtime.AllocateEdgeActor();
    const auto edge1 = runtime.AllocateEdgeActor();

    std::deque<TReadResult> replies;
    replies.push_back(TReadResult(edge0, 100, MakeReadMessages({0, 1, 2})));
    replies.push_back(TReadResult(edge1, 200, MakeReadMessages({0, 1, 2})));

    const auto enricherId = runtime.Register(CreateMessageEnricher(tabletId, 0, consumer, std::move(replies)));
    runtime.EnableScheduleForActor(enricherId);
    runtime.DispatchEvents();

    const std::vector<std::pair<ui64, TString>> expected = {
        {0, bodies[0]},
        {1, bodies[1]},
        {2, bodies[2]},
    };

    auto response0 = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge0, TDuration::Seconds(5));
    auto response1 = runtime.GrabEdgeEvent<TEvPQ::TEvMLPReadResponse>(edge1, TDuration::Seconds(5));
    UNIT_ASSERT(response0);
    UNIT_ASSERT(response1);

    AssertEnrichedResponse(*response0->Get(), expected);
    AssertEnrichedResponse(*response1->Get(), expected);
}

} // Y_UNIT_TEST_SUITE(TMLPEnricherTests)

} // namespace NKikimr::NPQ::NMLP
