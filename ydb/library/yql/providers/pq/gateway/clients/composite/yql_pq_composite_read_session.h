#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_topic_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_session.h>

namespace NYql {

struct TCompositeTopicReadSessionSettings {
    NYdb::NTopic::TReadSessionSettings BaseSettings;
    TDuration IdleTimeout;
    TDuration MaxPartitionReadSkew;
    NActors::TActorId AggregatorActor; // DqInfoAggregationActor
};

class ICompositeTopicReadSessionControl {
public:
    using TPtr = std::shared_ptr<ICompositeTopicReadSessionControl>;

    virtual ~ICompositeTopicReadSessionControl() = default;

    virtual void AdvancePartitionTime(ui64 partitionId, TInstant lastEventTime) = 0;
};

std::pair<std::shared_ptr<NYdb::NTopic::IReadSession>, ICompositeTopicReadSessionControl::TPtr> CreateCompositeTopicReadSession(
    const NActors::TActorContext& ctx,
    ITopicClient& topicClient,
    const TCompositeTopicReadSessionSettings& settings
);

/*
static TInstant GetEventWriteTime(const TReadSessionEvent::TDataReceivedEvent& event) {
    TInstant result;

    auto messagesCount = event.GetMessagesCount();
    if (event.HasCompressedMessages()) {
        const auto& compressedMessages = event.GetCompressedMessages();
        messagesCount -= compressedMessages.size();

        for (const auto& compressedMessage : compressedMessages) {
            result = std::max(result, compressedMessage.GetWriteTime());
        }
    }

    if (messagesCount) {
        for (const auto& message : event.GetMessages()) {
            result = std::max(result, message.GetWriteTime());
        }
    }

    return result;
}
*/

} // namespace NYql
