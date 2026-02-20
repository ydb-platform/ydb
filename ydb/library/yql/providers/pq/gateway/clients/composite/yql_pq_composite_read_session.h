#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_topic_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_session.h>

namespace NYql {

struct TCompositeTopicReadSessionSettings {
    NDq::TTxId TxId;
    ui64 TaskId = 0;
    TString Cluster;
    ui64 AmountPartitionsCount = 0;
    NMonitoring::TDynamicCounterPtr Counters;
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

} // namespace NYql
