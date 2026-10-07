#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/providers/abstract/message_stream/message_stream_client.h>

namespace NYql {

struct TCompositeTopicReadSessionSettings {
    NDq::TTxId TxId;
    ui64 TaskId = 0;
    TString Cluster;
    ui64 AmountPartitionsCount = 0;
    ui64 InputIndex = 0;
    NMonitoring::TDynamicCounterPtr Counters;
    NFq::TMessageStreamReadSessionSettings BaseSettings;
    TDuration IdleTimeout;
    TDuration MaxPartitionReadSkew;
    NActors::TActorId AggregatorActor; // TDqPqInfoAggregationActor
};

class ICompositeTopicReadSessionControl {
public:
    using TPtr = std::shared_ptr<ICompositeTopicReadSessionControl>;

    virtual ~ICompositeTopicReadSessionControl() = default;

    virtual void AdvancePartitionTime(ui64 partitionId, TInstant lastEventTime) = 0;

    virtual TString GetInternalState() = 0;
};

std::pair<std::shared_ptr<NFq::IMessageStreamReadSession>, ICompositeTopicReadSessionControl::TPtr> CreateCompositeTopicReadSession(
    const NActors::TActorContext& ctx,
    NFq::IMessageStreamDataClient& topicClient,
    const TCompositeTopicReadSessionSettings& settings
);

} // namespace NYql
