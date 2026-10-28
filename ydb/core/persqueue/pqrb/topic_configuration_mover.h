#pragma once

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <optional>
#include <util/generic/hash_set.h>

namespace NKikimr::NPQ {

class TTopicConfigurationMover : public TBaseTabletActor<TTopicConfigurationMover>, private TConstantLogPrefix {
public:
    TTopicConfigurationMover(ui64 tabletId, const TActorId& tabletActor,
        const NKikimrPQ::TMirrorPartitionConfig& config);

    void Bootstrap(const TActorContext& ctx);

protected:
    virtual NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic();
    virtual NYdb::NTopic::TAsyncDescribeConsumerResult DescribeConsumer(const TString& consumer);

private:
    struct TEvTopicDescription : TEventLocal<TEvTopicDescription, EventSpaceBegin(TEvents::ES_PRIVATE)> {
        explicit TEvTopicDescription(NYdb::NTopic::TAsyncDescribeTopicResult result);
        NYdb::NTopic::TAsyncDescribeTopicResult Result;
    };
    struct TEvConsumerDescription : TEventLocal<TEvConsumerDescription, EventSpaceBegin(TEvents::ES_PRIVATE) + 1> {
        TEvConsumerDescription(size_t consumer, NYdb::NTopic::TAsyncDescribeConsumerResult result);
        size_t Consumer;
        NYdb::NTopic::TAsyncDescribeConsumerResult Result;
    };
    struct TEvRetryConsumer : TEventLocal<TEvRetryConsumer, EventSpaceBegin(TEvents::ES_PRIVATE) + 2> {
        explicit TEvRetryConsumer(size_t consumer);
        size_t Consumer;
    };

    STFUNC(StateInit);
    STFUNC(StateWork);
    STFUNC(StateWaitingOffsets);
    STFUNC(StateDone);

    void Handle(TEvPQ::TEvInitCredentials::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvCredentialsCreated::TPtr& ev, const TActorContext& ctx);
    void RetryInit(const TString& error, const TActorContext& ctx);
    void RequestTopicDescription(const TActorContext& ctx);
    void RequestConsumerDescription(size_t consumer, const TActorContext& ctx);
    void PumpConsumerDescriptions(const TActorContext& ctx);
    void SendOffsets(const TActorContext& ctx);
    void Handle(TEvPQ::TEvPartitionConsumerOffsetsAck::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTopicDescription::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvConsumerDescription::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvRetryConsumer::TPtr& ev, const TActorContext& ctx);
    TStructuredMessage BuildLogPrefix() const override;

    static constexpr TDuration INIT_RETRY_INTERVAL_START = TDuration::Seconds(1);
    static constexpr TDuration INIT_RETRY_INTERVAL_MAX = TDuration::Seconds(60);
    static constexpr size_t MAX_CONSUMER_REQUESTS_IN_FLIGHT = 5;
    static constexpr TDuration DESCRIBE_RETRY_INTERVAL = TDuration::Seconds(5);

    const NKikimrPQ::TMirrorPartitionConfig Config;
    NYdb::TCredentialsProviderFactoryPtr CredentialsProvider;
    std::optional<NYdb::NTopic::TTopicClient> TopicClient;
    TDuration InitRetryInterval = INIT_RETRY_INTERVAL_START;
    bool CredentialsRequestInFlight = false;
    std::optional<NYdb::NTopic::TDescribeTopicResult> TopicDescription;
    THashMap<ui64, NKikimrPQ::TEvPartitionConsumerOffsets> PartitionOffsets;
    THashSet<ui64> PendingOffsetAcks;
    size_t NextConsumer = 0;
    size_t ConsumerRequestsInFlight = 0;
};

} // namespace NKikimr::NPQ
