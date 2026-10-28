#include "topic_configuration_replicator.h"
#include "topic_configuration_replicator_factory.h"

#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
#include <util/generic/hash_set.h>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ {

TTopicConfigurationReplicator::TEvTopicDescription::TEvTopicDescription(NYdb::NTopic::TAsyncDescribeTopicResult result)
    : Result(std::move(result))
{}

TTopicConfigurationReplicator::TEvConsumerDescription::TEvConsumerDescription(size_t consumer,
    NYdb::NTopic::TAsyncDescribeConsumerResult result)
    : Consumer(consumer)
    , Result(std::move(result))
{}

TTopicConfigurationReplicator::TEvRetryConsumer::TEvRetryConsumer(size_t consumer)
    : Consumer(consumer)
{}

TTopicConfigurationReplicator::TTopicConfigurationReplicator(ui64 tabletId, const TActorId& tabletActor,
    const NKikimrPQ::TMirrorPartitionConfig& config)
    : TBaseTabletActor(tabletId, tabletActor, NKikimrServices::PERSQUEUE)
    , Config(config)
{}

void TTopicConfigurationReplicator::Bootstrap(const TActorContext& ctx) {
    Become(&TThis::StateInit);
    ctx.Send(SelfId(), new TEvPQ::TEvInitCredentials());
}

void TTopicConfigurationReplicator::Handle(TEvPQ::TEvInitCredentials::TPtr&, const TActorContext& ctx) {
    if (CredentialsRequestInFlight) {
        return;
    }

    auto* factory = AppData(ctx)->PersQueueMirrorReaderFactory;
    if (!factory) {
        RetryInit("Mirror reader factory is not configured", ctx);
        return;
    }

    CredentialsRequestInFlight = true;
    factory->GetCredentialsProvider(Config.GetCredentials()).Subscribe(
        [actorSystem = ctx.ActorSystem(), selfId = SelfId()](const auto& result) {
            THolder<TEvPQ::TEvCredentialsCreated> response;
            try {
                response = MakeHolder<TEvPQ::TEvCredentialsCreated>(result.GetValue());
            } catch (...) {
                response = MakeHolder<TEvPQ::TEvCredentialsCreated>(CurrentExceptionMessage());
            }
            actorSystem->Send(new IEventHandle(selfId, selfId, response.Release()));
        });
}

void TTopicConfigurationReplicator::Handle(TEvPQ::TEvCredentialsCreated::TPtr& ev, const TActorContext& ctx) {
    CredentialsRequestInFlight = false;
    if (ev->Get()->Error) {
        RetryInit(*ev->Get()->Error, ctx);
        return;
    }

    CredentialsProvider = ev->Get()->Credentials;
    auto* factory = AppData(ctx)->PersQueueMirrorReaderFactory;
    if (!factory) {
        RetryInit("Mirror reader factory is not configured", ctx);
        return;
    }

    try {
        TopicClient.emplace(factory->GetTopicClient(Config, CredentialsProvider));
    } catch (...) {
        RetryInit(CurrentExceptionMessage(), ctx);
        return;
    }

    InitRetryInterval = INIT_RETRY_INTERVAL_START;
    LOG_I("Configuration replicator topic client initialized");
    Become(&TThis::StateWork);
    RequestTopicDescription(ctx);
}

void TTopicConfigurationReplicator::RetryInit(const TString& error, const TActorContext& ctx) {
    LOG_W("Cannot initialize configuration replicator topic client", {"error", error});
    TopicClient.reset();
    CredentialsProvider.reset();
    ctx.Schedule(InitRetryInterval, new TEvPQ::TEvInitCredentials());
    InitRetryInterval = Min(InitRetryInterval * 2, INIT_RETRY_INTERVAL_MAX);
}

TStructuredMessage TTopicConfigurationReplicator::BuildLogPrefix() const {
    return YDB_LOG_CREATE_MESSAGE({"tablet_id", TabletId}, {"source_topic", Config.GetTopic()});
}

STFUNC(TTopicConfigurationReplicator::StateInit) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPQ::TEvInitCredentials, Handle);
        HFunc(TEvPQ::TEvCredentialsCreated, Handle);
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
    }
}

STFUNC(TTopicConfigurationReplicator::StateWork) {
    switch (ev->GetTypeRewrite()) {
        CFunc(TEvents::TEvWakeup::EventType, RequestTopicDescription);
        HFunc(TEvTopicDescription, Handle);
        HFunc(TEvConsumerDescription, Handle);
        HFunc(TEvRetryConsumer, Handle);
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
    }
}

NYdb::NTopic::TAsyncDescribeTopicResult TTopicConfigurationReplicator::DescribeTopic() {
    return TopicClient->DescribeTopic(Config.GetTopic(), NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true));
}

NYdb::NTopic::TAsyncDescribeConsumerResult TTopicConfigurationReplicator::DescribeConsumer(const TString& consumer) {
    return TopicClient->DescribeConsumer(Config.GetTopic(), consumer,
        NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true));
}

void TTopicConfigurationReplicator::RequestTopicDescription(const TActorContext& ctx) {
    try {
        DescribeTopic().Subscribe([actorSystem = ctx.ActorSystem(), selfId = SelfId()](const auto& result) {
            actorSystem->Send(new IEventHandle(selfId, selfId, new TEvTopicDescription(result)));
        });
    } catch (...) {
        LOG_W("Cannot describe source topic", {"error", CurrentExceptionMessage()});
        ctx.Schedule(DESCRIBE_RETRY_INTERVAL, new TEvents::TEvWakeup());
    }
}

void TTopicConfigurationReplicator::Handle(TEvTopicDescription::TPtr& ev, const TActorContext& ctx) {
    try {
        const auto& result = ev->Get()->Result.GetValue();
        if (!result.IsSuccess()) {
            ythrow yexception() << result.GetIssues().ToString();
        }
        for (const auto& partition : result.GetTopicDescription().GetPartitions()) {
            if (!partition.GetPartitionStats()) {
                ythrow yexception() << "Missing statistics for partition " << partition.GetPartitionId();
            }
        }
        TopicDescription.emplace(result);
    } catch (...) {
        LOG_W("Cannot describe source topic", {"error", CurrentExceptionMessage()});
        ctx.Schedule(DESCRIBE_RETRY_INTERVAL, new TEvents::TEvWakeup());
        return;
    }

    for (const auto& partition : TopicDescription->GetTopicDescription().GetPartitions()) {
        auto& offsets = PartitionOffsets[partition.GetPartitionId()];
        offsets.SetPartitionId(partition.GetPartitionId());
        offsets.SetStartOffset(partition.GetPartitionStats()->GetStartOffset());
        offsets.SetEndOffset(partition.GetPartitionStats()->GetEndOffset());
    }
    PumpConsumerDescriptions(ctx);
}

void TTopicConfigurationReplicator::RequestConsumerDescription(size_t consumer, const TActorContext& ctx) {
    try {
        const TString name(TopicDescription->GetTopicDescription().GetConsumers()[consumer].GetConsumerName());
        DescribeConsumer(name).Subscribe(
            [actorSystem = ctx.ActorSystem(), selfId = SelfId(), consumer](const auto& result) {
                actorSystem->Send(new IEventHandle(selfId, selfId, new TEvConsumerDescription(consumer, result)));
            });
    } catch (...) {
        LOG_W("Cannot describe source consumer", {"error", CurrentExceptionMessage()});
        ctx.Schedule(DESCRIBE_RETRY_INTERVAL, new TEvRetryConsumer(consumer));
    }
}

void TTopicConfigurationReplicator::PumpConsumerDescriptions(const TActorContext& ctx) {
    const auto& consumers = TopicDescription->GetTopicDescription().GetConsumers();
    while (NextConsumer < consumers.size() && ConsumerRequestsInFlight < MAX_CONSUMER_REQUESTS_IN_FLIGHT) {
        ++ConsumerRequestsInFlight;
        RequestConsumerDescription(NextConsumer++, ctx);
    }
    if (NextConsumer == consumers.size() && ConsumerRequestsInFlight == 0) {
        for (const auto& [partition, offsets] : PartitionOffsets) {
            PendingOffsetAcks.insert(partition);
        }
        Become(&TThis::StateWaitingOffsets);
        SendOffsets(ctx);
    }
}

void TTopicConfigurationReplicator::Handle(TEvConsumerDescription::TPtr& ev, const TActorContext& ctx) {
    const auto consumer = ev->Get()->Consumer;
    const TString name(TopicDescription->GetTopicDescription().GetConsumers()[consumer].GetConsumerName());
    try {
        const auto& result = ev->Get()->Result.GetValue();
        if (!result.IsSuccess()) {
            ythrow yexception() << result.GetIssues().ToString();
        }
        const auto& partitions = result.GetConsumerDescription().GetPartitions();
        THashSet<ui64> seen;
        for (const auto& partition : partitions) {
            if (!partition.GetPartitionConsumerStats() || !PartitionOffsets.contains(partition.GetPartitionId())
                || !seen.insert(partition.GetPartitionId()).second)
            {
                ythrow yexception() << "Missing statistics or unexpected partition " << partition.GetPartitionId();
            }
        }
        if (seen.size() != PartitionOffsets.size()) {
            ythrow yexception() << "Incomplete partition statistics";
        }
        // Validate the whole response before adding offsets, so retries do not leave partial results.
        for (const auto& partition : partitions) {
            auto* offset = PartitionOffsets.at(partition.GetPartitionId()).AddConsumerOffsets();
            offset->SetConsumer(name);
            offset->SetCommittedOffset(partition.GetPartitionConsumerStats()->GetCommittedOffset());
        }
    } catch (...) {
        LOG_W("Cannot describe source consumer", {"consumer", name}, {"error", CurrentExceptionMessage()});
        ctx.Schedule(DESCRIBE_RETRY_INTERVAL, new TEvRetryConsumer(consumer));
        return;
    }

    --ConsumerRequestsInFlight;
    PumpConsumerDescriptions(ctx);
}

void TTopicConfigurationReplicator::Handle(TEvRetryConsumer::TPtr& ev, const TActorContext& ctx) {
    RequestConsumerDescription(ev->Get()->Consumer, ctx);
}

void TTopicConfigurationReplicator::SendOffsets(const TActorContext& ctx) {
    if (PendingOffsetAcks.empty()) {
        Become(&TThis::StateDone);
        ctx.Send(TabletActorId, new TEvPQ::TEvTopicConfigurationReplicationFinished());
    } else {
        for (const auto partition : PendingOffsetAcks) {
            auto event = MakeHolder<TEvPQ::TEvPartitionConsumerOffsets>();
            event->Record.CopyFrom(PartitionOffsets.at(partition));
            ctx.Send(TabletActorId, event.Release(), 0, partition + 1);
        }
    }
    // Keep retrying delivery, or completion notification, until PQRB changes the config.
    ctx.Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
}

void TTopicConfigurationReplicator::Handle(TEvPQ::TEvPartitionConsumerOffsetsAck::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    if (!record.HasPartitionId() || ev->Cookie != ui64(record.GetPartitionId()) + 1
        || !PendingOffsetAcks.erase(record.GetPartitionId()))
    {
        return;
    }
    if (PendingOffsetAcks.empty()) {
        Become(&TThis::StateDone);
        LOG_I("All partitions acknowledged source consumer offsets");
        ctx.Send(TabletActorId, new TEvPQ::TEvTopicConfigurationReplicationFinished());
    }
}

STFUNC(TTopicConfigurationReplicator::StateWaitingOffsets) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvPQ::TEvPartitionConsumerOffsetsAck, Handle);
        CFunc(TEvents::TEvWakeup::EventType, SendOffsets);
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
    }
}

STFUNC(TTopicConfigurationReplicator::StateDone) {
    switch (ev->GetTypeRewrite()) {
        CFunc(TEvents::TEvWakeup::EventType, SendOffsets);
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
    }
}

NActors::IActor* CreateTopicConfigurationReplicator(ui64 tabletId, const NActors::TActorId& tabletActor,
    const NKikimrPQ::TMirrorPartitionConfig& config)
{
    return new TTopicConfigurationReplicator(tabletId, tabletActor, config);
}

} // namespace NKikimr::NPQ
