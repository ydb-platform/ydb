#include "read_balancer__mlp_balancing.h"
#include "read_balancer_log.h"

namespace NKikimr::NPQ::NBalancing {

TMLPConsumer::TMLPConsumer(TMLPBalancer& balancer, const TString& consumerName)
    : Balancer(balancer)
    , ConsumerName(consumerName) {
}

TDuration TMLPConsumer::GetReceiveAttemptIdPeriod() const {
    if (const auto* consumerConfig = NPQ::GetConsumer(GetConfig(), ConsumerName)) {
        return TDuration::MilliSeconds(consumerConfig->GetReadRequestAttemptIdPeriodMs());
    }
    return TDuration::MilliSeconds(NKikimrPQ::TPQTabletConfig::TConsumer().GetReadRequestAttemptIdPeriodMs());
}

TReceiveAttemptPartitionDelete TMLPConsumer::MakeDeleteKey(const TString& receiveAttemptId) const {
    return TReceiveAttemptPartitionDelete{
        .Key = {
            .Consumer = ConsumerName,
            .ReceiveAttemptId = receiveAttemptId,
        },
    };
}

const NKikimrPQ::TPQTabletConfig& TMLPConsumer::GetConfig() const {
    return Balancer.GetConfig();
}

const TPartitionGraph& TMLPConsumer::GetPartitionGraph() const {
    return Balancer.GetPartitionGraph();
}

const TPartitionGraph::Node* TMLPConsumer::PickNextPartition() {
    if (PartitionsForBalancing.empty()) {
        const auto& activePartitions = Balancer.GetActivePartitions();
        if (activePartitions.empty()) {
            return nullptr;
        }
        auto partitionId = PartitionIterator++ % activePartitions.size();
        return GetPartitionGraph().GetPartition(activePartitions[partitionId]);
    }

    auto partitionId = PartitionIterator++ % PartitionsForBalancing.size();
    return GetPartitionGraph().GetPartition(PartitionsForBalancing[partitionId]);
}

TPrepareGetPartitionResponse TMLPConsumer::PrepareGetPartitionResponse(const TString& receiveAttemptId, TInstant now) {
    TPrepareGetPartitionResponse result;

    if (!receiveAttemptId.empty()) {
        if (auto it = ReceiveAttemptPartitions.find(receiveAttemptId); it != ReceiveAttemptPartitions.end()) {
            if (it->second.Expiry > now) {
                if (const auto* node = GetPartitionGraph().GetPartition(it->second.PartitionId)) {
                    it->second.Expiry = now + GetReceiveAttemptIdPeriod();
                    result.Node = node;
                    result.PersistChanges.Upsert = TReceiveAttemptPartitionUpsert{
                        .Key = {
                            .Consumer = ConsumerName,
                            .ReceiveAttemptId = receiveAttemptId,
                        },
                        .PartitionId = node->Id,
                        .ExpiryMicros = it->second.Expiry.MicroSeconds(),
                    };
                    return result;
                }
            }
            result.PersistChanges.Deletes.push_back(MakeDeleteKey(receiveAttemptId));
            ReceiveAttemptPartitions.erase(it);
        }
    }

    result.Node = PickNextPartition();
    if (!result.Node) {
        return result;
    }

    if (!receiveAttemptId.empty()) {
        const auto expiry = now + GetReceiveAttemptIdPeriod();
        ReceiveAttemptPartitions[receiveAttemptId] = {
            .PartitionId = result.Node->Id,
            .Expiry = expiry,
        };
        result.PersistChanges.Upsert = TReceiveAttemptPartitionUpsert{
            .Key = {
                .Consumer = ConsumerName,
                .ReceiveAttemptId = receiveAttemptId,
            },
            .PartitionId = result.Node->Id,
            .ExpiryMicros = expiry.MicroSeconds(),
        };
    }

    return result;
}

void TMLPConsumer::RestoreReceiveAttemptPartition(const TString& receiveAttemptId, ui32 partitionId, TInstant expiry) {
    ReceiveAttemptPartitions[receiveAttemptId] = {
        .PartitionId = partitionId,
        .Expiry = expiry,
    };
}

std::vector<TReceiveAttemptPartitionDelete> TMLPConsumer::CollectExpiredReceiveAttemptPartitions(TInstant now) {
    std::vector<TReceiveAttemptPartitionDelete> deletes;
    absl::erase_if(ReceiveAttemptPartitions, [&](const auto& entry) {
        if (entry.second.Expiry <= now) {
            deletes.push_back(MakeDeleteKey(entry.first));
            return true;
        }
        return false;
    });
    return deletes;
}

bool TMLPConsumer::SetUseForReading(
    ui32 partitionId,
    std::optional<bool> readingIsFinished,
    std::optional<bool> useForReading,
    const std::optional<TMetrics>& metrics,
    ui32 generation,
    ui64 cookie
) {
    auto& status = Partitions[partitionId];

    if (status.Generation < generation || (status.Generation == generation && status.Cookie < cookie)) {
        auto result = false;

        if (readingIsFinished) {
            result |= status.ReadingIsFinished != *readingIsFinished;
            status.ReadingIsFinished = *readingIsFinished;
        }
        if (useForReading) {
            result |= status.UseForReading != *useForReading;
            status.UseForReading = *useForReading;
        }

        auto calc = [](ui64 aggregatedValue, ui64 currentValue, ui64 newValue) {
            auto v = aggregatedValue + newValue;
            return v > currentValue ? v - currentValue : 0;
        };

        if (metrics) {
            Metrics.Messages = calc(Metrics.Messages, status.Metrics.Messages, metrics->Messages);
            Metrics.DelayedMessages = calc(Metrics.DelayedMessages, status.Metrics.DelayedMessages, metrics->DelayedMessages);
            Metrics.LockedMessages = calc(Metrics.LockedMessages, status.Metrics.LockedMessages, metrics->LockedMessages);
            status.Metrics = *metrics;
        }

        status.Generation = generation;
        status.Cookie = cookie;

        return result;
    }

    return false;
}

void TMLPConsumer::Rebuild() {
    PartitionsForBalancing.clear();

    size_t activePartitions = 0;
    size_t readyForReadingPartitions = 0;
    for (const auto& [partitionId, status] : Partitions) {
        if (status.ReadingIsFinished) {
            continue;
        }
        activePartitions++;
        if (status.UseForReading) {
            readyForReadingPartitions++;
        }
    }

    PartitionsForBalancing.reserve(readyForReadingPartitions ? readyForReadingPartitions : activePartitions);
    for (const auto& [partitionId, status] : Partitions) {
        if (status.ReadingIsFinished) {
            continue;
        }
        if (!readyForReadingPartitions || status.UseForReading) {
            PartitionsForBalancing.push_back(partitionId);
        }
    }

    PQ_LOG_D("Rebuild " << JoinSeq(",", PartitionsForBalancing) << " partitions for balancing");
}

const TMLPConsumer::TMetrics& TMLPConsumer::GetMetrics() const {
    return Metrics;
}

TMLPBalancer::TMLPBalancer(TPersQueueReadBalancer& topicActor)
    : TopicActor(topicActor) {
}

TPrepareGetPartitionResponse TMLPBalancer::PrepareGetPartitionResponse(
    const TString& consumerName,
    const TString& receiveAttemptId,
    TInstant now
) {
    TPrepareGetPartitionResponse result;

    auto* consumerConfig = NPQ::GetConsumer(GetConfig(), consumerName);
    if (!consumerConfig) {
        PQ_LOG_D("Consumer '" << consumerName << "' does not exist");
        result.IsError = true;
        result.ErrorStatus = Ydb::StatusIds::SCHEME_ERROR;
        result.ErrorMessage = TStringBuilder() << "Consumer '" << consumerName << "' does not exist";
        return result;
    }

    if (consumerConfig->GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
        PQ_LOG_D("Consumer '" << consumerName << "' is not MLP consumer");
        result.IsError = true;
        result.ErrorStatus = Ydb::StatusIds::SCHEME_ERROR;
        result.ErrorMessage = TStringBuilder() << "Consumer '" << consumerName << "' is not MLP consumer";
        return result;
    }

    auto [it, newConsumer] = Consumers.try_emplace(consumerName, *this, consumerName);
    auto& consumer = it->second;
    if (newConsumer) {
        consumer.Rebuild();
    }

    result = consumer.PrepareGetPartitionResponse(receiveAttemptId, now);
    if (!result.IsError && !result.Node) {
        result.IsError = true;
        result.ErrorStatus = Ydb::StatusIds::SCHEME_ERROR;
        result.ErrorMessage = "No partitions for balancing";
    }

    return result;
}

void TMLPBalancer::RestoreReceiveAttemptPartition(
    const TString& consumerName,
    const TString& receiveAttemptId,
    ui32 partitionId,
    TInstant expiry
) {
    auto [it, newConsumer] = Consumers.try_emplace(consumerName, *this, consumerName);
    if (newConsumer) {
        it->second.Rebuild();
    }
    it->second.RestoreReceiveAttemptPartition(receiveAttemptId, partitionId, expiry);
}

std::vector<TReceiveAttemptPartitionDelete> TMLPBalancer::CollectExpiredReceiveAttemptPartitions(TInstant now) {
    std::vector<TReceiveAttemptPartitionDelete> deletes;
    for (auto& [_, consumer] : Consumers) {
        auto consumerDeletes = consumer.CollectExpiredReceiveAttemptPartitions(now);
        deletes.insert(deletes.end(), std::make_move_iterator(consumerDeletes.begin()), std::make_move_iterator(consumerDeletes.end()));
    }
    return deletes;
}

void TMLPBalancer::Handle(TEvPQ::TEvMLPGetRuntimeAttributesRequest::TPtr& ev) {
    const auto& consumerName = ev->Get()->GetConsumer();

    const auto* consumerConfig = NPQ::GetConsumer(GetConfig(), consumerName);
    if (!consumerConfig) {
        PQ_LOG_D("Consumer '" << consumerName << "' does not exist");
        TopicActor.Send(ev->Sender, new TEvPQ::TEvMLPErrorResponse(Ydb::StatusIds::SCHEME_ERROR,
            TStringBuilder() << "Consumer '" << consumerName << "' does not exist"), 0, ev->Cookie);
        return;
    }

    if (consumerConfig->GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
        PQ_LOG_D("Consumer '" << consumerName << "' is not MLP consumer");
        TopicActor.Send(ev->Sender, new TEvPQ::TEvMLPErrorResponse(Ydb::StatusIds::SCHEME_ERROR,
            TStringBuilder() << "Consumer '" << consumerName << "' is not MLP consumer"), 0, ev->Cookie);
        return;
    }

    auto it = Consumers.find(consumerName);
    if (it == Consumers.end()) {
        PQ_LOG_D("Consumer '" << consumerName << "' is not initialized");
        TopicActor.Send(ev->Sender, new TEvPQ::TEvMLPGetRuntimeAttributesResponse(0, 0, 0), 0, ev->Cookie);
        return;
    }

    const auto& consumer = it->second;
    const auto& metrics = consumer.GetMetrics();

    auto response = std::make_unique<TEvPQ::TEvMLPGetRuntimeAttributesResponse>(
        metrics.Messages,
        metrics.DelayedMessages,
        metrics.LockedMessages
    );
    TopicActor.Send(ev->Sender, std::move(response), 0, ev->Cookie);
}

void TMLPBalancer::Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext&) {
    PQ_LOG_D("Handle TEvPersQueue::TEvStatusResponse " << ev->Get()->Record.ShortDebugString());

    absl::flat_hash_map<TString, bool> mlpConsumers;
    for (const auto& consumer : GetConfig().GetConsumers()) {
        if (consumer.GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
            mlpConsumers[consumer.GetName()] = false;
        }
    }

    auto& record = ev->Get()->Record;
    for (const auto& partitionResult : record.GetPartResult()) {
        const auto partitionId = partitionResult.GetPartition();
        const auto generation = partitionResult.GetGeneration();
        const auto cookie = partitionResult.GetCookie();

        for (const auto& consumerResult : partitionResult.GetConsumerResult()) {
            const auto& consumerName = consumerResult.GetConsumer();

            auto mit = mlpConsumers.find(consumerName);
            if (mit != std::end(mlpConsumers)) {
                auto [it, inserted] = Consumers.try_emplace(consumerName, *this, consumerName);
                auto& consumer = it->second;

                auto readingIsFinished = consumerResult.GetReadingFinished();
                auto useForReading = consumerResult.GetUseForReading();

                TMLPConsumer::TMetrics metrics{
                    .Messages = consumerResult.GetMLPMessageCount(),
                    .DelayedMessages = consumerResult.GetMLPDelayedMessageCount(),
                    .LockedMessages = consumerResult.GetMLPLockedMessageCount(),
                };
                mit->second |= consumer.SetUseForReading(partitionId, readingIsFinished, useForReading, metrics, generation, cookie) || inserted;
            }
        }
    }

    for (auto& [consumerName, changed] : mlpConsumers) {
        if (changed) {
            Consumers.at(consumerName).Rebuild();
        }
    }
}

void TMLPBalancer::Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext&) {
    auto& record = ev->Get()->Record;
    PQ_LOG_D("Handle TEvPQ::TEvReadingPartitionStatusRequest " << record.ShortDebugString());
    SetUseForReading(record.GetConsumer(),
                     record.GetPartitionId(),
                     true, // reading is finished
                     std::nullopt, // use for reading
                     TMLPConsumer::TMetrics{
                        .Messages = record.GetMessageCount(),
                        .DelayedMessages = record.GetDelayedMessageCount(),
                        .LockedMessages = record.GetLockedMessageCount(),
                     },
                     record.GetGeneration(),
                     record.GetCookie());
}

void TMLPBalancer::Handle(TEvPQ::TEvMLPConsumerStatus::TPtr& ev) {
    auto& record = ev->Get()->Record;
    PQ_LOG_D("Handle TEvPQ::TEvMLPConsumerStatus " << record.ShortDebugString());
    SetUseForReading(record.GetConsumer(),
                     record.GetPartitionId(),
                     std::nullopt, // reading is finished
                     record.GetUseForReading(), // use for reading
                     TMLPConsumer::TMetrics{
                        .Messages = record.GetMessageCount(),
                        .DelayedMessages = record.GetDelayedMessageCount(),
                        .LockedMessages = record.GetLockedMessageCount(),
                     },
                     record.GetGeneration(),
                     record.GetCookie());
}

void TMLPBalancer::UpdateConfig(const std::vector<ui32>& addedPartitions) {
    absl::flat_hash_set<TString> mlpConsumers;
    for (const auto& consumer : GetConfig().GetConsumers()) {
        if (consumer.GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
            mlpConsumers.insert(consumer.GetName());
        }
    }

    for (auto it = Consumers.begin(); it != Consumers.end();) {
        auto& [consumerName, consumer] = *it;
        it++;

        if (mlpConsumers.contains(consumerName)) {
            for (const auto& partitionId : addedPartitions) {
                consumer.SetUseForReading(partitionId, false, false, std::nullopt, 0, 0);
            }
            if (!addedPartitions.empty()) {
                consumer.Rebuild();
            }
        } else {
            Consumers.erase(consumerName);
        }
    }

    for (const auto& consumerName : mlpConsumers) {
        auto [it, inserted] = Consumers.try_emplace(consumerName, *this, consumerName);
        if (inserted) {
            it->second.Rebuild();
        }
    }
}

void TMLPBalancer::SetUseForReading(const TString& consumerName,
                                    ui32 partitionId,
                                    std::optional<bool> readingIsFinished,
                                    std::optional<bool> useForReading,
                                    const std::optional<TMLPConsumer::TMetrics>& metrics,
                                    ui32 generation,
                                    ui64 cookie) {
    auto* consumerConfig = NPQ::GetConsumer(GetConfig(), consumerName);
    if (!consumerConfig) {
        PQ_LOG_D("Consumer '" << consumerName << "' does not exist");
        return;
    }

    if (consumerConfig->GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
        PQ_LOG_D("Consumer '" << consumerName << "' is not MLP consumer");
        return;
    }

    auto [it, _] = Consumers.try_emplace(consumerName, *this, consumerName);
    auto& consumer = it->second;

    if (consumer.SetUseForReading(partitionId, readingIsFinished, useForReading, metrics, generation, cookie)) {
        consumer.Rebuild();
    }
}

const NKikimrPQ::TPQTabletConfig& TMLPBalancer::GetConfig() const {
    return TopicActor.TabletConfig;
}

const TPartitionGraph& TMLPBalancer::GetPartitionGraph() const {
    return TopicActor.PartitionGraph;
}

const std::vector<ui32>& TMLPBalancer::GetActivePartitions() const {
    return TopicActor.ActivePartitions;
}

} // namespace NKikimr::NPQ::NBalancing
