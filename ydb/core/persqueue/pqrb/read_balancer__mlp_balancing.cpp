#include "read_balancer__mlp_balancing.h"
#include "read_balancer_log.h"

namespace NKikimr::NPQ::NBalancing {

TMLPConsumer::TMLPConsumer(TMLPBalancer& balancer)
    : Balancer(balancer) {
}

const NKikimrPQ::TPQTabletConfig& TMLPConsumer::GetConfig() const {
    return Balancer.GetConfig();
}

const TPartitionGraph& TMLPConsumer::GetPartitionGraph() const {
    return Balancer.GetPartitionGraph();
}

const TPartitionGraph::Node* TMLPConsumer::NextPartition() {
    AFL_VERIFY_DEBUG(!PartitionsForBalancing.empty());
    if (PartitionsForBalancing.empty()) {
        return nullptr;
    }

    auto partitionId = PartitionIterator++ % PartitionsForBalancing.size();
    return GetPartitionGraph().GetPartition(PartitionsForBalancing[partitionId]);
}

bool TMLPConsumer::SetUseForReading(ui32 partitionId, ui64 messages, bool useForReading, ui32 generation, ui64 cookie) {
    auto& status = Partitions[partitionId];

    if (status.Generation < generation || (status.Generation == generation && status.Cookie < cookie)) {
        auto oldUseForReading = status.UseForReading;

        status.Messages = messages;
        status.UseForReading = useForReading;
        status.Generation = generation;
        status.Cookie = cookie;

        return oldUseForReading != useForReading;
    }

    return false;
}

void TMLPConsumer::Rebuild() {
    PartitionsForBalancing.clear();

    if (Partitions.empty()) {
        for (const auto& partitionConfig : GetConfig().GetAllPartitions()) {
            Partitions[partitionConfig.GetPartitionId()] = {0, 0, 0, true};
        }
    }

    PartitionsForBalancing.reserve(Partitions.size());
    for (const auto& [partitionId, status] : Partitions) {
        if (status.UseForReading) {
            PartitionsForBalancing.push_back(partitionId);
        }
    }

    PQ_LOG_D("Rebuild " << JoinSeq(",", PartitionsForBalancing) << " partitions for balancing");
}

TMLPBalancer::TMLPBalancer(TPersQueueReadBalancer& topicActor)
    : TopicActor(topicActor) {
}

void TMLPBalancer::Handle(TEvPQ::TEvMLPGetPartitionRequest::TPtr& ev) {
    auto& consumerName = ev->Get()->GetConsumer();

    auto* consumerConfig = NPQ::GetConsumer(GetConfig(), consumerName);
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

    auto [it, newConsumer] = Consumers.try_emplace(consumerName, *this);
    auto& consumer = it->second;
    if (newConsumer) {
        consumer.Rebuild();
    }

    auto* node = consumer.NextPartition();
    if (!node) {
        TopicActor.Send(ev->Sender, new TEvPQ::TEvMLPErrorResponse(Ydb::StatusIds::SCHEME_ERROR,
            TStringBuilder() << "No partitions for balancing"), 0, ev->Cookie);
        return;
    }

    TopicActor.Send(ev->Sender, new TEvPQ::TEvMLPGetPartitionResponse(node->Id, node->TabletId), 0, ev->Cookie);
}

void TMLPBalancer::Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext&) {
    absl::flat_hash_map<TString, bool> mlpConsumers;
    for (const auto& consumer : GetConfig().GetConsumers()) {
        if (consumer.GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
            mlpConsumers[consumer.GetName()] = false;
        }
    }

    auto& record = ev->Get()->Record;
    for (const auto& partitionResult : record.GetPartResult()) {
        const auto partitionId = partitionResult.GetPartition();
        const auto endOffset = partitionResult.GetEndOffset();
        const auto generation = partitionResult.GetGeneration();
        const auto cookie = partitionResult.GetCookie();

        for (const auto& consumerResult : partitionResult.GetConsumerResult()) {
            const auto& consumerName = consumerResult.GetConsumer();

            auto mit = mlpConsumers.find(consumerName);
            if (mit != std::end(mlpConsumers)) {
                auto [it, _] = Consumers.try_emplace(consumerName, *this);
                auto& consumer = it->second;

                auto useForReading = consumerResult.GetUseForReading();
                auto messages = std::max<i64>(0, endOffset - consumerResult.GetCommitedOffset());
                mit->second |=consumer.SetUseForReading(partitionId, messages, useForReading, generation, cookie);
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
                     0, // expected messages
                     false, // use for reading
                     record.GetGeneration(),
                     record.GetCookie());
}

void TMLPBalancer::Handle(TEvPQ::TEvMLPConsumerStatus::TPtr& ev) {
    auto& record = ev->Get()->Record;
    PQ_LOG_D("Handle TEvPQ::TEvMLPConsumerStatus " << record.ShortDebugString());
    SetUseForReading(record.GetConsumer(),
                     record.GetPartitionId(),
                     record.GetMessages(), // expected messages
                     record.GetUseForReading(), // use for reading
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
                consumer.SetUseForReading(partitionId, 0, true, 0, 0);
            }
            consumer.Rebuild();
        } else {
            Consumers.erase(consumerName);
        }
    }
}

void TMLPBalancer::SetUseForReading(const TString& consumerName, ui32 partitionId, ui64 messages, bool useForReading, ui32 generation, ui64 cookie) {
    auto* consumerConfig = NPQ::GetConsumer(GetConfig(), consumerName);
    if (!consumerConfig) {
        PQ_LOG_D("Consumer '" << consumerName << "' does not exist");
        return;
    }

    if (consumerConfig->GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
        PQ_LOG_D("Consumer '" << consumerName << "' is not MLP consumer");
        return;
    }

    auto [it, _] = Consumers.try_emplace(consumerName, *this);
    auto& consumer = it->second;

    if (consumer.SetUseForReading(partitionId, messages, useForReading, generation, cookie)) {
        consumer.Rebuild();
    }

}

const NKikimrPQ::TPQTabletConfig& TMLPBalancer::GetConfig() const {
    return TopicActor.TabletConfig;
}

const TPartitionGraph& TMLPBalancer::GetPartitionGraph() const {
    return TopicActor.PartitionGraph;
}

} // namespace NKikimr::NPQ::NBalancing
