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
    AFL_ENSURE(!PartitionsForBalancing.empty());

    auto partitionId = PartitionIterator++ % PartitionsForBalancing.size();
    return GetPartitionGraph().GetPartition(PartitionsForBalancing[partitionId]);
}

bool TMLPConsumer::SetCommittedState(ui32 partitionId, ui64 messages, bool committed, ui32 generation, ui64 cookie) {
    auto& status = Partitions[partitionId];

    if (status.generation < generation || (status.generation == generation && status.cookie < cookie)) {
        auto oldCommitted = status.committed;

        status.messages = messages;
        status.committed = committed;
        status.generation = generation;
        status.cookie = cookie;

        return oldCommitted != committed;
    }

    return false;
}

void TMLPConsumer::Rebuild() {
    PartitionsForBalancing.clear();

    if (Partitions.empty()) {
        for (const auto& partitionConfig : GetConfig().GetAllPartitions()) {
            Partitions[partitionConfig.GetPartitionId()] = {0, 0, 0, false};
        }
    }

    PartitionsForBalancing.reserve(Partitions.size());
    for (const auto& [partitionId, status] : Partitions) {
        if (!status.committed) {
            PartitionsForBalancing.push_back(partitionId);
        }
    }
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
        Cerr << (TStringBuilder() << ">>>>> DO rebuild" << Endl);
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
            
                bool changed = consumer.SetCommittedState(partitionId, endOffset - consumerResult.GetCommitedOffset(), consumerResult.GetReadingFinished(), generation, cookie);
                mit->second |= changed;
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
    auto& result = ev->Get()->Record;
    auto& consumerName = result.GetConsumer();

    auto* consumerConfig = NPQ::GetConsumer(GetConfig(), consumerName);
    if (!consumerConfig) {
        return;
    }

    if (consumerConfig->GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
        return;
    }

    auto [it, _] = Consumers.try_emplace(consumerName, *this);
    auto& consumer = it->second;

    if (consumer.SetCommittedState(result.GetPartitionId(), 0, true, result.GetGeneration(), result.GetCookie())) {
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
