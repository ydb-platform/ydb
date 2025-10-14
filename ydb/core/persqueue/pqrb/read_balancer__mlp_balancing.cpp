#include "read_balancer__mlp_balancing.h"
#include "read_balancer_log.h"

namespace NKikimr::NPQ::NBalancing {

TMLPConsumer::TMLPConsumer(TMLPBalancer& balancer)
    : Balancer(balancer) {
}

const NKikimrPQ::TPQTabletConfig& TMLPConsumer::GetConfig() const {
    return Balancer.GetConfig();
}

const NKikimrPQ::TPQTabletConfig::TPartition& TMLPConsumer::NextPartition() {
    // TODO skip inactive partitions without messages
    auto partitionId = PartitionIterator++ % GetConfig().GetPartitions().size();
    return GetConfig().GetPartitions(partitionId);
}

TMLPBalancer::TMLPBalancer(TPersQueueReadBalancer& topicActor)
    : TopicActor(topicActor) {
}

void TMLPBalancer::Handle(TEvPersQueue::TEvMLPGetPartitionRequest::TPtr& ev) {
    auto& consumerName = ev->Get()->GetConsumer();

    auto* consumerConfig = NPQ::GetConsumer(GetConfig(), consumerName);
    if (!consumerConfig) {
        PQ_LOG_D("Consumer '" << consumerName << "' not found");
        TopicActor.Send(ev->Sender, new TEvPersQueue::TEvMLPGetPartitionResponse(NPersQueue::NErrorCode::EErrorCode::SCHEMA_ERROR), 0, ev->Cookie);
        return;
    }

    // TODO check that consumer is MLP

    auto [it, _] = Consumers.try_emplace(consumerName, *this);
    auto& consumer = it->second;

    auto& partitionConfig = consumer.NextPartition();
    TopicActor.Send(ev->Sender, new TEvPersQueue::TEvMLPGetPartitionResponse(partitionConfig.GetPartitionId(), partitionConfig.GetTabletId()), 0, ev->Cookie);
}

const NKikimrPQ::TPQTabletConfig& TMLPBalancer::GetConfig() const {
    return TopicActor.TabletConfig;
}

} // namespace NKikimr::NPQ::NBalancing
