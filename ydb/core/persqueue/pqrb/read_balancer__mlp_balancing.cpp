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
    // TODO MLP skip inactive partitions without messages
    AFL_ENSURE(!GetConfig().GetAllPartitions().empty());
    auto partitionId = PartitionIterator++ % GetConfig().GetAllPartitions().size();
    return GetConfig().GetAllPartitions(partitionId);
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

    auto [it, _] = Consumers.try_emplace(consumerName, *this);
    auto& consumer = it->second;

    auto& partitionConfig = consumer.NextPartition();
    TopicActor.Send(ev->Sender, new TEvPQ::TEvMLPGetPartitionResponse(partitionConfig.GetPartitionId(), partitionConfig.GetTabletId()), 0, ev->Cookie);
}

const NKikimrPQ::TPQTabletConfig& TMLPBalancer::GetConfig() const {
    return TopicActor.TabletConfig;
}

} // namespace NKikimr::NPQ::NBalancing
