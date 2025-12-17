#pragma once

#include "read_balancer.h"

namespace NKikimr::NPQ::NBalancing {

class TMLPBalancer;

class TMLPConsumer {
public:
    explicit TMLPConsumer(TMLPBalancer& balancer);

    const NKikimrPQ::TPQTabletConfig::TPartition& NextPartition();

    const NKikimrPQ::TPQTabletConfig& GetConfig() const;

private:
    const TMLPBalancer& Balancer;
    ui32 PartitionIterator = 0;
};

class TMLPBalancer {
public:
    explicit TMLPBalancer(TPersQueueReadBalancer& topicActor);

    void Handle(TEvPQ::TEvMLPGetPartitionRequest::TPtr&);

    const NKikimrPQ::TPQTabletConfig& GetConfig() const;

private:
    TPersQueueReadBalancer& TopicActor;

    std::unordered_map<TString, TMLPConsumer> Consumers;
};

} // namespace NKikimr::NPQ::NBalancing
