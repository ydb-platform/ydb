#pragma once

#include "read_balancer.h"

namespace NKikimr::NPQ::NBalancing {

class TMLPBalancer;

class TMLPConsumer {
public:
    explicit TMLPConsumer(TMLPBalancer& balancer);

    const TPartitionGraph::Node* NextPartition();

    const NKikimrPQ::TPQTabletConfig& GetConfig() const;
    const TPartitionGraph& GetPartitionGraph() const;

    bool SetCommittedState(ui32 partitionId, ui64 messages, bool committed, ui32 generation, ui64 cookie);
    void Rebuild();

private:
    const TMLPBalancer& Balancer;

    ui32 PartitionIterator = 0;
    std::vector<ui32> PartitionsForBalancing;

    struct TPartitionStatus {
        ui64 messages = 0; 
        ui64 cookie = 0;
        ui32 generation = 0;
        bool committed = false;
    };
    absl::flat_hash_map<ui32, TPartitionStatus> Partitions;
};

class TMLPBalancer {
public:
    explicit TMLPBalancer(TPersQueueReadBalancer& topicActor);

    void Handle(TEvPQ::TEvMLPGetPartitionRequest::TPtr&);

    void Handle(TEvPersQueue::TEvStatusResponse::TPtr&, const TActorContext&);
    void Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext& ctx);

    void UpdateConfig(const std::vector<ui32>& addedPartitions);

    const NKikimrPQ::TPQTabletConfig& GetConfig() const;
    const TPartitionGraph& GetPartitionGraph() const;

private:
    TPersQueueReadBalancer& TopicActor;

    absl::flat_hash_map<TString, TMLPConsumer> Consumers;
};

} // namespace NKikimr::NPQ::NBalancing
