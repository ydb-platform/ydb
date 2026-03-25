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

    bool SetUseForReading(ui32 partitionId, ui64 messages, std::optional<bool> readingIsFinished,
        std::optional<bool> useForReading, ui32 generation, ui64 cookie);
    void Rebuild();

private:
    const TMLPBalancer& Balancer;

    ui32 PartitionIterator = 0;
    std::vector<ui32> PartitionsForBalancing;

    struct TPartitionStatus {
        ui64 Messages = 0;
        ui64 Cookie = 0;
        ui32 Generation = 0;
        // True if the reading of the partition is inactive and last messages ware committed.
        bool ReadingIsFinished = true;
        // True if the partition is may used for reading.
        bool UseForReading = false;
    };
    absl::flat_hash_map<ui32, TPartitionStatus> Partitions;
};

class TMLPBalancer {
public:
    explicit TMLPBalancer(TPersQueueReadBalancer& topicActor);

    void Handle(TEvPQ::TEvMLPGetPartitionRequest::TPtr&);

    void Handle(TEvPersQueue::TEvStatusResponse::TPtr&, const TActorContext&);
    void Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvMLPConsumerStatus::TPtr&);

    void UpdateConfig(const std::vector<ui32>& addedPartitions);

    void SetUseForReading(const TString& consumerName,
                          ui32 partitionId,
                          ui64 messages,
                          std::optional<bool> readingIsFinished,
                          std::optional<bool> useForReading,
                          ui32 generation,
                          ui64 cookie);

    const NKikimrPQ::TPQTabletConfig& GetConfig() const;
    const TPartitionGraph& GetPartitionGraph() const;
    const std::vector<ui32>& GetActivePartitions() const;

private:
    TPersQueueReadBalancer& TopicActor;

    absl::flat_hash_map<TString, TMLPConsumer> Consumers;
};

} // namespace NKikimr::NPQ::NBalancing
