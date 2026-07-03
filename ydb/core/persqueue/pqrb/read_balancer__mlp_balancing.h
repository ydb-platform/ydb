#pragma once

#include "read_balancer.h"

namespace NKikimr::NPQ::NBalancing {

class TMLPBalancer;

class TMLPConsumer {
public:
    struct TMetrics {
        ui64 Messages = 0;
        ui64 DelayedMessages = 0;
        ui64 LockedMessages = 0;
    };

public:
    explicit TMLPConsumer(TMLPBalancer& balancer);

    // When receiveAttemptId is set, repeated calls with the same id return the same
    // partition (kept in a runtime map) so that SQS FIFO replay reads hit one partition.
    const TPartitionGraph::Node* NextPartition(const TString& receiveAttemptId = {});

    const NKikimrPQ::TPQTabletConfig& GetConfig() const;
    const TPartitionGraph& GetPartitionGraph() const;

    bool SetUseForReading(
        ui32 partitionId,
        std::optional<bool> readingIsFinished,
        std::optional<bool> useForReading,
        const std::optional<TMetrics>& metrics,
        ui32 generation,
        ui64 cookie
    );
    void Rebuild();

    const TMetrics& GetMetrics() const;

private:
    const TMLPBalancer& Balancer;

    ui32 PartitionIterator = 0;
    std::vector<ui32> PartitionsForBalancing;

    // Runtime-only mapping of SQS FIFO receive-request-attempt-id to the partition it was
    // routed to. Not persisted; lives as long as the balancer keeps the consumer in memory.
    absl::flat_hash_map<TString, ui32> ReceiveAttemptPartitions;

    struct TPartitionStatus {
        ui64 Cookie = 0;
        ui32 Generation = 0;
        // True if the reading of the partition is inactive and last messages ware committed.
        bool ReadingIsFinished = true;
        // True if the partition is may used for reading.
        bool UseForReading = false;
        TMetrics Metrics;
    };
    absl::flat_hash_map<ui32, TPartitionStatus> Partitions;
    TMetrics Metrics;
};

class TMLPBalancer {
public:
    explicit TMLPBalancer(TPersQueueReadBalancer& topicActor);

    void Handle(TEvPQ::TEvMLPGetPartitionRequest::TPtr&);
    void Handle(TEvPQ::TEvMLPGetRuntimeAttributesRequest::TPtr&);

    void Handle(TEvPersQueue::TEvStatusResponse::TPtr&, const TActorContext&);
    void Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvMLPConsumerStatus::TPtr&);

    void UpdateConfig(const std::vector<ui32>& addedPartitions);

    void SetUseForReading(const TString& consumerName,
                          ui32 partitionId,
                          std::optional<bool> readingIsFinished,
                          std::optional<bool> useForReading,
                          const std::optional<TMLPConsumer::TMetrics>& metrics,
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
