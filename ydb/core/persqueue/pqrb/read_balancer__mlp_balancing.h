#pragma once

#include "read_balancer.h"

namespace NKikimr::NPQ::NBalancing {

class TMLPBalancer;

struct TNextPartitionPersistChanges {
    std::optional<TReceiveAttemptPartitionUpsert> Upsert;
    std::vector<TReceiveAttemptPartitionDelete> Deletes;
};

struct TPrepareGetPartitionResponse {
    bool IsError = false;
    Ydb::StatusIds::StatusCode ErrorStatus = Ydb::StatusIds::SUCCESS;
    TString ErrorMessage;
    const TPartitionGraph::Node* Node = nullptr;
    TNextPartitionPersistChanges PersistChanges;
};

class TMLPConsumer {
public:
    struct TMetrics {
        ui64 Messages = 0;
        ui64 DelayedMessages = 0;
        ui64 LockedMessages = 0;
    };

public:
    explicit TMLPConsumer(TMLPBalancer& balancer, const TString& consumerName);

    // When receiveAttemptId is set, repeated calls with the same id return the same
    // partition (kept in a persisted map) so that SQS FIFO replay reads hit one partition.
    TPrepareGetPartitionResponse PrepareGetPartitionResponse(const TString& receiveAttemptId, TInstant now);

    void RestoreReceiveAttemptPartition(const TString& receiveAttemptId, ui32 partitionId, TInstant expiry);
    std::vector<TReceiveAttemptPartitionDelete> CollectExpiredReceiveAttemptPartitions(TInstant now);

    const TMetrics& GetMetrics() const;

    bool SetUseForReading(
        ui32 partitionId,
        std::optional<bool> readingIsFinished,
        std::optional<bool> useForReading,
        const std::optional<TMetrics>& metrics,
        ui32 generation,
        ui64 cookie
    );
    void Rebuild();

    const NKikimrPQ::TPQTabletConfig& GetConfig() const;
    const TPartitionGraph& GetPartitionGraph() const;

private:
    TDuration GetReceiveAttemptIdPeriod() const;
    TReceiveAttemptPartitionDelete MakeDeleteKey(const TString& receiveAttemptId) const;
    const TPartitionGraph::Node* PickNextPartition();

    const TMLPBalancer& Balancer;
    const TString ConsumerName;

    ui32 PartitionIterator = 0;
    std::vector<ui32> PartitionsForBalancing;

    // Mapping of SQS FIFO receive-request-attempt-id to the partition it was routed to.
    struct TReceiveAttemptPartition {
        ui32 PartitionId = 0;
        TInstant Expiry;
    };
    absl::flat_hash_map<TString, TReceiveAttemptPartition> ReceiveAttemptPartitions;

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

    TPrepareGetPartitionResponse PrepareGetPartitionResponse(
        const TString& consumerName,
        const TString& receiveAttemptId,
        TInstant now
    );
    void RestoreReceiveAttemptPartition(
        const TString& consumerName,
        const TString& receiveAttemptId,
        ui32 partitionId,
        TInstant expiry
    );
    std::vector<TReceiveAttemptPartitionDelete> CollectExpiredReceiveAttemptPartitions(TInstant now);

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
