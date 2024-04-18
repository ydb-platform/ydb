#pragma once

#include "read_balancer.h"

namespace NKikimr::NPQ::NBalancing {

using namespace NTabletFlatExecutor;

struct TSession;
struct TConsumer;
class TBalancer;

struct TPartition {
    // Client had commited rad offset equals EndOffset of the partition
    bool Commited = false;
    // ReadSession reach EndOffset of the partition
    bool ReadingFinished = false;
    // ReadSession connected with new SDK with garantee of read order
    bool ScaleAwareSDK = false;
    // ReadSession reach EndOffset of the partition by first request
    bool StartedReadingFromEndOffset = false;

    size_t Iteration = 0;
    ui64 Cookie = 0;

    TActorId LastPipe;

        // Generation of PQ-tablet and cookie for synchronization of commit information.
    ui32 PartitionGeneration;
    ui64 PartitionCookie;

    // Return true if the reading of the partition has been finished and children's partitions are readable.
    bool IsFinished() const;
    // Return true if children's partitions can't be balance separately.
    bool NeedReleaseChildren() const;
    bool BalanceToOtherPipe() const;

    // Called when reading from a partition is started.
    // Return true if the reading of the partition has been finished before.
    bool StartReading();
    // Called when reading from a partition is stopped.
    // Return true if children's partitions can't be balance separately.
    bool StopReading();

    // Called when the partition is inactive and commited offset is equal to EndOffset.
    // Return true if the commited status changed.
    bool SetCommittedState(ui32 generation, ui64 cookie);
    // Called when the partition reading finished.
    // Return true if the reading status changed.
    bool SetFinishedState(bool scaleAwareSDK, bool startedReadingFromEndOffset);
    // Called when the parent partition is reading.
    bool Reset();
};

// Multiple partitions balancing together always in one reading session
struct TPartitionFamilty {
    enum class EStatus {
        Active,    // The family are reading
        Releasing, // The family is waiting for partition to be released
        Free,      // The family isn't reading
        Destroyed  // The family will destroyed after releasing
    };

    TConsumer& Consumer;

    size_t Id;
    EStatus Status;
    EStatus TargetStatus;

    // Partitions that are in the family
    std::vector<ui32> Partitions;
    // Partitions wich was added to the family.
    std::set<ui32> AttachedPartitions;

    // The reading session in which the family is currently being read.
    TSession* Session;
    // Partitions that are in the family
    std::unordered_set<ui32> LockedPartitions;

    // The number of active partitions in the family
    size_t ActivePartitionCount;
    // The number of inactive partitions in the family
    size_t InactivePartitionCount;

    // Reading sessions that have a list of partitions to read and these sessions can read this family
    std::unordered_map<TActorId, TSession*> SpecialSessions;

    TPartitionFamilty(TConsumer& consumerInfo, size_t id, std::vector<ui32>&& partitions);
    ~TPartitionFamilty() = default;

    // Releases all partitions of the family.
    void Release(const TActorContext& ctx, EStatus targetStatus = EStatus::Free);
    // Processes the signal from the reading session that the partition has been released.
    // Return true if all partitions has been unlocked.
    bool Unlock(const TActorId& sender, ui32 partitionId, const TActorContext& ctx);
    // Processes the signal that the reading session has ended.
    void Reset();
    // Starts reading the family in the specified reading session.
    void StartReading(TSession& session, const TActorContext& ctx);
    // Add partitions to the family.
    void AttachePartitions(const std::vector<ui32>& partitions, const TActorContext& ctx);

    // The partition became active
    void ActivatePartition(ui32 partitionId);
    // The partition became inactive
    void InactivatePartition(ui32 partitionId);

    TString DebugStr() const;

private:
    const TString& Topic() const;
    const TString& TopicPath() const;
    ui32 TabletGeneration() const;

    const TPartitionInfo* GetPartitionInfo(ui32 partitionId) const;
    TPartition* GetPartitionStatus(ui32 partitionId);
    bool IsReadable(ui32 partitionId) const;
    ui32 NextStep();

private:
    template<typename TPartitions>
    std::pair<size_t, size_t> ClassifyPartitions(const TPartitions& partitions);
    void UpdatePartitionMapping(const std::vector<ui32>& partitions);
    void UpdateSpecialSessions();
    void LockPartition(ui32 partitionId, const TActorContext& ctx);
    std::unique_ptr<TEvPersQueue::TEvReleasePartition> MakeEvReleasePartition(ui32 partitionId) const;
    std::unique_ptr<TEvPersQueue::TEvLockPartition> MakeEvLockPartition(ui32 partitionId, ui32 step) const;
    TString GetPrefix() const;
};

struct TPartitionFamilyComparator {
    bool operator()(const TPartitionFamilty* lhs, const TPartitionFamilty* rhs) const {
        if (lhs->ActivePartitionCount != rhs->ActivePartitionCount) {
            return lhs->ActivePartitionCount < rhs->ActivePartitionCount;
        }
        if (lhs->InactivePartitionCount != rhs->InactivePartitionCount) {
            return lhs->InactivePartitionCount < rhs->InactivePartitionCount;
        }
        return (lhs->Id < rhs->Id);
    }
};

using TOrderedPartitionFamilies = std::set<TPartitionFamilty*, TPartitionFamilyComparator>;

struct TConsumer {
    TBalancer& Balancer;

    TString ConsumerName;

    size_t NextFamilyId;
    std::unordered_map<size_t, const std::unique_ptr<TPartitionFamilty>> Families;

    // Mapping the IDs of the partitions to the families they belong to
    std::unordered_map<ui32, TPartitionFamilty*> PartitionMapping;
    // All reading sessions in which the family is currently being read.
    std::unordered_map<TActorId, TSession*> ReadingSessions;

    // Families is not reading now.
    std::unordered_map<size_t, TPartitionFamilty*> UnreadableFamilies;

    std::unordered_map<ui32, TPartition> Partitions;

    ui32 Step;

    TConsumer(TBalancer& balancer, const TString& consumerName);
    ~TConsumer() = default;

    const TString& Topic() const;
    const TString& TopicPath() const;
    ui32 TabletGeneration() const;
    const TPartitionInfo* GetPartitionInfo(ui32 partitionId) const;
    TPartition* GetPartitionStatus(ui32 partitionId);
    ui32 NextStep();

    void RegisterPartition(ui32 partitionId, const TActorContext& ctx);
    void UnregisterPartition(ui32 partitionId);
    void InitPartitions(const TActorContext& ctx);

    void CreateFamily(std::vector<ui32>&& partitions, const TActorContext& ctx);
    TPartitionFamilty* FindFamily(ui32 partitionId);

    void RegisterReadingSession(TSession* session, const TActorContext& ctx);
    void UnregisterReadingSession(TSession* session);

    bool Unlock(const TActorId& sender, ui32 partitionId, const TActorContext& ctx);

    bool SetCommittedState(ui32 partitionId, ui32 generation, ui64 cookie);
    bool ProccessReadingFinished(ui32 partitionId, const TActorContext& ctx);
    void StartReading(ui32 partitionId, const TActorContext& ctx);
    void FinishReading(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx);

    void Balance(const TActorContext& ctx);
    void Release(ui32 partitionId, const TActorContext& ctx);

    bool IsReadable(ui32 partitionId);
    bool IsFinished(ui32 partitionId);

    bool ScalingSupport() const;

private:
    void Release(TPartitionFamilty* family, const TActorContext& ctx);

    TString GetPrefix() const;
};

struct TSession {
    TSession(const TActorId& pipeClient);

    // The consumer name
    TString ClientId;
    TString Session;
    TActorId Sender;
    TActorId PipeClient;

    TString ClientNode;
    ui32 ProxyNodeId;
    TInstant CreateTimestamp;

    // partitions which are reading
    std::unordered_set<ui32> Partitions;
    // the number of pipes connected from SessionActor to ReadBalancer
    ui32 ServerActors;

    size_t ActivePartitionCount;
    size_t InactivePartitionCount;

    // The partition families that are being read by this session.
    TOrderedPartitionFamilies Families;

    void Init(const TString& clientId, const TString& session, const TActorId& sender, const std::vector<ui32>& partitions);

    // true if client connected to read from concret partitions
    bool WithGroups() const;
    bool AllPartitionsReadable(const std::vector<ui32>& partitions) const;

    TString DebugStr() const;
};

struct TStatistics {
    struct TConsumerStatistics {
        struct TPartitionStatistics {
            ui32 PartitionId;
            ui64 TabletId = 0;
            ui32 State = 0;
            TString Session;
        };

        TString ConsumerName;
        std::vector<TPartitionStatistics> Partitions;
    };

    struct TSessionStatistics {
        TString Session;
        size_t ActivePartitionCount;
        size_t InactivePartitionCount;
        size_t SuspendedPartitionCount;
        size_t TotalPartitionCount;
    };

    std::vector<TConsumerStatistics> Consumers;
    std::vector<TSessionStatistics> Sessions;

    size_t FreePartitions;
};

class TBalancer {
public:
    TBalancer(TPersQueueReadBalancer& topicActor);

    const TString& Topic() const;
    const TString& TopicPath() const;
    ui32 TabletGeneration() const;

    const TPartitionInfo* GetPartitionInfo(ui32 partitionId) const;
    const std::unordered_map<ui32, TPartitionInfo>& GetPartitionsInfo() const;
    const TPartitionGraph& GetPartitionGraph() const;
    bool ScalingSupport() const;
    i32 GetLifetimeSeconds() const;

    TConsumer* GetConsumer(const TString& consumerName);
    const TStatistics GetStatistics() const;

    void UpdateConfig(std::vector<ui32> addedPartitions, std::vector<ui32> deletedPartitions, const TActorContext& ctx);
    bool SetCommittedState(const TString& consumer, ui32 partitionId, ui32 generation, ui64 cookie, const TActorContext& ctx);

    void Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvReadingPartitionStartedRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPersQueue::TEvPartitionReleased::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPQ::TEvWakeupReleasePartition::TPtr &ev, const TActorContext& ctx);

    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext&);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext&);

    void Handle(TEvPersQueue::TEvRegisterReadSession::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPersQueue::TEvGetReadSessionsInfo::TPtr& ev, const TActorContext& ctx);

private:
    TString GetPrefix() const;

private:
    TPersQueueReadBalancer& TopicActor;

    std::unordered_map<TActorId, std::unique_ptr<TSession>> Sessions;
    std::unordered_map<TString, std::unique_ptr<TConsumer>> Consumers;
};

}
