#pragma once

#include "read_balancer.h"

namespace NKikimr::NPQ::NBalancing {

using namespace NTabletFlatExecutor;

using TPartitionInfo = TPersQueueReadBalancer::TPartitionInfo;

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

    // Generation of PQ-tablet and cookie for synchronization of commit information.
    ui32 PartitionGeneration;
    ui64 PartitionCookie;

    // Return true if the reading of the partition has been finished and children's partitions are readable.
    bool IsInactive() const;
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
struct TPartitionFamily {
    friend struct TConsumer;

    enum class EStatus {
        Active,    // The family are reading
        Releasing, // The family is waiting for partition to be released
        Free
    };

    enum class ETargetStatus {
        Free,      // The family will be free for balancing.
        Destroy,   // The family will be destroyed after releasing.
        Merge      // The family will be merged with other family.
    };

    TConsumer& Consumer;

    size_t Id;
    EStatus Status;
    ETargetStatus TargetStatus;

    std::vector<ui32> RootPartitions;
    // Partitions that are in the family
    std::vector<ui32> Partitions;

    std::unordered_set<ui32> WantedPartitions;

    // The reading session in which the family is currently being read.
    TSession* Session;
    // Partitions that are in the family
    std::unordered_set<ui32> LockedPartitions;

    // The number of active partitions in the family.
    size_t ActivePartitionCount = 0;
    // The number of inactive partitions in the family.
    size_t InactivePartitionCount = 0;

    // Reading sessions that have a list of partitions to read and these sessions can read this family
    std::unordered_map<TActorId, TSession*> SpecialSessions;

    TActorId LastPipe;
    size_t MergeTo;

    TPartitionFamily(TConsumer& consumerInfo, size_t id, std::vector<ui32>&& partitions);
    ~TPartitionFamily() = default;

    bool IsActive() const;
    bool IsFree() const;
    bool IsRelesing() const;

    bool IsCommon() const;
    bool IsLonely() const;
    bool HasActivePartitions() const;

    // Releases all partitions of the family.
    void Release(const TActorContext& ctx, ETargetStatus targetStatus = ETargetStatus::Free);
    // Processes the signal from the reading session that the partition has been released.
    // Return true if all partitions has been unlocked.
    bool Unlock(const TActorId& sender, ui32 partitionId, const TActorContext& ctx);
    // Processes the signal that the reading session has ended.
    bool Reset(const TActorContext& ctx);
    bool Reset(ETargetStatus targetStatus, const TActorContext& ctx);
    // Starts reading the family in the specified reading session.
    void StartReading(TSession& session, const TActorContext& ctx);
    // Add partitions to the family.
    void AttachePartitions(const std::vector<ui32>& partitions, const TActorContext& ctx);
    void Merge(TPartitionFamily* other);

    // The partition became active
    void ActivatePartition(ui32 partitionId);
    // The partition became inactive
    void InactivatePartition(ui32 partitionId);

    bool PossibleForBalance(TSession* session);

    TString DebugStr() const;

private:
    void Destroy(const TActorContext& ctx);
    void AfterRelease();

private:
    const TString& Topic() const;
    const TString& TopicPath() const;
    ui32 TabletGeneration() const;

    const TPartitionInfo* GetPartitionInfo(ui32 partitionId) const;
    TPartition* GetPartition(ui32 partitionId);
    bool IsReadable(ui32 partitionId) const;
    ui32 NextStep();

private:
    void ClassifyPartitions();
    template<typename TPartitions>
    std::pair<size_t, size_t> ClassifyPartitions(const TPartitions& partitions);
    void UpdatePartitionMapping(const std::vector<ui32>& partitions);
    void UpdateSpecialSessions();
    void ChangePartitionCounters(ssize_t activeDiff, ssize_t inactiveDiff);
    void LockPartition(ui32 partitionId, const TActorContext& ctx);
    std::unique_ptr<TEvPersQueue::TEvReleasePartition> MakeEvReleasePartition(ui32 partitionId) const;
    std::unique_ptr<TEvPersQueue::TEvLockPartition> MakeEvLockPartition(ui32 partitionId, ui32 step) const;
    TString GetPrefix() const;
};

struct TPartitionFamilyComparator {
    bool operator()(const TPartitionFamily* lhs, const TPartitionFamily* rhs) const;
};

using TOrderedPartitionFamilies = std::set<TPartitionFamily*, TPartitionFamilyComparator>;

struct SessionComparator {
    bool operator()(const TSession* lhs, const TSession* rhs) const;
};

using TOrderedSessions = std::set<TSession*, SessionComparator>;

// It contains all the logic of balancing the reading sessions of a single consumer: the distribution of partitions
// across reading sessions, the uniformity of the load.
struct TConsumer {
    friend struct TPartitionFamily;

    TBalancer& Balancer;

    TString ConsumerName;

    size_t NextFamilyId;
    std::unordered_map<size_t, const std::unique_ptr<TPartitionFamily>> Families;

    // Mapping the IDs of the partitions to the families they belong to
    std::unordered_map<ui32, TPartitionFamily*> PartitionMapping;
    // All reading sessions in which the family is currently being read.
    std::unordered_map<TActorId, TSession*> Sessions;

    // Families is not reading now.
    std::unordered_map<size_t, TPartitionFamily*> UnreadableFamilies;
    // Families that require balancing. Only families are included here if there are reading
    // sessions that want to read the partitions of this family.
    std::unordered_map<size_t, TPartitionFamily*> FamiliesRequireBalancing;

    std::unordered_map<ui32, TPartition> Partitions;

    bool BalanceScheduled;

    TConsumer(TBalancer& balancer, const TString& consumerName);
    ~TConsumer() = default;

    const TString& Topic() const;
    const TString& TopicPath() const;
    ui32 TabletGeneration() const;
    const TPartitionInfo* GetPartitionInfo(ui32 partitionId) const;
    TPartition* GetPartition(ui32 partitionId);
    const TPartitionGraph& GetPartitionGraph() const;
    ui32 NextStep();

    void RegisterPartition(ui32 partitionId, const TActorContext& ctx);
    void UnregisterPartition(ui32 partitionId, const TActorContext& ctx);
    void InitPartitions(const TActorContext& ctx);

    TPartitionFamily* CreateFamily(std::vector<ui32>&& partitions, const TActorContext& ctx);
    TPartitionFamily* CreateFamily(std::vector<ui32>&& partitions, TPartitionFamily::EStatus status, const TActorContext& ctx);
    bool BreakUpFamily(ui32 partitionId, bool destroy, const TActorContext& ctx);
    bool BreakUpFamily(TPartitionFamily* family, ui32 partitionId, bool destroy, const TActorContext& ctx);
    std::pair<TPartitionFamily*, bool> MergeFamilies(TPartitionFamily* lhs, TPartitionFamily* rhs, const TActorContext& ctx);
    void DestroyFamily(TPartitionFamily* family, const TActorContext& ctx);
    TPartitionFamily* FindFamily(ui32 partitionId);

    void RegisterReadingSession(TSession* session, const TActorContext& ctx);
    void UnregisterReadingSession(TSession* session, const TActorContext& ctx);

    bool Unlock(const TActorId& sender, ui32 partitionId, const TActorContext& ctx);

    bool SetCommittedState(ui32 partitionId, ui32 generation, ui64 cookie);
    bool ProccessReadingFinished(ui32 partitionId, const TActorContext& ctx);
    void StartReading(ui32 partitionId, const TActorContext& ctx);
    void FinishReading(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx);

    void ScheduleBalance(const TActorContext& ctx);
    void Balance(const TActorContext& ctx);
    void Release(ui32 partitionId, const TActorContext& ctx);

    bool IsReadable(ui32 partitionId);
    bool IsInactive(ui32 partitionId);

    bool ScalingSupport() const;

private:
    TString GetPrefix() const;
};

struct TSession {
    TSession(const TActorId& pipe);

    const TActorId Pipe;

    // The consumer name
    TString ClientId;
    TString SessionName;
    TActorId Sender;

    TString ClientNode;
    ui32 ProxyNodeId;
    TInstant CreateTimestamp;

    // partitions which are reading
    std::unordered_set<ui32> Partitions;
    // the number of pipes connected from SessionActor to ReadBalancer
    ui32 ServerActors;

    // The number of active partitions
    size_t ActivePartitionCount;
    // The number of inactive partitions
    size_t InactivePartitionCount;
    // The number of releasing partitions (active and inactive)
    size_t ReleasingPartitionCount;

    // The number of active families (the status equal Active)
    size_t ActiveFamilyCount;
    // The number of releasing families (the status equal Releasing)
    size_t ReleasingFamilyCount;

    // The partition families that are being read by this session.
    std::unordered_map<size_t, TPartitionFamily*> Families;

    // true if client connected to read from concret partitions
    bool WithGroups() const;

    template<typename TCollection>
    bool AllPartitionsReadable(const TCollection& partitions) const;

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
    friend struct TConsumer;
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

    void Handle(TEvPQ::TEvBalanceConsumer::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx);

private:
    TString GetPrefix() const;
    ui32 NextStep();

private:
    TPersQueueReadBalancer& TopicActor;

    std::unordered_map<TActorId, std::unique_ptr<TSession>> Sessions;
    std::unordered_map<TString, std::unique_ptr<TConsumer>> Consumers;

    ui32 Step;
};

}
