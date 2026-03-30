#pragma once

#include "mlp.h"
#include "mlp_common.h"

#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/pqdata_mlp.pb.h>

#include <library/cpp/time_provider/time_provider.h>

#include <deque>
#include <map>


namespace NKikimr::NPQ::NMLP {

class TStorage {
    static constexpr size_t MAX_MESSAGES = 48000;
    static constexpr size_t MIN_MESSAGES = 100;
    static constexpr size_t MAX_PROCESSING_COUNT = 1023;
    static constexpr TDuration VACUUM_INTERVAL = TDuration::Seconds(1);

    struct TParentPartitionExternalLockInfo;

public:
    // The maximum number of messages per flight. If a larger number is required, then you need
    // to increase the number of partitions in the topic.
    const size_t MaxMessages;
    // The minimum number of messages in flight. We try to maintain this number of messages so
    // that we can respond without delay.
    const size_t MinMessages;

    const size_t MaxFastMessages;
    const size_t MaxSlowMessages;

    // The maximum supported time delta. If it has reached this value, then it is necessary
    // to shift the BaseDeadline. Allows you to store deadlines for up to 18 hours.
    static constexpr size_t MaxDeadlineDelta = Max<ui16>();
    static constexpr TDuration MaxDeadline = TDuration::Hours(12);

public:
    enum class EMessageStatus : int {
        // The message is waiting to be processed.
        Unprocessed = 0,
        // The message is locked because it is currently being processed.
        Locked = 1,
        // Message processing completed successfully.
        Committed = 2,
        // The message needs to be moved to the DLQ queue.
        DLQ = 3,
        // The message is delayed and will be processed after the delay expires.
        Delayed = 4,
    };

    struct TMessage {
        ui32 Status: 3 = static_cast<ui32>(EMessageStatus::Unprocessed);
        ui32 Reserve: 3 = 0;
        // It stores how many times the message was submitted to work.
        // If the value is large, then the message has been processed several times,
        // but it has never been processed successfully.
        ui32 ProcessingCount: 10 = 0;
        // For locked messages, the time after which the message should be returned to the queue by timeout.
        ui32 DeadlineDelta: 16 = 0;
        ui32 HasMessageGroupId: 1 = false;
        // Hash of the message group. For consumers who keep the order of messages, it is guaranteed that
        // messages within the same group are processed sequentially in the order in which they were recorded
        // in the topic.
        ui32 MessageGroupIdHash: 31 = 0;
        ui32 WriteTimestampDelta: 26 = 0;
        ui32 Reserve2: 6 = 0;
        ui32 LockingTimestampMilliSecondsDelta: 26 = 0;
        ui32 LockingTimestampSign: 1 = 0;
        ui32 Reserve3: 5 = 0;

        EMessageStatus GetStatus() const {
            return static_cast<EMessageStatus>(Status);
        }

        void SetStatus(EMessageStatus status) {
            Status = static_cast<ui32>(status);
        }
    };
    static_assert(sizeof(TMessage) == sizeof(ui32) * 4);

    struct TMessageWrapper {
        bool SlowZone;
        ui64 Offset;
        EMessageStatus Status;
        ui32 ProcessingCount;
        TInstant ProcessingDeadline;
        TInstant WriteTimestamp;
        TInstant LockingTimestamp;
        std::optional<ui32> MessageGroupIdHash;
        bool MessageGroupIsLocked;
    };

    struct TMessageIterator {
        TMessageIterator(const TStorage& storage, std::map<ui64, TMessage>::const_iterator it, ui64 offset);

        TMessageIterator& operator++();
        TMessageWrapper operator*() const;
        bool operator==(const TMessageIterator& other) const;

    private:
        const TStorage& Storage;
        std::map<ui64, TMessage>::const_iterator Iterator;
        ui64 Offset;
    };

    friend struct TMessageIterator;

    struct TBatch {
        friend class TStorage;

        TBatch(TStorage* storage);

        bool SerializeTo(NKikimrPQ::TMLPStorageWAL&);

        bool Empty() const;

        size_t AddedMessageCount() const;
        size_t ChangedMessageCount() const;
        size_t DLQMessageCount() const;
        bool GetPurged() const;

    protected:
        void AddNewMessage(ui64 offset);
        void AddChange(ui64 offset);
        void AddToDLQ(ui64 offset, ui64 seqNo);
        void MoveToSlow(ui64 offset);
        void DeleteFromSlow(ui64 offset);
        void SetPurged();
        void SetUpdateExternalLockedMessageGroupsId(ui32 parentPartitionId);

        void Compacted(size_t count);
        void MoveBaseTime(TInstant baseDeadline, TInstant baseWriteTimestamp);

    private:
        TStorage* Storage;

        std::vector<ui64> ChangedMessages;
        std::optional<ui64> FirstNewMessage;
        size_t NewMessageCount = 0;
        std::vector<TDLQMessage> AddedToDLQ;
        std::vector<ui64> MovedToSlowZone;
        std::vector<ui64> DeletedFromSlowZone;
        size_t CompactedMessages = 0;
        bool Purged = false;
        absl::flat_hash_set<ui32> UpdateExternalLockedMessageGroupsId;

        std::optional<TInstant> BaseDeadline;
        std::optional<TInstant> BaseWriteTimestamp;
    };

    struct TStorageSettings {
        size_t MinMessages = MIN_MESSAGES;
        size_t MaxMessages = MAX_MESSAGES;
        bool KeepMessageOrder = false;
        std::vector<ui32> ParentPartitionId; // 0 - root, 1 - merge, 2 - split
    };

    explicit TStorage(TIntrusivePtr<ITimeProvider> timeProvider, const TStorageSettings& settings);

    void SetMaxMessageProcessingCount(ui32 MaxMessageProcessingCount);
    void SetRetentionPeriod(std::optional<TDuration> retentionPeriod);
    void SetDeadLetterPolicy(std::optional<NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy> deadLetterPolicy);

    ui64 GetFirstOffset() const;
    size_t GetMessageCount() const;
    ui64 GetLastOffset() const;
    ui64 GetFirstUncommittedOffset() const;
    ui64 GetFirstUnlockedOffset() const;
    TInstant GetBaseDeadline() const;
    TInstant GetBaseWriteTimestamp() const;
    TInstant GetMessageDeadline(ui64 message);
    std::pair<const TMessage*, bool> GetMessage(ui64 message);
    bool DLQEmpty() const;
    std::deque<TDLQMessage> GetDLQMessages();
    const absl::flat_hash_set<ui32>& GetLockedMessageGroupsId() const;
    void InitMetrics();
    bool HasRetentionExpiredMessages() const;
    bool GetKeepMessageOrder() const;
    bool HasUnlockedMessageGroupsId() const;
    NKikimrPQ::EReadWithKeepOrder ReadWithKeepOrder() const;

    struct TPosition {
        std::optional<std::map<ui64, TMessage>::iterator> SlowPosition;
        ui64 FastPosition = 0;
    };
    // Return the next message for client processing.
    // deadline - time for processing visibility
    // fromOffset indicates from which offset it is necessary to continue searching for the next free message.
    //            it is an optimization for the case when the method is called several times in a row.
    std::optional<TReadMessage> Next(TInstant deadline, TPosition& position);
    bool Commit(ui64 message);
    bool Unlock(ui64 message);
    // For SQS compatibility
    // https://docs.amazonaws.cn/en_us/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html
    bool ChangeMessageDeadline(ui64 message, TInstant deadline);
    bool Purge(ui64 endOffset);
    bool AddMessage(ui64 offset, bool hasMessagegroup, ui32 messageGroupIdHash, TInstant writeTimestamp, TDuration delay = TDuration::Zero());
    bool MarkDLQMoved(TDLQMessage message);
    bool WakeUpDLQ();
    struct TUpdateExternalLockedMessageGroupsResult {
        bool Applied : 1 = false;
        bool Invalid : 1 = false;
        bool VersionChanged : 1 = false;
        bool ModeChanged : 1 = false;
        bool SetChanged : 1 = false;
    };
    TUpdateExternalLockedMessageGroupsResult UpdateExternalLockedMessageGroupsId(const NKikimrPQ::TExternalLockedMessageGroupsId&);

    size_t ProccessDeadlines();
    size_t Compact();
    void MoveBaseDeadline();

    TBatch ExtractBatch();
    bool IsBatchEmpty() const;

    bool Initialize(const NKikimrPQ::TMLPStorageSnapshot& snapshot);
    bool SerializeTo(NKikimrPQ::TMLPStorageSnapshot& snapshot);
    bool ApplyWAL(const NKikimrPQ::TMLPStorageWAL&);
    void SerializeFullExternalLockedMessageGroupsIdTo(NKikimrPQ::TExternalLockedMessageGroupsId& msg, const TParentPartitionExternalLockInfo& info) const;

    const TMetrics& GetMetrics() const;

    TString DebugString() const;

    TMessageIterator begin() const;
    TMessageIterator end() const;

private:
    // offsetDelte, TMessage
    std::pair<const TMessage*, bool> GetMessageInt(ui64 offset) const;
    std::pair<TMessage*, bool> GetMessageInt(ui64 offset);
    std::pair<TMessage*, bool> GetMessageInt(ui64 offset, EMessageStatus expectedStatus);
    ui64 NormalizeDeadline(TInstant deadline);

    ui64 DoLock(ui64 offset, TMessage& message, TInstant deadline);
    bool DoCommit(ui64 offset, size_t& totalMetrics);
    bool DoUnlock(ui64 offset);
    void DoUnlock(ui64 offset, TMessage& message);
    bool DoUndelay(ui64 offset);
    TUpdateExternalLockedMessageGroupsResult DoUpdateExternalLockedMessageGroupsId(const NKikimrPQ::TExternalLockedMessageGroupsId&, bool loadState);
    bool CanReadMessageGroupIdHash(ui32 messageGroupIdHash) const;

    void UpdateFirstUncommittedOffset();

    TInstant GetMessageLockingTime(const TMessage& message) const;
    void SetMessageLockingTime(TMessage& message, const TInstant& lockingTime, const TInstant& baseDeadline) const;
    void UpdateMessageLockingDurationMetrics(const TMessage& message);
    void MoveBaseDeadline(TInstant newBaseDeadline, TInstant newBaseWriteTimestamp);

    void RemoveMessage(ui64 offset, const TMessage& message);
    void UpdateMessageMetrics(const TMessage& message);

    std::optional<ui32> GetRetentionDeadlineDelta() const;

private:
    const TIntrusivePtr<ITimeProvider> TimeProvider;

    bool KeepMessageOrder = false;
    ui32 MaxMessageProcessingCount = 1000;
    std::optional<TDuration> RetentionPeriod = TDuration::Days(365);
    std::optional<NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy> DeadLetterPolicy;

    // Offset of the first message loaded for processing. All messages with a smaller offset
    // have either already been committed or deleted from the partition.
    ui64 FirstOffset = 0;
    ui64 FirstUncommittedOffset = 0;
    ui64 FirstUnlockedOffset = 0;

    TInstant BaseDeadline;
    TInstant BaseWriteTimestamp;
    TInstant NextVacuumRun;

    std::deque<TMessage> Messages;
    std::map<ui64, TMessage> SlowMessages;
    absl::flat_hash_set<ui32> LockedMessageGroupsId;
    std::deque<TDLQMessage> DLQQueue;
    // offset->seqNo
    absl::flat_hash_map<ui64, ui64> DLQMessages;

    struct TParentPartitionExternalLockInfo {
        ui32 PartitionId = 0;
        NKikimrPQ::EReadWithKeepOrder ReadWithKeepOrder = NKikimrPQ::EReadWithKeepOrder::READ_WITH_KEEP_ORDER_BLOCK_ALL;
        ui64 TabletGeneration = 0;
        ui64 ConsumerGeneration = 0;
        ui64 ConsumerStep = 0;
    };
    std::vector<TParentPartitionExternalLockInfo> ParentPartitionExternalLockInfo;

    TBatch Batch;
    TMetrics Metrics;
};



}
