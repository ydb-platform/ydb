#pragma once

#include "mlp.h"

#include <library/cpp/time_provider/time_provider.h>

#include <util/datetime/base.h>

#include <deque>
#include <unordered_set>

namespace NKikimr::NPQ::NMLP {

// TODO MLP Slow zone
class TStorage {
public:
    // The maximum number of messages per flight. If a larger number is required, then you need
    // to increase the number of partitions in the topic.
    static constexpr size_t MaxMessages = 50000;
    // The minimum number of messages in flight. We try to maintain this number of messages so
    // that we can respond without delay.
    static constexpr size_t MinMessages = 100;

    // The maximum supported time delta. If it has reached this value, then it is necessary
    // to shift the BaseDeadline. Allows you to store deadlines for up to 18 hours.
    static constexpr size_t MaxDeadlineDelta = Max<ui16>();

public:
    enum EMessageStatus {
        // The message is waiting to be processed.
        Unprocessed = 0,
        // The message is locked because it is currently being processed.
        Locked = 1,
        // Message processing completed successfully.
        Committed = 2,
        // The message needs to be moved to the DLQ queue.
        DLQ = 3
    };

    struct TMessage {
        ui64 Status: 3 = EMessageStatus::Unprocessed;
        ui64 Reserve: 2;
        ui64 HasMessageGroupId: 1 = false;
        // It stores how many times the message was submitted to work.
        // If the value is large, then the message has been processed several times,
        // but it has never been processed successfully.
        ui64 ReceiveCount: 10 = 0;
        // For locked messages, the time after which the message should be returned to the queue by timeout.
        ui64 DeadlineDelta: 16 = 0;
        // Hash of the message group. For consumers who keep the order of messages, it is guaranteed that
        // messages within the same group are processed sequentially in the order in which they were recorded
        // in the topic.
        ui64 MessageGroupIdHash: 32 = 0;
    };
    static_assert(sizeof(TMessage) == sizeof(ui64));

    struct TMetrics {
        size_t InflyMessageCount = 0;
        size_t UnprocessedMessageCount = 0;
        size_t LockedMessageCount = 0;
        size_t LockedMessageGroupCount = 0;
        size_t CommittedMessageCount = 0;
        size_t DeadlineExpiredMessageCount = 0;
        size_t DLQMessageCount = 0;
    };

    TStorage(TIntrusivePtr<ITimeProvider> timeProvider);

    void SetKeepMessageOrder(bool keepMessageOrder);
    void SetMaxMessageReceiveCount(ui32 maxMessageReceiveCount);

    ui64 GetFirstOffset() const;
    ui64 GetLastOffset() const;
    ui64 GetFirstUncommittedOffset() const;
    ui64 GetFirstUnlockedOffset() const;
    TInstant GetBaseDeadline() const;
    TInstant GetMessageDeadline(TMessageId message);
    const TMessage* GetMessage(TMessageId message);


    // Return the next message for client processing.
    // deadline - time for processing visibility
    // fromOffset indicates from which offset it is necessary to continue searching for the next free message.
    //            it is an optimization for the case when the method is called several times in a row.
    struct NextResult {
        TMessageId Message;
        ui64 FromOffset;
    };
    std::optional<NextResult> Next(TInstant deadline, ui64 fromOffset = 0);
    bool Commit(TMessageId message);
    bool Unlock(TMessageId message);
    // For SQS compatibility
    // https://docs.amazonaws.cn/en_us/AWSSimpleQueueService/latest/APIReference/API_ChangeMessageVisibility.html
    bool ChangeMessageDeadline(TMessageId message, TInstant deadline);

    void AddMessage(ui64 offset, bool hasMessagegroup, ui32 messageGroupIdHash);

    size_t ProccessDeadlines();
    // TODO MLP удалять сообщения если в партиции сместился StartOffset
    size_t Compact();
    void MoveBaseDeadline();

    bool InitializeFromSnapshot(const NKikimrPQ::TMLPStorageSnapshot& snapshot);
    bool CreateSnapshot(NKikimrPQ::TMLPStorageSnapshot& snapshot);

    const TMetrics& GetMetrics() const;

    TString DebugString() const;

private:
    // offsetDelte, TMessage
    TMessage* GetMessageInt(ui64 offset);
    TMessage* GetMessageInt(ui64 offset, EMessageStatus expectedStatus);
    ui64 NormalizeDeadline(TInstant deadline);

    TMessageId DoLock(ui64 offsetDelta, TInstant deadline);
    bool DoCommit(ui64 offset);
    bool DoUnlock(ui64 offset);
    void DoUnlock(TMessage& message, ui64 offset);

    void UpdateFirstUncommittedOffset();

private:
    const TIntrusivePtr<ITimeProvider> TimeProvider;

    bool KeepMessageOrder = false;
    ui32 MaxMessageReceiveCount = 1000;

    // Offset of the first message loaded for processing. All messages with a smaller offset
    // have either already been committed or deleted from the partition.
    ui64 FirstOffset = 0;
    ui64 FirstUncommittedOffset = 0;
    ui64 FirstUnlockedOffset = 0;

    TInstant BaseDeadline;

    std::deque<TMessage> Messages;
    std::unordered_set<ui32> LockedMessageGroupsId;

    TMetrics Metrics;
};



}
