#pragma once

#include "operation_queue_timer.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

class TSchemeShard::TBackgroundCompactionQueue
    : public NOperationQueue::TOperationQueueWithTimer<
        TShardCompactionInfo,
        TCompactionQueueImpl,
        TEvPrivate::EvRunBackgroundCompaction,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        NKikimrServices::TActivity::SCHEMESHARD_BACKGROUND_COMPACTION>
{
    using TBase = NOperationQueue::TOperationQueueWithTimer<
        TShardCompactionInfo,
        TCompactionQueueImpl,
        TEvPrivate::EvRunBackgroundCompaction,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        NKikimrServices::TActivity::SCHEMESHARD_BACKGROUND_COMPACTION>;

public:
    using TBase::TBase;
};

class TSchemeShard::TBackgroundCompactionStarter : public TBackgroundCompactionQueue::IStarter {
public:
    explicit TBackgroundCompactionStarter(TSchemeShard* self)
        : Self(self)
    { }

    NOperationQueue::EStartStatus StartOperation(const TShardCompactionInfo& info) override {
        return Self->StartBackgroundCompaction(info);
    }

    void OnTimeout(const TShardCompactionInfo& info) override {
        Self->OnBackgroundCompactionTimeout(info);
    }

private:
    TSchemeShard* Self;
};

class TSchemeShard::TBorrowedCompactionQueue
    : public NOperationQueue::TOperationQueueWithTimer<
        TShardIdx,
        TFifoQueue<TShardIdx>,
        TEvPrivate::EvRunBorrowedCompaction,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        NKikimrServices::TActivity::SCHEMESHARD_BORROWED_COMPACTION>
{
    using TBase = NOperationQueue::TOperationQueueWithTimer<
        TShardIdx,
        TFifoQueue<TShardIdx>,
        TEvPrivate::EvRunBorrowedCompaction,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        NKikimrServices::TActivity::SCHEMESHARD_BORROWED_COMPACTION>;

public:
    using TBase::TBase;
};

class TSchemeShard::TBorrowedCompactionStarter : public TBorrowedCompactionQueue::IStarter {
public:
    explicit TBorrowedCompactionStarter(TSchemeShard* self)
        : Self(self)
    { }

    NOperationQueue::EStartStatus StartOperation(const TShardIdx& shardIdx) override {
        return Self->StartBorrowedCompaction(shardIdx);
    }

    void OnTimeout(const TShardIdx& shardIdx) override {
        Self->OnBorrowedCompactionTimeout(shardIdx);
    }

private:
    TSchemeShard* Self;
};

class TSchemeShard::TForcedCompactionQueue
    : public NOperationQueue::TOperationQueueWithTimer<
        TShardIdx,
        TFifoQueue<TShardIdx>,
        TEvPrivate::EvRunForcedCompaction,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        NKikimrServices::TActivity::SCHEMESHARD_FORCED_COMPACTION>
{
    using TBase = NOperationQueue::TOperationQueueWithTimer<
        TShardIdx,
        TFifoQueue<TShardIdx>,
        TEvPrivate::EvRunForcedCompaction,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        NKikimrServices::TActivity::SCHEMESHARD_FORCED_COMPACTION>;

public:
    using TBase::TBase;
};

class TSchemeShard::TForcedCompactionStarter : public TForcedCompactionQueue::IStarter {
public:
    explicit TForcedCompactionStarter(TSchemeShard* self)
        : Self(self)
    { }

    NOperationQueue::EStartStatus StartOperation(const TShardIdx& shardIdx) override {
        return Self->StartForcedCompaction(shardIdx);
    }

    void OnTimeout(const TShardIdx& shardIdx) override {
        Self->OnForcedCompactionTimeout(shardIdx);
    }

private:
    TSchemeShard* Self;
};

class TSchemeShard::TBackgroundCleaningQueue
    : public NOperationQueue::TOperationQueueWithTimer<
        TPathId,
        TFifoQueue<TPathId>,
        TEvPrivate::EvRunBackgroundCleaning,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        NKikimrServices::TActivity::SCHEMESHARD_BACKGROUND_CLEANING>
{
    using TBase = NOperationQueue::TOperationQueueWithTimer<
        TPathId,
        TFifoQueue<TPathId>,
        TEvPrivate::EvRunBackgroundCleaning,
        NKikimrServices::FLAT_TX_SCHEMESHARD,
        NKikimrServices::TActivity::SCHEMESHARD_BACKGROUND_CLEANING>;

public:
    using TBase::TBase;
};

class TSchemeShard::TBackgroundCleaningStarter : public TBackgroundCleaningQueue::IStarter {
public:
    explicit TBackgroundCleaningStarter(TSchemeShard* self)
        : Self(self)
    { }

    NOperationQueue::EStartStatus StartOperation(const TPathId& pathId) override {
        return Self->StartBackgroundCleaning(pathId);
    }

    void OnTimeout(const TPathId& pathId) override {
        Self->OnBackgroundCleaningTimeout(pathId);
    }

private:
    TSchemeShard* Self;
};

} // namespace NKikimr::NSchemeShard
