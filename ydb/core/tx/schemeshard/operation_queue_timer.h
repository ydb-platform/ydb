#pragma once

#include "schemeshard_identificators.h"
#include "schemeshard_info_types.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/util/operation_queue.h>

// TODO: TOperationQueueWithTimer is a good candidate for core/util, but since
// it uses actorlib_impl, which depends on core/util, it
// can't be part of util. No other better place yet and since
// it is used in schemedard only then I put it here.

namespace NKikimr {

// TODO: make part of util?
namespace NOperationQueue {

template <typename T, typename TQueue, int Ev>
class TOperationQueueWithTimer
    : public TActor<TOperationQueueWithTimer<T, TQueue, Ev>>
    , public ITimer
    , public TOperationQueue<T, TQueue>
{
    using TThis = ::NKikimr::NOperationQueue::TOperationQueueWithTimer<T, TQueue, Ev>;
    using TActorBase = TActor<TOperationQueueWithTimer<T, TQueue, Ev>>;
    using TBase = TOperationQueue<T, TQueue>;

    struct TEvWakeupQueue : public TEventLocal<TEvWakeupQueue, Ev> {
        TEvWakeupQueue() = default;
    };

private:
    TActorId LongTimerId;
    TInstant When;

public:
    TOperationQueueWithTimer(const typename TBase::TConfig& config,
                             typename TBase::IStarter& starter)
        : TActorBase(&TThis::StateWork)
        , TBase(config, starter, *this)
    {}

    template <typename TReadyQueueConfig>
    TOperationQueueWithTimer(const typename TBase::TConfig& config,
                             const TReadyQueueConfig& queueConfig,
                             typename TBase::IStarter& starter)
        : TActorBase(&TThis::StateWork)
        , TBase(config, queueConfig, starter, *this)
    {}

    void Shutdown(const TActorContext &ctx) {
        if (LongTimerId)
            ctx.Send(LongTimerId, new TEvents::TEvPoison);

        TActorBase::PassAway();
    }

    TInstant GetWakeupTime() const { return When; }

private:
    // ITimer, note that it is made private,
    // since it should be called only from TBase
    void SetWakeupTimer(TInstant t) override {
        if (When > t)
            this->Send(LongTimerId, new TEvents::TEvPoison);

        When = t;
        LongTimerId = CreateLongTimer(t - Now(),
            new IEventHandle(TActorBase::SelfId(), TActorBase::SelfId(), new TEvWakeupQueue));
    }

    TInstant Now() override {
        return AppData()->TimeProvider->Now();
    }

    void HandleWakeup() {
        TBase::Wakeup();
    }

    STFUNC(StateWork) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvWakeupQueue::EventType, HandleWakeup);
        }
    }
};

} // NOperationQueue

namespace NSchemeShard {

struct TShardCompactionInfo {
    TShardIdx ShardIdx;

    ui64 SearchHeight = 0;
    ui64 LastFullCompactionTs = 0;
    ui64 RowCount = 0;
    ui64 RowDeletes = 0;

    explicit TShardCompactionInfo(const TShardIdx& id)
        : ShardIdx(id)
    {}

    TShardCompactionInfo(const TShardIdx& id, const TTableInfo::TPartitionStats& stats)
        : ShardIdx(id)
        , SearchHeight(stats.SearchHeight)
        , LastFullCompactionTs(stats.FullCompactionTs)
        , RowCount(stats.RowCount)
        , RowDeletes(stats.RowDeletes)
    {}

    TShardCompactionInfo(const TShardCompactionInfo&) = default;

    TShardCompactionInfo& operator =(const TShardCompactionInfo& rhs) = default;

    bool operator ==(const TShardCompactionInfo& rhs) const {
        // note that only identity intentionally checked
        return ShardIdx == rhs.ShardIdx;
    }

    size_t Hash() const {
        return ShardIdx.Hash();
    }

    explicit operator size_t() const {
        return Hash();
    }

    struct TLessBySearchHeight {
        bool operator()(const TShardCompactionInfo& lhs, const TShardCompactionInfo& rhs) const {
            // note ">" is intentional to have on top items with bigger search height
            return lhs.SearchHeight > rhs.SearchHeight;
        }
    };

    struct TLessByCompactionTs {
        bool operator()(const TShardCompactionInfo& lhs, const TShardCompactionInfo& rhs) const {
            // on top we have items with less TS, i.e. older ones
            return lhs.LastFullCompactionTs < rhs.LastFullCompactionTs;
        }
    };

    struct TLessByRowDeletes {
        bool operator()(const TShardCompactionInfo& lhs, const TShardCompactionInfo& rhs) const {
            // note ">" is intentional to have on top items with bigger number of deleted rows
            return lhs.RowDeletes > rhs.RowDeletes;
        }
    };

    TString ToString() const {
        TStringStream ss;
        ss << "{" << ShardIdx
           << ", SH# " << SearchHeight
           << ", Rows# " << RowCount
           << ", Deletes# " << RowDeletes
           << ", Compaction# " << TInstant::Seconds(LastFullCompactionTs) << "}";
        return ss.Str();
    }
};

// The queue contains multiple queues inside:
// * by last full compaction TS
// * by search height
// * by number of deleted rows
// Queues are active in round robin manner. Same shard might
// be in all queues.
//
// Note that in Enqueue we do some check and might skip
// the shard being enqueued depending on config.
//
// When TOperationQueue::Update() calls TCompactionQueueImpl::UpdateIfFound(),
// TCompactionQueueImpl might remove updated item depending on the config, but
// TOperationQueue will not remove the item from running/waiting items, it will
// be fully deleted only when TOperationQueue tries to TCompactionQueueImpl::Enqueue()
// the item again.
class TCompactionQueueImpl {
public:
    struct TConfig {
        ui32 SearchHeightThreshold = 0;
        ui32 RowDeletesThreshold = 0;
        ui32 RowCountThreshold = 0;

        TConfig() = default;
    };

private:
    using TCompactionQueueLastCompaction = NOperationQueue::TQueueWithPriority<
        TShardCompactionInfo,
        TShardCompactionInfo::TLessByCompactionTs>;

     using TCompactionQueueSearchHeight = NOperationQueue::TQueueWithPriority<
        TShardCompactionInfo,
        TShardCompactionInfo::TLessBySearchHeight>;

     using TCompactionQueueRowDeletes = NOperationQueue::TQueueWithPriority<
        TShardCompactionInfo,
        TShardCompactionInfo::TLessByRowDeletes>;

    // Enumeration defines round robin order
    enum class EActiveQueue {
        ByLastCompaction, // must be first
        BySearchHeight,
        ByRowDeletes, // must be last, see PopFront()
    };

private:
    TConfig Config;

    // all shards from other queues always go into this queue,
    // i.e. if shard presents in any other queue it also presents here
    TCompactionQueueLastCompaction QueueLastCompaction;

    // note that it can be empty depending on stats and SearchHeightThreshold
    TCompactionQueueSearchHeight QueueSearchHeight;

    // note that it can be empty depending on stats and RowDeletesThreshold
    TCompactionQueueRowDeletes QueueRowDeletes;

    EActiveQueue ActiveQueue = EActiveQueue::ByLastCompaction;

public:
    TCompactionQueueImpl() = default;

    TCompactionQueueImpl(const TConfig& config)
        : Config(config)
    {}

    void UpdateConfig(const TConfig& config) {
        if (&Config != &config)
            Config = config;
    }

    bool Enqueue(const TShardCompactionInfo& info) {
        if (info.RowCount < Config.RowCountThreshold) {
            return false;
        }

        if (info.SearchHeight >= Config.SearchHeightThreshold)
            QueueSearchHeight.Enqueue(info);

        if (info.RowDeletes >= Config.RowDeletesThreshold) {
            QueueRowDeletes.Enqueue(info);
        }

        return QueueLastCompaction.Enqueue(info);
    }

    bool Remove(const TShardCompactionInfo& info) {
        QueueSearchHeight.Remove(info);
        QueueRowDeletes.Remove(info);
        return QueueLastCompaction.Remove(info);
    }

    bool UpdateIfFound(const TShardCompactionInfo& info) {
        if (info.RowCount < Config.RowCountThreshold) {
            return Remove(info);
        }

        if (info.SearchHeight >= Config.SearchHeightThreshold) {
            QueueSearchHeight.UpdateIfFound(info);
        } else {
            QueueSearchHeight.Remove(info);
        }

        if (info.RowDeletes >= Config.RowDeletesThreshold) {
            QueueRowDeletes.UpdateIfFound(info);
        } else {
            QueueRowDeletes.Remove(info);
        }

        return QueueLastCompaction.UpdateIfFound(info);
    }

    void Clear() {
        QueueSearchHeight.Clear();
        QueueRowDeletes.Clear();
        QueueLastCompaction.Clear();
    }

    const TShardCompactionInfo& Front() const {
        switch (ActiveQueue) {
        case EActiveQueue::ByLastCompaction:
            return QueueLastCompaction.Front();
        case EActiveQueue::BySearchHeight:
            return QueueSearchHeight.Front();
        case EActiveQueue::ByRowDeletes:
            return QueueRowDeletes.Front();
        }
    }

    void PopFront() {
        const auto& front = Front();
        switch (ActiveQueue) {
        case EActiveQueue::ByLastCompaction: {
            QueueSearchHeight.Remove(front);
            QueueRowDeletes.Remove(front);
            QueueLastCompaction.PopFront();
            break;
        }
        case EActiveQueue::BySearchHeight: {
            QueueLastCompaction.Remove(front);
            QueueRowDeletes.Remove(front);
            QueueSearchHeight.PopFront();
            break;
        }
        case EActiveQueue::ByRowDeletes: {
            QueueLastCompaction.Remove(front);
            QueueSearchHeight.Remove(front);
            QueueRowDeletes.PopFront();
            break;
        }
        }

        switch (ActiveQueue) {
        case EActiveQueue::ByLastCompaction:
            if (!QueueSearchHeight.Empty()) {
                ActiveQueue = EActiveQueue::BySearchHeight;
                break;
            }
            [[fallthrough]];
        case EActiveQueue::BySearchHeight:
            if (!QueueRowDeletes.Empty()) {
                ActiveQueue = EActiveQueue::ByRowDeletes;
                break;
            }
            [[fallthrough]];
        case EActiveQueue::ByRowDeletes:
            ActiveQueue = EActiveQueue::ByLastCompaction;
        }
    }

    bool Empty() const {
        return QueueLastCompaction.Empty();
    }

    size_t Size() const {
        return QueueLastCompaction.Size();
    }

    size_t SizeBySearchHeight() const {
        return QueueSearchHeight.Size();
    }

    size_t SizeByRowDeletes() const {
        return QueueRowDeletes.Size();
    }

    TString DumpQueueFronts() const {
        TStringStream ss;
        ss << "LastCompaction: {";
        if (!QueueLastCompaction.Empty())
            ss << QueueLastCompaction.Front();
        ss << "}, SearchHeight: {";
        if (!QueueSearchHeight.Empty())
            ss << QueueSearchHeight.Front();
        ss << "}, RowDeletes: {";
        if (!QueueRowDeletes.Empty())
            ss << QueueRowDeletes.Front();
        ss << "}";
        return ss.Str();
    }
};

} // NSchemeShard
} // NKikimr

template<>
inline void Out<NKikimr::NSchemeShard::TShardCompactionInfo>(
    IOutputStream& o,
    const NKikimr::NSchemeShard::TShardCompactionInfo& info)
{
    o << info.ToString();
}
