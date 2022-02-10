#pragma once 
 
#include "schemeshard_identificators.h" 
 
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/util/operation_queue.h>
 
// TODO: this is a good candidate for core/util, but since 
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
    { } 
 
    void Shutdown(const TActorContext &ctx) { 
        if (LongTimerId) 
            ctx.Send(LongTimerId, new TEvents::TEvPoison); 
 
        TActorBase::PassAway(); 
    } 
 
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
 
    explicit TShardCompactionInfo(const TShardIdx& id) 
        : ShardIdx(id) 
    {} 
 
    TShardCompactionInfo(const TShardIdx& id, ui64 searchHeight) 
        : ShardIdx(id) 
        , SearchHeight(searchHeight) 
    {} 
 
    TShardCompactionInfo(const TShardCompactionInfo&) = default;

    bool operator ==(const TShardCompactionInfo& rhs) const { 
        // note that only identity intentionally checked 
        return ShardIdx == rhs.ShardIdx; 
    } 
 
    TShardCompactionInfo& operator =(const TShardCompactionInfo& rhs) { 
        // TODO: assert that ID's are the same, because we 
        // use it as update rather than real assignment 
        SearchHeight = rhs.SearchHeight; 
        return *this; 
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
 
    TString ToString() const { 
        TStringStream ss; 
        ss << "{" << ShardIdx << "," << SearchHeight << "}"; 
        return ss.Str(); 
    } 
}; 
 
} // NSchemeShard 
} // NKikimr 
