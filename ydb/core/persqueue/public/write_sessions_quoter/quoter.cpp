#include "quoter.h"

#include <iterator>
#include <list>
#include <utility>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/persqueue/common/logging.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/util/counted_leaky_bucket.h>

namespace NKikimr::NPQ {

namespace {

static constexpr ui64 MAX_PENDING_REQUESTS = 1'000'000;

struct TKey {
    TString Topic;
    ui32 Partition;
    ui32 Generation;

    bool operator==(const TKey& other) const = default;

    template <typename H>
    friend H AbslHashValue(H h, const TKey& key) {
        return H::combine(std::move(h), key.Topic, key.Partition, key.Generation);
    }
};

class TWriteSessionsQuoter : public NActors::TActorBootstrapped<TWriteSessionsQuoter>
                           , public TLogPrefix {
public:
    TWriteSessionsQuoter()
        : TLogPrefix(NKikimrServices::PQ_RATE_LIMITER)
    {
    }

    void Bootstrap(const TActorContext& ctx);

    TStructuredMessage LogPrefix() const override {
        return {};
    }

    void Handle(TEvWriteSessionsQuoter::TEvNotify::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvWriteSessionsQuoter::TEvAcquireQuota::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvWriteSessionsQuoter::TEvRemove::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);
    void PassAway() override;

private:
    STFUNC(StateWork);

    void ProcessPending(TCountedLeakyBucket& bucket, const TKey& key, const TActorContext& ctx);
    void AddPending(const TKey& key, const TActorId& actorId);
    void RemoveFrontPending(const TKey& key);
    ui64 GetWriteSessionsInitRps(const TActorContext& ctx);

    struct TIndexItem {
        TKey Key;
        std::list<TActorId>::iterator Iterator;
    };

    struct TPendingItem {
        TActorId ActorId;
        TInstant StartPending;
    };

    absl::flat_hash_map<TKey, TCountedLeakyBucket> Buckets;
    absl::flat_hash_map<TKey, std::list<TActorId>> Pending;
    absl::flat_hash_map<TActorId, TIndexItem> PendingIndex;
};

void TWriteSessionsQuoter::Bootstrap(const TActorContext& ctx) {
    Become(&TWriteSessionsQuoter::StateWork);
    ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
}

void TWriteSessionsQuoter::Handle(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    const auto now = ctx.Now();
    for (auto& [key, bucket] : Buckets) {
        bucket.Update(now);
        ProcessPending(bucket, key, ctx);
    }
    ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
}

ui64 TWriteSessionsQuoter::GetWriteSessionsInitRps(const TActorContext& ctx) {
    return AppData(ctx)->PQConfig.GetWriteSessionsInitRps();
}

void TWriteSessionsQuoter::Handle(TEvWriteSessionsQuoter::TEvNotify::TPtr& ev, const TActorContext& ctx) {
    const auto& msg = *ev->Get();

    Buckets.try_emplace(
        TKey{
            .Topic = msg.Topic,
            .Partition = msg.Partition,
            .Generation = msg.Generation
        },
        GetWriteSessionsInitRps(ctx), TDuration::Seconds(1), ctx.Now()
    );
}

void TWriteSessionsQuoter::Handle(TEvWriteSessionsQuoter::TEvAcquireQuota::TPtr& ev, const TActorContext& ctx) {
    const auto& msg = *ev->Get();
    const auto key = TKey{
        .Topic = msg.Topic,
        .Partition = msg.Partition,
        .Generation = msg.Generation
    };

    auto bucket = Buckets.find(key);
    if (bucket == Buckets.end()) {
        LOG_W("Received TEvAcquireQuota for unknown topic-partition-generation");

        ctx.Send(ev->Sender, new TEvWriteSessionsQuoter::TEvQuotaDeclined());
        return;
    }

    // Each session can have at most one outstanding quota request.
    if (PendingIndex.contains(ev->Sender)) {
        return;
    }

    ProcessPending(bucket->second, key, ctx);

    if (!bucket->second.TryPush(ctx.Now(), 1)) {
        auto pending = Pending.find(key);
        if (pending != Pending.end() && pending->second.size() >= MAX_PENDING_REQUESTS) {
            LOG_W("Max pending requests reached for topic-partition-generation");
            ctx.Send(ev->Sender, new TEvWriteSessionsQuoter::TEvQuotaDeclined());
            return;
        }

        AddPending(key, ev->Sender);
    } else {
        ctx.Send(ev->Sender, new TEvWriteSessionsQuoter::TEvQuotaAcquired());
    }
}

void TWriteSessionsQuoter::Handle(TEvWriteSessionsQuoter::TEvRemove::TPtr& ev, const TActorContext& ctx) {
    const auto& msg = *ev->Get();
    const auto key = TKey{msg.Topic, msg.Partition, msg.Generation};

    Buckets.erase(key);

    if (auto pending = Pending.find(key); pending != Pending.end()) {
        for (const auto& actorId : pending->second) {
            PendingIndex.erase(actorId);
            ctx.Send(actorId, new TEvWriteSessionsQuoter::TEvQuotaDeclined());
        }
        Pending.erase(pending);
    }
}

void TWriteSessionsQuoter::PassAway() {
    for (auto& [_, pending] : Pending) {
        for (const auto& actorId : pending) {
            Send(actorId, new TEvWriteSessionsQuoter::TEvQuotaDeclined());
        }
    }
    NActors::TActorBootstrapped<TWriteSessionsQuoter>::PassAway();
}

void TWriteSessionsQuoter::ProcessPending(TCountedLeakyBucket& bucket, const TKey& key, const TActorContext& ctx) {
    auto pending = Pending.find(key);
    if (pending == Pending.end()) {
        return;
    }

    const auto now = ctx.Now();
    while (!pending->second.empty() && bucket.TryPush(now, 1)) {
        const auto actorId = pending->second.front();
        RemoveFrontPending(key);
        ctx.Send(actorId, new TEvWriteSessionsQuoter::TEvQuotaAcquired());
    }

    if (pending->second.empty()) {
        Pending.erase(pending);
    }
}

void TWriteSessionsQuoter::AddPending(const TKey& key, const TActorId& actorId) {
    auto& pending = Pending[key];
    pending.push_back(actorId);
    PendingIndex.emplace(actorId, TIndexItem{key, std::prev(pending.end())});
}

void TWriteSessionsQuoter::RemoveFrontPending(const TKey& key) {
    auto& pending = Pending[key];
    PendingIndex.erase(pending.front());
    pending.pop_front();
}

STFUNC(TWriteSessionsQuoter::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvWriteSessionsQuoter::TEvNotify, Handle);
        HFunc(TEvWriteSessionsQuoter::TEvAcquireQuota, Handle);
        HFunc(TEvWriteSessionsQuoter::TEvRemove, Handle);
        HFunc(TEvents::TEvWakeup, Handle);
        sFunc(NActors::TEvents::TEvPoison, PassAway);
    }
}

} // namespace

NActors::TActorId MakeWriteSessionsQuoterId() {
    return NActors::TActorId(0, "pq_wr_quota");
}

NActors::IActor* CreateWriteSessionsQuoter() {
    return new TWriteSessionsQuoter();
}

} // namespace NKikimr::NPQ
