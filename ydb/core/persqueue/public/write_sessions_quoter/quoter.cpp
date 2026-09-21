#include "quoter.h"

#include <queue>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/util/counted_leaky_bucket.h>
#include <ydb/core/persqueue/events/internal.h>

namespace NKikimr::NPQ {

namespace {

static constexpr ui64 QUOTA_WINDOW_MS = 1000;

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

class TWriteSessionsQuoter : public NActors::TActorBootstrapped<TWriteSessionsQuoter> {
public:
    TWriteSessionsQuoter() = default;
    ~TWriteSessionsQuoter() = default;

    void Bootstrap(const TActorContext& ctx);

    void Handle(TEvWriteSessionsQuoter::TEvNotify::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvWriteSessionsQuoter::TEvAcquireQuota::TPtr& ev, const TActorContext& ctx);
    void Wakeup(NActors::TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);

private:
    STFUNC(StateWork);

    absl::flat_hash_map<TKey, TCountedLeakyBucket> Buckets;
    absl::flat_hash_map<TKey, std::queue<TActorId>> Pending;
};

void TWriteSessionsQuoter::Bootstrap(const TActorContext& ctx) {
    Become(&TWriteSessionsQuoter::StateWork);
    ctx.Schedule(TDuration::MilliSeconds(QUOTA_WINDOW_MS), new NActors::TEvents::TEvWakeup());
}

void TWriteSessionsQuoter::Handle(TEvWriteSessionsQuoter::TEvNotify::TPtr& ev, const TActorContext& ctx) {
    const auto initSessionsRps = AppData(ctx)->PQConfig.GetWriteSessionsInitRps();
    const auto& msg = *ev->Get();

    Buckets.emplace(
        TKey{
            .Topic = msg.Topic,
            .Partition = msg.Partition,
            .Generation = msg.Generation
        },
        TCountedLeakyBucket(
            initSessionsRps,
            TDuration::MilliSeconds(QUOTA_WINDOW_MS),
            ctx.Now())
    );

    ctx.Send(ev->Sender, new TEvWriteSessionsQuoter::TEvQuoterInitialized());
}

void TWriteSessionsQuoter::Handle(TEvWriteSessionsQuoter::TEvAcquireQuota::TPtr& ev, const TActorContext& ctx) {
    const auto& msg = *ev->Get();
    const auto key = TKey{
        .Topic = msg.Topic,
        .Partition = msg.Partition,
        .Generation = msg.Generation
    };

    AFL_ENSURE(Buckets.contains(key)); // спорно, надо проверить, что это так.

    auto& pending = Pending[key];
    auto& bucket = Buckets[key];
    while (!pending.empty() && bucket.TryPush(ctx.Now(), 1)) {
        auto actorId = pending.front();
        pending.pop();
        ctx.Send(actorId, new TEvWriteSessionsQuoter::TEvQuotaAcquired());
    }
   
    if (!bucket.TryPush(ctx.Now(), 1)) {
        pending.push(ev->Sender);
    } else {
        ctx.Send(ev->Sender, new TEvWriteSessionsQuoter::TEvQuotaAcquired());
    }
}

void TWriteSessionsQuoter::Wakeup(NActors::TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    for (auto iter = Pending.begin(); iter != Pending.end();) {
        auto& bucket = Buckets[iter->first];
        auto& pending = iter->second;
        bool shouldStop = false;
        while (!pending.empty()) {
            auto pushed = bucket.TryPush(ctx.Now(), 1);
            if (!pushed) {
                shouldStop = true;
                break;
            }
        
            auto actorId = pending.front();
            pending.pop();
            ctx.Send(actorId, new TEvWriteSessionsQuoter::TEvQuotaAcquired());
        }

        if (shouldStop) {
            break;
        }

        Pending.erase(iter++);
    }

    ctx.Schedule(TDuration::MilliSeconds(QUOTA_WINDOW_MS), new NActors::TEvents::TEvWakeup());
}

STFUNC(TWriteSessionsQuoter::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvWriteSessionsQuoter::TEvNotify, Handle);
        HFunc(TEvWriteSessionsQuoter::TEvAcquireQuota, Handle);
        HFunc(NActors::TEvents::TEvWakeup, Wakeup);
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
