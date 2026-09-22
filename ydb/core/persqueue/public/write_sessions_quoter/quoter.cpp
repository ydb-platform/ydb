#include "quoter.h"

#include <deque>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/persqueue/common/logging.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/util/counted_leaky_bucket.h>
#include <ydb/core/persqueue/events/internal.h>

namespace NKikimr::NPQ {

namespace {

static constexpr ui64 QUOTA_WINDOW_MS = 1000;
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
    void Wakeup(NActors::TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);
    void PassAway() override;

private:
    STFUNC(StateWork);

    bool ProcessBucket(TCountedLeakyBucket& bucket, std::deque<TActorId>& pending, const TActorContext& ctx);

    absl::flat_hash_map<TKey, TCountedLeakyBucket> Buckets;
    absl::flat_hash_map<TKey, std::deque<TActorId>> Pending;
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
}

void TWriteSessionsQuoter::Handle(TEvWriteSessionsQuoter::TEvAcquireQuota::TPtr& ev, const TActorContext& ctx) {
    const auto& msg = *ev->Get();
    const auto key = TKey{
        .Topic = msg.Topic,
        .Partition = msg.Partition,
        .Generation = msg.Generation
    };

    if (!Buckets.contains(key)) {
        LOG_W("Received TEvAcquireQuota for unknown topic-partition-generation");

        ctx.Send(ev->Sender, new TEvWriteSessionsQuoter::TEvQuotaDeclined());
        return;
    }

    auto& pending = Pending[key];
    auto& bucket = Buckets[key];

    ProcessBucket(bucket, pending, ctx);
   
    if (!bucket.TryPush(ctx.Now(), 1)) {
        if (pending.size() >= MAX_PENDING_REQUESTS) {
            LOG_W("Max pending requests reached for topic-partition-generation");

            ctx.Send(ev->Sender, new TEvWriteSessionsQuoter::TEvQuotaDeclined());
            return;
        }

        pending.push_back(ev->Sender);
    } else {
        ctx.Send(ev->Sender, new TEvWriteSessionsQuoter::TEvQuotaAcquired());
    }

    if (pending.empty()) {
        Pending.erase(key);
    }
}

void TWriteSessionsQuoter::Handle(TEvWriteSessionsQuoter::TEvRemove::TPtr& ev, const TActorContext& ctx) {
    const auto& msg = *ev->Get();
    const auto key = TKey{msg.Topic, msg.Partition, msg.Generation};
    Buckets.erase(key);

    for (auto& actorId : Pending[key]) {
        ctx.Send(actorId, new TEvWriteSessionsQuoter::TEvQuotaDeclined());
    }

    Pending.erase(key);
}

void TWriteSessionsQuoter::Wakeup(NActors::TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    for (auto iter = Pending.begin(); iter != Pending.end();) {
        auto& bucket = Buckets[iter->first];
        bucket.Update(ctx.Now());
        auto& pending = iter->second;
        if (!ProcessBucket(bucket, pending, ctx)) {
            iter++;
            continue;
        }

        Pending.erase(iter++);
    }

    ctx.Schedule(TDuration::MilliSeconds(QUOTA_WINDOW_MS), new NActors::TEvents::TEvWakeup());
}

void TWriteSessionsQuoter::PassAway() {
    for (auto& [_, pending] : Pending) {
        for (const auto& actorId : pending) {
            Send(actorId, new TEvWriteSessionsQuoter::TEvQuotaDeclined());
        }
    }
    NActors::TActorBootstrapped<TWriteSessionsQuoter>::PassAway();
}

bool TWriteSessionsQuoter::ProcessBucket(TCountedLeakyBucket& bucket, std::deque<TActorId>& pending, const TActorContext& ctx) {
    bucket.Update(ctx.Now());
    while (!pending.empty()) {
        if (!bucket.TryPush(ctx.Now(), 1)) {
            return false;
        }

        auto actorId = pending.front();
        pending.pop_front();
        ctx.Send(actorId, new TEvWriteSessionsQuoter::TEvQuotaAcquired());
    }

    return true;
}

STFUNC(TWriteSessionsQuoter::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvWriteSessionsQuoter::TEvNotify, Handle);
        HFunc(TEvWriteSessionsQuoter::TEvAcquireQuota, Handle);
        HFunc(TEvWriteSessionsQuoter::TEvRemove, Handle);
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
