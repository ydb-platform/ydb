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
    void Handle(TEvWriteSessionsQuoter::TEvReleaseQuota::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvWriteSessionsQuoter::TEvRemove::TPtr& ev, const TActorContext& ctx);
    void PassAway() override;

private:
    STFUNC(StateWork);

    void ProcessHolders(std::list<TActorId>& holders, const TKey& key, const TActorContext& ctx);
    void AddPending(const TKey& key, const TActorId& actorId);
    void RemoveFrontPending(const TKey& key);
    ui64 GetMaxConcurrentInitializations(const TActorContext& ctx);
    void AddHolder(const TKey& key, const TActorId& actorId);
    void RemoveHolder(const TActorId& actorId);

    struct TIndexItem {
        TKey Key;
        std::list<TActorId>::iterator Iterator;
    };

    absl::flat_hash_map<TKey, std::list<TActorId>> Holders;
    absl::flat_hash_map<TActorId, TIndexItem> HoldersIndex;
    absl::flat_hash_map<TKey, std::list<TActorId>> Pending;
    absl::flat_hash_map<TActorId, TIndexItem> PendingIndex;
};

void TWriteSessionsQuoter::Bootstrap(const TActorContext&) {
    Become(&TWriteSessionsQuoter::StateWork);
}

ui64 TWriteSessionsQuoter::GetMaxConcurrentInitializations(const TActorContext& ctx) {
    return AppData(ctx)->PQConfig.GetMaxConcurrentWriteSessionInitializations();
}

void TWriteSessionsQuoter::Handle(TEvWriteSessionsQuoter::TEvNotify::TPtr& ev, const TActorContext&) {
    const auto& msg = *ev->Get();

    Holders.emplace(
        TKey{
            .Topic = msg.Topic,
            .Partition = msg.Partition,
            .Generation = msg.Generation
        },
        std::list<TActorId>()
    );
}

void TWriteSessionsQuoter::Handle(TEvWriteSessionsQuoter::TEvAcquireQuota::TPtr& ev, const TActorContext& ctx) {
    const auto& msg = *ev->Get();
    const auto key = TKey{
        .Topic = msg.Topic,
        .Partition = msg.Partition,
        .Generation = msg.Generation
    };

    if (!Holders.contains(key)) {
        LOG_W("Received TEvAcquireQuota for unknown topic-partition-generation");

        ctx.Send(ev->Sender, new TEvWriteSessionsQuoter::TEvQuotaDeclined());
        return;
    }

    // Each session can have at most one outstanding quota request.
    if (HoldersIndex.contains(ev->Sender) || PendingIndex.contains(ev->Sender)) {
        return;
    }

    auto& holders = Holders[key];
    ProcessHolders(holders, key, ctx);

    if (holders.size() >= GetMaxConcurrentInitializations(ctx)) {
        auto pending = Pending.find(key);
        if (pending != Pending.end() && pending->second.size() >= MAX_PENDING_REQUESTS) {
            LOG_W("Max pending requests reached for topic-partition-generation");
            ctx.Send(ev->Sender, new TEvWriteSessionsQuoter::TEvQuotaDeclined());
            return;
        }

        AddPending(key, ev->Sender);
    } else {
        AddHolder(key, ev->Sender);
        ctx.Send(ev->Sender, new TEvWriteSessionsQuoter::TEvQuotaAcquired());
    }
}

void TWriteSessionsQuoter::Handle(TEvWriteSessionsQuoter::TEvReleaseQuota::TPtr& ev, const TActorContext& ctx) {
    const auto& msg = *ev->Get();
    const auto key = TKey{msg.Topic, msg.Partition, msg.Generation};

    if (auto pending = PendingIndex.find(ev->Sender); pending != PendingIndex.end()) {
        if (pending->second.Key != key) {
            return;
        }

        auto queue = Pending.find(key);
        queue->second.erase(pending->second.Iterator);
        PendingIndex.erase(pending);
        if (queue->second.empty()) {
            Pending.erase(queue);
        }
        return;
    }

    auto holder = HoldersIndex.find(ev->Sender);
    if (holder == HoldersIndex.end() || holder->second.Key != key) {
        // Repeated releases and releases for removed generations are harmless.
        return;
    }

    RemoveHolder(ev->Sender);
    ProcessHolders(Holders[key], key, ctx);
}

void TWriteSessionsQuoter::Handle(TEvWriteSessionsQuoter::TEvRemove::TPtr& ev, const TActorContext& ctx) {
    const auto& msg = *ev->Get();
    const auto key = TKey{msg.Topic, msg.Partition, msg.Generation};

    if (auto holders = Holders.find(key); holders != Holders.end()) {
        for (const auto& actorId : holders->second) {
            HoldersIndex.erase(actorId);
        }
        Holders.erase(holders);
    }

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

void TWriteSessionsQuoter::ProcessHolders(std::list<TActorId>& holders, const TKey& key, const TActorContext& ctx) {
    auto pending = Pending.find(key);
    if (pending == Pending.end()) {
        return;
    }

    const auto limit = GetMaxConcurrentInitializations(ctx);
    while (!pending->second.empty() && holders.size() < limit) {
        const auto actorId = pending->second.front();
        RemoveFrontPending(key);
        AddHolder(key, actorId);
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

void TWriteSessionsQuoter::AddHolder(const TKey& key, const TActorId& actorId) {
    auto& holders = Holders[key];
    holders.push_back(actorId);
    HoldersIndex.emplace(actorId, TIndexItem{key, std::prev(holders.end())});
}

void TWriteSessionsQuoter::RemoveHolder(const TActorId& actorId) {
    auto holder = HoldersIndex.find(actorId);
    Holders[holder->second.Key].erase(holder->second.Iterator);
    HoldersIndex.erase(holder);
}

STFUNC(TWriteSessionsQuoter::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvWriteSessionsQuoter::TEvNotify, Handle);
        HFunc(TEvWriteSessionsQuoter::TEvAcquireQuota, Handle);
        HFunc(TEvWriteSessionsQuoter::TEvReleaseQuota, Handle);
        HFunc(TEvWriteSessionsQuoter::TEvRemove, Handle);
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
