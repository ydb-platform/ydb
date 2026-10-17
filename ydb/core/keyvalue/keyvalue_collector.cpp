#include "keyvalue_flat_impl.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/util/backoff.h>
#include <ydb/core/util/stlog.h>

#define YDB_LOG_THIS_FILE_COMPONENT KEYVALUE_GC

namespace NKikimr {
namespace NKeyValue {

class TKeyValueCollector : public TActorBootstrapped<TKeyValueCollector> {
    TActorId KeyValueActorId;
    TIntrusivePtr<TCollectOperation> CollectOperation;
    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    ui32 RecordGeneration;
    ui32 PerGenerationCounter;

    using TCollectKey = std::tuple<ui32, ui8>; // groupId, channel
    struct TChunk {
        ui64 RequestCookie = 0; // zero when no attempt is in flight
        ui32 TryCounter = 0;
        TBackoffTimer BackoffTimer{CollectorErrorInitialBackoffMs, CollectorErrorMaxBackoffMs};
    };
    struct TCollectInfo {
        TVector<TLogoBlobID> Keep;
        TVector<TLogoBlobID> DoNotKeep;
        // Indexed by immutable chunk ID; entries stay in place after acknowledgment.
        TVector<TChunk> Chunks;
        size_t ChunksRemaining = 0;
    };
    THashMap<TCollectKey, TCollectInfo> Collects;
    struct TRequestInfo {
        TCollectKey Key;
        size_t ChunkIndex;
    };
    THashMap<ui64, TRequestInfo> Requests;
    ui64 LastRequestCookie = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KEYVALUE_ACTOR;
    }

    TKeyValueCollector(const TActorId &keyValueActorId, TIntrusivePtr<TCollectOperation> &collectOperation,
            const TTabletStorageInfo *tabletInfo, ui32 recordGeneration, ui32 perGenerationCounter)
        : KeyValueActorId(keyValueActorId)
        , CollectOperation(collectOperation)
        , TabletInfo(const_cast<TTabletStorageInfo*>(tabletInfo))
        , RecordGeneration(recordGeneration)
        , PerGenerationCounter(perGenerationCounter)
    {
        Y_ABORT_UNLESS(CollectOperation.Get());
    }

    void Bootstrap() {
        YDB_LOG_DEBUG("Start KeyValueCollector",
            {"marker", "KVC04"},
            {"tabletId", TabletInfo->TabletID});

        // prepare keep/doNotKeep flags
        auto push = [&](const TLogoBlobID& id, auto flagsMember) {
            const ui32 groupId = TabletInfo->GroupFor(id);
            const TCollectKey key(groupId, id.Channel());
            auto& info = Collects[key];
            auto& v = info.*flagsMember;
            v.push_back(id);
        };
        for (const auto& id : CollectOperation->Keep) {
            push(id, &TCollectInfo::Keep);
        }
        for (const auto& id : CollectOperation->DoNotKeep) {
            push(id, &TCollectInfo::DoNotKeep);
        }
        Y_ABORT_UNLESS(!CollectOperation->Keep || CollectOperation->AdvanceBarrier);

        // fill in required channel/group pairs
        if (CollectOperation->AdvanceBarrier) {
            for (const auto& channel : TabletInfo->Channels) {
                if (channel.Channel < BLOB_CHANNEL) { // skip system channels
                    continue;
                }
                if (!channel.History.empty()) {
                    const auto& history = channel.History.back();
                    Collects.try_emplace(TCollectKey(history.GroupID, channel.Channel));
                }
            }
        }

        if (Collects.empty()) {
            return SendCompleteGCAndDie();
        }
        for (auto& [key, info] : Collects) {
            const size_t chunkCount = Max<size_t>(1,
                (info.Keep.size() + info.DoNotKeep.size() + CollectorMaxFlagsPerMessage - 1) /
                    CollectorMaxFlagsPerMessage);
            info.Chunks.resize(chunkCount);
            info.ChunksRemaining = chunkCount;
            // Send all flag chunks concurrently, reserving the final chunk for the barrier.
            const size_t initialChunks = chunkCount - (CollectOperation->AdvanceBarrier && chunkCount > 1);
            for (size_t chunkIndex = 0; chunkIndex < initialChunks; ++chunkIndex) {
                SendChunk(key, info, chunkIndex);
            }
        }
        Become(&TThis::StateWait);
    }

    static TVector<TLogoBlobID>* MakeChunk(const TVector<TLogoBlobID>& flags, size_t offset, size_t count) {
        return count ? new TVector<TLogoBlobID>(flags.begin() + offset, flags.begin() + offset + count) : nullptr;
    }

    void SendChunk(const TCollectKey& key, TCollectInfo& info, size_t chunkIndex) {
        TChunk& chunk = info.Chunks[chunkIndex];
        const auto [groupId, channel] = key;
        const size_t offset = chunkIndex * CollectorMaxFlagsPerMessage;
        const size_t keepOffset = Min(offset, info.Keep.size());
        const size_t keepCount = Min<size_t>(info.Keep.size() - keepOffset, CollectorMaxFlagsPerMessage);
        const size_t doNotKeepOffset = offset - keepOffset;
        const size_t doNotKeepCount = Min<size_t>(info.DoNotKeep.size() - doNotKeepOffset,
            CollectorMaxFlagsPerMessage - keepCount);
        const bool advanceBarrier = CollectOperation->AdvanceBarrier && chunkIndex + 1 == info.Chunks.size();
        Y_ABORT_UNLESS(!advanceBarrier || info.ChunksRemaining == 1);
        Y_ABORT_UNLESS(!chunk.RequestCookie);
        // Each attempt has its own cookie: replies can arrive out of order, including late replies to retries.
        chunk.RequestCookie = ++LastRequestCookie;
        Y_ABORT_UNLESS(chunk.RequestCookie);
        Requests.emplace(chunk.RequestCookie, TRequestInfo{key, chunkIndex});

        // Every chunk carries the same PerGenerationCounter: only the barrier-bearing command is sequence-checked.
        auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(TabletInfo->TabletID, RecordGeneration,
            PerGenerationCounter, channel, advanceBarrier, CollectOperation->Header.CollectGeneration,
            CollectOperation->Header.CollectStep, MakeChunk(info.Keep, keepOffset, keepCount),
            MakeChunk(info.DoNotKeep, doNotKeepOffset, doNotKeepCount), TInstant::Max(), true,
            TWriteSource::KeyValueGC);
        YDB_LOG_DEBUG("Sending TEvCollectGarbage",
            {"marker", "KVC00"},
            {"tabletId", TabletInfo->TabletID},
            {"groupId", groupId},
            {"channel", static_cast<int>(channel)},
            {"recordGeneration", RecordGeneration},
            {"perGenerationCounter", PerGenerationCounter},
            {"advanceBarrier", advanceBarrier},
            {"collectGeneration", CollectOperation->Header.CollectGeneration},
            {"collectStep", CollectOperation->Header.CollectStep},
            {"chunkIndex", chunkIndex},
            {"requestCookie", chunk.RequestCookie},
            {"keepSize", keepCount},
            {"doNotKeepSize", doNotKeepCount},
            {"keepLeft", info.Keep.size() - keepOffset - keepCount},
            {"doNotKeepLeft", info.DoNotKeep.size() - doNotKeepOffset - doNotKeepCount});
        SendToBSProxy(SelfId(), groupId, ev.release(), chunk.RequestCookie);
    }

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr &ev) {
        const auto requestIt = Requests.find(ev->Cookie);
        if (requestIt == Requests.end()) {
            // A completed attempt cannot acknowledge another chunk or a newer retry.
            return;
        }
        const TRequestInfo request = requestIt->second;
        const auto it = Collects.find(request.Key);
        Y_ABORT_UNLESS(it != Collects.end());
        TCollectInfo& info = it->second;
        Y_ABORT_UNLESS(request.ChunkIndex < info.Chunks.size());
        TChunk& chunk = info.Chunks[request.ChunkIndex];
        if (!chunk.RequestCookie) {
            // The failed attempt's mapping belongs to its retry timer. Ignore duplicate replies.
            return;
        }
        Y_ABORT_UNLESS(chunk.RequestCookie == ev->Cookie);
        chunk.RequestCookie = 0;
        const NKikimrProto::EReplyStatus status = ev->Get()->Status;

        YDB_LOG_DEBUG("Receive TEvCollectGarbageResult",
            {"marker", "KVC11"},
            {"tabletId", TabletInfo->TabletID},
            {"groupId", std::get<0>(request.Key)},
            {"channel", static_cast<int>(std::get<1>(request.Key))},
            {"chunkIndex", request.ChunkIndex},
            {"requestCookie", ev->Cookie},
            {"status", status});

        if (status == NKikimrProto::OK) {
            Requests.erase(requestIt);
            Y_ABORT_UNLESS(info.ChunksRemaining);
            if (--info.ChunksRemaining == 0) {
                Collects.erase(it);
                if (Collects.empty()) {
                    SendCompleteGCAndDie();
                }
            } else if (CollectOperation->AdvanceBarrier && info.ChunksRemaining == 1) {
                SendChunk(request.Key, info, info.Chunks.size() - 1);
            }
        } else if (++chunk.TryCounter < CollectorMaxErrors) {
            // Keep the mapping until this chunk's timer fires, then assign the retry a fresh cookie.
            TActivationContext::Schedule(TActivationContext::Monotonic() + chunk.BackoffTimer.Next(),
                new IEventHandle(TEvents::TSystem::Wakeup, 0, SelfId(), {}, nullptr, ev->Cookie));
        } else {
            HandleErrorAndDie();
        }
    }

    void SendCompleteGCAndDie() {
        YDB_LOG_DEBUG("Collector send CompleteGC",
            {"marker", "KVC19"},
            {"tabletId", TabletInfo->TabletID});
        Send(KeyValueActorId, new TEvKeyValue::TEvCompleteGC(false));
        PassAway();
    }

    void HandleErrorAndDie() {
        YDB_LOG_ERROR("Garbage Collector catch the error, send PoisonPill to the tablet",
            {"marker", "KVC18"},
            {"tabletId", TabletInfo->TabletID});
        Send(KeyValueActorId, new TEvents::TEvPoisonPill());
        PassAway();
    }

    void HandleWakeup(STATEFN_SIG) {
        const auto requestIt = Requests.find(ev->Cookie);
        Y_ABORT_UNLESS(requestIt != Requests.end());
        const TRequestInfo request = requestIt->second;
        Requests.erase(requestIt);
        const auto it = Collects.find(request.Key);
        Y_ABORT_UNLESS(it != Collects.end());
        SendChunk(request.Key, it->second, request.ChunkIndex);
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            fFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            cFunc(TEvents::TSystem::Poison, PassAway);
            default:
                break;
        }
    }
};

IActor* CreateKeyValueCollector(const TActorId &keyValueActorId, TIntrusivePtr<TCollectOperation> &collectOperation,
        const TTabletStorageInfo *TabletInfo, ui32 recordGeneration, ui32 perGenerationCounter) {
    return new TKeyValueCollector(keyValueActorId, collectOperation, TabletInfo, recordGeneration, perGenerationCounter);
}

} // NKeyValue
} // NKikimr
