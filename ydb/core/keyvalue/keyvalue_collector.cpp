#include "keyvalue_flat_impl.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/util/backoff.h>
#include <ydb/core/util/stlog.h>

namespace NKikimr {
namespace NKeyValue {

class TKeyValueCollector : public TActorBootstrapped<TKeyValueCollector> {
    TActorId KeyValueActorId;
    TIntrusivePtr<TCollectOperation> CollectOperation;
    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    ui32 RecordGeneration;
    ui32 PerGenerationCounter;
    std::set<TMonotonic> WakeupScheduled;

    using TCollectKey = std::tuple<ui32, ui8>; // groupId, channel
    struct TCollectInfo {
        TVector<TLogoBlobID> Keep;
        TVector<TLogoBlobID> DoNotKeep;
        ui32 TryCounter = 0;
        bool RequestInFlight = false;
        TMonotonic NextTryTimestamp;
        TBackoffTimer BackoffTimer{CollectorErrorInitialBackoffMs, CollectorErrorMaxBackoffMs};
    };
    THashMap<TCollectKey, TCollectInfo> Collects;

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
        STLOG(PRI_DEBUG, KEYVALUE_GC, KVC04, "Start KeyValueCollector", (TabletId, TabletInfo->TabletID));

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

        Action();
        Become(&TThis::StateWait);
    }

    void Action() {
        const TMonotonic now = TActivationContext::Monotonic();
        TMonotonic nextTryTimestamp = TMonotonic::Max();
        for (auto& [key, value] : Collects) {
            if (value.RequestInFlight) {
                continue;
            } else if (now < value.NextTryTimestamp) { // time hasn't come yet
                nextTryTimestamp = Min(nextTryTimestamp, value.NextTryTimestamp);
                continue;
            }

            const auto [groupId, channel] = key;
            const bool advanceBarrier = CollectOperation->AdvanceBarrier;
            auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(TabletInfo->TabletID, RecordGeneration,
                PerGenerationCounter, channel, advanceBarrier, CollectOperation->Header.CollectGeneration,
                CollectOperation->Header.CollectStep, value.Keep ? new TVector<TLogoBlobID>(value.Keep) : nullptr,
                value.DoNotKeep ? new TVector<TLogoBlobID>(value.DoNotKeep) : nullptr, TInstant::Max(), true);
            STLOG(PRI_DEBUG, KEYVALUE_GC, KVC00, "Sending TEvCollectGarbage", (TabletId, TabletInfo->TabletID),
                (GroupId, groupId), (Channel, (int)channel), (RecordGeneration, RecordGeneration),
                (PerGenerationCounter, PerGenerationCounter), (AdvanceBarrier, advanceBarrier),
                (CollectGeneration, CollectOperation->Header.CollectGeneration),
                (CollectStep, CollectOperation->Header.CollectStep), (Keep.size, value.Keep.size()),
                (DoNotKeep.size, value.DoNotKeep.size()));
            SendToBSProxy(SelfId(), groupId, ev.release(), static_cast<ui64>(groupId) << 8 | channel);
            value.RequestInFlight = true;
        }
        if (nextTryTimestamp != TMonotonic::Max() && (WakeupScheduled.empty() || nextTryTimestamp < *WakeupScheduled.begin())) {
            TActivationContext::Schedule(nextTryTimestamp, new IEventHandle(TEvents::TSystem::Wakeup, 0, SelfId(), {},
                nullptr, nextTryTimestamp.GetValue()));
            WakeupScheduled.insert(nextTryTimestamp);
        }
        if (Collects.empty()) {
            SendCompleteGCAndDie();
        }
    }

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr &ev) {
        NKikimrProto::EReplyStatus status = ev->Get()->Status;

        const TCollectKey key(ev->Cookie >> 8, static_cast<ui8>(ev->Cookie));

        STLOG(PRI_DEBUG, KEYVALUE_GC, KVC11, "Receive TEvCollectGarbageResult",
            (TabletId, TabletInfo->TabletID), (GroupId, std::get<0>(key)), (Channel, (int)std::get<1>(key)),
            (Status, status));

        const auto it = Collects.find(key);
        Y_ABORT_UNLESS(it != Collects.end());
        TCollectInfo& info = it->second;
        Y_ABORT_UNLESS(info.RequestInFlight);
        info.RequestInFlight = false;

        if (status == NKikimrProto::OK) {
            Collects.erase(it);
        } else if (++info.TryCounter < CollectorMaxErrors) {
            info.NextTryTimestamp = TActivationContext::Monotonic() + TDuration::MilliSeconds(info.BackoffTimer.NextBackoffMs());
        } else {
            return HandleErrorAndDie();
        }

        Action();
    }

    void SendCompleteGCAndDie() {
        STLOG(PRI_DEBUG, KEYVALUE_GC, KVC19, "Collector send CompleteGC", (TabletId, TabletInfo->TabletID));
        Send(KeyValueActorId, new TEvKeyValue::TEvCompleteGC(false));
        PassAway();
    }

    void HandleErrorAndDie() {
        STLOG(PRI_ERROR, KEYVALUE_GC, KVC18, "Garbage Collector catch the error, send PoisonPill to the tablet",
            (TabletId, TabletInfo->TabletID));
        Send(KeyValueActorId, new TEvents::TEvPoisonPill());
        PassAway();
    }

    void HandleWakeup(STATEFN_SIG) {
        const size_t numErased = WakeupScheduled.erase(TMonotonic::FromValue(ev->Cookie));
        Y_ABORT_UNLESS(numErased == 1);
        Action();
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
