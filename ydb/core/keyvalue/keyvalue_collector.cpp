#include "keyvalue_flat_impl.h"

#include <ydb/core/blobstorage/dsproxy/blobstorage_backoff.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr {
namespace NKeyValue {
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Collector
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TGroupCollector {
    TDeque<TLogoBlobID> Keep;
    TDeque<TLogoBlobID> DoNotKeep;
};

class TKeyValueCollector : public TActorBootstrapped<TKeyValueCollector> {
    TActorId KeyValueActorId;
    TIntrusivePtr<TCollectOperation> CollectOperation;
    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    ui32 RecordGeneration;
    ui32 PerGenerationCounter;
    TBackoffTimer BackoffTimer;
    ui64 CollectorErrors;
    bool IsSpringCleanup;

    TMap<ui32, TMap<ui32, TGroupCollector>> CollectorForGroupForChannel;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KEYVALUE_ACTOR;
    }

    TKeyValueCollector(const TActorId &keyValueActorId, TIntrusivePtr<TCollectOperation> &collectOperation,
            const TTabletStorageInfo *tabletInfo, ui32 recordGeneration, ui32 perGenerationCounter,
            bool isSpringCleanup)
        : KeyValueActorId(keyValueActorId)
        , CollectOperation(collectOperation)
        , TabletInfo(const_cast<TTabletStorageInfo*>(tabletInfo))
        , RecordGeneration(recordGeneration)
        , PerGenerationCounter(perGenerationCounter)
        , BackoffTimer(CollectorErrorInitialBackoffMs, CollectorErrorMaxBackoffMs)
        , CollectorErrors(0)
        , IsSpringCleanup(isSpringCleanup)
    {
        Y_VERIFY(CollectOperation.Get());
    }

    void Bootstrap(const TActorContext &ctx) {
        ui32 endChannel = TabletInfo->Channels.size();
        for (ui32 channelIdx = BLOB_CHANNEL; channelIdx < endChannel; ++channelIdx) {
            const auto *channelInfo = TabletInfo->ChannelInfo(channelIdx);
            for (auto historyIt = channelInfo->History.begin(); historyIt != channelInfo->History.end(); ++historyIt) {
                if (IsSpringCleanup) {
                    CollectorForGroupForChannel[channelIdx][historyIt->GroupID];
                } else {
                    auto nextHistoryIt = historyIt;
                    nextHistoryIt++;
                    if (nextHistoryIt == channelInfo->History.end()) {
                        CollectorForGroupForChannel[channelIdx][historyIt->GroupID];
                    }
                }
            }
        }

        for (const auto &blob: CollectOperation->Keep) {
            ui32 groupId = TabletInfo->ChannelInfo(blob.Channel())->GroupForGeneration(blob.Generation());
            Y_VERIFY(groupId != Max<ui32>(), "Keep Blob# %s is mapped to an invalid group (-1)!",
                    blob.ToString().c_str());
            CollectorForGroupForChannel[blob.Channel()][groupId].Keep.push_back(blob);
        }
        for (const auto &blob: CollectOperation->DoNotKeep) {
            const ui32 groupId = TabletInfo->ChannelInfo(blob.Channel())->GroupForGeneration(blob.Generation());
            Y_VERIFY(groupId != Max<ui32>(), "DoNotKeep Blob# %s is mapped to an invalid group (-1)!",
                    blob.ToString().c_str());
            CollectorForGroupForChannel[blob.Channel()][groupId].DoNotKeep.push_back(blob);
        }

        SendTheRequest(ctx);
        Become(&TThis::StateWait);
    }

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr &ev, const TActorContext &ctx) {
        NKikimrProto::EReplyStatus status = ev->Get()->Status;

        if (status == NKikimrProto::OK) {
            // Success
            CollectorForGroupForChannel.begin()->second.erase(
                    CollectorForGroupForChannel.begin()->second.begin());
            if (CollectorForGroupForChannel.begin()->second.empty()) {
                CollectorForGroupForChannel.erase(
                        CollectorForGroupForChannel.begin());
            }
            if (CollectorForGroupForChannel.empty()) {
                ctx.Send(KeyValueActorId, new TEvKeyValue::TEvEraseCollect());
                Die(ctx);
                return;
            }
            SendTheRequest(ctx);
            return;
        }

        Y_VERIFY(CollectorForGroupForChannel.begin() != CollectorForGroupForChannel.end());
        Y_VERIFY(CollectorForGroupForChannel.begin()->second.begin() !=
                CollectorForGroupForChannel.begin()->second.end());
        ui32 channelIdx = CollectorForGroupForChannel.begin()->first;
        ui32 groupId = CollectorForGroupForChannel.begin()->second.begin()->first;

        CollectorErrors++;
        if (status == NKikimrProto::RACE || status == NKikimrProto::BLOCKED || status == NKikimrProto::NO_GROUP || CollectorErrors > CollectorMaxErrors) {
            LOG_ERROR_S(ctx, NKikimrServices::KEYVALUE, "Tablet# " << TabletInfo->TabletID
                << " Collector got Status# " << NKikimrProto::EReplyStatus_Name(status)
                << " from Group# " << groupId << " Channel# " << channelIdx
                << " CollectorErrors# " << CollectorErrors
                << " Marker# KVC01");
            // Die
            ctx.Send(KeyValueActorId, new TEvents::TEvPoisonPill());
            Die(ctx);
            return;
        }

        // Rertry
        ui64 backoffMs = BackoffTimer.NextBackoffMs();
        if (backoffMs) {
            const TDuration &timeout = TDuration::MilliSeconds(backoffMs);
            ctx.Schedule(timeout, new TEvents::TEvWakeup());
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "Tablet# " << TabletInfo->TabletID
                << " Collector got Status# " << NKikimrProto::EReplyStatus_Name(status)
                << " from Group# " << groupId << " Channel# " << channelIdx
                << " Retrying immediately. Marker# KVC02");
            SendTheRequest(ctx);
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Y_VERIFY(CollectorForGroupForChannel.begin() != CollectorForGroupForChannel.end());
        Y_VERIFY(CollectorForGroupForChannel.begin()->second.begin() !=
                CollectorForGroupForChannel.begin()->second.end());
        ui32 channelIdx = CollectorForGroupForChannel.begin()->first;
        ui32 groupId = CollectorForGroupForChannel.begin()->second.begin()->first;
        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "Tablet# " << TabletInfo->TabletID
                << " Collector retrying with"
                << " Group# " << groupId << " Channel# " << channelIdx
                << " Marker# KVC03");
        SendTheRequest(ctx);
        return;
    }

    void Handle(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Die(ctx);
        return;
    }

    void PrepareVector(const TDeque<TLogoBlobID> &in, THolder<TVector<TLogoBlobID>> &out) {
        ui64 size = in.size();
        if (size) {
            out.Reset(new TVector<TLogoBlobID>(size));
            ui64 outIdx = 0;
            for (const auto &blob: in) {
                (*out)[outIdx] = blob;
                ++outIdx;
            }
            Y_VERIFY(outIdx == size);
        }
    }

    void SendTheRequest(const TActorContext &ctx) {
        THolder<TVector<TLogoBlobID>> keep;
        THolder<TVector<TLogoBlobID>> doNotKeep;
        PrepareVector(CollectorForGroupForChannel.begin()->second.begin()->second.Keep, keep);
        PrepareVector(CollectorForGroupForChannel.begin()->second.begin()->second.DoNotKeep, doNotKeep);
        ui32 channelIdx = CollectorForGroupForChannel.begin()->first;
        ui32 groupId = CollectorForGroupForChannel.begin()->second.begin()->first;
        SendToBSProxy(ctx, groupId,
            new TEvBlobStorage::TEvCollectGarbage(TabletInfo->TabletID, RecordGeneration, PerGenerationCounter,
                channelIdx, true,
                CollectOperation->Header.CollectGeneration, CollectOperation->Header.CollectStep,
                keep.Release(), doNotKeep.Release(), TInstant::Max(), true), 0);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
            HFunc(TEvents::TEvPoisonPill, Handle);
            default:
                break;
        }
    }
};

IActor* CreateKeyValueCollector(const TActorId &keyValueActorId, TIntrusivePtr<TCollectOperation> &collectOperation,
        const TTabletStorageInfo *tabletInfo, ui32 recordGeneration, ui32 perGenerationCounter, bool isSpringCleanup) {
    return new TKeyValueCollector(keyValueActorId, collectOperation, tabletInfo, recordGeneration,
        perGenerationCounter, isSpringCleanup);
}

} // NKeyValue
} // NKikimr
