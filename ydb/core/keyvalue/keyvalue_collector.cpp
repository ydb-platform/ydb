#include "keyvalue_flat_impl.h"


#include <ydb/core/base/counters.h>
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
    ui32 Step = 0;
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

    // [channel][groupId]
    TVector<TMap<ui32, TGroupCollector>> CollectorForGroupForChannel;
    ui32 EndChannel = 0;
    bool IsMultiStepMode = false;
    TMap<ui32, TGroupCollector>::iterator CurrentChannelGroup;

    // For Keep
    ui32 ChannelIdxInVector = 0;
    TMaybe<THelpers::TGenerationStep> MinGenStepInCircle;

    // For DoNotKeep
    TVector<TLogoBlobID> CollectedDoNotKeep;

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
        , IsMultiStepMode(CollectOperation->Keep.size() + CollectOperation->DoNotKeep.size() > MaxCollectGarbageFlagsPerMessage)
    {
        Y_VERIFY(CollectOperation.Get());
    }

    ui32 GetVecIdxFromChannelIdx(ui32 channelIdx) {
        return EndChannel - 1 - channelIdx;
    }

    ui32 GetChannelIdxFromVecIdx(ui32 deqIdx) {
        return EndChannel - 1 - deqIdx;
    }

    void Bootstrap(const TActorContext &ctx) {
        EndChannel = TabletInfo->Channels.size();
        CollectorForGroupForChannel.resize(EndChannel - BLOB_CHANNEL);
        for (ui32 channelIdx = BLOB_CHANNEL; channelIdx < EndChannel; ++channelIdx) {
            const auto *channelInfo = TabletInfo->ChannelInfo(channelIdx);
            for (auto historyIt = channelInfo->History.begin(); historyIt != channelInfo->History.end(); ++historyIt) {
                if (IsSpringCleanup) {
                    CollectorForGroupForChannel[GetVecIdxFromChannelIdx(channelIdx)][historyIt->GroupID];
                } else {
                    auto nextHistoryIt = historyIt;
                    nextHistoryIt++;
                    if (nextHistoryIt == channelInfo->History.end()) {
                        CollectorForGroupForChannel[GetVecIdxFromChannelIdx(channelIdx)][historyIt->GroupID];
                    }
                }
            }
        }

        if (IsMultiStepMode) {
            CollectedDoNotKeep.reserve(MaxCollectGarbageFlagsPerMessage);
        }

        Sort(CollectOperation->Keep);
        for (const auto &blob: CollectOperation->Keep) {
            ui32 groupId = TabletInfo->ChannelInfo(blob.Channel())->GroupForGeneration(blob.Generation());
            Y_VERIFY(groupId != Max<ui32>(), "Keep Blob# %s is mapped to an invalid group (-1)!",
                    blob.ToString().c_str());
            CollectorForGroupForChannel[GetVecIdxFromChannelIdx(blob.Channel())][groupId].Keep.push_back(blob);
        }
        for (const auto &blob: CollectOperation->DoNotKeep) {
            const ui32 groupId = TabletInfo->ChannelInfo(blob.Channel())->GroupForGeneration(blob.Generation());
            Y_VERIFY(groupId != Max<ui32>(), "DoNotKeep Blob# %s is mapped to an invalid group (-1)!",
                    blob.ToString().c_str());
            CollectorForGroupForChannel[GetVecIdxFromChannelIdx(blob.Channel())][groupId].DoNotKeep.push_back(blob);
        }

        MinGenStepInCircle = THelpers::TGenerationStep(Max<ui32>(), Max<ui32>());
        ChannelIdxInVector = CollectorForGroupForChannel.size() - 1;
        CurrentChannelGroup = CollectorForGroupForChannel.back().begin();
        SendTheRequest(ctx);
        Become(&TThis::StateWait);
    }

    bool ChangeGroup(const TActorContext &ctx) {
        if (CollectorForGroupForChannel.back().empty()) {
            while (CollectorForGroupForChannel.size() && CollectorForGroupForChannel.back().empty()) {
                CollectorForGroupForChannel.pop_back();
            }
            if (CollectorForGroupForChannel.empty()) {
                ctx.Send(KeyValueActorId, new TEvKeyValue::TEvCompleteGC());
                Die(ctx);
                return true;
            }
            ChannelIdxInVector = CollectorForGroupForChannel.size() - 1;
            CurrentChannelGroup = CollectorForGroupForChannel[ChannelIdxInVector].begin();
        } else {
            do {
                if (ChannelIdxInVector) {
                    ChannelIdxInVector--;
                } else {
                    ChannelIdxInVector = CollectorForGroupForChannel.size() - 1;
                    CurrentChannelGroup = CollectorForGroupForChannel[ChannelIdxInVector].begin();
                    SendPartitialCompleteGC(true);
                    return true;
                }
            } while (CollectorForGroupForChannel[ChannelIdxInVector].empty());
            CurrentChannelGroup = CollectorForGroupForChannel[ChannelIdxInVector].begin();
        }
        return false;
    }

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr &ev, const TActorContext &ctx) {

        NKikimrProto::EReplyStatus status = ev->Get()->Status;

        if (status == NKikimrProto::OK) {
            // Success

            bool isLastRequestInCollector = false;
            {
                TGroupCollector &collector = CurrentChannelGroup->second;
                isLastRequestInCollector = (collector.Step == collector.Keep.size() + collector.DoNotKeep.size());
            }
            if (isLastRequestInCollector) {
                CurrentChannelGroup = CollectorForGroupForChannel[ChannelIdxInVector].erase(CurrentChannelGroup);
            } else {
                CurrentChannelGroup++;
            }
            if (CurrentChannelGroup == CollectorForGroupForChannel[ChannelIdxInVector].end()) {
                if (ChangeGroup(ctx)) {
                    return;
                }
            }
            SendTheRequest(ctx);
            return;
        }

        ui32 channelIdx = GetChannelIdxFromVecIdx(ChannelIdxInVector);
        ui32 groupId = CurrentChannelGroup->first;

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
        ui32 channelIdx = GetChannelIdxFromVecIdx(ChannelIdxInVector);
        ui32 groupId = CurrentChannelGroup->first;
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

    void Handle(TEvKeyValue::TEvContinueGC::TPtr &ev) {
        LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::KEYVALUE, "Tablet# " << TabletInfo->TabletID
                << " Collector continue GC Marker# KVC04");
        MinGenStepInCircle = {};
        CollectedDoNotKeep = std::move(ev->Get()->Buffer);
        CollectedDoNotKeep.clear();
        SendTheRequest(TActivationContext::AsActorContext());
    }

    void SendPartitialCompleteGC(bool endCircle) {
        LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::KEYVALUE, "Tablet# " << TabletInfo->TabletID
                << "end of round# " << (endCircle ? "yes" : "no")
                << " Collector send PartitialCompleteGC Marker# KVC05");
        auto ev = std::make_unique<TEvKeyValue::TEvPartitialCompleteGC>();
        if (endCircle && MinGenStepInCircle) {
            ev->CollectedGenerationStep = std::move(MinGenStepInCircle);
        }
        ev->CollectedDoNotKeep = std::move(CollectedDoNotKeep);

        TActivationContext::Send(new IEventHandle(KeyValueActorId, SelfId(), ev.release()));
    }

    void SendTheRequest(const TActorContext &ctx) {
        THolder<TVector<TLogoBlobID>> keep;
        THolder<TVector<TLogoBlobID>> doNotKeep;

        TGroupCollector &collector = CurrentChannelGroup->second;

        ui32 doNotKeepSize = collector.DoNotKeep.size();
        if (collector.Step < doNotKeepSize) {
            doNotKeepSize -= collector.Step;
        } else {
            doNotKeepSize = 0;
        }

        if (doNotKeepSize && CollectedDoNotKeep.size() + doNotKeepSize > MaxCollectGarbageFlagsPerMessage) {
            SendPartitialCompleteGC(false);
            return;
        }

        if (doNotKeepSize) {
            doNotKeepSize = Min(doNotKeepSize, (ui32)MaxCollectGarbageFlagsPerMessage);
            doNotKeep.Reset(new TVector<TLogoBlobID>(doNotKeepSize));
            auto begin = collector.DoNotKeep.begin() + collector.Step;
            auto end = begin + doNotKeepSize;

            collector.Step += doNotKeepSize;
            Copy(begin, end, doNotKeep->begin());
            Copy(doNotKeep->cbegin(), doNotKeep->cend(), std::back_inserter(CollectedDoNotKeep));
        }

        ui32 keepStartIdx = 0;
        if (collector.Step >= collector.DoNotKeep.size()) {
            keepStartIdx = collector.Step - collector.DoNotKeep.size();
        }
        ui32 keepSize = Min(collector.Keep.size() - keepStartIdx, MaxCollectGarbageFlagsPerMessage - doNotKeepSize);
        if (keepSize) {
            keep.Reset(new TVector<TLogoBlobID>(keepSize));
            TMaybe<THelpers::TGenerationStep> collectedGenStep;
            THelpers::TGenerationStep prevGenStep = THelpers::GenerationStep(collector.Keep.front());
            auto begin = collector.Keep.begin() + keepStartIdx;
            auto end = begin + keepSize;
            ui32 idx = 0;
            for (auto it = begin; it != end; ++it, ++idx) {
                THelpers::TGenerationStep genStep = THelpers::GenerationStep(*it);
                if (prevGenStep != genStep) {
                    collectedGenStep = prevGenStep;
                    prevGenStep = genStep;
                }
                (*keep)[idx] = *it;
            }
            collector.Step += idx;
            if (collectedGenStep && MinGenStepInCircle) {
                MinGenStepInCircle = Min(*MinGenStepInCircle, *collectedGenStep);
            } else if (collectedGenStep) {
                MinGenStepInCircle = collectedGenStep;
            }
        }

        bool isLast = (collector.Keep.size() + collector.DoNotKeep.size() == collector.Step);

        ui32 collectGeneration = CollectOperation->Header.CollectGeneration;
        ui32 collectStep = CollectOperation->Header.CollectStep;
        ui32 channelIdx = GetChannelIdxFromVecIdx(CollectorForGroupForChannel.size() - 1);
        ui32 groupId = CurrentChannelGroup->first;
        SendToBSProxy(ctx, groupId,
            new TEvBlobStorage::TEvCollectGarbage(TabletInfo->TabletID, RecordGeneration, PerGenerationCounter,
                channelIdx, isLast, collectGeneration, collectStep,
                keep.Release(), doNotKeep.Release(), TInstant::Max(), true), (ui64)TKeyValueState::ECollectCookie::Soft);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            hFunc(TEvKeyValue::TEvContinueGC, Handle);
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
