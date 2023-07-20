#include "keyvalue_flat_impl.h"


#include <ydb/core/base/counters.h>
#include <ydb/core/blobstorage/dsproxy/blobstorage_backoff.h>
#include <ydb/core/util/stlog.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NKikimr {
namespace NKeyValue {
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Collector
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TGroupCollector {
    TDeque<TLogoBlobID> Keep;
    TDeque<TLogoBlobID> DoNotKeep;
    ui32 CountOfSentFlags = 0;
    ui32 NextCountOfSentFlags = 0;
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
    bool IsRepeatedRequest = false;

    // [channel][groupId]
    TVector<TMap<ui32, TGroupCollector>> CollectorForGroupForChannel;
    ui32 EndChannel = 0;

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

    ui32 GetVecIdxFromChannelIdx(ui32 channelIdx) {
        return EndChannel - 1 - channelIdx;
    }

    ui32 GetChannelIdFromVecIdx(ui32 deqIdx) {
        return EndChannel - 1 - deqIdx;
    }

    void Bootstrap() {
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

        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC04, "Start KeyValueCollector",
            (TabletId, TabletInfo->TabletID),
            (Keep, CollectOperation->Keep.size()), (DoNotKeep, CollectOperation->DoNotKeep.size()));

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
        ui64 maxDoNotKeepSizeInGroupChannel = 0;
        for (auto &groups : CollectorForGroupForChannel) {
            for (auto &[groupId, collector] : groups) {
                maxDoNotKeepSizeInGroupChannel = Max(maxDoNotKeepSizeInGroupChannel, collector.DoNotKeep.size());
            }
        }

        SendTheRequest();
        Become(&TThis::StateWait);
    }

    ui32 GetCurretChannelId() {
        return GetChannelIdFromVecIdx(CollectorForGroupForChannel.size() - 1);
    }

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr &ev) {
        NKikimrProto::EReplyStatus status = ev->Get()->Status;
        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC11, "Receive TEvCollectGarbageResult",
            (TabletId, TabletInfo->TabletID),
            (Status, status));

        auto collectorsOfCurrentChannel = CollectorForGroupForChannel.rbegin();
        if (collectorsOfCurrentChannel == CollectorForGroupForChannel.rend()) {
            STLOG(NLog::PRI_ERROR, NKikimrServices::KEYVALUE_GC, KVC17,
                "Collectors must be exist when we recieve TEvCollectGarbageResult",
                (TabletId, TabletInfo->TabletID), (CollectorErrors, CollectorErrors));
            HandleErrorAndDie();
            return;
        }

        auto currentCollectorIterator = collectorsOfCurrentChannel->begin();
        if  (currentCollectorIterator == collectorsOfCurrentChannel->end()) {
            STLOG(NLog::PRI_ERROR, NKikimrServices::KEYVALUE_GC, KVC17,
                "Collectors must be exist in the current channel when we recieve TEvCollectGarbageResult",
                (TabletId, TabletInfo->TabletID), (Channel, GetCurretChannelId()), (CollectorErrors, CollectorErrors));
            HandleErrorAndDie();
            return;
        }

        if (status == NKikimrProto::OK) {
            // Success
            bool isLastRequestInCollector = false;
            {
                TGroupCollector &collector = currentCollectorIterator->second;
                collector.CountOfSentFlags = collector.NextCountOfSentFlags;
                isLastRequestInCollector = (collector.CountOfSentFlags == collector.Keep.size() + collector.DoNotKeep.size());
            }
            if (isLastRequestInCollector) {
                STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC08, "Last group was empty, it's erased",
                    (TabletId, TabletInfo->TabletID), (GroupId, currentCollectorIterator->first), (Channel, GetCurretChannelId()));
                currentCollectorIterator = collectorsOfCurrentChannel->erase(currentCollectorIterator);
            }
            if (currentCollectorIterator == collectorsOfCurrentChannel->end()) {
                STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC08, "Last channel was empty, it's erased",
                    (TabletId, TabletInfo->TabletID), (Channel, GetCurretChannelId()));
                CollectorForGroupForChannel.pop_back();
            }
            if (CollectorForGroupForChannel.empty()) {
                SendCompleteGCAndDie();
                return;
            }
            SendTheRequest();
            return;
        }

        ui32 channelId = GetCurretChannelId();
        ui32 groupId = currentCollectorIterator->first;

        CollectorErrors++;
        if (status == NKikimrProto::RACE || status == NKikimrProto::BLOCKED || status == NKikimrProto::NO_GROUP || CollectorErrors > CollectorMaxErrors) {
            STLOG(NLog::PRI_ERROR, NKikimrServices::KEYVALUE_GC, KVC01, "Collector got not OK status",
                (TabletId, TabletInfo->TabletID), (GroupId, groupId), (Channel, channelId), (Status, status),
                (CollectorErrors, CollectorErrors), (CollectorMaxErrors, CollectorMaxErrors));
            HandleErrorAndDie();
            return;
        }

        // Rertry
        IsRepeatedRequest = true;
        ui64 backoffMs = BackoffTimer.NextBackoffMs();
        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC02, "Collector got not OK status, retry",
            (TabletId, TabletInfo->TabletID), (GroupId, groupId), (Channel, channelId),
            (Status, status), (BackoffMs, backoffMs), (RetryingImmediately, (backoffMs ? "no" : "yes")));
        if (backoffMs) {
            const TDuration &timeout = TDuration::MilliSeconds(backoffMs);
            TActivationContext::Schedule(timeout, new IEventHandle(TEvents::TEvWakeup::EventType, 0, SelfId(), SelfId(), nullptr, 0));
        } else {
            SendTheRequest();
        }
    }

    void SendCompleteGCAndDie() {
        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC19, "Collector send CompleteGC",
            (TabletId, TabletInfo->TabletID));
        Send(KeyValueActorId, new TEvKeyValue::TEvCompleteGC());
        PassAway();
    }

    void SendTheRequest() {
        auto collectorsOfCurrentChannel = CollectorForGroupForChannel.rbegin();
        if (collectorsOfCurrentChannel == CollectorForGroupForChannel.rend()) {
            STLOG(NLog::PRI_ERROR, NKikimrServices::KEYVALUE_GC, KVC30,
                "Collectors must be exist when we try to send the request",
                (TabletId, TabletInfo->TabletID), (CollectorErrors, CollectorErrors));
            HandleErrorAndDie();
            return;
        }

        auto currentCollectorIterator = collectorsOfCurrentChannel->begin();
        if  (currentCollectorIterator == collectorsOfCurrentChannel->end()) {
            STLOG(NLog::PRI_ERROR, NKikimrServices::KEYVALUE_GC, KVC31,
                "Collectors must be exist in the current channel we try to send the request",
                (TabletId, TabletInfo->TabletID), (Channel, GetCurretChannelId()), (CollectorErrors, CollectorErrors));
            HandleErrorAndDie();
            return;
        }

        THolder<TVector<TLogoBlobID>> keep;
        THolder<TVector<TLogoBlobID>> doNotKeep;

        TGroupCollector &collector = currentCollectorIterator->second;
        collector.NextCountOfSentFlags = collector.CountOfSentFlags;

        ui32 doNotKeepSize = collector.DoNotKeep.size();
        if (collector.NextCountOfSentFlags < doNotKeepSize) {
            doNotKeepSize -= collector.NextCountOfSentFlags;
        } else {
            doNotKeepSize = 0;
        }

        if (doNotKeepSize) {
            doNotKeepSize = Min(doNotKeepSize, (ui32)MaxCollectGarbageFlagsPerMessage);
            doNotKeep.Reset(new TVector<TLogoBlobID>(doNotKeepSize));
            auto begin = collector.DoNotKeep.begin() + collector.NextCountOfSentFlags;
            auto end = begin + doNotKeepSize;

            collector.NextCountOfSentFlags += doNotKeepSize;
            Copy(begin, end, doNotKeep->begin());
        }

        ui32 keepStartIdx = 0;
        if (collector.NextCountOfSentFlags >= collector.DoNotKeep.size()) {
            keepStartIdx = collector.NextCountOfSentFlags - collector.DoNotKeep.size();
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
            collector.NextCountOfSentFlags += idx;
        }

        bool isLast = (collector.Keep.size() + collector.DoNotKeep.size() == collector.NextCountOfSentFlags);

        ui32 collectGeneration = CollectOperation->Header.CollectGeneration;
        ui32 collectStep = CollectOperation->Header.CollectStep;
        ui32 channelIdx = GetChannelIdFromVecIdx(CollectorForGroupForChannel.size() - 1);
        ui32 groupId = currentCollectorIterator->first;


        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC16, "Send GC request",
            (TabletId, TabletInfo->TabletID), (CollectGeneration, collectGeneration),
            (CollectStep, collectStep), (ChannelIdx, channelIdx), (GroupId, groupId),
            (KeepSize, keepSize), (DoNotKeepSize, doNotKeepSize), (IsLast, isLast));
        SendToBSProxy(TActivationContext::AsActorContext(), groupId,
            new TEvBlobStorage::TEvCollectGarbage(TabletInfo->TabletID, RecordGeneration, PerGenerationCounter,
                channelIdx, isLast, collectGeneration, collectStep,
                keep.Release(), doNotKeep.Release(), TInstant::Max(), true), (ui64)TKeyValueState::ECollectCookie::Soft);
        IsRepeatedRequest = false;
    }

    void HandleWakeUp() {
        auto collectorsOfCurrentChannel = CollectorForGroupForChannel.rbegin();
        if (collectorsOfCurrentChannel == CollectorForGroupForChannel.rend()) {
            STLOG(NLog::PRI_ERROR, NKikimrServices::KEYVALUE_GC, KVC17,
                "Collectors must be exist when we try to resend the request",
                (TabletId, TabletInfo->TabletID), (CollectorErrors, CollectorErrors));
            HandleErrorAndDie();
            return;
        }

        auto currentCollectorIterator = collectorsOfCurrentChannel->begin();
        if  (currentCollectorIterator == collectorsOfCurrentChannel->end()) {
            STLOG(NLog::PRI_ERROR, NKikimrServices::KEYVALUE_GC, KVC17,
                "Collectors must be exist in the current channel we try to resend the request",
                (TabletId, TabletInfo->TabletID), (Channel, GetCurretChannelId()), (CollectorErrors, CollectorErrors));
            HandleErrorAndDie();
            return;
        }

        ui32 channelId = GetCurretChannelId();
        ui32 groupId = currentCollectorIterator->first;
        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC03, "Collector retrying",
            (TabletId, TabletInfo->TabletID), (GroupId, groupId), (Channel, channelId));
        SendTheRequest();
    }

    void HandlePoisonPill() {
        STLOG(NLog::PRI_ERROR, NKikimrServices::KEYVALUE_GC, KVC12, "Poisoned",
            (TabletId, TabletInfo->TabletID));
        PassAway();
    }

    void HandleErrorAndDie() {
        STLOG(NLog::PRI_ERROR, NKikimrServices::KEYVALUE_GC, KVC18, "Garbage Collector catch the error, send PoisonPill to the tablet",
            (TabletId, TabletInfo->TabletID));
        Send(KeyValueActorId, new TEvents::TEvPoisonPill());
        PassAway();
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            cFunc(TEvents::TEvWakeup::EventType, HandleWakeUp);
            cFunc(TEvents::TEvPoisonPill::EventType, HandlePoisonPill);
            default:
                break;
        }
    }
};

IActor* CreateKeyValueCollector(const TActorId &keyValueActorId, TIntrusivePtr<TCollectOperation> &collectOperation,
        const TTabletStorageInfo *TabletInfo, ui32 recordGeneration, ui32 perGenerationCounter, bool isSpringCleanup) {
    return new TKeyValueCollector(keyValueActorId, collectOperation, TabletInfo, recordGeneration,
        perGenerationCounter, isSpringCleanup);
}

} // NKeyValue
} // NKikimr
