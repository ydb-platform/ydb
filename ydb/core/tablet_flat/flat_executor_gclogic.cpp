#include "flat_executor_gclogic.h"
#include "flat_bio_eggs.h"
#include <ydb/core/base/tablet.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

TExecutorGCLogic::TExecutorGCLogic(TIntrusiveConstPtr<TTabletStorageInfo> info, TAutoPtr<NPageCollection::TSteppedCookieAllocator> cookies)
    : HistoryCutter(info)
    , TabletStorageInfo(std::move(info))
    , Cookies(cookies)
    , Generation(Cookies->Gen)
    , Slicer(1, Cookies.Get(), NBlockIO::BlockSize)
    , SnapshotStep(0)
    , PrevSnapshotStep(0)
    , ConfirmedOnSendStep(0)
    , AllowGarbageCollection(false)
{
}

void TExecutorGCLogic::WriteToLog(TLogCommit &commit) {
    TGCTime time(Generation, commit.Step);
    TGCLogEntry& uncommittedDelta = UncommittedDeltaLog[time];
    uncommittedDelta.Time = time;

    const ui32 gcEntriesInBatch = 250000; // ~7mb rawsize

    Cookies->Switch(commit.Step, true /* require step switch */);

    const ui32 gcGrowSize = commit.GcDelta.Created.size();
    const ui32 gcLeftSize = commit.GcDelta.Deleted.size();

    if (commit.Type == ECommit::Snap || gcGrowSize + gcLeftSize < gcEntriesInBatch) {
        uncommittedDelta.Delta = commit.GcDelta; /* preserve for embedding  */
    } else {
        ui32 placedDiscovered = 0;
        ui32 placedLeft = 0;

        uncommittedDelta.Delta = std::exchange(commit.GcDelta, { });

        while (placedDiscovered < gcGrowSize || placedLeft < gcLeftSize) {
            NKikimrExecutorFlat::TExternalGcEntry proto;

            ui32 leftInBatch = gcEntriesInBatch;
            if (placedDiscovered < gcGrowSize) {
                const ui32 toMove = Min(leftInBatch, gcGrowSize - placedDiscovered);
                auto it = uncommittedDelta.Delta.Created.begin() + placedDiscovered;
                LogoBlobIDRepatedFromLogoBlobIDVector(proto.MutableGcDiscovered(), it, it + toMove);
                placedDiscovered += toMove;
                leftInBatch -= toMove;
            }

            if (leftInBatch && placedLeft < gcLeftSize) {
                const ui32 toMove = Min(leftInBatch, gcLeftSize - placedLeft);
                auto it = uncommittedDelta.Delta.Deleted.begin() + placedLeft;
                LogoBlobIDRepatedFromLogoBlobIDVector(proto.MutableGcLeft(), it, it + toMove);
                placedLeft += toMove;
            }

            Slicer.One(commit.Refs, proto.SerializeAsString(), true);
        }
    }
}

TGCLogEntry TExecutorGCLogic::SnapshotLog(ui32 step) {
    TGCTime snapshotTime(Generation, step);
    TGCLogEntry snapshot(snapshotTime);
    for (const auto& chIt : ChannelInfo) {
        for (const auto& le : chIt.second.CommittedDelta) {
            Y_ABORT_UNLESS(le.first <= snapshotTime);
            TExecutorGCLogic::MergeVectors(snapshot.Delta.Created, le.second.Created);
            TExecutorGCLogic::MergeVectors(snapshot.Delta.Deleted, le.second.Deleted);
        }
    }

    for (const auto &it : UncommittedDeltaLog) {
        TExecutorGCLogic::MergeVectors(snapshot.Delta.Created, it.second.Delta.Created);
        TExecutorGCLogic::MergeVectors(snapshot.Delta.Deleted, it.second.Delta.Deleted);
    }

    PrevSnapshotStep = SnapshotStep;
    SnapshotStep = step;
    return snapshot;
}

void TExecutorGCLogic::SnapToLog(NKikimrExecutorFlat::TLogSnapshot &snap, ui32 step) {
    TGCLogEntry gcLogEntry = SnapshotLog(step);
    auto *gcSnapDiscovered = snap.MutableGcSnapDiscovered();
    auto *gcSnapLeft = snap.MutableGcSnapLeft();

    gcSnapDiscovered->Reserve(gcLogEntry.Delta.Created.size());
    for (const TLogoBlobID &x : gcLogEntry.Delta.Created)
        LogoBlobIDFromLogoBlobID(x, gcSnapDiscovered->Add());

    gcSnapLeft->Reserve(gcLogEntry.Delta.Deleted.size());
    for (const TLogoBlobID &x : gcLogEntry.Delta.Deleted)
        LogoBlobIDFromLogoBlobID(x, gcSnapLeft->Add());

    for (const auto &chIt : ChannelInfo) {
        if (chIt.second.CommitedGcBarrier) {
            auto *x = snap.AddGcBarrierInfo();
            x->SetChannel(chIt.first);
            x->SetSetToGeneration(chIt.second.CommitedGcBarrier.Generation);
            x->SetSetToStep(chIt.second.CommitedGcBarrier.Step);

            if (!chIt.second.CutHistory && chIt.second.GcWaitFor == 0) {
                ChannelsToCutHistory.push_back(chIt.first);
            }
        }
    }
}

void TExecutorGCLogic::OnCommitLog(ui32 step, ui32 confirmedOnSend, const TActorContext& ctx) {
    auto it = UncommittedDeltaLog.find(TGCTime(Generation, step));
    if (it != UncommittedDeltaLog.end()) {
        ApplyDelta(it->first, it->second.Delta);
        UncommittedDeltaLog.erase(it);
    }

    ConfirmedOnSendStep = Max(ConfirmedOnSendStep, confirmedOnSend);

    if (step >= SnapshotStep)
        SendCollectGarbage(ctx);
}

void TExecutorGCLogic::OnCollectGarbageResult(TEvBlobStorage::TEvCollectGarbageResult::TPtr &ptr) {
    TEvBlobStorage::TEvCollectGarbageResult* ev = ptr->Get();
    TChannelInfo& channel = ChannelInfo[ev->Channel];
    if (ev->Status == NKikimrProto::EReplyStatus::OK) {
        channel.OnCollectGarbageSuccess();
    } else {
        channel.OnCollectGarbageFailure();
    }
}

void TExecutorGCLogic::ApplyLogEntry(TGCLogEntry& entry) {
    ApplyDelta(entry.Time, entry.Delta);
}

void TExecutorGCLogic::ApplyLogSnapshot(TGCLogEntry &snapshot, const TVector<std::pair<ui32, ui64>> &barriers) {
    ApplyLogEntry(snapshot);
    for (auto &xpair : barriers) {
        const ui32 channel = xpair.first;
        const std::pair<ui32, ui32> barrier = ExpandGenStepPair(xpair.second);
        ChannelInfo[channel].CommitedGcBarrier = {barrier.first, barrier.second};
    }
}

void TExecutorGCLogic::HoldBarrier(ui32 step) {
    Y_ABORT_UNLESS(true == HoldBarriersSet.insert(TGCTime(Generation, step)).second);
}

void TExecutorGCLogic::ReleaseBarrier(ui32 step) {
    Y_ABORT_UNLESS(1 == HoldBarriersSet.erase(TGCTime(Generation, step)));
}

ui32 TExecutorGCLogic::GetActiveGcBarrier() {
    if (HoldBarriersSet.empty())
        return Max<ui32>();
    return HoldBarriersSet.begin()->Step;
}

void TExecutorGCLogic::FollowersSyncComplete(bool isBoot) {
    Y_UNUSED(isBoot);
    AllowGarbageCollection = true;
}

void TExecutorGCLogic::Confirm(const TActorContext &ctx, TActorId launcher) {
    Cerr << (TStringBuilder() << "CutHistory " << TabletStorageInfo->TabletID << "\n");
    for (auto channel : ChannelsToCutHistory) {
        Cerr << (TStringBuilder() << "CutHistory " << TabletStorageInfo->TabletID <<  " " << channel << "\n");
        auto historyToCut = HistoryCutter.GetHistoryToCut(channel);
        for (const auto* historyEntry : historyToCut) {
            Cerr << (TStringBuilder() << "CutHistory " << TabletStorageInfo->TabletID <<  " " << channel << " " << historyEntry->FromGeneration << "\n");
            TAutoPtr<TEvTablet::TEvCutTabletHistory> ev(new TEvTablet::TEvCutTabletHistory);
            auto &record = ev->Record;
            record.SetTabletID(TabletStorageInfo->TabletID);
            record.SetChannel(channel);
            record.SetFromGeneration(historyEntry->FromGeneration);
            record.SetGroupID(historyEntry->GroupID);
            ctx.Send(launcher, ev.Release());
            Cerr << "Sent\n";
        }
        ChannelInfo[channel].CutHistory = true;
        Cerr << "out of\n";
    }
    Cerr << "here\n";
    ChannelsToCutHistory.clear();
}

void TExecutorGCLogic::ApplyDelta(TGCTime time, TGCBlobDelta &delta) {
    for (const TLogoBlobID &blobId : delta.Created) {
        auto &channel = ChannelInfo[blobId.Channel()];
        TGCTime gcTime(blobId.Generation(), blobId.Step());
        Y_ABORT_UNLESS(channel.KnownGcBarrier < gcTime);
        channel.CommittedDelta[gcTime].Created.push_back(blobId);
    }

    for (const TLogoBlobID &blobId : delta.Deleted) {
        auto &channel = ChannelInfo[blobId.Channel()];
        channel.CommittedDelta[time].Deleted.push_back(blobId);
    }
}

void TExecutorGCLogic::SendCollectGarbage(const TActorContext& ctx) {
    if (!AllowGarbageCollection)
        return;

    const TGCTime uncommittedTime(UncommittedDeltaLog.empty() ? TGCTime::Infinity() : UncommittedDeltaLog.begin()->first);
    const TGCTime uncommitedSnap(Generation, ConfirmedOnSendStep >= SnapshotStep ? SnapshotStep : PrevSnapshotStep);
    const TGCTime minBarrier = HoldBarriersSet.empty() ? TGCTime::Infinity() : *HoldBarriersSet.begin();

    const TGCTime minTime = std::min(uncommittedTime, std::min(uncommitedSnap, minBarrier));

    for (auto it = ChannelInfo.begin(); it != ChannelInfo.end(); ++it) {
        it->second.SendCollectGarbage(minTime, TabletStorageInfo.Get(), it->first, Generation, ctx);
    }
}

TExecutorGCLogic::TChannelInfo::TChannelInfo()
    : GcCounter(1)
    , GcWaitFor(0)
{
}

void TExecutorGCLogic::TChannelInfo::ApplyDelta(TGCTime time, TGCBlobDelta& delta) {
    TGCBlobDelta& committedDelta = CommittedDelta[time];
    DoSwap(committedDelta, delta);
    Y_DEBUG_ABORT_UNLESS(delta.Created.empty() && delta.Deleted.empty());
}

void TExecutorGCLogic::MergeVectors(TVector<TLogoBlobID>& destination, const TVector<TLogoBlobID>& source) {
    if (!source.empty()) {
        destination.insert(destination.end(), source.begin(), source.end());
    }
}

void TExecutorGCLogic::MergeVectors(THolder<TVector<TLogoBlobID>>& destination, const TVector<TLogoBlobID>& source) {
    if (!source.empty()) {
        if (!destination) {
            destination.Reset(new TVector<TLogoBlobID>(source));
        } else {
            MergeVectors(*destination.Get(), source);
        }
    }
}

void DeduplicateGCKeepVectors(TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep, ui32 barrierGen, ui32 barrierStep) {
    if (keep && doNotKeep && !keep->empty() && !doNotKeep->empty()) {
        // vectors must be sorted!

        TVector<TLogoBlobID>::const_iterator keepIt = keep->begin();
        TVector<TLogoBlobID>::const_iterator keepEnd = keep->end();
        TVector<TLogoBlobID>::const_iterator notIt = doNotKeep->begin();
        TVector<TLogoBlobID>::const_iterator notEnd = doNotKeep->end();
        TVector<TLogoBlobID>::iterator keepIns = keep->begin();
        TVector<TLogoBlobID>::iterator notIns = doNotKeep->begin();

        bool keepModified = false;
        bool notKeepModified = false;

        for (;;) {
            if (keepIt == keepEnd) {
                if (notKeepModified && notIt != notEnd) {
                    const ui64 toCopy = notEnd - notIt;
                    MemMove(&*notIns, &*notIt, toCopy);
                    notIns += toCopy;
                }
                break;
            }

            if (notIt == notEnd) {
                if (keepModified && keepIt != keepEnd) {
                    const ui64 toCopy = keepEnd - keepIt;
                    MemMove(&*keepIns, &*keepIt, toCopy);
                    keepIns += toCopy;
                }
                break;
            }

            if (*keepIt < *notIt) {
                if (keepModified)
                    *keepIns = *keepIt;
                ++keepIns;
                ++keepIt;
            } else if (*notIt < *keepIt) {
                if (notKeepModified)
                    *notIns = *notIt;
                ++notIns;
                ++notIt;
            } else {
                // we discard keep mark anycase and
                // could eleminate donotkeep mark if seen both events in live tail
                if (keepIt->Generation() != barrierGen || keepIt->Step() <= barrierStep) {
                    if (notKeepModified)
                        *notIns = *notIt;
                    ++notIns;
                } else {
                    notKeepModified = true;
                }

                keepModified = true;
                ++keepIt;
                ++notIt;
            }
        }

        if (keepModified)
            keep->erase(keepIns, keepEnd);
        if (notKeepModified)
            doNotKeep->erase(notIns, notEnd);
    }
}

TVector<TLogoBlobID>* TExecutorGCLogic::CreateVector(const TVector<TLogoBlobID>& source) {
    if (!source.empty()) {
        return new TVector<TLogoBlobID>(source);
    } else {
        return nullptr;
    }
}

TExecutorGCLogic::TIntrospection TExecutorGCLogic::IntrospectStateSize() const {
     TIntrospection ret;

     for (auto &xpair : UncommittedDeltaLog) {
         ++ret.UncommitedEntries;
         ret.UncommitedBlobIds += xpair.second.Delta.Created.size() + xpair.second.Delta.Deleted.size();
     }
     ret.UncommitedEntriesBytes += ret.UncommitedBlobIds * sizeof(TLogoBlobID) + 96;
     ret.BarriersSetSize += HoldBarriersSet.size();

     for (auto xchannelp : ChannelInfo) {
         for (auto &xpair : xchannelp.second.CommittedDelta) {
             ++ret.CommitedEntries;
             ret.CommitedBlobIdsKnown += xpair.second.Created.size();
             ret.CommitedBlobIdsLeft += xpair.second.Deleted.size();
         }
         ret.CommitedEntriesBytes += sizeof(TLogoBlobID) *(ret.CommitedBlobIdsKnown + ret.CommitedBlobIdsLeft) + 96;
     }

     return ret;
}

namespace {
    void ValidateGCVector(ui64 tabletId, ui32 channel, const char* name, const TVector<TLogoBlobID>& vec) {
        for (size_t i = 0; i < vec.size(); ++i) {
            Y_ABORT_UNLESS(vec[i].TabletID() == tabletId,
                "Foreign blob %s in %s vector (tablet %" PRIu64 ", channel %" PRIu32 ")",
                vec[i].ToString().c_str(), name, tabletId, channel);
            Y_ABORT_UNLESS(vec[i].Channel() == channel,
                "Wrong channel blob %s in %s vector (tablet %" PRIu64 ", channel %" PRIu32 ")",
                vec[i].ToString().c_str(), name, tabletId, channel);
            if (i > 0) {
                Y_ABORT_UNLESS(vec[i-1] < vec[i],
                    "Out of order blobs %s and %s in %s vector (tablet %" PRIu64 ", channel %" PRIu32 ")",
                    vec[i-1].ToString().c_str(), vec[i].ToString().c_str(), name, tabletId, channel);
            }
        }
    }
}

void TExecutorGCLogic::TChannelInfo::SendCollectGarbageEntry(
            const TActorContext &ctx,
            TVector<TLogoBlobID> &&keep, TVector<TLogoBlobID> &&notKeep,
            ui64 tabletid, ui32 channel, ui32 bsgroup, ui32 generation)
{
    ValidateGCVector(tabletid, channel, "Keep", keep);
    ValidateGCVector(tabletid, channel, "DoNotKeep", notKeep);
    THolder<TEvBlobStorage::TEvCollectGarbage> ev =
        MakeHolder<TEvBlobStorage::TEvCollectGarbage>(
            tabletid,
            generation, GcCounter,
            channel, true,
            KnownGcBarrier.Generation, KnownGcBarrier.Step,
            keep.empty() ? nullptr : new TVector<TLogoBlobID>(std::move(keep)),
            notKeep.empty() ? nullptr : new TVector<TLogoBlobID>(std::move(notKeep)),
            TInstant::Max(),
            true);
    GcCounter += ev->PerGenerationCounterStepSize();
    SendToBSProxy(ctx, bsgroup, ev.Release());
    ++GcWaitFor;
}

void TExecutorGCLogic::TChannelInfo::SendCollectGarbage(TGCTime uncommittedTime, const TTabletStorageInfo *tabletStorageInfo, ui32 channel, ui32 generation, const TActorContext& ctx) {
    if (GcWaitFor > 0)
        return;

    TVector<TLogoBlobID> keep;
    TVector<TLogoBlobID> notKeep;

    TGCTime collectBarrier;

    for (auto it = CommittedDelta.begin(), end = CommittedDelta.end(); it != end; ++it) {
        if (it->first < uncommittedTime) {
            keep.insert(keep.end(), it->second.Created.begin(), it->second.Created.end());
            notKeep.insert(notKeep.end(), it->second.Deleted.begin(), it->second.Deleted.end());
            collectBarrier = it->first;
        } else
            break;
    }

    // The first barrier of gen:0 (zero entry) is special
    TGCTime zeroTime{ generation, 0 };
    if (KnownGcBarrier < zeroTime && collectBarrier < zeroTime && zeroTime <= uncommittedTime) {
        collectBarrier = zeroTime;
    }

    if (collectBarrier) {
        Sort(keep);
        Sort(notKeep);

        DeduplicateGCKeepVectors(&keep, &notKeep, generation, KnownGcBarrier.Step);

        CollectSent = collectBarrier;
        KnownGcBarrier = collectBarrier;

        const auto *channelInfo = tabletStorageInfo->ChannelInfo(channel);
        Y_ABORT_UNLESS(channelInfo);


        const ui32 lastCommitedGcBarrier = CommitedGcBarrier.Generation;
        const ui32 firstFlagGen = std::min<ui32>(keep.empty() ? Max<ui32>() : keep.front().Generation(), notKeep.empty() ? Max<ui32>() : notKeep.front().Generation());
        const auto *latestEntry = channelInfo->LatestEntry();

        if (lastCommitedGcBarrier >= latestEntry->FromGeneration && firstFlagGen >= latestEntry->FromGeneration) {
            // normal case, commit gc info for last entry only
            SendCollectGarbageEntry(ctx, std::move(keep), std::move(notKeep), tabletStorageInfo->TabletID, channel, latestEntry->GroupID, generation);
        } else {
        // bloated case - spread among different groups
            TMap<ui32, std::pair<TVector<TLogoBlobID>, TVector<TLogoBlobID>>> affectedGroups;

            { // mark every actual generation for update
                auto xit = UpperBound(channelInfo->History.begin(), channelInfo->History.end(), lastCommitedGcBarrier, TTabletChannelInfo::THistoryEntry::TCmp());
                if (xit != channelInfo->History.begin()) {
                    --xit;
                }
                for (; xit != channelInfo->History.end(); ++xit)
                    affectedGroups[xit->GroupID];
            }

            ui32 activeGen = Max<ui32>();
            ui32 activeGroup = Max<ui32>();
            TVector<TLogoBlobID> *vec = nullptr;

            for (const auto &blobId : keep) {
                if (activeGen != blobId.Generation()) {
                    activeGen = blobId.Generation();
                    activeGroup = channelInfo->GroupForGeneration(blobId.Generation());
                    vec = &affectedGroups[activeGroup].first;
                }

                vec->push_back(blobId);
            }

            activeGen = Max<ui32>();
            activeGroup = Max<ui32>();
            vec = nullptr;

            for (const auto &blobId : notKeep) {
                if (activeGen != blobId.Generation()) {
                    activeGen = blobId.Generation();
                    activeGroup = channelInfo->GroupForGeneration(blobId.Generation());
                    vec = &affectedGroups[activeGroup].second;
                }

                vec->push_back(blobId);
            }

            for (auto &xpair : affectedGroups) {
                SendCollectGarbageEntry(ctx, std::move(xpair.second.first), std::move(xpair.second.second), tabletStorageInfo->TabletID, channel, xpair.first, generation);
            }
        }
    }
}

void TExecutorGCLogic::TChannelInfo::OnCollectGarbageSuccess() {
    if (--GcWaitFor || !CollectSent)
        return;

    auto it = CommittedDelta.upper_bound(CollectSent);
    if (it != CommittedDelta.begin()) {
        CommittedDelta.erase(CommittedDelta.begin(), it);
    }

    CollectSent.Clear();
    CommitedGcBarrier = KnownGcBarrier;
}

void TExecutorGCLogic::TChannelInfo::OnCollectGarbageFailure() {
    CollectSent.Clear();
    --GcWaitFor;
}

}
}
