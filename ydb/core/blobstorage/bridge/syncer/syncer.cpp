#include "syncer.h"
#include "syncer_impl.h"
#include <ydb/core/util/stlog.h>

namespace NKikimr::NStorage::NBridge {

    TSyncerActor::TSyncerActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TBridgePileId targetPileId)
        : Info(std::move(info))
        , TargetPileId(targetPileId)
    {
        Y_ABORT_UNLESS(!Info || Info->IsBridged());
    }

    void TSyncerActor::Bootstrap() {
        LogId = TStringBuilder() << SelfId();
        STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS00, "bootstrapping bridged blobstorage syncer", (LogId, LogId),
            (TargetPileId, TargetPileId));
        Become(&TThis::StateFunc);
        if (Info) {
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(true));
        }
    }

    void TSyncerActor::PassAway() {
        TActorBootstrapped::PassAway();
    }

    void TSyncerActor::Handle(TEvBlobStorage::TEvConfigureProxy::TPtr ev) {
        if (!Info) {
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(true));
        }
        Info = std::move(ev->Get()->Info);
        Y_ABORT_UNLESS(Info);
        Y_ABORT_UNLESS(Info->IsBridged());
    }

    void TSyncerActor::Terminate(std::optional<TString> errorReason) {
        STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS04, "syncing finished", (LogId, LogId), (ErrorReason, errorReason));
        PassAway();
    }

    void TSyncerActor::Handle(TEvNodeWardenStorageConfig::TPtr ev) {
        const bool initial = !BridgeInfo;
        BridgeInfo = ev->Get()->BridgeInfo;
        Y_ABORT_UNLESS(BridgeInfo);
        const auto& targetPile = *BridgeInfo->GetPile(TargetPileId);
        if (targetPile.State != NKikimrBridge::TClusterState::NOT_SYNCHRONIZED) {
            // there is absolutely no need in synchronization: pile is either marked synchronized (what would be strange),
            // or it is disconnected; we terminate in either case
            return Terminate("target pile is not NOT_SYNCHRONIZED anymore");
        }
        if (!initial && BridgeInfo->GetPile(SourcePileId)->State != NKikimrBridge::TClusterState::SYNCHRONIZED) {
            return Terminate("source pile is not SYNCHRONIZED anymore");
        }
        if (initial) {
            InitiateSync();
        }
    }

    void TSyncerActor::InitiateSync() {
        // remember our source pile (primary one) on first start; actually, we can pick any of SYNCHRONIZED
        Y_ABORT_UNLESS(BridgeInfo->PrimaryPile->State == NKikimrBridge::TClusterState::SYNCHRONIZED);
        SourcePileId = BridgeInfo->PrimaryPile->BridgePileId;
        const auto& groups = Info->GetBridgeGroupIds();
        Y_ABORT_UNLESS(groups.size() == std::size(BridgeInfo->Piles));
        SourceGroupId = groups[SourcePileId.GetRawId()];
        TargetGroupId = groups[TargetPileId.GetRawId()];
        LogId = TStringBuilder() << LogId << '{' << SourceGroupId << "->" << TargetGroupId << '}';
        SourceState = &GroupAssimilateState[SourceGroupId];
        TargetState = &GroupAssimilateState[TargetGroupId];
        IssueAssimilateRequest(SourceGroupId);
        IssueAssimilateRequest(TargetGroupId);
        STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS01, "initiating sync", (LogId, LogId), (SourceGroupId, SourceGroupId),
            (TargetGroupId, TargetGroupId));
    }

    void TSyncerActor::DoMergeLoop() {
        if (SourceState->RequestInFlight || TargetState->RequestInFlight) {
            return; // nothing to merge yet
        }

        auto mergeBlocks = [&](auto *sourceItem, auto *targetItem) {
            STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS05, "merging block", (LogId, LogId), (SourceItem, sourceItem),
                (TargetItem, targetItem));
        };

        auto mergeBarriers = [&](auto *sourceItem, auto *targetItem) {
            STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS06, "merging barrier", (LogId, LogId), (SourceItem, sourceItem),
                (TargetItem, targetItem));
        };

        auto mergeBlobs = [&](auto *sourceItem, auto *targetItem) {
            STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS03, "merging blob", (LogId, LogId), (SourceItem, sourceItem),
                (TargetItem, targetItem));
        };

#define MERGE(NAME) \
        if (!DoMergeEntities(SourceState->NAME, TargetState->NAME, SourceState->NAME##Finished, TargetState->NAME##Finished, merge##NAME)) { \
            return; \
        }
        MERGE(Blocks)
        MERGE(Barriers)
        MERGE(Blobs)
#undef MERGE
    }

    template<typename T, typename TCallback>
    bool TSyncerActor::DoMergeEntities(std::deque<T>& source, std::deque<T>& target, bool sourceFinished, bool targetFinished,
            TCallback&& merge) {
        while ((!source.empty() || sourceFinished) && (!target.empty() || targetFinished)) {
            auto *sourceItem = source.empty() ? nullptr : &source.front();
            auto *targetItem = target.empty() ? nullptr : &target.front();
            if (!sourceItem && !targetItem) { // both queues have exhausted
                Y_ABORT_UNLESS(sourceFinished && targetFinished);
                return true;
            } else if (sourceItem && targetItem) { // we have items in both queues, have to pick one according to key
                const auto& sourceKey = sourceItem->GetKey();
                const auto& targetKey = targetItem->GetKey();
                if (sourceKey < targetKey) {
                    targetItem = nullptr;
                } else if (targetKey < sourceKey) {
                    sourceItem = nullptr;
                }
            }
            merge(sourceItem, targetItem);
            if (sourceItem) {
                source.pop_front();
            }
            if (targetItem) {
                target.pop_front();
            }
        }

        if (!sourceFinished && source.empty()) {
            IssueAssimilateRequest(SourceGroupId);
        }
        if (!targetFinished && target.empty()) {
            IssueAssimilateRequest(TargetGroupId);
        }

        return false;
    }

    void TSyncerActor::IssueAssimilateRequest(TGroupId groupId) {
        const auto it = GroupAssimilateState.find(groupId);
        Y_ABORT_UNLESS(it != GroupAssimilateState.end());
        TAssimilateState& state = it->second;

        Y_ABORT_UNLESS(!state.BlocksFinished || !state.BarriersFinished || !state.BlobsFinished);

        STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS02, "issuing assimilate request", (LogId, LogId), (GroupId, groupId),
            (SkipBlocksUpTo, state.SkipBlocksUpTo ? ToString(*state.SkipBlocksUpTo) : "<none>"),
            (SkipBarriersUpTo, state.SkipBarriersUpTo ? TString(TStringBuilder() << '[' <<
                std::get<0>(*state.SkipBarriersUpTo) << ':' << (int)std::get<1>(*state.SkipBarriersUpTo) << ']') : "<none>"),
            (SkipBlobsUpTo, state.SkipBlobsUpTo ? state.SkipBlobsUpTo->ToString() : "<none>"));

        SendToBSProxy(SelfId(), it->first, new TEvBlobStorage::TEvAssimilate(state.SkipBlocksUpTo, state.SkipBarriersUpTo,
            state.SkipBlobsUpTo, /*ignoreDecommitState=*/ true, /*reverse=*/ true), groupId.GetRawId());

        Y_ABORT_UNLESS(!state.RequestInFlight);
        state.RequestInFlight = true;
    }

    void TSyncerActor::Handle(TEvBlobStorage::TEvAssimilateResult::TPtr ev) {
        const auto groupId = TGroupId::FromValue(ev->Cookie);

        const auto it = GroupAssimilateState.find(groupId);
        Y_ABORT_UNLESS(it != GroupAssimilateState.end());
        TAssimilateState& state = it->second;

        Y_ABORT_UNLESS(state.RequestInFlight);
        state.RequestInFlight = false;

        auto& msg = *ev->Get();

        if (state.BlocksFinished) {
            Y_ABORT_UNLESS(msg.Blocks.empty());
        } else if (msg.Blocks.empty() || !msg.Barriers.empty() || !msg.Blobs.empty()) {
            state.BlocksFinished = true;
            state.SkipBlocksUpTo.emplace(); // the last value as we are going reverse
        } else {
            state.SkipBlocksUpTo.emplace(msg.Blocks.back().TabletId);
        }

        if (state.BarriersFinished) {
            Y_ABORT_UNLESS(state.BlocksFinished);
            Y_ABORT_UNLESS(msg.Barriers.empty());
        } else if (msg.Barriers.empty() || !msg.Blobs.empty()) {
            state.BarriersFinished = true;
            state.SkipBarriersUpTo.emplace(); // the same logic for barriers
        } else {
            auto& lastBarrier = msg.Barriers.back();
            state.SkipBarriersUpTo.emplace(lastBarrier.TabletId, lastBarrier.Channel);
        }

        if (state.BlobsFinished) {
            Y_ABORT("unexpected state");
        } else if (msg.Blobs.empty()) {
            state.BlobsFinished = true;
        } else {
            state.SkipBlobsUpTo.emplace(msg.Blobs.back().Id);
        }

        if (state.Blocks.empty()) {
            state.Blocks = std::move(msg.Blocks);
        } else {
            state.Blocks.insert(state.Blocks.end(), msg.Blocks.begin(), msg.Blocks.end());
        }

        if (state.Barriers.empty()) {
            state.Barriers = std::move(msg.Barriers);
        } else {
            state.Barriers.insert(state.Barriers.end(), msg.Barriers.begin(), msg.Barriers.end());
        }

        if (state.Blobs.empty()) {
            state.Blobs = std::move(msg.Blobs);
        } else {
            state.Blobs.insert(state.Blobs.end(), msg.Blobs.begin(), msg.Blobs.end());
        }

        DoMergeLoop();
    }

    STRICT_STFUNC(TSyncerActor::StateFunc,
        hFunc(TEvBlobStorage::TEvConfigureProxy, Handle)
        hFunc(TEvBlobStorage::TEvAssimilateResult, Handle)
        hFunc(TEvNodeWardenStorageConfig, Handle)
        cFunc(TEvents::TSystem::Poison, PassAway)
    )

    IActor *CreateSyncerActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TBridgePileId targetPileId) {
        return new TSyncerActor(std::move(info), targetPileId);
    }

} // NKikimr::NStorage::NBridge
