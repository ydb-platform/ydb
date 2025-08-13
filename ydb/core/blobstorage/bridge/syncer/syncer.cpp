#include "syncer.h"
#include "syncer_impl.h"
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/core/util/stlog.h>

namespace NKikimr::NBridge {

    TSyncerActor::TSyncerActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TGroupId sourceGroupId, TGroupId targetGroupId)
        : Info(std::move(info))
        , SourceGroupId(sourceGroupId)
        , TargetGroupId(targetGroupId)
    {
        Y_ABORT_UNLESS(Info);
        Y_ABORT_UNLESS(Info->IsBridged());
    }

    void TSyncerActor::Bootstrap() {
        LogId = TStringBuilder() << SelfId() << Info->GroupID << '{' << SourceGroupId << "->" << TargetGroupId << '}';

        const auto& state = Info->Group->GetBridgeGroupState();
        bool found = false;
        for (size_t i = 0; i < state.PileSize(); ++i) {
            const auto& pile = state.GetPile(i);
            const auto groupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
            if (groupId == SourceGroupId) {
                if (pile.GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                    Y_DEBUG_ABORT("TGroupState.TPile is not in SYNCED state for primary pile");
                    return Terminate("group is not synced for primary pile");
                }
                SourceGroupGeneration = pile.GetGroupGeneration();
            } else if (groupId == TargetGroupId) {
                Stage = pile.GetStage();
                if (Stage == NKikimrBridge::TGroupState::SYNCED) {
                    return Terminate(std::nullopt); // everything is already synced
                }
                TargetGroupGeneration = pile.GetGroupGeneration();
                found = true;
            }
        }
        if (!found) {
            Y_DEBUG_ABORT("target group not found in TGroupState");
            return Terminate("target group not found in TGroupState");
        }

        LogId = TStringBuilder() << SelfId() << Info->GroupID << '{' << SourceGroupId << "->" << TargetGroupId << '#'
            << NKikimrBridge::TGroupState::EStage_Name(Stage) << '}';

        // reset sync states to their original state and decide what to synchronize
        if (Stage != NKikimrBridge::TGroupState::BLOCKS) { // no blocks needed
            SourceState.SkipBlocksUpTo.emplace(TargetState.SkipBlocksUpTo.emplace(0));
        }
        // barriers are read at a special substage
        SourceState.SkipBarriersUpTo.emplace(TargetState.SkipBarriersUpTo.emplace(0, 0));
        if (Stage == NKikimrBridge::TGroupState::BLOCKS) { // blobs not needed only at the first stage
            SourceState.SkipBlobsUpTo.emplace(TargetState.SkipBlobsUpTo.emplace(Min<TLogoBlobID>()));
        }

        STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS00, "bootstrapping bridged blobstorage syncer", (LogId, LogId));

        Become(&TThis::StateFunc);

        // issue requests
        IssueAssimilateRequest(false);
        IssueAssimilateRequest(true);
    }

    void TSyncerActor::PassAway() {
        TActorBootstrapped::PassAway();
    }

    void TSyncerActor::Terminate(std::optional<TString> errorReason) {
        STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS04, "syncing finished", (LogId, LogId), (ErrorReason, errorReason));
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new NStorage::TEvNodeWardenNotifySyncerFinished(Info->GroupID,
            Info->GroupGeneration, SourceGroupId, TargetGroupId, std::move(errorReason)));
        PassAway();
    }

    void TSyncerActor::Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr ev) {
        auto& record = ev->Get()->Record;
        STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSSxx, "TEvControllerConfigResponse", (LogId, LogId), (Record, record));
        const auto& response = record.GetResponse();
        if (!response.GetSuccess()) {
            Terminate(TStringBuilder() << "failed to switch group state in BSC: " << response.GetErrorDescription());
        }
    }

    void TSyncerActor::Handle(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr ev) {
        auto& record = ev->Get()->Record;
        STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSSxx, "TEvNodeConfigInvokeOnRootResult", (LogId, LogId), (Record, record));
        if (record.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
            Terminate(TStringBuilder() << "failed to switch static group state: " << record.GetErrorReason());
        }
    }

    void TSyncerActor::DoMergeLoop() {
        if (SourceState.RequestInFlight || TargetState.RequestInFlight) {
            return; // nothing to merge yet
        }

        std::vector<TLogoBlobID> keepToIssue;
        std::vector<TLogoBlobID> doNotKeepToIssue;

        auto mergeBlocks = [&](auto *sourceItem, auto *targetItem, const auto& key) {
            STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS05, "merging block", (LogId, LogId), (SourceItem, sourceItem),
                (TargetItem, targetItem));
            // this operation is only possible while syncing blocks, so enforce it
            Y_ABORT_UNLESS(Stage == NKikimrBridge::TGroupState::BLOCKS);
            const auto& [tabletId] = key;
            std::optional<ui32> sourceGeneration = sourceItem
                ? std::make_optional(sourceItem->BlockedGeneration)
                : std::nullopt;
            std::optional<ui32> targetGeneration = targetItem
                ? std::make_optional(targetItem->BlockedGeneration)
                : std::nullopt;
            if (sourceGeneration < targetGeneration) {
                // synced group has lesser blocked generation for this tablet than unsynced one: we need to update
                // blocked generation in source group and this will lead to possible tablet restart
                Y_ABORT_UNLESS(targetGeneration);
                IssueQuery(false, std::make_unique<TEvBlobStorage::TEvBlock>(tabletId, *targetGeneration, TInstant::Max()));
            } else if (targetGeneration < sourceGeneration) {
                // we just need to update target group generation to current one in source group
                Y_ABORT_UNLESS(sourceGeneration);
                IssueQuery(true, std::make_unique<TEvBlobStorage::TEvBlock>(tabletId, *sourceGeneration, TInstant::Max()));
            }
        };

        auto mergeBarriers = [&](auto *sourceItem, auto *targetItem, const auto& /*key*/) {
            STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS06, "merging barrier", (LogId, LogId), (SourceItem, sourceItem),
                (TargetItem, targetItem));
            Y_ABORT_UNLESS(Stage == NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP);
            if (!sourceItem) {
                return;
            }
            auto issueCollectGarbage = [&](auto& item, auto *existingItem, bool hard) {
                if (existingItem) {
                    if (item.RecordGeneration == existingItem->RecordGeneration &&
                            item.PerGenerationCounter == existingItem->PerGenerationCounter &&
                            item.CollectStep == existingItem->CollectStep &&
                            item.CollectGeneration == existingItem->CollectGeneration) {
                        return; // nothing to update
                    }
                    const auto& existingKey = std::make_tuple(existingItem->RecordGeneration, existingItem->PerGenerationCounter);
                    const auto& newKey = std::make_tuple(item.RecordGeneration, item.PerGenerationCounter);
                    if (existingKey < newKey) {
                        // update, newer key received
                    } else if (newKey < existingKey) {
                        // we have newer key, maybe it was written just now
                        return;
                    } else {
                        STLOG(PRI_CRIT, BS_BRIDGE_SYNC, BRSSxx, "incorrect barrier",
                            (Target.RecordGeneration, existingItem->RecordGeneration),
                            (Target.PerGenerationCounter, existingItem->PerGenerationCounter),
                            (Target.CollectGeneration, existingItem->CollectGeneration),
                            (Target.CollectStep, existingItem->CollectStep),
                            (Source.RecordGeneration, item.RecordGeneration),
                            (Source.PerGenerationCounter, item.PerGenerationCounter),
                            (Source.CollectGeneration, item.CollectGeneration),
                            (Source.CollectStep, item.CollectStep),
                            (Hard, hard));
                    }
                }

                if (item.RecordGeneration || item.PerGenerationCounter || item.CollectGeneration || item.CollectStep) {
                    IssueQuery(true, std::make_unique<TEvBlobStorage::TEvCollectGarbage>(sourceItem->TabletId,
                        item.RecordGeneration, item.PerGenerationCounter, sourceItem->Channel, true,
                        item.CollectGeneration, item.CollectStep, nullptr, nullptr, TInstant::Max(), false, hard, true));
                }
            };
            issueCollectGarbage(sourceItem->Soft, targetItem ? &targetItem->Soft : nullptr, false);
            issueCollectGarbage(sourceItem->Hard, targetItem ? &targetItem->Hard : nullptr, true);
        };

        auto mergeBlobs = [&](auto *sourceItem, auto *targetItem, const auto& key) {
            STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS03, "merging blob", (LogId, LogId), (SourceItem, sourceItem),
                (TargetItem, targetItem));

            const auto& [blobId] = key;

            switch (Stage) {
                case NKikimrBridge::TGroupState::WRITE_KEEP:
                    if (sourceItem && sourceItem->Keep && (!targetItem || !targetItem->Keep)) {
                        keepToIssue.push_back(blobId);
                    }
                    break;

                case NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP:
                    if (sourceItem && sourceItem->Keep && (!targetItem || !targetItem->Keep)) {
                        keepToIssue.push_back(blobId);
                    }
                    if ((!sourceItem || sourceItem->DoNotKeep) && targetItem && !targetItem->DoNotKeep) {
                        doNotKeepToIssue.push_back(blobId);
                    }
                    break;

                case NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP_DATA:
                    if (targetItem) {
                        // a target item already exists, we need to check its placement by issuing index query; when we have to
                        // adjust keep flags
                        IssueQuery(true, std::make_unique<TEvBlobStorage::TEvGet>(blobId, 0, 0, TInstant::Max(),
                            NKikimrBlobStorage::FastRead, true, true));
                    } else {
                        // no target item exists at all, we have to read and then write it
                        IssueQuery(false, std::make_unique<TEvBlobStorage::TEvGet>(blobId, 0, 0, TInstant::Max(),
                            NKikimrBlobStorage::FastRead));
                    }
                    break;

                default:
                    Y_ABORT();
            }
        };

        auto finish = [&] {
            std::ranges::sort(keepToIssue);
            std::ranges::sort(doNotKeepToIssue);
            size_t keep = 0;
            size_t doNotKeep = 0;
            while (keep < keepToIssue.size() || doNotKeep < doNotKeepToIssue.size()) {
                ui64 tabletId;
                if (keep == keepToIssue.size()) {
                    tabletId = doNotKeepToIssue[doNotKeep].TabletID();
                } else if (doNotKeep == doNotKeepToIssue.size()) {
                    tabletId = keepToIssue[keep].TabletID();
                } else {
                    tabletId = Min(keepToIssue[keep].TabletID(), doNotKeepToIssue[doNotKeep].TabletID());
                }
                std::vector<TLogoBlobID> tempKeep;
                auto dkv = std::make_unique<TVector<TLogoBlobID>>();
                while (keep < keepToIssue.size() && keepToIssue[keep].TabletID() == tabletId) {
                    tempKeep.push_back(keepToIssue[keep++]);
                }
                while (doNotKeep < doNotKeepToIssue.size() && doNotKeepToIssue[doNotKeep].TabletID() == tabletId) {
                    dkv->push_back(doNotKeepToIssue[doNotKeep++]);
                }
                auto kv = std::make_unique<TVector<TLogoBlobID>>();
                std::ranges::set_difference(tempKeep, *dkv, std::back_inserter(*kv)); // do not keep flag overrides keep
                IssueQuery(true, std::make_unique<TEvBlobStorage::TEvCollectGarbage>(tabletId, Max<ui32>(),
                    0u, false, 0u, 0u, kv->empty() ? nullptr : kv.release(), dkv->empty() ? nullptr : dkv.release(),
                    TInstant::Max()));
            }
        };

#define MERGE(NAME) \
        if (!DoMergeEntities(SourceState.NAME, TargetState.NAME, SourceState.NAME##Finished, TargetState.NAME##Finished, \
                merge##NAME, LastMerged##NAME)) { \
            return finish(); \
        }
        MERGE(Blocks)
        MERGE(Barriers)
        MERGE(Blobs)
#undef MERGE

        finish();
        Finished = true;
        CheckIfDone();
    }

    void TSyncerActor::CheckIfDone() {
        if (Finished && Payloads.empty()) {
            if (Errors) {
                Terminate("errors encountered during sync");
            } else if (Stage == NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP && BarriersStage == 0) {
                // second stage of processing
                ++BarriersStage;

                // now process solely barriers
                std::ranges::fill(GroupAssimilateState, TAssimilateState());
                SourceState.SkipBlocksUpTo.emplace(TargetState.SkipBlocksUpTo.emplace(0));
                SourceState.SkipBlobsUpTo.emplace(TargetState.SkipBlobsUpTo.emplace(Min<TLogoBlobID>()));
                LastMergedBlocks.reset();
                LastMergedBarriers.reset();
                LastMergedBlobs.reset();

                // issue next assimilate queries for barriers
                IssueAssimilateRequest(false);
                IssueAssimilateRequest(true);

                // clear finished flag
                Finished = false;
            } else {
                // sync stage finished successfully for this group
                Terminate(std::nullopt);
            }
        }
    }

    template<typename T, typename TCallback, typename TKey>
    bool TSyncerActor::DoMergeEntities(std::deque<T>& source, std::deque<T>& target, bool sourceFinished, bool targetFinished,
            TCallback&& merge, std::optional<TKey>& lastMerged) {
        while ((!source.empty() || sourceFinished) && (!target.empty() || targetFinished)) {
            auto *sourceItem = source.empty() ? nullptr : &source.front();
            auto *targetItem = target.empty() ? nullptr : &target.front();
            if (!sourceItem && !targetItem) { // both queues have exhausted
                Y_ABORT_UNLESS(sourceFinished && targetFinished);
                return true;
            }
            const TKey& key = sourceItem && (!targetItem || targetItem->GetKey() < sourceItem->GetKey())
                ? sourceItem->GetKey()
                : targetItem->GetKey();
            if (sourceItem && targetItem) {
                if (sourceItem->GetKey() < targetItem->GetKey()) {
                    sourceItem = nullptr;
                } else if (targetItem->GetKey() < sourceItem->GetKey()) {
                    targetItem = nullptr;
                }
            }
            Y_ABORT_UNLESS(!lastMerged || key < lastMerged);
            Y_DEBUG_ABORT_UNLESS(!sourceItem || sourceItem->GetKey() == key);
            Y_DEBUG_ABORT_UNLESS(!targetItem || targetItem->GetKey() == key);
            lastMerged.emplace(key);
            merge(sourceItem, targetItem, key);
            if (sourceItem) {
                source.pop_front();
            }
            if (targetItem) {
                target.pop_front();
            }
        }

        if (!sourceFinished && source.empty()) {
            IssueAssimilateRequest(false);
        }
        if (!targetFinished && target.empty()) {
            IssueAssimilateRequest(true);
        }

        return false;
    }

    void TSyncerActor::IssueQuery(bool toTargetGroup, std::unique_ptr<IEventBase> ev, TQueryPayload payload) {
        switch (ev->Type()) {
#define MSG(TYPE) \
            case TEvBlobStorage::TYPE: { \
                auto& msg = static_cast<TEvBlobStorage::T##TYPE&>(*ev); \
                STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS07, #TYPE, (LogId, LogId), (ToTargetGroup, toTargetGroup), \
                    (Msg, msg), (QueriesInFlight, QueriesInFlight), \
                    (PendingQueries.size, PendingQueries.size()), (MaxQueriesInFlight, MaxQueriesInFlight), \
                    (Payloads.size, Payloads.size())); \
                msg.ForceGroupGeneration.emplace(toTargetGroup ? TargetGroupGeneration : SourceGroupGeneration); \
                break; \
            }

            MSG(EvAssimilate)
            MSG(EvBlock)
            MSG(EvCollectGarbage)
            MSG(EvPut)
            MSG(EvGet)

            default:
                Y_ABORT();
#undef MSG
        }

        const ui64 cookie = NextCookie++;
        const auto [it, inserted] = Payloads.emplace(cookie, std::move(payload));
        Y_ABORT_UNLESS(inserted);

        std::unique_ptr<IEventHandle> handle(CreateEventForBSProxy(SelfId(),
            toTargetGroup ? TargetGroupId : SourceGroupId, ev.release(), cookie));

        if (QueriesInFlight < MaxQueriesInFlight) {
            TActivationContext::Send(handle.release());
            ++QueriesInFlight;
        } else {
            PendingQueries.push_back(std::move(handle));
        }
    }

    void TSyncerActor::Handle(TEvBlobStorage::TEvBlockResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS09, "TEvBlockResult", (LogId, LogId), (Msg, msg));
        const NKikimrProto::EReplyStatus status = msg.Status;
        const bool success = status == NKikimrProto::OK
            || status == NKikimrProto::ALREADY
            || status == NKikimrProto::BLOCKED;
        OnQueryFinished(ev->Cookie, success);
        CheckIfDone();
    }

    void TSyncerActor::Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS10, "TEvCollectGarbageResult", (LogId, LogId), (Msg, msg));
        const NKikimrProto::EReplyStatus status = msg.Status;
        const bool success = status == NKikimrProto::OK;
        OnQueryFinished(ev->Cookie, success);
        CheckIfDone();
    }

    void TSyncerActor::Handle(TEvBlobStorage::TEvPutResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS11, "TEvPutResult", (LogId, LogId), (Msg, msg));
        const NKikimrProto::EReplyStatus status = msg.Status;
        const bool success = status == NKikimrProto::OK;
        OnQueryFinished(ev->Cookie, success);
        CheckIfDone();
    }

    void TSyncerActor::Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
        auto& msg = *ev->Get();
        std::optional<TString> errorReason;
        if (msg.Status != NKikimrProto::OK) {
            errorReason = TStringBuilder() << "TEvGet failed Status# " << msg.Status
                << " ErrorReason# " << msg.ErrorReason;
        } else if (msg.ResponseSz != 1) {
            errorReason = "TEvGet error: ResponseSz incorrect";
            Y_DEBUG_ABORT();
        } else if (auto& r = msg.Responses[0]; msg.GroupId == SourceGroupId.GetRawId()) { // it was a data query for copying
            if (r.Status == NKikimrProto::OK) {
                // rewrite this blob with keep flag, if set
                IssueQuery(true, std::make_unique<TEvBlobStorage::TEvPut>(r.Id, TRcBuf(r.Buffer), TInstant::Max(),
                    NKikimrBlobStorage::TabletLog, TEvBlobStorage::TEvPut::TacticDefault, r.Keep && !r.DoNotKeep,
                    /*ignoreBlocks=*/ true), {});
            } else if (r.Status == NKikimrProto::NODATA) {
                // this blob may have vanished, it's okay if we couldn't have read it; there was no matching target item
                // so we don't need to issue any keep flags
            } else if (r.Status == NKikimrProto::ERROR) {
                // an error occured when reading this exact blob
                errorReason = "TEvGet from source group failed with ERROR";
            } else {
                Y_DEBUG_ABORT();
                errorReason = TStringBuilder() << "TEvGet from source group failed with " << r.Status;
            }
        } else if (msg.GroupId == TargetGroupId.GetRawId()) { // it was an index query for checking target blob
            if (r.Status == NKikimrProto::OK) {
                // blob exists and okay; its redundancy has been restored; all we need is to check keep flag and issue
                // new value unless it is correct
                //if (!r.Keep && expected) { FIXME TODO
                //}
            } else if (r.Status == NKikimrProto::NODATA) {
                // we have to query this blob and do full rewrite -- there was no data for it
                IssueQuery(false, std::make_unique<TEvBlobStorage::TEvGet>(r.Id, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::FastRead));
            } else if (r.Status == NKikimrProto::ERROR) {
                errorReason = "TEvGet from target group failed with ERROR";
            } else {
                Y_DEBUG_ABORT();
                errorReason = TStringBuilder() << "TEvGet from target group failed with " << r.Status;
            }
        } else {
            Y_DEBUG_ABORT();
            errorReason = "TEvGetResult from unexpected group";
        }
        STLOG(errorReason ? PRI_NOTICE : PRI_DEBUG, BS_BRIDGE_SYNC, BRSS08, "TEvGetResult", (LogId, LogId), (Msg, msg),
            (ErrorReason, errorReason));
        OnQueryFinished(ev->Cookie, !errorReason);
        CheckIfDone();
    }

    TSyncerActor::TQueryPayload TSyncerActor::OnQueryFinished(ui64 cookie, bool success) {
        const auto it = Payloads.find(cookie);
        Y_ABORT_UNLESS(it != Payloads.end());
        TQueryPayload res = std::move(it->second);
        Payloads.erase(it);

        Errors |= !success;

        if (PendingQueries.empty()) {
            --QueriesInFlight;
        } else {
            Y_ABORT_UNLESS(QueriesInFlight == MaxQueriesInFlight);
            TActivationContext::Send(PendingQueries.front().release());
            PendingQueries.pop_front();
        }

        return res;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Assimilator operation

    void TSyncerActor::IssueAssimilateRequest(bool toTargetGroup) {
        TAssimilateState& state = GroupAssimilateState[toTargetGroup];

        Y_ABORT_UNLESS(state.BlocksFinished >= state.BarriersFinished);
        Y_ABORT_UNLESS(state.BarriersFinished >= state.BlobsFinished);
        Y_ABORT_UNLESS(!state.BlobsFinished);

        STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSS02, "issuing assimilate request", (LogId, LogId), (ToTargetGroup, toTargetGroup),
            (SkipBlocksUpTo, state.SkipBlocksUpTo ? ToString(*state.SkipBlocksUpTo) : "<none>"),
            (SkipBarriersUpTo, state.SkipBarriersUpTo ? TString(TStringBuilder() << '[' <<
                std::get<0>(*state.SkipBarriersUpTo) << ':' << (int)std::get<1>(*state.SkipBarriersUpTo) << ']') : "<none>"),
            (SkipBlobsUpTo, state.SkipBlobsUpTo ? state.SkipBlobsUpTo->ToString() : "<none>"));

        IssueQuery(toTargetGroup, std::make_unique<TEvBlobStorage::TEvAssimilate>(
            state.SkipBlocksUpTo, state.SkipBarriersUpTo, state.SkipBlobsUpTo,
            /*ignoreDecommitState=*/ true, /*reverse=*/ true),
            TQueryPayload{
                .ToTargetGroup = toTargetGroup,
            });

        Y_ABORT_UNLESS(!state.RequestInFlight);
        state.RequestInFlight = true;
    }

    void TSyncerActor::Handle(TEvBlobStorage::TEvAssimilateResult::TPtr ev) {
        auto& msg = *ev->Get();

        if (msg.Status != NKikimrProto::OK) {
            return Terminate(TStringBuilder() << "TEvAssimilate failed: " << ev->Get()->ErrorReason);
        }

        TQueryPayload payload = OnQueryFinished(ev->Cookie, true);
        TAssimilateState& state = GroupAssimilateState[payload.ToTargetGroup];

        STLOG(PRI_DEBUG, BS_BRIDGE_SYNC, BRSSxx, "got assimilate result", (LogId, LogId), (Status, msg.Status),
            (Blocks.size, msg.Blocks.size()), (Barriers.size, msg.Barriers.size()), (Blobs.size, msg.Blobs.size()),
            (ToTargetGroup, payload.ToTargetGroup));

        Y_ABORT_UNLESS(state.RequestInFlight);
        state.RequestInFlight = false;

#define CHECK(PREV_FINISHED, WHAT, NO_NEXT, KEY) \
        if (bool& finished = state.WHAT##Finished; PREV_FINISHED && !finished) { \
            if (msg.WHAT.empty() || !(NO_NEXT)) { \
                finished = true; \
                state.Skip##WHAT##UpTo.emplace(); \
            } else { \
                state.Skip##WHAT##UpTo.emplace(msg.WHAT.back().KEY); \
            } \
        } else { \
            Y_ABORT_UNLESS(msg.WHAT.empty()); \
        } \
        if (state.WHAT.empty()) { \
            state.WHAT = std::move(msg.WHAT); \
        } else { \
            state.WHAT.insert(state.WHAT.end(), msg.WHAT.begin(), msg.WHAT.end()); \
        }

        //    PREV_FINISHED           WHAT      NO_NEXT                                    KEY
        CHECK(true,                   Blocks,   msg.Barriers.empty() && msg.Blobs.empty(), TabletId)
        CHECK(state.BlocksFinished,   Barriers, msg.Blobs.empty(),                         GetKey())
        CHECK(state.BarriersFinished, Blobs,    true,                                      Id      )
#undef CHECK

        DoMergeLoop();
    }

    STRICT_STFUNC(TSyncerActor::StateFunc,
        hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle)
        hFunc(TEvBlobStorage::TEvAssimilateResult, Handle)
        hFunc(TEvBlobStorage::TEvBlockResult, Handle)
        hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle)
        hFunc(TEvBlobStorage::TEvPutResult, Handle)
        hFunc(TEvBlobStorage::TEvGetResult, Handle)
        hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, Handle)
        cFunc(TEvents::TSystem::Poison, PassAway)
    )

    IActor *CreateSyncerActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TGroupId sourceGroupId, TGroupId targetGroupId) {
        return new TSyncerActor(std::move(info), sourceGroupId, targetGroupId);
    }

} // NKikimr::NBridge
