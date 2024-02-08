#include "data.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    void TData::CommitTrash(void *cookie) {
        auto [first, last] = InFlightTrash.equal_range(cookie);
        std::unordered_set<TRecordsPerChannelGroup*> records;
        for (auto it = first; it != last; ++it) {
            auto& record = GetRecordsPerChannelGroup(it->second);
            record.MoveToTrash(this, it->second);
            records.insert(&record);
            InFlightTrashSize -= it->second.BlobSize();
        }
        InFlightTrash.erase(first, last);
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_IN_FLIGHT_TRASH_SIZE] = InFlightTrashSize;

        for (TRecordsPerChannelGroup *record : records) {
            record->CollectIfPossible(this);
        }
    }

    void TData::HandleTrash(TRecordsPerChannelGroup& record) {
        const ui32 generation = Self->Executor()->Generation();
        THashMap<ui32, std::unique_ptr<TEvBlobDepot::TEvPushNotify>> outbox;

        Y_DEBUG_ABORT_UNLESS(!record.CollectGarbageRequestsInFlight);
        Y_DEBUG_ABORT_UNLESS(record.Collectible(this));
        Y_DEBUG_ABORT_UNLESS(Loaded); // we must have correct Trash and Used values

        // check if we can issue a hard barrier
        const TGenStep hardGenStep = record.GetHardGenStep(this);
        Y_ABORT_UNLESS(record.HardGenStep <= hardGenStep); // ensure hard barrier does not decrease
        if (record.HardGenStep < hardGenStep) {
            auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(Self->TabletID(), generation,
                record.PerGenerationCounter++, record.Channel, true, hardGenStep.Generation(), hardGenStep.Step(),
                nullptr, nullptr, TInstant::Max(), false /*isMultiCollectAllowed*/, true /*hard*/);

            std::optional<TLogoBlobID> minTrashId = record.Trash.empty()
                ? std::nullopt
                : std::make_optional(*record.Trash.begin());
            std::optional<TLogoBlobID> maxTrashId = record.Trash.empty()
                ? std::nullopt
                : std::make_optional(*--record.Trash.end());

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT85, "issuing hard barrier TEvCollectGarbage", (Id, Self->GetLogId()),
                (Channel, int(record.Channel)), (GroupId, record.GroupId), (Msg, ev->ToString()),
                (HardGenStep, hardGenStep), (MinTrashId, minTrashId), (MaxTrashId, maxTrashId));

            ++record.CollectGarbageRequestsInFlight;

            const ui64 id = ++LastCollectCmdId;
            const ui64 queryId = RandomNumber<ui64>();
            CollectCmds.emplace(id, TCollectCmd{.QueryId = queryId, .GroupId = record.GroupId, .Hard = true,
                .GenStep = hardGenStep});

            SendToBSProxy(Self->SelfId(), record.GroupId, ev.release(), id);
        }

        Y_ABORT_UNLESS(generation >= 1);
        const TGenStep barrierGenStep(generation - 1, Max<ui32>());

        if (record.Trash.empty() && record.IssuedGenStep == barrierGenStep) {
            return; // no trash to collect with soft barrier
        }

        Y_ABORT_UNLESS(record.Channel < Self->Channels.size());
        auto& channel = Self->Channels[record.Channel];

        const TGenStep trashGenStep = record.Trash.empty() ? TGenStep() : TGenStep(*--record.Trash.end());
        TGenStep nextGenStep = Max(record.IssuedGenStep, trashGenStep, barrierGenStep);
        std::set<TLogoBlobID>::iterator trashEndIter = record.Trash.end();

        // step we are going to invalidate (including blobs with this one)
        TBlobSeqId leastExpectedBlobId = channel.GetLeastExpectedBlobId(generation);
        const ui32 invalidatedStep = nextGenStep.Step(); // the step we want to invalidate and garbage collect

        if (TGenStep(leastExpectedBlobId) <= nextGenStep) {
            // remove invalidated step from allocations
            auto blobSeqId = TBlobSeqId::FromSequentalNumber(record.Channel, generation, channel.NextBlobSeqId);
            Y_ABORT_UNLESS(record.LastConfirmedGenStep < TGenStep(blobSeqId));
            if (blobSeqId.Step <= invalidatedStep) {
                blobSeqId.Step = invalidatedStep + 1;
                blobSeqId.Index = 0;
                channel.NextBlobSeqId = blobSeqId.ToSequentialNumber();
            }

            // recalculate least expected blob id -- it may change if the given id set was empty
            leastExpectedBlobId = channel.GetLeastExpectedBlobId(generation);
        }

        if (TGenStep(leastExpectedBlobId) <= nextGenStep) {
            // issue notifications to agents -- we want to trim their ids
            for (auto& [agentId, agent] : Self->Agents) {
                const auto [it, inserted] = agent.InvalidatedStepInFlight.emplace(record.Channel, invalidatedStep);
                if (inserted || it->second < invalidatedStep) {
                    it->second = invalidatedStep;

                    if (agent.Connection) {
                        auto& ev = outbox[agentId];
                        if (!ev) {
                            ev.reset(new TEvBlobDepot::TEvPushNotify);
                        }
                        auto *item = ev->Record.AddInvalidatedSteps();
                        item->SetChannel(record.Channel);
                        item->SetGeneration(generation);
                        item->SetInvalidatedStep(invalidatedStep);
                    }
                }
            }

            // adjust the barrier to keep it safe now (till we trim ids)
            const TLogoBlobID maxId(Self->TabletID(), leastExpectedBlobId.Generation,
                leastExpectedBlobId.Step, record.Channel, 0, 0);
            trashEndIter = record.Trash.lower_bound(maxId);
            nextGenStep = Max(record.IssuedGenStep,
                barrierGenStep,
                trashEndIter != record.Trash.begin()
                    ? TGenStep(*std::prev(trashEndIter))
                    : TGenStep());
        }

        Y_ABORT_UNLESS(nextGenStep < TGenStep(leastExpectedBlobId));

        TVector<TLogoBlobID> keep;
        TVector<TLogoBlobID> doNotKeep;
        std::vector<TLogoBlobID> trashInFlight;

        for (auto it = record.Trash.begin(); it != trashEndIter; ++it) {
            if (const TGenStep genStep(*it); genStep <= hardGenStep) {
                continue; // this blob will be deleted by hard barrier, no need for do-not-keep flag
            } else if (genStep <= record.IssuedGenStep) {
                doNotKeep.push_back(*it);
            } else if (nextGenStep < genStep) {
                Y_ABORT();
            }
            trashInFlight.push_back(*it);
        }

        const TLogoBlobID keepFrom(Self->TabletID(), record.LastConfirmedGenStep.Generation(),
            record.LastConfirmedGenStep.Step(), record.Channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie,
            TLogoBlobID::MaxPartId, TLogoBlobID::MaxCrcMode);
        for (auto it = record.Used.upper_bound(keepFrom); it != record.Used.end() && TGenStep(*it) <= nextGenStep; ++it) {
            Y_ABORT_UNLESS(record.LastConfirmedGenStep < TGenStep(*it));
            Y_ABORT_UNLESS(hardGenStep < TGenStep(*it));
            keep.push_back(*it);
        }

        const bool collect = nextGenStep > record.LastConfirmedGenStep;
        Y_ABORT_UNLESS(nextGenStep >= record.IssuedGenStep);

        if (trashInFlight.empty()) {
            Y_ABORT_UNLESS(keep.empty() || record.IssuedGenStep != nextGenStep); // nothing to do here
        } else {
            auto keep_ = keep ? std::make_unique<TVector<TLogoBlobID>>(std::move(keep)) : nullptr;
            auto doNotKeep_ = doNotKeep ? std::make_unique<TVector<TLogoBlobID>>(std::move(doNotKeep)) : nullptr;

            auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(Self->TabletID(), generation,
                record.PerGenerationCounter, record.Channel, collect, nextGenStep.Generation(), nextGenStep.Step(),
                keep_.get(), doNotKeep_.get(), TInstant::Max(), true);

            keep_.release();
            doNotKeep_.release();

            ++record.CollectGarbageRequestsInFlight;
            record.PerGenerationCounter += ev->Collect;
            record.TrashInFlight.swap(trashInFlight);
            record.IssuedGenStep = nextGenStep;

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT11, "issuing TEvCollectGarbage", (Id, Self->GetLogId()),
                (Channel, int(record.Channel)), (GroupId, record.GroupId), (Msg, ev->ToString()),
                (LastConfirmedGenStep, record.LastConfirmedGenStep), (IssuedGenStep, record.IssuedGenStep),
                (LeastExpectedBlobId, leastExpectedBlobId), (TrashInFlight.size, record.TrashInFlight.size()));

            const ui64 id = ++LastCollectCmdId;
            const ui64 queryId = RandomNumber<ui64>();
            CollectCmds.emplace(id, TCollectCmd{.QueryId = queryId, .GroupId = record.GroupId, .Hard = false});

            if (IS_LOG_PRIORITY_ENABLED(NLog::PRI_TRACE, NKikimrServices::BLOB_DEPOT_EVENTS)) {
                if (ev->Keep) {
                    for (const TLogoBlobID& blobId : *ev->Keep) {
                        Y_ABORT_UNLESS(blobId.Channel() == record.Channel);
                        BDEV(BDEV00, "TrashManager_issueKeep", (BDT, Self->TabletID()), (GroupId, record.GroupId),
                            (Channel, int(record.Channel)), (Q, queryId), (Cookie, id), (BlobId, blobId));
                    }
                }
                if (ev->DoNotKeep) {
                    for (const TLogoBlobID& blobId : *ev->DoNotKeep) {
                        Y_ABORT_UNLESS(blobId.Channel() == record.Channel);
                        BDEV(BDEV01, "TrashManager_issueDoNotKeep", (BDT, Self->TabletID()), (GroupId, record.GroupId),
                            (Channel, int(record.Channel)), (Q, queryId), (Cookie, id), (BlobId, blobId));
                    }
                }
                if (collect) {
                    BDEV(BDEV02, "TrashManager_issueCollect", (BDT, Self->TabletID()), (GroupId, record.GroupId),
                        (Channel, int(ev->Channel)), (Q, queryId), (Cookie, id), (RecordGeneration, ev->RecordGeneration),
                        (PerGenerationCounter, ev->PerGenerationCounter), (Hard, ev->Hard),
                        (CollectGeneration, ev->CollectGeneration), (CollectStep, ev->CollectStep));
                }
            }

            ExecuteIssueGC(record.Channel, record.GroupId, record.IssuedGenStep, std::move(ev), id);
        }

        for (auto& [agentId, ev] : outbox) {
            TAgent& agent = Self->GetAgent(agentId);
            const ui64 id = ++agent.LastRequestId;
            auto& request = agent.InvalidateStepRequests[id];
            for (const auto& item : ev->Record.GetInvalidatedSteps()) {
                request[item.GetChannel()] = item.GetInvalidatedStep();
            }

            Y_ABORT_UNLESS(agent.Connection);
            agent.PushCallbacks.emplace(id, std::bind(&TData::OnPushNotifyResult, this, std::placeholders::_1));
            TActivationContext::Send(new IEventHandle(agent.Connection->AgentId, agent.Connection->PipeServerId, ev.release(), 0, id));
        }
    }

    void TData::Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev) {
        auto cmd = CollectCmds.extract(ev->Cookie);
        Y_ABORT_UNLESS(cmd);
        const TCollectCmd& info = cmd.mapped();
        const ui32 groupId = info.GroupId;

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT12, "TEvCollectGarbageResult", (Id, Self->GetLogId()),
            (Channel, ev->Get()->Channel), (GroupId, groupId), (Hard, info.Hard), (Msg, ev->Get()->ToString()));

        BDEV(BDEV03, "TrashManager_collectResult", (BDT, Self->TabletID()), (GroupId, groupId), (Channel, ev->Get()->Channel),
            (Q, info.QueryId), (Cookie, ev->Cookie), (Status, ev->Get()->Status), (ErrorReason, ev->Get()->ErrorReason));

        TRecordsPerChannelGroup& record = GetRecordsPerChannelGroup(ev->Get()->Channel, groupId);
        Y_ABORT_UNLESS(record.CollectGarbageRequestsInFlight);

        if (ev->Get()->Status == NKikimrProto::OK) {
            if (info.Hard) {
                ExecuteHardGC(record.Channel, record.GroupId, info.GenStep);
            } else {
                record.InitialCollectionComplete = true;
                record.OnSuccessfulCollect(this);
                ExecuteConfirmGC(record.Channel, record.GroupId, std::exchange(record.TrashInFlight, {}), 0,
                    record.LastConfirmedGenStep);
            }
        } else {
            if (!info.Hard) {
                record.TrashInFlight.clear();
            }
            record.ClearInFlight(this);
        }
    }

    void TData::OnCommitConfirmedGC(ui8 channel, ui32 groupId, std::vector<TLogoBlobID> trashDeleted) {
        TrimChannelHistory(channel, groupId, std::move(trashDeleted));
        TRecordsPerChannelGroup& record = GetRecordsPerChannelGroup(channel, groupId);
        record.ClearInFlight(this);
    }
    
    void TData::CollectTrashByHardBarrier(ui8 channel, ui32 groupId, TGenStep hardGenStep,
            const std::function<bool(TLogoBlobID)>& callback) {
        TRecordsPerChannelGroup& record = GetRecordsPerChannelGroup(channel, groupId);
        for (auto it = record.Trash.begin(); it != record.Trash.end() && TGenStep(*it) <= hardGenStep && callback(*it); ) {
            record.DeleteTrashRecord(this, it);
        }
    }

    void TData::OnCommitHardGC(ui8 channel, ui32 groupId, TGenStep hardGenStep) {
        TRecordsPerChannelGroup& record = GetRecordsPerChannelGroup(channel, groupId);
        record.HardGenStep = hardGenStep;
        record.ClearInFlight(this);
    }

    void TData::TrimChannelHistory(ui8 channel, ui32 groupId, std::vector<TLogoBlobID> trashDeleted) {
        TRecordsPerChannelGroup& record = GetRecordsPerChannelGroup(channel, groupId);

        const TTabletStorageInfo *info = Self->Info();
        Y_ABORT_UNLESS(info);
        const TTabletChannelInfo *ch = info->ChannelInfo(channel);
        Y_ABORT_UNLESS(ch);
        auto& history = ch->History;
        Y_ABORT_UNLESS(!history.empty());

        const ui64 tabletId = Self->TabletID();

        ui32 prevGenerationBegin = 0;
        ui32 prevGenerationEnd = 0;

        Y_DEBUG_ABORT_UNLESS(std::is_sorted(trashDeleted.begin(), trashDeleted.end()));

        for (const TLogoBlobID& id : trashDeleted) {
            Y_ABORT_UNLESS(id.TabletID() == tabletId);
            Y_ABORT_UNLESS(id.Channel() == channel);

            const ui32 generation = id.Generation();
            if (prevGenerationBegin <= generation && generation < prevGenerationEnd) {
                // the range was already processed, skip
            } else if (history.back().FromGeneration <= generation) {
                // in current generation, skip; this is also the last range, as the prefix is equal for all items
                // and they are in sorted order -- it's safe to quit now
                break;
            } else {
                auto it = std::upper_bound(history.begin(), history.end(), generation,
                    TTabletChannelInfo::THistoryEntry::TCmp());
                Y_ABORT_UNLESS(it != history.end());
                prevGenerationEnd = it->FromGeneration;
                Y_ABORT_UNLESS(it != history.begin());
                prevGenerationBegin = std::prev(it)->FromGeneration;
                Y_ABORT_UNLESS(prevGenerationBegin <= generation && generation < prevGenerationEnd);

                TLogoBlobID min(tabletId, prevGenerationBegin, 0, channel, 0, 0);
                TLogoBlobID max(tabletId, prevGenerationEnd - 1, Max<ui32>(), channel, TLogoBlobID::MaxBlobSize,
                    TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId, TLogoBlobID::MaxCrcMode);

                if (record.Used.lower_bound(min) != record.Used.upper_bound(max)) {
                    // we have some used records in this range, skip it
                } else if (record.Trash.lower_bound(min) != record.Trash.upper_bound(max)) {
                    // we have some still undeleted trash in this range, skip it too
                } else if (AlreadyCutHistory.emplace(channel, prevGenerationBegin).second) {
                    auto ev = std::make_unique<TEvTablet::TEvCutTabletHistory>();
                    auto& record = ev->Record;
                    record.SetTabletID(tabletId);
                    record.SetChannel(channel);
                    record.SetFromGeneration(prevGenerationBegin);
                    Self->Send(MakeLocalID(Self->SelfId().NodeId()), ev.release());
                }
            }
        }
    }

} // NKikimr::NBlobDepot
