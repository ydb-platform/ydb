#include "data.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    void TData::CommitTrash(void *cookie) {
        auto [first, last] = InFlightTrash.equal_range(cookie);
        std::unordered_set<TRecordsPerChannelGroup*> records;
        for (auto it = first; it != last; ++it) {
            auto& record = GetRecordsPerChannelGroup(it->second);
            record.MoveToTrash(it->second);
            records.insert(&record);
        }
        InFlightTrash.erase(first, last);

        for (TRecordsPerChannelGroup *record : records) {
            record->CollectIfPossible(this);
        }
    }

    void TData::HandleTrash(TRecordsPerChannelGroup& record) {
        const ui32 generation = Self->Executor()->Generation();
        THashMap<ui32, std::unique_ptr<TEvBlobDepot::TEvPushNotify>> outbox;

        Y_VERIFY(!record.CollectGarbageRequestInFlight);
        Y_VERIFY(!record.Trash.empty());

        Y_VERIFY(record.Channel < Self->Channels.size());
        auto& channel = Self->Channels[record.Channel];

        TGenStep nextGenStep(*--record.Trash.end());
        std::set<TLogoBlobID>::iterator trashEndIter = record.Trash.end();

        // step we are going to invalidate (including blobs with this one)
        TBlobSeqId leastExpectedBlobId = channel.GetLeastExpectedBlobId(generation);
        const ui32 invalidatedStep = nextGenStep.Step(); // the step we want to invalidate and garbage collect

        if (TGenStep(leastExpectedBlobId) <= nextGenStep) {
            // remove invalidated step from allocations
            auto blobSeqId = TBlobSeqId::FromSequentalNumber(record.Channel, generation, channel.NextBlobSeqId);
            Y_VERIFY(record.LastConfirmedGenStep < TGenStep(blobSeqId));
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
            if (trashEndIter != record.Trash.begin()) {
                nextGenStep = TGenStep(*std::prev(trashEndIter));
            } else {
                nextGenStep = {};
            }
        }

        TVector<TLogoBlobID> keep;
        TVector<TLogoBlobID> doNotKeep;
        std::vector<TLogoBlobID> trashInFlight;

        for (auto it = record.Trash.begin(); it != trashEndIter; ++it) {
            if (const TGenStep genStep(*it); genStep <= record.LastConfirmedGenStep) {
                doNotKeep.push_back(*it);
            } else if (nextGenStep < genStep) {
                Y_FAIL();
            }
            trashInFlight.push_back(*it);
        }

        const TLogoBlobID keepFrom(Self->TabletID(), record.LastConfirmedGenStep.Generation(),
            record.LastConfirmedGenStep.Step(), record.Channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie,
            TLogoBlobID::MaxPartId, TLogoBlobID::MaxCrcMode);
        for (auto it = record.Used.upper_bound(keepFrom); it != record.Used.end() && TGenStep(*it) <= nextGenStep; ++it) {
            Y_VERIFY(record.LastConfirmedGenStep < TGenStep(*it));
            keep.push_back(*it);
        }

        const bool collect = nextGenStep > record.LastConfirmedGenStep;

        if (trashInFlight.empty()) {
            Y_VERIFY(keep.empty()); // nothing to do here
        } else {
            auto keep_ = keep ? std::make_unique<TVector<TLogoBlobID>>(std::move(keep)) : nullptr;
            auto doNotKeep_ = doNotKeep ? std::make_unique<TVector<TLogoBlobID>>(std::move(doNotKeep)) : nullptr;

            auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(Self->TabletID(), generation,
                record.PerGenerationCounter, record.Channel, collect, nextGenStep.Generation(), nextGenStep.Step(),
                keep_.get(), doNotKeep_.get(), TInstant::Max(), true);

            keep_.release();
            doNotKeep_.release();

            record.CollectGarbageRequestInFlight = true;
            record.PerGenerationCounter += ev->Collect ? ev->PerGenerationCounterStepSize() : 0;
            record.TrashInFlight.swap(trashInFlight);
            record.IssuedGenStep = Max(nextGenStep, record.LastConfirmedGenStep);

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT11, "issuing TEvCollectGarbage", (Id, Self->GetLogId()),
                (Channel, int(record.Channel)), (GroupId, record.GroupId), (Msg, ev->ToString()),
                (LastConfirmedGenStep, record.LastConfirmedGenStep), (IssuedGenStep, record.IssuedGenStep),
                (LeastExpectedBlobId, leastExpectedBlobId), (TrashInFlight.size, record.TrashInFlight.size()));

            const ui64 id = ++LastCollectCmdId;
            CollectCmdToGroup.emplace(id, record.GroupId);

            if (ev->Keep) {
                for (const TLogoBlobID& blobId : *ev->Keep) {
                    BDEV(BDEV00, "TrashManager_issueKeep", (BDT, Self->TabletID()), (GroupId, record.GroupId),
                        (BlobId, blobId), (Cookie, id));
                }
            }
            if (ev->DoNotKeep) {
                for (const TLogoBlobID& blobId : *ev->DoNotKeep) {
                    BDEV(BDEV01, "TrashManager_issueDoNotKeep", (BDT, Self->TabletID()), (GroupId, record.GroupId),
                        (BlobId, blobId), (Cookie, id));
                }
            }
            if (collect) {
                BDEV(BDEV02, "TrashManager_issueCollect", (BDT, Self->TabletID()), (GroupId, record.GroupId),
                    (Channel, int(ev->Channel)), (RecordGeneration, ev->RecordGeneration), (Hard, ev->Hard),
                    (CollectGeneration, ev->CollectGeneration), (CollectStep, ev->CollectStep), (Cookie, id));
            }

            if (collect) {
                ExecuteIssueGC(record.Channel, record.GroupId, record.IssuedGenStep, std::move(ev), id);
            } else {
                SendToBSProxy(Self->SelfId(), record.GroupId, ev.release(), id);
            }
        }

        for (auto& [agentId, ev] : outbox) {
            TAgent& agent = Self->GetAgent(agentId);
            const ui64 id = ++agent.LastRequestId;
            auto& request = agent.InvalidateStepRequests[id];
            for (const auto& item : ev->Record.GetInvalidatedSteps()) {
                request[item.GetChannel()] = item.GetInvalidatedStep();
            }

            Y_VERIFY(agent.Connection);
            agent.PushCallbacks.emplace(id, std::bind(&TData::OnPushNotifyResult, this, std::placeholders::_1));
            TActivationContext::Send(new IEventHandle(agent.Connection->AgentId, agent.Connection->PipeServerId, ev.release(), 0, id));
        }
    }

    void TData::Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT12, "TEvCollectGarbageResult", (Id, Self->GetLogId()),
            (Channel, ev->Get()->Channel), (GroupId, ev->Cookie), (Msg, ev->Get()->ToString()));

        BDEV(BDEV03, "TrashManager_collectResult", (BDT, Self->TabletID()), (Cookie, ev->Cookie),
            (Status, ev->Get()->Status), (ErrorReason, ev->Get()->ErrorReason));

        auto cmd = CollectCmdToGroup.extract(ev->Cookie);
        Y_VERIFY(cmd);
        const ui32 groupId = cmd.mapped();

        const auto& key = std::make_tuple(ev->Get()->Channel, groupId);
        const auto it = RecordsPerChannelGroup.find(key);
        Y_VERIFY(it != RecordsPerChannelGroup.end());
        auto& record = it->second;
        if (ev->Get()->Status == NKikimrProto::OK) {
            Y_VERIFY(record.CollectGarbageRequestInFlight);
            record.OnSuccessfulCollect(this);
            ExecuteConfirmGC(record.Channel, record.GroupId, std::exchange(record.TrashInFlight, {}),
                record.LastConfirmedGenStep);
        } else {
            record.TrashInFlight.clear();
            record.ClearInFlight(this);
        }
    }

    void TData::OnCommitConfirmedGC(ui8 channel, ui32 groupId) {
        const auto& key = std::make_tuple(channel, groupId);
        const auto it = RecordsPerChannelGroup.find(key);
        Y_VERIFY(it != RecordsPerChannelGroup.end());
        it->second.ClearInFlight(this);
    }

} // NKikimr::NBlobDepot
