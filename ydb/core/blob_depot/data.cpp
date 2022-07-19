#include "data.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    class TData::TTxIssueGC : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const ui8 Channel;
        const ui32 GroupId;
        const TGenStep IssuedGenStep;
        std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> CollectGarbage;

    public:
        TTxIssueGC(TBlobDepot *self, ui8 channel, ui32 groupId, TGenStep issuedGenStep,
                std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> collectGarbage)
            : TTransactionBase(self)
            , Channel(channel)
            , GroupId(groupId)
            , IssuedGenStep(issuedGenStep)
            , CollectGarbage(std::move(collectGarbage))
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::GC>().Key(Channel, GroupId).Update<Schema::GC::IssuedGenStep>(ui64(IssuedGenStep));
            return true;
        }

        void Complete(const TActorContext&) override {
            SendToBSProxy(Self->SelfId(), GroupId, CollectGarbage.release(), GroupId);
        }
    };

    class TData::TTxConfirmGC : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const ui8 Channel;
        const ui32 GroupId;
        std::vector<TLogoBlobID> TrashDeleted;
        const TGenStep ConfirmedGenStep;

        static constexpr ui32 MaxKeysToProcessAtOnce = 10'000;

    public:
        TTxConfirmGC(TBlobDepot *self, ui8 channel, ui32 groupId, std::vector<TLogoBlobID> trashDeleted, TGenStep confirmedGenStep)
            : TTransactionBase(self)
            , Channel(channel)
            , GroupId(groupId)
            , TrashDeleted(std::move(trashDeleted))
            , ConfirmedGenStep(confirmedGenStep)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);

            for (ui32 i = 0; i < TrashDeleted.size() && i < MaxKeysToProcessAtOnce; ++i) {
                db.Table<Schema::Trash>().Key(TKey(TrashDeleted[i]).MakeBinaryKey()).Delete();
            }
            if (TrashDeleted.size() <= MaxKeysToProcessAtOnce) {
                TrashDeleted.clear();
                db.Table<Schema::GC>().Key(Channel, GroupId).Update<Schema::GC::ConfirmedGenStep>(ui64(ConfirmedGenStep));
            } else {
                std::vector<TLogoBlobID> temp;
                temp.insert(temp.end(), TrashDeleted.begin() + MaxKeysToProcessAtOnce, TrashDeleted.end());
                temp.swap(TrashDeleted);
            }

            return true;
        }

        void Complete(const TActorContext&) override {
            if (TrashDeleted.empty()) {
                Self->Data->OnCommitConfirmedGC(Channel, GroupId);
            } else { // resume transaction
                Self->Execute(std::make_unique<TTxConfirmGC>(Self, Channel, GroupId, std::move(TrashDeleted), ConfirmedGenStep));
            }
        }
    };

    std::optional<TData::TValue> TData::FindKey(const TKey& key) {
        const auto it = Data.find(key);
        return it != Data.end() ? std::make_optional(it->second) : std::nullopt;
    }

    TData::TRecordsPerChannelGroup& TData::GetRecordsPerChannelGroup(TLogoBlobID id) {
        TTabletStorageInfo *info = Self->Info();
        const ui32 groupId = info->GroupFor(id.Channel(), id.Generation());
        Y_VERIFY(groupId != Max<ui32>());
        const auto& key = std::make_tuple(id.TabletID(), id.Channel(), groupId);
        const auto [it, _] = RecordsPerChannelGroup.emplace(std::piecewise_construct, key, key);
        return it->second;
    }

    void TData::AddDataOnLoad(TKey key, TString value) {
        NKikimrBlobDepot::TValue proto;
        const bool success = proto.ParseFromString(value);
        Y_VERIFY(success);
        PutKey(std::move(key), {
            .Meta = proto.GetMeta(),
            .ValueChain = std::move(*proto.MutableValueChain()),
            .KeepState = proto.GetKeepState(),
            .Public = proto.GetPublic(),
        });
    }

    void TData::AddTrashOnLoad(TLogoBlobID id) {
        auto& record = GetRecordsPerChannelGroup(id);
        record.Trash.insert(id);
        record.EnqueueForCollectionIfPossible(this);
    }

    void TData::AddGenStepOnLoad(ui8 channel, ui32 groupId, TGenStep issuedGenStep, TGenStep confirmedGenStep) {
        const auto& key = std::make_tuple(Self->TabletID(), channel, groupId);
        const auto [it, _] = RecordsPerChannelGroup.emplace(std::piecewise_construct, key, key);
        auto& record = it->second;
        record.IssuedGenStep = issuedGenStep;
        record.LastConfirmedGenStep = confirmedGenStep;
    }

    void TData::PutKey(TKey key, TValue&& data) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT08, "PutKey", (TabletId, Self->TabletID()), (Key, key.ToString(Self->Config)),
            (KeepState, NKikimrBlobDepot::EKeepState_Name(data.KeepState)));

        EnumerateBlobsForValueChain(data.ValueChain, Self->TabletID(), [&](TLogoBlobID id) {
            if (!RefCount[id]++) {
                // first mention of this id
                auto& record = GetRecordsPerChannelGroup(id);
                const auto [_, inserted] = record.Used.insert(id);
                Y_VERIFY(inserted);
            }
        });

        Data[std::move(key)] = std::move(data);
    }

    std::optional<TString> TData::UpdateKeepState(TKey key, NKikimrBlobDepot::EKeepState keepState) {
        const auto it = Data.find(key);
        if (it != Data.end() && keepState <= it->second.KeepState) {
            return std::nullopt;
        }
        auto& value = Data[std::move(key)];
        value.KeepState = keepState;
        return ToValueProto(value);
    }

    void TData::DeleteKey(const TKey& key, const std::function<void(TLogoBlobID)>& updateTrash, void *cookie) {
        const auto it = Data.find(key);
        Y_VERIFY(it != Data.end());
        TValue& value = it->second;
        EnumerateBlobsForValueChain(value.ValueChain, Self->TabletID(), [&](TLogoBlobID id) {
            const auto it = RefCount.find(id);
            Y_VERIFY(it != RefCount.end());
            if (!--it->second) {
                InFlightTrash.emplace(cookie, id);
                RefCount.erase(it);
                updateTrash(id);
            }
        });
        Data.erase(it);
    }

    void TData::CommitTrash(void *cookie) {
        auto range = InFlightTrash.equal_range(cookie);
        for (auto it = range.first; it != range.second; ++it) {
            auto& record = GetRecordsPerChannelGroup(it->second);
            record.MoveToTrash(this, it->second);
        }
        InFlightTrash.erase(range.first, range.second);
    }

    TString TData::ToValueProto(const TValue& value) {
        NKikimrBlobDepot::TValue proto;
        if (value.Meta) {
            proto.SetMeta(value.Meta);
        }
        proto.MutableValueChain()->CopyFrom(value.ValueChain);
        if (proto.GetKeepState() != value.KeepState) {
            proto.SetKeepState(value.KeepState);
        }
        if (proto.GetPublic() != value.Public) {
            proto.SetPublic(value.Public);
        }

        TString s;
        const bool success = proto.SerializeToString(&s);
        Y_VERIFY(success);
        return s;
    }

    void TData::HandleTrash() {
        const ui32 generation = Self->Executor()->Generation();
        THashMap<ui32, std::unique_ptr<TEvBlobDepot::TEvPushNotify>> outbox;

        while (RecordsWithTrash) {
            TRecordsPerChannelGroup& record = *RecordsWithTrash.PopFront();

            Y_VERIFY(!record.CollectGarbageRequestInFlight);
            Y_VERIFY(record.TabletId == Self->TabletID());
            Y_VERIFY(!record.Trash.empty());

            Y_VERIFY(record.Channel < Self->Channels.size());
            auto& channel = Self->Channels[record.Channel];

            TGenStep nextGenStep(*--record.Trash.end());

            // step we are going to invalidate (including blobs with this one)
            if (TGenStep(record.LeastExpectedBlobId) <= nextGenStep) {
                const ui32 invalidatedStep = nextGenStep.Step(); // the step we want to invalidate and garbage collect

                // remove invalidated step from allocations
                auto blobSeqId = TBlobSeqId::FromSequentalNumber(record.Channel, generation, channel.NextBlobSeqId);
                Y_VERIFY(record.LastConfirmedGenStep < TGenStep(blobSeqId));
                if (blobSeqId.Step <= invalidatedStep) {
                    blobSeqId.Step = invalidatedStep + 1;
                    blobSeqId.Index = 0;
                    channel.NextBlobSeqId = blobSeqId.ToSequentialNumber();
                }

                // issue notifications to agents
                for (auto& [agentId, agent] : Self->Agents) {
                    if (!agent.AgentId) {
                        continue;
                    }
                    const auto [it, inserted] = agent.InvalidatedStepInFlight.emplace(record.Channel, invalidatedStep);
                    if (inserted || it->second < invalidatedStep) {
                        it->second = invalidatedStep;

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
                
                // adjust the barrier to keep it safe now
                const TLogoBlobID maxId(record.TabletId, record.LeastExpectedBlobId.Generation,
                    record.LeastExpectedBlobId.Step, record.Channel, 0, 0);
                const auto it = record.Trash.lower_bound(maxId);
                if (it != record.Trash.begin()) {
                    nextGenStep = TGenStep(*std::prev(it));
                } else {
                    nextGenStep = {};
                }
            }

            auto keep = std::make_unique<TVector<TLogoBlobID>>();
            auto doNotKeep = std::make_unique<TVector<TLogoBlobID>>();

            for (auto it = record.Trash.begin(); it != record.Trash.end() && TGenStep(*it) <= record.LastConfirmedGenStep; ++it) {
                doNotKeep->push_back(*it);
            }

            const TLogoBlobID keepFrom(record.TabletId, record.LastConfirmedGenStep.Generation(),
                record.LastConfirmedGenStep.Step(), record.Channel, 0, 0);
            for (auto it = record.Used.upper_bound(keepFrom); it != record.Used.end() && TGenStep(*it) <= nextGenStep; ++it) {
                keep->push_back(*it);
            }

            if (keep->empty()) {
                keep.reset();
            }
            if (doNotKeep->empty()) {
                doNotKeep.reset();
            }
            const bool collect = nextGenStep > record.LastConfirmedGenStep;

            if (!keep && !doNotKeep && !collect) {
                continue; // nothing to do here
            }

            auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(record.TabletId, generation,
                record.PerGenerationCounter, record.Channel, collect, nextGenStep.Generation(), nextGenStep.Step(),
                keep.get(), doNotKeep.get(), TInstant::Max(), true);
            keep.release();
            doNotKeep.release();

            record.CollectGarbageRequestInFlight = true;
            record.PerGenerationCounter += ev->Collect ? ev->PerGenerationCounterStepSize() : 0;
            record.TrashInFlight.insert(record.TrashInFlight.end(), record.Trash.begin(), record.Trash.end());
            record.IssuedGenStep = Max(nextGenStep, record.LastConfirmedGenStep);

            record.TIntrusiveListItem<TRecordsPerChannelGroup, TRecordWithTrash>::Unlink();

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT09, "issuing TEvCollectGarbage", (TabletId, Self->TabletID()),
                (Channel, int(record.Channel)), (GroupId, record.GroupId), (Msg, ev->ToString()),
                (LastConfirmedGenStep, record.LastConfirmedGenStep), (IssuedGenStep, record.IssuedGenStep),
                (TrashInFlight.size, record.TrashInFlight.size()));

            if (collect) {
                Self->Execute(std::make_unique<TTxIssueGC>(Self, record.Channel, record.GroupId, record.IssuedGenStep,
                    std::move(ev)));
            } else {
                SendToBSProxy(Self->SelfId(), record.GroupId, ev.release(), record.GroupId);
            }
        }

        for (auto& [agentId, ev] : outbox) {
            TAgent& agent = Self->GetAgent(agentId);
            const ui64 id = ++agent.LastRequestId;
            auto& request = agent.InvalidateStepRequests[id];
            for (const auto& item : ev->Record.GetInvalidatedSteps()) {
                request[item.GetChannel()] = item.GetInvalidatedStep();
            }

            Y_VERIFY(agent.AgentId);
            TActivationContext::Send(new IEventHandle(*agent.AgentId, Self->SelfId(), ev.release(), 0, id));
        }
    }

    void TData::Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT10, "TEvCollectGarbageResult", (TabletId, ev->Get()->TabletId),
            (Channel, ev->Get()->Channel), (GroupId, ev->Cookie), (Msg, ev->Get()->ToString()));
        const auto& key = std::make_tuple(ev->Get()->TabletId, ev->Get()->Channel, ev->Cookie);
        const auto it = RecordsPerChannelGroup.find(key);
        Y_VERIFY(it != RecordsPerChannelGroup.end());
        auto& record = it->second;
        if (ev->Get()->Status == NKikimrProto::OK) {
            Y_VERIFY(record.CollectGarbageRequestInFlight);
            record.OnSuccessfulCollect(this);
            Self->Execute(std::make_unique<TTxConfirmGC>(Self, record.Channel, record.GroupId,
                std::exchange(record.TrashInFlight, {}), record.LastConfirmedGenStep));
        } else {
            record.ClearInFlight(this);
            HandleTrash();
        }
    }

    void TData::Handle(TEvBlobDepot::TEvPushNotifyResult::TPtr ev) {
        TAgent& agent = Self->GetAgent(ev->Sender.NodeId());
        const ui32 generation = Self->Executor()->Generation();

        std::set<TBlobSeqId> writesInFlight;
        for (const auto& item : ev->Get()->Record.GetWritesInFlight()) {
            writesInFlight.insert(TBlobSeqId::FromProto(item));
        }

        if (const auto it = agent.InvalidateStepRequests.find(ev->Cookie); it != agent.InvalidateStepRequests.end()) {
            for (const auto& [channel, invalidatedStep] : it->second) {
                const ui32 channel_ = channel;
                const ui32 invalidatedStep_ = invalidatedStep;
                auto& agentGivenIdRanges = agent.GivenIdRanges[channel];
                auto& givenIdRanges = Self->Channels[channel].GivenIdRanges;

                auto begin = writesInFlight.lower_bound(TBlobSeqId{channel, 0, 0, 0});
                auto end = writesInFlight.upper_bound(TBlobSeqId{channel, Max<ui32>(), Max<ui32>(), TBlobSeqId::MaxIndex});

                auto makeWritesInFlight = [&] {
                    TStringStream s;
                    s << "[";
                    for (auto it = begin; it != end; ++it) {
                        s << (it != begin ? " " : "") << it->ToString();
                    }
                    s << "]";
                    return s.Str();
                };

                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT99, "Trim", (TabletId, Self->TabletID()), (AgentId, agent.ConnectedNodeId),
                    (Id, ev->Cookie), (Channel, channel_), (InvalidatedStep, invalidatedStep_),
                    (GivenIdRanges, Self->Channels[channel_].GivenIdRanges),
                    (Agent.GivenIdRanges, agent.GivenIdRanges[channel_]),
                    (WritesInFlight, makeWritesInFlight()));

                for (auto it = begin; it != end; ++it) {
                    Y_VERIFY_S(agentGivenIdRanges.GetPoint(it->ToSequentialNumber()), "blobSeqId# " << it->ToString());
                    Y_VERIFY_S(givenIdRanges.GetPoint(it->ToSequentialNumber()), "blobSeqId# " << it->ToString());
                }

                const TBlobSeqId trimmedBlobSeqId{channel, generation, invalidatedStep, TBlobSeqId::MaxIndex};
                const ui64 validSince = trimmedBlobSeqId.ToSequentialNumber() + 1;
                givenIdRanges.Subtract(agentGivenIdRanges.Trim(validSince));

                for (auto it = begin; it != end; ++it) {
                    agentGivenIdRanges.AddPoint(it->ToSequentialNumber());
                    givenIdRanges.AddPoint(it->ToSequentialNumber());
                }

                OnLeastExpectedBlobIdChange(channel);
            }
            agent.InvalidateStepRequests.erase(it);
        } else {
            Y_VERIFY_DEBUG(false);
        }
        HandleTrash();
    }

    void TData::OnCommitConfirmedGC(ui8 channel, ui32 groupId) {
        const auto& key = std::make_tuple(Self->TabletID(), channel, groupId);
        const auto it = RecordsPerChannelGroup.find(key);
        Y_VERIFY(it != RecordsPerChannelGroup.end());
        it->second.ClearInFlight(this);
    }

    bool TData::CanBeCollected(ui32 groupId, TBlobSeqId id) const {
        const auto it = RecordsPerChannelGroup.find(std::make_tuple(Self->TabletID(), id.Channel, groupId));
        return it != RecordsPerChannelGroup.end() && TGenStep(id) <= it->second.IssuedGenStep;
    }

    void TData::OnLeastExpectedBlobIdChange(ui8 channel) {
        auto& ch = Self->Channels[channel];
        const ui64 minSequenceNumber = ch.GivenIdRanges.IsEmpty()
            ? ch.NextBlobSeqId
            : ch.GivenIdRanges.GetMinimumValue();
        const TBlobSeqId leastExpectedBlobId = TBlobSeqId::FromSequentalNumber(channel, Self->Executor()->Generation(),
            minSequenceNumber);

        const TTabletStorageInfo *info = Self->Info();
        const TTabletChannelInfo *storageChannel = info->ChannelInfo(leastExpectedBlobId.Channel);
        Y_VERIFY(storageChannel);
        for (const auto& entry : storageChannel->History) {
            const auto& key = std::make_tuple(info->TabletID, storageChannel->Channel, entry.GroupID);
            auto [it, _] = RecordsPerChannelGroup.emplace(std::piecewise_construct, key, key);
            auto& record = it->second;
            record.OnLeastExpectedBlobIdChange(this, leastExpectedBlobId);
        }
    }

    void TData::TRecordsPerChannelGroup::MoveToTrash(TData *self, TLogoBlobID id) {
        const auto usedIt = Used.find(id);
        Y_VERIFY(usedIt != Used.end());
        Trash.insert(Used.extract(usedIt));
        EnqueueForCollectionIfPossible(self);
    }

    void TData::TRecordsPerChannelGroup::OnSuccessfulCollect(TData *self) {
        auto it = Trash.begin();
        for (const TLogoBlobID& id : TrashInFlight) {
            for (; it != Trash.end() && *it < id; ++it) {}
            Y_VERIFY(it != Trash.end() && *it == id);
            it = Trash.erase(it);
        }
        LastConfirmedGenStep = IssuedGenStep;
        EnqueueForCollectionIfPossible(self);
    }

    void TData::TRecordsPerChannelGroup::OnLeastExpectedBlobIdChange(TData *self, TBlobSeqId leastExpectedBlobId) {
        Y_VERIFY_S(LeastExpectedBlobId <= leastExpectedBlobId, "Prev# " << LeastExpectedBlobId.ToString()
            << " Next# " << leastExpectedBlobId.ToString());
        if (LeastExpectedBlobId < leastExpectedBlobId) {
            LeastExpectedBlobId = leastExpectedBlobId;
            EnqueueForCollectionIfPossible(self);
        }
    }

    void TData::TRecordsPerChannelGroup::ClearInFlight(TData *self) {
        Y_VERIFY(CollectGarbageRequestInFlight);
        CollectGarbageRequestInFlight = false;
        EnqueueForCollectionIfPossible(self);
    }

    void TData::TRecordsPerChannelGroup::EnqueueForCollectionIfPossible(TData *self) {
        if (!CollectGarbageRequestInFlight && TabletId == self->Self->TabletID() && Empty() && !Trash.empty()) {
            self->RecordsWithTrash.PushBack(this);
        }
    }

} // NKikimr::NBlobDepot
