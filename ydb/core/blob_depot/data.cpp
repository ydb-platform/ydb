#include "data.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TData::TTxConfirmGC : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const ui8 Channel;
        const ui32 GroupId;
        std::vector<TLogoBlobID> TrashDeleted;
        const ui64 ConfirmedGenStep;

        static constexpr ui32 MaxKeysToProcessAtOnce = 10'000;

    public:
        TTxConfirmGC(TBlobDepot *self, ui8 channel, ui32 groupId, std::vector<TLogoBlobID> trashDeleted,
                ui64 confirmedGenStep)
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
                db.Table<Schema::ConfirmedGC>().Key(Channel, GroupId).Update<Schema::ConfirmedGC::ConfirmedGenStep>(
                    ConfirmedGenStep);
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

    std::optional<TBlobDepot::TData::TValue> TBlobDepot::TData::FindKey(const TKey& key) {
        const auto it = Data.find(key);
        return it != Data.end() ? std::make_optional(it->second) : std::nullopt;
    }

    TBlobDepot::TData::TRecordsPerChannelGroup& TBlobDepot::TData::GetRecordsPerChannelGroup(TLogoBlobID id) {
        TTabletStorageInfo *info = Self->Info();
        const ui32 groupId = info->GroupFor(id.Channel(), id.Generation());
        Y_VERIFY(groupId != Max<ui32>());
        const auto& key = std::make_tuple(id.TabletID(), id.Channel(), groupId);
        const auto [it, _] = RecordsPerChannelGroup.try_emplace(key, id.TabletID(), id.Channel(), groupId);
        return it->second;
    }

    void TBlobDepot::TData::AddDataOnLoad(TKey key, TString value) {
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

    void TBlobDepot::TData::AddTrashOnLoad(TLogoBlobID id) {
        auto& record = GetRecordsPerChannelGroup(id);
        record.Trash.insert(id);
        OnTrashInserted(record);
    }

    void TBlobDepot::TData::AddConfirmedGenStepOnLoad(ui8 channel, ui32 groupId, ui64 confirmedGenStep) {
        const auto& key = std::make_tuple(Self->TabletID(), channel, groupId);
        const auto [it, _] = RecordsPerChannelGroup.try_emplace(key, Self->TabletID(), channel, groupId);
        auto& record = it->second;
        record.LastConfirmedGenStep = confirmedGenStep;
    }

    void TBlobDepot::TData::PutKey(TKey key, TValue&& data) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT01, "PutKey", (TabletId, Self->TabletID()), (Key, key.ToString(Self->Config)),
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

    void TBlobDepot::TData::OnTrashInserted(TRecordsPerChannelGroup& record) {
        if (!record.CollectGarbageRequestInFlight && record.TabletId == Self->TabletID()) {
            RecordsWithTrash.PushBack(&record);
        }
    }

    std::optional<TString> TBlobDepot::TData::UpdateKeepState(TKey key, NKikimrBlobDepot::EKeepState keepState) {
        const auto it = Data.find(key);
        if (it != Data.end() && keepState <= it->second.KeepState) {
            return std::nullopt;
        }
        auto& value = Data[std::move(key)];
        value.KeepState = keepState;
        return ToValueProto(value);
    }

    void TBlobDepot::TData::DeleteKey(const TKey& key, const std::function<void(TLogoBlobID)>& updateTrash, void *cookie) {
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

    void TBlobDepot::TData::CommitTrash(void *cookie) {
        auto range = InFlightTrash.equal_range(cookie);
        for (auto it = range.first; it != range.second; ++it) {
            auto& record = GetRecordsPerChannelGroup(it->second);
            const auto usedIt = record.Used.find(it->second);
            Y_VERIFY(usedIt != record.Used.end());
            record.Trash.insert(record.Used.extract(usedIt));
            OnTrashInserted(record);
        }
        InFlightTrash.erase(range.first, range.second);
    }

    TString TBlobDepot::TData::ToValueProto(const TValue& value) {
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

    void TBlobDepot::TData::HandleTrash() {
        const ui32 generation = Self->Executor()->Generation();

        for (TRecordsPerChannelGroup& record : RecordsWithTrash) {
            Y_VERIFY(!record.CollectGarbageRequestInFlight);
            Y_VERIFY(record.TabletId == Self->TabletID());
            Y_VERIFY(!record.Trash.empty());

            ui64 nextGenStep = 0;

            auto& channel = Self->PerChannelRecords[record.Channel];
            if (channel.GivenStepIndex.empty()) {
                nextGenStep = GenStep(*--record.Trash.end());
            } else {
                const auto& [leastStep, leastIndex] = *channel.GivenStepIndex.begin();
                const TLogoBlobID maxId(record.TabletId, generation, leastStep, record.Channel, 0, 0);
                const auto it = record.Trash.lower_bound(maxId);
                if (it != record.Trash.begin()) {
                    nextGenStep = GenStep(*std::prev(it));
                }
            }

            auto keep = std::make_unique<TVector<TLogoBlobID>>();
            auto doNotKeep = std::make_unique<TVector<TLogoBlobID>>();

            // FIXME: check for blob leaks when LastConfirmedGenStep is not properly persisted
            for (auto it = record.Trash.begin(); it != record.Trash.end() && GenStep(*it) <= record.LastConfirmedGenStep; ++it) {
                doNotKeep->push_back(*it);
            }

            // FIXME: check for blob loss when LastConfirmedGenStep is not properly persisted
            const TLogoBlobID keepFrom(record.TabletId, ui32(record.LastConfirmedGenStep >> 32),
                ui32(record.LastConfirmedGenStep), record.Channel, 0, 0);
            for (auto it = record.Used.upper_bound(keepFrom); it != record.Used.end() && GenStep(*it) <= nextGenStep; ++it) {
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
                continue; // skip this one
            }

            auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(record.TabletId, generation,
                record.PerGenerationCounter, record.Channel, collect, ui32(nextGenStep >> 32), ui32(nextGenStep),
                keep.get(), doNotKeep.get(), TInstant::Max(), true);
            keep.release();
            doNotKeep.release();

            record.CollectGarbageRequestInFlight = true;
            record.PerGenerationCounter += ev->Collect ? ev->PerGenerationCounterStepSize() : 0;
            record.TrashInFlight.insert(record.TrashInFlight.end(), record.Trash.begin(), record.Trash.end());
            record.NextGenStep = Max(nextGenStep, record.LastConfirmedGenStep);

            auto& kind = Self->ChannelKinds[Self->ChannelToKind[record.Channel]];
            const auto blobSeqId = TBlobSeqId::FromBinary(generation, kind, kind.NextBlobSeqId);
            Y_VERIFY(record.LastConfirmedGenStep < GenStep(generation, blobSeqId.Step));
            if (GenStep(generation, blobSeqId.Step) <= nextGenStep) {
                kind.NextBlobSeqId = TBlobSeqId{record.Channel, generation, ui32(nextGenStep) + 1, 0}.ToBinary(kind);
            }

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT01, "issuing TEvCollectGarbage", (TabletId, Self->TabletID()),
                (Channel, record.Channel), (GroupId, record.GroupId), (Msg, ev->ToString()),
                (LastConfirmedGenStep, record.LastConfirmedGenStep), (NextGenStep, record.NextGenStep),
                (TrashInFlight.size, record.TrashInFlight.size()));

            SendToBSProxy(Self->SelfId(), record.GroupId, ev.release(), record.GroupId);
        }

        RecordsWithTrash.Clear();
    }

    void TBlobDepot::TData::Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT01, "TEvCollectGarbageResult", (TabletId, ev->Get()->TabletId),
            (Channel, ev->Get()->Channel), (GroupId, ev->Cookie), (Msg, ev->Get()->ToString()));
        const auto& key = std::make_tuple(ev->Get()->TabletId, ev->Get()->Channel, ev->Cookie);
        const auto it = RecordsPerChannelGroup.find(key);
        Y_VERIFY(it != RecordsPerChannelGroup.end());
        auto& record = it->second;
        Y_VERIFY(record.CollectGarbageRequestInFlight);
        if (ev->Get()->Status == NKikimrProto::OK) {
            for (const TLogoBlobID& id : record.TrashInFlight) { // make it merge
                record.Trash.erase(id);
            }
            record.LastConfirmedGenStep = record.NextGenStep;
            Self->Execute(std::make_unique<TTxConfirmGC>(Self, record.Channel, record.GroupId,
                std::exchange(record.TrashInFlight, {}), record.LastConfirmedGenStep));
        } else {
            record.CollectGarbageRequestInFlight = false;
            OnTrashInserted(record);
            HandleTrash();
        }
    }

    void TBlobDepot::TData::OnCommitConfirmedGC(ui8 channel, ui32 groupId) {
        const auto& key = std::make_tuple(Self->TabletID(), channel, groupId);
        const auto it = RecordsPerChannelGroup.find(key);
        Y_VERIFY(it != RecordsPerChannelGroup.end());
        auto& record = it->second;
        Y_VERIFY(record.CollectGarbageRequestInFlight);
        record.CollectGarbageRequestInFlight = false;
        if (!record.Trash.empty()) {
            OnTrashInserted(record);
            HandleTrash();
        }
    }

} // NKikimr::NBlobDepot
