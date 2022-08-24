#include "garbage_collection.h"
#include "schema.h"
#include "data.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TBarrierServer::TTxCollectGarbage : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        std::optional<TString> Error;

        TBarrier& Barrier;
        std::unique_ptr<TEvBlobDepot::TEvCollectGarbage::THandle>& Request;
        const ui64 TabletId;
        const ui8 Channel;
        int KeepIndex = 0;
        int DoNotKeepIndex = 0;
        ui32 NumKeysProcessed = 0;
        bool Done = false;

        static constexpr ui32 MaxKeysToProcessAtOnce = 10'000;

    public:
        TTxCollectGarbage(TBlobDepot *self, ui64 tabletId, ui8 channel, ui32 keepIndex = 0, ui32 doNotKeepIndex = 0)
            : TTransactionBase(self)
            , Barrier(Self->BarrierServer->Barriers[std::make_tuple(tabletId, channel)])
            , Request(Barrier.ProcessingQ.front())
            , TabletId(tabletId)
            , Channel(channel)
            , KeepIndex(keepIndex)
            , DoNotKeepIndex(doNotKeepIndex)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            Y_VERIFY(Self->Data->IsLoaded());
            Validate();
            if (!Error) {
                auto& record = Request->Get()->Record;
                Done = ProcessFlags(txc, KeepIndex, record.GetKeep(), NKikimrBlobDepot::EKeepState::Keep)
                    && ProcessFlags(txc, DoNotKeepIndex, record.GetDoNotKeep(), NKikimrBlobDepot::EKeepState::DoNotKeep)
                    && ProcessBarrier(txc);
            }
            return true;
        }

        void Complete(const TActorContext&) override {
            Self->Data->CommitTrash(this);

            if (Done || Error) {
                auto [response, _] = TEvBlobDepot::MakeResponseFor(*Request, Self->SelfId(),
                    Error ? NKikimrProto::ERROR : NKikimrProto::OK, std::move(Error));
                TActivationContext::Send(response.release());
                Barrier.ProcessingQ.pop_front();
                Self->Data->HandleTrash();
                if (!Barrier.ProcessingQ.empty()) {
                    Self->Execute(std::make_unique<TTxCollectGarbage>(Self, TabletId, Channel));
                }
            } else {
                Self->Execute(std::make_unique<TTxCollectGarbage>(Self, TabletId, Channel, KeepIndex, DoNotKeepIndex));
            }
        }

        void Validate() {
            // validate the command first
            auto& record = Request->Get()->Record;
            if (record.HasCollectGeneration() && record.HasCollectStep()) {
                if (!record.HasTabletId() || !record.HasChannel() || record.GetChannel() > TLogoBlobID::MaxChannel) {
                    Error = "TabletId/Channel are either not set or invalid";
                } else if (!record.HasGeneration() || !record.HasPerGenerationCounter()) {
                    Error = "Generation/PerGenerationCounter are not set";
                } else {
                    const auto key = std::make_tuple(record.GetTabletId(), record.GetChannel());
                    auto& barriers = Self->BarrierServer->Barriers;
                    if (const auto it = barriers.find(key); it != barriers.end()) {
                        // extract existing barrier record
                        auto& barrier = it->second;
                        const TGenStep barrierGenCtr = record.GetHard() ? barrier.HardGenCtr : barrier.SoftGenCtr;
                        const TGenStep barrierGenStep = record.GetHard() ? barrier.Hard : barrier.Soft;

                        // extract new parameters from protobuf
                        const TGenStep genCtr(record.GetGeneration(), record.GetPerGenerationCounter());
                        const TGenStep collectGenStep(record.GetCollectGeneration(), record.GetCollectStep());

                        // validate them
                        if (genCtr < barrierGenCtr) {
                            Error = "record generation:counter is obsolete";
                        } else if (genCtr == barrierGenCtr) {
                            if (barrierGenStep != collectGenStep) {
                                Error = "repeated command with different collect parameters received";
                            }
                        } else if (collectGenStep < barrierGenStep) {
                            Error = "decreasing barrier";
                        }
                    }
                }
            } else if (record.HasCollectGeneration() || record.HasCollectStep()) {
                Error = "CollectGeneration/CollectStep set incorrectly";
            }
        }

        bool ProcessFlags(TTransactionContext& txc, int& index,
                const NProtoBuf::RepeatedPtrField<NKikimrProto::TLogoBlobID>& items,
                NKikimrBlobDepot::EKeepState state) {
            NIceDb::TNiceDb db(txc.DB);
            for (; index < items.size() && NumKeysProcessed < MaxKeysToProcessAtOnce; ++index) {
                const auto id = LogoBlobIDFromLogoBlobID(items[index]);
                TData::TKey key(id);
                if (const auto& value = Self->Data->UpdateKeepState(key, state)) {
                    db.Table<Schema::Data>().Key(key.MakeBinaryKey()).Update<Schema::Data::Value>(*value);
                    ++NumKeysProcessed;
                }
            }

            return index == items.size();
        }

        bool ProcessBarrier(TTransactionContext& txc) {
            NIceDb::TNiceDb db(txc.DB);

            const auto& record = Request->Get()->Record;
            if (record.HasCollectGeneration() && record.HasCollectStep()) {
                const bool hard = record.GetHard();

                auto processKey = [&](const TData::TKey& key, const TData::TValue& value) {
                    if (value.KeepState != NKikimrBlobDepot::EKeepState::Keep || hard) {
                        const TLogoBlobID id(key.GetBlobId());
                        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT14, "DeleteKey", (Id, Self->GetLogId()), (BlobId, id));
                        db.Table<Schema::Data>().Key(key.MakeBinaryKey()).Delete();
                        auto updateTrash = [&](TLogoBlobID id) {
                            db.Table<Schema::Trash>().Key(id.AsBinaryString()).Update();
                        };
                        Self->Data->DeleteKey(key, updateTrash, this);
                        ++NumKeysProcessed;
                    }

                    return NumKeysProcessed < MaxKeysToProcessAtOnce;
                };

                const TData::TKey first(TLogoBlobID(record.GetTabletId(), 0, 0, record.GetChannel(), 0, 0));
                const TData::TKey last(TLogoBlobID(record.GetTabletId(), record.GetCollectGeneration(),
                    record.GetCollectStep(), record.GetChannel(), TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie,
                    TLogoBlobID::MaxPartId, TLogoBlobID::MaxCrcMode));

                Self->Data->ScanRange(&first, &last, TData::EScanFlags::INCLUDE_BEGIN | TData::EScanFlags::INCLUDE_END,
                    processKey);

                if (NumKeysProcessed == MaxKeysToProcessAtOnce) {
                    return false;
                }

                const auto key = std::make_tuple(record.GetTabletId(), record.GetChannel());
                auto& barrier = Self->BarrierServer->Barriers[key];
                TGenStep& barrierGenCtr = record.GetHard() ? barrier.HardGenCtr : barrier.SoftGenCtr;
                TGenStep& barrierGenStep = record.GetHard() ? barrier.Hard : barrier.Soft;

                const TGenStep genCtr(record.GetGeneration(), record.GetPerGenerationCounter());
                const TGenStep collectGenStep(record.GetCollectGeneration(), record.GetCollectStep());
                Y_VERIFY(barrierGenCtr <= genCtr);
                barrierGenCtr = genCtr;
                Y_VERIFY(barrierGenStep <= collectGenStep);
                barrierGenStep = collectGenStep;

                if (record.GetHard()) {
                    db.Table<Schema::Barriers>().Key(record.GetTabletId(), record.GetChannel()).Update(
                        NIceDb::TUpdate<Schema::Barriers::HardGenCtr>(ui64(genCtr)),
                        NIceDb::TUpdate<Schema::Barriers::Hard>(ui64(collectGenStep))
                    );
                } else {
                    db.Table<Schema::Barriers>().Key(record.GetTabletId(), record.GetChannel()).Update(
                        NIceDb::TUpdate<Schema::Barriers::SoftGenCtr>(ui64(genCtr)),
                        NIceDb::TUpdate<Schema::Barriers::Soft>(ui64(collectGenStep))
                    );
                }
            }

            return true;
        }
    };

    void TBlobDepot::TBarrierServer::AddBarrierOnLoad(ui64 tabletId, ui8 channel, TGenStep softGenCtr, TGenStep soft,
            TGenStep hardGenCtr, TGenStep hard) {
        Barriers[std::make_tuple(tabletId, channel)] = {
            .SoftGenCtr = softGenCtr,
            .Soft = soft,
            .HardGenCtr = hardGenCtr,
            .Hard = hard,
        };
    }

    void TBlobDepot::TBarrierServer::AddBarrierOnDecommit(const TEvBlobStorage::TEvAssimilateResult::TBarrier& barrier,
            NTabletFlatExecutor::TTransactionContext& txc) {
        NIceDb::TNiceDb db(txc.DB);

        const auto key = std::make_tuple(barrier.TabletId, barrier.Channel);
        auto& current = Barriers[key];
#define DO(TYPE) \
        { \
            const TGenStep barrierGenCtr(barrier.TYPE.RecordGeneration, barrier.TYPE.PerGenerationCounter); \
            const TGenStep barrierCollect(barrier.TYPE.CollectGeneration, barrier.TYPE.CollectStep); \
            if (current.TYPE##GenCtr < barrierGenCtr) { \
                if (current.TYPE <= barrierCollect) { \
                    current.TYPE##GenCtr = barrierGenCtr; \
                    current.TYPE = barrierCollect; \
                    db.Table<Schema::Barriers>().Key(barrier.TabletId, barrier.Channel).Update( \
                        NIceDb::TUpdate<Schema::Barriers::TYPE##GenCtr>(ui64(barrierGenCtr)), \
                        NIceDb::TUpdate<Schema::Barriers::TYPE>(ui64(barrierCollect)) \
                    ); \
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT45, "replacing " #TYPE " barrier through decommission", \
                        (TabletId, barrier.TabletId), (Channel, int(barrier.Channel)), \
                        (GenCtr, current.TYPE##GenCtr), (Collect, current.TYPE), \
                        (Barrier, barrier)); \
                } else { \
                    STLOG(PRI_ERROR, BLOB_DEPOT, BDT36, "decreasing " #TYPE " barrier through decommission", \
                        (TabletId, barrier.TabletId), (Channel, int(barrier.Channel)), \
                        (GenCtr, current.TYPE##GenCtr), (Collect, current.TYPE), \
                        (Barrier, barrier)); \
                } \
            } else if (current.TYPE##GenCtr == barrierGenCtr && current.TYPE != barrierCollect) { \
                STLOG(PRI_ERROR, BLOB_DEPOT, BDT43, "barrier value mismatch through decommission", \
                    (TabletId, barrier.TabletId), (Channel, int(barrier.Channel)), \
                    (GenCtr, current.TYPE##GenCtr), (Collect, current.TYPE), \
                    (Barrier, barrier)); \
            } \
        }

        DO(Hard)
        DO(Soft)
    }

    void TBlobDepot::TBarrierServer::Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev) {
        const auto& record = ev->Get()->Record;
        const auto key = std::make_tuple(record.GetTabletId(), record.GetChannel());
        auto& barrier = Barriers[key];
        barrier.ProcessingQ.emplace_back(ev.Release());
        if (Self->Data->IsLoaded() && barrier.ProcessingQ.size() == 1) {
            Self->Execute(std::make_unique<TTxCollectGarbage>(Self, record.GetTabletId(), record.GetChannel()));
        }
    }

    void TBlobDepot::TBarrierServer::GetBlobBarrierRelation(TLogoBlobID id, bool *underSoft, bool *underHard) const {
        const auto it = Barriers.find(std::make_tuple(id.TabletID(), id.Channel()));
        const TGenStep genStep(id);
        *underSoft = it == Barriers.end() ? false : genStep <= it->second.Soft;
        *underHard = it == Barriers.end() ? false : genStep <= it->second.Hard;
    }

    void TBlobDepot::TBarrierServer::OnDataLoaded() {
        for (auto& [key, barrier] : Barriers) {
            if (!barrier.ProcessingQ.empty()) {
                const auto& [tabletId, channel] = key;
                Self->Execute(std::make_unique<TTxCollectGarbage>(Self, tabletId, channel));
            }
        }
    }

} // NKikimr::NBlobDepot
