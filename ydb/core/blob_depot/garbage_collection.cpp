#include "garbage_collection.h"
#include "schema.h"
#include "data.h"
#include "blocks.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TBarrierServer::TTxCollectGarbage : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        std::unique_ptr<TEvBlobDepot::TEvCollectGarbage::THandle> Request;
        int KeepIndex = 0;
        int DoNotKeepIndex = 0;

        ui32 MaxItems = 10'000;

        bool Finished = false;
        bool MoreData = false;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_COLLECT_GARBAGE; }

        TTxCollectGarbage(TBlobDepot *self, std::unique_ptr<TEvBlobDepot::TEvCollectGarbage::THandle> request)
            : TTransactionBase(self)
            , Request(std::move(request))
        {}

        TTxCollectGarbage(TTxCollectGarbage& predecessor)
            : TTransactionBase(predecessor.Self)
            , Request(std::move(predecessor.Request))
            , KeepIndex(predecessor.KeepIndex)
            , DoNotKeepIndex(predecessor.DoNotKeepIndex)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT77, "TTxCollectGarbage::Execute", (Id, Self->GetLogId()),
                (Sender, Request->Sender), (Cookie, Request->Cookie));

            Y_ABORT_UNLESS(Self->Data->IsLoaded());

            if (!ValidateRequest()) {
                return true;
            }

            auto& record = Request->Get()->Record;

            MoreData = !ProcessFlags(txc, KeepIndex, record.GetKeep(), NKikimrBlobDepot::EKeepState::Keep) ||
                !ProcessFlags(txc, DoNotKeepIndex, record.GetDoNotKeep(), NKikimrBlobDepot::EKeepState::DoNotKeep) ||
                !ProcessBarrier(txc);

            return true;
        }

        void Complete(const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT78, "TTxCollectGarbage::Complete", (Id, Self->GetLogId()),
                (Sender, Request->Sender), (Cookie, Request->Cookie), (Finished, Finished), (MoreData, MoreData));

            Self->Data->CommitTrash(this);

            if (Finished) { // do nothing, error has already been reported
            } else if (MoreData) { // resume processing in next transaction
                Self->Execute(std::make_unique<TTxCollectGarbage>(*this));
            } else {
                Finish(std::nullopt);
            }
        }

        bool ProcessFlags(TTransactionContext& txc, int& index,
                const NProtoBuf::RepeatedPtrField<NKikimrProto::TLogoBlobID>& items,
                NKikimrBlobDepot::EKeepState state) {
            NIceDb::TNiceDb db(txc.DB);
            for (; index < items.size() && MaxItems; ++index) {
                MaxItems -= Self->Data->UpdateKeepState(TData::TKey(LogoBlobIDFromLogoBlobID(items[index])), state, txc, this);
            }
            return index == items.size();
        }

        bool ProcessBarrier(TTransactionContext& txc) {
            const auto& record = Request->Get()->Record;
            if (!record.HasCollectGeneration() || !record.HasCollectStep()) {
                return true; // no barrier, nothing to do
            }

            const ui64 tabletId = record.GetTabletId();
            const ui8 channel = record.GetChannel();
            const bool hard = record.GetHard();
            const TGenStep genCtr(record.GetGeneration(), record.GetPerGenerationCounter());
            const TGenStep collectGenStep(record.GetCollectGeneration(), record.GetCollectStep());

            const auto key = std::make_tuple(tabletId, channel);
            TBarrier& barrier = Self->BarrierServer->Barriers[key];
            TGenStep& barrierGenCtr = hard ? barrier.HardGenCtr : barrier.SoftGenCtr;
            TGenStep& barrierGenStep = hard ? barrier.Hard : barrier.Soft;

            if (genCtr < barrierGenCtr) { // obsolete barrier command, just ignore the barrier
                return true;
            }

            Y_ABORT_UNLESS(barrierGenCtr <= genCtr);
            Y_ABORT_UNLESS(barrierGenStep <= collectGenStep);

            if (!Self->Data->OnBarrierShift(tabletId, channel, hard, barrierGenStep, collectGenStep, MaxItems, txc, this)) {
                return false;
            }

            barrierGenCtr = genCtr;
            barrierGenStep = collectGenStep;

            Self->BarrierServer->ValidateBlobInvariant(tabletId, channel);

            auto row = NIceDb::TNiceDb(txc.DB).Table<Schema::Barriers>().Key(tabletId, channel);
            if (hard) {
                row.Update(
                    NIceDb::TUpdate<Schema::Barriers::HardGenCtr>(ui64(genCtr)),
                    NIceDb::TUpdate<Schema::Barriers::Hard>(ui64(collectGenStep))
                );
            } else {
                row.Update(
                    NIceDb::TUpdate<Schema::Barriers::SoftGenCtr>(ui64(genCtr)),
                    NIceDb::TUpdate<Schema::Barriers::Soft>(ui64(collectGenStep))
                );
            }

            return true;
        }

        bool ValidateRequest() {
            const auto& record = Request->Get()->Record;

            const ui64 tabletId = record.GetTabletId();
            const ui32 generation = record.GetGeneration();
            if (!Self->BlocksManager->CheckBlock(tabletId, generation)) {
                Finish("block race detected", NKikimrProto::BLOCKED);
                return false;
            }

            if (!record.HasCollectGeneration() && !record.HasCollectStep()) {
                return true;
            } else if (!record.HasCollectGeneration() || !record.HasCollectStep()) {
                Finish("incorrect CollectGeneration/CollectStep pair");
                return false;
            }

            const ui8 channel = record.GetChannel();
            const auto key = std::make_tuple(tabletId, channel);
            auto& barriers = Self->BarrierServer->Barriers;
            if (const auto it = barriers.find(key); it != barriers.end()) {
                // extract existing barrier record
                auto& barrier = it->second;
                const bool hard = record.GetHard();
                const TGenStep barrierGenCtr = hard ? barrier.HardGenCtr : barrier.SoftGenCtr;
                const TGenStep barrierGenStep = hard ? barrier.Hard : barrier.Soft;
                const TGenStep genCtr(record.GetGeneration(), record.GetPerGenerationCounter());
                const TGenStep collectGenStep(record.GetCollectGeneration(), record.GetCollectStep());

                // validate them
                if (genCtr < barrierGenCtr) {
                    if (barrierGenStep < collectGenStep) {
                        Finish("incorrect barrier sequence");
                        return false;
                    }
                } else if (genCtr == barrierGenCtr) {
                    if (barrierGenStep != collectGenStep) {
                        Finish("repeated command with different collect parameters received");
                        return false;
                    }
                } else if (collectGenStep < barrierGenStep) {
                    Finish("decreasing barrier");
                    return false;
                }
            }

            return true;
        }

        void Finish(std::optional<TString> error, std::optional<NKikimrProto::EReplyStatus> status = {}) {
            Y_ABORT_UNLESS(!Finished);
            auto [response, _] = TEvBlobDepot::MakeResponseFor(*Request, status.value_or(error ? NKikimrProto::ERROR :
                NKikimrProto::OK), std::move(error));
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT82, "TTxCollectGarbage::Finish", (Id, Self->GetLogId()),
                (Sender, Request->Sender), (Cookie, Request->Cookie), (Error, error));
            TActivationContext::Send(response.release());
            Finished = true;
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

    bool TBlobDepot::TBarrierServer::AddBarrierOnDecommit(const TEvBlobStorage::TEvAssimilateResult::TBarrier& barrier,
            ui32& maxItems, NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        NIceDb::TNiceDb db(txc.DB);

        const auto key = std::make_tuple(barrier.TabletId, barrier.Channel);
        auto& current = Barriers[key];
#define DO(TYPE, HARD) \
        { \
            const TGenStep barrierGenCtr(barrier.TYPE.RecordGeneration, barrier.TYPE.PerGenerationCounter); \
            const TGenStep barrierCollect(barrier.TYPE.CollectGeneration, barrier.TYPE.CollectStep); \
            if (current.TYPE##GenCtr < barrierGenCtr) { \
                if (current.TYPE <= barrierCollect) { \
                    if (!Self->Data->OnBarrierShift(barrier.TabletId, barrier.Channel, HARD, current.TYPE, \
                            barrierCollect, maxItems, txc, cookie)) { \
                        return false; \
                    } \
                    current.TYPE##GenCtr = barrierGenCtr; \
                    current.TYPE = barrierCollect; \
                    db.Table<Schema::Barriers>().Key(barrier.TabletId, barrier.Channel).Update( \
                        NIceDb::TUpdate<Schema::Barriers::TYPE##GenCtr>(ui64(barrierGenCtr)), \
                        NIceDb::TUpdate<Schema::Barriers::TYPE>(ui64(barrierCollect)) \
                    ); \
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT45, "replaced " #TYPE " barrier through decommission", \
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

        DO(Hard, true)
        DO(Soft, false)

        Self->BarrierServer->ValidateBlobInvariant(barrier.TabletId, barrier.Channel);

        return true;
    }

    void TBlobDepot::TBarrierServer::Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev) {
        const auto& record = ev->Get()->Record;
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT74, "TBarrierServer::Handle(TEvCollectGarbage)", (Id, Self->GetLogId()),
            (Sender, ev->Sender), (Cookie, ev->Cookie), (Msg, record));
        if (Self->Data->IsLoaded()) {
            Self->Execute(std::make_unique<TTxCollectGarbage>(Self,
                std::unique_ptr<TEvBlobDepot::TEvCollectGarbage::THandle>(ev.Release())));
        } else {
            const auto key = std::make_tuple(record.GetTabletId(), record.GetChannel());
            Barriers[key].ProcessingQ.emplace_back(ev.Release());
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
            for (auto& ev : std::exchange(barrier.ProcessingQ, {})) {
                Self->Execute(std::make_unique<TTxCollectGarbage>(Self, std::move(ev)));
            }
        }
    }

    void TBlobDepot::TBarrierServer::ValidateBlobInvariant(ui64 tabletId, ui8 channel) {
#ifndef NDEBUG
        for (const bool hard : {true, false}) {
            const auto it = Barriers.find(std::make_tuple(tabletId, channel));
            Y_ABORT_UNLESS(it != Barriers.end());
            const TBarrier& barrier = it->second;
            const TGenStep& barrierGenStep = hard ? barrier.Hard : barrier.Soft;
            const TData::TKey first(TLogoBlobID(tabletId, 0, 0, channel, 0, 0));
            const TData::TKey last(TLogoBlobID(tabletId, barrierGenStep.Generation(), barrierGenStep.Step(),
                channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId,
                TLogoBlobID::MaxCrcMode));

            TData::TScanRange r{first, last, TData::EScanFlags::INCLUDE_BEGIN | TData::EScanFlags::INCLUDE_END};
            Self->Data->ScanRange(r, nullptr, nullptr, [&](const TData::TKey& key, const TData::TValue& value) {
                // there must be no blobs under the hard barrier and no blobs with mode other than Keep under the soft one
                Y_VERIFY_S(!hard && value.KeepState == NKikimrBlobDepot::EKeepState::Keep, "Key# " << key.ToString()
                    << " Value# " << value.ToString());
                return true;
            });
        }
#   if 0
        TData::TScanRange r{TData::TKey::Min(), TData::TKey::Max()};
        Self->Data->ScanRange(r, nullptr, nullptr, [&](const TData::TKey& key, const TData::TValue& value) {
            bool underSoft, underHard;
            Self->BarrierServer->GetBlobBarrierRelation(key.GetBlobId(), &underSoft, &underHard);
            Y_ABORT_UNLESS(!underHard && (!underSoft || value.KeepState == NKikimrBlobDepot::EKeepState::Keep));
            return true;
        });
#   endif
#else
        Y_UNUSED(tabletId);
        Y_UNUSED(channel);
#endif
    }

} // NKikimr::NBlobDepot
