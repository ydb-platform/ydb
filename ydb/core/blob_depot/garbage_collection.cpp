#include "garbage_collection.h"
#include "schema.h"
#include "data.h"

namespace NKikimr::NBlobDepot {

    using TAdvanceBarrierCallback = std::function<void(std::optional<TString>)>;

    class TBlobDepot::TBarrierServer::TTxAdvanceBarrier : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        static constexpr ui32 MaxKeysToProcessAtOnce = 10'000;

        const ui64 TabletId;
        const ui8 Channel;
        const bool Hard;
        const TGenStep GenCtr;
        const TGenStep CollectGenStep;
        TAdvanceBarrierCallback Callback;

        bool MoreData = false;
        bool Success = false;

    public:
        TTxAdvanceBarrier(TBlobDepot *self, ui64 tabletId, ui8 channel, bool hard, TGenStep genCtr, TGenStep collectGenStep,
                TAdvanceBarrierCallback callback)
            : TTransactionBase(self)
            , TabletId(tabletId)
            , Channel(channel)
            , Hard(hard)
            , GenCtr(genCtr)
            , CollectGenStep(collectGenStep)
            , Callback(std::move(callback))
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            Y_VERIFY(Self->Data->IsLoaded());

            const auto key = std::make_tuple(TabletId, Channel);
            auto& barriers = Self->BarrierServer->Barriers;
            const auto [it, inserted] = barriers.try_emplace(key);
            if (!inserted) {
                // extract existing barrier record
                auto& barrier = it->second;
                const TGenStep barrierGenCtr = Hard ? barrier.HardGenCtr : barrier.SoftGenCtr;
                const TGenStep barrierGenStep = Hard ? barrier.Hard : barrier.Soft;

                // validate them
                if (GenCtr < barrierGenCtr) {
                    Callback("record generation:counter is obsolete");
                    return true;
                } else if (GenCtr == barrierGenCtr) {
                    if (barrierGenStep != CollectGenStep) {
                        Callback("repeated command with different collect parameters received");
                        return true;
                    }
                } else if (CollectGenStep < barrierGenStep) {
                    Callback("decreasing barrier");
                    return true;
                }
            }

            TBarrier& barrier = it->second;
            TGenStep& barrierGenCtr = Hard ? barrier.HardGenCtr : barrier.SoftGenCtr;
            TGenStep& barrierGenStep = Hard ? barrier.Hard : barrier.Soft;

            Y_VERIFY(barrierGenCtr <= GenCtr);
            Y_VERIFY(barrierGenStep <= CollectGenStep);

            ui32 maxItems = MaxKeysToProcessAtOnce;
            if (!Self->Data->OnBarrierShift(TabletId, Channel, Hard, barrierGenStep, CollectGenStep, maxItems, txc, this)) {
                MoreData = true;
                return true;
            }

            barrierGenCtr = GenCtr;
            barrierGenStep = CollectGenStep;

            Self->BarrierServer->ValidateBlobInvariant(TabletId, Channel);

            auto row = NIceDb::TNiceDb(txc.DB).Table<Schema::Barriers>().Key(TabletId, Channel);
            if (Hard) {
                row.Update(
                    NIceDb::TUpdate<Schema::Barriers::HardGenCtr>(ui64(GenCtr)),
                    NIceDb::TUpdate<Schema::Barriers::Hard>(ui64(CollectGenStep))
                );
            } else {
                row.Update(
                    NIceDb::TUpdate<Schema::Barriers::SoftGenCtr>(ui64(GenCtr)),
                    NIceDb::TUpdate<Schema::Barriers::Soft>(ui64(CollectGenStep))
                );
            }

            Success = true;
            return true;
        }

        void Complete(const TActorContext&) override {
            Self->Data->CommitTrash(this);

            if (MoreData) {
                Self->Execute(std::make_unique<TTxAdvanceBarrier>(Self, TabletId, Channel, Hard, GenCtr, CollectGenStep,
                    std::move(Callback)));
            } else if (Success) {
                Callback(std::nullopt);
            }
        }
    };

    class TBlobDepot::TBarrierServer::TTxCollectGarbage : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        std::optional<TString> Error;

        TBarrier& Barrier;
        std::unique_ptr<TEvBlobDepot::TEvCollectGarbage::THandle>& Request;
        const ui64 TabletId;
        const ui8 Channel;
        int KeepIndex = 0;
        int DoNotKeepIndex = 0;

        ui32 NumKeysProcessed = 0;
        bool MoreData = false;

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
            auto& record = Request->Get()->Record;
            MoreData = !ProcessFlags(txc, KeepIndex, record.GetKeep(), NKikimrBlobDepot::EKeepState::Keep)
                || !ProcessFlags(txc, DoNotKeepIndex, record.GetDoNotKeep(), NKikimrBlobDepot::EKeepState::DoNotKeep);
            return true;
        }

        void Complete(const TActorContext&) override {
            if (MoreData) {
                Self->Execute(std::make_unique<TTxCollectGarbage>(Self, TabletId, Channel, KeepIndex, DoNotKeepIndex));
            } else if (auto& record = Request->Get()->Record; record.HasCollectGeneration() && record.HasCollectStep()) {
                Self->Execute(std::make_unique<TTxAdvanceBarrier>(Self, TabletId, Channel, record.GetHard(),
                        TGenStep(record.GetGeneration(), record.GetPerGenerationCounter()),
                        TGenStep(record.GetCollectGeneration(), record.GetCollectStep()),
                        [self = Self, tabletId = TabletId, channel = Channel](std::optional<TString> error) {
                    self->BarrierServer->FinishRequest(tabletId, channel, std::move(error));
                }));
            } else if (record.HasCollectGeneration() || record.HasCollectStep()) {
                Self->BarrierServer->FinishRequest(TabletId, Channel, "incorrect CollectGeneration/CollectStep setting");
            } else { // do not collect anything here
                Self->BarrierServer->FinishRequest(TabletId, Channel, std::nullopt);
            }
        }

        bool ProcessFlags(TTransactionContext& txc, int& index,
                const NProtoBuf::RepeatedPtrField<NKikimrProto::TLogoBlobID>& items,
                NKikimrBlobDepot::EKeepState state) {
            NIceDb::TNiceDb db(txc.DB);
            for (; index < items.size() && NumKeysProcessed < MaxKeysToProcessAtOnce; ++index) {
                const auto id = LogoBlobIDFromLogoBlobID(items[index]);
                TData::TKey key(id);
                NumKeysProcessed += Self->Data->UpdateKeepState(key, state, txc, this);
            }
            return index == items.size();
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

        Self->BarrierServer->ValidateBlobInvariant(tabletId, channel);
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

    void TBlobDepot::TBarrierServer::ValidateBlobInvariant(ui64 tabletId, ui8 channel) {
#ifndef NDEBUG
        for (const bool hard : {true, false}) {
            const auto it = Barriers.find(std::make_tuple(tabletId, channel));
            Y_VERIFY(it != Barriers.end());
            const TBarrier& barrier = it->second;
            const TGenStep& barrierGenStep = hard ? barrier.Hard : barrier.Soft;
            const TData::TKey first(TLogoBlobID(tabletId, 0, 0, channel, 0, 0));
            const TData::TKey last(TLogoBlobID(tabletId, barrierGenStep.Generation(), barrierGenStep.Step(),
                channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId,
                TLogoBlobID::MaxCrcMode));
            Self->Data->ScanRange(&first, &last, TData::EScanFlags::INCLUDE_BEGIN | TData::EScanFlags::INCLUDE_END,
                    [&](const TData::TKey& key, const TData::TValue& value) {
                // there must be no blobs under the hard barrier and no blobs with mode other than Keep under the soft one
                Y_VERIFY_S(!hard && value.KeepState == NKikimrBlobDepot::EKeepState::Keep, "Key# " << key.ToString()
                    << " Value# " << value.ToString());
                return true;
            });
        }
#   if 0
        Self->Data->ScanRange(nullptr, nullptr, {}, [&](const TData::TKey& key, const TData::TValue& value) {
            bool underSoft, underHard;
            Self->BarrierServer->GetBlobBarrierRelation(key.GetBlobId(), &underSoft, &underHard);
            Y_VERIFY(!underHard && (!underSoft || value.KeepState == NKikimrBlobDepot::EKeepState::Keep));
            return true;
        });
#   endif
#else
        Y_UNUSED(tabletId);
        Y_UNUSED(channel);
#endif
    }

    void TBlobDepot::TBarrierServer::FinishRequest(ui64 tabletId, ui8 channel, std::optional<TString> error) {
        const auto key = std::make_tuple(tabletId, channel);
        auto& barrier = Barriers[key];
        Y_VERIFY(!barrier.ProcessingQ.empty());
        auto& request = barrier.ProcessingQ.front();
        auto [response, _] = TEvBlobDepot::MakeResponseFor(*request, Self->SelfId(),
            error ? NKikimrProto::ERROR : NKikimrProto::OK, std::move(error));
        TActivationContext::Send(response.release());
        barrier.ProcessingQ.pop_front();
        Self->Data->HandleTrash();
        if (!barrier.ProcessingQ.empty()) {
            Self->Execute(std::make_unique<TTxCollectGarbage>(Self, tabletId, channel));
        }
    }

} // NKikimr::NBlobDepot
