#include "garbage_collection.h"
#include "schema.h"
#include "data.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TBarrierServer::TTxCollectGarbage : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        std::optional<TString> Error;

        std::unique_ptr<TEvBlobDepot::TEvCollectGarbage::THandle> Request;
        int KeepIndex = 0;
        int DoNotKeepIndex = 0;
        ui32 NumKeysProcessed = 0;
        bool Done = false;

        static constexpr ui32 MaxKeysToProcessAtOnce = 10'000;

    public:
        TTxCollectGarbage(TBlobDepot *self, std::unique_ptr<TEvBlobDepot::TEvCollectGarbage::THandle> request,
                ui32 keepIndex = 0, ui32 doNotKeepIndex = 0)
            : TTransactionBase(self)
            , Request(std::move(request))
            , KeepIndex(keepIndex)
            , DoNotKeepIndex(doNotKeepIndex)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
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
                Self->Data->HandleTrash();
            } else {
                Self->Execute(std::make_unique<TTxCollectGarbage>(Self, std::move(Request), KeepIndex, DoNotKeepIndex));
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
                    const auto key = std::make_pair(record.GetTabletId(), record.GetChannel());
                    auto& barriers = Self->BarrierServer->Barriers;
                    if (const auto it = barriers.find(key); it != barriers.end()) {
                        auto& barrier = it->second;
                        const ui64 recordGenStep = GenStep(record.GetGeneration(), record.GetPerGenerationCounter());
                        const ui64 collectGenStep = GenStep(record.GetCollectGeneration(), record.GetCollectStep());
                        ui64& currentGenStep = record.GetHard() ? barrier.Hard : barrier.Soft;
                        if (recordGenStep < barrier.LastRecordGenStep) {
                            Error = "record generation:counter is obsolete";
                        } else if (recordGenStep == barrier.LastRecordGenStep) {
                            if (currentGenStep != collectGenStep) {
                                Error = "repeated command with different collect parameters received";
                            }
                        } else if (collectGenStep < currentGenStep) {
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
                        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT01, "DeleteKey", (TabletId, Self->TabletID()), (BlobId, id));
                        db.Table<Schema::Data>().Key(key.MakeBinaryKey()).Delete();
                        auto updateTrash = [&](TLogoBlobID id) {
                            db.Table<Schema::Trash>().Key(TString(id.AsBinaryString())).Update();
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

                const auto key = std::make_pair(record.GetTabletId(), record.GetChannel());
                auto& barriers = Self->BarrierServer->Barriers;
                auto& barrier = barriers[key];
                const ui64 recordGenStep = GenStep(record.GetGeneration(), record.GetPerGenerationCounter());
                const ui64 collectGenStep = GenStep(record.GetCollectGeneration(), record.GetCollectStep());
                ui64& currentGenStep = record.GetHard() ? barrier.Hard : barrier.Soft;
                Y_VERIFY(barrier.LastRecordGenStep <= recordGenStep);
                barrier.LastRecordGenStep = recordGenStep;
                Y_VERIFY(currentGenStep <= collectGenStep);
                currentGenStep = collectGenStep;

                db.Table<Schema::Barriers>().Key(record.GetTabletId(), record.GetChannel()).Update(
                    NIceDb::TUpdate<Schema::Barriers::LastRecordGenStep>(recordGenStep),
                    NIceDb::TUpdate<Schema::Barriers::Soft>(barrier.Soft),
                    NIceDb::TUpdate<Schema::Barriers::Hard>(barrier.Hard)
                );
            }

            return true;
        }
    };

    void TBlobDepot::TBarrierServer::AddBarrierOnLoad(ui64 tabletId, ui8 channel, ui64 lastRecordGenStep,
            ui64 soft, ui64 hard) {
        Barriers[std::make_pair(tabletId, channel)] = {
            .LastRecordGenStep = lastRecordGenStep,
            .Soft = soft,
            .Hard = hard,
        };
    }

    void TBlobDepot::TBarrierServer::Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev) {
        Self->Execute(std::make_unique<TTxCollectGarbage>(Self,
            std::unique_ptr<TEvBlobDepot::TEvCollectGarbage::THandle>(ev.Release())));
    }

    bool TBlobDepot::TBarrierServer::CheckBlobForBarrier(TLogoBlobID id) const {
        const auto key = std::make_pair(id.TabletID(), id.Channel());
        const auto it = Barriers.find(key);
        return it == Barriers.end() || GenStep(id) > Max(it->second.Soft, it->second.Hard);
    }

    void TBlobDepot::TBarrierServer::GetBlobBarrierRelation(TLogoBlobID id, bool *underSoft, bool *underHard) const {
        const auto key = std::make_pair(id.TabletID(), id.Channel());
        const auto it = Barriers.find(key);
        const ui64 genStep = GenStep(id);
        *underSoft = it == Barriers.end() ? false : genStep <= it->second.Soft;
        *underHard = it == Barriers.end() ? false : genStep <= it->second.Hard;
    }

} // NKikimr::NBlobDepot
