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
            , Barrier(Self->BarrierServer->Barriers[std::make_pair(tabletId, channel)])
            , Request(Barrier.ProcessingQ.front())
            , TabletId(tabletId)
            , Channel(channel)
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
                    const auto key = std::make_pair(record.GetTabletId(), record.GetChannel());
                    auto& barriers = Self->BarrierServer->Barriers;
                    if (const auto it = barriers.find(key); it != barriers.end()) {
                        // extract existing barrier record
                        auto& barrier = it->second;
                        const auto barrierGenCounter = std::make_tuple(barrier.RecordGeneration, barrier.PerGenerationCounter);
                        const TGenStep barrierGenStep = record.GetHard() ? barrier.Hard : barrier.Soft;

                        // extract new parameters from protobuf
                        const auto genCounter = std::make_tuple(record.GetGeneration(), record.GetPerGenerationCounter());
                        const TGenStep collectGenStep(record.GetCollectGeneration(), record.GetCollectStep());

                        // validate them
                        if (genCounter < barrierGenCounter) {
                            Error = "record generation:counter is obsolete";
                        } else if (genCounter == barrierGenCounter) {
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
                        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT11, "DeleteKey", (TabletId, Self->TabletID()), (BlobId, id));
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
                auto barrierGenCounter = std::tie(barrier.RecordGeneration, barrier.PerGenerationCounter);
                TGenStep& barrierGenStep = record.GetHard() ? barrier.Hard : barrier.Soft;

                const auto genCounter = std::make_tuple(record.GetGeneration(), record.GetPerGenerationCounter());
                const TGenStep collectGenStep(record.GetCollectGeneration(), record.GetCollectStep());
                Y_VERIFY(barrierGenCounter <= genCounter);
                barrierGenCounter = genCounter;
                Y_VERIFY(barrierGenStep <= collectGenStep);
                barrierGenStep = collectGenStep;

                db.Table<Schema::Barriers>().Key(record.GetTabletId(), record.GetChannel()).Update(
                    NIceDb::TUpdate<Schema::Barriers::RecordGeneration>(std::get<0>(genCounter)),
                    NIceDb::TUpdate<Schema::Barriers::PerGenerationCounter>(std::get<1>(genCounter)),
                    NIceDb::TUpdate<Schema::Barriers::Soft>(ui64(barrier.Soft)),
                    NIceDb::TUpdate<Schema::Barriers::Hard>(ui64(barrier.Hard))
                );
            }

            return true;
        }
    };

    void TBlobDepot::TBarrierServer::AddBarrierOnLoad(ui64 tabletId, ui8 channel, ui32 recordGeneration, ui32 perGenerationCounter,
            TGenStep soft, TGenStep hard) {
        Barriers[std::make_pair(tabletId, channel)] = {
            .RecordGeneration = recordGeneration,
            .PerGenerationCounter = perGenerationCounter,
            .Soft = soft,
            .Hard = hard,
        };
    }

    void TBlobDepot::TBarrierServer::Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev) {
        const auto& record = ev->Get()->Record;
        const auto key = std::make_pair(record.GetTabletId(), record.GetChannel());
        auto& barrier = Barriers[key];
        barrier.ProcessingQ.emplace_back(ev.Release());
        if (barrier.ProcessingQ.size() == 1) {
            Self->Execute(std::make_unique<TTxCollectGarbage>(Self, record.GetTabletId(), record.GetChannel()));
        }
    }

    bool TBlobDepot::TBarrierServer::CheckBlobForBarrier(TLogoBlobID id) const {
        const auto it = Barriers.find(std::make_pair(id.TabletID(), id.Channel()));
        return it == Barriers.end() || TGenStep(id) > Max(it->second.Soft, it->second.Hard);
    }

    void TBlobDepot::TBarrierServer::GetBlobBarrierRelation(TLogoBlobID id, bool *underSoft, bool *underHard) const {
        const auto it = Barriers.find(std::make_pair(id.TabletID(), id.Channel()));
        const TGenStep genStep(id);
        *underSoft = it == Barriers.end() ? false : genStep <= it->second.Soft;
        *underHard = it == Barriers.end() ? false : genStep <= it->second.Hard;
    }

} // NKikimr::NBlobDepot
