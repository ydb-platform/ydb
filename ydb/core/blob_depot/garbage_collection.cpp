#include "blob_depot_tablet.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    namespace {
        static ui64 GenStep(ui32 gen, ui32 step) {
            return static_cast<ui64>(gen) << 32 | step;
        }
    }

    class TBlobDepot::TGarbageCollectionManager {
        TBlobDepot* const Self;

        struct TBarrier {
            ui64 LastRecordGenStep = 0;
            ui64 Soft = 0;
            ui64 Hard = 0;
        };

        THashMap<std::pair<ui64, ui8>, TBarrier> Barriers;

    private:
        class TTxCollectGarbage : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            std::optional<TString> Error;

            std::unique_ptr<TEvBlobDepot::TEvCollectGarbage::THandle> Request;
            int KeepIndex = 0;
            int DoNotKeepIndex = 0;
            ui32 NumKeysProcessed = 0;
            std::vector<TString> KeysToDelete;
            std::vector<std::pair<TString, NKikimrBlobDepot::EKeepState>> KeepStateUpdates;
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
                Self->DeleteKeys(KeysToDelete);
                Self->UpdateKeepState(KeepStateUpdates);

                if (Done || Error) {
                    if (Done) {
                        ApplyBarrier();
                    }
                    auto [response, _] = TEvBlobDepot::MakeResponseFor(*Request, Self->SelfId(),
                        Error ? NKikimrProto::ERROR : NKikimrProto::OK, std::move(Error));
                    TActivationContext::Send(response.release());
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
                        auto& barriers = Self->GarbageCollectionManager->Barriers;
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
                    const TStringBuf key = id.AsBinaryString();
                    if (const auto& value = Self->UpdatesKeepState(key, state)) {
                        KeepStateUpdates.emplace_back(key, state);
                        db.Table<Schema::Data>().Key(TString(key)).Update<Schema::Data::Value>(*value);
                        ++NumKeysProcessed;
                    }
                }

                return index == items.size();
            }

            bool ProcessBarrier(TTransactionContext& txc) {
                NIceDb::TNiceDb db(txc.DB);

                const auto& record = Request->Get()->Record;
                if (record.HasCollectGeneration() && record.HasCollectStep()) {
                    const TLogoBlobID first(record.GetTabletId(), 0, 0, record.GetChannel(), 0, 0);
                    const TLogoBlobID last(record.GetTabletId(), record.GetCollectGeneration(), record.GetCollectStep(),
                        record.GetChannel(), TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId,
                        TLogoBlobID::MaxCrcMode);
                    const bool hard = record.GetHard();

                    auto processKey = [&](TStringBuf key, const TDataValue& value) {
                        if (value.KeepState != NKikimrBlobDepot::EKeepState::Keep || hard) {
                            db.Table<Schema::Data>().Key(TString(key)).Delete();
                            KeysToDelete.emplace_back(key);
                            ++NumKeysProcessed;
                        }

                        return NumKeysProcessed < MaxKeysToProcessAtOnce;
                    };

                    Self->ScanRange(first.AsBinaryString(), last.AsBinaryString(), EScanFlags::INCLUDE_BEGIN | EScanFlags::INCLUDE_END,
                        processKey);

                    if (NumKeysProcessed == MaxKeysToProcessAtOnce) {
                        return false;
                    }

                    auto row = db.Table<Schema::Barriers>().Key(record.GetTabletId(), record.GetChannel());
                    const ui64 collectGenStep = GenStep(record.GetCollectGeneration(), record.GetCollectStep());
                    if (record.GetHard()) {
                        row.Update<Schema::Barriers::Hard>(collectGenStep);
                    } else {
                        row.Update<Schema::Barriers::Soft>(collectGenStep);
                    }
                    row.Update<Schema::Barriers::LastRecordGenStep>(GenStep(record.GetGeneration(), record.GetPerGenerationCounter()));
                }

                return true;
            }

            void ApplyBarrier() {
                const auto& record = Request->Get()->Record;
                if (record.HasCollectGeneration() && record.HasCollectStep()) {
                    const auto key = std::make_pair(record.GetTabletId(), record.GetChannel());
                    auto& barriers = Self->GarbageCollectionManager->Barriers;
                    auto& barrier = barriers[key];
                    const ui64 recordGenStep = GenStep(record.GetGeneration(), record.GetPerGenerationCounter());
                    const ui64 collectGenStep = GenStep(record.GetCollectGeneration(), record.GetCollectStep());
                    ui64& currentGenStep = record.GetHard() ? barrier.Hard : barrier.Soft;
                    Y_VERIFY(barrier.LastRecordGenStep <= recordGenStep);
                    barrier.LastRecordGenStep = recordGenStep;
                    Y_VERIFY(currentGenStep <= collectGenStep);
                    currentGenStep = collectGenStep;
                }
            }
        };

    public:
        TGarbageCollectionManager(TBlobDepot *self)
            : Self(self)
        {}

        void Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev) {
            std::unique_ptr<TEvBlobDepot::TEvCollectGarbage::THandle> uniq(ev.Release());
            Self->Execute(std::make_unique<TTxCollectGarbage>(Self, std::move(uniq)));
        }

        bool CheckBlobForBarrier(TLogoBlobID id) const {
            const auto key = std::make_pair(id.TabletID(), id.Channel());
            const auto it = Barriers.find(key);
            const ui64 genstep = static_cast<ui64>(id.Generation()) << 32 | id.Step();
            return it == Barriers.end() || genstep > Max(it->second.Soft, it->second.Hard);
        }
    };

    TBlobDepot::TGarbageCollectionManagerPtr TBlobDepot::CreateGarbageCollectionManager() {
        return {new TGarbageCollectionManager{this}, std::default_delete<TGarbageCollectionManager>{}};
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev) {
        GarbageCollectionManager->Handle(ev);
    }

    bool TBlobDepot::CheckBlobForBarrier(TLogoBlobID id) const {
        return GarbageCollectionManager->CheckBlobForBarrier(id);
    }

} // NKikimr::NBlobDepot
