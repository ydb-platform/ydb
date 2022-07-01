#include "blob_depot_tablet.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TGarbageCollectionManager {
        TBlobDepot *Self;

    private:
        class TTxCollectGarbage : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            std::unique_ptr<TEvBlobDepot::TEvCollectGarbage::THandle> Request;
            ui32 KeepIndex = 0;
            ui32 DoNotKeepIndex = 0;
            ui32 NumKeysProcessed = 0;
            std::vector<TString> KeysToDelete;
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
                Done = ProcessKeepFlags(txc) && ProcessDoNotKeepFlags(txc) && ApplyBarrier(txc);
                return true;
            }

            void Complete(const TActorContext&) override {
                Self->DeleteKeys(KeysToDelete);
                if (Done) {
                    auto [response, _] = TEvBlobDepot::MakeResponseFor(*Request, Self->SelfId(), NKikimrProto::OK, std::nullopt);
                    TActivationContext::Send(response.release());
                } else {
                    Self->Execute(std::make_unique<TTxCollectGarbage>(Self, std::move(Request), KeepIndex, DoNotKeepIndex));
                }
            }

            bool ProcessKeepFlags(TTransactionContext& /*txc*/) {
                const auto& record = Request->Get()->Record;
                while (KeepIndex < record.KeepSize() && NumKeysProcessed < MaxKeysToProcessAtOnce) {
                    ++KeepIndex;
                    ++NumKeysProcessed;
                }

                return KeepIndex == record.KeepSize();
            }

            bool ProcessDoNotKeepFlags(TTransactionContext& /*txc*/) {
                const auto& record = Request->Get()->Record;
                while (DoNotKeepIndex < record.DoNotKeepSize() && NumKeysProcessed < MaxKeysToProcessAtOnce) {
                    ++DoNotKeepIndex;
                    ++NumKeysProcessed;
                }

                return DoNotKeepIndex == record.DoNotKeepSize();
            }

            bool ApplyBarrier(TTransactionContext& txc) {
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
                }

                return true;
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
    };

    TBlobDepot::TGarbageCollectionManagerPtr TBlobDepot::CreateGarbageCollectionManager() {
        return {new TGarbageCollectionManager{this}, std::default_delete<TGarbageCollectionManager>{}};
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev) {
        GarbageCollectionManager->Handle(ev);
    }

} // NKikimr::NBlobDepot
