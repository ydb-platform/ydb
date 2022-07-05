#include "blob_depot_tablet.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvCommitBlobSeq::TPtr ev) {
        class TTxCommitBlobSeq : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle> Request;
            std::vector<std::pair<TString, NKikimrBlobDepot::TValue>> UpdateQ;

        public:
            TTxCommitBlobSeq(TBlobDepot *self, std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle> request)
                : TTransactionBase(self)
                , Request(std::move(request))
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                NIceDb::TNiceDb db(txc.DB);

                for (const auto& item : Request->Get()->Record.GetItems()) {
                    NKikimrBlobDepot::TValue value;
                    if (item.HasMeta()) {
                        value.SetMeta(item.GetMeta());
                    }
                    auto *chain = value.AddValueChain();
                    chain->MutableLocator()->CopyFrom(item.GetBlobLocator());

                    TString valueData;
                    const bool success = value.SerializeToString(&valueData);
                    Y_VERIFY(success);

                    const TString& key = item.GetKey();
                    if (key.size() == 3 * sizeof(ui64)) {
                        const TLogoBlobID id(reinterpret_cast<const ui64*>(key.data()));
                        if (!Self->CheckBlobForBarrier(id)) {
                            continue; // FIXME: report error somehow (?)
                        }
                    }

                    db.Table<Schema::Data>().Key(item.GetKey()).Update<Schema::Data::Value>(valueData);
                    UpdateQ.emplace_back(item.GetKey(), std::move(value));
                }

                return true;
            }

            void Complete(const TActorContext&) override {
                auto [response, record] = TEvBlobDepot::MakeResponseFor(*Request, Self->SelfId());
                for (auto& [key, value] : UpdateQ) {
                    Self->PutKey(std::move(key), {
                        .Meta = value.GetMeta(),
                        .ValueChain = std::move(*value.MutableValueChain()),
                        .KeepState = value.GetKeepState(),
                        .Public = value.GetPublic(),
                    });
                    auto *responseItem = record->AddItems();
                    responseItem->SetStatus(NKikimrProto::OK);
                }
                TActivationContext::Send(response.release());
            }
        };

        Execute(std::make_unique<TTxCommitBlobSeq>(this, std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle>(
            ev.Release())));
    }

} // NKikimr::NBlobDepot
