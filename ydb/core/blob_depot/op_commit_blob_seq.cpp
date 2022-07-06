#include "blob_depot_tablet.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvCommitBlobSeq::TPtr ev) {
        class TTxCommitBlobSeq : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle> Request;
            std::unique_ptr<IEventHandle> Response;

        public:
            TTxCommitBlobSeq(TBlobDepot *self, std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle> request)
                : TTransactionBase(self)
                , Request(std::move(request))
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                NIceDb::TNiceDb db(txc.DB);

                NKikimrBlobDepot::TEvCommitBlobSeqResult *responseRecord;
                std::tie(Response, responseRecord) = TEvBlobDepot::MakeResponseFor(*Request, Self->SelfId());

                for (const auto& item : Request->Get()->Record.GetItems()) {
                    auto *responseItem = responseRecord->AddItems();
                    responseItem->SetStatus(NKikimrProto::OK);

                    NKikimrBlobDepot::TValue value;
                    if (item.HasMeta()) {
                        value.SetMeta(item.GetMeta());
                    }
                    auto *chain = value.AddValueChain();
                    chain->MutableLocator()->CopyFrom(item.GetBlobLocator());

                    const TString& key = item.GetKey();
                    if (key.size() == 3 * sizeof(ui64)) {
                        const TLogoBlobID id(reinterpret_cast<const ui64*>(key.data()));
                        if (!Self->CheckBlobForBarrier(id)) {
                            responseItem->SetStatus(NKikimrProto::ERROR);
                            responseItem->SetErrorReason(TStringBuilder() << "BlobId# " << id << " is being put beyond the barrier");
                            continue;
                        }
                    }

                    Self->PutKey(std::move(key), {
                        .Meta = value.GetMeta(),
                        .ValueChain = std::move(*value.MutableValueChain()),
                        .KeepState = value.GetKeepState(),
                        .Public = value.GetPublic(),
                    });

                    TString valueData;
                    const bool success = value.SerializeToString(&valueData);
                    Y_VERIFY(success);

                    db.Table<Schema::Data>().Key(item.GetKey()).Update<Schema::Data::Value>(valueData);
                }

                return true;
            }

            void Complete(const TActorContext&) override {
                TActivationContext::Send(Response.release());
            }
        };

        Execute(std::make_unique<TTxCommitBlobSeq>(this, std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle>(
            ev.Release())));
    }

} // NKikimr::NBlobDepot
