#include "blob_depot_tablet.h"
#include "schema.h"
#include "data.h"
#include "garbage_collection.h"

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

                TAgent& agent = Self->GetAgent(Request->Recipient);
                const ui32 generation = Self->Executor()->Generation();

                for (const auto& item : Request->Get()->Record.GetItems()) {
                    auto key = TData::TKey::FromBinaryKey(item.GetKey(), Self->Config);

                    auto *responseItem = responseRecord->AddItems();
                    responseItem->SetStatus(NKikimrProto::OK);

                    NKikimrBlobDepot::TValue value;
                    if (item.HasMeta()) {
                        value.SetMeta(item.GetMeta());
                    }
                    if (const auto keepState = Self->Data->GetKeepState(key); keepState != NKikimrBlobDepot::EKeepState::Default) {
                        value.SetKeepState(keepState);
                    }
                    auto *chain = value.AddValueChain();
                    auto *locator = chain->MutableLocator();
                    locator->CopyFrom(item.GetBlobLocator());

                    const auto blobSeqId = TBlobSeqId::FromProto(locator->GetBlobSeqId());
                    const bool canBeCollected = Self->Data->CanBeCollected(locator->GetGroupId(), blobSeqId);

                    if (blobSeqId.Generation == generation) {
                        // check for internal sanity -- we can't issue barriers on given ids without confirmed trimming
                        Y_VERIFY_S(!canBeCollected, "BlobSeqId# " << blobSeqId.ToString());

                        // mark given blob as committed only when it was issued in current generation -- only for this
                        // generation we have correct GivenIdRanges
                        MarkGivenIdCommitted(agent, blobSeqId);
                    } else if (canBeCollected) {
                        // we can't accept this record, because it is potentially under already issued barrier
                        responseItem->SetStatus(NKikimrProto::ERROR);
                        responseItem->SetErrorReason("generation race");
                        continue;
                    }

                    if (!CheckKeyAgainstBarrier(key, value)) {
                        responseItem->SetStatus(NKikimrProto::ERROR);
                        responseItem->SetErrorReason(TStringBuilder() << "BlobId# " << key.ToString()
                            << " is being put beyond the barrier");
                        continue;
                    }

                    TString valueData;
                    const bool success = value.SerializeToString(&valueData);
                    Y_VERIFY(success);

                    db.Table<Schema::Data>().Key(item.GetKey()).Update<Schema::Data::Value>(valueData);

                    Self->Data->PutKey(std::move(key), TData::TValue(std::move(value)));
                }

                return true;
            }

            void MarkGivenIdCommitted(TAgent& agent, const TBlobSeqId& blobSeqId) {
                Y_VERIFY(blobSeqId.Channel < Self->Channels.size());

                const ui64 value = blobSeqId.ToSequentialNumber();
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT18, "MarkGivenIdCommitted", (Id, Self->GetLogId()),
                    (AgentId, agent.ConnectedNodeId), (BlobSeqId, blobSeqId), (Value, value),
                    (GivenIdRanges, Self->Channels[blobSeqId.Channel].GivenIdRanges),
                    (Agent.GivenIdRanges, agent.GivenIdRanges[blobSeqId.Channel]));

                agent.GivenIdRanges[blobSeqId.Channel].RemovePoint(value, nullptr);

                bool wasLeast;
                Self->Channels[blobSeqId.Channel].GivenIdRanges.RemovePoint(value, &wasLeast);
                if (wasLeast) {
                    Self->Data->OnLeastExpectedBlobIdChange(blobSeqId.Channel);
                }
            }

            bool CheckKeyAgainstBarrier(const TData::TKey& key, const NKikimrBlobDepot::TValue& value) {
                const auto& v = key.AsVariant();
                const auto *id = std::get_if<TLogoBlobID>(&v);
                return !id || Self->BarrierServer->CheckBlobForBarrier(*id) ||
                    value.GetKeepState() == NKikimrBlobDepot::EKeepState::Keep;
            }

            void Complete(const TActorContext&) override {
                Self->Data->HandleTrash();
                TActivationContext::Send(Response.release());
            }
        };

        Execute(std::make_unique<TTxCommitBlobSeq>(this, std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle>(
            ev.Release())));
    }

} // NKikimr::NBlobDepot
