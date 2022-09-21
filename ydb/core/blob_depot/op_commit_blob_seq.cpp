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
                    if (!TData::TValue::Validate(item)) {
                        responseItem->SetStatus(NKikimrProto::ERROR);
                        responseItem->SetErrorReason("TEvCommitBlobSeq item protobuf is not valid");
                        continue;
                    }
                    const auto& blobLocator = item.GetBlobLocator();

                    responseItem->SetStatus(NKikimrProto::OK);

                    const auto blobSeqId = TBlobSeqId::FromProto(blobLocator.GetBlobSeqId());
                    const bool canBeCollected = Self->Data->CanBeCollected(blobLocator.GetGroupId(), blobSeqId);

                    if (blobSeqId.Generation == generation) {
                        // check for internal sanity -- we can't issue barriers on given ids without confirmed trimming
                        Y_VERIFY_S(!canBeCollected || item.GetCommitNotify(), "BlobSeqId# " << blobSeqId.ToString());

                        // mark given blob as committed only when it was issued in current generation -- only for this
                        // generation we have correct GivenIdRanges
                        if (!item.GetCommitNotify()) {
                            MarkGivenIdCommitted(agent, blobSeqId);
                        }
                    } else if (canBeCollected) {
                        // we can't accept this record, because it is potentially under already issued barrier
                        responseItem->SetStatus(NKikimrProto::ERROR);
                        responseItem->SetErrorReason("generation race");
                        continue;
                    }

                    TString error;
                    if (!CheckKeyAgainstBarrier(key, &error)) {
                        responseItem->SetStatus(NKikimrProto::ERROR);
                        responseItem->SetErrorReason(TStringBuilder() << "BlobId# " << key.ToString()
                            << " is being put beyond the barrier: " << error);
                        continue;
                    }

                    if (item.GetCommitNotify()) {
                        if (item.GetUncertainWrite()) {
                            responseItem->SetStatus(NKikimrProto::ERROR);
                            responseItem->SetErrorReason("UncertainWrite along with CommitNotify");
                        } else if (const TData::TValue *v = Self->Data->FindKey(key); v && v->SameValueChainAsIn(item)) {
                            Self->Data->MakeKeyCertain(key);
                        } else {
                            // data race -- this value has been altered since it was previously written
                            responseItem->SetStatus(NKikimrProto::RACE);
                        }
                    } else {
                        Self->Data->UpdateKey(key, item, item.GetUncertainWrite(), txc, this);
                    }
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

            bool CheckKeyAgainstBarrier(const TData::TKey& key, TString *error) {
                const auto& v = key.AsVariant();
                if (const auto *id = std::get_if<TLogoBlobID>(&v)) {
                    bool underSoft, underHard;
                    Self->BarrierServer->GetBlobBarrierRelation(*id, &underSoft, &underHard);
                    if (underHard) {
                        *error = TStringBuilder() << "under hard barrier# " << Self->BarrierServer->ToStringBarrier(
                            id->TabletID(), id->Channel(), true);
                        return false;
                    } else if (underSoft) {
                        const TData::TValue *value = Self->Data->FindKey(key);
                        if (!value || value->KeepState != NKikimrBlobDepot::EKeepState::Keep) {
                            *error = TStringBuilder() << "under soft barrier# " << Self->BarrierServer->ToStringBarrier(
                                id->TabletID(), id->Channel(), false);
                            return false;
                        }
                    }
                }
                return true;
            }

            void Complete(const TActorContext&) override {
                Self->Data->CommitTrash(this);
                Self->Data->HandleTrash();
                TActivationContext::Send(Response.release());
            }
        };

        Execute(std::make_unique<TTxCommitBlobSeq>(this, std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle>(
            ev.Release())));
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvDiscardSpoiledBlobSeq::TPtr ev) {
        TAgent& agent = GetAgent(ev->Recipient);
        const ui32 generation = Executor()->Generation();

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT57, "TEvDiscardSpoiledBlobSeq", (Id, GetLogId()), (AgentId, agent.ConnectedNodeId),
            (Msg, ev->Get()->Record));

        // FIXME(alexvru): delete uncertain keys containing this BlobSeqId as they were never written

        for (const auto& item : ev->Get()->Record.GetItems()) {
            const auto blobSeqId = TBlobSeqId::FromProto(item);
            if (blobSeqId.Generation == generation) {
                Y_VERIFY(blobSeqId.Channel < Channels.size());
                const ui64 value = blobSeqId.ToSequentialNumber();
                agent.GivenIdRanges[blobSeqId.Channel].RemovePoint(value, nullptr);
                bool wasLeast;
                Channels[blobSeqId.Channel].GivenIdRanges.RemovePoint(value, &wasLeast);
                if (wasLeast) {
                    Data->OnLeastExpectedBlobIdChange(blobSeqId.Channel);
                }
            }
        }
    }

} // NKikimr::NBlobDepot
