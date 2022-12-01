#include "blob_depot_tablet.h"
#include "schema.h"
#include "data.h"
#include "garbage_collection.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvCommitBlobSeq::TPtr ev) {
        class TTxCommitBlobSeq : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            const ui32 NodeId;
            std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle> Request;
            std::unique_ptr<IEventHandle> Response;

        public:
            TTxCommitBlobSeq(TBlobDepot *self, ui32 nodeId, std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle> request)
                : TTransactionBase(self)
                , NodeId(nodeId)
                , Request(std::move(request))
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                TAgent& agent = Self->GetAgent(NodeId);
                if (!agent.Connection) { // agent disconnected while transaction was in queue -- drop this request
                    return true;
                }

                if (!LoadMissingKeys(txc)) {
                    return false;
                }

                NIceDb::TNiceDb db(txc.DB);

                NKikimrBlobDepot::TEvCommitBlobSeqResult *responseRecord;
                std::tie(Response, responseRecord) = TEvBlobDepot::MakeResponseFor(*Request, Self->SelfId());

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

                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT68, "TTxCommitBlobSeq process key", (Id, Self->GetLogId()),
                        (Key, key), (Item, item), (CanBeCollected, canBeCollected), (Generation, generation));

                    if (blobSeqId.Generation == generation) {
                        // check for internal sanity -- we can't issue barriers on given ids without confirmed trimming
                        Y_VERIFY_S(!canBeCollected || item.GetCommitNotify(), "BlobSeqId# " << blobSeqId.ToString());
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
                        Self->Data->UpdateKey(key, item, txc, this);
                        if (blobSeqId.Generation == generation) {
                            // mark given blob as committed only when it was issued in current generation -- only for this
                            // generation we have correct GivenIdRanges; and we can do this only after updating key as the
                            // callee function may trigger garbage collection
                            MarkGivenIdCommitted(agent, blobSeqId);
                        }
                    }
                }

                return true;
            }

            bool LoadMissingKeys(TTransactionContext& txc) {
                NIceDb::TNiceDb db(txc.DB);
                if (Self->Data->IsLoaded()) {
                    return true;
                }
                bool success = true;
                for (const auto& item : Request->Get()->Record.GetItems()) {
                    auto key = TData::TKey::FromBinaryKey(item.GetKey(), Self->Config);
                    if (Self->Data->IsKeyLoaded(key)) {
                        continue;
                    }
                    using Table = Schema::Data;
                    auto row = db.Table<Table>().Key(item.GetKey()).Select();
                    if (!row.IsReady()) {
                        success = false;
                    } else if (row.IsValid()) {
                        Self->Data->AddDataOnLoad(std::move(key), row.GetValue<Table::Value>(),
                            row.GetValueOrDefault<Table::UncertainWrite>());
                    }
                }
                return success;
            }

            void MarkGivenIdCommitted(TAgent& agent, const TBlobSeqId& blobSeqId) {
                Y_VERIFY(blobSeqId.Channel < Self->Channels.size());

                const ui64 value = blobSeqId.ToSequentialNumber();
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT18, "MarkGivenIdCommitted", (Id, Self->GetLogId()),
                    (AgentId, agent.Connection->NodeId), (BlobSeqId, blobSeqId), (Value, value),
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
                TActivationContext::Send(Response.release());
            }
        };

        TAgent& agent = GetAgent(ev->Recipient);
        Execute(std::make_unique<TTxCommitBlobSeq>(this, agent.Connection->NodeId,
            std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle>(ev.Release())));
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvDiscardSpoiledBlobSeq::TPtr ev) {
        TAgent& agent = GetAgent(ev->Recipient);
        const ui32 generation = Executor()->Generation();

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT57, "TEvDiscardSpoiledBlobSeq", (Id, GetLogId()), (AgentId, agent.Connection->NodeId),
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
