#include "blob_depot_tablet.h"
#include "schema.h"
#include "data.h"
#include "garbage_collection.h"
#include "blocks.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvCommitBlobSeq::TPtr ev) {
        class TTxCommitBlobSeq : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            const ui32 NodeId;
            const ui64 AgentInstanceId;
            std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle> Request;
            std::unique_ptr<IEventHandle> Response;
            std::vector<TBlobSeqId> BlobSeqIds;
            std::set<TBlobSeqId> FailedBlobSeqIds;
            std::set<TBlobSeqId> CanBeCollectedBlobSeqIds;
            std::set<TBlobSeqId> AllowedBlobSeqIds;

        public:
            TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_COMMIT_BLOB_SEQ; }

            TTxCommitBlobSeq(TBlobDepot *self, TAgent& agent, std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle> request)
                : TTransactionBase(self)
                , NodeId(agent.Connection->NodeId)
                , AgentInstanceId(*agent.AgentInstanceId)
                , Request(std::move(request))
            {
                const ui32 generation = Self->Executor()->Generation();
                const auto& items = Request->Get()->Record.GetItems();
                Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_PUTS_INCOMING] += items.size();
                for (const auto& item : items) {
                    if (TData::TValue::Validate(item) && !item.GetCommitNotify()) {
                        const auto blobSeqId = TBlobSeqId::FromProto(item.GetBlobLocator().GetBlobSeqId());
                        if (Self->Data->CanBeCollected(blobSeqId)) {
                            // check for internal sanity -- we can't issue barriers on given ids without confirmed trimming
                            Y_VERIFY_S(blobSeqId.Generation < generation, "committing trimmed BlobSeqId"
                                << " BlobSeqId# " << blobSeqId.ToString()
                                << " Id# " << Self->GetLogId());
                            CanBeCollectedBlobSeqIds.insert(blobSeqId);
                        } else if (!Self->Data->BeginCommittingBlobSeqId(agent, blobSeqId)) {
                            FailedBlobSeqIds.insert(blobSeqId);
                        } else {
                            AllowedBlobSeqIds.insert(blobSeqId);
                        }
                        BlobSeqIds.push_back(blobSeqId);
                    }
                }
            }

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                TAgent& agent = Self->GetAgent(NodeId);
                if (!agent.Connection || agent.AgentInstanceId != AgentInstanceId) { // agent disconnected while transaction was in queue -- drop this request
                    return true;
                }

                if (!LoadMissingKeys(txc)) {
                    return false;
                }

                NIceDb::TNiceDb db(txc.DB);

                NKikimrBlobDepot::TEvCommitBlobSeqResult *responseRecord;
                std::tie(Response, responseRecord) = TEvBlobDepot::MakeResponseFor(*Request);

                const ui32 generation = Self->Executor()->Generation();

                for (const auto& item : Request->Get()->Record.GetItems()) {
                    auto *responseItem = responseRecord->AddItems();
                    if (!TData::TValue::Validate(item)) {
                        responseItem->SetStatus(NKikimrProto::ERROR);
                        responseItem->SetErrorReason("TEvCommitBlobSeq item protobuf is not valid");
                        continue;
                    }
                    const auto& blobLocator = item.GetBlobLocator();

                    const auto blobSeqId = TBlobSeqId::FromProto(blobLocator.GetBlobSeqId());
                    if (FailedBlobSeqIds.contains(blobSeqId)) {
                        responseItem->SetStatus(NKikimrProto::ERROR);
                        responseItem->SetErrorReason("couldn't start commit sequence for blob");
                        continue;
                    }

                    responseItem->SetStatus(NKikimrProto::OK);

                    const bool canBeCollected = Self->Data->CanBeCollected(blobSeqId);

                    auto key = TData::TKey::FromBinaryKey(item.GetKey(), Self->Config);
                    if (!item.GetCommitNotify()) {
                        if (const auto& v = key.AsVariant(); const auto *id = std::get_if<TLogoBlobID>(&v)) {
                            if (!Self->BlocksManager->CheckBlock(id->TabletID(), id->Generation())) {
                                // FIXME(alexvru): ExtraBlockChecks?
                                responseItem->SetStatus(NKikimrProto::BLOCKED);
                                responseItem->SetErrorReason("block race detected");
                                continue;
                            }
                        }
                    }

                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT68, "TTxCommitBlobSeq process key", (Id, Self->GetLogId()),
                        (Key, key), (Item, item), (CanBeCollected, canBeCollected), (Generation, generation));

                    if (canBeCollected) {
                        // we can't accept this record, because it is potentially under already issued barrier
                        responseItem->SetStatus(NKikimrProto::ERROR);
                        responseItem->SetErrorReason("generation race");
                        continue;
                    }

                    Y_VERIFY_DEBUG_S(!CanBeCollectedBlobSeqIds.contains(blobSeqId), "BlobSeqId# " << blobSeqId);

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
                        Y_VERIFY_DEBUG_S(AllowedBlobSeqIds.contains(blobSeqId), "BlobSeqId# " << blobSeqId);
                        Y_VERIFY_DEBUG_S(
                            Self->Channels[blobSeqId.Channel].GetLeastExpectedBlobId(generation) <= blobSeqId,
                            "BlobSeqId# " << blobSeqId
                            << " LeastExpectedBlobId# " << Self->Channels[blobSeqId.Channel].GetLeastExpectedBlobId(generation)
                            << " Generation# " << generation);
                        Y_VERIFY_DEBUG_S(blobSeqId.Generation == generation, "BlobSeqId# " << blobSeqId << " Generation# " << generation);
                        Y_VERIFY_DEBUG_S(Self->Channels[blobSeqId.Channel].SequenceNumbersInFlight.contains(blobSeqId.ToSequentialNumber()),
                            "BlobSeqId# " << blobSeqId);
                        Self->Data->UpdateKey(key, item, txc, this);
                    }
                }

                for (const auto& item : Response->Get<TEvBlobDepot::TEvCommitBlobSeqResult>()->Record.GetItems()) {
                    Self->TabletCounters->Cumulative()[
                        item.GetStatus() == NKikimrProto::OK
                            ? NKikimrBlobDepot::COUNTER_PUTS_OK
                            : NKikimrBlobDepot::COUNTER_PUTS_ERROR
                    ].Increment(1);
                }

                return true;
            }

            bool LoadMissingKeys(TTransactionContext& txc) {
                NIceDb::TNiceDb db(txc.DB);
                if (Self->Data->IsLoaded()) {
                    return true;
                }
                for (const auto& item : Request->Get()->Record.GetItems()) {
                    auto key = TData::TKey::FromBinaryKey(item.GetKey(), Self->Config);
                    if (!Self->Data->EnsureKeyLoaded(key, txc)) {
                        return false;
                    }
                }
                return true;
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
                TAgent& agent = Self->GetAgent(NodeId);
                for (const TBlobSeqId blobSeqId : BlobSeqIds) {
                    Self->Data->EndCommittingBlobSeqId(agent, blobSeqId);
                }
                Self->Data->CommitTrash(this);
                TActivationContext::Send(Response.release());
            }
        };

        Execute(std::make_unique<TTxCommitBlobSeq>(this, GetAgent(ev->Recipient),
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
                Y_ABORT_UNLESS(blobSeqId.Channel < Channels.size());
                auto& channel = Channels[blobSeqId.Channel];

                const TBlobSeqId leastExpectedBlobIdBefore = channel.GetLeastExpectedBlobId(generation);

                const ui64 value = blobSeqId.ToSequentialNumber();
                agent.GivenIdRanges[blobSeqId.Channel].RemovePoint(value);
                Channels[blobSeqId.Channel].GivenIdRanges.RemovePoint(value);

                if (channel.GetLeastExpectedBlobId(generation) != leastExpectedBlobIdBefore) {
                    Data->OnLeastExpectedBlobIdChange(blobSeqId.Channel);
                }
            }
        }
    }

} // NKikimr::NBlobDepot
