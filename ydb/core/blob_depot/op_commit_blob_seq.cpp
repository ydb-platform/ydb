#include "blob_depot_tablet.h"
#include "schema.h"
#include "data.h"
#include "blocks.h"
#include "s3.h"

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
                    if (!TData::TValue::Validate(item)) {
                        continue;
                    }
                    if (!item.GetCommitNotify() && item.HasBlobLocator()) {
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

                if (!Self->Data->LoadMissingKeys(Request->Get()->Record, txc)) {
                    return false;
                }

                NKikimrBlobDepot::TEvCommitBlobSeqResult *responseRecord;
                std::tie(Response, responseRecord) = TEvBlobDepot::MakeResponseFor(*Request);

                const ui32 generation = Self->Executor()->Generation();

                for (const auto& item : Request->Get()->Record.GetItems()) {
                    auto *responseItem = responseRecord->AddItems();

                    auto finishWithError = [&](NKikimrProto::EReplyStatus status, const TString& errorReason) {
                        responseItem->SetStatus(status);
                        responseItem->SetErrorReason(errorReason);
                        if (item.HasS3Locator()) {
                            const auto& locator = TS3Locator::FromProto(item.GetS3Locator());
                            const size_t numErased = agent.S3WritesInFlight.erase(locator);
                            Y_ABORT_UNLESS(numErased);
                            Self->S3Manager->AddTrashToCollect(locator);
                        }
                    };

                    if (!TData::TValue::Validate(item)) {
                        finishWithError(NKikimrProto::ERROR, "TEvCommitBlobSeq item protobuf is not valid");
                        continue;
                    }

                    bool canBeCollected = false; // can the just-written-blob be collected with GC logic?

                    if (item.HasBlobLocator()) {
                        const auto& blobLocator = item.GetBlobLocator();

                        const auto blobSeqId = TBlobSeqId::FromProto(blobLocator.GetBlobSeqId());
                        if (FailedBlobSeqIds.contains(blobSeqId)) {
                            finishWithError(NKikimrProto::ERROR, "couldn't start commit sequence for blob");
                            continue;
                        }

                        canBeCollected = Self->Data->CanBeCollected(blobSeqId);

                        Y_VERIFY_DEBUG_S(canBeCollected || !CanBeCollectedBlobSeqIds.contains(blobSeqId),
                            "BlobSeqId# " << blobSeqId);
                    }

                    if (item.HasS3Locator()) {
                        const auto& locator = TS3Locator::FromProto(item.GetS3Locator());
                        if (locator.Generation < generation) {
                            finishWithError(NKikimrProto::ERROR, "S3 locator is obsolete");
                            continue;
                        }
                    }

                    responseItem->SetStatus(NKikimrProto::OK);

                    auto key = TData::TKey::FromBinaryKey(item.GetKey(), Self->Config);
                    if (!item.GetCommitNotify()) {
                        bool blocksPass = true;
                        if (const auto& v = key.AsVariant(); const auto *id = std::get_if<TLogoBlobID>(&v)) {
                            blocksPass = item.GetIgnoreBlock() ||
                                Self->BlocksManager->CheckBlock(id->TabletID(), id->Generation());
                        }
                        for (const auto& extra : item.GetExtraBlockChecks()) {
                            if (!blocksPass) {
                                break;
                            }
                            blocksPass = Self->BlocksManager->CheckBlock(extra.GetTabletId(), extra.GetGeneration());
                        }
                        if (!blocksPass) {
                            finishWithError(NKikimrProto::BLOCKED, "block race detected");
                            continue;
                        }
                    }

                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT68, "TTxCommitBlobSeq process key", (Id, Self->GetLogId()),
                        (Key, key), (Item, item), (CanBeCollected, canBeCollected), (Generation, generation));

                    if (canBeCollected) {
                        // we can't accept this record, because it is potentially under already issued barrier
                        finishWithError(NKikimrProto::ERROR, "generation race");
                        continue;
                    }

                    if (auto error = Self->Data->CheckKeyAgainstBarrier(key)) {
                        finishWithError(NKikimrProto::ERROR, TStringBuilder() << "BlobId# " << key.ToString()
                            << " is being put beyond the barrier: " << *error);
                        continue;
                    }

                    if (item.GetCommitNotify()) {
                        if (item.GetUncertainWrite()) {
                            finishWithError(NKikimrProto::ERROR, "UncertainWrite along with CommitNotify");
                        } else if (const TData::TValue *v = Self->Data->FindKey(key); v && v->SameValueChainAsIn(item)) {
                            Self->Data->MakeKeyCertain(key);
                        } else {
                            // data race -- this value has been altered since it was previously written
                            finishWithError(NKikimrProto::RACE, "value has been altered since it was previously written");
                        }
                    } else {
                        if (item.HasBlobLocator()) {
                            const auto blobSeqId = TBlobSeqId::FromProto(item.GetBlobLocator().GetBlobSeqId());
                            Y_VERIFY_DEBUG_S(AllowedBlobSeqIds.contains(blobSeqId), "BlobSeqId# " << blobSeqId);
                            Y_VERIFY_DEBUG_S(
                                Self->Channels[blobSeqId.Channel].GetLeastExpectedBlobId(generation) <= blobSeqId,
                                "BlobSeqId# " << blobSeqId
                                << " LeastExpectedBlobId# " << Self->Channels[blobSeqId.Channel].GetLeastExpectedBlobId(generation)
                                << " Generation# " << generation);
                            Y_VERIFY_DEBUG_S(blobSeqId.Generation == generation, "BlobSeqId# " << blobSeqId << " Generation# " << generation);
                            Y_VERIFY_DEBUG_S(Self->Channels[blobSeqId.Channel].SequenceNumbersInFlight.contains(blobSeqId.ToSequentialNumber()),
                                "BlobSeqId# " << blobSeqId);
                        }
                        if (item.HasS3Locator()) {
                            auto locator = TS3Locator::FromProto(item.GetS3Locator());

                            // remove written item from the trash
                            NIceDb::TNiceDb(txc.DB).Table<Schema::TrashS3>().Key(locator.Generation, locator.KeyId).Delete();

                            // remove item from agent's inflight
                            const size_t numErased = agent.S3WritesInFlight.erase(locator);
                            Y_ABORT_UNLESS(numErased == 1);

                            Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_PUTS_OK] += 1;
                            Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_PUTS_BYTES] += locator.Len;
                        }
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

        const auto& record = ev->Get()->Record;

        for (const auto& item : record.GetItems()) {
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

        for (const auto& item : record.GetS3Locators()) {
            const auto& locator = TS3Locator::FromProto(item);
            const size_t numErased = agent.S3WritesInFlight.erase(locator);
            Y_ABORT_UNLESS(numErased == 1);
            S3Manager->AddTrashToCollect(locator);
        }
    }

} // NKikimr::NBlobDepot
