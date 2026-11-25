#include "blob_depot_tablet.h"
#include "schema.h"
#include "data.h"
#include "blocks.h"
#include "s3.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

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
                : TTransactionBase(self, std::move(request->TraceId))
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
                        if (const auto it = agent.ExpiredSteps.find(blobSeqId.Channel);
                                blobSeqId.Generation == generation && it != agent.ExpiredSteps.end() &&
                                blobSeqId.Step <= it->second) {
                            // A commit queued during reconnect can arrive before the agent applies invalidations.
                            // Its range is already reclaimed, and GC may already have advanced past this id.
                            FailedBlobSeqIds.insert(blobSeqId);
                            continue;
                        }
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
                // Checking node and instance id alone is not enough: a transient disconnect and reconnect of the
                // same agent instance keeps both, while OnAgentDisconnect has already emptied S3WritesInFlight --
                // the erase below would then trip Y_ABORT_UNLESS(numErased). The pipe server this request arrived
                // on is also exactly what the response is addressed through, so require it to still be current.
                TAgent *agentPtr = Self->FindAgent(Request->Recipient);
                if (!agentPtr || agentPtr->AgentInstanceId != AgentInstanceId) { // agent disconnected while transaction was in queue -- drop this request
                    return true;
                }
                TAgent& agent = *agentPtr;

                if (!Self->Data->LoadMissingKeys(Request->Get()->Record, txc)) {
                    return false;
                }

                NKikimrBlobDepot::TEvCommitBlobSeqResult *responseRecord;
                std::tie(Response, responseRecord) = TEvBlobDepot::MakeResponseFor(*Request);

                const ui32 generation = Self->Executor()->Generation();

                for (const auto& item : Request->Get()->Record.GetItems()) {
                    // Older agents cannot report whether an earlier SDK attempt may still arrive.
                    const bool s3PutMayArriveLate = !item.HasS3PutMayArriveLate() || item.GetS3PutMayArriveLate();
                    auto *responseItem = responseRecord->AddItems();

                    auto finishWithError = [&](NKikimrProto::EReplyStatus status, const TString& errorReason) {
                        responseItem->SetStatus(status);
                        responseItem->SetErrorReason(errorReason);
                        if (item.HasS3Locator()) {
                            const auto& locator = TS3Locator::FromProto(item.GetS3Locator());
                            if (agent.S3WritesInFlight.erase(locator)) {
                                const TInstant retireAt = s3PutMayArriveLate
                                    ? Self->S3Manager->GetPutWatchRetireAt() : TInstant();
                                Self->Data->AddToS3Trash(locator, txc, this, retireAt);
                                Self->S3Manager->OnS3WriteInFlightRemoved(/*success=*/false);
                            }
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

                    YDB_LOG_DEBUG("TTxCommitBlobSeq process key",
                        {"marker", "BDT68"},
                        {"id", Self->GetLogId()},
                        {"key", key},
                        {"item", item},
                        {"canBeCollected", canBeCollected},
                        {"generation", generation});

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

                            if (s3PutMayArriveLate) {
                                Self->Data->FinishS3Put(locator, TData::ES3PutOutcome::MayArriveLate, true, txc, this);
                            }
                            NIceDb::TNiceDb(txc.DB).Table<Schema::TrashS3>().Key(locator.Generation, locator.KeyId).Delete();

                            // remove item from agent's inflight
                            const size_t numErased = agent.S3WritesInFlight.erase(locator);
                            Y_ABORT_UNLESS(numErased == 1);

                            Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_PUTS_OK] += 1;
                            Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_PUTS_BYTES] += locator.Len;

                            Self->S3Manager->OnS3WriteInFlightRemoved(/*success=*/true);
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

        YDB_LOG_DEBUG("TEvDiscardSpoiledBlobSeq",
            {"marker", "BDT57"},
            {"id", GetLogId()},
            {"agentId", agent.Connection->NodeId},
            {"msg", ev->Get()->Record});

        // FIXME(alexvru): delete uncertain keys containing this BlobSeqId as they were never written

        const auto& record = ev->Get()->Record;

        // Arm S3 put throttling before the spoiled locators get processed so that subsequent prepare-write events
        // (already serialized after this one on the same agent pipe) see the updated state and get queued.
        if (record.GetS3SlowDown()) {
            S3Manager->NotifyPutSlowDown();
        }

        for (const auto& item : record.GetItems()) {
            const auto blobSeqId = TBlobSeqId::FromProto(item);
            if (blobSeqId.Generation != generation) {
                continue;
            } else if (blobSeqId.Channel >= Channels.size()) {
                YDB_LOG_ERROR("TEvDiscardSpoiledBlobSeq names a channel out of range",
                    {"marker", "BDT97"},
                    {"id", GetLogId()},
                    {"blobSeqId", blobSeqId});
                continue;
            }

            auto& channel = Channels[blobSeqId.Channel];
            auto& agentRange = agent.GivenIdRanges[blobSeqId.Channel];
            const ui64 value = blobSeqId.ToSequentialNumber();

            // RemovePoint aborts on a point we do not hold, and these items come off the wire: an id that was
            // already committed, reclaimed, or simply repeated within this message must not take the tablet down.
            if (!agentRange.GetPoint(value) || !channel.GivenIdRanges.GetPoint(value)) {
                YDB_LOG_WARN("TEvDiscardSpoiledBlobSeq names a BlobSeqId we do not hold",
                    {"marker", "BDT98"},
                    {"id", GetLogId()},
                    {"agentId", agent.Connection->NodeId},
                    {"blobSeqId", blobSeqId});
                continue;
            }

            const TBlobSeqId leastExpectedBlobIdBefore = channel.GetLeastExpectedBlobId(generation);

            agentRange.RemovePoint(value);
            channel.GivenIdRanges.RemovePoint(value);

            if (channel.GetLeastExpectedBlobId(generation) != leastExpectedBlobIdBefore) {
                Data->OnLeastExpectedBlobIdChange(blobSeqId.Channel);
            }
        }

        std::vector<TS3Locator> abandoned;
        for (const auto& item : record.GetS3Locators()) {
            const auto locator = TS3Locator::FromProto(item);
            if (agent.S3WritesInFlight.erase(locator)) {
                abandoned.push_back(locator);
                S3Manager->OnS3WriteInFlightRemoved(/*success=*/false);
            }
        }
        // An older agent cannot tell us whether an earlier SDK attempt is still outstanding.
        auto outcome = TData::ES3PutOutcome::MayArriveLate;
        if (record.HasS3PutMayArriveLate() && !record.GetS3PutMayArriveLate()) {
            outcome = record.GetS3SlowDown() ? TData::ES3PutOutcome::NotStored : TData::ES3PutOutcome::Stored;
        }
        Data->DiscardAgentS3Puts(std::move(abandoned), outcome);
    }

} // NKikimr::NBlobDepot
