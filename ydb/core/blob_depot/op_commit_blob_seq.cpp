#include "blob_depot_tablet.h"
#include "schema.h"
#include "data.h"
#include "blocks.h"
#include "s3.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TTxCommitBlobSeq : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        std::vector<TPendingCommitBlobSeq> Commits;
        std::vector<TBlobSeqId> BlobSeqIds;
        std::set<TBlobSeqId> FailedBlobSeqIds;
        std::set<TBlobSeqId> CanBeCollectedBlobSeqIds;
        std::set<TBlobSeqId> AllowedBlobSeqIds;
        std::vector<std::unique_ptr<IEventHandle>> Responses;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_COMMIT_BLOB_SEQ; }

        TTxCommitBlobSeq(TBlobDepot *self, std::vector<TPendingCommitBlobSeq> commits)
            : TTransactionBase(self, commits.empty() ? NWilson::TTraceId{} : std::move(commits.front().Request->TraceId))
            , Commits(std::move(commits))
        {
            const ui32 generation = Self->Executor()->Generation();
            for (auto& commit : Commits) {
                TAgent& agent = Self->GetAgent(commit.NodeId);
                const auto& items = commit.Request->Get()->Record.GetItems();
                Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_PUTS_INCOMING] += items.size();
                for (const auto& item : items) {
                    if (!TData::TValue::Validate(item)) {
                        continue;
                    }
                    if (!item.GetCommitNotify() && item.HasBlobLocator()) {
                        const auto blobSeqId = TBlobSeqId::FromProto(item.GetBlobLocator().GetBlobSeqId());
                        if (Self->Data->CanBeCollected(blobSeqId)) {
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
        }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            for (auto& commit : Commits) {
                if (!Self->Data->LoadMissingKeys(commit.Request->Get()->Record, txc)) {
                    return false;
                }
            }

            Responses.clear();
            Responses.reserve(Commits.size());

            const ui32 generation = Self->Executor()->Generation();

            for (auto& commit : Commits) {
                TAgent& agent = Self->GetAgent(commit.NodeId);
                if (!agent.Connection || agent.AgentInstanceId != commit.AgentInstanceId) {
                    Responses.emplace_back();
                    continue;
                }

                NKikimrBlobDepot::TEvCommitBlobSeqResult *responseRecord;
                std::unique_ptr<IEventHandle> response;
                std::tie(response, responseRecord) = TEvBlobDepot::MakeResponseFor(*commit.Request);

                for (const auto& item : commit.Request->Get()->Record.GetItems()) {
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

                    bool canBeCollected = false;

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

                            NIceDb::TNiceDb(txc.DB).Table<Schema::TrashS3>().Key(locator.Generation, locator.KeyId).Delete();

                            const size_t numErased = agent.S3WritesInFlight.erase(locator);
                            Y_ABORT_UNLESS(numErased == 1);

                            Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_PUTS_OK] += 1;
                            Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_PUTS_BYTES] += locator.Len;
                        }
                        Self->Data->UpdateKey(key, item, txc, this);
                    }
                }

                for (const auto& item : response->Get<TEvBlobDepot::TEvCommitBlobSeqResult>()->Record.GetItems()) {
                    Self->TabletCounters->Cumulative()[
                        item.GetStatus() == NKikimrProto::OK
                            ? NKikimrBlobDepot::COUNTER_PUTS_OK
                            : NKikimrBlobDepot::COUNTER_PUTS_ERROR
                    ].Increment(1);
                }

                Responses.push_back(std::move(response));
            }

            return true;
        }

        void Complete(const TActorContext&) override {
            Y_ABORT_UNLESS(!Commits.empty());
            TAgent& agent = Self->GetAgent(Commits.front().NodeId);
            for (const TBlobSeqId blobSeqId : BlobSeqIds) {
                Self->Data->EndCommittingBlobSeqId(agent, blobSeqId);
            }
            Self->Data->CommitTrash(this);
            for (auto& response : Responses) {
                if (response) {
                    TActivationContext::Send(response.release());
                }
            }
            Self->CommitBlobSeqTxInFlight = false;
            Self->StartCommitBlobSeqTxIfNeeded();
        }
    };

    void TBlobDepot::ReleaseS3HttpThrottle(TAgent& agent, const NKikimrBlobDepot::TEvCommitBlobSeq& record, bool success) {
        ui32 released = 0;
        for (const auto& item : record.GetItems()) {
            if (item.HasS3Locator() && !item.GetCommitNotify()) {
                const auto locator = TS3Locator::FromProto(item.GetS3Locator());
                released += agent.S3ThrottleHeld.erase(locator);
            }
        }
        if (released) {
            S3Manager->OnS3WriteInFlightRemoved(success, released);
        }
    }

    void TBlobDepot::StartCommitBlobSeqTxIfNeeded() {
        if (CommitBlobSeqTxInFlight || PendingCommitBlobSeq.empty()) {
            return;
        }
        std::vector<TPendingCommitBlobSeq> batch;
        batch.reserve(Min<size_t>(PendingCommitBlobSeq.size(), MaxCommitBlobSeqBatch));
        while (!PendingCommitBlobSeq.empty() && batch.size() < MaxCommitBlobSeqBatch) {
            batch.push_back(std::move(PendingCommitBlobSeq.front()));
            PendingCommitBlobSeq.pop_front();
        }
        CommitBlobSeqTxInFlight = true;
        Execute(std::make_unique<TTxCommitBlobSeq>(this, std::move(batch)));
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvCommitBlobSeq::TPtr ev) {
        TAgent& agent = GetAgent(ev->Recipient);
        ReleaseS3HttpThrottle(agent, ev->Get()->Record, /*success=*/true);

        PendingCommitBlobSeq.push_back(TPendingCommitBlobSeq{
            .NodeId = agent.Connection->NodeId,
            .AgentInstanceId = *agent.AgentInstanceId,
            .Request = std::unique_ptr<TEvBlobDepot::TEvCommitBlobSeq::THandle>(ev.Release()),
        });
        StartCommitBlobSeqTxIfNeeded();
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

        ui32 throttleReleased = 0;
        for (const auto& item : record.GetS3Locators()) {
            const auto& locator = TS3Locator::FromProto(item);
            const size_t numErased = agent.S3WritesInFlight.erase(locator);
            Y_ABORT_UNLESS(numErased == 1);
            throttleReleased += agent.S3ThrottleHeld.erase(locator);
            if (!record.GetS3SlowDown()) { // in case of SlowDown these items never had the chance of being written
                S3Manager->AddTrashToCollect(locator);
            }
        }
        if (throttleReleased) {
            S3Manager->OnS3WriteInFlightRemoved(/*success=*/false, throttleReleased);
        }
    }

} // NKikimr::NBlobDepot
