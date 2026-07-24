#include "s3.h"
#include "blocks.h"

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;

    class TS3Manager::TTxPrepareWriteS3 : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const ui32 NodeId;
        const ui64 AgentInstanceId;
        std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle> Request;
        std::unique_ptr<IEventHandle> Response;
        ui32 AllocatedLocatorCount = 0;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_PREPARE_WRITE_S3; }

        TTxPrepareWriteS3(TBlobDepot *self, TAgent& agent, std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle> request)
            : TTransactionBase(self)
            , NodeId(agent.Connection->NodeId)
            , AgentInstanceId(*agent.AgentInstanceId)
            , Request(std::move(request))
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            AllocatedLocatorCount = 0;

            TAgent& agent = Self->GetAgent(NodeId);
            if (!agent.Connection || agent.AgentInstanceId != AgentInstanceId) {
                // agent has been disconnected while transaction was in queue -- do nothing
                return true;
            }

            if (!Self->Data->LoadMissingKeys(Request->Get()->Record, txc)) {
                // we haven't loaded all of the required keys
                return false;
            }

            NIceDb::TNiceDb db(txc.DB);

            NKikimrBlobDepot::TEvPrepareWriteS3Result *responseRecord;
            std::tie(Response, responseRecord) = TEvBlobDepot::MakeResponseFor(*Request);

            for (const auto& record = Request->Get()->Record; const auto& item : record.GetItems()) {
                auto *responseItem = responseRecord->AddItems();

                TString error;
                if (NKikimrProto::EReplyStatus status = CheckItem(item, error); status != NKikimrProto::OK) {
                    // basic checks have failed (blocked, item was deleted, or something else)
                    responseItem->SetStatus(status);
                    responseItem->SetErrorReason(error);
                } else {
                    responseItem->SetStatus(NKikimrProto::OK);

                    const TS3Locator locator = Self->S3Manager->AllocateS3Locator(item.GetLen());
                    locator.ToProto(responseItem->MutableS3Locator());

                    // we put it here until operation is completed; if tablet restarts and operation fails, then this
                    // key will be deleted; we also rewrite spoiled locator because Len is different
                    db.Table<Schema::TrashS3>().Key(locator.Generation, locator.KeyId).Update<Schema::TrashS3::Len>(locator.Len);

                    const bool inserted = agent.S3WritesInFlight.insert(locator).second;
                    Y_ABORT_UNLESS(inserted);
                    ++AllocatedLocatorCount;
                }
            }

            return true;
        }

        NKikimrProto::EReplyStatus CheckItem(const NKikimrBlobDepot::TEvPrepareWriteS3::TItem& item, TString& error) {
            auto key = TData::TKey::FromBinaryKey(item.GetKey(), Self->Config);

            bool blocksPass = std::visit(TOverloaded{
                [](TStringBuf) { return true; },
                [&](TLogoBlobID blobId) { return Self->BlocksManager->CheckBlock(blobId.TabletID(), blobId.Generation()); }
            }, key.AsVariant());

            for (const auto& extra : item.GetExtraBlockChecks()) {
                if (!blocksPass) {
                    break;
                }
                blocksPass = Self->BlocksManager->CheckBlock(extra.GetTabletId(), extra.GetGeneration());
            }

            if (!blocksPass) {
                error = "blocked";
                return NKikimrProto::BLOCKED;
            }

            if (auto e = Self->Data->CheckKeyAgainstBarrier(key)) {
                error = TStringBuilder() << "BlobId# " << key.ToString() << " is being put beyond the barrier: " << *e;
                return NKikimrProto::ERROR;
            }

            return NKikimrProto::OK;
        }

        void Complete(const TActorContext&) override {
            if (AllocatedLocatorCount) {
                Self->S3Manager->OnS3WriteInFlightAdded(AllocatedLocatorCount);
            }
            if (Response) {
                TActivationContext::Send(Response.release());
            }
        }
    };

    TS3Locator TS3Manager::AllocateS3Locator(ui32 len) {
        return {
            .Len = len,
            .Generation = Self->Executor()->Generation(),
            .KeyId = NextKeyId++,
        };
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvPrepareWriteS3::TPtr ev) {
        S3Manager->HandlePrepareWriteS3(std::move(ev));
    }

    void TS3Manager::HandlePrepareWriteS3(TEvBlobDepot::TEvPrepareWriteS3::TPtr ev) {
        const TMonotonic now = TActivationContext::Monotonic();
        const bool timeThrottled = now < PutThrottleUntil;
        const bool concurrencyThrottled = S3WritesInFlight >= CurrentMaxWritesInFlight;

        if (timeThrottled || concurrencyThrottled) {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS23, "TEvPrepareWriteS3 queued", (Id, Self->GetLogId()),
                (TimeThrottled, timeThrottled), (ConcurrencyThrottled, concurrencyThrottled),
                (S3WritesInFlight, S3WritesInFlight), (CurrentMaxWritesInFlight, CurrentMaxWritesInFlight),
                (QueueSize, PendingPrepareWrites.size()));
            PendingPrepareWrites.push_back(std::move(ev));
            if (timeThrottled && !PutWakeupScheduled) {
                TActivationContext::Schedule(PutThrottleUntil, new IEventHandle(TEvPrivate::EvPutThrottleWakeup,
                    0, Self->SelfId(), {}, nullptr, 0));
                PutWakeupScheduled = true;
            }
            return;
        }

        auto& agent = Self->GetAgent(ev->Recipient);

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS07, "TEvPrepareWriteS3", (Id, Self->GetLogId()),
            (AgentId, agent.Connection->NodeId), (Msg, ev->Get()->Record));

        Self->Execute(std::make_unique<TTxPrepareWriteS3>(Self, agent,
            std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle>(ev.Release())));
    }

    void TS3Manager::NotifyPutSlowDown() {
        Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_PUTS_SLOW_DOWN] += 1;

        CurrentMaxWritesInFlight = 1;
        ConsecutiveSuccessfulWriteBatches = 0;
        const TDuration delay = PutBackoff.Next();
        PutThrottleUntil = TActivationContext::Monotonic() + delay;

        STLOG(PRI_WARN, BLOB_DEPOT, BDTS22, "S3 put throttled", (Id, Self->GetLogId()),
            (Delay, delay), (CurrentMaxWritesInFlight, CurrentMaxWritesInFlight),
            (S3WritesInFlight, S3WritesInFlight), (QueueSize, PendingPrepareWrites.size()));
        BDEV(BDEV42, "S3_put_throttled", (BDT, Self->TabletID()), (DelayMs, delay.MilliSeconds()),
            (QueueSize, PendingPrepareWrites.size()));

        if (!PutWakeupScheduled) {
            TActivationContext::Schedule(PutThrottleUntil, new IEventHandle(TEvPrivate::EvPutThrottleWakeup,
                0, Self->SelfId(), {}, nullptr, 0));
            PutWakeupScheduled = true;
        }
    }

    void TS3Manager::HandlePutThrottleWakeup() {
        PutWakeupScheduled = false;
        RunPendingPrepareWritesIfPossible();
    }

    void TS3Manager::RunPendingPrepareWritesIfPossible() {
        const TMonotonic now = TActivationContext::Monotonic();
        if (now < PutThrottleUntil) {
            if (!PutWakeupScheduled && !PendingPrepareWrites.empty()) {
                TActivationContext::Schedule(PutThrottleUntil, new IEventHandle(TEvPrivate::EvPutThrottleWakeup,
                    0, Self->SelfId(), {}, nullptr, 0));
                PutWakeupScheduled = true;
            }
            return;
        }

        while (!PendingPrepareWrites.empty() && S3WritesInFlight < CurrentMaxWritesInFlight) {
            auto ev = std::move(PendingPrepareWrites.front());
            PendingPrepareWrites.pop_front();

            auto& agent = Self->GetAgent(ev->Recipient);
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS07, "TEvPrepareWriteS3", (Id, Self->GetLogId()),
                (AgentId, agent.Connection->NodeId), (Msg, ev->Get()->Record));

            Self->Execute(std::make_unique<TTxPrepareWriteS3>(Self, agent,
                std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle>(ev.Release())));
        }
    }

    void TS3Manager::OnS3WriteInFlightAdded(ui32 count) {
        S3WritesInFlight += count;
    }

    void TS3Manager::OnS3WriteInFlightRemoved(bool success) {
        Y_ABORT_UNLESS(S3WritesInFlight);
        --S3WritesInFlight;

        if (success && CurrentMaxWritesInFlight < MaxWritesInFlight) {
            if (++ConsecutiveSuccessfulWriteBatches >= SuccessesPerWriteConcurrencyStepUp) {
                ConsecutiveSuccessfulWriteBatches = 0;
                ++CurrentMaxWritesInFlight;
                if (CurrentMaxWritesInFlight >= MaxWritesInFlight) {
                    CurrentMaxWritesInFlight = MaxWritesInFlight;
                    PutBackoff.Reset();
                }
            }
        }

        RunPendingPrepareWritesIfPossible();
    }

} // NKikimr::NBlobDepot
