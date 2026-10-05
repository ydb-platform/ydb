#include "s3.h"
#include "blocks.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;

    class TS3Manager::TTxPrepareWriteS3 : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const ui64 AgentInstanceId;
        std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle> Request;
        std::unique_ptr<IEventHandle> Response;
        ui32 AllocatedLocatorCount = 0;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_PREPARE_WRITE_S3; }

        TTxPrepareWriteS3(TBlobDepot *self, TAgent& agent, std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle> request)
            : TTransactionBase(self, std::move(request->TraceId))
            , AgentInstanceId(*agent.AgentInstanceId)
            , Request(std::move(request))
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            AllocatedLocatorCount = 0;

            // The response is addressed back through the pipe server this request arrived on (MakeResponseFor uses
            // Request->Recipient as the sender), and the agent drops any response that does not come from its
            // current pipe server. Matching NodeId and AgentInstanceId is not enough: a transient disconnect and
            // reconnect of the same agent instance keeps both, and we would then allocate locators that the agent
            // never hears about and therefore never commits or discards -- leaking a tablet-wide write slot.
            TAgent *agentPtr = Self->FindAgent(Request->Recipient);
            if (!agentPtr || agentPtr->AgentInstanceId != AgentInstanceId) {
                // the connection this request arrived on is gone -- do nothing
                return true;
            }
            TAgent& agent = *agentPtr;

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

            if (AllocatedLocatorCount) {
                // This has to stay in lockstep with agent.S3WritesInFlight, which is what OnAgentDisconnect counts
                // when it releases the slots of a departing agent -- and it may well run between Execute and
                // Complete of this very transaction. TTxCommitBlobSeq releases them from Execute for the same reason.
                Self->S3Manager->OnS3WriteInFlightAdded(AllocatedLocatorCount);
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
        ApplyMaxWritesInFlight();

        const TMonotonic now = TActivationContext::Monotonic();
        const bool timeThrottled = now < PutThrottleUntil;
        const bool concurrencyThrottled = S3WritesInFlight >= CurrentMaxWritesInFlight;

        if (timeThrottled || concurrencyThrottled) {
            YDB_LOG_DEBUG_COMP(BLOB_DEPOT, "TEvPrepareWriteS3 queued",
                {"marker", "BDTS23"},
                {"id", Self->GetLogId()},
                {"timeThrottled", timeThrottled},
                {"concurrencyThrottled", concurrencyThrottled},
                {"S3WritesInFlight", S3WritesInFlight},
                {"currentMaxWritesInFlight", CurrentMaxWritesInFlight},
                {"queueSize", PendingPrepareWrites.size()});
            PendingPrepareWrites.push_back(std::move(ev));
            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_PUT_PENDING_QUEUE_SIZE] = PendingPrepareWrites.size();
            if (timeThrottled && !PutWakeupScheduled) {
                TActivationContext::Schedule(PutThrottleUntil, new IEventHandle(TEvPrivate::EvPutThrottleWakeup,
                    0, Self->SelfId(), {}, nullptr, 0));
                PutWakeupScheduled = true;
            }
            return;
        }

        auto& agent = Self->GetAgent(ev->Recipient);

        YDB_LOG_DEBUG_COMP(BLOB_DEPOT, "TEvPrepareWriteS3",
            {"marker", "BDTS07"},
            {"id", Self->GetLogId()},
            {"agentId", agent.Connection->NodeId},
            {"msg", ev->Get()->Record});

        Self->Execute(std::make_unique<TTxPrepareWriteS3>(Self, agent,
            std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle>(ev.Release())));
    }

    void TS3Manager::NotifyPutSlowDown() {
        Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_PUTS_SLOW_DOWN] += 1;
        Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_PUT_THROTTLE_ACTIVATIONS] += 1;

        CurrentMaxWritesInFlight = 1;
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_PUT_MAX_WRITES_IN_FLIGHT] = CurrentMaxWritesInFlight;
        ConsecutiveSuccessfulWriteBatches = 0;
        const TDuration delay = PutBackoff.Next();
        PutThrottleUntil = TActivationContext::Monotonic() + delay;

        YDB_LOG_WARN_COMP(BLOB_DEPOT, "S3 put throttled",
            {"marker", "BDTS22"},
            {"id", Self->GetLogId()},
            {"delay", delay},
            {"currentMaxWritesInFlight", CurrentMaxWritesInFlight},
            {"S3WritesInFlight", S3WritesInFlight},
            {"queueSize", PendingPrepareWrites.size()});
        YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "S3_put_throttled",
            {"marker", "BDEV42"},
            {"BDT", Self->TabletID()},
            {"delayMs", delay.MilliSeconds()},
            {"queueSize", PendingPrepareWrites.size()});

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

        ApplyMaxWritesInFlight();

        while (!PendingPrepareWrites.empty() && S3WritesInFlight < CurrentMaxWritesInFlight) {
            auto ev = std::move(PendingPrepareWrites.front());
            PendingPrepareWrites.pop_front();

            // the pipe server this request came through may have vanished (or have been superseded by a newer
            // connection of the same agent) while the request was sitting in the queue -- there is no one to answer
            // to anymore, so just drop it
            TAgent *agent = Self->FindAgent(ev->Recipient);
            if (!agent) {
                YDB_LOG_DEBUG_COMP(BLOB_DEPOT, "dropping queued TEvPrepareWriteS3 of a disconnected agent",
                    {"marker", "BDTS37"},
                    {"id", Self->GetLogId()},
                    {"pipeServerId", ev->Recipient},
                    {"sender", ev->Sender});
                continue;
            }

            YDB_LOG_DEBUG_COMP(BLOB_DEPOT, "TEvPrepareWriteS3",
                {"marker", "BDTS07"},
                {"id", Self->GetLogId()},
                {"agentId", agent->Connection->NodeId},
                {"msg", ev->Get()->Record});

            Self->Execute(std::make_unique<TTxPrepareWriteS3>(Self, *agent,
                std::unique_ptr<TEvBlobDepot::TEvPrepareWriteS3::THandle>(ev.Release())));
        }
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_PUT_PENDING_QUEUE_SIZE] = PendingPrepareWrites.size();
    }

    void TS3Manager::DropPendingPrepareWrites(const TActorId& pipeServerId) {
        if (PendingPrepareWrites.empty()) {
            return;
        }

        std::deque<TEvBlobDepot::TEvPrepareWriteS3::TPtr> remain;
        size_t numDropped = 0;
        for (auto& ev : PendingPrepareWrites) {
            if (ev->Recipient == pipeServerId) {
                ++numDropped;
            } else {
                remain.push_back(std::move(ev));
            }
        }
        // every kept item has already been moved into `remain`, so the swap has to happen unconditionally
        PendingPrepareWrites.swap(remain);
        if (!numDropped) {
            return;
        }
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_PUT_PENDING_QUEUE_SIZE] = PendingPrepareWrites.size();

        YDB_LOG_DEBUG_COMP(BLOB_DEPOT, "dropped queued TEvPrepareWriteS3 on pipe server disconnect",
            {"marker", "BDTS38"},
            {"id", Self->GetLogId()},
            {"pipeServerId", pipeServerId},
            {"numDropped", numDropped},
            {"queueSize", PendingPrepareWrites.size()});
    }

    void TS3Manager::OnS3WriteInFlightAdded(ui32 count) {
        S3WritesInFlight += count;
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_PUT_WRITES_IN_FLIGHT] = S3WritesInFlight;
    }

    void TS3Manager::OnS3WriteInFlightRemoved(bool success) {
        Y_ABORT_UNLESS(S3WritesInFlight);
        --S3WritesInFlight;
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_PUT_WRITES_IN_FLIGHT] = S3WritesInFlight;

        const ui32 maxWritesInFlight = MaxWritesInFlight();
        if (success && CurrentMaxWritesInFlight < maxWritesInFlight) {
            if (++ConsecutiveSuccessfulWriteBatches >= SuccessesPerWriteConcurrencyStepUp) {
                ConsecutiveSuccessfulWriteBatches = 0;
                ++CurrentMaxWritesInFlight;
                if (CurrentMaxWritesInFlight >= maxWritesInFlight) {
                    CurrentMaxWritesInFlight = maxWritesInFlight;
                    PutBackoff.Reset();
                }

                Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_PUT_MAX_WRITES_IN_FLIGHT] = CurrentMaxWritesInFlight;
            }
        }

        RunPendingPrepareWritesIfPossible();
    }

    void TS3Manager::OnS3WritesInFlightAbandoned(ui32 count) {
        Y_ABORT_UNLESS(S3WritesInFlight >= count);
        S3WritesInFlight -= count;
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_PUT_WRITES_IN_FLIGHT] = S3WritesInFlight;

        // these writes never completed, so the concurrency step-up logic must not treat them as successful batches
        RunPendingPrepareWritesIfPossible();
    }

} // NKikimr::NBlobDepot
