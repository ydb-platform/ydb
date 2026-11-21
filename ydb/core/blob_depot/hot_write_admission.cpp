#include "blob_depot_tablet.h"
#include "data.h"
#include "s3.h"

namespace NKikimr::NBlobDepot {
    namespace {
        constexpr ui64 CreditBatchBytes = 1 << 20;
        constexpr TDuration AdmissionRetry = TDuration::MilliSeconds(100);
        constexpr TDuration CreditRecallInterval = TDuration::MilliSeconds(250);

        void Reply(TEvBlobDepot::TEvHotWriteCredit::THandle& request, NKikimrProto::EReplyStatus status,
                const char *error, ui64 granted = 0) {
            auto [response, record] = TEvBlobDepot::MakeResponseFor(request);
            record->SetEpoch(request.Get()->Record.GetEpoch());
            record->SetStatus(status);
            if (granted) {
                record->SetGrantedBytes(granted);
            }
            if (error) {
                record->SetErrorReason(error);
            }
            TActivationContext::Send(response.release());
        }
    }

    bool TBlobDepot::IsHotWriteAdmissionEnabled() const {
        return IsDataCoolingEnabled() && Config.GetS3BackendSettings().GetAsyncMode().HasMaxPendingBytes();
    }

    ui64 TBlobDepot::GetHotWriteHeadroom() const {
        const ui64 maxBytes = Config.GetS3BackendSettings().GetAsyncMode().GetMaxPendingBytes();
        const ui64 live = Data->GetTotalStoredDataSize();
        const ui64 headroom = maxBytes - Min(maxBytes, live);
        const ui64 durableHeadroom = headroom - Min(headroom, Data->GetHotBytesPendingTrashCommit());
        return durableHeadroom - Min(durableHeadroom, HotWriteCreditBytes);
    }

    ui64 TBlobDepot::GetHotWriteCoolingBytesNeeded() const {
        if (!IsHotWriteAdmissionEnabled() || !Data->IsLoaded()) {
            return 0;
        }
        const ui64 agentDemand = HotWriteCreditQueue.empty() ? 0 : HotWriteCreditQueue.front()->Get()->Record.GetMinBytes();
        const ui64 internalDemand = InternalHotWriteWaiters.empty() ? 0 : InternalHotWriteWaiters.rbegin()->first.first;
        const ui64 demand = Max(agentDemand, internalDemand);
        const ui64 headroom = GetHotWriteHeadroom();
        return demand - Min(demand, headroom);
    }

    void TBlobDepot::RefreshHotWriteRate() {
        const auto& mode = Config.GetS3BackendSettings().GetAsyncMode();
        if (!mode.HasThrottleStartBytes() || !Data->IsLoaded()) {
            return;
        }
        const TMonotonic now = TActivationContext::Monotonic();
        const ui64 headroom = GetHotWriteHeadroom();
        const ui64 interval = mode.GetMaxPendingBytes() - mode.GetThrottleStartBytes();
        if (headroom > interval) {
            HotWriteBucket.SetUnlimited();
            return;
        }
        if (HotWriteBucket.IsUnlimited()) {
            // Start the throttled band with no credit accumulated during unlimited admission.
            HotWriteBucket.SetCapacity(0);
            HotWriteBucket.SetCapacity(MaxWriteCreditBytes);
        }
        HotWriteBucket.Fill(now);
        HotWriteBucket.SetRate(static_cast<double>(mode.GetThrottleMaxBytesPerSecond()) * headroom / interval);
    }

    ui64 TBlobDepot::GetAvailableHotWriteBytes() const {
        const ui64 headroom = Data->IsLoaded() ? GetHotWriteHeadroom() : 0;
        return HotWriteBucket.IsUnlimited() ? headroom : Min(headroom, static_cast<ui64>(HotWriteBucket.Available()));
    }

    void TBlobDepot::ScheduleHotWriteAdmission() {
        if (!IsHotWriteAdmissionEnabled()) {
            return;
        }
        RefreshHotWriteRate();
        if ((!HotWriteCreditQueue.empty() || !InternalHotWriteWaiters.empty())
                && !std::exchange(HotWriteAdmissionScheduled, true)) {
            TActivationContext::Send(new IEventHandle(TEvPrivate::EvRunHotWriteAdmission, 0, SelfId(), {}, nullptr, 0));
        }
    }

    void TBlobDepot::ChargeHotWriteCredits(ui64 bytes) {
        if (!HotWriteBucket.IsUnlimited()) {
            HotWriteBucket.Take(bytes);
        }
        HotWriteCreditBytes += bytes;
        TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_HOT_WRITE_CREDIT_BYTES] = HotWriteCreditBytes;
        RefreshHotWriteRate();
    }

    void TBlobDepot::ReleaseHotWriteCredits(ui64 bytes) {
        if (!bytes) {
            return;
        }
        Y_ABORT_UNLESS(bytes <= HotWriteCreditBytes);
        HotWriteCreditBytes -= bytes;
        TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_HOT_WRITE_CREDIT_BYTES] = HotWriteCreditBytes;
        ScheduleHotWriteAdmission();
    }

    void TBlobDepot::DropHotWriteCredits(TAgent& agent) {
        std::erase_if(HotWriteCreditQueue, [&](const auto& ev) {
            return ev->Recipient == agent.Connection->PipeServerId;
        });
        TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_HOT_WRITE_WAITERS] = HotWriteCreditQueue.size();
        ReleaseHotWriteCredits(std::exchange(agent.HotWriteCreditBytes, 0));
        agent.LastHotWriteCreditRecall = {};
    }

    void TBlobDepot::RecallHotWriteCredits() {
        if (!HotWriteCreditBytes) {
            return;
        }
        const TMonotonic now = TActivationContext::Monotonic();
        for (auto& [_, agent] : Agents) {
            if (agent.Connection && agent.HotWriteCreditBytes && (!agent.LastHotWriteCreditRecall
                    || now - agent.LastHotWriteCreditRecall >= CreditRecallInterval)) {
                agent.LastHotWriteCreditRecall = now;
                auto ev = std::make_unique<TEvBlobDepot::TEvPushNotify>();
                ev->Record.SetRecallHotWriteCredits(true);
                TActivationContext::Send(new IEventHandle(agent.Connection->AgentId, agent.Connection->PipeServerId,
                    ev.release(), 0, ++agent.LastRequestId));
            }
        }
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvHotWriteCredit::TPtr ev) {
        auto& agent = GetAgent(ev->Recipient);
        const auto& request = ev->Get()->Record;
        if (!IsHotWriteAdmissionEnabled() || !agent.SupportsHotWriteCredits || request.GetEpoch() != agent.LastConnectionSequence
                || request.GetReturnBytes() > agent.HotWriteCreditBytes) {
            if (request.HasMinBytes()) {
                Reply(*ev, NKikimrProto::ERROR, "invalid hot write credit epoch or return amount");
            }
            return;
        }
        if (const ui64 returned = request.GetReturnBytes()) {
            agent.HotWriteCreditBytes -= returned;
            ReleaseHotWriteCredits(returned);
        }
        if (!request.HasMinBytes()) {
            return;
        }
        const ui64 maxBytes = Config.GetS3BackendSettings().GetAsyncMode().GetMaxPendingBytes();
        if (!request.GetMinBytes() || request.GetMinBytes() > Min(maxBytes, MaxWriteCreditBytes)) {
            return Reply(*ev, NKikimrProto::ERROR, "write exceeds the hot data byte limit");
        }
        if (request.GetFailOnSlowDown() && !HotWriteCreditQueue.empty()) {
            return Reply(*ev, NKikimrProto::ERROR, "SlowDown");
        }
        HotWriteCreditQueue.push_back(ev);
        TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_HOT_WRITE_WAITERS] = HotWriteCreditQueue.size();
        ScheduleHotWriteAdmission();
    }

    void TBlobDepot::WakeHotWriteAdmission(TAutoPtr<IEventHandle> ev) {
        if (HotWriteAdmissionTimer.Consume(ev->Cookie)) {
            ScheduleHotWriteAdmission();
        }
    }

    void TBlobDepot::RunHotWriteAdmission() {
        HotWriteAdmissionScheduled = false;
        if (!IsHotWriteAdmissionEnabled()) {
            return;
        }
        RefreshHotWriteRate();
        // Expire deadlines throughout the queue, including requests behind one without a deadline.
        const ui64 nowUs = TActivationContext::Now().MicroSeconds();
        std::erase_if(HotWriteCreditQueue, [&](const auto& ev) {
            const auto& request = ev->Get()->Record;
            if (request.HasDeadlineUs() && request.GetDeadlineUs() <= nowUs) {
                Reply(*ev, NKikimrProto::DEADLINE, "hot write credit deadline exceeded");
                return true;
            }
            return false;
        });
        while (!HotWriteCreditQueue.empty()) {
            auto& ev = HotWriteCreditQueue.front();
            const auto& request = ev->Get()->Record;
            const ui64 available = GetAvailableHotWriteBytes();
            const ui64 minBytes = request.GetMinBytes();
            if (available < minBytes) {
                if (request.GetFailOnSlowDown()) {
                    Reply(*ev, NKikimrProto::ERROR, "SlowDown");
                    HotWriteCreditQueue.pop_front();
                    continue;
                }
                break;
            }
            ui64 granted = Min(available, Max(minBytes, CreditBatchBytes));
            const auto& mode = Config.GetS3BackendSettings().GetAsyncMode();
            if (mode.HasThrottleStartBytes() && HotWriteBucket.IsUnlimited()) {
                // Let a single PUT cross the threshold, but keep speculative credit below the rate limit.
                const ui64 remainingBelowThrottle = available - (mode.GetMaxPendingBytes() - mode.GetThrottleStartBytes());
                granted = Min(granted, Max(minBytes, remainingBelowThrottle));
            }
            GetAgent(ev->Recipient).HotWriteCreditBytes += granted;
            ChargeHotWriteCredits(granted);
            Reply(*ev, NKikimrProto::OK, nullptr, granted);
            HotWriteCreditQueue.pop_front();
        }
        TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_HOT_WRITE_WAITERS] = HotWriteCreditQueue.size();

        if (GetHotWriteCoolingBytesNeeded()) {
            RecallHotWriteCredits();
            S3Manager->MaybeScheduleCooling();
        }
        const auto now = TActivationContext::Monotonic();
        auto wakeAt = HotWriteCreditQueue.empty() ? TMonotonic::Max() : now + AdmissionRetry;
        if (auto it = InternalHotWriteWaiters.begin(); it != InternalHotWriteWaiters.end()) {
            const auto& [bytes, actor] = it->first;
            if (GetAvailableHotWriteBytes() >= bytes) {
                TActivationContext::Send(new IEventHandle(it->second, 0, actor, SelfId(), nullptr, 0));
                InternalHotWriteWaiters.erase(it);
                // The writer retries on this mailbox. Recheck the next waiter after its reservation.
                ScheduleHotWriteAdmission();
            } else if (GetHotWriteHeadroom() >= bytes && HotWriteBucket.GetRate() > 0) {
                // Headroom changes wake admission directly. Only token refill needs a clock.
                const auto refill = TDuration::Seconds((bytes - HotWriteBucket.Available()) / HotWriteBucket.GetRate());
                wakeAt = Min(wakeAt, now + Max(refill, TDuration::MilliSeconds(1)));
            }
        }
        HotWriteAdmissionTimer.Schedule(wakeAt, SelfId(), TEvPrivate::EvWakeHotWriteAdmission);
    }

    bool TBlobDepot::IsInternalWriteTooLarge(ui64 bytes) const {
        return IsHotWriteAdmissionEnabled() && bytes > Config.GetS3BackendSettings().GetAsyncMode().GetMaxPendingBytes();
    }

    void TBlobDepot::WaitForInternalHotWrite(ui64 bytes, TActorId actor, ui32 eventType) {
        const bool inserted = InternalHotWriteWaiters.emplace(std::make_pair(bytes, actor), eventType).second;
        Y_ABORT_UNLESS(inserted);
        ScheduleHotWriteAdmission();
    }

    bool TBlobDepot::CancelInternalHotWriteWait(ui64 bytes, TActorId actor) {
        if (InternalHotWriteWaiters.erase({bytes, actor})) {
            ScheduleHotWriteAdmission();
            return true;
        }
        return false;
    }

    TBlobDepot::TInternalAllocation TBlobDepot::TryAllocateInternalBlobSeqId(TChannelInfo& channel, ui64 bytes) {
        const ui64 sequence = channel.NextBlobSeqId;
        const auto id = TBlobSeqId::FromSequentalNumber(channel.Index, Executor()->Generation(), sequence);
        if (IsHotWriteAdmissionEnabled()) {
            if (IsInternalWriteTooLarge(bytes)) {
                return EInternalWriteError::TooLarge;
            }
            RefreshHotWriteRate();
            if (bytes > GetAvailableHotWriteBytes()) {
                return EInternalWriteError::Retry;
            }
            Y_ABORT_UNLESS(InternalHotWriteCredits.emplace(id, bytes).second);
            ChargeHotWriteCredits(bytes);
        }

        ++channel.NextBlobSeqId;
        // Keep the collect barrier below this sequence while the write is in flight.
        const bool inserted = channel.AssimilatedBlobsInFlight.insert(sequence).second;
        Y_ABORT_UNLESS(inserted);
        return id;
    }

    void TBlobDepot::ReleaseInternalBlobSeqId(const TBlobSeqId& blobSeqId) {
        ReleaseInternalHotWrite(blobSeqId);
        TChannelInfo& channel = Channels[blobSeqId.Channel];
        const ui32 generation = Executor()->Generation();
        const TBlobSeqId leastExpectedBlobIdBefore = channel.GetLeastExpectedBlobId(generation);
        const size_t numErased = channel.AssimilatedBlobsInFlight.erase(blobSeqId.ToSequentialNumber());
        Y_ABORT_UNLESS(numErased == 1);
        if (leastExpectedBlobIdBefore != channel.GetLeastExpectedBlobId(generation)) {
            Data->OnLeastExpectedBlobIdChange(channel.Index);
        }
    }

    void TBlobDepot::ReleaseInternalHotWrite(const TBlobSeqId& id) {
        if (auto credit = InternalHotWriteCredits.extract(id)) {
            ReleaseHotWriteCredits(credit.mapped());
        }
    }

} // NKikimr::NBlobDepot
