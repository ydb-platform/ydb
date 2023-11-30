#include "actor.h"
#include "actor_virtual.h"
#include "actorsystem.h"
#include "executor_thread.h"
#include <ydb/library/actors/util/datetime.h>

namespace NActors {
    Y_POD_THREAD(TThreadContext*) TlsThreadContext(nullptr);
    thread_local TActivationContext *TActivationContextHolder::Value = nullptr;
    TActivationContextHolder TlsActivationContext;

    [[gnu::noinline]] TActivationContextHolder::operator bool() const {
        asm volatile("");
        return Value != nullptr;
    }

    [[gnu::noinline]] TActivationContextHolder::operator TActivationContext*() const {
        asm volatile("");
        return Value;
    }

    [[gnu::noinline]] TActivationContext *TActivationContextHolder::operator ->() {
        asm volatile("");
        return Value;
    }

    [[gnu::noinline]] TActivationContext& TActivationContextHolder::operator *() {
        asm volatile("");
        return *Value;
    }

    [[gnu::noinline]] TActivationContextHolder& TActivationContextHolder::operator =(TActivationContext *context) {
        asm volatile("");
        Value = context;
        return *this;
    }

    template<i64 Increment>
    static void UpdateQueueSizeAndTimestamp(TActorUsageImpl<true>& impl, ui64 time) {
        ui64 usedTimeIncrement = 0;
        using T = TActorUsageImpl<true>;

        for (;;) {
            uint64_t value = impl.QueueSizeAndTimestamp.load();
            ui64 count = value >> T::TimestampBits;

            count += Increment;
            Y_ABORT_UNLESS((count & ~T::CountMask) == 0);

            ui64 timestamp = value;
            if (Increment == 1 && count == 1) {
                timestamp = time;
            } else if (Increment == -1 && count == 0) {
                usedTimeIncrement = (static_cast<ui64>(time) - timestamp) & T::TimestampMask;
                timestamp = 0; // reset timestamp to some zero value
            }

            const ui64 updated = (timestamp & T::TimestampMask) | (count << T::TimestampBits);
            if (impl.QueueSizeAndTimestamp.compare_exchange_weak(value, updated)) {
                break;
            }
        }

        if (usedTimeIncrement && impl.LastUsageTimestamp <= time) {
            impl.UsedTime += usedTimeIncrement;
        }
    }

    void TActorUsageImpl<true>::OnEnqueueEvent(ui64 time) {
        UpdateQueueSizeAndTimestamp<+1>(*this, time);
    }

    void TActorUsageImpl<true>::OnDequeueEvent() {
        UpdateQueueSizeAndTimestamp<-1>(*this, GetCycleCountFast());
    }

    double TActorUsageImpl<true>::GetUsage(ui64 time) {
        ui64 used = UsedTime.exchange(0);
        if (const ui64 value = QueueSizeAndTimestamp.load(); value >> TimestampBits) {
            used += (static_cast<ui64>(time) - value) & TimestampMask;
        }

        Y_ABORT_UNLESS(LastUsageTimestamp <= time);
        ui64 passed = time - LastUsageTimestamp;
        LastUsageTimestamp = time;

        if (!passed) {
            return 0;
        }

        return (double)Min(passed, used) / passed;
    }

    void IActor::Describe(IOutputStream &out) const noexcept {
        SelfActorId.Out(out);
    }

    bool IActor::Send(TAutoPtr<IEventHandle> ev) const noexcept {
        return TActivationContext::Send(ev);
    }

    bool IActor::Send(const TActorId& recipient, IEventBase* ev, ui32 flags, ui64 cookie, NWilson::TTraceId traceId) const noexcept {
        return SelfActorId.Send(recipient, ev, flags, cookie, std::move(traceId));
    }

    void TActivationContext::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        TlsActivationContext->ExecutorThread.Schedule(deadline, ev, cookie);
    }

    void TActivationContext::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        TlsActivationContext->ExecutorThread.Schedule(deadline, ev, cookie);
    }

    void TActivationContext::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        TlsActivationContext->ExecutorThread.Schedule(delta, ev, cookie);
    }

    void TActorIdentity::Schedule(TInstant deadline, IEventBase* ev, ISchedulerCookie* cookie) const {
        return TActivationContext::Schedule(deadline, new IEventHandle(*this, {}, ev), cookie);
    }

    void TActorIdentity::Schedule(TMonotonic deadline, IEventBase* ev, ISchedulerCookie* cookie) const {
        return TActivationContext::Schedule(deadline, new IEventHandle(*this, {}, ev), cookie);
    }

    void TActorIdentity::Schedule(TDuration delta, IEventBase* ev, ISchedulerCookie* cookie) const {
        return TActivationContext::Schedule(delta, new IEventHandle(*this, {}, ev), cookie);
    }

    TActorId TActivationContext::RegisterWithSameMailbox(IActor* actor, TActorId parentId) {
        Y_DEBUG_ABORT_UNLESS(parentId);
        auto& ctx = *TlsActivationContext;
        return ctx.ExecutorThread.RegisterActor(actor, &ctx.Mailbox, parentId.Hint(), parentId);
    }

    TActorId TActorContext::RegisterWithSameMailbox(IActor* actor) const {
        return ExecutorThread.RegisterActor(actor, &Mailbox, SelfID.Hint(), SelfID);
    }

    TActorId IActor::RegisterWithSameMailbox(IActor* actor) const noexcept {
        return TlsActivationContext->ExecutorThread.RegisterActor(actor, &TlsActivationContext->Mailbox, SelfActorId.Hint(), SelfActorId);
    }

    TActorId TActivationContext::InterconnectProxy(ui32 destinationNodeId) {
        return TlsActivationContext->ExecutorThread.ActorSystem->InterconnectProxy(destinationNodeId);
    }

    TActorSystem* TActivationContext::ActorSystem() {
        return TlsActivationContext->ExecutorThread.ActorSystem;
    }

    i64 TActivationContext::GetCurrentEventTicks() {
        return GetCycleCountFast() - TlsActivationContext->EventStart;
    }

    double TActivationContext::GetCurrentEventTicksAsSeconds() {
        return NHPTimer::GetSeconds(GetCurrentEventTicks());
    }

    TActorId IActor::Register(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId) const noexcept {
        return TlsActivationContext->ExecutorThread.RegisterActor(actor, mailboxType, poolId, SelfActorId);
    }

    void TActorContext::Schedule(TInstant deadline, IEventBase* ev, ISchedulerCookie* cookie) const {
        ExecutorThread.Schedule(deadline, new IEventHandle(SelfID, TActorId(), ev), cookie);
    }

    void TActorContext::Schedule(TMonotonic deadline, IEventBase* ev, ISchedulerCookie* cookie) const {
        ExecutorThread.Schedule(deadline, new IEventHandle(SelfID, TActorId(), ev), cookie);
    }

    void TActorContext::Schedule(TDuration delta, IEventBase* ev, ISchedulerCookie* cookie) const {
        ExecutorThread.Schedule(delta, new IEventHandle(SelfID, TActorId(), ev), cookie);
    }

    void IActor::Schedule(TInstant deadline, IEventBase* ev, ISchedulerCookie* cookie) const noexcept {
        TlsActivationContext->ExecutorThread.Schedule(deadline, new IEventHandle(SelfActorId, TActorId(), ev), cookie);
    }

    void IActor::Schedule(TMonotonic deadline, IEventBase* ev, ISchedulerCookie* cookie) const noexcept {
        TlsActivationContext->ExecutorThread.Schedule(deadline, new IEventHandle(SelfActorId, TActorId(), ev), cookie);
    }

    void IActor::Schedule(TDuration delta, IEventBase* ev, ISchedulerCookie* cookie) const noexcept {
        TlsActivationContext->ExecutorThread.Schedule(delta, new IEventHandle(SelfActorId, TActorId(), ev), cookie);
    }

    TInstant TActivationContext::Now() {
        return TlsActivationContext->ExecutorThread.ActorSystem->Timestamp();
    }

    TMonotonic TActivationContext::Monotonic() {
        return TlsActivationContext->ExecutorThread.ActorSystem->Monotonic();
    }

    TInstant TActorContext::Now() const {
        return ExecutorThread.ActorSystem->Timestamp();
    }

    TMonotonic TActorContext::Monotonic() const {
        return ExecutorThread.ActorSystem->Monotonic();
    }

    NLog::TSettings* TActivationContext::LoggerSettings() const {
        return ExecutorThread.ActorSystem->LoggerSettings();
    }

    std::pair<ui32, ui32> TActorContext::CountMailboxEvents(ui32 maxTraverse) const {
        return Mailbox.CountMailboxEvents(SelfID.LocalId(), maxTraverse);
    }

    std::pair<ui32, ui32> IActor::CountMailboxEvents(ui32 maxTraverse) const {
        return TlsActivationContext->Mailbox.CountMailboxEvents(SelfActorId.LocalId(), maxTraverse);
    }

    void IActor::Die(const TActorContext& ctx) {
        if (ctx.SelfID)
            Y_ABORT_UNLESS(ctx.SelfID == SelfActorId);
        PassAway();
    }

    void IActor::PassAway() {
        auto& cx = *TlsActivationContext;
        cx.ExecutorThread.UnregisterActor(&cx.Mailbox, SelfActorId);
    }

    double IActor::GetElapsedTicksAsSeconds() const {
        return NHPTimer::GetSeconds(ElapsedTicks);
    }

    void TActorCallbackBehaviour::Receive(IActor* actor, TAutoPtr<IEventHandle>& ev) {
        (actor->*StateFunc)(ev);
    }

    void TActorVirtualBehaviour::Receive(IActor* actor, std::unique_ptr<IEventHandle> ev) {
        Y_ABORT_UNLESS(!!ev && ev->GetBase());
        ev->GetBase()->Execute(actor, std::move(ev));
    }

    void IActor::Registered(TActorSystem* sys, const TActorId& owner) {
        // fallback to legacy method, do not use it anymore
        if (auto eh = AfterRegister(SelfId(), owner)) {
            if (!TlsThreadContext || TlsThreadContext->SendingType == ESendingType::Common) {
                sys->Send(eh);
            } else {
                sys->SpecificSend(eh);
            }
        }
    }
}
