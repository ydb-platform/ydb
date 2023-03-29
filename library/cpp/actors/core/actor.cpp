#include "actor.h"
#include "actor_virtual.h"
#include "actorsystem.h"
#include "executor_thread.h"
#include <library/cpp/actors/util/datetime.h>

namespace NActors {
    Y_POD_THREAD(TThreadContext*) TlsThreadContext(nullptr);
    Y_POD_THREAD(TActivationContext*) TlsActivationContext(nullptr);

    void IActor::Describe(IOutputStream &out) const noexcept {
        SelfActorId.Out(out);
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
        Y_VERIFY_DEBUG(parentId);
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
            Y_VERIFY(ctx.SelfID == SelfActorId);
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
        (actor->*StateFunc)(ev, TActivationContext::AsActorContext());
    }

    void TActorVirtualBehaviour::Receive(IActor* actor, std::unique_ptr<IEventHandle> ev) {
        Y_VERIFY(!!ev && ev->GetBase());
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
