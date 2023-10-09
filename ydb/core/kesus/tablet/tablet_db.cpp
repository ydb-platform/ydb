#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

void TKesusTablet::PersistSysParam(NIceDb::TNiceDb& db, ui64 id, const TString& value) {
    db.Table<Schema::SysParams>().Key(id).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(value));
}

void TKesusTablet::PersistDeleteSession(NIceDb::TNiceDb& db, ui64 sessionId) {
    db.Table<Schema::Sessions>().Key(sessionId).Delete();
}

void TKesusTablet::PersistDeleteSessionSemaphore(NIceDb::TNiceDb& db, ui64 sessionId, ui64 semaphoreId) {
    db.Table<Schema::SessionSemaphores>().Key(sessionId, semaphoreId).Delete();
}

void TKesusTablet::PersistDeleteSemaphore(NIceDb::TNiceDb& db, ui64 semaphoreId) {
    db.Table<Schema::Semaphores>().Key(semaphoreId).Delete();
}

void TKesusTablet::PersistStrictMarker(NIceDb::TNiceDb& db) {
    PersistSysParam(db, Schema::SysParam_StrictMarkerCounter, ToString(++StrictMarkerCounter));
}

void TKesusTablet::DoDeleteSession(
    NIceDb::TNiceDb& db, TSessionInfo* session, TVector<TDelayedEvent>& events)
{
    ui64 sessionId = session->Id;
    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::KESUS_TABLET,
        "[" << TabletID() << "] Deleting session " << sessionId);
    if (session->DetachProxy()) {
        TabletCounters->Simple()[COUNTER_SESSION_ACTIVE_COUNT].Add(-1);
    }
    for (auto& kv : session->OwnedSemaphores) {
        ui64 semaphoreId = kv.first;
        auto* semaphore = Semaphores.FindPtr(semaphoreId);
        Y_ABORT_UNLESS(semaphore, "Session %" PRIu64 " owns missing semaphore: %" PRIu64, sessionId, semaphoreId);
        DoDeleteSessionSemaphore(db, semaphore, &kv.second, events);
    }
    for (auto& kv : session->WaitingSemaphores) {
        ui64 semaphoreId = kv.first;
        auto* semaphore = Semaphores.FindPtr(semaphoreId);
        Y_ABORT_UNLESS(semaphore, "Session %" PRIu64 " waiting for missing semaphore: %" PRIu64, sessionId, semaphoreId);
        DoDeleteSessionSemaphore(db, semaphore, &kv.second, events);
    }
    Sessions.erase(sessionId);
    PersistDeleteSession(db, sessionId);
    TabletCounters->Simple()[COUNTER_SESSION_COUNT].Add(-1);
}

void TKesusTablet::DoDeleteSemaphore(
    NIceDb::TNiceDb& db, TSemaphoreInfo* semaphore, TVector<TDelayedEvent>& events)
{
    ui64 semaphoreId = semaphore->Id;
    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::KESUS_TABLET,
        "[" << TabletID() << "] Deleting semaphore " << semaphoreId << " " << semaphore->Name.Quote());
    for (auto* owner : semaphore->Owners) {
        auto* session = Sessions.FindPtr(owner->SessionId);
        Y_ABORT_UNLESS(session);
        PersistDeleteSessionSemaphore(db, owner->SessionId, semaphoreId);
        session->OwnedSemaphores.erase(semaphoreId);
        TabletCounters->Simple()[COUNTER_SEMAPHORE_OWNER_COUNT].Add(-1);
    }
    for (const auto& kv : semaphore->Waiters) {
        auto* waiter = kv.second;
        auto* session = Sessions.FindPtr(waiter->SessionId);
        if (auto* proxy = session->OwnerProxy) {
            session->ConsumeSemaphoreWaitCookie(semaphore, [&](ui64 cookie) {
                events.emplace_back(
                    proxy->ActorID,
                    cookie,
                    new TEvKesus::TEvAcquireSemaphoreResult(
                        proxy->Generation,
                        Ydb::StatusIds::ABORTED,
                        "Semaphore destroyed"));
            });
        }
        PersistDeleteSessionSemaphore(db, waiter->SessionId, semaphoreId);
        session->WaitingSemaphores.erase(semaphoreId);
        TabletCounters->Simple()[COUNTER_SEMAPHORE_WAITER_COUNT].Add(-1);
    }
    semaphore->NotifyWatchers(events, true, true);
    SemaphoresByName.erase(semaphore->Name);
    Semaphores.erase(semaphoreId);
    PersistDeleteSemaphore(db, semaphoreId);
    TabletCounters->Simple()[COUNTER_SEMAPHORE_COUNT].Add(-1);
}

void TKesusTablet::DoDeleteSessionSemaphore(
    NIceDb::TNiceDb& db, TSemaphoreInfo* semaphore, TSemaphoreOwnerInfo* owner, TVector<TDelayedEvent>& events)
{
    ui64 semaphoreId = semaphore->Id;
    ui64 sessionId = owner->SessionId;
    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::KESUS_TABLET,
        "[" << TabletID() << "] Deleting session " << sessionId << " / semaphore " << semaphoreId << " " << semaphore->Name.Quote()
            << " owner link");
    Y_ABORT_UNLESS(semaphore->Owners.contains(owner));
    semaphore->Count -= owner->Count;
    semaphore->Owners.erase(owner);
    PersistDeleteSessionSemaphore(db, sessionId, semaphoreId);
    TabletCounters->Simple()[COUNTER_SEMAPHORE_OWNER_COUNT].Add(-1);
    if (semaphore->IsEmpty() && semaphore->Ephemeral) {
        semaphore->NotifyWatchers(events, true, true);
        SemaphoresByName.erase(semaphore->Name);
        Semaphores.erase(semaphoreId);
        PersistDeleteSemaphore(db, semaphoreId);
        TabletCounters->Simple()[COUNTER_SEMAPHORE_COUNT].Add(-1);
    } else {
        DoProcessSemaphoreQueue(semaphore, events, true);
        Y_ABORT_UNLESS(!semaphore->Ephemeral || !semaphore->IsEmpty());
    }
}

void TKesusTablet::DoDeleteSessionSemaphore(
    NIceDb::TNiceDb& db, TSemaphoreInfo* semaphore, TSemaphoreWaiterInfo* waiter, TVector<TDelayedEvent>& events)
{
    ui64 semaphoreId = semaphore->Id;
    ui64 orderId = waiter->OrderId;
    ui64 sessionId = waiter->SessionId;
    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::KESUS_TABLET,
        "[" << TabletID() << "] Deleting session " << sessionId << " / semaphore " << semaphoreId << " " << semaphore->Name.Quote()
            << " waiter link");
    Y_ABORT_UNLESS(semaphore->Waiters.Value(orderId, nullptr) == waiter);
    bool needProcessSemaphoreQueue = semaphore->GetFirstOrderId() == orderId;
    semaphore->Waiters.erase(orderId);
    PersistDeleteSessionSemaphore(db, sessionId, semaphoreId);
    TabletCounters->Simple()[COUNTER_SEMAPHORE_WAITER_COUNT].Add(-1);
    if (needProcessSemaphoreQueue) {
        Y_ABORT_UNLESS(!semaphore->IsEmpty());
        DoProcessSemaphoreQueue(semaphore, events);
        Y_ABORT_UNLESS(!semaphore->IsEmpty());
    }
}

void TKesusTablet::DoProcessSemaphoreQueue(
        TSemaphoreInfo* semaphore, TVector<TDelayedEvent>& events, bool ownersChanged)
{
    ui64 semaphoreId = semaphore->Id;
    auto it = semaphore->Waiters.begin();
    while (it != semaphore->Waiters.end()) {
        auto* waiter = it->second;
        if (!semaphore->CanAcquire(waiter->Count)) {
            break;
        }

        ui64 orderId = waiter->OrderId;
        ui64 sessionId = waiter->SessionId;
        LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::KESUS_TABLET,
            "[" << TabletID() << "] Processing semaphore " << semaphoreId << " " << semaphore->Name.Quote()
                << " queue: next order #" << orderId << " session " << sessionId);

        auto* session = Sessions.FindPtr(sessionId);
        Y_ABORT_UNLESS(session,
            "Semaphore %s points to missing session: %" PRIu64 " (wait order %" PRIu64 ")",
            semaphore->Name.Quote().data(), sessionId, orderId);

        Y_ABORT_UNLESS(!session->OwnedSemaphores.contains(semaphoreId));
        auto* owner = &session->OwnedSemaphores[semaphoreId];
        owner->OrderId = orderId;
        owner->SessionId = sessionId;
        owner->Count = waiter->Count;
        owner->Data = waiter->Data;
        semaphore->Count += waiter->Count;
        semaphore->Waiters.erase(it++);
        semaphore->Owners.insert(owner);
        session->WaitingSemaphores.erase(semaphoreId);
        TabletCounters->Simple()[COUNTER_SEMAPHORE_OWNER_COUNT].Add(1);
        TabletCounters->Simple()[COUNTER_SEMAPHORE_WAITER_COUNT].Add(-1);

        if (auto* proxy = session->OwnerProxy) {
            session->ConsumeSemaphoreWaitCookie(semaphore, [&](ui64 cookie) {
                events.emplace_back(
                    proxy->ActorID,
                    cookie,
                    new TEvKesus::TEvAcquireSemaphoreResult(proxy->Generation));
            });
        }

        ownersChanged = true;
    }

    semaphore->NotifyWatchers(events, false, ownersChanged);
}

void TKesusTablet::TSessionInfo::ClearWatchCookies() {
    for (const auto& kv : SemaphoreWatchCookie) {
        auto* semaphore = kv.first;
        semaphore->DataWatchers.erase(this);
        semaphore->OwnersWatchers.erase(this);
    }
    SemaphoreWatchCookie.clear();
}

void TKesusTablet::TSemaphoreInfo::NotifyWatchers(TVector<TDelayedEvent>& events, bool dataChanged, bool ownersChanged) {
    if (dataChanged) {
        for (auto* session : DataWatchers) {
            OwnersWatchers.erase(session);
            Y_ABORT_UNLESS(session->OwnerProxy, "unexpected notify for unattached session");
            events.emplace_back(
                session->OwnerProxy->ActorID,
                session->RemoveSemaphoreWatchCookie(this),
                new TEvKesus::TEvDescribeSemaphoreChanged(
                    session->OwnerProxy->Generation, dataChanged, ownersChanged));
        }
        DataWatchers.clear();
    }
    if (ownersChanged) {
        for (auto* session : OwnersWatchers) {
            if (!dataChanged) {
                DataWatchers.erase(session);
            }
            Y_ABORT_UNLESS(session->OwnerProxy, "unexpected notify for unattached session");
            events.emplace_back(
                session->OwnerProxy->ActorID,
                session->RemoveSemaphoreWatchCookie(this),
                new TEvKesus::TEvDescribeSemaphoreChanged(
                    session->OwnerProxy->Generation, dataChanged, ownersChanged));
        }
        OwnersWatchers.clear();
    }
}

}
}
