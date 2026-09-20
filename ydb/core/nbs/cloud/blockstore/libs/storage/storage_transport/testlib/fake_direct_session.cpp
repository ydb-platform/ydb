#include "fake_direct_session.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Forwards inbound events into the session-owned IReceiveCallback looked up by
// LookupId (the virtual reply ActorId the transport registered). Holds a
// weak_ptr so Shutdown() / UnregisterReceiveCallback() can destroy the session
// without leaving dangling raw pointers.
class TReplyBridge: public TActor<TReplyBridge>
{
public:
    TReplyBridge(std::weak_ptr<TFakeDirectSession> session, TActorId lookupId)
        : TActor(&TThis::StateFunc)
        , Session(std::move(session))
        , LookupId(lookupId)
    {}

private:
    STATEFN(StateFunc)
    {
        switch (ev->GetTypeRewrite()) {
            case TEvents::TSystem::Poison:
                PassAway();
                return;
        }

        if (auto session = Session.lock()) {
            if (auto callback = session->FindCallback(LookupId)) {
                callback->Receive(ev);
            }
        }
    }

    const std::weak_ptr<TFakeDirectSession> Session;
    const TActorId LookupId;
};

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] std::unique_ptr<IEventHandle> RewriteSender(
    TAutoPtr<IEventHandle> ev,
    const TActorId& newSender)
{
    if (ev->HasEvent()) {
        return std::unique_ptr<IEventHandle>(new IEventHandle(
            ev->Recipient,
            newSender,
            ev->ReleaseBase().Release(),
            ev->Flags,
            ev->Cookie,
            nullptr,
            std::move(ev->TraceId)));
    }

    return std::unique_ptr<IEventHandle>(new IEventHandle(
        ev->Type,
        ev->Flags,
        ev->Recipient,
        newSender,
        ev->ReleaseChainBuffer(),
        ev->Cookie,
        nullptr,
        std::move(ev->TraceId)));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFakeDirectSession::TFakeDirectSession(TActorSystem* actorSystem)
    : ActorSystem(actorSystem)
{
    Y_ABORT_UNLESS(ActorSystem);
}

TIntrusivePtr<IReceiveCallback> TFakeDirectSession::FindCallback(
    const TActorId& localActorId) const
{
    with_lock (Lock) {
        if (const auto* reg = Registrations.FindPtr(localActorId)) {
            return reg->Callback;
        }
    }
    return nullptr;
}

bool TFakeDirectSession::Send(
    TAutoPtr<IEventHandle> ev,
    TIntrusivePtr<IReceiveCallback> replyCallback)
{
    if (!Connected.load(std::memory_order_acquire)) {
        return false;
    }

    // Optional per-Send registration (IDirectSession interface allows it).
    // The production transport uses long-lived RegisterReceiveCallback instead.
    if (replyCallback) {
        RegisterReceiveCallback(ev->Sender, std::move(replyCallback));
    }

    TActorId bridgeId;
    {
        with_lock (Lock) {
            if (const auto* reg = Registrations.FindPtr(ev->Sender)) {
                bridgeId = reg->BridgeId;
            }
        }
    }

    if (bridgeId) {
        // Virtual sender NodeId matches the local node — DDisk
        // ValidateConnection checks Sender.NodeId() and InterconnectSession
        // (empty here for both Connect and datapath). Cookie is preserved.
        auto rewritten = RewriteSender(std::move(ev), bridgeId);
        ActorSystem->Send(rewritten.release());
        SentEventCount.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    ActorSystem->Send(ev.Release());
    SentEventCount.fetch_add(1, std::memory_order_relaxed);
    return true;
}

void TFakeDirectSession::RegisterReceiveCallback(
    const TActorId& localActorId,
    TIntrusivePtr<IReceiveCallback> callback)
{
    if (!Connected.load(std::memory_order_acquire)) {
        return;
    }

    if (!callback) {
        UnregisterReceiveCallback(localActorId);
        return;
    }

    TActorId previousBridge;
    {
        with_lock (Lock) {
            if (auto* reg = Registrations.FindPtr(localActorId)) {
                previousBridge = reg->BridgeId;
                reg->Callback = std::move(callback);
            } else {
                auto bridgeId = ActorSystem->Register(
                    new TReplyBridge(shared_from_this(), localActorId));
                Registrations[localActorId] = TRegistration{
                    .Callback = std::move(callback),
                    .BridgeId = bridgeId,
                };
            }
        }
    }

    if (previousBridge) {
        // Replacing the callback keeps the same bridge actor.
        Y_UNUSED(previousBridge);
    }
}

void TFakeDirectSession::UnregisterReceiveCallback(const TActorId& localActorId)
{
    TActorId bridgeId;
    {
        with_lock (Lock) {
            if (auto it = Registrations.find(localActorId);
                it != Registrations.end())
            {
                bridgeId = it->second.BridgeId;
                Registrations.erase(it);
            }
        }
    }
    if (bridgeId) {
        ActorSystem->Send(
            new IEventHandle(bridgeId, TActorId(), new TEvents::TEvPoison()));
    }
}

void TFakeDirectSession::Shutdown()
{
    Connected.store(false, std::memory_order_release);

    // Dropping outstanding registrations completes in-flight promises with
    // OUTDATED / "Session broken" via IReplyHandler / IReceiveCallback
    // destructors — the same contract as a real IDirectSession dying.
    THashMap<TActorId, TRegistration, TActorId::THash> toDrop;
    with_lock (Lock) {
        toDrop.swap(Registrations);
    }
    for (auto& [localActorId, reg]: toDrop) {
        Y_UNUSED(localActorId);
        if (reg.BridgeId) {
            ActorSystem->Send(new IEventHandle(
                reg.BridgeId,
                TActorId(),
                new TEvents::TEvPoison()));
        }
    }
}

bool TFakeDirectSession::IsConnected() const
{
    return Connected.load(std::memory_order_acquire);
}

ui64 TFakeDirectSession::GetSentEventCount() const
{
    return SentEventCount.load(std::memory_order_relaxed);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib
