#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/interconnect/interconnect_direct_session.h>

#include <util/generic/hash.h>
#include <util/system/mutex.h>

#include <atomic>
#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib {

////////////////////////////////////////////////////////////////////////////////

// Test double for IDirectSession. Supports the long-lived registration flow
// used by TICDirectStorageTransport: RegisterReceiveCallback once for a
// virtual reply ActorId, then many Send(ev) calls with that Sender. Creates
// exactly one bridge actor per registered localActorId; Send rewrites Sender
// to the bridge so the stub can reply locally and the bridge forwards into
// IReceiveCallback::Receive.
//
// Important for DDisk ValidateConnection: Connect and datapath both travel
// locally with empty InterconnectSession, so the session-id check still passes.
// Cookies pass through RewriteSender untouched for cookie demultiplexing.
class TFakeDirectSession
    : public NActors::IDirectSession
    , public std::enable_shared_from_this<TFakeDirectSession>
{
public:
    explicit TFakeDirectSession(NActors::TActorSystem* actorSystem);

    bool Send(
        TAutoPtr<NActors::IEventHandle> ev,
        TIntrusivePtr<NActors::IReceiveCallback> replyCallback =
            nullptr) override;

    void RegisterReceiveCallback(
        const NActors::TActorId& localActorId,
        TIntrusivePtr<NActors::IReceiveCallback> callback) override;

    void UnregisterReceiveCallback(
        const NActors::TActorId& localActorId) override;

    void Shutdown();

    [[nodiscard]] bool IsConnected() const;

    // Number of events successfully forwarded via Send(). Used by tests to
    // prove the datapath hit IDirectSession rather than falling back.
    [[nodiscard]] ui64 GetSentEventCount() const;

    // Used by the reply-bridge actor; returns nullptr if Shutdown() /
    // Unregister already dropped the callback.
    [[nodiscard]] TIntrusivePtr<NActors::IReceiveCallback> FindCallback(
        const NActors::TActorId& localActorId) const;

private:
    struct TRegistration
    {
        TIntrusivePtr<NActors::IReceiveCallback> Callback;
        NActors::TActorId BridgeId;
    };

    NActors::TActorSystem* const ActorSystem;
    std::atomic<bool> Connected{true};
    std::atomic<ui64> SentEventCount{0};

    mutable TMutex Lock;
    // Keyed by the transport's virtual reply ActorId (localActorId).
    THashMap<NActors::TActorId, TRegistration, NActors::TActorId::THash>
        Registrations;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib
