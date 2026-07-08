#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event.h>

#include <util/generic/hash.h>
#include <util/system/mutex.h>

#include <functional>
#include <memory>

namespace NActors {

    // Thread-safe handle to an established interconnect session (v2) that allows sending and receiving
    // events to/from the remote peer without going through the actor system. It is delivered to
    // subscribers inside TEvInterconnect::TEvNodeConnected as a std::shared_ptr.
    //
    // Lifecycle / races: both the session actor and every user hold a std::shared_ptr to the same
    // object. When the connection is lost the session calls Shutdown(), which atomically transitions
    // the handle into a disconnected state (Send() starts returning false and all registered callbacks
    // are dropped). All methods are safe to call concurrently from arbitrary threads.
    class IDirectSession {
    public:
        // Invoked (on the session actor thread) for an incoming event addressed to a registered ActorId.
        using TReceiveCallback = std::function<void(TAutoPtr<IEventHandle>)>;

        virtual ~IDirectSession() = default;

        // Sends an event to the remote peer. The event's Recipient identifies the destination actor on
        // the peer node. Returns false if the session is no longer connected. Thread-safe.
        virtual bool Send(TAutoPtr<IEventHandle> ev) = 0;

        // Registers (or replaces) a callback invoked for incoming events addressed to localActorId.
        // Has no effect once the session is disconnected. Thread-safe.
        virtual void RegisterReceiveCallback(const TActorId& localActorId, TReceiveCallback callback) = 0;

        // Removes a previously registered callback. Thread-safe.
        virtual void UnregisterReceiveCallback(const TActorId& localActorId) = 0;
    };

    // Concrete implementation of IDirectSession backing a TInterconnectSessionTCPv2 connection.
    // The data-plane transfer (actual serialization / socket handoff of outgoing events and decoding
    // of incoming ones) is intentionally left as a stub; the thread-safe registration table and the
    // connect/disconnect lifecycle are fully implemented.
    class TDirectSession final : public IDirectSession {
    public:
        bool Send(TAutoPtr<IEventHandle> ev) override {
            TGuard<TMutex> guard(Mutex);
            if (!Connected) {
                return false;
            }
            // TODO(v2 data plane): hand the event off to the session's outgoing stream.
            Y_UNUSED(ev);
            return true;
        }

        void RegisterReceiveCallback(const TActorId& localActorId, TReceiveCallback callback) override {
            TGuard<TMutex> guard(Mutex);
            if (!Connected) {
                return;
            }
            Callbacks[localActorId] = std::move(callback);
        }

        void UnregisterReceiveCallback(const TActorId& localActorId) override {
            TGuard<TMutex> guard(Mutex);
            Callbacks.erase(localActorId);
        }

        // Delivers an incoming event to the callback registered for its Recipient, if any. Intended to
        // be called on the session actor thread by the (future) v2 data plane. Returns true if a
        // callback consumed the event. Thread-safe.
        bool DeliverIncoming(TAutoPtr<IEventHandle> ev) {
            TReceiveCallback callback;
            {
                TGuard<TMutex> guard(Mutex);
                if (!Connected) {
                    return false;
                }
                const auto it = Callbacks.find(ev->GetRecipientRewrite());
                if (it == Callbacks.end()) {
                    return false;
                }
                callback = it->second;
            }
            // invoke outside the lock to avoid re-entrancy/deadlocks
            callback(std::move(ev));
            return true;
        }

        // Transitions the handle into the disconnected state. Called by the session on teardown.
        // After this returns, Send() fails and no callbacks remain registered. Thread-safe.
        void Shutdown() {
            TGuard<TMutex> guard(Mutex);
            Connected = false;
            Callbacks.clear();
        }

        bool IsConnected() const {
            TGuard<TMutex> guard(Mutex);
            return Connected;
        }

    private:
        mutable TMutex Mutex;
        bool Connected = true;
        THashMap<TActorId, TReceiveCallback, TActorId::THash> Callbacks;
    };

}
