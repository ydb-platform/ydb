#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/util/funnel_queue.h>

#include "events/events.h"

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/system/mutex.h>

#include <atomic>
#include <memory>

namespace NActors {

    // Refcounted receiver for incoming events delivered outside the actor system. An instance is
    // registered for a local ActorId; the session invokes Receive() (on its input thread) for every
    // incoming event addressed to that ActorId. Held via TIntrusivePtr, so the session co-owns the
    // receiver for as long as it stays registered.
    class IReceiveCallback : public TThrRefBase {
    public:
        virtual void Receive(TAutoPtr<IEventHandle> ev) = 0;
    };

    // Thread-safe handle to an established interconnect session that allows sending and receiving
    // events to/from the remote peer without going through the actor system. It is delivered to
    // subscribers inside TEvInterconnect::TEvNodeConnected as a std::shared_ptr and is universal: the
    // same interface is exposed by both the classic (v1) and the newer (v2) session implementations,
    // so consumers do not need to change when upgrading to a newer protocol.
    //
    // Lifecycle / races: both the session actor and every user hold a std::shared_ptr to the same
    // object. When the connection is lost the session calls Shutdown(), which transitions the handle
    // into a disconnected state (Send() starts returning false and all registered callbacks are
    // dropped). All methods are safe to call concurrently from arbitrary threads.
    class IDirectSession {
    public:
        virtual ~IDirectSession() = default;

        // Sends an event to the remote peer. The event's Recipient identifies the destination actor on
        // the peer node. If replyCallback is set, it is registered for ev->Sender BEFORE the event is
        // dispatched (equivalent to RegisterReceiveCallback(ev->Sender, replyCallback) followed by
        // Send(ev), but saving one virtual call and guaranteeing the reply cannot race ahead of the
        // registration). Returns false if the session is no longer connected. Thread-safe.
        virtual bool Send(TAutoPtr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> replyCallback = nullptr) = 0;

        // Registers (or replaces) a callback invoked for incoming events addressed to localActorId.
        // Has no effect once the session is disconnected. Thread-safe.
        virtual void RegisterReceiveCallback(const TActorId& localActorId, TIntrusivePtr<IReceiveCallback> callback) = 0;

        // Removes a previously registered callback. Thread-safe.
        virtual void UnregisterReceiveCallback(const TActorId& localActorId) = 0;
    };

    // Direct session backing a classic (v1) TInterconnectSessionTCP connection.
    //
    // Design: the producer side (Send / Register / Unregister, callable from arbitrary threads) never
    // takes a lock. Registration changes are pushed to a lock-free MPSC queue; when the queue turns
    // non-empty the (stable) session actor is woken to drain it. The callback table itself is owned by
    // the session/input-session mailbox thread -- the session and its TInputSessionTCP share one
    // mailbox (RegisterWithSameMailbox), so the table is applied and looked up from a single thread
    // without any synchronization. The queue is drained on the wake event and also right before an
    // incoming event is dispatched, so a reply can never overtake its registration.
    //
    // Outgoing events are routed through the actor system, which delivers them to the peer via the
    // existing interconnect machinery.
    class TDirectSessionV1 final : public IDirectSession {
    public:
        TDirectSessionV1(TActorSystem* actorSystem, const TActorId& sessionId, ui32 peerNodeId)
            : ActorSystem(actorSystem)
            , SessionId(sessionId)
            , PeerNodeId(peerNodeId)
        {}

        // ---- producer side (arbitrary threads), lock-free ----

        bool Send(TAutoPtr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> replyCallback = nullptr) override {
            if (!Connected.load(std::memory_order_acquire)) {
                return false;
            }
            if (replyCallback) {
                EnqueueCommand(ev->Sender, std::move(replyCallback));
            }
            Y_ABORT_UNLESS(ev->GetRecipientRewrite().NodeId() == PeerNodeId);
            ActorSystem->Send(ev);
            return true;
        }

        void RegisterReceiveCallback(const TActorId& localActorId, TIntrusivePtr<IReceiveCallback> callback) override {
            if (Connected.load(std::memory_order_acquire)) {
                EnqueueCommand(localActorId, std::move(callback));
            }
        }

        void UnregisterReceiveCallback(const TActorId& localActorId) override {
            if (Connected.load(std::memory_order_acquire)) {
                EnqueueCommand(localActorId, nullptr);
            }
        }

        // ---- consumer side (session/input-session mailbox thread only) ----

        // Applies every queued registration command into the table. Called on the
        // EvProcessDirectSessionQueue wake and before dispatching incoming events.
        void ApplyPendingCommands(bool drop = false) {
            while (!CommandQueue.IsEmpty()) {
                TCommand& cmd = CommandQueue.Top();
                if (!drop) {
                    if (cmd.Callback) {
                        Callbacks[cmd.ActorId] = std::move(cmd.Callback);
                    } else {
                        Callbacks.erase(cmd.ActorId);
                    }
                }
                CommandQueue.Pop();
            }
        }

        // Delivers an incoming event to the callback registered for its Recipient, if any. Returns true
        // if a callback consumed the event (ev is moved out), false otherwise (ev is left untouched and
        // the caller should deliver the event through the actor system).
        bool DeliverIncoming(TAutoPtr<IEventHandle>& ev) {
            ApplyPendingCommands();
            if (Callbacks.empty()) {
                return false;
            }
            // just received -> Recipient and RecipientRewrite are guaranteed to match
            Y_DEBUG_ABORT_UNLESS(ev->Recipient == ev->GetRecipientRewrite());
            if (const auto it = Callbacks.find(ev->Recipient); it != Callbacks.end()) {
                it->second->Receive(std::move(ev));
                return true;
            }
            return false;
        }

        // Transitions the handle into the disconnected state. Called by the session on teardown, on the
        // same mailbox thread that owns the table, so it can safely drop the callbacks.
        void Shutdown() {
            Connected.store(false, std::memory_order_release);
            ApplyPendingCommands(true); // drain the queue to release its entries
            Callbacks.clear();
        }

    private:
        struct TCommand {
            TActorId ActorId;
            TIntrusivePtr<IReceiveCallback> Callback; // null => unregister
        };

        void EnqueueCommand(const TActorId& actorId, TIntrusivePtr<IReceiveCallback> callback) {
            if (CommandQueue.Push(TCommand{actorId, std::move(callback)})) {
                // queue went empty -> non-empty: wake the stable session actor to drain the queue
                ActorSystem->Send(new IEventHandle(static_cast<ui32>(ENetwork::EvProcessDirectSessionQueue),
                    0, SessionId, TActorId(), nullptr, 0));
            }
        }

    private:
        TActorSystem* const ActorSystem;
        const TActorId SessionId;
        const ui32 PeerNodeId;
        std::atomic<bool> Connected = true;
        TFunnelQueue<TCommand> CommandQueue; // MPSC: many producers, single consumer (mailbox thread)
        // touched only on the session/input-session mailbox thread
        THashMap<TActorId, TIntrusivePtr<IReceiveCallback>, TActorId::THash> Callbacks;
    };

    // Direct session backing a newer (v2) TInterconnectSessionTCPv2 connection. The outgoing data plane
    // (serialization / socket handoff) is intentionally left as a stub for now, so this implementation
    // keeps a simple mutex-guarded registration table. An atomic counter lets DeliverIncoming skip the
    // mutex entirely when no receivers are registered.
    class TDirectSessionV2 final : public IDirectSession {
    public:
        bool Send(TAutoPtr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> replyCallback = nullptr) override {
            {
                TGuard<TMutex> guard(Mutex);
                if (!Connected) {
                    return false;
                }
                if (replyCallback) {
                    RegisterLocked(ev->Sender, std::move(replyCallback));
                }
            }
            // TODO(v2 data plane): hand the event off to the session's outgoing stream.
            Y_UNUSED(ev);
            return true;
        }

        void RegisterReceiveCallback(const TActorId& localActorId, TIntrusivePtr<IReceiveCallback> callback) override {
            TGuard<TMutex> guard(Mutex);
            if (!Connected) {
                return;
            }
            RegisterLocked(localActorId, std::move(callback));
        }

        void UnregisterReceiveCallback(const TActorId& localActorId) override {
            TGuard<TMutex> guard(Mutex);
            if (Callbacks.erase(localActorId)) {
                CallbackCount.store(Callbacks.size(), std::memory_order_release);
            }
        }

        bool DeliverIncoming(TAutoPtr<IEventHandle>& ev) {
            if (CallbackCount.load(std::memory_order_acquire) == 0) {
                return false;
            }
            TIntrusivePtr<IReceiveCallback> callback;
            {
                TGuard<TMutex> guard(Mutex);
                if (!Connected) {
                    return false;
                }
                // just received -> Recipient and RecipientRewrite are guaranteed to match
                const auto it = Callbacks.find(ev->Recipient);
                if (it == Callbacks.end()) {
                    return false;
                }
                callback = it->second;
            }
            callback->Receive(std::move(ev));
            return true;
        }

        void Shutdown() {
            TGuard<TMutex> guard(Mutex);
            Connected = false;
            Callbacks.clear();
            CallbackCount.store(0, std::memory_order_release);
        }

    private:
        // Mutex must be held and Connected must be true.
        void RegisterLocked(const TActorId& localActorId, TIntrusivePtr<IReceiveCallback> callback) {
            Callbacks[localActorId] = std::move(callback);
            CallbackCount.store(Callbacks.size(), std::memory_order_release);
        }

    private:
        mutable TMutex Mutex;
        bool Connected = true;
        std::atomic<size_t> CallbackCount = 0;
        THashMap<TActorId, TIntrusivePtr<IReceiveCallback>, TActorId::THash> Callbacks;
    };

}
