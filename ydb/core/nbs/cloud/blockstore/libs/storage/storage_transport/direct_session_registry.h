#pragma once

#include "session_reply_router.h"

#include <ydb/library/actors/interconnect/interconnect_direct_session.h>

#include <util/generic/hash.h>
#include <util/system/mutex.h>

#include <memory>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

// Immutable per-node entry published into TDirectSessionRegistry. Session is
// the IDirectSession handle; ReplyActorId is the long-lived virtual sender
// registered with Router for cookie-based reply demultiplexing.
struct TSessionEntry
{
    std::shared_ptr<NActors::IDirectSession> Session;
    NActors::TActorId ReplyActorId;
    TIntrusivePtr<TSessionReplyRouter> Router;

    [[nodiscard]] explicit operator bool() const
    {
        return Session != nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

// Allocates a reply ActorId from the actor-system ID space, creates a router,
// registers it on the session, and returns the complete entry. Call before
// publishing into TDirectSessionRegistry.
[[nodiscard]] TSessionEntry MakeSessionEntry(
    NActors::TActorSystem* actorSystem,
    std::shared_ptr<NActors::IDirectSession> session);

////////////////////////////////////////////////////////////////////////////////

// Thread-safe nodeId -> TSessionEntry map used by TICDirectStorageTransport.
// Hot-path Get() is lock-free (atomic shared_ptr load of an immutable
// snapshot). Writers (transport actor on TEvNodeConnected /
// TEvNodeDisconnected) copy-on-write under a mutex and publish a new snapshot.
class TDirectSessionRegistry
{
public:
    TDirectSessionRegistry() = default;

    [[nodiscard]] TSessionEntry Get(ui32 nodeId) const;

    // Actor-thread publishers. Empty entry (or !entry.Session) resets.
    void Set(ui32 nodeId, TSessionEntry entry);
    void Reset(ui32 nodeId);
    void Clear();

private:
    using TSnapshot = THashMap<ui32, TSessionEntry>;

    // Published via std::atomic_load / std::atomic_store (free-function
    // shared_ptr atomics). Arcadia's libc++ does not yet provide
    // std::atomic<std::shared_ptr<T>>. Writers hold Mutex; readers never take
    // it.
    mutable std::shared_ptr<const TSnapshot> Snapshot =
        std::make_shared<const TSnapshot>();
    TMutex Mutex;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
