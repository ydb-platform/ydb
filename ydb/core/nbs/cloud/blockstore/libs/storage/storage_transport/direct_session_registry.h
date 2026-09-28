#pragma once

#include "session_reply_router.h"

#include <ydb/library/actors/interconnect/interconnect_direct_session.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/system/spinlock.h>

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
// Hot-path Get() is wait-free via THotSwap. Writers (transport actor on
// TEvNodeConnected / TEvNodeDisconnected) copy-on-write under WriterMutex and
// publish a new snapshot. WriterMutex is needed because THotSwap serializes
// AtomicStore but not the read-modify-write of the map contents.
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
    struct TSnapshot: public TAtomicRefCount<TSnapshot>
    {
        THashMap<ui32, TSessionEntry> Entries;

        TSnapshot() = default;

        TSnapshot(const TSnapshot& other)
            : Entries(other.Entries)
        {}
    };

    THotSwap<TSnapshot> Snapshot{MakeIntrusive<TSnapshot>()};
    TAdaptiveLock WriterMutex;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
