#include "direct_session_registry.h"

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TSessionEntry MakeSessionEntry(
    TActorSystem* actorSystem,
    std::shared_ptr<IDirectSession> session)
{
    Y_ABORT_UNLESS(actorSystem);
    Y_ABORT_UNLESS(session);

    // Same shape as TInterconnectProxyTCP::GenerateSessionVirtualId: localId
    // comes from the shared actor-system ID space, so it cannot collide with a
    // real actor and multiple tablets on one node need no coordination.
    //
    // DDisk ValidateConnection (ddisk_actor_connect.cpp) requires, for
    // non-internal ToDDisk credentials:
    //   1. connection.NodeId == ev.Sender.NodeId()
    //   2. connection.InterconnectSessionId == ev.InterconnectSession
    // Connect stores NodeId from the Connect event's Sender and
    // InterconnectSession from the inbound handle. Datapath events must match:
    //   (1) Sender must carry the local node ID (same node as the transport
    //       actor that issued Connect).
    //   (2) Real ICv1/ICv2 inbound delivery stamps InterconnectSession with the
    //       session actor id. That is the same id Connect saw, so the check
    //       passes for direct sends. Local/fake paths leave both empty.
    const TActorId replyActorId(
        actorSystem->NodeId,
        /*poolId=*/0,
        actorSystem->AllocateIDSpace(1),
        /*hint=*/0);

    auto router = MakeIntrusive<TSessionReplyRouter>();
    session->RegisterReceiveCallback(replyActorId, router);

    return TSessionEntry{
        .Session = std::move(session),
        .ReplyActorId = replyActorId,
        .Router = std::move(router),
    };
}

////////////////////////////////////////////////////////////////////////////////

TSessionEntry TDirectSessionRegistry::Get(ui32 nodeId) const
{
    auto snapshot = Snapshot.AtomicLoad();
    if (const auto* entry = snapshot->Entries.FindPtr(nodeId)) {
        return *entry;
    }
    return {};
}

void TDirectSessionRegistry::Set(ui32 nodeId, TSessionEntry entry)
{
    if (!entry) {
        Reset(nodeId);
        return;
    }

    with_lock (WriterMutex) {
        auto next = MakeIntrusive<TSnapshot>(*Snapshot.AtomicLoad());
        if (auto* previous = next->Entries.FindPtr(nodeId);
            previous && previous->Session)
        {
            // Drop the previous long-lived registration before replacing.
            previous->Session->UnregisterReceiveCallback(
                previous->ReplyActorId);
        }
        next->Entries[nodeId] = std::move(entry);
        Snapshot.AtomicStore(next);
    }
}

void TDirectSessionRegistry::Reset(ui32 nodeId)
{
    with_lock (WriterMutex) {
        auto current = Snapshot.AtomicLoad();
        if (!current->Entries.FindPtr(nodeId)) {
            return;
        }
        auto next = MakeIntrusive<TSnapshot>(*current);
        if (auto* previous = next->Entries.FindPtr(nodeId);
            previous && previous->Session)
        {
            previous->Session->UnregisterReceiveCallback(
                previous->ReplyActorId);
        }
        next->Entries.erase(nodeId);
        Snapshot.AtomicStore(next);
    }
}

void TDirectSessionRegistry::Clear()
{
    with_lock (WriterMutex) {
        auto current = Snapshot.AtomicLoad();
        for (const auto& [nodeId, entry]: current->Entries) {
            Y_UNUSED(nodeId);
            if (entry.Session) {
                entry.Session->UnregisterReceiveCallback(entry.ReplyActorId);
            }
        }
        Snapshot.AtomicStore(MakeIntrusive<TSnapshot>());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
