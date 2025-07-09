#pragma once

#include "ydb_proxy.h"

#include <util/generic/maybe.h>

namespace NKikimr::NReplication {

using namespace NYdb::NTopic;

class TPartitionEndWatcher {
    inline void MaybeSendPartitionEnd(const TActorId& client) {
        if (!EndPartitionSessionEvent || CommittedOffset != PendingCommittedOffset) {
            return;
        }

        ActorOps->Send(client, new TEvYdbProxy::TEvEndTopicPartition(*EndPartitionSessionEvent));
    }

public:
    inline explicit TPartitionEndWatcher(IActorOps* actorOps)
        : ActorOps(actorOps)
    {
    }

    inline void SetEvent(TReadSessionEvent::TEndPartitionSessionEvent&& event, const TActorId& client) {
        EndPartitionSessionEvent = std::move(event);
        MaybeSendPartitionEnd(client);
    }

    inline void UpdatePendingCommittedOffset(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
        if (event.GetMessagesCount()) {
            if (event.HasCompressedMessages()) {
                PendingCommittedOffset = event.GetCompressedMessages().back().GetOffset();
            } else {
                PendingCommittedOffset = event.GetMessages().back().GetOffset();
            }
        }
    }

    inline void SetCommittedOffset(ui64 offset, const TActorId& client) {
        CommittedOffset = offset;
        MaybeSendPartitionEnd(client);
    }

    inline void Clear(ui64 committedOffset) {
        PendingCommittedOffset = committedOffset;
        CommittedOffset = committedOffset;
        EndPartitionSessionEvent.Clear();
    }

private:
    IActorOps* const ActorOps;
    ui64 PendingCommittedOffset = 0;
    ui64 CommittedOffset = 0;
    TMaybe<TReadSessionEvent::TEndPartitionSessionEvent> EndPartitionSessionEvent;

}; // TPartitionEndWatcher

}
