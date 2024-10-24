#pragma once

#include "ydb/library/yverify_stream/yverify_stream.h"
#include "ydb_proxy.h"

namespace NKikimr::NReplication {

using namespace NYdb::NTopic;

class TPartitionEndWatcher {
    void MaybeSendPartitionEnd(const TActorId& client) {
        if (!EndPartitionSessionEvent || CommittedOffset != PendingCommittedOffset) {
            return;
        }

        ActorOps->Send(client, new TEvYdbProxy::TEvTopicEndPartition(*EndPartitionSessionEvent));
    }

public:
    explicit TPartitionEndWatcher(IActorOps* actorOps)
        : ActorOps(actorOps)
    {
    }

    void SetEvent(TReadSessionEvent::TEndPartitionSessionEvent&& event, const TActorId& client) {
        EndPartitionSessionEvent = std::move(event);
        MaybeSendPartitionEnd(client);
    }

    void UpdatePendingCommittedOffset(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
        if (auto count = event.GetMessagesCount()) {
            if (event.HasCompressedMessages()) {
                PendingCommittedOffset = event.GetCompressedMessages()[count - 1].GetOffset();
            } else {
                PendingCommittedOffset = event.GetMessages()[count - 1].GetOffset();
            }
        }
    }

    void SetCommittedOffset(ui64 offset, const TActorId& client) {
        CommittedOffset = offset - 1;
        MaybeSendPartitionEnd(client);
    }

    void Clear() {
        PendingCommittedOffset = 0;
        CommittedOffset = 0;
        EndPartitionSessionEvent.Clear();
    }

private:
    IActorOps* const ActorOps;
    ui64 PendingCommittedOffset = 0;
    ui64 CommittedOffset = 0;
    TMaybe<TReadSessionEvent::TEndPartitionSessionEvent> EndPartitionSessionEvent;
}; // TPartitionEndWatcher

}
