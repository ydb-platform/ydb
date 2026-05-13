#include "blob_recovery_impl.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_queues.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_VDISK_SCRUB

namespace NKikimr {

    void TBlobRecoveryActor::StartQueues() {
        struct TQueueActorIdWrapper {
            TQueueInfo Wrap(TActorId &&id) const {
                return {std::move(id)};
            }
        };
        const NBackpressure::TQueueClientId clientId(NBackpressure::EQueueClientType::ReplJob,
            Info->GetTotalVDisksNum() + Info->GetOrderNumber(VCtx->ShortSelfVDisk)); // distinct queue client id
        CreateQueuesForVDisks(Queues, SelfId(), Info, VCtx, Info->GetVDisks(), Counters,
            clientId, NKikimrBlobStorage::EVDiskQueueId::GetLowRead, "PeerScrub",
            TInterconnectChannels::IC_BLOBSTORAGE_ASYNC_DATA, false, TQueueActorIdWrapper());
    }

    void TBlobRecoveryActor::StopQueues() {
        for (const auto& [vdiskId, queue] : Queues) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, queue.QueueActorId, {}, nullptr, 0));
        }
    }

    void TBlobRecoveryActor::Handle(TEvVGenerationChange::TPtr ev) {
        YDB_LOG_INFO(VDISKP(LogPrefix, "received group generation change notification"),
            {"Marker", "VDS28"},
            {"SelfId", SelfId()},
            {"Msg", ev->Get()->ToString()});
        for (const auto& [vdiskId, queue] : Queues) {
            Send(queue.QueueActorId, ev->Get()->Clone());
        }
        Info = ev->Get()->NewInfo;
    }

    void TBlobRecoveryActor::Handle(TEvProxyQueueState::TPtr ev) {
        const auto it = Queues.find(ev->Get()->VDiskId);
        Y_VERIFY_S(it != Queues.end(), LogPrefix);
        it->second.IsConnected = ev->Get()->IsConnected;
        YDB_LOG_INFO(VDISKP(LogPrefix, "BS_QUEUE state update"),
            {"Marker", "VDS29"},
            {"SelfId", SelfId()},
            {"VDiskId", it->first},
            {"IsConnected", it->second.IsConnected});
        EvaluateConnectionQuorum();
    }

    void TBlobRecoveryActor::EvaluateConnectionQuorum() {
        TBlobStorageGroupInfo::TGroupVDisks connected(&Info->GetTopology());
        connected |= {&Info->GetTopology(), VCtx->ShortSelfVDisk}; // assume this disk as working one; doesn't work for replication
        for (const auto& [vdiskId, queue] : Queues) {
            if (queue.IsConnected) {
                connected |= {&Info->GetTopology(), vdiskId};
            }
        }
        IsConnected = Info->GetQuorumChecker().CheckFailModelForGroup(~connected);
        if (IsConnected) {
            SendPendingQueries();
        }
    }

} // NKikimr
