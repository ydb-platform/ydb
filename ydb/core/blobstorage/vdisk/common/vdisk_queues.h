#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_iter.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_common.h>


namespace NKikimr {

    inline TVDiskIdShort GetVDiskID(const TBlobStorageGroupInfo::TVDiskInfo &vdisk) {
        return vdisk.VDiskIdShort;
    }

    inline TVDiskID GetVDiskID(const std::pair<TVDiskID, TActorId> &p) {
        return p.first;
    }

    inline TActorId GetVDiskActorId(const TBlobStorageGroupInfo::TVDiskInfo &vdisk,
            const TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> &gInfo)
    {
        return gInfo->GetActorId(vdisk.VDiskIdShort);
    }

    inline TActorId GetVDiskActorId(const std::pair<TVDiskID, TActorId> &p,
            const TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> &)
    {
        return p.second;
    }

    struct TBasicQueueActorIdWrapper {
        TActorId Wrap(TActorId &&id) const {
            return id;
        }
    };

    template <typename TContainer, typename TWrappedQueueActorId>
    void EmplaceToContainer(TContainer &cont, const TVDiskIdShort &vdisk, TWrappedQueueActorId &&actorId) {
        cont.emplace(vdisk, std::move(actorId));
    }

    inline void EmplaceToContainer(std::deque<std::pair<TVDiskID, TActorId>> &cont, const TVDiskID &vdisk, TActorId &&actorId) {
        cont.emplace_back(vdisk, std::move(actorId));
    }

    template <typename TContainer, typename TVDiskContainer, typename TWrapper = TBasicQueueActorIdWrapper>
    void CreateQueuesForVDisks(TContainer &cont, const TActorId &parent,
            const TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> &gInfo,
            const TIntrusivePtr<TVDiskContext> &vCtx, const TVDiskContainer &disks,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters> &groupCounters,
            const NBackpressure::TQueueClientId &queueClientId, NKikimrBlobStorage::EVDiskQueueId vDiskQueueId,
            const TString &queueName, TInterconnectChannels::EInterconnectChannels interconnectChannel,
            const bool useActorSystemTimeInBSQueue, TWrapper wrapper = {})
    {
        for (auto &vdiskInfo : disks) {
            auto vdisk = GetVDiskID(vdiskInfo);
            if (vdisk != vCtx->ShortSelfVDisk) {
                TIntrusivePtr<NBackpressure::TFlowRecord> flowRecord = MakeIntrusive<NBackpressure::TFlowRecord>();
                TActorId vdiskActorId = GetVDiskActorId(vdiskInfo, gInfo);
                std::unique_ptr<IActor> queue;
                queue.reset(CreateVDiskBackpressureClient(gInfo, vdisk,
                        vDiskQueueId, groupCounters, vCtx, queueClientId, queueName,
                        interconnectChannel, vdiskActorId.NodeId() == parent.NodeId(),
                        TDuration::Minutes(1), flowRecord, NMonitoring::TCountableBase::EVisibility::Private,
                        useActorSystemTimeInBSQueue));
                TActorId serviceId = TActivationContext::Register(queue.release(), parent);
                EmplaceToContainer(cont, vdisk, wrapper.Wrap(std::move(serviceId)));
            }
        }
    }

} // NKikimr
