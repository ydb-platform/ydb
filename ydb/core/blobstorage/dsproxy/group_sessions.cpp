#include "group_sessions.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>

namespace NKikimr {

static NKikimrBlobStorage::EVDiskQueueId VDiskQueues[] = {
    NKikimrBlobStorage::EVDiskQueueId::PutTabletLog,
    NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob,
    NKikimrBlobStorage::EVDiskQueueId::PutUserData,
    NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead,
    NKikimrBlobStorage::EVDiskQueueId::GetFastRead,
    NKikimrBlobStorage::EVDiskQueueId::GetDiscover,
    NKikimrBlobStorage::EVDiskQueueId::GetLowRead,
};

TString QueueIdName(NKikimrBlobStorage::EVDiskQueueId queueId) {
    switch (queueId) {
        case NKikimrBlobStorage::EVDiskQueueId::PutTabletLog: return "PutTabletLog";
        case NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob: return "PutAsyncBlob";
        case NKikimrBlobStorage::EVDiskQueueId::PutUserData:  return "PutUserData";
        case NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead: return "GetAsyncRead";
        case NKikimrBlobStorage::EVDiskQueueId::GetFastRead:  return "GetFastRead";
        case NKikimrBlobStorage::EVDiskQueueId::GetDiscover:  return "GetDiscover";
        case NKikimrBlobStorage::EVDiskQueueId::GetLowRead:   return "GetLowRead";
        default:                                              Y_ABORT("unexpected EVDiskQueueId");
    }
}

TGroupSessions::TGroupSessions(const TIntrusivePtr<TBlobStorageGroupInfo>& info, const TBSProxyContextPtr& bspctx,
        const TActorId& monActor, const TActorId& proxyActor, bool useActorSystemTimeInBSQueue)
    : GroupQueues(MakeIntrusive<TGroupQueues>(info->GetTopology()))
    , ConnectedQueuesMask(info->GetTotalVDisksNum(), 0)
    , MonActor(monActor)
    , ProxyActor(proxyActor)
{
    const ui32 nodeId = TlsActivationContext->ExecutorThread.ActorSystem->NodeId;

    for (const auto& vdisk : info->GetVDisks()) {
        auto vd = info->GetVDiskId(vdisk.OrderNumber);
        auto& stateVDisk = GroupQueues->FailDomains[vdisk.FailDomainOrderNumber].VDisks[vd.VDisk];
        const ui32 targetNodeId = info->GetActorId(vdisk.OrderNumber).NodeId();

        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = GetServiceCounters(AppData()->Counters, "dsproxy_queue");

        for (NKikimrBlobStorage::EVDiskQueueId queueId : VDiskQueues) {
            ui32 interconnectChannel = 0;
            switch (queueId) {
                case NKikimrBlobStorage::PutTabletLog:
                case NKikimrBlobStorage::PutUserData:
                case NKikimrBlobStorage::GetFastRead:
                    interconnectChannel = TInterconnectChannels::IC_BLOBSTORAGE;
                    break;

                case NKikimrBlobStorage::GetDiscover:
                    interconnectChannel = TInterconnectChannels::IC_BLOBSTORAGE_DISCOVER;
                    break;

                case NKikimrBlobStorage::PutAsyncBlob:
                case NKikimrBlobStorage::GetAsyncRead:
                case NKikimrBlobStorage::GetLowRead:
                    interconnectChannel = TInterconnectChannels::IC_BLOBSTORAGE_ASYNC_DATA;
                    break;

                default:
                    Y_ABORT("unexpected queue id");
            }

            TIntrusivePtr<NBackpressure::TFlowRecord> flowRecord(new NBackpressure::TFlowRecord);

            std::unique_ptr<NActors::IActor> queueActor(CreateVDiskBackpressureClient(info, vd, queueId, counters, bspctx,
                NBackpressure::TQueueClientId(NBackpressure::EQueueClientType::DSProxy, nodeId), QueueIdName(queueId),
                interconnectChannel, nodeId == targetNodeId, TDuration::Minutes(1), flowRecord,
                NMonitoring::TCountableBase::EVisibility::Public, useActorSystemTimeInBSQueue));

            TActorId queue = TActivationContext::Register(queueActor.release(), ProxyActor, TMailboxType::ReadAsFilled,
                AppData()->SystemPoolId);

            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << info->GroupID
                << " Actor# " << ProxyActor
                << " Create Queue# " << queue.ToString()
                << " targetNodeId# " << targetNodeId
                << " Marker# DSP01");

            auto& q = stateVDisk.Queues.GetQueue(queueId);
            q.ActorId = queue;
            q.FlowRecord = std::move(flowRecord);
            q.ExtraBlockChecksSupport.reset();
        }
    }
}

void TGroupSessions::Poison() {
    for (const auto& domain : GroupQueues->FailDomains) {
        for (const auto& vdisk : domain.VDisks) {
            vdisk.Queues.ForEachQueue([](const auto& queue) {
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, queue.ActorId, {}, nullptr, 0));
            });
        }
    }
}

bool TGroupSessions::GoodToGo(const TBlobStorageGroupInfo::TTopology& topology, bool waitForAllVDisks) {
    // create a set of connected disks
    TBlobStorageGroupInfo::TGroupVDisks connected(&topology);
    for (ui32 i = 0; i < ConnectedQueuesMask.size(); ++i) {
        if (ConnectedQueuesMask[i] == AllQueuesMask) {
            connected += TBlobStorageGroupInfo::TGroupVDisks::CreateFromMask(&topology, ui64(1) << i);
        }
    }

    // check if we have quorum; in force mode we wait for all disks
    return waitForAllVDisks
        ? connected.GetNumSetItems() == topology.GetTotalVDisksNum()
        : topology.GetQuorumChecker().CheckQuorumForGroup(connected);
}

void TGroupSessions::QueueConnectUpdate(ui32 orderNumber, NKikimrBlobStorage::EVDiskQueueId queueId, bool connected,
        bool extraGroupChecksSupport, std::shared_ptr<const TCostModel> costModel, const TBlobStorageGroupInfo::TTopology& topology) {
    const auto v = topology.GetVDiskId(orderNumber);
    const ui32 fdom = topology.GetFailDomainOrderNumber(v);
    auto& f = GroupQueues->FailDomains[fdom];
    auto& vdisk = f.VDisks[v.VDisk];
    auto& q = vdisk.Queues.GetQueue(queueId);

    bool updated = false;

    if (connected) {
        ConnectedQueuesMask[orderNumber] |= 1 << queueId;
        q.ExtraBlockChecksSupport = extraGroupChecksSupport;
        Y_ABORT_UNLESS(costModel);
        if (!q.CostModel || *q.CostModel != *costModel) {
            updated = true;
            q.CostModel = costModel;
        }
    } else {
        ConnectedQueuesMask[orderNumber] &= ~(1 << queueId);
        q.ExtraBlockChecksSupport.reset();
        if (q.CostModel) {
            updated = true;
            q.CostModel = nullptr;
        }
    }
    q.IsConnected = connected;

    if (updated) {
        auto iterate = [](auto& currentCostModel, const auto& next) {
            if (next.CostModel) {
                if (!currentCostModel) {
                    currentCostModel.emplace(*next.CostModel);
                } else {
                    currentCostModel->PessimisticComposition(*next.CostModel);
                }
            }
        };

        auto update = [](std::shared_ptr<const TCostModel>& current, const std::optional<TCostModel>& recalculated) {
            if (!recalculated) {
                current.reset();
            } else {
                if (!current || *current != *recalculated) {
                    current = std::make_shared<const TCostModel>(*recalculated);
                }
            }
        };

        // recalculate CostModel for the whole VDisk
        std::optional<TCostModel> pessimistic;
        vdisk.CostModel.reset();
        vdisk.Queues.ForEachQueue([&](auto& q) { iterate(pessimistic, q); });
        update(vdisk.CostModel, pessimistic);

        // do the same for the fail domain
        f.CostModel.reset();
        pessimistic.reset();
        for (const auto& vdisk : f.VDisks) {
            iterate(pessimistic, vdisk);
        }
        update(f.CostModel, pessimistic);

        // and for the whole group
        GroupQueues->CostModel.reset();
        pessimistic.reset();
        for (const auto& fdom : GroupQueues->FailDomains) {
            iterate(pessimistic, fdom);
        }
        update(GroupQueues->CostModel, pessimistic);
    }
}

ui32 TGroupSessions::GetMinREALHugeBlobInBytes() const {
    return GroupQueues->CostModel ? GroupQueues->CostModel->MinREALHugeBlobInBytes : 0;
}

ui32 TGroupSessions::GetNumUnconnectedDisks() {
    ui32 n = 0;
    for (const ui8 mask : ConnectedQueuesMask) {
        n += mask != AllQueuesMask;
    }
    return n;
}

} // NKikimr
