#include "node_warden_impl.h"

#include <ydb/core/blobstorage/crypto/default.h>

#include <ydb/library/pdisk_io/file_params.h>
#include <ydb/library/pdisk_io/wcache.h>

#include <util/string/split.h>

using namespace NKikimr;
using namespace NStorage;

void TNodeWarden::RegisterPendingActor(const TActorId& actorId) {
    const bool inserted = PendingMessageQ.emplace(actorId, std::deque<std::unique_ptr<IEventHandle>>()).second;
    Y_VERIFY(inserted);
}

void TNodeWarden::EnqueuePendingMessage(TAutoPtr<IEventHandle> ev) {
    ev = IEventHandle::Forward(ev, ev->GetForwardOnNondeliveryRecipient());
    const auto it = PendingMessageQ.find(ev->Recipient);
    if (it != PendingMessageQ.end()) {
        it->second.emplace_back(ev.Release());
    } else {
        TActivationContext::Send(ev);
    }
}

void TNodeWarden::IssuePendingMessages(const TActorId& actorId) {
    const auto it = PendingMessageQ.find(actorId);
    Y_VERIFY(it != PendingMessageQ.end());
    for (auto& ev : it->second) {
        TActivationContext::Send(ev.release());
    }
    PendingMessageQ.erase(it);
}

void TNodeWarden::ApplyServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet &serviceSet, bool isStatic, bool comprehensive, bool updateCache) {
    if (Cfg->IsCacheEnabled() && updateCache) {
        Y_VERIFY(!isStatic);
        return EnqueueSyncOp(WrapCacheOp(UpdateServiceSet(serviceSet, comprehensive, [=] {
            return ApplyServiceSet(serviceSet, false, comprehensive, false);
        })));
    }

    STLOG(PRI_DEBUG, BS_NODE, NW18, "ApplyServiceSet", (IsStatic, isStatic), (Comprehensive, comprehensive));

    // apply proxy information before we try to start VDisks/PDisks
    ApplyGroupInfoFromServiceSet(serviceSet);

    if (!EnableProxyMock) {
        // in mock mode we don't need PDisk/VDisk instances
        ApplyServiceSetPDisks(serviceSet);
        ApplyServiceSetVDisks(serviceSet);
    }

    // for comprehensive configuration -- stop created, but missing entities
    if (comprehensive) {
        std::set<TPDiskKey> pdiskQ;
        for (const auto& [pdiskId, _] : LocalPDisks) { // insert all running PDisk ids in the set
            pdiskQ.insert(pdiskId);
        }
        for (const auto& item : serviceSet.GetPDisks()) { // remove enumerated ids
            if (item.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::INITIAL ||
                    item.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::CREATE) {
                pdiskQ.erase({item.GetNodeID(), item.GetPDiskID()});
            }
        }
        for (const auto& [vslotId, _] : LocalVDisks) { // remove ids for PDisks with active VSlots
            pdiskQ.erase({vslotId.NodeId, vslotId.PDiskId});
        }
        for (const TPDiskKey& pdiskId : pdiskQ) { // terminate excessive PDisks
            Y_VERIFY(pdiskId.NodeId == LocalNodeId);
            DestroyLocalPDisk(pdiskId.PDiskId);
        }
    }

    for (auto& [vslotId, vdisk] : LocalVDisks) {
        if (vdisk.UnderlyingPDiskDestroyed) {
            auto& tempVSlotId = vslotId;
            STLOG_DEBUG_FAIL(BS_NODE, NW37, "UnderlyingPDiskDestroyed escaped", (VSlotId, tempVSlotId));
            vdisk.UnderlyingPDiskDestroyed = false;
        }
    }
}

void TNodeWarden::HandleIncrHugeInit(NIncrHuge::TEvIncrHugeInit::TPtr ev) {
    const TActorId keeperId = ev->GetForwardOnNondeliveryRecipient();
    const ui32 pdiskId = NIncrHuge::PDiskIdFromIncrHugeKeeperId(keeperId);

    // find local pdisk config to extract GUID
    auto it = LocalPDisks.find(TPDiskKey(LocalNodeId, pdiskId));
    Y_VERIFY(it != LocalPDisks.end());

    // get config
    const NKikimrBlobStorage::TIncrHugeConfig& config = Cfg->IncrHugeConfig;

    // fill in settings record
    NIncrHuge::TKeeperSettings settings{
        it->first.PDiskId,
        MakeBlobStoragePDiskID(it->first.NodeId, it->first.PDiskId),
        it->second.Record.GetPDiskGuid(),
        config.GetMinHugeBlobInBytes(),
        config.GetMinCleanChunks(),
        config.GetMinAllocationBatch(),
        config.GetUnalignedBlockSize(),
        config.GetMaxInFlightWrites(),
        NextLocalPDiskInitOwnerRound()
    };

    // register new actor
    TActorId actorId = Register(CreateIncrHugeKeeper(settings), TMailboxType::HTSwap, AppData()->SystemPoolId);

    // bind it to service
    TlsActivationContext->ExecutorThread.ActorSystem->RegisterLocalService(keeperId, actorId);

    // forward to just created service
    TActivationContext::Send(ev->Forward(keeperId));
}
