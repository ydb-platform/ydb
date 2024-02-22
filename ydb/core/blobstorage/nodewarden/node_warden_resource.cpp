#include "node_warden_impl.h"

#include <ydb/core/blobstorage/crypto/default.h>

#include <ydb/library/pdisk_io/file_params.h>
#include <ydb/library/pdisk_io/wcache.h>

#include <util/string/split.h>

using namespace NKikimr;
using namespace NStorage;

void TNodeWarden::RegisterPendingActor(const TActorId& actorId) {
    const bool inserted = PendingMessageQ.emplace(actorId, std::deque<std::unique_ptr<IEventHandle>>()).second;
    Y_ABORT_UNLESS(inserted);
}

void TNodeWarden::EnqueuePendingMessage(TAutoPtr<IEventHandle> ev) {
    const TActorId recipient = ev->GetForwardOnNondeliveryRecipient();
    ev = IEventHandle::Forward(std::move(ev), recipient);
    const auto it = PendingMessageQ.find(ev->Recipient);
    if (it != PendingMessageQ.end()) {
        it->second.emplace_back(ev.Release());
    } else {
        TActivationContext::Send(ev);
    }
}

void TNodeWarden::IssuePendingMessages(const TActorId& actorId) {
    const auto it = PendingMessageQ.find(actorId);
    Y_ABORT_UNLESS(it != PendingMessageQ.end());
    for (auto& ev : it->second) {
        TActivationContext::Send(ev.release());
    }
    PendingMessageQ.erase(it);
}

void TNodeWarden::ApplyServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet &serviceSet, bool isStatic, bool comprehensive, bool updateCache) {
    if (Cfg->IsCacheEnabled() && updateCache) {
        Y_ABORT_UNLESS(!isStatic);
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
            Y_ABORT_UNLESS(pdiskId.NodeId == LocalNodeId);
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

void TNodeWarden::Handle(TEvNodeWardenQueryStorageConfig::TPtr ev) {
    Send(ev->Sender, new TEvNodeWardenStorageConfig(StorageConfig, nullptr));
    if (ev->Get()->Subscribe) {
        StorageConfigSubscribers.insert(ev->Sender);
    }
}

void TNodeWarden::Handle(TEvNodeWardenStorageConfig::TPtr ev) {
    ev->Get()->Config->Swap(&StorageConfig);
    if (StorageConfig.HasBlobStorageConfig()) {
        if (const auto& bsConfig = StorageConfig.GetBlobStorageConfig(); bsConfig.HasServiceSet()) {
            const NKikimrBlobStorage::TNodeWardenServiceSet *proposed = nullptr;
            if (const auto& proposedConfig = ev->Get()->ProposedConfig) {
                Y_VERIFY_S(StorageConfig.GetGeneration() < proposedConfig->GetGeneration(),
                    "StorageConfig.Generation# " << StorageConfig.GetGeneration()
                    << " ProposedConfig.Generation# " << proposedConfig->GetGeneration());
                Y_ABORT_UNLESS(proposedConfig->HasBlobStorageConfig()); // must have the BlobStorageConfig and the ServiceSet
                const auto& proposedBsConfig = proposedConfig->GetBlobStorageConfig();
                Y_ABORT_UNLESS(proposedBsConfig.HasServiceSet());
                proposed = &proposedBsConfig.GetServiceSet();
            }
            ApplyStorageConfig(bsConfig.GetServiceSet(), proposed);
        }
    }
    for (const TActorId& subscriber : StorageConfigSubscribers) {
        Send(subscriber, new TEvNodeWardenStorageConfig(StorageConfig, nullptr));
    }
    TActivationContext::Send(new IEventHandle(TEvBlobStorage::EvNodeWardenStorageConfigConfirm, 0, ev->Sender, SelfId(),
        nullptr, 0));
}

void TNodeWarden::HandleUnsubscribe(STATEFN_SIG) {
    StorageConfigSubscribers.erase(ev->Sender);
}

void TNodeWarden::ApplyStorageConfig(const NKikimrBlobStorage::TNodeWardenServiceSet& current,
        const NKikimrBlobStorage::TNodeWardenServiceSet *proposed) {
    if (!proposed) { // just start the required services
        // wipe out obsolete VSlots from running PDisks from current.Prev; however, it is not synchronous
        return ApplyStaticServiceSet(current);
    }

    NKikimrBlobStorage::TNodeWardenServiceSet ss(*proposed);
    // stop running obsolete VSlots to prevent them from answering
    ApplyStaticServiceSet(ss);
}

void TNodeWarden::ApplyStaticServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet& ss) {
    ApplyServiceSet(ss, true, false, true);
}

void TNodeWarden::HandleIncrHugeInit(NIncrHuge::TEvIncrHugeInit::TPtr ev) {
    const TActorId keeperId = ev->GetForwardOnNondeliveryRecipient();
    const ui32 pdiskId = NIncrHuge::PDiskIdFromIncrHugeKeeperId(keeperId);

    // find local pdisk config to extract GUID
    auto it = LocalPDisks.find(TPDiskKey(LocalNodeId, pdiskId));
    Y_ABORT_UNLESS(it != LocalPDisks.end());

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
