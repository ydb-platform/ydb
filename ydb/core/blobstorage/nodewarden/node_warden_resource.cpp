#include "node_warden_impl.h"
#include <ydb/core/base/statestorage_impl.h>
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
    if (StorageConfig.HasStateStorageConfig() && StorageConfig.HasStateStorageBoardConfig() && StorageConfig.HasSchemeBoardConfig()) {
        ApplyStateStorageConfig(ev->Get()->ProposedConfig.get());
    } else {
        Y_ABORT_UNLESS(!StorageConfig.HasStateStorageConfig() && !StorageConfig.HasStateStorageBoardConfig() &&
            !StorageConfig.HasSchemeBoardConfig());
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

void TNodeWarden::ApplyStateStorageConfig(const NKikimrBlobStorage::TStorageConfig* /*proposed*/) {
    if (!Cfg->DomainsConfig) {
        return; // no state storage management
    }

    // apply updates for the state storage proxy
#define FETCH_CONFIG(PART, PREFIX, PROTO) \
    Y_ABORT_UNLESS(StorageConfig.Has##PROTO##Config()); \
    char PART##Prefix[TActorId::MaxServiceIDLength] = PREFIX; \
    TIntrusivePtr<TStateStorageInfo> PART##Info = BuildStateStorageInfo(PART##Prefix, StorageConfig.Get##PROTO##Config());

    FETCH_CONFIG(stateStorage, "ssr", StateStorage)
    FETCH_CONFIG(board, "ssb", StateStorageBoard)
    FETCH_CONFIG(schemeBoard, "sbr", SchemeBoard)

    STLOG(PRI_DEBUG, BS_NODE, NW41, "ApplyStateStorageConfig",
        (StateStorageConfig, StorageConfig.GetStateStorageConfig()),
        (NewStateStorageInfo, *stateStorageInfo),
        (CurrentStateStorageInfo, StateStorageInfo.Get()),
        (StateStorageBoardConfig, StorageConfig.GetStateStorageBoardConfig()),
        (NewStateStorageBoardInfo, *boardInfo),
        (CurrentStateStorageBoardInfo, BoardInfo.Get()),
        (SchemeBoardConfig, StorageConfig.GetSchemeBoardConfig()),
        (NewSchemeBoardInfo, *schemeBoardInfo),
        (CurrentSchemeBoardInfo, SchemeBoardInfo.Get()));

    auto changed = [](const TStateStorageInfo& prev, const TStateStorageInfo& cur) {
        auto equalRing = [](const auto& r1, const auto& r2) {
            return r1.IsDisabled == r2.IsDisabled
                && r1.UseRingSpecificNodeSelection == r2.UseRingSpecificNodeSelection
                && r1.Replicas == r2.Replicas;
        };
        return prev.NToSelect != cur.NToSelect
            || prev.Rings.size() != cur.Rings.size()
            || !std::equal(prev.Rings.begin(), prev.Rings.end(), cur.Rings.begin(), equalRing)
            || prev.StateStorageVersion != cur.StateStorageVersion
            || prev.CompatibleVersions.size() != cur.CompatibleVersions.size()
            || !std::equal(prev.CompatibleVersions.begin(), prev.CompatibleVersions.end(), cur.CompatibleVersions.begin());
    };

    TActorSystem *as = TActivationContext::ActorSystem();
    if (!StateStorageProxyConfigured || changed(*StateStorageInfo, *stateStorageInfo) || changed(*BoardInfo, *boardInfo) ||
            changed(*SchemeBoardInfo, *schemeBoardInfo)) { // reconfigure proxy
        STLOG(PRI_INFO, BS_NODE, NW50, "updating state storage proxy configuration");
        Send(MakeStateStorageProxyID(), new  TEvStateStorage::TEvUpdateGroupConfig(stateStorageInfo, boardInfo,
            schemeBoardInfo));
        StateStorageProxyConfigured = true;
    } else { // no changes
        return;
    }

    // generate actor ids of local replicas
    THashSet<TActorId> localActorIds;
    auto scanLocalActorIds = [&](const TIntrusivePtr<TStateStorageInfo>& info) {
        if (info) {
            for (const auto& ring : info->Rings) {
                for (const auto& replicaId : ring.Replicas) {
                    if (replicaId.NodeId() == LocalNodeId) {
                        const auto [it, inserted] = localActorIds.insert(replicaId);
                        Y_ABORT_UNLESS(inserted);
                    }
                }
            }
        }
    };
    scanLocalActorIds(StateStorageInfo);
    scanLocalActorIds(BoardInfo);
    scanLocalActorIds(SchemeBoardInfo);

    // start new replicas if needed
    auto startReplicas = [&](TIntrusivePtr<TStateStorageInfo>&& info, auto&& factory, const char *comp, auto *which) {
        for (const auto& ring : info->Rings) {
            for (ui32 index = 0; index < ring.Replicas.size(); ++index) {
                if (const TActorId& replicaId = ring.Replicas[index]; replicaId.NodeId() == LocalNodeId) {
                    if (ReplicaStartPending.contains(replicaId)) {
                        // this operation is already pending, we just have to wait
                    } else if (localActorIds.erase(replicaId)) {
                        if (const TActorId actorId = as->RegisterLocalService(replicaId, TActorId())) {
                            STLOG(PRI_INFO, BS_NODE, NW05, "terminating existing state storage replica",
                                (Component, comp), (ReplicaId, replicaId), (ActorId, actorId));
                            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(),
                                nullptr, 0)); // expect to terminate immediately upon reception of this message

                            auto startReplica = [this, factory, which, comp, expectedReplicaId = replicaId] {
                                const auto& info = *which;
                                ReplicaStartPending.erase(expectedReplicaId);
                                bool started = false;
                                for (const auto& ring : info->Rings) {
                                    for (ui32 index = 0; index < ring.Replicas.size(); ++index) {
                                        const auto& replicaId = ring.Replicas[index];
                                        if (replicaId == expectedReplicaId) {
                                            STLOG(PRI_INFO, BS_NODE, NW44, "delayed starting new state storage replica",
                                                (Component, comp), (ReplicaId, replicaId), (Index, index), (Config, *info));
                                            Y_ABORT_UNLESS(!started);
                                            started = true;
                                            TActorSystem *as = TActivationContext::ActorSystem();
                                            const TActorId prevActorId = as->RegisterLocalService(replicaId,
                                                as->Register(factory(info, index), TMailboxType::ReadAsFilled,
                                                AppData()->SystemPoolId));
                                            Y_VERIFY_S(!prevActorId, "unacceptable race in StateStorage replica registration"
                                                " Component# " << comp
                                                << " Index# " << index
                                                << " Config# " << info->ToString()
                                                << " ReplicaId# " << replicaId);
                                        }
                                    }
                                }
                                if (!started) {
                                    STLOG(PRI_INFO, BS_NODE, NW48, "did not start new state storage replica",
                                        (Component, comp), (ReplicaId, expectedReplicaId), (Config, *info));
                                }
                            };

                            const TActorId forwardOnNondelivery = SelfId();
                            const ui64 cookie = NextGoneCookie++;
                            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Gone, 0, actorId, SelfId(),
                                nullptr, cookie, &forwardOnNondelivery)); // this message is expected to be forwarded
                            GoneCallbacks.emplace(cookie, startReplica);
                            ReplicaStartPending.emplace(replicaId);
                        }
                    } else {
                        STLOG(PRI_INFO, BS_NODE, NW08, "starting new state storage replica",
                            (Component, comp), (ReplicaId, replicaId), (Index, index), (Config, *info));
                        as->RegisterLocalService(replicaId, as->Register(factory(info, index), TMailboxType::ReadAsFilled,
                            AppData()->SystemPoolId));
                    }
                }
            }
        }

        *which = std::move(info);
    };
    startReplicas(std::move(stateStorageInfo), CreateStateStorageReplica, "StateStorage", &StateStorageInfo);
    startReplicas(std::move(boardInfo), CreateStateStorageBoardReplica, "StateStorageBoard", &BoardInfo);
    startReplicas(std::move(schemeBoardInfo), CreateSchemeBoardReplica, "SchemeBoard", &SchemeBoardInfo);

    // terminate unused replicas
    for (const auto& replicaId : localActorIds) {
        STLOG(PRI_INFO, BS_NODE, NW43, "terminating useless state storage replica", (ReplicaId, replicaId));
        const TActorId actorId = as->RegisterLocalService(actorId, TActorId());
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
    }
}

void TNodeWarden::HandleGone(STATEFN_SIG) {
    auto nh = GoneCallbacks.extract(ev->Cookie);
    Y_ABORT_UNLESS(nh);
    nh.mapped()();
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
