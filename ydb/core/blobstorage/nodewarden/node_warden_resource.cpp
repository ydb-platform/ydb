#include "node_warden.h"
#include "node_warden_impl.h"
#include "distconf.h"
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/incrhuge/incrhuge_keeper.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/library/pdisk_io/file_params.h>
#include <ydb/library/pdisk_io/wcache.h>
#include <library/cpp/streams/zstd/zstd.h>
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

void TNodeWarden::ApplyServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet &serviceSet, bool isStatic,
        bool comprehensive, bool updateCache, const char *origin) {
    if (Cfg->IsCacheEnabled() && updateCache) {
        Y_ABORT_UNLESS(!isStatic);
        return EnqueueSyncOp(WrapCacheOp(UpdateServiceSet(serviceSet, comprehensive, [=, this] {
            ApplyServiceSet(serviceSet, false, comprehensive, false, origin);
        })));
    }

    STLOG(PRI_DEBUG, BS_NODE, NW18, "ApplyServiceSet", (IsStatic, isStatic), (Comprehensive, comprehensive),
        (Origin, origin), (ServiceSet, serviceSet));

    // apply proxy information before we try to start VDisks/PDisks
    ApplyGroupInfoFromServiceSet(serviceSet);

    // merge new configuration into current one
    NKikimrBlobStorage::TNodeWardenServiceSet *target = isStatic ? &StaticServices : &DynamicServices;
    NProtoBuf::RepeatedPtrField<TServiceSetPDisk> *to = target->MutablePDisks();
    if (comprehensive) {
        to->Clear();
    }
    MergeServiceSetPDisks(to, serviceSet.GetPDisks());

    if (!EnableProxyMock) {
        // in mock mode we don't need PDisk/VDisk instances
        ApplyServiceSetPDisks();
        ApplyServiceSetVDisks(serviceSet);
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
    if (AppData()->BridgeModeEnabled && !BridgeInfo) {
        // block until bridge information is filled in by distconf after bootstrapping
        PendingQueryStorageConfigQ.push_back(ev);
        return;
    }

    Send(ev->Sender, new TEvNodeWardenStorageConfig(StorageConfig, nullptr, SelfManagementEnabled, BridgeInfo));
    if (ev->Get()->Subscribe) {
        StorageConfigSubscribers.insert(ev->Sender);
    }
}

void TNodeWarden::Handle(TEvNodeWardenStorageConfig::TPtr ev) {
    auto *msg = ev->Get();
    StorageConfig = std::move(msg->Config);
    SelfManagementEnabled = msg->SelfManagementEnabled;
    BridgeInfo = std::move(msg->BridgeInfo);

    if (StorageConfig->HasBlobStorageConfig()) {
        if (const auto& bsConfig = StorageConfig->GetBlobStorageConfig(); bsConfig.HasServiceSet()) {
            const NKikimrBlobStorage::TNodeWardenServiceSet *proposed = nullptr;
            if (const auto& proposedConfig = ev->Get()->ProposedConfig) {
                Y_VERIFY_S(StorageConfig->GetGeneration() < proposedConfig->GetGeneration(),
                    "StorageConfig.Generation# " << StorageConfig->GetGeneration()
                    << " ProposedConfig.Generation# " << proposedConfig->GetGeneration());
                Y_ABORT_UNLESS(proposedConfig->HasBlobStorageConfig()); // must have the BlobStorageConfig and the ServiceSet
                const auto& proposedBsConfig = proposedConfig->GetBlobStorageConfig();
                Y_ABORT_UNLESS(proposedBsConfig.HasServiceSet());
                proposed = &proposedBsConfig.GetServiceSet();
            }
            ApplyStorageConfig(bsConfig.GetServiceSet(), proposed);
        }
    }

    if (StorageConfig->HasStateStorageConfig() && StorageConfig->HasStateStorageBoardConfig() && StorageConfig->HasSchemeBoardConfig()) {
        ApplyStateStorageConfig(ev->Get()->ProposedConfig.get());
    } else {
        Y_ABORT_UNLESS(!StorageConfig->HasStateStorageConfig() && !StorageConfig->HasStateStorageBoardConfig() &&
            !StorageConfig->HasSchemeBoardConfig());
    }

    for (const TActorId& subscriber : StorageConfigSubscribers) {
        Send(subscriber, new TEvNodeWardenStorageConfig(StorageConfig, nullptr, SelfManagementEnabled, BridgeInfo));
    }

    if (StorageConfig->HasConfigComposite()) {
        TString mainConfigYaml;
        ui64 mainConfigYamlVersion;
        auto error = DecomposeConfig(StorageConfig->GetConfigComposite(), &mainConfigYaml, &mainConfigYamlVersion, nullptr);
        if (error) {
            STLOG_DEBUG_FAIL(BS_NODE, NW49, "failed to decompose yaml configuration", (Error, error));
        } else if (mainConfigYaml) {
            std::optional<TString> storageConfigYaml;
            std::optional<ui64> storageConfigYamlVersion;
            if (StorageConfig->HasCompressedStorageYaml()) {
                try {
                    TStringInput s(StorageConfig->GetCompressedStorageYaml());
                    storageConfigYaml.emplace(TZstdDecompress(&s).ReadAll());
                    storageConfigYamlVersion.emplace(NYamlConfig::GetStorageMetadata(*storageConfigYaml).Version.value_or(0));
                } catch (const std::exception& ex) {
                    Y_ABORT("CompressedStorageYaml format incorrect: %s", ex.what());
                }
            }

            // TODO(alexvru): make this blocker for confirmation?
            PersistConfig(std::move(mainConfigYaml), mainConfigYamlVersion, std::move(storageConfigYaml),
                storageConfigYamlVersion);
        }
    } else {
        Y_DEBUG_ABORT_UNLESS(!StorageConfig->HasCompressedStorageYaml());
    }

    TActivationContext::Send(new IEventHandle(TEvBlobStorage::EvNodeWardenStorageConfigConfirm, 0, ev->Sender, SelfId(),
        nullptr, ev->Cookie));

    if (BridgeInfo) {
        for (auto& ev : std::exchange(PendingQueryStorageConfigQ, {})) {
            TAutoPtr<IEventHandle> temp(ev.Release());
            Receive(temp);
        }
    }
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

    ApplyStaticServiceSet(current);
}

void TNodeWarden::ApplyStateStorageConfig(const NKikimrBlobStorage::TStorageConfig* /*proposed*/) {
    if (!Cfg->DomainsConfig) {
        return; // no state storage management
    }

    // apply updates for the state storage proxy
#define FETCH_CONFIG(PART, PROTO) \
    Y_ABORT_UNLESS(StorageConfig->Has##PROTO##Config()); \
    TIntrusivePtr<TStateStorageInfo> PART##Info = Build##PROTO##Info(StorageConfig->Get##PROTO##Config());

    FETCH_CONFIG(stateStorage, StateStorage)
    FETCH_CONFIG(board, StateStorageBoard)
    FETCH_CONFIG(schemeBoard, SchemeBoard)

    STLOG(PRI_DEBUG, BS_NODE, NW55, "ApplyStateStorageConfig",
        (StateStorageConfig, StorageConfig->GetStateStorageConfig()),
        (NewStateStorageInfo, *stateStorageInfo),
        (CurrentStateStorageInfo, StateStorageInfo.Get()),
        (StateStorageBoardConfig, StorageConfig->GetStateStorageBoardConfig()),
        (NewStateStorageBoardInfo, *boardInfo),
        (CurrentStateStorageBoardInfo, BoardInfo.Get()),
        (SchemeBoardConfig, StorageConfig->GetSchemeBoardConfig()),
        (NewSchemeBoardInfo, *schemeBoardInfo),
        (CurrentSchemeBoardInfo, SchemeBoardInfo.Get()));

    auto changed = [](const TStateStorageInfo& prev, const TStateStorageInfo& cur) {

        auto equalGroup = [](const auto& g1, const auto& g2) {
            auto equalRing = [](const auto& r1, const auto& r2) {
                return r1.IsDisabled == r2.IsDisabled
                    && r1.UseRingSpecificNodeSelection == r2.UseRingSpecificNodeSelection
                    && r1.Replicas == r2.Replicas;
            };
            return g1.Rings.size() == g2.Rings.size()
                && g1.NToSelect == g2.NToSelect
                && g1.WriteOnly == g2.WriteOnly
                && g1.State == g2.State
                && std::equal(g1.Rings.begin(), g1.Rings.end(), g2.Rings.begin(), equalRing);
        };
        return prev.RingGroups.size() != cur.RingGroups.size()
            || !std::equal(prev.RingGroups.begin(), prev.RingGroups.end(), cur.RingGroups.begin(), equalGroup)
            || prev.StateStorageVersion != cur.StateStorageVersion
            || prev.ClusterStateGeneration != cur.ClusterStateGeneration
            || prev.ClusterStateGuid != cur.ClusterStateGuid
            || prev.CompatibleVersions.size() != cur.CompatibleVersions.size()
            || !std::equal(prev.CompatibleVersions.begin(), prev.CompatibleVersions.end(), cur.CompatibleVersions.begin());
    };


    TActorSystem *as = TActivationContext::ActorSystem();
    const bool changedStateStorage = !StateStorageProxyConfigured || changed(*StateStorageInfo, *stateStorageInfo);
    const bool changedBoard = !StateStorageProxyConfigured || changed(*BoardInfo, *boardInfo);
    const bool changedSchemeBoard = !StateStorageProxyConfigured || changed(*SchemeBoardInfo, *schemeBoardInfo);
    if (!changedStateStorage && !changedBoard && !changedSchemeBoard) {
        return; // no changes
    }

    // start new replicas if needed
    THashSet<TActorId> localActorIds;
    THashSet<TActorId> newActorIds;
    auto startReplicas = [&](TIntrusivePtr<TStateStorageInfo>&& info, auto&& factory, const char *comp, auto *which) {
        // collect currently running local replicas
        if (const auto& current = *which) {
            for (const auto& ringGroup : current->RingGroups) {
                for (const auto& ring : ringGroup.Rings) {
                    for (const auto& replicaId : ring.Replicas) {
                        if (replicaId.NodeId() == LocalNodeId) {
                            STLOG(PRI_INFO, BS_NODE, NW54, "Local replica found", (Component, comp), (ReplicaId, replicaId));
                            localActorIds.insert(replicaId);
                        }
                    }
                }
            }
        }

        for (const auto& ringGroup : info->RingGroups) {
            for (const auto& ring : ringGroup.Rings) {
                for (ui32 index = 0; index < ring.Replicas.size(); ++index) {
                    if (const TActorId& replicaId = ring.Replicas[index]; replicaId.NodeId() == LocalNodeId) {
                        if (!localActorIds.contains(replicaId) && !newActorIds.contains(replicaId)) {
                            STLOG(PRI_INFO, BS_NODE, NW08, "starting state storage new replica",
                                (Component, comp), (ReplicaId, replicaId), (Index, index), (Config, *info));
                            as->RegisterLocalService(replicaId, as->Register(factory(info, index), TMailboxType::ReadAsFilled,
                                AppData()->SystemPoolId));
                        } else if (which == &StateStorageInfo && !newActorIds.contains(replicaId)) {
                            Send(replicaId, new TEvStateStorage::TEvUpdateGroupConfig(info, nullptr, nullptr));
                        } else if (which == &BoardInfo && !newActorIds.contains(replicaId)) {
                            Send(replicaId, new TEvStateStorage::TEvUpdateGroupConfig(nullptr, info, nullptr));
                        } else if (which == &SchemeBoardInfo && !newActorIds.contains(replicaId)) {
                            Send(replicaId, new TEvStateStorage::TEvUpdateGroupConfig(nullptr, nullptr, info));
                        }
                        newActorIds.insert(replicaId);
                    }
                }
            }
        }

        *which = std::move(info);
    };
    if (changedStateStorage) {
        startReplicas(std::move(stateStorageInfo), CreateStateStorageReplica, "StateStorage", &StateStorageInfo);
    }
    if (changedBoard) {
        startReplicas(std::move(boardInfo), CreateStateStorageBoardReplica, "StateStorageBoard", &BoardInfo);
    }
    if (changedSchemeBoard) {
        startReplicas(std::move(schemeBoardInfo), CreateSchemeBoardReplica, "SchemeBoard", &SchemeBoardInfo);
    }

    // reconfigure proxy
    STLOG(PRI_INFO, BS_NODE, NW50, "updating state storage proxy configuration");
    if (StateStorageProxyConfigured) {
        Send(MakeStateStorageProxyID(), new TEvStateStorage::TEvUpdateGroupConfig(StateStorageInfo, BoardInfo,
            SchemeBoardInfo));
    } else {
        const TActorId newInstance = as->Register(CreateStateStorageProxy(StateStorageInfo, BoardInfo, SchemeBoardInfo),
            TMailboxType::ReadAsFilled, AppData()->SystemPoolId);
        const TActorId stubInstance = as->RegisterLocalService(MakeStateStorageProxyID(), newInstance);
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, stubInstance, newInstance, nullptr, 0));
        StateStorageProxyConfigured = true;
    }

    // terminate unused replicas
    for (const auto& replicaId : localActorIds) {
        if (!newActorIds.contains(replicaId)) {
            STLOG(PRI_INFO, BS_NODE, NW43, "terminating useless state storage replica", (ReplicaId, replicaId));
            const TActorId actorId = as->RegisterLocalService(replicaId, TActorId());
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
        }
    }
}

void TNodeWarden::ApplyStaticServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet& ss) {
    ApplyServiceSet(ss, true /*isStatic*/, true /*comprehensive*/, false /*updateCache*/, "distconf");
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
    TActivationContext::ActorSystem()->RegisterLocalService(keeperId, actorId);

    // forward to just created service
    TActivationContext::Send(ev->Forward(keeperId));
}

void TNodeWarden::Handle(TEvNodeWardenNotifyConfigMismatch::TPtr ev) {
    //TODO: config mismatch with node
    auto *msg = ev->Get();
    STLOG(PRI_INFO, BS_NODE, NW51, "TEvNodeWardenNotifyConfigMismatch: NodeId: " << msg->NodeId
        << " ClusterStateGeneration: " << msg->ClusterStateGeneration << " ClusterStateGuid: " << msg->ClusterStateGuid);
}

void TNodeWarden::Handle(TEvNodeWardenQueryBaseConfig::TPtr ev) {
    auto request = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
    request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
    const ui64 cookie = NextConfigCookie++;
    SendToController(std::move(request), cookie);

    ConfigInFlight.emplace(cookie, [this, sender = ev->Sender, cookie = ev->Cookie](TEvBlobStorage::TEvControllerConfigResponse *ev) {
        auto response = std::make_unique<TEvNodeWardenBaseConfig>();
        if (ev) {
            auto *record = ev->Record.MutableResponse();
            if (record->GetSuccess() && record->StatusSize() == 1) {
                response->BaseConfig = std::move(*record->MutableStatus(0)->MutableBaseConfig());
            }
        }
        Send(sender, response.release(), 0, cookie);
    });
}
