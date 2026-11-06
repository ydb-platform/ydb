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

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

using namespace NKikimr;
using namespace NStorage;

namespace {
    std::set<TPDiskKey> FindPDiskConflicts(const NKikimrBlobStorage::TNodeWardenServiceSet& cached,
            const NKikimrBlobStorage::TNodeWardenServiceSet& current) {
        std::map<std::pair<ui32, TString>, ui32> idsByPath;
        std::map<TPDiskKey, ui64> guidsById;
        for (const auto& pdisk : current.GetPDisks()) {
            idsByPath.emplace(std::make_pair(pdisk.GetNodeID(), pdisk.GetPath()), pdisk.GetPDiskID());
            guidsById.emplace(TPDiskKey(pdisk), pdisk.GetPDiskGuid());
        }
        std::set<TPDiskKey> conflicts;
        for (const auto& pdisk : cached.GetPDisks()) {
            const auto it = idsByPath.find({pdisk.GetNodeID(), pdisk.GetPath()});
            const auto guid = guidsById.find(TPDiskKey(pdisk));
            if ((it != idsByPath.end() && it->second != pdisk.GetPDiskID())
                    || (guid != guidsById.end() && guid->second != pdisk.GetPDiskGuid())) {
                conflicts.emplace(pdisk);
            }
        }
        return conflicts;
    }
}

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

void TNodeWarden::ApplyControllerServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet &serviceSet, bool comprehensive) {
    const ui64 generation = ControllerServiceSetGeneration;
    auto apply = [=, this] {
        if (generation == ControllerServiceSetGeneration && CanConnectToController()) {
            ApplyServiceSet(serviceSet, false, comprehensive, "controller");
            if (comprehensive) {
                LastConfirmedControllerServiceSetGeneration = generation;
            }
        } else if (comprehensive && generation >= LastConfirmedControllerServiceSetGeneration) {
            // Keep recovery possible if the first response's cache write outlives its pipe, but filter
            // identities superseded by static configuration, like a replay of the cache itself.
            // An incremental update may omit the PDisk needed to validate its VDisks.
            ApplyCachedServiceSet(serviceSet);
        } else if (generation == ControllerServiceSetGeneration) {
            // A disconnected pile can still receive updates over an existing BSC pipe.
            // Keep its group generations current without recreating disk services:
            // otherwise a local proxy can race with VDisks that already know the newer generation.
            ApplyGroupInfoFromServiceSet(serviceSet, false, false);
        }
    };
    if (Cfg->IsCacheEnabled()) {
        return EnqueueSyncOp(WrapCacheOp(UpdateServiceSet(serviceSet, comprehensive, std::move(apply))));
    }
    return apply();
}

void TNodeWarden::ApplyServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet &serviceSet, bool isStatic,
        bool comprehensive, const char *origin) {
    YDB_LOG_DEBUG("ApplyServiceSet",
        {"marker", "NW18"},
        {"isStatic", isStatic},
        {"comprehensive", comprehensive},
        {"origin", origin},
        {"serviceSet", serviceSet});

    if (comprehensive) {
        PruneLocalVDisks(serviceSet, isStatic);
    }

    for (const auto& incomingGroup : serviceSet.GetGroups()) {
        const ui32 groupId = incomingGroup.GetGroupID();
        if (incomingGroup.GetEntityStatus() != NKikimrBlobStorage::DESTROY) {
            const auto it = Groups.find(groupId);
            const auto *ep = it != Groups.end() ? &it->second.EncryptionParams : nullptr;
            const bool storedHasEncryptionMode = ep && ep->HasEncryptionMode();
            const ui32 storedEncryptionMode = storedHasEncryptionMode ? ep->GetEncryptionMode() : 0;
            const ui32 storedLifeCyclePhase = storedHasEncryptionMode ? ep->GetLifeCyclePhase() : 0;
            const ui32 incomingEncryptionMode = incomingGroup.GetEncryptionMode();
            const bool incomingEncryptionModeIsKnown = incomingEncryptionMode <= static_cast<ui32>(TBlobStorageGroupInfo::EEM_ENC_V1);
            const bool firstInvalidEncryptionMode = !storedHasEncryptionMode && !incomingEncryptionModeIsKnown;
            const bool encryptionModeChanged = storedHasEncryptionMode && storedEncryptionMode != incomingEncryptionMode;
            if (firstInvalidEncryptionMode || encryptionModeChanged) {
                STLOG(PRI_ERROR, BS_NODE, NW114, "ApplyServiceSet group EncryptionMode diagnostics",
                    (Origin, origin),
                    (GroupId, groupId),
                    (IncomingGeneration, incomingGroup.GetGroupGeneration()),
                    (StoredHasEncryptionMode, storedHasEncryptionMode),
                    (StoredEncryptionModeRaw, storedEncryptionMode),
                    (IncomingEncryptionModeRaw, incomingEncryptionMode),
                    (StoredLifeCyclePhase, storedLifeCyclePhase),
                    (IncomingLifeCyclePhase, incomingGroup.GetLifeCyclePhase()));
                break;
            }
        }
    }

    // apply proxy information before we try to start VDisks/PDisks
    ApplyGroupInfoFromServiceSet(serviceSet, isStatic, comprehensive);

    // merge new configuration into current one
    NKikimrBlobStorage::TNodeWardenServiceSet *target = isStatic ? &StaticServices : &DynamicServices;
    NProtoBuf::RepeatedPtrField<TServiceSetPDisk> *to = target->MutablePDisks();
    TVector<TServiceSetPDisk> pdisksToRestart;
    if (comprehensive) {
        to->Clear();
    }
    MergeServiceSetPDisks(to, serviceSet.GetPDisks(), pdisksToRestart);
    RetireConflictingDynamicPDisks(isStatic);

    UpdateBlobStorageExecutorPoolMapping();
    for (const TServiceSetPDisk& pdisk : pdisksToRestart) {
        // Reconciliation can reject a disk after the merge has already queued its restart.
        // Check the surviving configuration, rather than the running actor: MovePDisk can change its GUID.
        const bool configured = std::ranges::any_of(target->GetPDisks(), [&](const auto& current) {
            return TPDiskKey(current) == TPDiskKey(pdisk)
                && current.GetPDiskGuid() == pdisk.GetPDiskGuid() && current.GetPath() == pdisk.GetPath();
        });
        if (configured) {
            DoRestartLocalPDisk(pdisk);
        }
    }

    if (!EnableProxyMock) {
        // in mock mode we don't need PDisk/VDisk instances
        ApplyServiceSetPDisks();
        ApplyServiceSetVDisks(serviceSet, isStatic);
    }

    for (auto& [vslotId, vdisk] : LocalVDisks) {
        if (vdisk.UnderlyingPDiskDestroyed) {
            auto& tempVSlotId = vslotId;
            YDB_LOG_DEBUG_COMP_FAIL(BS_NODE, "UnderlyingPDiskDestroyed escaped",
                {"marker", "NW37"},
                {"VSlotId", tempVSlotId});
            vdisk.UnderlyingPDiskDestroyed = false;
        }
    }
}

void TNodeWarden::ApplyCachedServiceSet(NKikimrBlobStorage::TNodeWardenServiceSet serviceSet) {
    // Cache recovery may run after distconf has already installed a replacement on the same path.
    // Never let that delayed replay resurrect the superseded PDisk or its VDisks.
    auto conflicts = FindPDiskConflicts(serviceSet, StaticServices);
    conflicts.merge(FindPDiskConflicts(serviceSet, DynamicServices));
    EraseIf(*serviceSet.MutablePDisks(), [&](const auto& pdisk) {
        const bool conflicting = conflicts.contains(TPDiskKey(pdisk));
        if (conflicting) {
            YDB_LOG_NOTICE("Ignoring cached PDisk conflicting with current configuration",
                {"marker", "NW123"}, {"pdisk", pdisk});
        }
        return conflicting;
    });
    EraseIf(*serviceSet.MutableVDisks(), [&](const auto& vdisk) {
        const auto& loc = vdisk.GetVDiskLocation();
        return conflicts.contains({loc.GetNodeID(), loc.GetPDiskID()});
    });

    ApplyServiceSet(serviceSet, false, false, "cache");
}

bool TNodeWarden::CanConnectToController() const {
    return !BridgeInfo || !BridgeInfo->SelfNodePile
        || NBridge::PileStateTraits(BridgeInfo->SelfNodePile->State).AllowsConnection;
}

template<typename TPredicate>
void TNodeWarden::RetireLocalVDisksIf(TPredicate&& predicate) {
    for (auto it = LocalVDisks.begin(); it != LocalVDisks.end(); ) {
        TVDiskRecord& vdisk = it->second;
        const bool staticVDisk = TGroupID(vdisk.GetGroupId()).ConfigurationType() == EGroupConfigurationType::Static;
        if (predicate(it->first, vdisk, staticVDisk)) {
            YDB_LOG_NOTICE("Retiring superseded local VDisk",
                {"marker", "NW122"},
                {"VSlotId", vdisk.GetVSlotId()},
                {"VDiskId", vdisk.GetVDiskId()});
            PoisonLocalVDisk(vdisk);
            DestroyLocalVDisk(vdisk);
            it = LocalVDisks.erase(it);
        } else {
            ++it;
        }
    }
}

void TNodeWarden::RetireConflictingDynamicPDisks(bool isStatic) {
    const auto conflicts = FindPDiskConflicts(DynamicServices, StaticServices);
    if (conflicts.empty()) {
        return;
    }
    // Static identities are authoritative when the two configurations disagree. Retire conflicting
    // dynamic actors without slaying or clearing media, including delayed updates on a live BSC pipe.
    // A controller update losing to a static configuration may just be ahead of it: restore it after a catch-up.
    ControllerServicesDropped |= !isStatic;
    EraseIf(*DynamicServices.MutablePDisks(), [&](const auto& pdisk) {
        const bool conflicting = conflicts.contains(TPDiskKey(pdisk));
        if (conflicting) {
            YDB_LOG_NOTICE("Retiring PDisk superseded by static configuration",
                {"marker", "NW124"}, {"pdisk", pdisk});
        }
        return conflicting;
    });
    RetireLocalVDisksIf([&](const TVSlotId& slot, const TVDiskRecord&, bool staticVDisk) {
        return conflicts.contains({slot.NodeId, slot.PDiskId}) && (isStatic || !staticVDisk);
    });
}

void TNodeWarden::Handle(TEvNodeWardenQueryStorageConfig::TPtr ev) {
    if (AppData()->BridgeModeEnabled && !BridgeInfo) {
        // block until bridge information is filled in by distconf after bootstrapping
        PendingQueryStorageConfigQ.push_back(ev);
        return;
    }

    Send(ev->Sender, new TEvNodeWardenStorageConfig(StorageConfig, SelfManagementEnabled, BridgeInfo));
    if (ev->Get()->Subscribe) {
        StorageConfigSubscribers.insert(ev->Sender);
    }
}

void TNodeWarden::Handle(TEvNodeWardenStorageConfig::TPtr ev) {
    auto *msg = ev->Get();
    StorageConfig = std::move(msg->Config);
    CommittedStorageConfig = std::move(msg->CommittedConfig);
    SelfManagementEnabled = msg->SelfManagementEnabled;
    const bool couldConnect = CanConnectToController();
    BridgeInfo = std::move(msg->BridgeInfo);
    const bool canConnect = CanConnectToController();
    if (!canConnect || !couldConnect) {
        // A rejected handshake can deliver this config before the old BSC pipe reports failure.
        // Fence controller updates still waiting for their cache writes before applying replacements.
        ++ControllerServiceSetGeneration;
    }
    // Disk services received while the pile was disallowed to connect were ignored.
    ControllerServicesDropped |= !couldConnect && canConnect;

    if (StorageConfig->HasBlobStorageConfig()) {
        const auto& bsConfig = StorageConfig->GetBlobStorageConfig();
        if (bsConfig.HasServiceSet()) {
            ApplyStorageConfig(bsConfig.GetServiceSet());
            if (ControllerServicesDropped && PipeClientId && canConnect) {
                // BSC sends full state only in reply to registration; the destroyed pipe gets reestablished.
                ControllerServicesDropped = false;
                NTabletPipe::CloseClient(SelfId(), PipeClientId);
            }
        }
        SyncRateConfigLimit = bsConfig.GetBridgeSyncRateBytesPerSecond();
        UpdateSyncRateQuoter();
    }

    if (StorageConfig->HasStateStorageConfig() && StorageConfig->HasStateStorageBoardConfig() && StorageConfig->HasSchemeBoardConfig()) {
        ApplyStateStorageConfig();
    } else {
        Y_ABORT_UNLESS(!StorageConfig->HasStateStorageConfig() && !StorageConfig->HasStateStorageBoardConfig() &&
            !StorageConfig->HasSchemeBoardConfig());
    }

    for (const TActorId& subscriber : StorageConfigSubscribers) {
        Send(subscriber, new TEvNodeWardenStorageConfig(StorageConfig, SelfManagementEnabled, BridgeInfo, CommittedStorageConfig));
    }

    if (StorageConfig->HasConfigComposite()) {
        TString mainConfigYaml;
        ui64 mainConfigYamlVersion;
        auto error = DecomposeConfig(StorageConfig->GetConfigComposite(), &mainConfigYaml, &mainConfigYamlVersion, nullptr);
        if (error) {
            YDB_LOG_DEBUG_COMP_FAIL(BS_NODE, "failed to decompose yaml configuration",
                {"marker", "NW49"},
                {"Error", error});
        } else if (mainConfigYaml) {
            std::optional<TString> storageConfigYaml = GetStorageYaml(*StorageConfig);
            std::optional<ui64> storageConfigYamlVersion = storageConfigYaml
                ? std::make_optional(NYamlConfig::GetStorageMetadata(*storageConfigYaml).Version.value_or(0))
                : std::nullopt;

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

void TNodeWarden::ApplyStorageConfig(const NKikimrBlobStorage::TNodeWardenServiceSet& current) {
    ApplyStaticServiceSet(current);
}

void TNodeWarden::ApplyStateStorageConfig() {
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

    YDB_LOG_DEBUG("ApplyStateStorageConfig",
        {"marker", "NW55"},
        {"stateStorageConfig", StorageConfig->GetStateStorageConfig()},
        {"newStateStorageInfo", *stateStorageInfo},
        {"currentStateStorageInfo", StateStorageInfo.Get()},
        {"stateStorageBoardConfig", StorageConfig->GetStateStorageBoardConfig()},
        {"newStateStorageBoardInfo", *boardInfo},
        {"currentStateStorageBoardInfo", BoardInfo.Get()},
        {"schemeBoardConfig", StorageConfig->GetSchemeBoardConfig()},
        {"newSchemeBoardInfo", *schemeBoardInfo},
        {"currentSchemeBoardInfo", SchemeBoardInfo.Get()});

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
                    if (ring.IsDisabled) {
                        continue;
                    }
                    for (const auto& replicaId : ring.Replicas) {
                        if (replicaId.NodeId() == LocalNodeId) {
                            YDB_LOG_INFO("Local replica found",
                                {"marker", "NW54"},
                                {"component", comp},
                                {"replicaId", replicaId});
                            localActorIds.insert(replicaId);
                        }
                    }
                }
            }
        }

        for (const auto& ringGroup : info->RingGroups) {
            for (const auto& ring : ringGroup.Rings) {
                if (ring.IsDisabled) {
                    continue;
                }
                for (ui32 index = 0; index < ring.Replicas.size(); ++index) {
                    if (const TActorId& replicaId = ring.Replicas[index]; replicaId.NodeId() == LocalNodeId) {
                        if (!localActorIds.contains(replicaId) && !newActorIds.contains(replicaId)) {
                            YDB_LOG_INFO("Starting state storage new replica",
                                {"marker", "NW08"},
                                {"component", comp},
                                {"replicaId", replicaId},
                                {"index", index},
                                {"config", *info});
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
    YDB_LOG_INFO("Updating state storage proxy configuration",
        {"marker", "NW50"});
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
            YDB_LOG_INFO("Terminating useless state storage replica",
                {"marker", "NW43"},
                {"replicaId", replicaId});
            const TActorId actorId = as->RegisterLocalService(replicaId, TActorId());
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
        }
    }
}

void TNodeWarden::PruneLocalVDisks(const NKikimrBlobStorage::TNodeWardenServiceSet& ss, bool isStatic) {
    // A node recovering from old disk metadata or BSC cache may have missed the DESTROY records.
    // Omitted slots must not keep obsolete PDisks occupying device paths after a comprehensive update.
    std::map<TVSlotId, ui64> slots;
    for (const auto& vdisk : ss.GetVDisks()) {
        if (vdisk.GetVDiskLocation().GetNodeID() == LocalNodeId) {
            slots.emplace(TVSlotId(vdisk.GetVDiskLocation()), vdisk.GetVDiskLocation().GetPDiskGuid());
        }
    }
    RetireLocalVDisksIf([&](const TVSlotId& slot, const TVDiskRecord& vdisk, bool staticVDisk) {
        const auto expected = slots.find(slot);
        return staticVDisk == isStatic && (expected == slots.end()
            || expected->second != vdisk.Config.GetVDiskLocation().GetPDiskGuid());
    });
}

void TNodeWarden::ApplyStaticServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet& ss) {
    ApplyServiceSet(ss, true /*isStatic*/, true /*comprehensive*/, "distconf");
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
    TActorId actorId = Register(CreateIncrHugeKeeper(settings), TMailboxType::HTSwap,
        GetBlobStorageExecutorPoolId(pdiskId).value_or(AppData()->SystemPoolId));

    // bind it to service
    TActivationContext::ActorSystem()->RegisterLocalService(keeperId, actorId);

    // forward to just created service
    TActivationContext::Send(ev->Forward(keeperId));
}

void TNodeWarden::Handle(TEvNodeWardenNotifyConfigMismatch::TPtr ev) {
    //TODO: config mismatch with node
    auto *msg = ev->Get();
    YDB_LOG_INFO("TEvNodeWardenNotifyConfigMismatch",
        {"marker", "NW51"},
        {"nodeId", msg->NodeId},
        {"clusterStateGeneration", msg->ClusterStateGeneration},
        {"clusterStateGuid", msg->ClusterStateGuid});
}

void TNodeWarden::Handle(TEvNodeWardenQueryBaseConfig::TPtr ev) {
    auto request = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
    request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
    const ui64 cookie = NextConfigCookie++;
    SendToController(std::move(request), cookie);

    ConfigInFlight.emplace(cookie, [this, sender = ev->Sender, cookie = ev->Cookie](TEvBlobStorage::TEvControllerConfigResponse *ev) {
        auto response = std::make_unique<TEvNodeWardenBaseConfig>();
        if (!ev) {
            response->ErrorReason = "pipe to BSC disconnected";
        } else if (auto *record = ev->Record.MutableResponse(); record->GetSuccess() && record->StatusSize() == 1) {
            response->BaseConfig = std::move(*record->MutableStatus(0)->MutableBaseConfig());
        } else {
            response->ErrorReason = TStringBuilder() << "BSC failed to provide base config: " << record->GetErrorDescription();
        }
        Send(sender, response.release(), 0, cookie);
    });
}

void TNodeWarden::Handle(TEvNodeWardenListLocalDDisks::TPtr ev) {
    auto res = std::make_unique<TEvNodeWardenListLocalDDisksResult>();
    for (auto& [k, v] : LocalVDisks) {
        if (v.RuntimeData && v.RuntimeData->DDisk) {
            auto vdId = v.GetVSlotId().GetVDiskServiceId();
            auto [nodeId, pdiskId, vslotId] = DecomposeVDiskServiceId(vdId);
            res->Infos.emplace_back(MakeBlobStorageDDiskId(nodeId, pdiskId, vslotId), MakeBlobStoragePersistentBufferId(nodeId, pdiskId, vslotId));
        }
    }
    Send(ev->Sender, res.release(), 0, ev->Cookie);
}
