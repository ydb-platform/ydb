#include "node_warden.h"
#include "node_warden_impl.h"
#include "node_warden_events.h"

#include <ydb/core/blobstorage/bridge/syncer/syncer.h>

#include <ydb/core/blob_depot/agent/agent.h>

#include <ydb/core/util/random.h>

namespace NKikimr::NStorage {

    TIntrusivePtr<TBlobStorageGroupInfo> TNodeWarden::NeedGroupInfo(ui32 groupId) {
        if (EjectedGroups.count(groupId)) {
            return nullptr;
        }

        auto& group = Groups[groupId];
        if (const auto& info = group.Info; !info) {
            // we do not have relevant group configuration, request it; we can't return group now
            RequestGroupConfig(groupId, group);
            return nullptr;
        } else if (group.EncryptionParams.GetEncryptionMode() == TBlobStorageGroupInfo::EEM_NONE) {
            // no encryption required, group info relevant
            Y_ABORT_UNLESS(group.EncryptionParams.HasEncryptionMode());
            return info;
        } else {
            // encryption required, check key's life cycle phase
            switch (info->GetLifeCyclePhase()) {
                case TBlobStorageGroupInfo::ELCP_INITIAL:
                    // we have to request key here, if we haven't done this yet; if there is no main key set, then we
                    // just return group without key and use it in limited mode
                    if (const TEncryptionKey& mainKey = GetGroupMainKey(groupId)) {
                        ProposeKey(groupId, mainKey, group.EncryptionParams);
                        return nullptr;
                    } else {
                        Y_ABORT_UNLESS(!info->GetCypherKey()->GetIsKeySet()); // ensure no key loaded
                        return info;
                    }

                case TBlobStorageGroupInfo::ELCP_IN_TRANSITION:
                    // someone was proposing key and its transaction is in flight; we have to re-request the key after
                    // some time, no key proposition needed; retry logic is implemented in application processor
                    return nullptr;

                case TBlobStorageGroupInfo::ELCP_IN_USE:
                    // key is in use and loaded, return the group
                    Y_ABORT_UNLESS(info->GetCypherKey()->GetIsKeySet());
                    return info;

                case TBlobStorageGroupInfo::ELCP_PROPOSE:
                case TBlobStorageGroupInfo::ELCP_KEY_CRC_ERROR:
                case TBlobStorageGroupInfo::ELCP_KEY_VERSION_ERROR:
                case TBlobStorageGroupInfo::ELCP_KEY_ID_ERROR:
                case TBlobStorageGroupInfo::ELCP_KEY_NOT_LOADED:
                    // run proxy in limited mode
                    Y_ABORT_UNLESS(!info->GetCypherKey()->GetIsKeySet()); // ensure no key loaded
                    return info;
            }
        }
    }

    void TNodeWarden::ProposeKey(ui32 groupId, const TEncryptionKey& mainKey, const NKikimrBlobStorage::TGroupInfo& encryptionParams) {
        TCypherKey groupKey;
        ui8 *keyBytes = nullptr;
        ui32 keySizeBytes = 0;
        groupKey.MutableKeyBytes(&keyBytes, &keySizeBytes);
        SafeEntropyPoolRead(keyBytes, keySizeBytes);
        TString encryptedGroupKey;
        ui32 h = Crc32c(keyBytes, keySizeBytes);
        encryptedGroupKey.resize(keySizeBytes + sizeof(ui32));
        char *destination = encryptedGroupKey.Detach();

        const ui64 groupKeyNonce = encryptionParams.GetGroupKeyNonce();

        TStreamCypher cypher;
        bool isKeySet = cypher.SetKey(static_cast<const TCypherKey&>(mainKey));
        Y_ABORT_UNLESS(isKeySet);
        cypher.StartMessage(groupKeyNonce, 0);
        cypher.Encrypt(destination, keyBytes, keySizeBytes);
        destination += keySizeBytes;
        cypher.Encrypt(destination, &h, sizeof(h));

        Y_ABORT_UNLESS(encryptedGroupKey.size() == groupKey.GetKeySizeBytes() + sizeof(ui32));

        // Send the request
        STLOG(PRI_DEBUG, BS_NODE, NW68, "ConfigureLocalProxy propose", (GroupId, groupId), (MainKey, mainKey));
        SendToController(std::make_unique<TEvBlobStorage::TEvControllerProposeGroupKey>(LocalNodeId, groupId,
            TBlobStorageGroupInfo::ELCP_PROPOSE, mainKey.Id, encryptedGroupKey, mainKey.Version, groupKeyNonce));
    }

    TEncryptionKey& TNodeWarden::GetGroupMainKey(ui32 groupId) {
        return TGroupID(groupId).ConfigurationType() == EGroupConfigurationType::Static
            ? Cfg->StaticKey
            : Cfg->TenantKey;
    }

    void TNodeWarden::ApplyGroupInfo(ui32 groupId, ui32 generation, const NKikimrBlobStorage::TGroupInfo *newGroup,
            bool fromController, bool fromResolver) {
        // if the group is marked as 'ejected', this is a race
        if (EjectedGroups.count(groupId)) {
            return;
        }

        // some basic consistency checks
        Y_ABORT_UNLESS(!newGroup || (newGroup->GetGroupID() == groupId && newGroup->GetGroupGeneration() == generation));

        // log if from resolver
        if (fromResolver) {
            STLOG(PRI_NOTICE, BS_NODE, NW73, "ApplyGroupInfo from resolver", (GroupId, groupId), (GroupGeneration, generation));
        }

        // obtain group record
        const auto [it, _] = Groups.try_emplace(groupId);
        TGroupRecord& group = it->second;
        group.MaxKnownGeneration = Max(group.MaxKnownGeneration, generation);
        if (newGroup) {
            const auto erasure = static_cast<TBlobStorageGroupType::EErasureSpecies>(newGroup->GetErasureSpecies());
            Y_DEBUG_ABORT_UNLESS(!group.GType || group.GType->GetErasure() == erasure);
            group.GType.emplace(erasure);
        }

        // forget pending queries
        if (fromController) {
            group.ProposeRequestPending = false;
        }
        group.GetGroupRequestPending = false;

        // update group content and encryption stuff
        bool groupChanged = false; // did the 'Group' field change somehow?
        if (newGroup) {
            auto& currentGroup = group.Group;

            // generate serialized string to compare it to the one after changes
            TString before;
            if (currentGroup) {
                const bool success = currentGroup->SerializeToString(&before);
                Y_ABORT_UNLESS(success);
            }

            // apply basic protobuf changes
            if (!currentGroup || currentGroup->GetGroupGeneration() < newGroup->GetGroupGeneration()) {
                currentGroup.emplace(*newGroup);
            }

            // apply encryption parameters from new protobuf
            auto& ep = group.EncryptionParams;
            Y_VERIFY_S(!ep.HasEncryptionMode() || ep.GetEncryptionMode() == newGroup->GetEncryptionMode(),
                "sudden EncryptionMode change from# " << static_cast<TBlobStorageGroupInfo::EEncryptionMode>(ep.GetEncryptionMode())
                << " to# " << static_cast<TBlobStorageGroupInfo::EEncryptionMode>(newGroup->GetEncryptionMode()) << " GroupId# " << groupId);

            if (!ep.HasEncryptionMode() || group.EncryptionParams.GetLifeCyclePhase() != TBlobStorageGroupInfo::ELCP_IN_USE) {
                // copy encryption mode and then copy other parameters if encryption is enabled
                ep.SetEncryptionMode(newGroup->GetEncryptionMode());
                if (ep.GetEncryptionMode() != TBlobStorageGroupInfo::EEM_NONE) {
                    ep.SetLifeCyclePhase(newGroup->GetLifeCyclePhase());
                    ep.SetMainKeyId(newGroup->GetMainKeyId());
                    ep.SetEncryptedGroupKey(newGroup->GetEncryptedGroupKey());
                    ep.SetGroupKeyNonce(newGroup->GetGroupKeyNonce());
                    ep.SetMainKeyVersion(newGroup->GetMainKeyVersion());

                    if (ep.GetLifeCyclePhase() == TBlobStorageGroupInfo::ELCP_IN_TRANSITION) {
                        // re-request group configuration for this group after some timeout
                        TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(TEvPrivate::EvGetGroup, 0,
                            SelfId(), {}, nullptr, groupId));
                    }
                }
            } else if (newGroup->GetLifeCyclePhase() == TBlobStorageGroupInfo::ELCP_IN_USE) {
                // validate encryption parameters -- they cannot change
                Y_ABORT_UNLESS(ep.GetMainKeyId() == newGroup->GetMainKeyId());
                Y_ABORT_UNLESS(ep.GetEncryptedGroupKey() == newGroup->GetEncryptedGroupKey());
                Y_ABORT_UNLESS(ep.GetGroupKeyNonce() == newGroup->GetGroupKeyNonce());
                Y_ABORT_UNLESS(ep.GetMainKeyVersion() == newGroup->GetMainKeyVersion());
            }

            // put encryption parameters overlay over the group proto
            currentGroup->MergeFrom(group.EncryptionParams);

            // check if group content has changed
            TString after;
            Y_ABORT_UNLESS(currentGroup);
            const bool success = currentGroup->SerializeToString(&after);
            Y_ABORT_UNLESS(success);
            groupChanged = before != after;

            if (groupChanged && Cfg->IsCacheEnabled() && TGroupID(groupId).ConfigurationType() == EGroupConfigurationType::Dynamic) {
                EnqueueSyncOp(WrapCacheOp(UpdateGroupInCache(*currentGroup)));
            }
        }

        if (const auto& currentGroup = group.Group; !currentGroup) {
            // we just do not have protobuf for the group, nothing else to do here
        } else if (currentGroup->GetGroupGeneration() != group.MaxKnownGeneration) {
            // we do not have relevant group configuration, but we know that there is one, so reset the configuration
            // for group/proxy and ask BSC for group info
            group.Info.Reset();
            group.NodeLayoutInfo.Reset();
            if (group.GroupResolver) {
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, group.GroupResolver, {}, nullptr, 0));
                group.GroupResolver = {};
            }
            RequestGroupConfig(groupId, group);
            if (group.ProxyId) {
                Send(group.ProxyId, new TEvBlobStorage::TEvConfigureProxy(nullptr, nullptr));
            }
        } else if (groupChanged) {
            // group has changed; obtain main encryption key for this group and try to parse group info from the protobuf
            auto& mainKey = GetGroupMainKey(groupId);
            TStringStream err;
            group.Info = TBlobStorageGroupInfo::Parse(*currentGroup, &mainKey, &err);
            if (group.Info->Type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc) {
                group.NodeLayoutInfo = MakeIntrusive<TNodeLayoutInfo>(NodeLocationMap[LocalNodeId], group.Info, NodeLocationMap);
            }
            Y_ABORT_UNLESS(group.EncryptionParams.HasEncryptionMode());
            if (const TString& s = err.Str()) {
                STLOG(PRI_ERROR, BS_NODE, NW19, "error while parsing group", (GroupId, groupId), (Err, s));
            }

            if (group.ProxyId) { // update configuration for running proxies
                auto info = NeedGroupInfo(groupId);
                auto counters = info
                    ? DsProxyPerPoolCounters->GetPoolCounters(info->GetStoragePoolName(), info->GetDeviceType())
                    : nullptr;

                if (info && info->BlobDepotId && !group.AgentProxy) {
                    // re-register proxy as an agent
                    group.AgentProxy = true;
                    TActorSystem *as = TActivationContext::ActorSystem();
                    group.ProxyId = Register(NBlobDepot::CreateBlobDepotAgent(groupId, info, group.ProxyId),
                        TMailboxType::ReadAsFilled, AppData()->SystemPoolId);
                    as->RegisterLocalService(MakeBlobStorageProxyID(groupId), group.ProxyId);
                }

                // forward ConfigureProxy anyway, because when we switch to BlobDepot agent, we still need to update
                // ds proxy configuration
                Send(group.ProxyId, new TEvBlobStorage::TEvConfigureProxy(std::move(info), group.NodeLayoutInfo,
                    std::move(counters)));
            }

            if (const auto& info = group.Info) {
                Send(WhiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateUpdate(info));
                for (auto& vdisk : group.VDisksOfGroup) {
                    UpdateGroupInfoForDisk(vdisk, info);
                }
            }

            if (const auto it = GroupPendingQueue.find(groupId); it != GroupPendingQueue.end()) {
                auto& queue = it->second;
                Y_ABORT_UNLESS(!queue.empty());

                if (!group.ProxyId) {
                    StartLocalProxy(groupId);
                }

                const auto& [timestamp, _] = queue.front();
                const size_t numErased = TimeoutToQueue.erase(std::make_tuple(timestamp, &*it));
                Y_ABORT_UNLESS(numErased == 1);

                for (auto& [timestamp, ev] : queue) {
                    THolder<IEventHandle> tmp(ev.release());
                    TActivationContext::Forward(tmp, ev->GetForwardOnNondeliveryRecipient());
                }

                GroupPendingQueue.erase(it);
            }
        }

        if (group.GroupResolver && group.Info) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, group.GroupResolver, {}, nullptr, 0));
            group.GroupResolver = {};
        }

        for (auto it = WorkingSyncers.lower_bound(TWorkingSyncer{.BridgeProxyGroupId = TGroupId::FromValue(groupId)});
                it != WorkingSyncers.end() && it->BridgeProxyGroupId.GetRawId() == groupId; ++it) {
            StartSyncerIfNeeded(const_cast<TWorkingSyncer&>(*it));
        }
    }

    void TNodeWarden::RequestGroupConfig(ui32 groupId, TGroupRecord& group) {
        STLOG(PRI_DEBUG, BS_NODE, NW98, "RequestGroupConfig", (GroupId, groupId));
        if (TGroupID(groupId).ConfigurationType() == EGroupConfigurationType::Static) {
            // do nothing, configs arrive through distributed configuration
        } else if (group.GetGroupRequestPending) {
            Y_ABORT_UNLESS(group.GroupResolver);
        } else {
            Y_ABORT_UNLESS(!group.GroupResolver);
            SendToController(std::make_unique<TEvBlobStorage::TEvControllerGetGroup>(LocalNodeId, &groupId, &groupId + 1));
            group.GroupResolver = RegisterWithSameMailbox(CreateGroupResolverActor(groupId));
            group.GetGroupRequestPending = true;
            Send(SelfId(), new TEvNodeWardenQueryCache(Sprintf("G%08" PRIx32, groupId), true));
        }
    }

    void TNodeWarden::ApplyGroupInfoFromServiceSet(const NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet) {
        for (const auto& group : serviceSet.GetGroups()) {
            const ui32 groupId = group.GetGroupID();
            if (group.GetEntityStatus() == NKikimrBlobStorage::DESTROY) {
                if (EjectedGroups.insert(groupId).second) {
                    TGroupRecord& group = Groups[groupId];
                    STLOG(PRI_DEBUG, BS_NODE, NW99, "destroying group", (GroupId, groupId), (ProxyId, group.ProxyId));
                    if (group.ProxyId) {
                        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, group.ProxyId, {}, nullptr, 0));
                    }
                    if (group.GroupResolver) {
                        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, group.GroupResolver, {}, nullptr, 0));
                    }
                    Groups.erase(groupId);
                    Send(SelfId(), new TEvNodeWardenUnsubscribeFromCache(Sprintf("G%08" PRIx32, groupId)));

                    // report group deletion to whiteboard
                    Send(WhiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateDelete(groupId));
                }
            } else {
                ApplyGroupInfo(groupId, group.GetGroupGeneration(), &group, true, false);
            }
        }
    }

    void TNodeWarden::Handle(TEvBlobStorage::TEvUpdateGroupInfo::TPtr ev) {
        auto *msg = ev->Get();
        bool fromResolver = false;
        if (const auto it = Groups.find(msg->GroupId.GetRawId()); it != Groups.end() && ev->Sender == it->second.GroupResolver) {
            it->second.GroupResolver = {};
            fromResolver = true;
        }
        ApplyGroupInfo(msg->GroupId.GetRawId(), msg->GroupGeneration, msg->GroupInfo ? &*msg->GroupInfo : nullptr, false, fromResolver);
    }

    void TNodeWarden::HandleGetGroup(TAutoPtr<IEventHandle> ev) {
        if (const auto it = Groups.find(ev->Cookie); it != Groups.end() &&
                it->second.EncryptionParams.GetLifeCyclePhase() == TBlobStorageGroupInfo::ELCP_IN_TRANSITION) {
            RequestGroupConfig(it->first, it->second);
        }
    }

    void TNodeWarden::ApplyWorkingSyncers(const NKikimrBlobStorage::TEvControllerNodeServiceSetUpdate& update) {
        std::set<TWorkingSyncer> toStop = WorkingSyncers;

        for (const auto& item : update.GetSyncers()) {
            using T = std::decay_t<decltype(item)>;
            const auto [it, inserted] = WorkingSyncers.emplace(TWorkingSyncer{
                .BridgeProxyGroupId = TGroupId::FromProto(&item, &T::GetBridgeProxyGroupId),
                .SourceGroupId = TGroupId::FromProto(&item, &T::GetSourceGroupId),
                .TargetGroupId = TGroupId::FromProto(&item, &T::GetTargetGroupId),
            });
            auto& syncer = const_cast<TWorkingSyncer&>(*it);
            syncer.PendingBridgeProxyGroupGeneration = item.GetBridgeProxyGroupGeneration();
            StartSyncerIfNeeded(syncer);
            toStop.erase(syncer);
        }

        for (const TWorkingSyncer& syncer : toStop) {
            if (syncer.ActorId) {
                STLOG(PRI_DEBUG, BS_NODE, NW65, "ApplyWorkingSyncers: stopping",
                    (BridgeProxyGroupId, syncer.BridgeProxyGroupId),
                    (BridgeProxyGroupGeneration, syncer.BridgeProxyGroupGeneration),
                    (SourceGroupId, syncer.SourceGroupId),
                    (TargetGroupId, syncer.TargetGroupId),
                    (PendingBridgeProxyGroupGeneration, syncer.PendingBridgeProxyGroupGeneration),
                    (ActorId, syncer.ActorId));
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, syncer.ActorId, {}, nullptr, 0));
            }
            WorkingSyncers.erase(syncer);
        }
    }

    void TNodeWarden::StartSyncerIfNeeded(TWorkingSyncer& syncer) {
        auto& group = Groups[syncer.BridgeProxyGroupId.GetRawId()];

        const bool stopCurrent = syncer.BridgeProxyGroupGeneration < syncer.PendingBridgeProxyGroupGeneration
            && syncer.ActorId;
        const bool startNew = (stopCurrent || !syncer.ActorId)
            && group.Info
            && group.Info->GroupGeneration == syncer.PendingBridgeProxyGroupGeneration;

        const ui32 prevBridgeProxyGroupGeneration = syncer.BridgeProxyGroupGeneration;
        const TActorId prevActorId = syncer.ActorId;

        if (stopCurrent) {
            // we've got already running syncer, but group generation gets changed, we need to restart it
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, syncer.ActorId, {}, nullptr, 0));
            syncer.ActorId = {};
            ++syncer.NumStop;
        }
        if (startNew) {
            syncer.BridgeProxyGroupGeneration = syncer.PendingBridgeProxyGroupGeneration;
            syncer.SyncerDataStats = std::make_unique<NBridge::TSyncerDataStats>();

            TBlobStorageGroupType sourceGroupType(TBlobStorageGroupType::ErasureNone);
            if (const auto it = Groups.find(syncer.SourceGroupId.GetRawId()); it != Groups.end() && it->second.GType) {
                sourceGroupType = *it->second.GType;
            } else {
                Y_DEBUG_ABORT("can't obtain source group type");
            }

            syncer.ActorId = Register(NBridge::CreateSyncerActor(group.Info, syncer.SourceGroupId, syncer.TargetGroupId,
                syncer.SyncerDataStats, SyncRateQuoter, sourceGroupType));
            syncer.Finished = false;
            syncer.ErrorReason.reset();
            ++syncer.NumStart;
        }
        if (stopCurrent || startNew) {
            STLOG(PRI_DEBUG, BS_NODE, NW64, "StartSyncerIfNeeded",
                (BridgeProxyGroupId, syncer.BridgeProxyGroupId),
                (PrevBridgeProxyGroupGeneration, prevBridgeProxyGroupGeneration),
                (BridgeProxyGroupGeneration, syncer.BridgeProxyGroupGeneration),
                (SourceGroupId, syncer.SourceGroupId),
                (TargetGroupId, syncer.TargetGroupId),
                (PendingBridgeProxyGroupGeneration, syncer.PendingBridgeProxyGroupGeneration),
                (PrevActorId, prevActorId),
                (ActorId, syncer.ActorId),
                (HasGroupInfo, static_cast<bool>(group.Info)),
                (GroupInfoGeneration, group.Info ? std::make_optional(group.Info->GroupGeneration) : std::nullopt),
                (StopCurrent, stopCurrent),
                (StartNew, startNew));
        }
    }

    void TNodeWarden::Handle(TEvNodeWardenNotifySyncerFinished::TPtr ev) {
        auto& msg = *ev->Get();
        const auto it = WorkingSyncers.find(TWorkingSyncer{
            .BridgeProxyGroupId = msg.BridgeProxyGroupId,
            .SourceGroupId = msg.SourceGroupId,
            .TargetGroupId = msg.TargetGroupId,
        });
        if (it == WorkingSyncers.end()) {
            return;
        }
        auto& syncer = const_cast<TWorkingSyncer&>(*it);
        if (msg.BridgeProxyGroupGeneration != syncer.BridgeProxyGroupGeneration) {
            return; // generation mismatch
        }

        syncer.Finished = true;
        syncer.ErrorReason = std::move(msg.ErrorReason);
        syncer.LastErrorReason = syncer.ErrorReason;
        syncer.ActorId = {};
        ++(syncer.ErrorReason ? syncer.NumFinishError : syncer.NumFinishOK);

        auto notify = std::make_unique<TEvBlobStorage::TEvControllerUpdateSyncerState>();
        FillInWorkingSyncer(&notify->Record, syncer, true);
        SendToController(std::move(notify));
    }

    bool TNodeWarden::FillInWorkingSyncer(NKikimrBlobStorage::TEvControllerUpdateSyncerState *update,
            TWorkingSyncer& syncer, bool forceProgress) {
        auto *item = update->AddSyncers();
        syncer.BridgeProxyGroupId.CopyToProto(item, &std::decay_t<decltype(*item)>::SetBridgeProxyGroupId);
        item->SetBridgeProxyGroupGeneration(syncer.PendingBridgeProxyGroupGeneration);
        syncer.SourceGroupId.CopyToProto(item, &std::decay_t<decltype(*item)>::SetSourceGroupId);
        syncer.TargetGroupId.CopyToProto(item, &std::decay_t<decltype(*item)>::SetTargetGroupId);
        if (syncer.Finished) {
            item->SetFinished(true);
        }
        if (syncer.ErrorReason) {
            item->SetErrorReason(*syncer.ErrorReason);
        }

        if (!syncer.SyncerDataStats) {
            return false; // hasn't started yet (maybe waiting for correct group infos)
        }

        // report syncer progress
        auto& stats = *syncer.SyncerDataStats;
        auto res = false;

#define ISSUE_METRIC(NAME) \
        const ui64 current##NAME = stats.NAME; \
        if (forceProgress || syncer.Reported##NAME != current##NAME) { \
            item->Set##NAME(current##NAME); \
            const_cast<TWorkingSyncer&>(syncer).Reported##NAME = current##NAME; \
            res = true; \
        }

        ISSUE_METRIC(BytesDone)
        ISSUE_METRIC(BytesTotal)
        ISSUE_METRIC(BytesError)
        ISSUE_METRIC(BlobsDone)
        ISSUE_METRIC(BlobsTotal)
        ISSUE_METRIC(BlobsError)

#undef ISSUE_METRIC

        return res;
    }

    void TNodeWarden::NotifySyncersProgress() {
        auto notify = std::make_unique<TEvBlobStorage::TEvControllerUpdateSyncerState>();
        bool doSend = false;
        for (const TWorkingSyncer& syncer : WorkingSyncers) {
            if (FillInWorkingSyncer(&notify->Record, const_cast<TWorkingSyncer&>(syncer), false)) {
                doSend = true;
            } else {
                notify->Record.MutableSyncers()->RemoveLast();
            }
        }
        if (doSend) {
            SendToController(std::move(notify));
        }
    }

} // NKikimr::NStorage
