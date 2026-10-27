#include "config.h"
#include "group_geometry_info.h"
#include <ydb/core/blobstorage/base/storage_pool_pile_override.h>
#include <ydb/core/base/nameservice.h>

namespace NKikimr::NBsController {

    TBlobStorageController::TStoragePoolInfo TBlobStorageController::ResolveStoragePoolForPile(
            const TBoxStoragePoolId& id, const TStoragePoolInfo& pool, TBridgePileId pileId,
            const TBridgeInfo *bridgeInfo, const NKikimrBlobStorage::TStorageConfig *config) {
        auto result = pool;
        // Ordinary groups can span piles. Only physical parts with a pile identity use overrides.
        if (pool.DDisk || !pileId || !bridgeInfo || !config) {
            return result;
        }
        const auto *pile = bridgeInfo->FindPile(pileId);
        if (!pile) {
            return result;
        }
        const auto *rule = FindStoragePoolPileOverride(config->GetBlobStorageConfig(), pile->Name, pool.Kind);
        if (!rule) {
            return result;
        }
        // Keep the base pool intact: Console may later resend it with a different group count.
        NKikimrBlobStorage::TDefineStoragePool effective;
        Serialize(&effective, id, pool);
        ApplyStoragePoolPileOverride(effective, *rule);
        if (!TBlobStorageGroupType::ParseErasureName(result.ErasureSpecies, effective.GetErasureSpecies())) {
            throw TExError() << "invalid ErasureSpecies in storage pool pile override";
        }
        result.SetGroupGeometry(effective.GetGeometry());
        result.PDiskFilters.clear();
        for (const auto& filter : effective.GetPDiskFilter()) {
            result.PDiskFilters.insert(TStoragePoolInfo::TPDiskFilter::Parse(std::get<0>(id), std::get<1>(id), filter));
        }
        return result;
    }

    TBlobStorageController::TStoragePoolInfo TBlobStorageController::GetStoragePoolForPile(
            const TBoxStoragePoolId& id, TBridgePileId pileId) const {
        return ResolveStoragePoolForPile(id, StoragePools.at(id), pileId, BridgeInfo.get(), StorageConfig.get());
    }

    std::optional<TString> TBlobStorageController::ValidateStoragePoolPileOverrides(
            const NKikimrBlobStorage::TStorageConfig& config) const {
        if (auto error = NKikimr::ValidateStoragePoolPileOverrides(config.GetBlobStorageConfig())) {
            return error;
        }
        try {
            std::map<std::pair<TBoxStoragePoolId, TBridgePileId>, TGroupGeometryInfo> geometries;
            for (const auto& [groupId, group] : GroupMap) {
                if (!group->BridgePileId || group->DDisk || group->VDisksInGroup.empty()) {
                    continue;
                }
                const auto [it, inserted] = geometries.try_emplace(std::make_pair(group->StoragePoolId, group->BridgePileId));
                if (inserted) {
                    const auto pool = ResolveStoragePoolForPile(group->StoragePoolId, StoragePools.at(group->StoragePoolId),
                        group->BridgePileId, BridgeInfo.get(), &config);
                    it->second = TGroupGeometryInfo(TBlobStorageGroupType(pool.ErasureSpecies), pool.GetGroupGeometry());
                }
                const auto& geometry = it->second;
                if (group->ErasureSpecies != geometry.GetErasure()
                        || group->Topology->GetNumVDisksPerFailDomain() != geometry.GetNumVDisksPerFailDomain()
                        || group->Topology->GetTotalFailRealmsNum() != geometry.GetNumFailRealms()
                        || group->Topology->GetNumFailDomainsPerFailRealm() != geometry.GetNumFailDomainsPerFailRealm()) {
                    return TStringBuilder() << "storage pool pile override changes erasure or topology of existing GroupId# "
                        << groupId;
                }
            }
        } catch (const std::exception& ex) {
            return TStringBuilder() << "invalid storage pool pile override: " << ex.what();
        }
        return std::nullopt;
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDefineStoragePool& cmd, TStatus& status) {
        TBoxId boxId = cmd.GetBoxId();
        if (!cmd.HasBoxId()) {
            if (Boxes.Get().size() == 1) {
                boxId = Boxes.Get().begin()->first;
            } else {
                throw TExError() << "BoxId is required when there is not exactly one box";
            }
        }

        ui64 storagePoolId = cmd.GetStoragePoolId();
        if (!storagePoolId) {
            ui64 maxPoolId = 0;

            // TODO: optimize linear search

            const auto &pools = StoragePools.Get();
            for (auto it = pools.lower_bound({boxId, 0});
                 it != pools.end() && std::get<0>(it->first) == boxId;
                 ++it) {
                const ui64 id = std::get<1>(it->first);
                const TStoragePoolInfo &info = it->second;

                if (info.Name == cmd.GetName()) {
                    if (!id) {
                        throw TExError() << "StoragePoolId# 0 for Name# " << info.Name;
                    } else if (storagePoolId) {
                        throw TExError() << "Storage pool Name# " << info.Name << " is ambiguous";
                    } else {
                        storagePoolId = id;
                    }
                }

                maxPoolId = Max(maxPoolId, id);
            }

            if (!storagePoolId) {
                auto& nextStoragePoolId = NextStoragePoolId.Unshare();
                storagePoolId = Max(nextStoragePoolId, maxPoolId + 1);
                status.SetAssignedStoragePoolId(storagePoolId);
                nextStoragePoolId = storagePoolId + 1;
            }
        }

        const TBoxStoragePoolId id(boxId, storagePoolId);
        const ui64 nextGen = CheckGeneration(cmd, StoragePools.Get(), id);

        const auto current = StoragePools.Get().find(id);
        const bool existing = current != StoragePools.Get().end();
        const bool defaultBridgeMode = BridgeInfo && !BridgeInfo->IsImplicit;
        const bool bridgeMode = existing ? current->second.BridgeMode
            : cmd.HasBridgeMode() ? cmd.GetBridgeMode() : defaultBridgeMode;
        if (existing && cmd.HasBridgeMode() && cmd.GetBridgeMode() != bridgeMode) {
            throw TExError() << "can't change storage pool BridgeMode";
        }
        if (!existing && bridgeMode != defaultBridgeMode) {
            const auto& details = Self.StorageConfig
                ? Self.StorageConfig->GetClusterStateDetails()
                : NKikimrBridge::TClusterStateDetails::default_instance();
            if (!defaultBridgeMode || details.GetBscConversionPending()) {
                throw TExError() << "BridgeMode override requires a bridged cluster with BSC conversion complete";
            }
            if (!details.GetNonUniformStorageEnabled()) {
                throw TExError() << "BridgeMode override requires bridge_config.enable_non_uniform_storage";
            }
        }

        TStoragePoolInfo storagePool;
        storagePool.Name = cmd.GetName();
        storagePool.Generation = nextGen;

        const TString &erasureSpecies = cmd.GetErasureSpecies();
        if (!TBlobStorageGroupType::ParseErasureName(storagePool.ErasureSpecies, erasureSpecies)) {
            throw TExError() << "invalid ErasureSpecies# " << erasureSpecies;
        }

        // Resolve the default only at creation; updates preserve the stored mode.
        storagePool.BridgeMode = bridgeMode;

        if (cmd.HasGeometry()) {
            storagePool.SetGroupGeometry(cmd.GetGeometry());
        }

        // parse VDisk kind
        {
            const TString &kind = cmd.GetVDiskKind();
            const google::protobuf::EnumDescriptor *descr = NKikimrBlobStorage::TVDiskKind::EVDiskKind_descriptor();
            const google::protobuf::EnumValueDescriptor *value = descr->FindValueByName(kind);
            if (!value) {
                throw TExError() << "invalid VDiskKind# " << kind;
            }
            storagePool.VDiskKind = static_cast<NKikimrBlobStorage::TVDiskKind::EVDiskKind>(value->number());
        }

        if (cmd.HasUsagePattern()) {
            const auto &usage = cmd.GetUsagePattern();
            storagePool.SpaceBytes = usage.GetSpaceBytes();
            storagePool.WriteIOPS = usage.GetWriteIOPS();
            storagePool.WriteBytesPerSecond = usage.GetWriteBytesPerSecond();
            storagePool.ReadIOPS = usage.GetReadIOPS();
            storagePool.ReadBytesPerSecond = usage.GetReadBytesPerSecond();
            storagePool.InMemCacheBytes = usage.GetInMemCacheBytes();
        }

        storagePool.Kind = cmd.GetKind();
        storagePool.NumGroups = cmd.GetNumGroups();
        storagePool.EncryptionMode = cmd.GetEncryptionMode();
        storagePool.RandomizeGroupMapping = cmd.GetRandomizeGroupMapping();
        storagePool.DefaultGroupSizeInUnits = cmd.GetDefaultGroupSizeInUnits();

        for (const auto &userId : cmd.GetUserId()) {
            storagePool.UserIds.emplace(boxId, storagePoolId, userId);
        }

        for (const auto &item : cmd.GetPDiskFilter()) {
            storagePool.PDiskFilters.insert(TStoragePoolInfo::TPDiskFilter::Parse(boxId, storagePoolId, item));
        }

        if (cmd.HasExistingGroups()) {
            throw TExError() << "migration is not supported anymore";
        }

        if (cmd.HasScopeId()) {
            const auto& pb = cmd.GetScopeId();
            const TScopeId scopeId(pb.GetX1(), pb.GetX2());
            TKikimrScopeId x(scopeId);
            storagePool.SchemeshardId = x.GetSchemeshardId();
            storagePool.PathItemId = x.GetPathItemId();
        }

        const auto& spToGroups = StoragePoolGroups.Get();
        std::map<TBridgePileId, TBlobStorageGroupType::EErasureSpecies> erasures;
        for (auto it = spToGroups.lower_bound({id, Min<TGroupId>()}); it != spToGroups.end() && it->first == id; ++it) {
            const TGroupInfo *group = Groups.Find(it->second);
            Y_ABORT_UNLESS(group);
            // inline pile keeps its own VDisks, so it must match the pool geometry like any physical group
            if (group->BridgeGroupInfo && !group->IsInlinePile()) {
                continue;
            }
            const auto [erasure, inserted] = erasures.try_emplace(group->BridgePileId);
            if (inserted) {
                erasure->second = Self.ResolveStoragePoolForPile(id, storagePool, group->BridgePileId,
                    BridgeInfo.get(), StorageConfig.get()).ErasureSpecies;
            }
            if (group->ErasureSpecies != erasure->second) {
                throw TExError() << "GroupId# " << it->second << " has different erasure species";
            }
        }

        auto &storagePools = StoragePools.Unshare();
        if (const auto [spIt, inserted] = storagePools.try_emplace(id, std::move(storagePool)); !inserted) {
            TStoragePoolInfo& cur = spIt->second;
            if (cur.DDisk) {
                throw TExError() << "can't invoke DefineStoragePool against DDisk pool";
            }
            if (cur.SchemeshardId != storagePool.SchemeshardId || cur.PathItemId != storagePool.PathItemId) {
                for (auto it = spToGroups.lower_bound({id, Min<TGroupId>()}); it != spToGroups.end() && it->first == id; ++it) {
                    GroupContentChanged.insert(it->second);
                }
            }
            // retain some fields; TStoragePoolSettings are changed by UpdateStoragePoolSettings only
            storagePool.VDiskHeapAllocatorNumLeadingDisks = cur.VDiskHeapAllocatorNumLeadingDisks;
            cur = std::move(storagePool); // update existing storage pool
        }
        Fit.PoolsAndGroups.emplace(id, std::nullopt);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TReadStoragePool& cmd, TStatus& status) {
        const bool queryAllBoxes = cmd.HasBoxId() && cmd.GetBoxId() == Max<TBoxId>();

        TBoxId boxId = cmd.GetBoxId();
        if (!queryAllBoxes && !cmd.HasBoxId()) {
            if (Boxes.Get().size() == 1) {
                boxId = Boxes.Get().begin()->first;
            } else {
                throw TExError() << "BoxId is required when there is not exactly one box";
            }
        }

        // create set of allowed storage pool ids to query
        const auto& ids = cmd.GetStoragePoolId();
        THashSet<TBoxStoragePoolId> storagePoolIds;
        if (!ids.empty()) {
            if (queryAllBoxes) {
                throw TExError() << "StoragePoolId may only be specified when BoxId is defined";
            } else {
                for (const auto storagePoolId : ids) {
                    storagePoolIds.emplace(boxId, storagePoolId);
                }
            }
        }

        const auto& names = cmd.GetName();
        THashSet<TString> nameSet(names.begin(), names.end());

        // calculate lower and upper bounds for search
        const TBoxStoragePoolId since(queryAllBoxes ? Min<Schema::BoxStoragePool::BoxId::Type>() : boxId,
            Min<Schema::BoxStoragePool::StoragePoolId::Type>());
        const TBoxStoragePoolId till(queryAllBoxes ? Max<Schema::BoxStoragePool::BoxId::Type>() : boxId,
            Max<Schema::BoxStoragePool::StoragePoolId::Type>());

        // iterate through subset and add only requested entities
        const auto &storagePools = StoragePools.Get();
        for (auto it = storagePools.lower_bound(since); it != storagePools.end() && it->first <= till; ++it) {
            if (it->second.DDisk) {
                continue; // skip DDisk pool
            }
            if ((!storagePoolIds || storagePoolIds.contains(it->first)) && (!nameSet || nameSet.contains(it->second.Name))) {
                Serialize(status.AddStoragePool(), it->first, it->second);
            }
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDeleteStoragePool& cmd, TStatus& /*status*/) {
        const TBoxStoragePoolId id(cmd.GetBoxId(), cmd.GetStoragePoolId());
        CheckGeneration(cmd, StoragePools.Get(), id);
        auto &storagePools = StoragePools.Unshare();
        const auto it = storagePools.find(id);
        if (it == storagePools.end()) {
            throw TExError() << "StoragePoolId# " << id << " not found";
        } else if (it->second.DDisk) {
            throw TExError() << "can't invoke DeleteStoragePool against DDisk pool";
        }

        // collect groups first, as DeleteExistingGroup unbinds each of them from the pool
        std::vector<TGroupId> groups;
        const auto& storagePoolGroups = StoragePoolGroups.Get();
        for (auto it = storagePoolGroups.lower_bound({id, Min<TGroupId>()});
                it != storagePoolGroups.end() && it->first == id; ++it) {
            groups.push_back(it->second);
        }

        for (const TGroupId groupId : groups) {
            if (const TGroupInfo *groupInfo = Groups.Find(groupId)) {
                DeleteExistingGroupWithVSlots(*groupInfo);
            } else {
                throw TExError() << "GroupId# " << groupId << " not found";
            }
        }

        storagePools.erase(id);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TUpdateStoragePoolSettings& cmd, TStatus& /*status*/) {
        const TBoxStoragePoolId id(cmd.GetBoxId(), cmd.GetStoragePoolId());
        const auto it = StoragePools.Get().find(id);
        if (it == StoragePools.Get().end()) {
            throw TExError() << "StoragePoolId# " << id << " not found";
        } else if (it->second.DDisk) {
            throw TExError() << "can't invoke UpdateStoragePoolSettings against DDisk pool";
        }

        NKikimrBlobStorage::TStoragePoolSettings settings;
        Serialize(&settings, it->second);
        const auto *descriptor = settings.GetDescriptor();
        const auto *reflection = settings.GetReflection();
        for (const auto& name : cmd.GetReset()) {
            const auto *field = descriptor->FindFieldByName(name);
            if (!field) {
                throw TExError() << "unknown setting# " << name;
            } else if (reflection->HasField(cmd.GetSettings(), field)) {
                throw TExError() << "setting# " << name << " is both set and reset";
            }
            reflection->ClearField(&settings, field);
        }
        settings.MergeFrom(cmd.GetSettings());

        const TMaybe<ui32> numLeadingDisks = settings.HasVDiskHeapAllocatorNumLeadingDisks()
            ? MakeMaybe(settings.GetVDiskHeapAllocatorNumLeadingDisks())
            : Nothing();
        if (numLeadingDisks != it->second.VDiskHeapAllocatorNumLeadingDisks) {
            StoragePools.Unshare().at(id).VDiskHeapAllocatorNumLeadingDisks = numLeadingDisks;
            HeapAllocatorNumLeadingDisksChanged.insert(id);
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TChangeStoragePoolBridgeMode& cmd,
            TStatus& /*status*/) {
        const TBoxStoragePoolId id(cmd.GetBoxId(), cmd.GetStoragePoolId());
        const auto it = StoragePools.Get().find(id);
        if (it == StoragePools.Get().end()) {
            throw TExError() << "StoragePoolId# " << id << " not found";
        } else if (it->second.DDisk) {
            throw TExError() << "can't change BridgeMode of DDisk pool";
        }
        const ui64 nextGen = CheckGeneration(cmd, StoragePools.Get(), id);

        const auto& details = Self.StorageConfig
            ? Self.StorageConfig->GetClusterStateDetails()
            : NKikimrBridge::TClusterStateDetails::default_instance();
        if (!BridgeInfo || BridgeInfo->IsImplicit || details.GetBscConversionPending()) {
            throw TExError() << "BridgeMode change requires a bridged cluster with BSC conversion complete";
        } else if (!details.GetNonUniformStorageEnabled()) {
            throw TExError() << "BridgeMode change requires bridge_config.enable_non_uniform_storage";
        } else if (details.HasPendingPileOperation()) {
            throw TExError() << "BridgeMode can't be changed while the set of piles is being changed";
        } else if (it->second.BridgeMode == cmd.GetBridgeMode()) {
            return; // the pool has this mode already
        }

        // new groups and parts of the pool are placed by its new mode
        auto& pool = StoragePools.Unshare().at(id);
        pool.BridgeMode = cmd.GetBridgeMode();
        pool.Generation = nextGen;

        if (cmd.GetBridgeMode()) {
            MakeStoragePoolGroupsBridged(id);
        } else {
            MakeStoragePoolGroupsOrdinary(id, cmd.GetRetainPileName());
        }
        Callbacks.push_back([&self = Self] { self.CheckBridgeGroupModes(); });
    }

    template<typename TCallback>
    void TBlobStorageController::TConfigState::ForEachStoragePoolGroup(TBoxStoragePoolId storagePoolId,
            TCallback&& callback) {
        const auto& storagePoolGroups = StoragePoolGroups.Get();
        for (auto it = storagePoolGroups.lower_bound({storagePoolId, Min<TGroupId>()});
                it != storagePoolGroups.end() && it->first == storagePoolId; ++it) {
            const TGroupInfo *group = Groups.Find(it->second);
            Y_ABORT_UNLESS(group);
            callback(*group);
        }
    }

    void TBlobStorageController::TConfigState::MakeStoragePoolGroupsBridged(TBoxStoragePoolId storagePoolId) {
        // ordinary groups become inline piles in the piles they reside in
        ForEachStoragePoolGroup(storagePoolId, [&](const TGroupInfo& group) {
            if (group.BridgeGroupInfo || group.BridgePileId) {
                return; // the group is bridged already, or it is a part of bridged one
            } else if (group.BlocksBridgeConversion()) {
                throw TExError() << "GroupId# " << group.ID << " is being decommitted or recommissioned, it can't be"
                    " made bridged";
            } else if (!group.IsBridgeConversionCandidate()) {
                return; // virtual group has its data in other groups
            }
            std::optional<TBridgePileId> pileId;
            for (const TVSlotInfo *vslot : group.VDisksInGroup) {
                const auto *pile = BridgeInfo->GetPileForNode(vslot->VSlotId.NodeId);
                if (!pile) {
                    throw TExError() << "GroupId# " << group.ID << " has VDisk on NodeId# " << vslot->VSlotId.NodeId
                        << " which belongs to no pile";
                } else if (pileId && *pileId != pile->BridgePileId) {
                    throw TExError() << "GroupId# " << group.ID << " resides in several piles, it can't be made"
                        " bridged";
                }
                pileId = pile->BridgePileId;
            }
            Groups.FindForUpdate(group.ID)->MakeInlinePile(*pileId);
            GroupContentChanged.insert(group.ID);
        });

        // bridged groups get their parts in every pile of the cluster; they rely on the parts they have until these
        // parts are synced (the ones of the groups being made ordinary just before are not dropped then, they get
        // synced too); parts are created right away, so that the change fails when there are not enough resources
        std::vector<TBridgeGroupPart> parts;
        ForEachStoragePoolGroup(storagePoolId, [&](const TGroupInfo& group) {
            if (!group.BridgeGroupInfo) {
                return; // virtual group, or a part of bridged one
            }
            const auto& state = group.BridgeGroupInfo->GetBridgeGroupState();
            bool complete = true;
            for (const auto& pile : BridgeInfo->Piles) {
                if (pile.State == NKikimrBridge::TClusterState::INACTIVE) {
                    continue;
                } else if (const auto *part = NBridge::GetGroupPile(state, pile.BridgePileId); !part) {
                    const auto *pss = Self.FindPileSyncState(pile.BridgePileId);
                    parts.push_back({group.ID, pile.BridgePileId, pss ? pss->GetBecameUnsyncedGeneration() : 0});
                    complete = false;
                } else if (part->GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                    complete = false;
                }
            }
            if (complete) {
                return; // nothing to sync
            }
            // the group of ordinary pool has the only part, or the one it keeps while being made ordinary
            const auto authorityPileId = NBridge::GetAuthorityPileId(state);
            if (!authorityPileId) {
                throw TExError() << "GroupId# " << group.ID << " has no part it relies on";
            }
            auto *mutableState = Groups.FindForUpdate(group.ID)->BridgeGroupInfo->MutableBridgeGroupState();
            authorityPileId.CopyToProto(mutableState, &NKikimrBridge::TGroupState::SetAuthorityPileId);
            GroupContentChanged.insert(group.ID);
        });
        std::vector<std::tuple<TGroupId, TGroupId>> added;
        Self.AddBridgeGroupParts(*this, parts, &added);

        // parts get synced from the authority ones, unsynced parts of the groups being made ordinary get synced as well
        std::vector<std::tuple<TGroupId, TGroupId>> targets;
        ForEachStoragePoolGroup(storagePoolId, [&](const TGroupInfo& group) {
            if (group.BridgeGroupInfo && group.BridgeGroupInfo->GetBridgeGroupState().GetAuthorityPileId()) {
                NBridge::ForEachGroupPile(group.BridgeGroupInfo->GetBridgeGroupState(), [&](TBridgePileId,
                        const NKikimrBridge::TGroupState::TPile& pile) {
                    if (pile.GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                        targets.emplace_back(group.ID, TGroupId::FromProto(&pile,
                            &NKikimrBridge::TGroupState::TPile::GetGroupId));
                    }
                });
            }
        });
        Callbacks.push_back([&self = Self, targets = std::move(targets)] {
            for (const auto& [proxyGroupId, targetGroupId] : targets) {
                const auto [it, inserted] = self.TargetGroupToSyncerState.try_emplace(targetGroupId, proxyGroupId,
                    targetGroupId);
                if (inserted) {
                    self.SyncersRequiringAction.PushBack(&it->second);
                }
                // the part is not going to be deleted anymore, it gets the whole delay when dropped again
                self.DroppedBridgePartsSince.erase(targetGroupId);
            }
        });
    }

    void TBlobStorageController::TConfigState::MakeStoragePoolGroupsOrdinary(TBoxStoragePoolId storagePoolId,
            const TString& retainPileName) {
        std::optional<TBridgePileId> retainPileId;
        if (retainPileName) {
            for (const auto& pile : BridgeInfo->Piles) {
                if (!pile.IsFreeSlot && pile.Name == retainPileName) {
                    retainPileId = pile.BridgePileId;
                }
            }
            if (!retainPileId) {
                throw TExError() << "pile " << retainPileName << " not found";
            }
        }

        // every bridged group relies on the part it keeps from now on, the others are deleted afterwards
        ForEachStoragePoolGroup(storagePoolId, [&](const TGroupInfo& group) {
            if (!group.BridgeGroupInfo) {
                return; // ordinary or virtual group, or a part of bridged one
            }
            const auto& state = group.BridgeGroupInfo->GetBridgeGroupState();
            const TBridgePileId authorityPileId = NBridge::GetAuthorityPileId(state);
            TBridgePileId pileId;
            if (retainPileId) {
                pileId = *retainPileId;
            } else if (authorityPileId) {
                pileId = authorityPileId;
            } else if (group.IsInlinePile()) {
                pileId = group.BridgePileId;
            } else if (BridgeInfo->PrimaryPile) {
                pileId = BridgeInfo->PrimaryPile->BridgePileId;
            }
            const auto *part = NBridge::GetGroupPile(state, pileId);
            const auto *pile = BridgeInfo->FindPile(pileId);
            if (!part || !pile) {
                throw TExError() << "GroupId# " << group.ID << " has no part in BridgePileId# " << pileId;
            } else if (part->GetStage() != NKikimrBridge::TGroupState::SYNCED ||
                    (pileId != authorityPileId && pile->State != NKikimrBridge::TClusterState::SYNCHRONIZED) ||
                    !NBridge::PileStateTraits(pile->State).RequiresDataQuorum) {
                throw TExError() << "GroupId# " << group.ID << " part in pile " << pile->Name << " is not synchronized";
            } else if (pileId == authorityPileId && !state.GetAuthorityPileId()) {
                return; // the only part of the group
            }
            auto *mutableState = Groups.FindForUpdate(group.ID)->BridgeGroupInfo->MutableBridgeGroupState();
            pileId.CopyToProto(mutableState, &NKikimrBridge::TGroupState::SetAuthorityPileId);
            GroupContentChanged.insert(group.ID);
        });
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TProposeStoragePools& /*cmd*/, TStatus& status) {
        // first, scan through all existing groups that are not defined as a part of storage pools and check
        // their respective VDisks; as a first step, prepare set of groups inside storage pools
        THashSet<TGroupId> storagePoolGroups;
        for (const auto &kv : StoragePoolGroups.Get()) {
            const bool inserted = storagePoolGroups.insert(kv.second).second;
            Y_ABORT_UNLESS(inserted);
        }

        using TProperties = std::tuple<TBoxId,
              Schema::VSlot::Category::Type,
              Schema::PDisk::SharedWithOs::Type,
              Schema::PDisk::ReadCentric::Type,
              Schema::PDisk::Category::Type,
              Schema::Group::ErasureSpecies::Type>;

        THashMap<TProperties, TVector<TGroupId>> groupMap;

        // second, scan all groups and check those who are not in SPs
        Groups.ForEach([&](TGroupId groupId, const TGroupInfo& groupInfo) {
            if (storagePoolGroups.contains(groupId)) {
                return;
            }

            TMaybe<TBoxId> boxId;
            TMaybe<Schema::VSlot::Category::Type> vdiskKind;
            TMaybe<Schema::PDisk::SharedWithOs::Type> sharedWithOs;
            TMaybe<Schema::PDisk::ReadCentric::Type> readCentric;
            TMaybe<Schema::PDisk::Category::Type> category;

            for (const TVSlotInfo *vslot : groupInfo.VDisksInGroup) {
                auto updateProperty = [&](auto &dest, const auto &source, const char *name) {
                    if (!source) {
                        throw TExError() << "GroupId# " << groupId << " VSlot# " << vslot->VSlotId
                            << " " << name << " property not set";
                    } else if (!dest) {
                        dest = *source;
                    } else if (*dest != *source) {
                        throw TExError() << "GroupId# " << groupId << " VSlot# " << vslot->VSlotId
                            << " has different comprising VDisks/PDisks " << name << " property value";
                    }
                };

                updateProperty(vdiskKind, TMaybe<Schema::VSlot::Category::Type>(vslot->Kind), "VDiskKind");
                updateProperty(sharedWithOs, vslot->PDisk->SharedWithOs, "SharedWithOs");
                updateProperty(readCentric, vslot->PDisk->ReadCentric, "ReadCentric");
                updateProperty(category, TMaybe<Schema::PDisk::Category::Type>(vslot->PDisk->Kind), "PDisk Kind/Type");

                // find box that holds this PDisk
                updateProperty(boxId, MakeMaybe(vslot->PDisk->BoxId), "BoxId");
            }

            Y_ABORT_UNLESS(boxId && vdiskKind && sharedWithOs && readCentric && category);

            const TProperties key(*boxId, *vdiskKind, *sharedWithOs, *readCentric, *category, groupInfo.ErasureSpecies);
            groupMap[key].push_back(groupId);
        });

        THashMap<TBoxId, Schema::BoxStoragePool::StoragePoolId::Type> storagePoolIdPerBox;
        for (const auto &kv : groupMap) {
            TBoxId boxId;
            Schema::VSlot::Category::Type vdiskKind;
            Schema::PDisk::SharedWithOs::Type sharedWithOs;
            Schema::PDisk::ReadCentric::Type readCentric;
            Schema::PDisk::Category::Type pdiskCategory;
            Schema::Group::ErasureSpecies::Type erasure;
            std::tie(boxId, vdiskKind, sharedWithOs, readCentric, pdiskCategory, erasure) = kv.first;

            Schema::BoxStoragePool::StoragePoolId::Type storagePoolId = 1;

            auto it = storagePoolIdPerBox.find(boxId);
            if (it == storagePoolIdPerBox.end()) {
                const auto &storagePools = StoragePools.Get();
                auto it = storagePools.upper_bound(std::make_tuple(boxId,
                    Max<Schema::BoxStoragePool::StoragePoolId::Type>()));
                if (it != storagePools.begin() && std::get<0>((--it)->first) == boxId) {
                    storagePoolId = std::get<1>(it->first);
                }
                storagePoolIdPerBox[boxId] = storagePoolId + 1;
            } else {
                storagePoolId = it->second++;
            }

            const TPDiskCategory category(pdiskCategory);

            TString name = Sprintf("autogenerated %" PRIu64 ":%" PRIu64, boxId, storagePoolId);

            auto *sp = status.AddStoragePool();
            sp->SetBoxId(boxId);
            sp->SetStoragePoolId(storagePoolId);
            sp->SetName(name);
            sp->SetErasureSpecies(TBlobStorageGroupType::ErasureSpeciesName(erasure));
            sp->SetVDiskKind(NKikimrBlobStorage::TVDiskKind::EVDiskKind_Name(vdiskKind));
            sp->SetNumGroups(kv.second.size());

            auto *filter = sp->AddPDiskFilter();
            filter->AddProperty()->SetType(PDiskTypeToPDiskType(category.Type()));
            filter->AddProperty()->SetSharedWithOs(sharedWithOs);
            filter->AddProperty()->SetReadCentric(readCentric);
            filter->AddProperty()->SetKind(category.Kind());

            auto *existing = sp->MutableExistingGroups();
            for (TGroupId groupId : kv.second) {
                existing->AddGroupId(groupId.GetRawId());
            }
        }

        // add any existing storage pools to the resulting set
        NKikimrBlobStorage::TReadStoragePool query;
        query.SetBoxId(Max<ui64>());
        ExecuteStep(query, status);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TReassignGroupDisk& cmd,
            NKikimrBlobStorage::TConfigResponse::TStatus& /*status*/) {
        // find matching TVSlotInfo entity
        const TVDiskID vdiskId(TGroupId::FromProto(&cmd, &NKikimrBlobStorage::TReassignGroupDisk::GetGroupId),
            cmd.GetGroupGeneration(), cmd.GetFailRealmIdx(), cmd.GetFailDomainIdx(), cmd.GetVDiskIdx());

        // validate group and generation
        const TGroupInfo *group = Groups.Find(TGroupId::FromProto(&cmd, &NKikimrBlobStorage::TReassignGroupDisk::GetGroupId));
        if (!group) {
            throw TExError() << "GroupId# " << cmd.GetGroupId() << " not found";
        } else if (group->Generation != cmd.GetGroupGeneration()) {
            throw TExError() << "GroupId# " << cmd.GetGroupId() << " generation mismatch";
        } else if (!group->Topology->IsValidId(vdiskId)) {
            throw TExError() << "VDiskId# " << vdiskId << " out of range";
        }

        const ui32 orderNumber = group->Topology->GetOrderNumber(vdiskId);
        const TVSlotId vslotId = group->VDisksInGroup[orderNumber]->VSlotId;

        TPDiskId targetPDiskId;
        if (cmd.HasTargetPDiskId()) {
            const NKikimrBlobStorage::TPDiskId& proto = cmd.GetTargetPDiskId();
            const TPDiskId pdiskId(proto.GetNodeId(), proto.GetPDiskId());
            if (!PDisks.Find(pdiskId)) {
                throw TExError() << "TargetPDiskId# " << pdiskId.ToString() << " not found";
            }
            targetPDiskId = pdiskId;
        }

        ExplicitReconfigureMap.emplace(vslotId, targetPDiskId);
        if (cmd.GetSuppressDonorMode()) {
            SuppressDonorMode.insert(vslotId);
        }

        if (cmd.GetOnlyToLessOccupiedPDisk()) {
            Fit.OnlyToLessOccupiedPDisk = true;
        }

        if (cmd.GetPreferLessOccupiedRack()) {
            Fit.PreferLessOccupiedRack = true;
        }

        if (cmd.GetWithAttentionToReplication()) {
            Fit.WithAttentionToReplication = true;
        }

        Fit.PoolsAndGroups.emplace(group->StoragePoolId, group->ID);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TMoveGroups& cmd, TStatus& /*status*/) {
        auto& storagePools = StoragePools.Unshare();

        const TBoxStoragePoolId originId(cmd.GetBoxId(), cmd.GetOriginStoragePoolId());
        const TBoxStoragePoolId targetId(cmd.GetBoxId(), cmd.GetTargetStoragePoolId());

        auto getPool = [&](TBoxStoragePoolId poolId, ui64 poolGeneration, const char *name) -> TStoragePoolInfo& {
            if (auto it = storagePools.find(poolId); it != storagePools.end()) {
                TStoragePoolInfo& pool = it->second;
                const ui64 generation = pool.Generation.GetOrElse(1);
                if (generation != poolGeneration) {
                    throw TExError() << name << " StoragePoolId# " << poolId << " Generation# " << generation
                        << " does not match expected Generation# " << poolGeneration;
                }
                pool.Generation = generation + 1;
                return pool;
            } else {
                throw TExError() << name << " StoragePoolId# " << poolId << " not found";
            }
        };

        // find the origin and the target storage pools
        TStoragePoolInfo& origin = getPool(originId, cmd.GetOriginStoragePoolGeneration(), "origin");
        TStoragePoolInfo& target = getPool(targetId, cmd.GetTargetStoragePoolGeneration(), "target");

        if (origin.DDisk || target.DDisk) {
            throw TExError() << "can't move groups between DDisk pools";
        }
        if (origin.BridgeMode != target.BridgeMode) {
            throw TExError() << "can't move groups between pools with different BridgeMode";
        }

        auto& storagePoolGroups = StoragePoolGroups.Unshare();

        // create a list of groups to be moved
        const auto& m = cmd.GetExplicitGroupId();
        TVector<TGroupId> groups;
        std::transform(m.begin(), m.end(), std::back_inserter(groups), [](ui32 id) { return TGroupId::FromValue(id); });
        if (!groups) {
            for (auto it = storagePoolGroups.lower_bound({originId, Min<TGroupId>()});
                    it != storagePoolGroups.end() && it->first == originId; ++it) {
                groups.push_back(it->second);
            }
        }

        for (TGroupId groupId : groups) {
            // find the group belonging to the origin storage pool and remove it from the multimap
            if (!storagePoolGroups.erase({originId, groupId})) {
                throw TExError() << "GroupId# " << groupId << " not found in origin StoragePoolId# " << originId;
            }

            // make group belonging to the target storage pool
            storagePoolGroups.emplace(targetId, groupId);

            // find the group
            TGroupInfo *group = Groups.FindForUpdate(groupId);

            // internal consistency checks
            Y_ABORT_UNLESS(group);
            Y_ABORT_UNLESS(group->StoragePoolId == originId);

            // update the pool id of the group
            group->StoragePoolId = targetId;

            // correct the number of groups in the pools
            --origin.NumGroups;
            ++target.NumGroups;
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TChangeGroupSizeInUnits& cmd, TStatus& /*status*/) {
        auto& storagePools = StoragePools.Unshare();

        const TBoxStoragePoolId poolId(cmd.GetBoxId(), cmd.GetStoragePoolId());

        if (auto it = storagePools.find(poolId); it != storagePools.end()) {
            if (it->second.DDisk) {
                throw TExError() << "can't invoke ChangeGroupSizeInUnits against DDisk pool";
            }
            const ui64 nextGen = CheckGeneration(cmd, storagePools, poolId);
            it->second.Generation = nextGen;
        } else {
            throw TExError() << "StoragePoolId# " << poolId << " not found";
        }

        auto& storagePoolGroups = StoragePoolGroups.Unshare();

        // create a list of groups to be resized
        const auto& m = cmd.GetGroupId();
        TVector<TGroupId> groups;
        std::transform(m.begin(), m.end(), std::back_inserter(groups), [](ui32 id) { return TGroupId::FromValue(id); });
        if (groups.empty()) {
            for (auto it = storagePoolGroups.lower_bound({poolId, Min<TGroupId>()});
                    it != storagePoolGroups.end() && it->first == poolId; ++it) {
                groups.push_back(it->second);
            }
        }

        for (TGroupId groupId : groups) {
            // find the group
            TGroupInfo *group = Groups.FindForUpdate(groupId);
            if (!group || group->StoragePoolId != poolId) {
                throw TExError() << "GroupId# " << groupId << " not found in StoragePoolId# " << poolId;
            }

            auto oldSizeInUnits = group->GroupSizeInUnits;
            auto newSizeInUnits = cmd.GetSizeInUnits();
            auto updateSlotWeight = [&](TVSlotId vslotId) {
                TPDiskInfo* pdisk = PDisks.FindForUpdate(vslotId.ComprisingPDiskId());
                Y_ABORT_UNLESS(pdisk);

                pdisk->NumActiveDynamicSlots -= pdisk->GetOwnerWeight(oldSizeInUnits);
                pdisk->NumActiveDynamicSlots += pdisk->GetOwnerWeight(newSizeInUnits);
            };
            for (const auto& vdisk : group->VDisksInGroup) {
                updateSlotWeight(vdisk->VSlotId);
                // Donors remain live and are accounted with the current group weight.
                for (const TVSlotId& donorId : vdisk->Donors) {
                    updateSlotWeight(donorId);
                }
            }

            // update the group size
            group->GroupSizeInUnits = newSizeInUnits;
            GroupContentChanged.insert(groupId);
            Fit.PoolsAndGroups.emplace(poolId, groupId);
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TReadSettings& /*cmd*/, TStatus& status) {
        Self.SerializeSettings(status.MutableSettings());
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TQueryBaseConfig& cmd, TStatus& status) {
        NKikimrBlobStorage::TBaseConfig *pb = status.MutableBaseConfig();

        if (cmd.GetRetrieveDevices()) {
            DrivesSerials.ForEach([&](const auto& serial, const auto& driveInfo) {
                auto device = pb->AddDevice();
                device->SetSerialNumber(serial.Serial);
                device->SetBoxId(driveInfo.BoxId);
                if (driveInfo.NodeId) {
                    device->SetNodeId(driveInfo.NodeId.GetRef());
                }
                if (driveInfo.PDiskId) {
                    device->SetPDiskId(driveInfo.PDiskId.GetRef());
                }
                if (driveInfo.Path) {
                    device->SetPath(driveInfo.Path.GetRef());
                }
                if (driveInfo.Guid) {
                    device->SetGuid(driveInfo.Guid.GetRef());
                }
                device->SetLifeStage(driveInfo.LifeStage);
                device->SetType(driveInfo.PDiskType);
            });
        }

        const bool virtualGroupsOnly = cmd.GetVirtualGroupsOnly();

        THashSet<TGroupId> groupFilter;
        THashSet<TVSlotId> vslotFilter;
        THashSet<TPDiskId> pdiskFilter;

        if (virtualGroupsOnly) {
            Groups.ForEach([&](TGroupId groupId, const TGroupInfo& groupInfo) {
                if (groupInfo.VirtualGroupState || groupInfo.DecommitStatus != NKikimrBlobStorage::TGroupDecommitStatus::NONE) {
                    groupFilter.insert(groupId);
                }
            });
            VSlots.ForEach([&](TVSlotId vslotId, const TVSlotInfo& vslotInfo) {
                if (vslotInfo.Group && groupFilter.contains(vslotInfo.GroupId)) {
                    vslotFilter.insert(vslotId);
                    pdiskFilter.insert(vslotId.ComprisingPDiskId());
                }
            });
        }

        if (!cmd.GetSuppressPDisks()) {
            PDisks.ForEach([&](const TPDiskId& pdiskId, const TPDiskInfo& pdiskInfo) {
                if (!virtualGroupsOnly || pdiskFilter.contains(pdiskId)) {
                    Serialize(pb->AddPDisk(), pdiskId, pdiskInfo);
                }
            });
        }

        if (!cmd.GetSuppressVSlots()) {
            const TVSlotFinder vslotFinder{[this](const TVSlotId& vslotId, auto&& callback) {
                if (const TVSlotInfo *vslot = VSlots.Find(vslotId)) {
                    callback(*vslot);
                }
            }};
            VSlots.ForEach([&](TVSlotId vslotId, const TVSlotInfo& vslotInfo) {
                if (vslotInfo.Group && (!virtualGroupsOnly || vslotFilter.contains(vslotId))) {
                    Serialize(pb->AddVSlot(), vslotInfo, vslotFinder);
                }
            });
        }

        if (!cmd.GetSuppressGroups()) {
            TGroupInfo::TGroupFinder finder = [&](TGroupId groupId) { return Groups.Find(groupId); };
            const auto& pools = StoragePools.Get();

            Groups.ForEach([&](TGroupId groupId, const TGroupInfo& groupInfo) {
                if (!virtualGroupsOnly || groupFilter.contains(groupId)) {
                    auto* group = pb->AddGroup();
                    Serialize(group, groupInfo, finder, BridgeInfo.get());
                    if (const auto pool = pools.find(groupInfo.StoragePoolId); pool != pools.end()) {
                        group->SetStoragePoolName(pool->second.Name);
                    }
                }
            });
        }

        if (!virtualGroupsOnly) {
            // apply static group
            if (!cmd.GetSuppressPDisks()) {
                for (const auto& [pdiskId, pdisk] : StaticPDisks) {
                    if (PDisks.Find(pdiskId)) {
                        continue; // this pdisk was already reported
                    }
                    auto *x = pb->AddPDisk();
                    x->SetNodeId(pdisk.NodeId);
                    x->SetPDiskId(pdisk.PDiskId);
                    x->SetPath(pdisk.Path);
                    x->SetType(PDiskTypeToPDiskType(pdisk.Category.Type()));
                    x->SetKind(pdisk.Category.Kind());
                    if (pdisk.PDiskConfig) {
                        bool success = x->MutablePDiskConfig()->ParseFromString(pdisk.PDiskConfig);
                        Y_ABORT_UNLESS(success);
                    }
                    x->SetGuid(pdisk.Guid);
                    x->SetNumStaticSlots(pdisk.StaticSlotUsage);
                    x->SetDriveStatus(NKikimrBlobStorage::EDriveStatus::ACTIVE);
                    x->SetExpectedSlotCount(pdisk.ExpectedSlotCount);
                    x->SetExpectedSlotSize(pdisk.GetEffectiveExpectedSlotSize());
                    x->SetDecommitStatus(NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE);
                    if (pdisk.PDiskMetrics) {
                        x->MutablePDiskMetrics()->CopyFrom(*pdisk.PDiskMetrics);
                        x->MutablePDiskMetrics()->ClearPDiskId();
                        x->MutablePDiskMetrics()->SetUpdateTimestamp(pdisk.PDiskMetricsUpdateTimestamp.GetValue());
                    }
                }
            }
            if (!cmd.GetSuppressVSlots()) {
                for (const auto& [vslotId, vslot] : StaticVSlots) {
                    auto *x = pb->AddVSlot();
                    vslotId.Serialize(x->MutableVSlotId());
                    x->SetGroupId(vslot.VDiskId.GroupID.GetRawId());
                    x->SetGroupGeneration(vslot.VDiskId.GroupGeneration);
                    x->SetFailRealmIdx(vslot.VDiskId.FailRealm);
                    x->SetFailDomainIdx(vslot.VDiskId.FailDomain);
                    x->SetVDiskIdx(vslot.VDiskId.VDisk);
                    if (vslot.VDiskMetrics) {
                        x->SetAllocatedSize(vslot.VDiskMetrics->GetAllocatedSize());
                        x->MutableVDiskMetrics()->CopyFrom(*vslot.VDiskMetrics);
                        x->MutableVDiskMetrics()->ClearVDiskId();
                    }
                    x->SetStatus(NKikimrBlobStorage::EVDiskStatus_Name(vslot.VDiskStatus.value_or(NKikimrBlobStorage::EVDiskStatus::ERROR)));
                    x->SetReady(vslot.ReadySince <= Mono);
                }
            }
            if (!cmd.GetSuppressGroups()) {
                TStaticGroupInfo::TStaticGroupFinder finder = [this](TGroupId groupId) {
                    const auto it = StaticGroups.find(groupId);
                    return it != StaticGroups.end() ? &it->second : nullptr;
                };
                for (auto& [groupId, group] : StaticGroups) {
                    group.UpdateStatus(Mono, &Self);
                    group.UpdateLayoutCorrect(&Self);
                }
                for (const auto& [groupId, group] : StaticGroups) {
                    auto *x = pb->AddGroup();
                    groupId.CopyToProto(x, &NKikimrBlobStorage::TBaseConfig::TGroup::SetGroupId);
                    if (const auto& info = group.Info) {
                        x->SetGroupGeneration(info->GroupGeneration);
                        x->SetErasureSpecies(TBlobStorageGroupType::ErasureSpeciesName(info->Type.GetErasure()));
                        for (TActorId actorId : info->GetDynamicInfo().ServiceIdForOrderNumber) {
                            const auto& [nodeId, pdiskId, vdiskSlotId] = DecomposeVDiskServiceId(actorId);
                            const TVSlotId vslotId(nodeId, pdiskId, vdiskSlotId);
                            vslotId.Serialize(x->AddVSlotId());
                        }
                        const auto& status = group.GetStatus(finder, BridgeInfo.get());
                        x->SetOperatingStatus(status.OperatingStatus);
                        x->SetExpectedStatus(status.ExpectedStatus);
                    }
                }
            }
        }

        if (!cmd.GetSuppressNodes()) {
            TMap<TNodeId, NKikimrBlobStorage::TBaseConfig::TNode> nodes;
            for (const auto& [hostId, record] : *HostRecords) {
                TStringStream s;
                std::unordered_map<TString, ui32> map;
                for (const auto& [key, value] : record.Location.GetItems()) {
                    Save(&s, static_cast<ui8>(key));
                    Save(&s, static_cast<ui32>(map.emplace(value, map.size() + 1).first->second));
                }

                auto& node = nodes[record.NodeId];
                node.SetNodeId(record.NodeId);
                auto config = AppData()->DynamicNameserviceConfig;
                if (config && record.NodeId <= config->MaxStaticNodeId) {
                    node.SetType(NKikimrBlobStorage::NT_STATIC);
                } else if (config && record.NodeId <= config->MaxDynamicNodeId) {
                    node.SetType(NKikimrBlobStorage::NT_DYNAMIC);
                } else {
                    node.SetType(NKikimrBlobStorage::NT_UNKNOWN);
                }
                node.SetPhysicalLocation(s.Str());
                record.Location.Serialize(node.MutableLocation(), false); // this field has been introduced recently, so it doesn't have compatibility format
                const auto& nodes = Nodes.Get();
                if (const auto it = nodes.find(record.NodeId); it != nodes.end()) {
                    node.SetLastConnectTimestamp(it->second.LastConnectTimestamp.GetValue());
                    node.SetLastDisconnectTimestamp(it->second.LastDisconnectTimestamp.GetValue());
                    node.SetLastSeenTimestamp(it->second.LastConnectTimestamp <= it->second.LastDisconnectTimestamp ?
                        it->second.LastDisconnectTimestamp.GetValue() : Timestamp.GetValue());
                    node.SetConnected(bool(it->second.ConnectedServerId));
                }
                auto *key = node.MutableHostKey();
                key->SetFqdn(std::get<0>(hostId));
                key->SetIcPort(std::get<1>(hostId));
            }
            for (auto& [nodeId, node] : nodes) {
                pb->AddNode()->Swap(&node);
            }
        }

        Self.SerializeSettings(pb->MutableSettings());
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDropDonorDisk& cmd, TStatus& /*status*/) {
        // first, find matching vslot
        const TVSlotId& vslotId = cmd.GetVSlotId();
        const TVSlotInfo *vslot = VSlots.Find(vslotId);
        if (!vslot) {
            throw TExVSlotNotFound(vslotId);
        }

        // second, validate vdisk id
        const TVDiskID& vdiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());
        if (vslot->GetVDiskId() != vdiskId) {
            throw TExVDiskIdIncorrect(vdiskId, vslotId);
        }

        // check the donor mode
        if (vslot->Mood != TMood::Donor) {
            throw TExDiskIsNotDonor(vslotId, vdiskId);
        }

        // commit destruction
        DestroyVSlot(vslotId);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TWipeVDisk& cmd, TStatus& /*status*/) {
        // first, find matching vslot
        const TVSlotId& vslotId = cmd.GetVSlotId();
        TVSlotInfo *vslot = VSlots.FindForUpdate(vslotId);
        if (!vslot) {
            throw TExVSlotNotFound(vslotId);
        }

        // second, validate vdisk id
        const TVDiskID& vdiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());
        if (vslot->GetVDiskId() != vdiskId) {
            throw TExVDiskIdIncorrect(vdiskId, vslotId);
        }

        TGroupInfo *group = Groups.FindForUpdate(vslot->GroupId);
        vslot->Mood = TMood::Wipe;
        vslot->VDiskStatus = NKikimrBlobStorage::EVDiskStatus::ERROR;
        vslot->OnlyPhantomsRemain = false;
        vslot->IsReady = false;
        GroupFailureModelChanged.insert(group->ID);
        group->CalculateGroupStatus();

        // the wiped VDisk restores its data from the other ones, the VDisks not ready may miss data too
        TBlobStorageGroupInfo::TGroupVDisks dataless(group->Topology.get(), vslot->GetShortVDiskId());
        for (const TVSlotInfo *slot : group->VDisksInGroup) {
            if (!slot->IsReady) {
                dataless |= {group->Topology.get(), slot->GetShortVDiskId()};
            }
        }
        CheckBridgedPartDataRestorable(*group, dataless);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TSanitizeGroup& cmd, NKikimrBlobStorage::TConfigResponse::TStatus& /*status*/) {
        ui32 groupId = cmd.GetGroupId();
        SanitizingRequests.emplace(TGroupId::FromValue(groupId));
        const TGroupInfo *group = Groups.Find(TGroupId::FromValue(groupId));
        if (group) {
            Fit.PoolsAndGroups.emplace(group->StoragePoolId, TGroupId::FromValue(groupId));
        } else {
            throw TExGroupNotFound(groupId);
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TSetVDiskReadOnly& cmd, TStatus& /*status*/) {
        // first, find matching vslot
        const TVSlotId& vslotId = cmd.GetVSlotId();
        TVSlotInfo *vslot = VSlots.FindForUpdate(vslotId);
        if (!vslot) {
            throw TExVSlotNotFound(vslotId);
        }

        // second, validate vdisk id
        const TVDiskID& vdiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());
        if (vslot->GetVDiskId() != vdiskId) {
            throw TExVDiskIdIncorrect(vdiskId, vslotId);
        }

        // then validate transition direction
        const TMood::EValue currentMood = static_cast<TMood::EValue>(vslot->Mood);
        const TMood::EValue targetMood = cmd.GetValue() ? TMood::ReadOnly : TMood::Normal;
        bool allowedTransition = (
            (currentMood == TMood::Normal && targetMood == TMood::ReadOnly) ||
            (currentMood == TMood::ReadOnly && targetMood == TMood::Normal)
        );
        if (!allowedTransition) {
            throw TExError() << "unable to transition VDisk" <<
                " from " << TMood::Name(currentMood) <<
                " to " << TMood::Name(targetMood);
        }

        TGroupInfo *group = Groups.FindForUpdate(vslot->GroupId);
        vslot->Mood = targetMood;
        vslot->VDiskStatus = NKikimrBlobStorage::EVDiskStatus::ERROR;
        vslot->OnlyPhantomsRemain = false;
        vslot->IsReady = false;
        GroupFailureModelChanged.insert(group->ID);
        group->CalculateGroupStatus();
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDeleteSpecificGroups& cmd, TStatus& /*status*/) {
        for (ui32 groupId : cmd.GetGroupIds()) {
            const TGroupInfo *groupInfo = Groups.Find(TGroupId::FromValue(groupId));
            if (!groupInfo) {
                throw TExGroupNotFound(groupId);
            }
            DeleteExistingGroupWithVSlots(*groupInfo);
        }
    }

} // NKikimr::NBsController
