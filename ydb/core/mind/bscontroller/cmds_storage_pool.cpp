#include "config.h"
#include <ydb/core/base/nameservice.h>


namespace NKikimr::NBsController {

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDefineStoragePool& cmd, TStatus& status) {
        TBoxId boxId = cmd.GetBoxId();
        if (!boxId && Boxes.Get().size() == 1) {
            boxId = Boxes.Get().begin()->first;
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

        TStoragePoolInfo storagePool;
        storagePool.Name = cmd.GetName();
        storagePool.Generation = nextGen;

        const TString &erasureSpecies = cmd.GetErasureSpecies();
        storagePool.ErasureSpecies = TBlobStorageGroupType::ErasureSpeciesByName(erasureSpecies);
        if (storagePool.ErasureSpecies == TBlobStorageGroupType::ErasureSpeciesCount) {
            throw TExError() << "invalid ErasureSpecies# " << erasureSpecies;
        }

        if (cmd.HasGeometry()) {
            const auto &geom = cmd.GetGeometry();
            storagePool.RealmLevelBegin = geom.GetRealmLevelBegin();
            storagePool.RealmLevelEnd = geom.GetRealmLevelEnd();
            storagePool.DomainLevelBegin = geom.GetDomainLevelBegin();
            storagePool.DomainLevelEnd = geom.GetDomainLevelEnd();
            storagePool.NumFailRealms = geom.GetNumFailRealms();
            storagePool.NumFailDomainsPerFailRealm = geom.GetNumFailDomainsPerFailRealm();
            storagePool.NumVDisksPerFailDomain = geom.GetNumVDisksPerFailDomain();
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

        for (const auto &userId : cmd.GetUserId()) {
            storagePool.UserIds.emplace(boxId, storagePoolId, userId);
        }

        for (const auto &item : cmd.GetPDiskFilter()) {
            TStoragePoolInfo::TPDiskFilter filter;
            filter.BoxId = boxId;
            filter.StoragePoolId = storagePoolId;

            bool hasTypeProperty = false;

            for (const auto &property : item.GetProperty()) {
                switch (property.GetPropertyCase()) {
#define PROPERTY(NAME, PTR) \
                    case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::k ## NAME: { \
                        if (filter.NAME) { \
                            throw TExError() << #NAME << " already set"; \
                        } else { \
                            filter.NAME = property.Get ## NAME(); \
                            if (bool *p = (PTR)) { \
                                *p = true; \
                            } \
                            continue; \
                        } \
                    }

                    PROPERTY(Type, &hasTypeProperty)
                    PROPERTY(SharedWithOs, nullptr)
                    PROPERTY(ReadCentric, nullptr)
                    PROPERTY(Kind, nullptr)

                    case NKikimrBlobStorage::TPDiskFilter::TRequiredProperty::PROPERTY_NOT_SET:
                        throw TExError() << "TPDiskFilter.TRequiredProperty.Property not set";
                }

                throw TExError() << "TPDiskFilter.TRequiredProperty.Property has invalid case";
            }

            if (!hasTypeProperty) {
                throw TExError() << "TPDiskFilter did not specify Type property";
            }

            storagePool.PDiskFilters.insert(std::move(filter));
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
        auto r = spToGroups.equal_range(id);
        for (auto it = r.first; it != r.second; ++it) {
            const TGroupInfo *group = Groups.Find(it->second);
            Y_ABORT_UNLESS(group);
            if (group->ErasureSpecies != storagePool.ErasureSpecies) {
                throw TExError() << "GroupId# " << it->second << " has different erasure species";
            }
        }

        auto &storagePools = StoragePools.Unshare();
        if (const auto [spIt, inserted] = storagePools.try_emplace(id, std::move(storagePool)); !inserted) {
            TStoragePoolInfo& cur = spIt->second;
            if (cur.SchemeshardId != storagePool.SchemeshardId || cur.PathItemId != storagePool.PathItemId) {
                for (auto it = r.first; it != r.second; ++it) {
                    GroupContentChanged.insert(it->second);
                }
            }
            cur = std::move(storagePool); // update existing storage pool
        }
        Fit.PoolsAndGroups.emplace(id, std::nullopt);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TReadStoragePool& cmd, TStatus& status) {
        const bool queryAllBoxes = cmd.GetBoxId() == Max<TBoxId>();

        // create set of allowed storage pool ids to query
        const auto& ids = cmd.GetStoragePoolId();
        THashSet<TBoxStoragePoolId> storagePoolIds;
        if (!ids.empty()) {
            if (queryAllBoxes) {
                throw TExError() << "StoragePoolId may only be specified when BoxId is defined";
            } else {
                for (const auto storagePoolId : ids) {
                    storagePoolIds.emplace(cmd.GetBoxId(), storagePoolId);
                }
            }
        }

        const auto& names = cmd.GetName();
        THashSet<TString> nameSet(names.begin(), names.end());

        // calculate lower and upper bounds for search
        const TBoxStoragePoolId since(queryAllBoxes ? Min<Schema::BoxStoragePool::BoxId::Type>() : cmd.GetBoxId(),
            Min<Schema::BoxStoragePool::StoragePoolId::Type>());
        const TBoxStoragePoolId till(queryAllBoxes ? Max<Schema::BoxStoragePool::BoxId::Type>() : cmd.GetBoxId(),
            Max<Schema::BoxStoragePool::StoragePoolId::Type>());

        // iterate through subset and add only requested entities
        const auto &storagePools = StoragePools.Get();
        for (auto it = storagePools.lower_bound(since); it != storagePools.end() && it->first <= till; ++it) {
            if ((!storagePoolIds || storagePoolIds.count(it->first)) && (!nameSet || nameSet.count(it->second.Name))) {
                Serialize(status.AddStoragePool(), it->first, it->second);
            }
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDeleteStoragePool& cmd, TStatus& /*status*/) {
        const TBoxStoragePoolId id(cmd.GetBoxId(), cmd.GetStoragePoolId());
        CheckGeneration(cmd, StoragePools.Get(), id);
        auto &storagePools = StoragePools.Unshare();
        if (!storagePools.erase(id)) {
            throw TExError() << "StoragePoolId# " << id << " not found";
        }

        auto &storagePoolGroups = StoragePoolGroups.Unshare();
        auto range = storagePoolGroups.equal_range(id);
        for (auto it = range.first; it != range.second; ++it) {
            const TGroupId groupId = it->second;
            if (const TGroupInfo *groupInfo = Groups.Find(groupId)) {
                for (const TVSlotInfo *vslot : groupInfo->VDisksInGroup) {
                    DestroyVSlot(vslot->VSlotId);
                }
                DeleteExistingGroup(groupId);
            } else {
                throw TExError() << "GroupId# " << groupId << " not found";
            }
        }
        storagePoolGroups.erase(range.first, range.second);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TProposeStoragePools& /*cmd*/,TStatus& status) {
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
            if (storagePoolGroups.count(groupId)) {
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

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TReassignGroupDisk& cmd, NKikimrBlobStorage::TConfigResponse::TStatus& /*status*/) {
        // find matching TVSlotInfo entity
        const TVDiskID vdiskId(TGroupId::FromProto(&cmd, &NKikimrBlobStorage::TReassignGroupDisk::GetGroupId), cmd.GetGroupGeneration(), cmd.GetFailRealmIdx(),
                               cmd.GetFailDomainIdx(), cmd.GetVDiskIdx());

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

        auto& storagePoolGroups = StoragePoolGroups.Unshare();

        // create a list of groups to be moved
        const auto& m = cmd.GetExplicitGroupId();
        TVector<TGroupId> groups;
        std::transform(m.begin(), m.end(), std::back_inserter(groups), [](ui32 id) { return TGroupId::FromValue(id); });
        if (!groups) {
            for (auto it = storagePoolGroups.lower_bound(originId); it != storagePoolGroups.end() && it->first == originId; ++it) {
                groups.push_back(it->second);
            }
        }

        for (TGroupId groupId : groups) {
            // find the group belonging to the origin storage pool and remove it from the multimap
            auto p = storagePoolGroups.equal_range(originId);
            if (auto it = std::find(p.first, p.second, TMultiMap<TBoxStoragePoolId, TGroupId>::value_type(originId, groupId)); it != p.second) {
                storagePoolGroups.erase(it);
            } else {
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

        PDisks.ForEach([&](const TPDiskId& pdiskId, const TPDiskInfo& pdiskInfo) {
            if (!virtualGroupsOnly || pdiskFilter.contains(pdiskId)) {
                Serialize(pb->AddPDisk(), pdiskId, pdiskInfo);
            }
        });
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
        Groups.ForEach([&](TGroupId groupId, const TGroupInfo& groupInfo) {
            if (!virtualGroupsOnly || groupFilter.contains(groupId)) {
               Serialize(pb->AddGroup(), groupInfo);
            }
        });

        if (!virtualGroupsOnly) {
            const TMonotonic mono = TActivationContext::Monotonic();

            // apply static group
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
                x->SetDecommitStatus(NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE);
                if (pdisk.PDiskMetrics) {
                    x->MutablePDiskMetrics()->CopyFrom(*pdisk.PDiskMetrics);
                    x->MutablePDiskMetrics()->ClearPDiskId();
                }
            }
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
                x->SetReady(vslot.ReadySince <= mono);
            }
            if (const auto& s = Self.StorageConfig; s.HasBlobStorageConfig()) {
                if (const auto& bsConfig = s.GetBlobStorageConfig(); bsConfig.HasServiceSet()) {
                    const auto& ss = bsConfig.GetServiceSet();
                    for (const auto& group : ss.GetGroups()) {
                        auto *x = pb->AddGroup();
                        x->SetGroupId(group.GetGroupID());
                        x->SetGroupGeneration(group.GetGroupGeneration());
                        x->SetErasureSpecies(TBlobStorageGroupType::ErasureSpeciesName(group.GetErasureSpecies()));
                        for (const auto& realm : group.GetRings()) {
                            for (const auto& domain : realm.GetFailDomains()) {
                                for (const auto& location : domain.GetVDiskLocations()) {
                                    const TVSlotId vslotId(location.GetNodeID(), location.GetPDiskID(), location.GetVDiskSlotID());
                                    vslotId.Serialize(x->AddVSlotId());
                                }
                            }
                        }

                        TStringStream err;
                        auto info = TBlobStorageGroupInfo::Parse(group, nullptr, &err);
                        Y_VERIFY_DEBUG_S(info, "failed to parse static group, error# " << err.Str());
                        if (info) {
                            const auto *topology = &info->GetTopology();

                            TBlobStorageGroupInfo::TGroupVDisks failed(topology);
                            TBlobStorageGroupInfo::TGroupVDisks failedByPDisk(topology);

                            ui32 realmIdx = 0;
                            for (const auto& realm : group.GetRings()) {
                                ui32 domainIdx = 0;
                                for (const auto& domain : realm.GetFailDomains()) {
                                    ui32 vdiskIdx = 0;
                                    for (const auto& location : domain.GetVDiskLocations()) {
                                        const TVSlotId vslotId(location.GetNodeID(), location.GetPDiskID(), location.GetVDiskSlotID());
                                        const TVDiskIdShort vdiskId(realmIdx, domainIdx, vdiskIdx);

                                        if (const auto it = StaticVSlots.find(vslotId); it != StaticVSlots.end()) {
                                            if (mono <= it->second.ReadySince) { // VDisk can't be treated as READY one
                                                failed |= {topology, vdiskId};
                                            } else if (const TPDiskInfo *pdisk = PDisks.Find(vslotId.ComprisingPDiskId()); !pdisk || !pdisk->HasGoodExpectedStatus()) {
                                                failedByPDisk |= {topology, vdiskId};
                                            }
                                        } else {
                                            failed |= {topology, vdiskId};
                                        }

                                        ++vdiskIdx;
                                    }
                                    ++domainIdx;
                                }
                                ++realmIdx;
                            }

                            x->SetOperatingStatus(DeriveStatus(topology, failed));
                            x->SetExpectedStatus(DeriveStatus(topology, failed | failedByPDisk));
                        }
                    }
                }
            }
        }

        const TInstant now = TActivationContext::Now();
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
                    it->second.LastDisconnectTimestamp.GetValue() : now.GetValue());
            }
            auto *key = node.MutableHostKey();
            key->SetFqdn(std::get<0>(hostId));
            key->SetIcPort(std::get<1>(hostId));
        }
        for (auto& [nodeId, node] : nodes) {
            pb->AddNode()->Swap(&node);
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
        vslot->IsReady = false;
        GroupFailureModelChanged.insert(group->ID);
        group->CalculateGroupStatus();
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
        vslot->IsReady = false;
        GroupFailureModelChanged.insert(group->ID);
        group->CalculateGroupStatus();
    }

} // NKikimr::NBsController
