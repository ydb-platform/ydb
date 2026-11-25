#include "impl.h"
#include "config.h"
#include "group_geometry_info.h"

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/blob_depot/config.h>
#include <ydb/core/blob_depot/s3_route.h>
#include <ydb/core/blob_depot/events.h>
#include <ydb/core/blob_depot/s3_router.h>
#include <ydb/core/blob_depot/types.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <google/protobuf/util/message_differencer.h>
#include <util/generic/guid.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_CONTROLLER

namespace NKikimr::NBsController {

    namespace {
        template<typename TCallback>
        void ForEachS3NamespaceId(TStringBuf path, TCallback&& visit);
        bool HasGeneratedS3Namespace(const NKikimrBlobDepot::TBlobDepotConfig& config);

        struct TS3Namespace {
            TString Bucket;
            TString Prefix;

            bool operator==(const TS3Namespace&) const = default;
        };

        TS3Namespace GetS3Namespace(const NKikimrBlobDepot::TS3BackendSettings& backend, TStringBuf name) {
            const auto& settings = backend.GetSettings();
            return {settings.GetBucket(), NBlobDepot::MakeS3BasePath(settings, name)};
        }

        TS3Namespace GetS3Namespace(const NKikimrBlobDepot::TBlobDepotConfig& config) {
            return GetS3Namespace(config.GetS3BackendSettings(), config.GetName());
        }

        using EClaim = TBlobStorageController::EBlobDepotClaim;
        using TClaim = TBlobStorageController::TBlobDepotClaim;

        template<typename TCallback>
        void ForEachPhysicalS3Claim(const TS3Namespace& ns, EClaim kind, TCallback&& visit) {
            visit(TClaim{kind, ns.Bucket, ns.Prefix});
            if (const auto prefixError = NBlobDepot::ValidateS3BasePath(ns.Prefix)) {
                // A legacy client's dot-segment handling can alias an otherwise unrelated key in the bucket.
                visit(TClaim{EClaim::AmbiguousBucket, ns.Bucket, {}});
            }
        }

        template<typename TCallback>
        void ForEachLiveBlobDepotClaim(const TBlobStorageController::TGroupInfo& group,
                const NKikimrBlobDepot::TBlobDepotConfig& config, TCallback&& visit) {
            if (group.VirtualGroupName) {
                visit(TClaim{EClaim::GroupName, {}, *group.VirtualGroupName});
            }
            const auto ns = GetS3Namespace(config);
            // Reserve generated IDs even for local-only groups that may attach S3 later.
            if (!ns.Bucket.empty() || HasGeneratedS3Namespace(config)) {
                ForEachS3NamespaceId(ns.Prefix, visit);
            }
            if (!ns.Bucket.empty()) {
                ForEachPhysicalS3Claim(ns, EClaim::PhysicalPrefix, visit);
                const auto logical = GetS3Namespace(config.GetS3BackendSettings(), group.VirtualGroupName.GetOrElse(config.GetName()));
                const auto logicalError = NBlobDepot::ValidateS3BasePath(logical.Prefix);
                if (!logicalError) {
                    visit(TClaim{EClaim::LogicalPrefix, ns.Bucket, logical.Prefix});
                }
            }
        }

        void CheckS3ObjectKeyLength(const NKikimrBlobDepot::TS3BackendSettings& settings, TStringBuf storageName) {
            const auto& s3 = settings.GetSettings();
            if (!s3.GetBucket().empty() && NBlobDepot::MakeS3BasePath(s3, storageName).size() >
                    NBlobDepot::TS3Locator::MaxBasePathBytes()) {
                throw TExError() << "S3 object keys including the depot namespace and locator must fit in "
                    << NBlobDepot::TS3Locator::MaxObjectKeyBytes << " bytes";
            }
        }

        void RecordS3NamespaceValidation(NKikimrBlobDepot::TBlobDepotConfig& config) {
            const auto& s3 = config.GetS3BackendSettings();
            if (!s3.GetSettings().GetBucket().empty()) {
                config.SetS3NamespaceValidated(true);
                if (s3.HasAsyncMode() && AppData()->FeatureFlags.GetEnableBlobDepotDataCooling()) {
                    config.SetEverAuthorizedDataCooling(true);
                    config.SetS3CoolingAuthorized(true);
                }
            }
        }
    }

    NKikimrBlobDepot::TBlobDepotConfig TBlobStorageController::TRetiredS3Namespace::GetConfig() const {
        NKikimrBlobDepot::TBlobDepotConfig config;
        Y_ABORT_UNLESS(config.ParseFromString(S3Config));
        return config;
    }

    void TBlobStorageController::RebuildBlobDepotOwners() {
        for (const auto& [id, group] : GroupMap) {
            if (group->BlobDepotConfig) {
                NKikimrBlobDepot::TBlobDepotConfig config;
                Y_ABORT_UNLESS(config.ParseFromString(*group->BlobDepotConfig));
                ForEachLiveBlobDepotClaim(*group, config, [&](const TClaim& key) { BlobDepotOwners[key].insert(id); });
            }
        }
        for (const auto& [id, info] : RetiredS3Namespaces) {
            ForEachPhysicalS3Claim(GetS3Namespace(info.GetConfig()), EClaim::RetiredPrefix,
                [&](const TClaim& key) { BlobDepotOwners[key].insert(id); });
        }
    }

    namespace {
        // An S3 access point and its bucket can identify the very same keys. Check generated
        // suffixes independently of either the bucket name or a configurable path prefix.
        template<typename TCallback>
        void ForEachS3NamespaceId(TStringBuf path, TCallback&& visit) {
            TGUID guid;
            if (!NBlobDepot::ValidateS3BasePath(path)) {
                const auto suffix = path.RAfter('/');
                if (GetGuid(suffix, guid)) {
                    visit(TClaim{EClaim::NamespaceId, {}, TString(suffix)});
                }
                return;
            }
            // A legacy server may normalize dot segments. Any GUID segment left as the
            // final path component must remain reserved until the legacy owner is fenced.
            std::set<TStringBuf> seen;
            for (TStringBuf segment : StringSplitter(path).Split('/').SkipEmpty()) {
                if (GetGuid(segment, guid) && seen.insert(segment).second) {
                    visit(TClaim{EClaim::NamespaceId, {}, TString(segment)});
                }
            }
        }

        template<typename TCallback>
        void ForEachS3PathClaim(const TClaim& claim, TCallback&& visit) {
            const auto& [kind, bucket, prefix] = claim;
            if (kind == EClaim::PhysicalPrefix) {
                visit(TClaim{EClaim::LivePath, {}, prefix});
            } else if (kind == EClaim::RetiredPrefix) {
                visit(TClaim{EClaim::RetiredPath, {}, prefix});
                ForEachS3NamespaceId(prefix, visit);
            }
        }

        bool HasGeneratedS3Namespace(const NKikimrBlobDepot::TBlobDepotConfig& config) {
            const auto suffix = TStringBuf(config.GetName()).RAfter('/');
            TGUID guid;
            return suffix == config.GetGeneratedS3NamespaceId() && GetGuid(suffix, guid);
        }

        TString MakeRetiredS3Reservation(const NKikimrBlobDepot::TBlobDepotConfig& config) {
            const auto ns = GetS3Namespace(config);
            NKikimrBlobDepot::TBlobDepotConfig reservation;
            // The canonical path retains the ID used by the claim index after the work stops.
            reservation.SetName(ns.Prefix);
            reservation.MutableS3BackendSettings()->MutableSettings()->SetBucket(ns.Bucket);
            return reservation.SerializeAsString();
        }

    }

    void TBlobStorageController::RebuildS3PathClaims() {
        for (const auto& [claim, owners] : BlobDepotOwners) {
            ForEachS3PathClaim(claim, [&](const TClaim& path) {
                BlobDepotOwners[path].insert(owners.begin(), owners.end());
            });
        }
    }

    bool TBlobStorageController::HasOtherS3NamespaceOwner(TGroupId groupId,
            const NKikimrBlobDepot::TBlobDepotConfig& config) const {
        const auto ns = GetS3Namespace(config);
        if (BlobDepotOwners.contains({EClaim::LivePath, {}, ns.Prefix})) {
            return true;
        }
        auto hasOwner = [&](const TClaim& claim, auto isOwner) {
            const auto it = BlobDepotOwners.find(claim);
            return it != BlobDepotOwners.end() && std::ranges::any_of(it->second, [&](TGroupId other) {
                return other != groupId && isOwner(other);
            });
        };
        // A removed group keeps serving its last applied config until Hive actually ejects it.
        auto awaitingHive = [&](TGroupId other) { return BlobDepotDeleteQueue.contains(other); };
        // A completed malformed tombstone still fences new claims. Its stopped tablet
        // cannot own a live object. The same distinction applies to a reused GUID suffix.
        auto liveOrAwaitingHive = [&](TGroupId other) { return !RetiredS3Namespaces.contains(other) || awaitingHive(other); };
        return hasOwner({EClaim::RetiredPath, {}, ns.Prefix}, awaitingHive) ||
            hasOwner({EClaim::AmbiguousBucket, ns.Bucket, {}}, liveOrAwaitingHive) ||
            (HasGeneratedS3Namespace(config) &&
                hasOwner({EClaim::NamespaceId, {}, config.GetGeneratedS3NamespaceId()}, liveOrAwaitingHive));
    }

    void TBlobStorageController::TConfigState::ChangeBlobDepotClaim(const TClaim& key, TGroupId id, bool add) {
        auto change = [&](const TClaim& claim) {
            auto [it, inserted] = ChangedBlobDepotOwners.try_emplace(claim);
            if (inserted) {
                if (auto base = Self.BlobDepotOwners.find(claim); base != Self.BlobDepotOwners.end()) {
                    it->second = base->second;
                }
            }
            if (add) {
                Y_ABORT_UNLESS(it->second.insert(id).second);
            } else {
                Y_ABORT_UNLESS(it->second.erase(id));
            }
        };
        change(key);
        ForEachS3PathClaim(key, change);
    }

    void TBlobStorageController::TConfigState::ChangeLiveBlobDepotClaims(const TGroupInfo& group,
            const NKikimrBlobDepot::TBlobDepotConfig& config, bool add) {
        ForEachLiveBlobDepotClaim(group, config, [&](const TClaim& key) { ChangeBlobDepotClaim(key, group.ID, add); });
    }

    void TBlobStorageController::TConfigState::CommitBlobDepotOwners() {
        for (auto& [key, owners] : ChangedBlobDepotOwners) {
            if (owners.empty()) {
                Self.BlobDepotOwners.erase(key);
            } else {
                Self.BlobDepotOwners[key] = std::move(owners);
            }
        }
    }

    const std::set<TGroupId>& TBlobStorageController::TConfigState::FindBlobDepotOwners(const TClaim& key) const {
        if (const auto it = ChangedBlobDepotOwners.find(key); it != ChangedBlobDepotOwners.end()) {
            return it->second;
        }
        if (const auto it = Self.BlobDepotOwners.find(key); it != Self.BlobDepotOwners.end()) {
            return it->second;
        }
        static const std::set<TGroupId> empty;
        return empty;
    }

    void TBlobStorageController::TConfigState::DetachBlobDepot(TGroupInfo& group,
            const NKikimrBlobDepot::TBlobDepotConfig& config) {
        ChangeLiveBlobDepotClaims(group, config, false);
        group.VirtualGroupName = {};
        group.VirtualGroupState = {};
        group.HiveId = {};
        group.BlobDepotConfig = {};
        group.BlobDepotId = {};
        group.ErrorReason = {};
        group.NeedAlter = {};
    }

    void TBlobStorageController::TConfigState::RetireS3Namespace(TGroupId groupId,
            const NKikimrBlobDepot::TBlobDepotConfig& config) {
        if (config.HasTabletId() && !config.GetS3BackendSettings().GetSettings().GetBucket().empty()) {
            NKikimrBlobDepot::TBlobDepotConfig retired;
            retired.SetName(config.GetName());
            retired.SetEverAuthorizedDataCooling(config.GetEverAuthorizedDataCooling());
            if (HasGeneratedS3Namespace(config)) {
                retired.SetGeneratedS3NamespaceId(config.GetGeneratedS3NamespaceId());
            }
            retired.MutableS3BackendSettings()->CopyFrom(config.GetS3BackendSettings());
            auto [it, inserted] = NewRetiredS3Namespaces.try_emplace(groupId);
            Y_ABORT_UNLESS(inserted);
            it->second.BlobDepotId = config.GetTabletId();
            it->second.S3Config = retired.SerializeAsString();
            ForEachPhysicalS3Claim(GetS3Namespace(config), EClaim::RetiredPrefix,
                [&](const TClaim& key) { ChangeBlobDepotClaim(key, groupId, true); });
        }
    }

    void TBlobStorageController::TConfigState::CheckBlobDepotS3Prefix(const NKikimrBlobDepot::TS3BackendSettings& settings,
            TStringBuf groupName, TStringBuf storageName, std::optional<TGroupId> ownGroup) const {
        const auto requested = GetS3Namespace(settings, storageName);
        if (!requested.Bucket.empty()) {
            const auto& retired = FindBlobDepotOwners({EClaim::RetiredPath, {}, requested.Prefix});
            if (!retired.empty()) {
                throw TExError() << "S3 namespace is reserved for retired BlobDepot cleanup"
                    << TErrorParams::GroupId(retired.begin()->GetRawId());
            }
        }
        ForEachS3NamespaceId(requested.Prefix, [&](const TClaim& key) {
            for (TGroupId id : FindBlobDepotOwners(key)) {
                if (id != ownGroup) {
                    throw TExError() << "S3 namespace identifier is already used by another BlobDepot"
                        << TErrorParams::GroupId(id.GetRawId());
                }
            }
        });
        if (requested.Bucket.empty()) {
            return;
        }
        const auto groupNamespace = GetS3Namespace(settings, groupName);
        if (const auto error = NBlobDepot::ValidateS3BasePath(requested.Prefix)) {
            throw TExError() << *error;
        }
        // A migrated legacy group can still have dot segments in its virtual group name.
        // Only its persisted storage name is used for S3 requests.
        const auto logicalError = NBlobDepot::ValidateS3BasePath(groupNamespace.Prefix);
        if (logicalError && !ownGroup) {
            throw TExError() << *logicalError;
        }
        const auto& retired = FindBlobDepotOwners({EClaim::RetiredPrefix, requested.Bucket, requested.Prefix});
        if (!retired.empty()) {
            throw TExError() << "S3 namespace is reserved for retired BlobDepot cleanup"
                << TErrorParams::GroupId(retired.begin()->GetRawId());
        }
        auto rejectOtherOwner = [&](const std::set<TGroupId>& owners, const char* error) {
            auto it = owners.begin();
            if (it != owners.end() && ownGroup == *it) {
                ++it;
            }
            if (it != owners.end()) {
                throw TExError() << error << TErrorParams::GroupId(it->GetRawId());
            }
        };
        rejectOtherOwner(FindBlobDepotOwners({EClaim::AmbiguousBucket, requested.Bucket, {}}),
            "Another BlobDepot in the S3 bucket has an ambiguous key prefix");
        constexpr const char* collision = "S3 bucket and effective key prefix are already used by another BlobDepot";
        rejectOtherOwner(FindBlobDepotOwners({EClaim::PhysicalPrefix, requested.Bucket, requested.Prefix}), collision);
        if (!logicalError) {
            rejectOtherOwner(FindBlobDepotOwners({EClaim::LogicalPrefix, groupNamespace.Bucket, groupNamespace.Prefix}), collision);
        }
    }

    bool TBlobStorageController::TConfigState::KeepsRetiredS3Namespace(TGroupId id,
            const NKikimrBlobDepot::TS3BackendSettings& settings, TStringBuf storageName) const {
        const auto ns = GetS3Namespace(settings, storageName);
        if (NBlobDepot::ValidateS3BasePath(ns.Prefix) || FindBlobDepotOwners({EClaim::RetiredPath, {}, ns.Prefix}).empty() ||
                !FindBlobDepotOwners({EClaim::PhysicalPrefix, ns.Bucket, ns.Prefix}).contains(id)) {
            return false;
        }
        const auto& live = FindBlobDepotOwners({EClaim::LivePath, {}, ns.Prefix});
        const auto& ambiguous = FindBlobDepotOwners({EClaim::AmbiguousBucket, ns.Bucket, {}});
        return live.size() == 1 && live.contains(id) && std::ranges::none_of(ambiguous, [&](TGroupId other) {
            return !Self.RetiredS3Namespaces.contains(other) && !NewRetiredS3Namespaces.contains(other);
        });
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TAllocateVirtualGroup& cmd, TStatus& status) {
        if (!cmd.GetName()) {
            throw TExError() << "TAllocateVirtualGroup.Name must be set and be nonempty";
        } else if (cmd.GetHiveDesignatorCase() == NKikimrBlobStorage::TAllocateVirtualGroup::HIVEDESIGNATOR_NOT_SET) {
            throw TExError() << "TAllocateVirtualGroup.HiveId/Database is not specified";
        }

        if (const auto error = NBlobDepot::ValidateDataCooling(cmd.GetS3BackendSettings())) {
            throw TExError() << *error;
        }

        // make sure the operation is idempotent
        const auto& existing = FindBlobDepotOwners({EClaim::GroupName, {}, cmd.GetName()});
        if (!existing.empty()) {
            status.AddGroupId(existing.begin()->GetRawId());
            status.SetAlready(true);
            return;
        }

        // Persist the suffix in Name so all tablet and agent versions use the same physical keys.
        // Attaching a pre-existing test tablet retains its original layout.
        const TString namespaceId = cmd.GetBlobDepotId() ? TString() : CreateGuidAsString();
        const TString storageName = namespaceId ? TStringBuilder() << cmd.GetName() << '/' << namespaceId : cmd.GetName();
        CheckS3ObjectKeyLength(cmd.GetS3BackendSettings(), storageName);
        CheckBlobDepotS3Prefix(cmd.GetS3BackendSettings(), cmd.GetName(), storageName);

        // allocate group identifier
        auto& nextGroupId = NextVirtualGroupId.Unshare();
        TGroupID groupId(EGroupConfigurationType::Virtual, 1, nextGroupId.GetRawId());
        ++nextGroupId;

        // determine storage pool that will contain newly created virtual group
        TBoxStoragePoolId storagePoolId;
        auto& pools = StoragePools.Unshare();
        switch (cmd.GetStoragePoolCase()) {
            case NKikimrBlobStorage::TAllocateVirtualGroup::kStoragePoolName: {
                ui32 found = 0;
                for (const auto& [id, info] : pools) {
                    if (info.Name == cmd.GetStoragePoolName()) {
                        storagePoolId = id;
                        ++found;
                    }
                }
                if (!found) {
                    throw TExError() << "storage pool is not found";
                } else if (found > 1) {
                    throw TExError() << "ambigous storage pool name";
                }
                break;
            }

            case NKikimrBlobStorage::TAllocateVirtualGroup::kStoragePoolId: {
                const auto& x = cmd.GetStoragePoolId();
                storagePoolId = {x.GetBoxId(), x.GetStoragePoolId()};
                if (!pools.contains(storagePoolId)) {
                    throw TExError() << "storage pool is not found" << TErrorParams::BoxId(std::get<0>(storagePoolId))
                        << TErrorParams::StoragePoolId(std::get<1>(storagePoolId));
                }
                break;
            }

            case NKikimrBlobStorage::TAllocateVirtualGroup::STORAGEPOOL_NOT_SET:
                throw TExError() << "StoragePool must be specified in one of allowed ways";
        }

        auto& pool = pools.at(storagePoolId);
        if (pool.DDisk) {
            throw TExError() << "can't invoke AllocateVirtualGroup against DDisk pool";
        }

        // create entry in group table
        auto *group = Groups.ConstructInplaceNewEntry(TGroupId::FromValue(groupId.GetRaw()), TGroupId::FromValue(groupId.GetRaw()), 0u, 0u,
            TBlobStorageGroupType::ErasureNone, 0u, NKikimrBlobStorage::TVDiskKind::Default,
            pool.EncryptionMode.GetOrElse(TBlobStorageGroupInfo::EEM_NONE), TBlobStorageGroupInfo::ELCP_INITIAL,
            TString(), TString(), 0u, 0u, false, false, 0u, TBridgePileId(), storagePoolId, 0u, 0u, 0u, false);

        // bind group to storage pool
        ++pool.NumGroups;
        StoragePoolGroups.Unshare().emplace(storagePoolId, group->ID);

        auto& index = IndexGroupSpeciesToGroup.Unshare();
        index[group->GetGroupSpecies()].push_back(group->ID);

        group->VirtualGroupName = cmd.GetName();
        group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::NEW;
        group->HiveId = cmd.HasHiveId() ? MakeMaybe(cmd.GetHiveId()) : Nothing();
        group->Database = cmd.HasDatabase() ? MakeMaybe(cmd.GetDatabase()) : Nothing();
        group->NeedAlter = true;

        if (cmd.GetBlobDepotId()) {
            group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::WORKING;
            group->BlobDepotId = cmd.GetBlobDepotId();
            group->SeenOperational = true;
        }

        GroupFailureModelChanged.insert(group->ID);
        group->CalculateGroupStatus();
        group->CalculateLayoutStatus(&Self, {});

        NKikimrBlobDepot::TBlobDepotConfig config;
        config.SetVirtualGroupId(group->ID.GetRawId());
        config.MutableChannelProfiles()->CopyFrom(cmd.GetChannelProfiles());
        if (cmd.HasS3BackendSettings()) {
            config.MutableS3BackendSettings()->CopyFrom(cmd.GetS3BackendSettings());
            if (cmd.GetS3BackendSettings().HasAsyncMode()) {
                config.SetS3NamespacePinned(true);
            }
            RecordS3NamespaceValidation(config);
        }
        config.SetName(storageName);
        if (namespaceId) {
            config.SetGeneratedS3NamespaceId(namespaceId);
        }
        ChangeLiveBlobDepotClaims(*group, config, true);

        const bool success = config.SerializeToString(&group->BlobDepotConfig.ConstructInPlace());
        Y_ABORT_UNLESS(success);

        status.AddGroupId(group->ID.GetRawId());
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDecommitGroups& cmd, TStatus& /*status*/) {
        if (!cmd.HasDatabase()) {
            throw TExError() << "TDecommitGroups.Database is not specified, but it is mandatory";
        }

        for (const ui32 groupId : cmd.GetGroupIds()) {
            TGroupInfo *group = Groups.FindForUpdate(TGroupId::FromValue(groupId));
            if (!group) {
                throw TExGroupNotFound(groupId);
            } else if (group->DecommitStatus != NKikimrBlobStorage::TGroupDecommitStatus::NONE) {
                if (group->Database != cmd.GetDatabase()) {
                    throw TExError() << "different database specified for decommitting group" << TErrorParams::GroupId(groupId);
                }
                // group is already being decommitted -- make this operation idempotent
                continue;
            } else if (group->VirtualGroupState) {
                throw TExError() << "group is already virtual" << TErrorParams::GroupId(groupId);
            } else if (group->IsBridgePileOnly()) {
                // bridged group is decommitted as a whole through its bridge proxy group, single piles are not
                throw TExError() << "can't decommit a pile of bridged group, decommit its bridge proxy group instead"
                    << TErrorParams::GroupId(groupId);
            } else if (group->BridgeGroupInfo && Self.IsBeingRolledBack()) {
                // the group has to stay the inline pile with all its data until it is made ordinary one
                throw TExError() << "bridged group can't be decommitted while the cluster is being rolled back"
                    << TErrorParams::GroupId(groupId);
            }

            group->DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::PENDING;
            group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::NEW;
            group->Database = cmd.GetDatabase();
            group->NeedAlter = true;
            GroupFailureModelChanged.insert(group->ID);
            group->CalculateGroupStatus();

            NKikimrBlobDepot::TBlobDepotConfig config;
            config.SetVirtualGroupId(groupId);
            config.SetIsDecommittingGroup(true);

            auto *profiles = config.MutableChannelProfiles();
            profiles->CopyFrom(cmd.GetChannelProfiles());
            if (profiles->empty()) {
                const auto& sp = StoragePools.Get();
                const auto it = sp.find(group->StoragePoolId);
                if (it == sp.end()) {
                    throw TExError() << "no storage pool found" << TErrorParams::GroupId(groupId)
                        << TErrorParams::BoxId(std::get<0>(group->StoragePoolId))
                        << TErrorParams::StoragePoolId(std::get<1>(group->StoragePoolId));
                }
                const TString& storagePoolName = it->second.Name;

                bool found = false;
                for (const auto& [_, pool] : sp) {
                    if (pool.Name == storagePoolName) {
                        if (found) {
                            throw TExError() << "ambiguous storage pool name"
                                << TErrorParams::StoragePoolName(pool.Name);
                        } else {
                            found = true;
                        }
                    }
                }

                ui32 numPhysicalGroups = 0;
                const auto& storagePoolGroups = StoragePoolGroups.Get();
                for (auto it = storagePoolGroups.lower_bound({group->StoragePoolId, Min<TGroupId>()});
                        it != storagePoolGroups.end() && it->first == group->StoragePoolId; ++it) {
                    if (const TGroupInfo *poolGroup = Groups.Find(it->second)) {
                        // piles of bridged groups are not visible on their own, data goes to their proxy groups
                        numPhysicalGroups += poolGroup->IsPhysicalGroup() && !poolGroup->IsBridgePileOnly();
                    } else {
                        throw TExGroupNotFound(it->second.GetRawId())
                            << TErrorParams::BoxId(std::get<0>(group->StoragePoolId))
                            << TErrorParams::StoragePoolId(std::get<1>(group->StoragePoolId));
                    }
                }
                if (!numPhysicalGroups) {
                    throw TExError() << "no physical groups for decommission"
                        << TErrorParams::BoxId(std::get<0>(group->StoragePoolId))
                        << TErrorParams::StoragePoolId(std::get<1>(group->StoragePoolId));
                }

                auto *sys = profiles->Add();
                sys->SetStoragePoolName(storagePoolName);
                sys->SetChannelKind(NKikimrBlobDepot::TChannelKind::System);
                sys->SetCount(2);
                auto *data = profiles->Add();
                data->SetStoragePoolName(storagePoolName);
                data->SetChannelKind(NKikimrBlobDepot::TChannelKind::Data);
                data->SetCount(Min<ui32>(250, numPhysicalGroups));
            }

            const bool success = config.SerializeToString(&group->BlobDepotConfig.ConstructInPlace());
            Y_ABORT_UNLESS(success);
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TCancelVirtualGroup& cmd, TStatus& /*status*/) {
        const TGroupId groupId = TGroupId::FromProto(&cmd, &NKikimrBlobStorage::TCancelVirtualGroup::GetGroupId);
        TGroupInfo *group = Groups.FindForUpdate(groupId);
        if (!group) {
            throw TExGroupNotFound(groupId.GetRawId());
        } else if (!group->VirtualGroupState) {
            throw TExError() << "group is not virtual" << TErrorParams::GroupId(groupId.GetRawId());
        } else if (auto s = group->VirtualGroupState; s != NKikimrBlobStorage::EVirtualGroupState::NEW && s != NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED) {
            throw TExError() << "group is not in new/error state" << TErrorParams::GroupId(groupId.GetRawId());
        } else if (auto s = group->DecommitStatus; s != NKikimrBlobStorage::TGroupDecommitStatus::NONE && s != NKikimrBlobStorage::TGroupDecommitStatus::PENDING) {
            throw TExError() << "group decommit status is not NONE/PENDING" << TErrorParams::GroupId(groupId.GetRawId());
        } else {
            group->DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::NONE;
            group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::DELETING;
            group->NeedAlter = true;
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TReconfigureVirtualGroup& cmd,
            TStatus& /*status*/) {
        if (!cmd.GetName()) {
            throw TExError() << "TReconfigureVirtualGroup.Name must be set and be nonempty";
        }
        const auto& owners = FindBlobDepotOwners({EClaim::GroupName, {}, cmd.GetName()});
        if (owners.empty()) {
            throw TExError() << "Virtual group not found by designated name";
        } else if (owners.size() > 1) {
            throw TExError() << "Duplicate virtual group name";
        }
        const auto groupId = *owners.begin();
        TGroupInfo *group = Groups.FindForUpdate(groupId);
        if (!group->BlobDepotConfig) {
            Y_DEBUG_ABORT(); // this should never happen
            throw TExError() << "Virtual group does not contain BlobDepotConfig";
        }

        // parse current config
        bool validatedExclusiveClaim = false;
        NKikimrBlobDepot::TBlobDepotConfig config;
        bool success = config.ParseFromString(*group->BlobDepotConfig);
        if (!success) {
            Y_DEBUG_ABORT();
            throw TExError() << "Failed to parse BlobDepotConfig";
        }

        const auto& previous = config.GetS3BackendSettings();
        const auto& next = cmd.HasS3BackendSettings() ? cmd.GetS3BackendSettings() : previous;
        const bool hasExistingBucket = !previous.GetSettings().GetBucket().empty();
        const bool hasNextBucket = !next.GetSettings().GetBucket().empty();
        const auto& storageName = cmd.HasMigratedS3Name() ? cmd.GetMigratedS3Name() : config.GetName();
        const bool storageNameChanged = storageName != config.GetName();
        if (cmd.HasMigratedS3Name()) {
            if (!cmd.GetConfirmS3NamespaceMigration() || !hasNextBucket) {
                throw TExError() << "MigratedS3Name requires ConfirmS3NamespaceMigration and a destination bucket";
            }
            const auto normalized = NBlobDepot::MakeS3BasePath({}, storageName);
            const auto nameError = NBlobDepot::ValidateS3BasePath(normalized);
            if (normalized != storageName || nameError) {
                throw TExError() << "MigratedS3Name must be a nonempty canonical S3 path without dot segments";
            }
            const auto previousNameError = NBlobDepot::ValidateS3BasePath(NBlobDepot::MakeS3BasePath({}, config.GetName()));
            if (hasExistingBucket && storageNameChanged && !previousNameError) {
                throw TExError() << "MigratedS3Name may only replace a malformed legacy name once S3 is configured";
            }
        }
        if (cmd.GetConfirmS3Empty() && (!cmd.HasS3BackendSettings() || hasNextBucket)) {
            throw TExError() << "ConfirmS3Empty requires explicit S3BackendSettings without a bucket";
        }
        if (hasExistingBucket && !hasNextBucket && !cmd.GetConfirmS3Empty()) {
            throw TExError() << "Removing the S3 route requires ConfirmS3Empty after verifying that no S3 references,"
                << " unfinished writes, or pending deletes remain";
        }
        const bool namespaceChanged = GetS3Namespace(config) != GetS3Namespace(next, storageName);
        const bool routeChanged = hasExistingBucket && hasNextBucket && cmd.HasS3BackendSettings() &&
            !google::protobuf::util::MessageDifferencer::Equals(NBlobDepot::GetS3Route(previous), NBlobDepot::GetS3Route(next));
        const bool coolingChanged = previous.HasAsyncMode() != next.HasAsyncMode() ||
            !google::protobuf::util::MessageDifferencer::Equals(previous.GetAsyncMode(), next.GetAsyncMode());
        // A name-only command requests validation of the stored namespace, including for a legacy sync depot.
        // Credential rotation leaves its existing authorization intact.
        const bool validateStoredNamespace = hasNextBucket && !cmd.HasS3BackendSettings();
        const bool authorizeCooling = AppData()->FeatureFlags.GetEnableBlobDepotDataCooling() && next.HasAsyncMode() &&
            (!previous.HasAsyncMode() || (!config.GetS3CoolingAuthorized() && (!cmd.HasS3BackendSettings() || coolingChanged)));
        // Removing a redundant slash can enable legacy LIST even though point requests address the same keys.
        const bool patternChanged = previous.GetSettings().GetObjectKeyPattern() != next.GetSettings().GetObjectKeyPattern();
        const bool claimsChanged = namespaceChanged || storageNameChanged;
        const bool checkOwnership = patternChanged || claimsChanged || routeChanged || validateStoredNamespace || authorizeCooling;
        if (coolingChanged || checkOwnership) {
            if (const auto error = NBlobDepot::ValidateDataCooling(next)) {
                throw TExError() << *error;
            }
        }
        if (checkOwnership) {
            // A retained owner can rotate access to the same objects. This command neither acquires
            // a new path nor establishes the exclusive ownership required for GC and cooling.
            const bool keepRetiredRoute = routeChanged && cmd.GetConfirmS3RouteChange() &&
                !claimsChanged && !patternChanged && !coolingChanged && KeepsRetiredS3Namespace(groupId, next, storageName);
            if (!keepRetiredRoute) {
                CheckBlobDepotS3Prefix(next, cmd.GetName(), storageName, groupId);
                validatedExclusiveClaim = true;
            }
            if (hasExistingBucket && hasNextBucket && namespaceChanged && !cmd.GetConfirmS3NamespaceMigration()) {
                throw TExError() << "Changing the S3 bucket or key prefix requires migrating existing BlobDepot objects"
                    << " and setting ConfirmS3NamespaceMigration";
            }
            CheckS3ObjectKeyLength(next, storageName);
        }
        // Moving the namespace already requires fencing old writes and copying the data to the new route.
        if (routeChanged && !namespaceChanged && !cmd.GetConfirmS3RouteChange()) {
            throw TExError() << "Changing the configured S3 route requires ConfirmS3RouteChange to confirm that "
                "the new route accesses the same objects, including in-flight writes";
        }

        if (previous.HasAsyncMode() || next.HasAsyncMode()) {
            config.SetS3NamespacePinned(true);
        }
        if (claimsChanged) {
            ChangeLiveBlobDepotClaims(*group, config, false);
            config.ClearS3NamespaceValidated();
            config.ClearS3CoolingAuthorized();
        }
        if (cmd.HasMigratedS3Name()) {
            if (storageNameChanged) {
                config.ClearGeneratedS3NamespaceId();
            }
            config.SetName(storageName);
        }
        if (cmd.HasS3BackendSettings() && !hasNextBucket) {
            config.ClearS3BackendSettings();
        } else if (cmd.HasS3BackendSettings()) {
            config.MutableS3BackendSettings()->CopyFrom(next);
        }
        if (validatedExclusiveClaim) {
            RecordS3NamespaceValidation(config);
        }
        if (claimsChanged) {
            ChangeLiveBlobDepotClaims(*group, config, true);
        }
        // overwrite the config
        success = config.SerializeToString(&group->BlobDepotConfig.ConstructInPlace());
        Y_ABORT_UNLESS(success);

        // start altering machinery
        group->NeedAlter = true;
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TRecommissionGroups& cmd, TStatus& /*status*/) {
        for (const ui32 groupId : cmd.GetGroupIds()) {
            TGroupInfo *group = Groups.FindForUpdate(TGroupId::FromValue(groupId));
            if (!group) {
                throw TExGroupNotFound(groupId);
            }

            if (TGroupID(groupId).ConfigurationType() != EGroupConfigurationType::Dynamic) {
                // group can't be recommissioned, it could never have physical slots
                throw TExError() << "group is not dynamic one" << TErrorParams::GroupId(groupId);
            }

            if (!group->VirtualGroupState) {
                continue; // recommissioning is not required -- group has no virtual part in it
            }

            if (group->DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::RECOMMISSIONING) {
                continue; // recommissioning is already underway
            }

            if (group->DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::NONE &&
                    group->VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::DELETING) {
                continue; // tablet is already being deleted
            }

            if (group->DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::PENDING &&
                    (group->VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::NEW ||
                     group->VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED)) {
                // the new tablet is in the process of creation, just cancel it
                group->DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::NONE;
                group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::DELETING;
            } else {
                Y_DEBUG_ABORT_UNLESS(group->VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::WORKING);
                if (group->VirtualGroupState != NKikimrBlobStorage::EVirtualGroupState::WORKING) {
                    throw TExError() << "virtual group is not in WORKING state" << TErrorParams::GroupId(groupId);
                }
                // bridged group keeps its VDisks in its piles, and the one decommitted completely would get VDisks
                // allocated outside of any pile
                if (group->BridgeGroupInfo || (group->DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::DONE &&
                        StoragePools.Get().at(group->StoragePoolId).BridgeMode)) {
                    throw TExError() << "recommissioning of groups in bridge mode is not supported"
                        << TErrorParams::GroupId(groupId);
                }

                switch (group->DecommitStatus) {
                    case NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS:
                        Y_DEBUG_ABORT_UNLESS(!group->VDisksInGroup.empty());
                        if (!group->VDisksInGroup) {
                            throw TExError() << "group contains no VDisks despite IN_PROGRESS decommission state"
                                << TErrorParams::GroupId(groupId);
                        }
                        break;

                    case NKikimrBlobStorage::TGroupDecommitStatus::DONE:
                        Y_DEBUG_ABORT_UNLESS(group->VDisksInGroup.empty());
                        if (group->VDisksInGroup) {
                            throw TExError() << "group contains VDisks despite DONE decommission state"
                                << TErrorParams::GroupId(groupId);
                        }
                        // kindly ask futher logic to allocate a group here
                        Fit.PoolsAndGroups.emplace(group->StoragePoolId, group->ID);
                        Fit.GroupsToAllocate.insert(group->ID);
                        break;

                    default:
                        Y_DEBUG_ABORT("group is not in IN_PROGRESS/DONE decommission state");
                        throw TExError() << "group is not in IN_PROGRESS/DONE decommission state"
                            << TErrorParams::GroupId(groupId);
                }

                // switch group to recommissioning state, start machinery accordingly
                group->DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::RECOMMISSIONING;
            }

            // update state with the agents
            group->NeedAlter = true;

            // spin generation to deliver decommissioning state change to proxies
            GroupContentChanged.insert(group->ID);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    class TBlobStorageController::TVirtualGroupSetupMachine : public TActorBootstrapped<TVirtualGroupSetupMachine> {
        // a token to control lifetime within transactions
        struct TToken {};
        std::shared_ptr<TToken> Token = std::make_shared<TToken>();

        TBlobStorageController *Self;
        const TActorId ControllerId;
        const TGroupId GroupId;
        const bool Deleting = false;
        bool HiveDeleteAcknowledged = false;

    private:
        class TTxUpdateGroup : public TTransactionBase<TBlobStorageController> {
            TVirtualGroupSetupMachine* const Machine;
            const TActorId MachineId;
            const TGroupId GroupId;
            const std::weak_ptr<TToken> Token;
            std::optional<TConfigState> State;
            const std::function<bool(TGroupInfo&, TConfigState&)> Callback;

        public:
            TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_GROUP; }

            TTxUpdateGroup(TVirtualGroupSetupMachine *machine, std::function<bool(TGroupInfo&, TConfigState&)>&& callback)
                : TTransactionBase(machine->Self)
                , Machine(machine)
                , MachineId(Machine->SelfId())
                , GroupId(Machine->GroupId)
                , Token(Machine->Token)
                , Callback(std::move(callback))
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                if (Token.expired()) {
                    return true; // actor is already dead
                }
                if (const TGroupInfo *group = Self->FindGroup(GroupId); !group || group->VirtualGroupSetupMachineId != MachineId) {
                    return true; // another machine is already running
                }
                State.emplace(*Self, Self->HostRecords, TActivationContext::Now(), TActivationContext::Monotonic());
                TGroupInfo *group = State->Groups.FindForUpdate(GroupId);
                Y_ABORT_UNLESS(group);
                if (Callback(*group, *State)) {
                    group->CalculateGroupStatus();
                    group->CalculateLayoutStatus(Self, [&] {
                        const auto& pools = State->StoragePools.Get();
                        if (const auto it = pools.find(group->StoragePoolId); it != pools.end()) {
                            return TGroupGeometryInfo::FromStoragePool(group->Topology->GType,
                                State->GetStoragePoolForPile(group->StoragePoolId, group->BridgePileId));
                        }
                        Y_DEBUG_ABORT();
                        return TGroupGeometryInfo();
                    });
                } else {
                    State->DeleteExistingGroup(GroupId); // group is freed here
                }
                if (auto error = Self->ValidateAndCommitConfigUpdate(State, TConfigTxFlags::SuppressAll(), txc)) {
                    YDB_LOG_ERROR("Failed to commit update",
                        {"marker", "BSCVG08"},
                        {"virtualGroupId", GroupId},
                        {"error", *error});
                }
                return true;
            }

            void Complete(const TActorContext&) override {
                if (State) {
                    State->ApplyConfigUpdates();
                }
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Bootstrap, 0, MachineId, {}, nullptr, 0));
            }
        };

        class TTxFinishHiveDeletion : public TTransactionBase<TBlobStorageController> {
            const TActorId MachineId;
            const TGroupId GroupId;

        public:
            explicit TTxFinishHiveDeletion(TVirtualGroupSetupMachine* machine)
                : TTransactionBase(machine->Self)
                , MachineId(machine->SelfId())
                , GroupId(machine->GroupId)
            {}

            TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_DELETE_BLOB_DEPOT; }

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                NIceDb::TNiceDb(txc.DB).Table<Schema::BlobDepotDeleteQueue>().Key(GroupId.GetRawId()).Delete();
                Self->BlobDepotDeleteQueue.erase(GroupId);
                return true;
            }

            void Complete(const TActorContext& ctx) override {
                ctx.Send(MachineId, new TEvents::TEvPoison);
                if (auto it = Self->RetiredS3Namespaces.find(GroupId); it != Self->RetiredS3Namespaces.end()) {
                    Self->StartRetiredS3Cleanup(GroupId, it->second);
                }
            }
        };

    public:
        TVirtualGroupSetupMachine(TBlobStorageController *self, TGroupInfo& group)
            : Self(self)
            , ControllerId(Self->SelfId())
            , GroupId(group.ID)
        {}

        TVirtualGroupSetupMachine(TBlobStorageController *self, TGroupId groupId)
            : Self(self)
            , ControllerId(Self->SelfId())
            , GroupId(groupId)
            , Deleting(true)
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc);
            if (Expired()) { // BS_CONTROLLER is already dead
                return PassAway();
            }

            if (Deleting) {
                if (HiveDeleteAcknowledged) {
                    return;
                }
                const auto& info = Self->BlobDepotDeleteQueue.at(GroupId);
                if (info.HiveId && !HivePipeId) {
                    HiveDelete(*info.HiveId, info.BlobDepotId);
                } else if (!info.HiveId) {
                    OnHiveDeletionComplete();
                }
                return;
            }

            TGroupInfo *group = GetGroup();
            if (!group->VirtualGroupState) { // group was deleted or reset to non-decommitting during the last transaction
                return PassAway();
            }

            YDB_LOG_DEBUG("Bootstrap",
                {"marker", "BSCVG01"},
                {"groupId", GroupId},
                {"state", NKikimrBlobStorage::EVirtualGroupState_Name(*group->VirtualGroupState)},
                {"needAlter", group->NeedAlter});

            switch (*group->VirtualGroupState) {
                case NKikimrBlobStorage::EVirtualGroupState::NEW:
                    HiveCreate(group);
                    break;

                case NKikimrBlobStorage::EVirtualGroupState::WORKING:
                    if (group->NeedAlter.GetOrElse(false)) {
                        return ConfigureBlobDepot();
                    }
                    [[fallthrough]];
                case NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED:
                    IssueNodeNotifications(group);
                    PassAway();
                    break;

                case NKikimrBlobStorage::EVirtualGroupState::DELETING:
                    HiveDelete(group);
                    break;

                default:
                    Y_ABORT();
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        static NKikimrBlobDepot::TBlobDepotConfig GetConfig(const TGroupInfo* group) {
            NKikimrBlobDepot::TBlobDepotConfig config;
            Y_ABORT_UNLESS(group->BlobDepotConfig && config.ParseFromString(*group->BlobDepotConfig));
            return config;
        }

        template<typename T>
        void UpdateBlobDepotConfig(T&& callback) {
            Self->Execute(std::make_unique<TTxUpdateGroup>(this, [callback](TGroupInfo& group, TConfigState&) {
                auto config = GetConfig(&group);
                callback(config);
                TString data;
                const bool success = config.SerializeToString(&data);
                Y_ABORT_UNLESS(success);
                group.BlobDepotConfig = data;
                return true;
            }));
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        TActorId HivePipeId;
        TActorId TenantHivePipeId;
        TActorId BlobDepotPipeId;
        ui64 ApplyConfigTxId = 0;
        TString ConfigBeingApplied;
        ui64 RootHiveId = 0;
        bool TenantHiveInvalidated = false;
        bool TenantHiveInvalidateInProgress = false;
        bool IsDecommittingGroup = false;

        void HiveCreate(TGroupInfo *group) {
            auto config = GetConfig(group);
            IsDecommittingGroup = config.GetIsDecommittingGroup();

            if (config.HasTabletId()) {
                ConfigureBlobDepot();
            } else if (!group->HiveId) {
                HiveResolve(group);
            } else if (TenantHiveInvalidateInProgress && TenantHivePipeId) {
                // tenant hive storage pool invalidation still in progress, wait
            } else if (IsDecommittingGroup && config.HasTenantHiveId() && !TenantHiveInvalidated) {
                TenantHivePipeId = Register(NTabletPipe::CreateClient(SelfId(), config.GetTenantHiveId(),
                    NTabletPipe::TClientRetryPolicy::WithRetries()));
                HiveInvalidateGroups(TenantHivePipeId, group);
                TenantHiveInvalidateInProgress = true;
            } else if (!HivePipeId) {
                Y_ABORT_UNLESS(group->HiveId);
                HivePipeId = Register(NTabletPipe::CreateClient(SelfId(), *group->HiveId, NTabletPipe::TClientRetryPolicy::WithRetries()));
                if (IsDecommittingGroup) {
                    HiveInvalidateGroups(HivePipeId, group);
                }
            } else {
                HiveCreateTablet(group);
            }
        }

        void HiveResolve(TGroupInfo *group) {
            Y_ABORT_UNLESS(group->Database);
            Y_ABORT_UNLESS(!group->HiveId);
            const TString& database = *group->Database;
            TString domainPath;

            const auto& domainsInfo = AppData()->DomainsInfo;
            if (const auto& domain = domainsInfo->Domain) {
                domainPath = TStringBuilder() << '/' << domain->Name;
                if (database == domainPath || database.StartsWith(TStringBuilder() << domainPath << '/')) {
                    RootHiveId = domainsInfo->GetHive();
                    if (RootHiveId == TDomainsInfo::BadTabletId) {
                        return CreateFailed(TStringBuilder() << "failed to resolve Hive -- BadTabletId for Database# " << database);
                    }
                }
            }

            auto req = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            req->DatabaseName = domainPath;

            auto& item = req->ResultSet.emplace_back();
            item.Path = NKikimr::SplitPath(database);
            item.RedirectRequired = false;
            item.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(req));
        }

        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr ev) {
            auto& response = *ev->Get()->Request;
            Y_ABORT_UNLESS(response.ResultSet.size() == 1);
            auto& item = response.ResultSet.front();
            auto& domainInfo = item.DomainInfo;

            YDB_LOG_DEBUG("TEvDescribeSchemeResult",
                {"marker", "BSCVG16"},
                {"groupId", GroupId},
                {"result", response.ToString(*AppData()->TypeRegistry)});

            if (item.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok || !domainInfo || !item.DomainDescription) {
                return CreateFailed(TStringBuilder() << "failed to resolve Hive -- erroneous reply from SchemeCache or not a domain"
                    << " Response# " << response.ToString(*AppData()->TypeRegistry)
                    << " DomainInfo# " << static_cast<bool>(domainInfo)
                    << " DomainDescription# " << static_cast<bool>(item.DomainDescription));
            } else if (const auto& params = domainInfo->Params; !params.HasHive() && !RootHiveId) {
                return CreateFailed("failed to resolve Hive -- no Hive in SchemeCache reply");
            } else {
                auto& domain = item.DomainDescription->Description;

                THashSet<std::tuple<TString, TString>> storagePools; // name, kind
                THashSet<TString> storagePoolNames;
                for (const auto& item : domain.GetStoragePools()) {
                    storagePools.emplace(item.GetName(), item.GetKind());
                    storagePoolNames.insert(item.GetName());
                }

                auto *group = GetGroup();
                auto spIt = Self->StoragePools.find(group->StoragePoolId);
                Y_ABORT_UNLESS(spIt != Self->StoragePools.end());
                auto& sp = spIt->second;

                if (!storagePools.contains(std::make_tuple(sp.Name, sp.Kind)) && !sp.Name.StartsWith(*group->Database + ':')) {
                    return CreateFailed("failed to resolve Hive -- group storage pool is not listed in database");
                }

                const auto config = GetConfig(group);
                for (const auto& item : config.GetChannelProfiles()) {
                    const TString& name = item.GetStoragePoolName();
                    if (!storagePoolNames.contains(name) && !name.StartsWith(*group->Database + ':')) {
                        return CreateFailed("failed to resolve Hive -- tablet underlying storage pool is not listed in database");
                    }
                }

                const ui64 hiveId = params.HasHive() && !IsDecommittingGroup ? params.GetHive() : RootHiveId;
                if (!hiveId) {
                    return CreateFailed("failed to resolve Hive -- Hive is zero");
                }

                auto descr = item.DomainDescription;

                Self->Execute(std::make_unique<TTxUpdateGroup>(this, [=, this](TGroupInfo& group, TConfigState&) {
                    auto config = GetConfig(&group);
                    if (hiveId != RootHiveId) {
                        config.SetTenantHiveId(hiveId);
                    }
                    config.MutableHiveParams()->MutableObjectDomain()->CopyFrom(descr->Description.GetDomainKey());
                    TString data;
                    const bool success = config.SerializeToString(&data);
                    Y_ABORT_UNLESS(success);
                    group.BlobDepotConfig = data;
                    group.HiveId = RootHiveId;
                    return true;
                }));
            }
        }

        void HiveCreateTablet(TGroupInfo *group) {
            auto ev = std::make_unique<TEvHive::TEvCreateTablet>();

            auto config = GetConfig(group);
			if (config.HasHiveParams()) {
                ev->Record.CopyFrom(config.GetHiveParams());
            }
            ev->Record.SetOwner(Self->TabletID());
            ev->Record.SetOwnerIdx(GroupId.GetRawId());
            ev->Record.SetTabletType(TTabletTypes::BlobDepot);
            ev->Record.ClearBindedChannels();

            for (const auto& item : config.GetChannelProfiles()) {
                const TString& storagePoolName = item.GetStoragePoolName();
                NKikimrStoragePool::TChannelBind binding;
                binding.SetStoragePoolName(storagePoolName);
                if (config.GetIsDecommittingGroup()) {
                    binding.SetPhysicalGroupsOnly(true);
                }
                for (ui32 i = 0; i < item.GetCount(); ++i) {
                    ev->Record.AddBindedChannels()->CopyFrom(binding);
                }
            }

            YDB_LOG_INFO("Sending TEvCreateTablet",
                {"marker", "BSCVG00"},
                {"groupId", group->ID},
                {"hiveId", *group->HiveId},
                {"msg", ev->Record});

            NTabletPipe::SendData(SelfId(), HivePipeId, ev.release());
        }

        void HiveInvalidateGroups(TActorId pipeId, TGroupInfo *group) {
            auto config = GetConfig(group);
            Y_ABORT_UNLESS(config.GetIsDecommittingGroup());

            auto invalidateEv = std::make_unique<TEvHive::TEvInvalidateStoragePools>();
            auto& record = invalidateEv->Record;
            std::unordered_set<TString> names;
            for (const auto& item : config.GetChannelProfiles()) {
                const TString& storagePoolName = item.GetStoragePoolName();
                if (names.insert(storagePoolName).second) {
                    record.AddStoragePoolName(storagePoolName);
                }
            }
            NTabletPipe::SendData(SelfId(), pipeId, invalidateEv.release());

            NTabletPipe::SendData(SelfId(), pipeId, new TEvHive::TEvReassignOnDecommitGroup(GroupId.GetRawId()));
        }

        void HiveDelete(TGroupInfo *group) {
            auto config = GetConfig(group);
            if (!config.GetHiveContacted() || !group->HiveId) {
                // hive has never been contacted, so there is no possibility this tablet was created
                Y_ABORT_UNLESS(!config.HasTabletId());
                return DeleteBlobDepot();
            }

            HiveDelete(*group->HiveId, config.HasTabletId() ? MakeMaybe(config.GetTabletId()) : Nothing());
        }

        void HiveDelete(ui64 hiveId, TMaybe<ui64> tabletId) {
            Y_ABORT_UNLESS(!HivePipeId);
            Y_ABORT_UNLESS(hiveId);
            HivePipeId = Register(NTabletPipe::CreateClient(SelfId(), hiveId, NTabletPipe::TClientRetryPolicy::WithRetries()));

            auto ev = tabletId
                ? std::make_unique<TEvHive::TEvDeleteTablet>(Self->TabletID(), GroupId.GetRawId(), *tabletId, 0)
                : std::make_unique<TEvHive::TEvDeleteTablet>(Self->TabletID(), GroupId.GetRawId(), 0);

            YDB_LOG_INFO("Sending TEvDeleteTablet",
                {"marker", "BSCVG12"},
                {"groupId", GroupId.GetRawId()},
                {"hiveId", hiveId},
                {"msg", ev->Record});

            NTabletPipe::SendData(SelfId(), HivePipeId, ev.release());
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
            YDB_LOG_DEBUG("Received TEvClientConnected",
                {"marker", "BSCVG02"},
                {"groupId", GroupId.GetRawId()},
                {"status", ev->Get()->Status},
                {"clientId", ev->Get()->ClientId},
                {"hivePipeId", HivePipeId},
                {"blobDepotPipeId", BlobDepotPipeId});

            if (ev->Get()->Status != NKikimrProto::OK) {
                OnPipeError(ev->Get()->ClientId);
            } else if (ev->Get()->ClientId == HivePipeId && !Deleting) {
                TGroupInfo *group = GetGroup();
                if (group->VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::NEW) {
                    auto config = GetConfig(group);
                    if (!config.GetHiveContacted()) {
                        UpdateBlobDepotConfig([](auto& config) {
                            config.SetHiveContacted(true);
                        });
                    } else {
                        Bootstrap();
                    }
                }
            }
        }

        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
            YDB_LOG_DEBUG("Received TEvClientDestroyed",
                {"marker", "BSCVG03"},
                {"groupId", GroupId.GetRawId()},
                {"clientId", ev->Get()->ClientId},
                {"hivePipeId", HivePipeId},
                {"blobDepotPipeId", BlobDepotPipeId});

            OnPipeError(ev->Get()->ClientId);
        }

        void OnPipeError(TActorId clientId) {
            for (auto *pipeId : {&HivePipeId, &TenantHivePipeId, &BlobDepotPipeId}) {
                if (*pipeId == clientId) {
                    *pipeId = {};
                    Bootstrap();
                    break;
                }
            }
        }

        void Handle(TEvHive::TEvInvalidateStoragePoolsReply::TPtr ev) {
            YDB_LOG_INFO("Received TEvInvalidateStoragePoolsReply",
                {"marker", "BSCVG06"},
                {"groupId", GroupId.GetRawId()},
                {"msg", ev->Get()->Record});
        }

        void Handle(TEvHive::TEvReassignOnDecommitGroupReply::TPtr ev) {
            YDB_LOG_INFO("Received TEvReassignOnDecommitGroupReply",
                {"marker", "BSCVG07"},
                {"groupId", GroupId.GetRawId()},
                {"msg", ev->Get()->Record});
            if (TenantHiveInvalidateInProgress) {
                NTabletPipe::CloseAndForgetClient(SelfId(), TenantHivePipeId);
                TenantHiveInvalidateInProgress = false;
                TenantHiveInvalidated = true;
                Bootstrap();
            }
        }

        void Handle(TEvHive::TEvCreateTabletReply::TPtr ev) {
            YDB_LOG_INFO("Received TEvCreateTabletReply",
                {"marker", "BSCVG04"},
                {"groupId", GroupId.GetRawId()},
                {"msg", ev->Get()->Record});

            NTabletPipe::CloseAndForgetClient(SelfId(), HivePipeId);

            TGroupInfo *group = GetGroup();

            auto& record = ev->Get()->Record;
            if (record.GetStatus() == NKikimrProto::OK || record.GetStatus() == NKikimrProto::ALREADY) {
                auto config = GetConfig(group);
                Y_ABORT_UNLESS(!config.HasTabletId());
                const ui64 tabletId = record.GetTabletID();
                UpdateBlobDepotConfig([tabletId](auto& config) {
                    config.SetTabletId(tabletId);
                });
            } else {
                CreateFailed(TStringBuilder() << "failed to create BlobDepot tablet"
                    << " Reason# " << NKikimrHive::EErrorReason_Name(record.GetErrorReason())
                    << " Status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus()));
            }
        }

        void CreateFailed(const TString& error) {
            Self->Execute(std::make_unique<TTxUpdateGroup>(this, [=](TGroupInfo& group, TConfigState&) {
                group.VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED;
                group.NeedAlter = false;
                group.ErrorReason = error;
                return true;
            }));
        }

        void Handle(TEvHive::TEvTabletCreationResult::TPtr ev) {
            YDB_LOG_INFO("Received TEvTabletCreationResult",
                {"marker", "BSCVG05"},
                {"groupId", GroupId.GetRawId()},
                {"msg", ev->Get()->Record});
        }

        void Handle(TEvHive::TEvDeleteTabletReply::TPtr ev) {
            YDB_LOG_INFO("Received TEvDeleteTabletReply",
                {"marker", "BSCVG13"},
                {"groupId", GroupId.GetRawId()},
                {"msg", ev->Get()->Record});
            if (Deleting) {
                if (HiveDeleteAcknowledged) {
                    return;
                }
                NTabletPipe::CloseAndForgetClient(SelfId(), HivePipeId);
                if (ev->Get()->Record.GetStatus() == NKikimrProto::INVALID_OWNER && ev->Get()->Record.HasForwardRequest()) {
                    HiveDelete(ev->Get()->Record.GetForwardRequest().GetHiveTabletId(), Self->BlobDepotDeleteQueue.at(GroupId).BlobDepotId);
                } else if (ev->Get()->Record.GetStatus() == NKikimrProto::OK) {
                    OnHiveDeletionComplete();
                } else {
                    Schedule(TDuration::Minutes(1), new TEvents::TEvBootstrap);
                }
            } else {
                DeleteBlobDepot();
            }
        }

        void ConfigureBlobDepot() {
            if (BlobDepotPipeId) {
                return;
            }
            TGroupInfo *group = GetGroup();
            YDB_LOG_DEBUG("ConfigureBlobDepot",
                {"marker", "BSCVG14"},
                {"groupId", group->ID.GetRawId()});
            auto config = GetConfig(group);
            Y_ABORT_UNLESS(config.HasTabletId());
            Y_ABORT_UNLESS(!group->BlobDepotId || group->BlobDepotId == config.GetTabletId());
            const ui64 tabletId = config.GetTabletId();
            BlobDepotPipeId = Register(NTabletPipe::CreateClient(SelfId(), tabletId,
                NTabletPipe::TClientRetryPolicy::WithRetries()));
            auto ev = std::make_unique<TEvBlobDepot::TEvApplyConfig>();
            if (ConfigBeingApplied != *group->BlobDepotConfig || !ApplyConfigTxId) {
                ConfigBeingApplied = *group->BlobDepotConfig;
                ++ApplyConfigTxId;
            }
            ev->Record.SetTxId(ApplyConfigTxId);
            *ev->Record.MutableConfig() = std::move(config);
            NTabletPipe::SendData(SelfId(), BlobDepotPipeId, ev.release());
        }

        void DeleteBlobDepot() {
            auto *group = GetGroup();
            YDB_LOG_DEBUG("DeleteBlobDepot",
                {"marker", "BSCVG15"},
                {"groupId", group->ID.GetRawId()});
            Self->Execute(std::make_unique<TTxUpdateGroup>(this, [](TGroupInfo& group, TConfigState& state) {
                // a group with data stays without its BlobDepot: an ordinary one keeps it on its VDisks, and a bridge
                // proxy group, which has none, on its parts
                if (group.VDisksInGroup || group.BridgeGroupInfo) {
                    state.DetachBlobDepot(group, GetConfig(&group));
                    return true;
                } else {
                    return false;
                }
            }));
        }

        void OnHiveDeletionComplete() {
            HiveDeleteAcknowledged = true;
            YDB_LOG_DEBUG("BlobDepot deletion in Hive complete",
                {"marker", "BSCVG18"},
                {"groupId", GroupId.GetRawId()});
            Self->Execute(std::make_unique<TTxFinishHiveDeletion>(this));
        }

        void Handle(TEvBlobDepot::TEvApplyConfigResult::TPtr ev) {
            if (!BlobDepotPipeId || ev->Get()->Record.GetTxId() != ApplyConfigTxId) {
                return;
            }
            NTabletPipe::CloseAndForgetClient(SelfId(), BlobDepotPipeId);

            Self->Execute(std::make_unique<TTxUpdateGroup>(this, [applied = std::move(ConfigBeingApplied)](TGroupInfo& group, TConfigState& state) {
                // A later reconfiguration can commit before the tablet acknowledges this one.
                // Bootstrap will send the newer configuration while NeedAlter is still set.
                if (*group.BlobDepotConfig != applied) {
                    return true;
                }
                group.VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::WORKING;
                auto config = GetConfig(&group);
                Y_ABORT_UNLESS(config.HasTabletId());
                group.BlobDepotId = config.GetTabletId();
                group.NeedAlter = false;
                if (group.DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::PENDING) {
                    group.DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS;
                    state.GroupContentChanged.insert(group.ID);
                }
                return true;
            }));
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void IssueNodeNotifications(TGroupInfo *group) {
            for (const TNodeId nodeId : std::exchange(group->WaitingNodes, {})) {
                TNodeInfo& node = Self->GetNode(nodeId);
                node.WaitingForGroups.erase(group->ID);
                auto ev = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, nodeId);
                TSet<TGroupId> groups;
                groups.insert(group->ID);
                Self->ReadGroups(groups, false, ev.get(), nodeId);
                Send(MakeBlobStorageNodeWardenID(nodeId), ev.release());
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void PassAway() override {
            if (!Expired() && !Deleting) {
                TGroupInfo *group = GetGroup();
                group->VirtualGroupSetupMachineId = {};
            }

            NTabletPipe::CloseClient(SelfId(), HivePipeId);
            NTabletPipe::CloseClient(SelfId(), BlobDepotPipeId);

            TActorBootstrapped::PassAway();
        }

        STATEFN(StateFunc) {
            if (Expired()) {
                return PassAway();
            }
            switch ([[maybe_unused]] const ui32 type = ev->GetTypeRewrite()) {
                cFunc(TEvents::TSystem::Poison, PassAway);
                cFunc(TEvents::TSystem::Bootstrap, Bootstrap);
                hFunc(TEvTabletPipe::TEvClientConnected, Handle);
                hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                hFunc(TEvHive::TEvCreateTabletReply, Handle);
                hFunc(TEvHive::TEvTabletCreationResult, Handle);
                hFunc(TEvHive::TEvInvalidateStoragePoolsReply, Handle);
                hFunc(TEvHive::TEvReassignOnDecommitGroupReply, Handle);
                hFunc(TEvHive::TEvDeleteTabletReply, Handle);
                hFunc(TEvBlobDepot::TEvApplyConfigResult, Handle);

                default:
                    Y_DEBUG_ABORT("unexpected event Type# %08" PRIx32, type);
            }
        }

        TGroupInfo *GetGroup() {
            Y_ABORT_UNLESS(!Deleting);
            TGroupInfo *res = Self->FindGroup(GroupId);
            Y_ABORT_UNLESS(res);
            return res;
        }

        bool Expired() const {
            if (!TlsActivationContext->Mailbox.FindActor(ControllerId.LocalId())) { // BS_CONTROLLER died
                return true;
            } else if (Deleting) {
                return !Self->BlobDepotDeleteQueue.contains(GroupId);
            } else if (const TGroupInfo *group = Self->FindGroup(GroupId); !group) { // group is deleted
                return true;
            } else if (group->VirtualGroupSetupMachineId != SelfId()) { // another machine is started
                return true;
            } else {
                return false;
            }
        }
    };

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TManageRetiredS3Namespace& cmd,
            TStatus&) {
        using TCmd = NKikimrBlobStorage::TManageRetiredS3Namespace;
        using TTable = Schema::RetiredS3Namespaces;
        const TGroupId groupId = TGroupId::FromProto(&cmd, &TCmd::GetGroupId);
        const auto it = Self.RetiredS3Namespaces.find(groupId);
        if (it == Self.RetiredS3Namespaces.end()) {
            throw TExError() << "No retired S3 namespace for group " << groupId;
        }
        auto& info = UpdatedRetiredS3Namespaces.try_emplace(groupId, it->second).first->second;
        if (info.Phase == TTable::Reserved) {
            if (cmd.GetConfirmCleanupComplete()) {
                return;
            }
            throw TExError() << "Cleanup already finished for group " << groupId;
        }
        auto config = info.GetConfig();
        switch (cmd.GetActionCase()) {
        case TCmd::kCredentials: {
            const auto& keys = cmd.GetCredentials();
            if (keys.GetAccessKey().empty() || keys.GetSecretKey().empty()) {
                throw TExError() << "Both S3 cleanup credentials are required";
            }
            auto* settings = config.MutableS3BackendSettings()->MutableSettings();
            settings->SetAccessKey(keys.GetAccessKey());
            settings->SetSecretKey(keys.GetSecretKey());
            settings->ClearSecretableAccessKey();
            settings->ClearSecretableSecretKey();
            info.S3Config = config.SerializeAsString();
            break;
        }
        case TCmd::kConfirmExclusiveOwnership:
            if (!cmd.GetConfirmExclusiveOwnership() || NBlobDepot::ValidateS3BasePath(GetS3Namespace(config).Prefix)) {
                throw TExError() << "Automatic cleanup requires a valid physical prefix and confirmed exclusive ownership";
            }
            if (Self.HasOtherS3NamespaceOwner(groupId, config)) {
                throw TExError() << "Another BlobDepot still owns this S3 namespace or awaits Hive deletion";
            }
            info.LegacyCleanupAuthorized = true;
            break;
        case TCmd::kConfirmCleanupComplete:
            if (!cmd.GetConfirmCleanupComplete() || BlobDepotDeleteQueue.Get().contains(groupId)) {
                throw TExError() << "External cleanup must be confirmed after Hive has deleted the tablet";
            }
            info.S3Config = MakeRetiredS3Reservation(config);
            info.Phase = TTable::Reserved;
            info.NextS3Sweep.Clear();
            break;
        case TCmd::ACTION_NOT_SET:
            throw TExError() << "Choose a retired S3 cleanup action";
        }
        // Retry a failed request with the new credentials immediately. A pending final grace
        // period was set by a successful sweep and must survive every operator update.
        if (std::exchange(info.ConsecutiveFailures, 0u)) {
            info.NextS3Sweep.Clear();
        }
    }

    class TBlobStorageController::TRetiredS3Cleanup : public TActorBootstrapped<TRetiredS3Cleanup> {
        using TTable = Schema::RetiredS3Namespaces;
        TBlobStorageController* const Self;
        const TActorId ControllerId;
        const TGroupId GroupId;
        TActorId SweepActor;
        bool Updating = false;
        bool StartCommitted = false;
        bool HasSlot = false;
        std::optional<std::list<TActorId>::iterator> QueueEntry;
        static constexpr ui32 MaxConcurrentSweeps = 4;

        class TTxUpdateSweep : public TTransactionBase<TBlobStorageController> {
            const TActorId SchedulerId;
            const TGroupId GroupId;
            const TTable::ESweepPhase Phase;
            const TInstant NextSweep;
            const ui32 Failures;

        public:
            TTxUpdateSweep(TRetiredS3Cleanup* scheduler, TTable::ESweepPhase phase, TInstant nextSweep, ui32 failures)
                : TTransactionBase(scheduler->Self)
                , SchedulerId(scheduler->SelfId())
                , GroupId(scheduler->GroupId)
                , Phase(phase)
                , NextSweep(nextSweep)
                , Failures(failures)
            {}

            TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_SCHEDULE_RETIRED_S3_SWEEP; }

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                auto& info = Self->RetiredS3Namespaces.at(GroupId);
                if (info.SchedulerId != SchedulerId) {
                    return true; // an operator replaced the scheduler and its credentials
                }
                NIceDb::TNiceDb db(txc.DB);
                auto row = db.Table<TTable>().Key(GroupId.GetRawId());
                row.Update<TTable::SweepPhase>(info.Phase = Phase);
                row.Update<TTable::ConsecutiveFailures>(info.ConsecutiveFailures = Failures);
                if (Phase == TTable::Reserved) {
                    info.S3Config = MakeRetiredS3Reservation(info.GetConfig());
                    row.Update<TTable::S3Config>(info.S3Config);
                    row.UpdateToNull<TTable::NextS3Sweep>();
                    info.NextS3Sweep.Clear();
                } else {
                    row.Update<TTable::NextS3Sweep>(NextSweep);
                    info.NextS3Sweep = NextSweep;
                }
                return true;
            }

            void Complete(const TActorContext& ctx) override {
                ctx.Send(SchedulerId, new TEvents::TEvBootstrap);
            }
        };

        void Persist(TTable::ESweepPhase phase, TInstant next = {}, ui32 failures = 0) {
            Updating = true;
            Self->Execute(std::make_unique<TTxUpdateSweep>(this, phase, next, failures));
        }

        void WakeNext() {
            if (Self->ActiveRetiredS3Sweeps < MaxConcurrentSweeps && !Self->RetiredS3SweepQueue.empty()) {
                Send(Self->RetiredS3SweepQueue.front(), new TEvents::TEvWakeup);
            }
        }

        bool AcquireSlot() {
            auto& queue = Self->RetiredS3SweepQueue;
            if (!QueueEntry) {
                QueueEntry = queue.insert(queue.end(), SelfId());
            }
            if (*QueueEntry != queue.begin() || Self->ActiveRetiredS3Sweeps >= MaxConcurrentSweeps) {
                return false;
            }
            queue.erase(*QueueEntry);
            QueueEntry.reset();
            ++Self->ActiveRetiredS3Sweeps;
            HasSlot = true;
            WakeNext();
            return true;
        }

        void ReleaseSlot() {
            if (QueueEntry) {
                Self->RetiredS3SweepQueue.erase(*QueueEntry);
                QueueEntry.reset();
            }
            if (std::exchange(HasSlot, false)) {
                --Self->ActiveRetiredS3Sweeps;
            }
            WakeNext();
        }

    public:
        TRetiredS3Cleanup(TBlobStorageController* self, TGroupId groupId)
            : Self(self)
            , ControllerId(self->SelfId())
            , GroupId(groupId)
        {}

        bool IsCurrent() const {
            return TlsActivationContext->Mailbox.FindActor(ControllerId.LocalId()) &&
                Self->RetiredS3Namespaces.at(GroupId).SchedulerId == SelfId();
        }

        void Bootstrap() {
            if (!IsCurrent()) {
                return PassAway();
            }
            Become(&TThis::StateWork);
            Run();
        }

        void Run() {
            if (Updating || SweepActor) {
                return;
            }
            auto& info = Self->RetiredS3Namespaces.at(GroupId);
            if (info.Phase == TTable::Reserved) {
                return PassAway();
            }
            const auto now = TActivationContext::Now();
            if (info.NextS3Sweep && now < *info.NextS3Sweep) {
                return Schedule(*info.NextS3Sweep - now, new TEvents::TEvWakeup);
            }
            const auto config = info.GetConfig();
            if (const auto error = NBlobDepot::ValidateS3BasePath(GetS3Namespace(config).Prefix)) {
                YDB_LOG_WARN("Cannot list malformed retired S3 namespace",
                    {"marker", "BSCVG20"}, {"groupId", GroupId.GetRawId()}, {"error", error});
                return Persist(TTable::Reserved);
            }
            if (!HasGeneratedS3Namespace(config) && !info.LegacyCleanupAuthorized) {
                // An older configuration does not establish ownership across bucket aliases or
                // outstanding migrations. The management command restarts us after confirmation.
                YDB_LOG_INFO("Retired S3 namespace awaits operator confirmation",
                    {"marker", "BSCVG21"}, {"groupId", GroupId.GetRawId()});
                return PassAway();
            }
            if (info.Phase == TTable::AwaitingOwnership && !config.GetEverAuthorizedDataCooling() &&
                    !AppData()->FeatureFlags.GetEnableBlobDepotDataCooling()) {
                return Schedule(TDuration::Hours(1), new TEvents::TEvWakeup);
            }
            if (Self->HasOtherS3NamespaceOwner(GroupId, config)) {
                ReleaseSlot();
                StartCommitted = false;
                return Persist(info.Phase, now + TDuration::Days(1));
            }
            if (!StartCommitted) {
                // Order a physical sweep after any just-committed changes to ownership.
                StartCommitted = true;
                return Persist(info.Phase == TTable::AwaitingOwnership ? TTable::FirstSweep : info.Phase, now,
                    info.ConsecutiveFailures);
            }
            if (!AcquireSlot()) {
                return;
            }
            StartCommitted = false;
            if (auto* actor = NBlobDepot::CreateRetiredS3Sweep(SelfId(), info.BlobDepotId, config)) {
                SweepActor = Register(actor);
            } else {
                // S3 support is unavailable on this build.
                ReleaseSlot();
                Schedule(TDuration::Days(1), new TEvents::TEvWakeup);
            }
        }

        void Handle(TEvBlobDepot::TEvRetiredS3SweepComplete::TPtr ev) {
            Y_ABORT_UNLESS(SweepActor == ev->Sender);
            SweepActor = {};
            ReleaseSlot();
            const auto& info = Self->RetiredS3Namespaces.at(GroupId);
            const auto now = TActivationContext::Now();
            if (!ev->Get()->Success) {
                Self->TabletCounters->Cumulative()[NBlobStorageController::COUNTER_RETIRED_S3_SWEEP_FAILURES].Increment(1);
                const ui32 failures = Min(info.ConsecutiveFailures, Max<ui32>() - 1) + 1;
                const auto delay = Min(TDuration::Days(1), TDuration::Minutes(1u << Min(failures - 1, 11u)));
                return Persist(info.Phase, now + delay, failures);
            }
            if (info.Phase == TTable::FirstSweep) {
                Persist(TTable::SecondSweep, now + TDuration::Minutes(2));
            } else if (info.Phase == TTable::SecondSweep) {
                Persist(TTable::FinalSweep, now + NBlobDepot::MaxS3PutTimeout + TDuration::Minutes(2));
            } else {
                Y_ABORT_UNLESS(info.Phase == TTable::FinalSweep);
                Persist(TTable::Reserved);
            }
        }

        void PassAway() override {
            if (SweepActor) {
                Send(SweepActor, new TEvents::TEvPoison);
            }
            if (TlsActivationContext->Mailbox.FindActor(ControllerId.LocalId())) {
                ReleaseSlot();
                auto& info = Self->RetiredS3Namespaces.at(GroupId);
                if (info.SchedulerId == SelfId()) {
                    info.SchedulerId = {};
                }
            }
            TActorBootstrapped::PassAway();
        }

        STATEFN(StateWork) {
            if (!IsCurrent()) {
                return PassAway();
            }
            switch (ev->GetTypeRewrite()) {
                cFunc(TEvents::TSystem::Wakeup, Run);
                case TEvents::TSystem::Bootstrap:
                    Updating = false;
                    Run();
                    break;
                hFunc(TEvBlobDepot::TEvRetiredS3SweepComplete, Handle);
                cFunc(TEvents::TSystem::Poison, PassAway);
            }
        }
    };

    void TBlobStorageController::CommitVirtualGroupUpdates(TConfigState& state) {
        for (const auto& [base, overlay] : state.Groups.Diff()) {
            auto startSetupMachine = [this, &state, groupId = overlay->first](bool restart) {
                state.Callbacks.push_back([this, restart, groupId = groupId] {
                    TGroupInfo *group = FindGroup(groupId);
                    Y_ABORT_UNLESS(group);
                    if (restart && group->VirtualGroupSetupMachineId) { // terminate currently running machine
                        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, std::exchange(
                            group->VirtualGroupSetupMachineId, {}), {}, nullptr, 0));
                    }
                    // start new one, if required
                    if (!group->VirtualGroupSetupMachineId) {
                        StartVirtualGroupSetupMachine(group);
                    }
                });
            };

            if (!overlay->second || !overlay->second->VirtualGroupState) { // existing group was just deleted (or became non-virtual)
                if (const TActorId actorId = base ? base->second->VirtualGroupSetupMachineId : TActorId()) {
                    state.Callbacks.push_back([actorId] {
                        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, {}, nullptr, 0));
                    });
                }
            } else if (overlay->second->NeedAlter && *overlay->second->NeedAlter) {
                // we need to terminate currently running machine only if we are *initiating* blob depot deletion
                const bool restartNeeded = base &&
                    base->second->VirtualGroupState != NKikimrBlobStorage::EVirtualGroupState::DELETING &&
                    overlay->second->VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::DELETING;

                startSetupMachine(restartNeeded);
            }
        }

        if (state.BlobDepotDeleteQueue.Changed()) {
            for (const auto& [prev, cur] : Diff(&BlobDepotDeleteQueue, &state.BlobDepotDeleteQueue.Unshare())) {
                if (!prev) {
                    // Start external deletion only after the group removal is durable.
                    state.Callbacks.push_back([this, groupId = cur->first] {
                        StartVirtualGroupDeleteMachine(groupId, BlobDepotDeleteQueue.at(groupId));
                    });
                }
            }
        }
    }

    void TBlobStorageController::StartRetiredS3Cleanup(TGroupId groupId, TRetiredS3Namespace& info) {
        if (!info.SchedulerId && info.Phase != Schema::RetiredS3Namespaces::Reserved) {
            info.SchedulerId = RegisterWithSameMailbox(new TRetiredS3Cleanup(this, groupId));
        }
    }

    void TBlobStorageController::StartVirtualGroupSetupMachine(TGroupInfo *group) {
        Y_ABORT_UNLESS(!group->VirtualGroupSetupMachineId);
        group->VirtualGroupSetupMachineId = RegisterWithSameMailbox(new TVirtualGroupSetupMachine(this, *group));
    }

    void TBlobStorageController::StartVirtualGroupDeleteMachine(TGroupId groupId, TBlobDepotDeleteQueueInfo& info) {
        Y_ABORT_UNLESS(!info.VirtualGroupSetupMachineId);
        info.VirtualGroupSetupMachineId = RegisterWithSameMailbox(new TVirtualGroupSetupMachine(this, groupId));
    }

    void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerGroupDecommittedNotify::TPtr ev) {
        class TTxDecommitGroup : public TTransactionBase<TBlobStorageController> {
            TEvBlobStorage::TEvControllerGroupDecommittedNotify::TPtr Ev;
            std::optional<TConfigState> State;
            NKikimrProto::EReplyStatus Status = NKikimrProto::OK;
            TString ErrorReason;

        public:
            TTxDecommitGroup(TBlobStorageController *controller, TEvBlobStorage::TEvControllerGroupDecommittedNotify::TPtr ev)
                : TTransactionBase(controller)
                , Ev(ev)
            {}

            TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_DECOMMIT_GROUP; }

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                State.emplace(*Self, Self->HostRecords, TActivationContext::Now(), TActivationContext::Monotonic());
                Action(*State);
                if (auto error = Self->ValidateAndCommitConfigUpdate(State, TConfigTxFlags::SuppressAll(), txc)) {
                    YDB_LOG_INFO("Failed to commit update",
                        {"marker", "BSCVG09"},
                        {"error", *error});
                }
                return true;
            }

            void Action(TConfigState& state) {
                const ui32 groupId = Ev->Get()->Record.GetGroupId();
                TGroupInfo *group = state.Groups.FindForUpdate(TGroupId::FromValue(groupId));
                if (!group) {
                    std::tie(Status, ErrorReason) = std::make_tuple(NKikimrProto::ERROR, "group not found");
                } else if (group->DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::DONE) {
                    Status = NKikimrProto::ALREADY;
                } else if (group->DecommitStatus != NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS) {
                    std::tie(Status, ErrorReason) = std::make_tuple(NKikimrProto::ERROR, "group is not being decommitted");
                } else {
                    if (group->BridgeGroupInfo) {
                        // bridged group is decommitted as a whole: release piles along with their VDisks (except for
                        // the inline one, which is the group itself) and turn this group into an ordinary virtual one
                        const std::vector<TGroupId> pileGroupIds = TConfigState::GetBridgeParts(*group);
                        group->BridgeGroupInfo.reset();
                        group->BridgePileId = TBridgePileId();
                        group->BridgeProxyGroupId.reset();

                        // group entries are held by pointers, so deleting other groups keeps `group` valid
                        for (const TGroupId pileGroupId : pileGroupIds) {
                            if (const TGroupInfo *pileGroup = state.Groups.Find(pileGroupId)) {
                                state.DeleteExistingGroupWithVSlots(*pileGroup);
                            } else {
                                Y_DEBUG_ABORT_S("pile group not found GroupId# " << groupId << " PileGroupId# " << pileGroupId);
                            }
                        }
                    }
                    for (const TVSlotInfo *vslot : group->VDisksInGroup) {
                        state.DestroyVSlot(vslot->VSlotId);
                    }
                    group->VDisksInGroup.clear();
                    group->DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::DONE;
                    group->Topology = std::make_shared<TBlobStorageGroupInfo::TTopology>(group->Topology->GType, 0, 0, 0);
                    state.GroupContentChanged.insert(TGroupId::FromValue(groupId));
                }

                YDB_LOG_INFO("Decommission update processed",
                    {"marker", "BSCVG10"},
                    {"status", Status},
                    {"errorReason", ErrorReason});
            }

            void Complete(const TActorContext&) override {
                if (State) {
                    State->ApplyConfigUpdates();
                }
                auto ev = std::make_unique<TEvBlobStorage::TEvControllerGroupDecommittedResponse>(Status);
                if (ErrorReason) {
                    ev->Record.SetErrorReason(ErrorReason);
                }
                auto reply = std::make_unique<IEventHandle>(Ev->Sender, Self->SelfId(), ev.release(), 0, Ev->Cookie);
                if (Ev->InterconnectSession) {
                    reply->Rewrite(TEvInterconnect::EvForward, Ev->InterconnectSession);
                }
                TActivationContext::Send(std::move(reply));
            }
        };

        YDB_LOG_INFO("TEvControllerGroupDecommittedNotify received",
            {"marker", "BSCVG11"},
            {"msg", ev->Get()->Record});
        Execute(new TTxDecommitGroup(this, ev));
    }

    void TBlobStorageController::RenderVirtualGroups(IOutputStream& out) {
        HTML(out) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    out << "Virtual groups";
                }
                DIV_CLASS("panel-body") {
                    TABLE_CLASS("table table-condensed") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { out << "GroupId"; }
                                TABLEH() { out << "StoragePoolName"; }
                                TABLEH() { out << "Name"; }
                                TABLEH() { out << "BlobDepotId"; }
                                TABLEH() { out << "State"; }
                                TABLEH() { out << "HiveId"; }
                                TABLEH() { out << "ErrorReason"; }
                                TABLEH() { out << "DecommitStatus"; }
                            }
                        }
                        TABLEBODY() {
                            for (const auto& [groupId, group] : GroupMap) {
                                if (!group->VirtualGroupState) {
                                    continue;
                                }

                                TABLER() {
                                    TABLED() {
                                        out << groupId;
                                    }
                                    TABLED() {
                                        const auto it = StoragePools.find(group->StoragePoolId);
                                        Y_ABORT_UNLESS(it != StoragePools.end());
                                        out << it->second.Name;
                                    }
                                    TABLED() {
                                        if (group->VirtualGroupName) {
                                            out << *group->VirtualGroupName;
                                        } else {
                                            out << "<i>null</i>";
                                        }
                                    }
                                    TABLED() {
                                        if (group->BlobDepotId) {
                                            out << *group->BlobDepotId;
                                        } else {
                                            out << "<i>null</i>";
                                        }
                                    }
                                    TABLED() {
                                        out << NKikimrBlobStorage::EVirtualGroupState_Name(*group->VirtualGroupState);
                                    }
                                    TABLED() {
                                        if (group->HiveId) {
                                            out << *group->HiveId;
                                        } else {
                                            out << "<i>null</i>";
                                        }
                                    }
                                    TABLED() {
                                        if (group->ErrorReason) {
                                            out << *group->ErrorReason;
                                        } else {
                                            out << "<i>null</i>";
                                        }
                                    }
                                    TABLED() {
                                        out << NKikimrBlobStorage::TGroupDecommitStatus::E_Name(group->DecommitStatus);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

} // NKikimr::NBsController
