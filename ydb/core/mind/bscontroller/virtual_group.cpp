#include "impl.h"
#include "config.h"

namespace NKikimr::NBsController {

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TAllocateVirtualGroup& cmd, TStatus& status) {
        if (!cmd.GetName()) {
            throw TExError() << "TAllocateVirtualGroup.Name must be set and be nonempty";
        } else if (cmd.GetHiveDesignatorCase() == NKikimrBlobStorage::TAllocateVirtualGroup::HIVEDESIGNATOR_NOT_SET) {
            throw TExError() << "TAllocateVirtualGroup.HiveId/Database is not specified";
        }

        // make sure the operation is idempotent
        struct TExFound { TGroupId id; };
        try {
            Groups.ForEach([&](TGroupId key, const TGroupInfo& value) {
                if (value.VirtualGroupName == cmd.GetName()) {
                    throw TExFound{key};
                }
            });
        } catch (const TExFound& ex) {
            status.AddGroupId(ex.id.GetRawId());
            status.SetAlready(true);
            return;
        }

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

        // create entry in group table
        auto *group = Groups.ConstructInplaceNewEntry(TGroupId::FromValue(groupId.GetRaw()), TGroupId::FromValue(groupId.GetRaw()), 0u, 0u,
            TBlobStorageGroupType::ErasureNone, 0u, NKikimrBlobStorage::TVDiskKind::Default,
            pool.EncryptionMode.GetOrElse(TBlobStorageGroupInfo::EEM_NONE), TBlobStorageGroupInfo::ELCP_INITIAL,
            TString(), TString(), 0u, 0u, false, false, storagePoolId, 0u, 0u, 0u);

        // bind group to storage pool
        ++pool.NumGroups;
        StoragePoolGroups.Unshare().emplace(storagePoolId, group->ID);

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

        NKikimrBlobDepot::TBlobDepotConfig config;
        config.SetVirtualGroupId(group->ID.GetRawId());
        config.MutableChannelProfiles()->CopyFrom(cmd.GetChannelProfiles());

        const bool success = config.SerializeToString(&group->BlobDepotConfig.ConstructInPlace());
        Y_ABORT_UNLESS(success);

        status.AddGroupId(group->ID.GetRawId());
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDecommitGroups& cmd, TStatus& /*status*/) {
        if (cmd.GetHiveDesignatorCase() == NKikimrBlobStorage::TDecommitGroups::HIVEDESIGNATOR_NOT_SET) {
            throw TExError() << "TDecommitGroups.HiveId/Database is not specified";
        }

        for (const ui32 groupId : cmd.GetGroupIds()) {
            TGroupInfo *group = Groups.FindForUpdate(TGroupId::FromValue(groupId));
            if (!group) {
                throw TExGroupNotFound(groupId);
            } else if (group->DecommitStatus != NKikimrBlobStorage::TGroupDecommitStatus::NONE) {
                if (cmd.HasHiveId() && group->HiveId && *group->HiveId != cmd.GetHiveId()) {
                    throw TExError() << "different hive specified for decommitting group" << TErrorParams::GroupId(groupId);
                } else if (cmd.HasDatabase() && group->Database && *group->Database != cmd.GetDatabase()) {
                    throw TExError() << "different database specified for decommitting group" << TErrorParams::GroupId(groupId);
                } else if (cmd.HasHiveId() != group->HiveId.Defined() && cmd.HasDatabase() != group->Database.Defined()) {
                    throw TExError() << "different hive designator specified for decommitting group" << TErrorParams::GroupId(groupId);
                }
                // group is already being decommitted -- make this operation idempotent
                continue;
            } else if (group->VirtualGroupState) {
                throw TExError() << "group is already virtual" << TErrorParams::GroupId(groupId);
            }

            group->DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::PENDING;
            group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::NEW;
            group->HiveId = cmd.HasHiveId() ? MakeMaybe(cmd.GetHiveId()) : Nothing();
            group->Database = cmd.HasDatabase() ? MakeMaybe(cmd.GetDatabase()) : Nothing();
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
                for (auto [begin, end] = StoragePoolGroups.Get().equal_range(group->StoragePoolId); begin != end; ++begin) {
                    if (const TGroupInfo *poolGroup = Groups.Find(begin->second)) {
                        numPhysicalGroups += poolGroup->IsPhysicalGroup();
                    } else {
                        throw TExGroupNotFound(begin->second.GetRawId())
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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    class TBlobStorageController::TVirtualGroupSetupMachine : public TActorBootstrapped<TVirtualGroupSetupMachine> {
        // a token to control lifetime within transactions
        struct TToken {};
        std::shared_ptr<TToken> Token = std::make_shared<TToken>();

        TBlobStorageController *Self;
        const TActorId ControllerId;
        const TGroupId GroupId;
        const std::optional<TBlobDepotDeleteQueueInfo> DeleteInfo;

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
                State.emplace(*Self, Self->HostRecords, TActivationContext::Now());
                TGroupInfo *group = State->Groups.FindForUpdate(GroupId);
                Y_ABORT_UNLESS(group);
                if (!Callback(*group, *State)) {
                    State->DeleteExistingGroup(group->ID);
                }
                group->CalculateGroupStatus();
                TString error;
                if (State->Changed() && !Self->CommitConfigUpdates(*State, true, true, true, txc, &error)) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSCVG08, "failed to commit update", (VirtualGroupId, GroupId), (Error, error));
                    State->Rollback();
                    State.reset();
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

        class TTxDeleteBlobDepot : public TTransactionBase<TBlobStorageController> {
            TVirtualGroupSetupMachine* const Machine;
            const TActorId MachineId;
            const TGroupId GroupId;
            const std::weak_ptr<TToken> Token;
            std::optional<TConfigState> State;

        public:
            TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_DELETE_BLOB_DEPOT; }

            TTxDeleteBlobDepot(TVirtualGroupSetupMachine *machine)
                : TTransactionBase(machine->Self)
                , Machine(machine)
                , MachineId(Machine->SelfId())
                , GroupId(Machine->GroupId)
                , Token(Machine->Token)
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                if (Token.expired()) {
                    return true; // actor is already dead
                }
                State.emplace(*Self, Self->HostRecords, TActivationContext::Now());
                const size_t n = State->BlobDepotDeleteQueue.Unshare().erase(GroupId);
                Y_ABORT_UNLESS(n == 1);
                TString error;
                if (State->Changed() && !Self->CommitConfigUpdates(*State, true, true, true, txc, &error)) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSCVG17, "failed to commit update", (VirtualGroupId, GroupId), (Error, error));
                    State->Rollback();
                    State.reset();
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

    public:
        TVirtualGroupSetupMachine(TBlobStorageController *self, TGroupInfo& group)
            : Self(self)
            , ControllerId(Self->SelfId())
            , GroupId(group.ID)
        {}

        TVirtualGroupSetupMachine(TBlobStorageController *self, ui32 groupId, const TBlobDepotDeleteQueueInfo& info)
            : Self(self)
            , ControllerId(Self->SelfId())
            , GroupId(TGroupId::FromValue(groupId))
            , DeleteInfo(info)
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc);
            if (Expired()) { // BS_CONTROLLER is already dead
                return PassAway();
            }

            if (DeleteInfo) {
                Y_ABORT_UNLESS(Self->BlobDepotDeleteQueue.contains(GroupId));
                STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG19, "Bootstrap for delete", (GroupId, GroupId),
                    (HiveId, DeleteInfo->HiveId), (BlobDepotId, DeleteInfo->BlobDepotId));
                if (DeleteInfo->HiveId) {
                    HiveDelete(*DeleteInfo->HiveId, DeleteInfo->BlobDepotId);
                } else {
                    OnBlobDepotDeleted();
                }
                return;
            }

            TGroupInfo *group = GetGroup();
            if (!group->VirtualGroupState) { // group was deleted or reset to non-decommitting during the last transaction
                return PassAway();
            }

            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG01, "Bootstrap", (GroupId, GroupId),
                (State, NKikimrBlobStorage::EVirtualGroupState_Name(*group->VirtualGroupState)),
                (NeedAlter, group->NeedAlter));

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

        std::optional<NKikimrBlobDepot::TBlobDepotConfig> Config;

        NKikimrBlobDepot::TBlobDepotConfig& GetConfig(TGroupInfo *group) {
            if (!Config) {
                Config.emplace();
                Y_ABORT_UNLESS(group->BlobDepotConfig);
                const bool success = Config->ParseFromString(*group->BlobDepotConfig);
                Y_ABORT_UNLESS(success);
            }
            return *Config;
        }

        template<typename T>
        void UpdateBlobDepotConfig(T&& callback) {
            Self->Execute(std::make_unique<TTxUpdateGroup>(this, [this, callback](TGroupInfo& group, TConfigState&) {
                auto& config = GetConfig(&group);
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
        ui64 RootHiveId = 0;
        bool TenantHiveInvalidated = false;
        bool TenantHiveInvalidateInProgress = false;

        void HiveCreate(TGroupInfo *group) {
            auto& config = GetConfig(group);
            if (config.HasTabletId()) {
                ConfigureBlobDepot();
            } else if (!group->HiveId) {
                HiveResolve(group);
            } else if (TenantHiveInvalidateInProgress && TenantHivePipeId) {
                // tenant hive storage pool invalidation still in progress, wait
            } else if (config.GetIsDecommittingGroup() && config.HasTenantHiveId() && !TenantHiveInvalidated) {
                TenantHivePipeId = Register(NTabletPipe::CreateClient(SelfId(), config.GetTenantHiveId(),
                    NTabletPipe::TClientRetryPolicy::WithRetries()));
                HiveInvalidateGroups(TenantHivePipeId, group);
                TenantHiveInvalidateInProgress = true;
            } else if (!HivePipeId) {
                Y_ABORT_UNLESS(group->HiveId);
                HivePipeId = Register(NTabletPipe::CreateClient(SelfId(), *group->HiveId, NTabletPipe::TClientRetryPolicy::WithRetries()));
                if (config.GetIsDecommittingGroup()) {
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

            const auto& domainsInfo = AppData()->DomainsInfo;
            if (const auto& domain = domainsInfo->Domain) {
                const TString domainPath = TStringBuilder() << '/' << domain->Name;
                if (database == domainPath || database.StartsWith(TStringBuilder() << domainPath << '/')) {
                    RootHiveId = domainsInfo->GetHive();
                    if (RootHiveId == TDomainsInfo::BadTabletId) {
                        return CreateFailed(TStringBuilder() << "failed to resolve Hive -- BadTabletId for Database# " << database);
                    }
                }
            }

            auto req = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
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

            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG16, "TEvDescribeSchemeResult", (GroupId, GroupId),
                (Result, response.ToString(*AppData()->TypeRegistry)));

            if (item.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok || !domainInfo || !item.DomainDescription) {
                return CreateFailed(TStringBuilder() << "failed to resolve Hive -- erroneous reply from SchemeCache or not a domain");
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

                const auto& config = GetConfig(group);
                for (const auto& item : config.GetChannelProfiles()) {
                    const TString& name = item.GetStoragePoolName();
                    if (!storagePoolNames.contains(name) && !name.StartsWith(*group->Database + ':')) {
                        return CreateFailed("failed to resolve Hive -- tablet underlying storage pool is not listed in database");
                    }
                }

                const ui64 hiveId = params.HasHive() ? params.GetHive() : RootHiveId;
                if (!hiveId) {
                    return CreateFailed("failed to resolve Hive -- Hive is zero");
                }

                auto descr = item.DomainDescription;

                Self->Execute(std::make_unique<TTxUpdateGroup>(this, [=](TGroupInfo& group, TConfigState&) {
                    auto& config = GetConfig(&group);
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

            auto& config = GetConfig(group);
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

            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG00, "sending TEvCreateTablet", (GroupId, group->ID),
                (HiveId, *group->HiveId), (Msg, ev->Record));

            NTabletPipe::SendData(SelfId(), HivePipeId, ev.release());
        }

        void HiveInvalidateGroups(TActorId pipeId, TGroupInfo *group) {
            auto& config = GetConfig(group);
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
            auto& config = GetConfig(group);
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

            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG12, "sending TEvDeleteTablet", (GroupId.GetRawId(), GroupId.GetRawId()),
                (HiveId, hiveId), (Msg, ev->Record));

            NTabletPipe::SendData(SelfId(), HivePipeId, ev.release());
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG02, "received TEvClientConnected", (GroupId.GetRawId(), GroupId.GetRawId()),
                (Status, ev->Get()->Status), (ClientId, ev->Get()->ClientId), (HivePipeId, HivePipeId),
                (BlobDepotPipeId, BlobDepotPipeId));

            if (ev->Get()->Status != NKikimrProto::OK) {
                OnPipeError(ev->Get()->ClientId);
            } else if (ev->Get()->ClientId == HivePipeId && !DeleteInfo) {
                TGroupInfo *group = GetGroup();
                if (group->VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::NEW) {
                    auto& config = GetConfig(group);
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
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG03, "received TEvClientDestroyed", (GroupId.GetRawId(), GroupId.GetRawId()),
                (ClientId, ev->Get()->ClientId), (HivePipeId, HivePipeId), (BlobDepotPipeId, BlobDepotPipeId));

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
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG06, "received TEvInvalidateStoragePoolsReply", (GroupId.GetRawId(), GroupId.GetRawId()),
                (Msg, ev->Get()->Record));
        }

        void Handle(TEvHive::TEvReassignOnDecommitGroupReply::TPtr ev) {
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG07, "received TEvReassignOnDecommitGroupReply", (GroupId.GetRawId(), GroupId.GetRawId()),
                (Msg, ev->Get()->Record));
            if (TenantHiveInvalidateInProgress) {
                NTabletPipe::CloseAndForgetClient(SelfId(), TenantHivePipeId);
                TenantHiveInvalidateInProgress = false;
                TenantHiveInvalidated = true;
                Bootstrap();
            }
        }

        void Handle(TEvHive::TEvCreateTabletReply::TPtr ev) {
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG04, "received TEvCreateTabletReply", (GroupId.GetRawId(), GroupId.GetRawId()),
                (Msg, ev->Get()->Record));

            NTabletPipe::CloseAndForgetClient(SelfId(), HivePipeId);

            TGroupInfo *group = GetGroup();

            auto& record = ev->Get()->Record;
            if (record.GetStatus() == NKikimrProto::OK || record.GetStatus() == NKikimrProto::ALREADY) {
                auto& config = GetConfig(group);
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
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG05, "received TEvTabletCreationResult", (GroupId.GetRawId(), GroupId.GetRawId()),
                (Msg, ev->Get()->Record));
        }

        void Handle(TEvHive::TEvDeleteTabletReply::TPtr ev) {
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG13, "received TEvDeleteTabletReply", (GroupId.GetRawId(), GroupId.GetRawId()),
                (Msg, ev->Get()->Record));
            if (DeleteInfo) {
                OnBlobDepotDeleted();
            } else {
                DeleteBlobDepot();
            }
        }

        void ConfigureBlobDepot() {
            TGroupInfo *group = GetGroup();
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG14, "ConfigureBlobDepot", (GroupId.GetRawId(), group->ID.GetRawId()));
            auto& config = GetConfig(group);
            Y_ABORT_UNLESS(config.HasTabletId());
            Y_ABORT_UNLESS(!group->BlobDepotId || group->BlobDepotId == config.GetTabletId());
            const ui64 tabletId = config.GetTabletId();
            BlobDepotPipeId = Register(NTabletPipe::CreateClient(SelfId(), tabletId,
                NTabletPipe::TClientRetryPolicy::WithRetries()));
            auto ev = std::make_unique<TEvBlobDepot::TEvApplyConfig>();
            ev->Record.MutableConfig()->CopyFrom(config);
            NTabletPipe::SendData(SelfId(), BlobDepotPipeId, ev.release());
        }

        void DeleteBlobDepot() {
            auto *group = GetGroup();
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG15, "DeleteBlobDepot", (GroupId.GetRawId(), group->ID.GetRawId()));
            Self->Execute(std::make_unique<TTxUpdateGroup>(this, [](TGroupInfo& group, TConfigState&) {
                if (group.VDisksInGroup) {
                    group.VirtualGroupName = {};
                    group.VirtualGroupState = {};
                    group.HiveId = {};
                    group.BlobDepotConfig = {};
                    group.BlobDepotId = {};
                    group.ErrorReason = {};
                    group.NeedAlter = {};
                    return true;
                } else {
                    return false;
                }
            }));
        }

        void OnBlobDepotDeleted() {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG18, "OnBlobDepotDeleted", (GroupId.GetRawId(), GroupId.GetRawId()));
            Self->Execute(std::make_unique<TTxDeleteBlobDepot>(this));
        }

        void Handle(TEvBlobDepot::TEvApplyConfigResult::TPtr /*ev*/) {
            NTabletPipe::CloseAndForgetClient(SelfId(), BlobDepotPipeId);

            Self->Execute(std::make_unique<TTxUpdateGroup>(this, [&](TGroupInfo& group, TConfigState& state) {
                group.VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::WORKING;
                auto& config = GetConfig(&group);
                Y_ABORT_UNLESS(config.HasTabletId());
                group.BlobDepotId = config.GetTabletId();
                group.NeedAlter = false;
                if (group.DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::PENDING) {
                    group.DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS;
                    state.GroupContentChanged.insert(GroupId);
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
                TSet<ui32> groups;
                groups.insert(group->ID.GetRawId());
                Self->ReadGroups(groups, false, ev.get(), nodeId);
                Send(MakeBlobStorageNodeWardenID(nodeId), ev.release());
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void PassAway() override {
            if (!Expired() && !DeleteInfo) {
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
            switch (const ui32 type = ev->GetTypeRewrite()) {
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
            Y_ABORT_UNLESS(!DeleteInfo);
            TGroupInfo *res = Self->FindGroup(GroupId);
            Y_ABORT_UNLESS(res);
            return res;
        }

        bool Expired() const {
            if (!TlsActivationContext->Mailbox.FindActor(ControllerId.LocalId())) { // BS_CONTROLLER died
                return true;
            } else if (DeleteInfo) {
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
                if (!prev) { // a new item was just inserted, start delete machine
                    StartVirtualGroupDeleteMachine(cur->first, cur->second);
                }
            }
        }
    }

    void TBlobStorageController::StartVirtualGroupSetupMachine(TGroupInfo *group) {
        Y_ABORT_UNLESS(!group->VirtualGroupSetupMachineId);
        group->VirtualGroupSetupMachineId = RegisterWithSameMailbox(new TVirtualGroupSetupMachine(this, *group));
    }

    void TBlobStorageController::StartVirtualGroupDeleteMachine(TGroupId groupId, TBlobDepotDeleteQueueInfo& info) {
        Y_ABORT_UNLESS(!info.VirtualGroupSetupMachineId);
        info.VirtualGroupSetupMachineId = RegisterWithSameMailbox(new TVirtualGroupSetupMachine(this, groupId.GetRawId(), info));
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
                State.emplace(*Self, Self->HostRecords, TActivationContext::Now());
                Action(*State);
                TString error;
                if (State->Changed() && !Self->CommitConfigUpdates(*State, true, true, true, txc, &error)) {
                    STLOG(PRI_INFO, BS_CONTROLLER, BSCVG09, "failed to commit update", (Error, error));
                    State->Rollback();
                    State.reset();
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
                    for (const TVSlotInfo *vslot : group->VDisksInGroup) {
                        state.DestroyVSlot(vslot->VSlotId);
                    }
                    group->VDisksInGroup.clear();
                    group->DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::DONE;
                    group->Topology = std::make_shared<TBlobStorageGroupInfo::TTopology>(group->Topology->GType, 0, 0, 0);
                    state.GroupContentChanged.insert(TGroupId::FromValue(groupId));
                }

                STLOG(PRI_INFO, BS_CONTROLLER, BSCVG10, "decommission update processed", (Status, Status),
                    (ErrorReason, ErrorReason));
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

        STLOG(PRI_INFO, BS_CONTROLLER, BSCVG11, "TEvControllerGroupDecommittedNotify received", (Msg, ev->Get()->Record));
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
