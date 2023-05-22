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
            status.AddGroupId(ex.id);
            status.SetAlready(true);
            return;
        }

        // allocate group identifier
        auto& nextGroupId = NextVirtualGroupId.Unshare();
        TGroupID groupId(EGroupConfigurationType::Virtual, 1, nextGroupId);
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
        auto *group = Groups.ConstructInplaceNewEntry(groupId.GetRaw(), groupId.GetRaw(), 0u, 0u,
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

        group->CalculateGroupStatus();

        NKikimrBlobDepot::TBlobDepotConfig config;
        config.SetVirtualGroupId(group->ID);
        config.MutableChannelProfiles()->CopyFrom(cmd.GetChannelProfiles());

        const bool success = config.SerializeToString(&group->BlobDepotConfig.ConstructInPlace());
        Y_VERIFY(success);

        status.AddGroupId(group->ID);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDecommitGroups& cmd, TStatus& /*status*/) {
        if (cmd.GetHiveDesignatorCase() == NKikimrBlobStorage::TDecommitGroups::HIVEDESIGNATOR_NOT_SET) {
            throw TExError() << "TDecommitGroups.HiveId/Database is not specified";
        }

        for (const TGroupId groupId : cmd.GetGroupIds()) {
            TGroupInfo *group = Groups.FindForUpdate(groupId);
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
                        throw TExGroupNotFound(begin->second)
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
            Y_VERIFY(success);
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TCancelVirtualGroup& cmd, TStatus& /*status*/) {
        const TGroupId groupId = cmd.GetGroupId();
        TGroupInfo *group = Groups.FindForUpdate(groupId);
        if (!group) {
            throw TExGroupNotFound(groupId);
        } else if (!group->VirtualGroupState) {
            throw TExError() << "group is not virtual" << TErrorParams::GroupId(groupId);
        } else if (auto s = group->VirtualGroupState; s != NKikimrBlobStorage::EVirtualGroupState::NEW && s != NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED) {
            throw TExError() << "group is not in new/error state" << TErrorParams::GroupId(groupId);
        } else if (auto s = group->DecommitStatus; s != NKikimrBlobStorage::TGroupDecommitStatus::NONE && s != NKikimrBlobStorage::TGroupDecommitStatus::PENDING) {
            throw TExError() << "group decommit status is not NONE/PENDING" << TErrorParams::GroupId(groupId);
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

    private:
        class TTxUpdateGroup : public TTransactionBase<TBlobStorageController> {
            TVirtualGroupSetupMachine* const Machine;
            const TActorId MachineId;
            const TGroupId GroupId;
            const std::weak_ptr<TToken> Token;
            std::optional<TConfigState> State;
            const std::function<bool(TGroupInfo&)> Callback;

        public:
            TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_GROUP; }

            TTxUpdateGroup(TVirtualGroupSetupMachine *machine, std::function<bool(TGroupInfo&)>&& callback)
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
                Y_VERIFY(group);
                if (!Callback(*group)) {
                    State->Groups.DeleteExistingEntry(group->ID);
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

    public:
        TVirtualGroupSetupMachine(TBlobStorageController *self, TGroupInfo& group)
            : Self(self)
            , ControllerId(Self->SelfId())
            , GroupId(group.ID)
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc);
            if (Expired()) { // BS_CONTROLLER is already dead
                return PassAway();
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
                    Y_FAIL();
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::optional<NKikimrBlobDepot::TBlobDepotConfig> Config;

        NKikimrBlobDepot::TBlobDepotConfig& GetConfig(TGroupInfo *group) {
            if (!Config) {
                Config.emplace();
                Y_VERIFY(group->BlobDepotConfig);
                const bool success = Config->ParseFromString(*group->BlobDepotConfig);
                Y_VERIFY(success);
            }
            return *Config;
        }

        template<typename T>
        void UpdateBlobDepotConfig(T&& callback) {
            Self->Execute(std::make_unique<TTxUpdateGroup>(this, [this, callback](TGroupInfo& group) {
                auto& config = GetConfig(&group);
                callback(config);
                TString data;
                const bool success = config.SerializeToString(&data);
                Y_VERIFY(success);
                group.BlobDepotConfig = data;
                return true;
            }));
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        TActorId HivePipeId;
        TActorId BlobDepotPipeId;

        void HiveCreate(TGroupInfo *group) {
            auto& config = GetConfig(group);
            if (config.HasTabletId()) {
                ConfigureBlobDepot();
            } else if (!group->HiveId) {
                HiveResolve(group);
            } else if (HivePipeId) {
                HiveCreateTablet(group);
            } else {
                Y_VERIFY(group->HiveId);
                HivePipeId = Register(NTabletPipe::CreateClient(SelfId(), *group->HiveId, NTabletPipe::TClientRetryPolicy::WithRetries()));
            }
        }

        void HiveResolve(TGroupInfo *group) {
            Y_VERIFY(group->Database);
            Y_VERIFY(!group->HiveId);
            const TString& database = *group->Database;
            const auto& domainsInfo = AppData()->DomainsInfo;
            if (auto *domain = domainsInfo->GetDomainByName(database)) {
                const ui64 hiveId = domainsInfo->GetHive(domain->DefaultHiveUid);
                if (hiveId == TDomainsInfo::BadTabletId) {
                    return CreateFailed(TStringBuilder() << "failed to resolve Hive -- BadTabletId for Database# " << database);
                }
                Self->Execute(std::make_unique<TTxUpdateGroup>(this, [hiveId](TGroupInfo& group) {
                    group.HiveId = hiveId;
                    return true;
                }));
            } else {
                auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
                auto& item = request->ResultSet.emplace_back();
                item.Path = SplitPath(database);
                item.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
                item.RedirectRequired = false;
                Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()));
            }
        }

        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr ev) {
            auto& result = *ev->Get()->Request;
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG16, "TEvNavigateKeySetResult", (TabletId, Self->TabletID()),
                (GroupId, GroupId), (Result, result.ToString(*AppData()->TypeRegistry)));
            if (result.ResultSet.size() != 1) {
                return CreateFailed(TStringBuilder() << "failed to resolve Hive -- incorrect reply from SchemeCache");
            }
            auto& item = result.ResultSet.front();
            if (item.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                return CreateFailed(TStringBuilder() << "failed to resolve Hive -- error while resolving database Status# " << item.Status);
            } else if (!item.DomainInfo) {
                return CreateFailed(TStringBuilder() << "failed to resolve Hive -- no DomainInfo in NavigateKeySetResult");
            }
            const auto& params = item.DomainInfo->Params;
            if (!params.HasHive()) {
                return CreateFailed(TStringBuilder() << "failed to resolve Hive -- no Hive field in ProcessingParams");
            } else if (const ui64 hiveId = params.GetHive()) {
                Self->Execute(std::make_unique<TTxUpdateGroup>(this, [hiveId](TGroupInfo& group) {
                    group.HiveId = hiveId;
                    return true;
                }));
            } else {
                return CreateFailed(TStringBuilder() << "failed to resolve Hive -- zero Hive value in ProcessingParams");
            }
        }

        void HiveCreateTablet(TGroupInfo *group) {
            TChannelsBindings bindings;
            std::unordered_set<TString> names;
            auto invalidateEv = std::make_unique<TEvHive::TEvInvalidateStoragePools>();
            auto& record = invalidateEv->Record;

            auto& config = GetConfig(group);
            for (const auto& item : config.GetChannelProfiles()) {
                for (ui32 i = 0; i < item.GetCount(); ++i) {
                    const TString& storagePoolName = item.GetStoragePoolName();

                    NKikimrStoragePool::TChannelBind binding;
                    binding.SetStoragePoolName(storagePoolName);
                    if (config.GetIsDecommittingGroup()) {
                        binding.SetPhysicalGroupsOnly(true);
                        if (i == 0 && names.insert(storagePoolName).second) {
                            record.AddStoragePoolName(storagePoolName);
                        }
                    }
                    bindings.push_back(std::move(binding));
                }
            }

            if (!names.empty()) {
                NTabletPipe::SendData(SelfId(), HivePipeId, invalidateEv.release());
            }

            if (config.GetIsDecommittingGroup()) {
                NTabletPipe::SendData(SelfId(), HivePipeId, new TEvHive::TEvReassignOnDecommitGroup(group->ID));
            }

            auto ev = std::make_unique<TEvHive::TEvCreateTablet>(Self->TabletID(), group->ID, TTabletTypes::BlobDepot, bindings);
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG00, "sending TEvCreateTablet", (TabletId, Self->TabletID()),
                (GroupId, group->ID), (HiveId, *group->HiveId), (Msg, ev->Record));

            NTabletPipe::SendData(SelfId(), HivePipeId, ev.release());
        }

        void HiveDelete(TGroupInfo *group) {
            auto& config = GetConfig(group);
            if (!config.GetHiveContacted() || !group->HiveId) {
                // hive has never been contacted, so there is no possibility this tablet was created
                Y_VERIFY(!config.HasTabletId());
                return DeleteBlobDepot();
            }

            Y_VERIFY(!HivePipeId);
            Y_VERIFY(group->HiveId);
            HivePipeId = Register(NTabletPipe::CreateClient(SelfId(), *group->HiveId, NTabletPipe::TClientRetryPolicy::WithRetries()));

            auto ev = config.HasTabletId()
                ? std::make_unique<TEvHive::TEvDeleteTablet>(Self->TabletID(), group->ID, config.GetTabletId(), 0)
                : std::make_unique<TEvHive::TEvDeleteTablet>(Self->TabletID(), group->ID, 0);

            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG12, "sending TEvDeleteTablet", (TabletId, Self->TabletID()),
                (GroupId, group->ID), (HiveId, *group->HiveId), (Msg, ev->Record));

            NTabletPipe::SendData(SelfId(), HivePipeId, ev.release());
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG02, "received TEvClientConnected", (TabletId, Self->TabletID()),
                (Status, ev->Get()->Status), (ClientId, ev->Get()->ClientId), (HivePipeId, HivePipeId),
                (BlobDepotPipeId, BlobDepotPipeId));

            if (ev->Get()->Status != NKikimrProto::OK) {
                OnPipeError(ev->Get()->ClientId);
            } else if (ev->Get()->ClientId == HivePipeId) {
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
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG03, "received TEvClientDestroyed", (TabletId, Self->TabletID()),
                (ClientId, ev->Get()->ClientId), (HivePipeId, HivePipeId), (BlobDepotPipeId, BlobDepotPipeId));

            OnPipeError(ev->Get()->ClientId);
        }

        void OnPipeError(TActorId clientId) {
            if (clientId == HivePipeId) {
                HivePipeId = {};
                Bootstrap();
            } else if (clientId == BlobDepotPipeId) {
                BlobDepotPipeId = {};
                Bootstrap();
            }
        }

        void Handle(TEvHive::TEvInvalidateStoragePoolsReply::TPtr ev) {
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG06, "received TEvInvalidateStoragePoolsReply", (TabletId, Self->TabletID()),
                (Msg, ev->Get()->Record));
        }

        void Handle(TEvHive::TEvReassignOnDecommitGroupReply::TPtr ev) {
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG07, "received TEvReassignOnDecommitGroupReply", (TabletId, Self->TabletID()),
                (Msg, ev->Get()->Record));
        }

        void Handle(TEvHive::TEvCreateTabletReply::TPtr ev) {
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG04, "received TEvCreateTabletReply", (TabletId, Self->TabletID()),
                (Msg, ev->Get()->Record));

            NTabletPipe::CloseAndForgetClient(SelfId(), HivePipeId);

            TGroupInfo *group = GetGroup();

            auto& record = ev->Get()->Record;
            if (record.GetStatus() == NKikimrProto::OK || record.GetStatus() == NKikimrProto::ALREADY) {
                auto& config = GetConfig(group);
                Y_VERIFY(!config.HasTabletId());
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
            Self->Execute(std::make_unique<TTxUpdateGroup>(this, [=](TGroupInfo& group) {
                group.VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED;
                group.NeedAlter = false;
                group.ErrorReason = error;
                return true;
            }));
        }

        void Handle(TEvHive::TEvTabletCreationResult::TPtr ev) {
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG05, "received TEvTabletCreationResult", (TabletId, Self->TabletID()),
                (Msg, ev->Get()->Record));
        }

        void Handle(TEvHive::TEvDeleteTabletReply::TPtr ev) {
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG13, "received TEvDeleteTabletReply", (TabletId, Self->TabletID()),
                (Msg, ev->Get()->Record));
            DeleteBlobDepot();
        }

        void ConfigureBlobDepot() {
            TGroupInfo *group = GetGroup();
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG14, "ConfigureBlobDepot", (TabletId, Self->TabletID()), (GroupId, group->ID));
            auto& config = GetConfig(group);
            Y_VERIFY(config.HasTabletId());
            Y_VERIFY(!group->BlobDepotId || group->BlobDepotId == config.GetTabletId());
            const ui64 tabletId = config.GetTabletId();
            BlobDepotPipeId = Register(NTabletPipe::CreateClient(SelfId(), tabletId,
                NTabletPipe::TClientRetryPolicy::WithRetries()));
            auto ev = std::make_unique<TEvBlobDepot::TEvApplyConfig>();
            ev->Record.MutableConfig()->CopyFrom(config);
            NTabletPipe::SendData(SelfId(), BlobDepotPipeId, ev.release());
        }

        void DeleteBlobDepot() {
            auto *group = GetGroup();
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG15, "DeleteBlobDepot", (TabletId, Self->TabletID()), (GroupId, group->ID));
            Self->Execute(std::make_unique<TTxUpdateGroup>(this, [](TGroupInfo& group) {
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

        void Handle(TEvBlobDepot::TEvApplyConfigResult::TPtr /*ev*/) {
            NTabletPipe::CloseAndForgetClient(SelfId(), BlobDepotPipeId);

            Self->Execute(std::make_unique<TTxUpdateGroup>(this, [&](TGroupInfo& group) {
                group.VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::WORKING;
                auto& config = GetConfig(&group);
                Y_VERIFY(config.HasTabletId());
                group.BlobDepotId = config.GetTabletId();
                group.NeedAlter = false;
                if (group.DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::PENDING) {
                    group.DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS;
                    group.ContentChanged = true;
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
                groups.insert(group->ID);
                Self->ReadGroups(groups, false, ev.get(), nodeId);
                Send(MakeBlobStorageNodeWardenID(nodeId), ev.release());
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void PassAway() override {
            if (!Expired()) {
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
                    Y_VERIFY_DEBUG(false, "unexpected event Type# %08" PRIx32, type);
            }
        }

        TGroupInfo *GetGroup() {
            TGroupInfo *res = Self->FindGroup(GroupId);
            Y_VERIFY(res);
            return res;
        }

        bool Expired() const {
            if (!TlsActivationContext->Mailbox.FindActor(ControllerId.LocalId())) { // BS_CONTROLLER died
                return true;
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
                    Y_VERIFY(group);
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
    }

    void TBlobStorageController::StartVirtualGroupSetupMachine(TGroupInfo *group) {
        Y_VERIFY(!group->VirtualGroupSetupMachineId);
        group->VirtualGroupSetupMachineId = RegisterWithSameMailbox(new TVirtualGroupSetupMachine(this, *group));
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
                TGroupInfo *group = state.Groups.FindForUpdate(groupId);
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
                    group->ContentChanged = true;
                    group->Topology = std::make_shared<TBlobStorageGroupInfo::TTopology>(group->Topology->GType, 0, 0, 0);
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
                                        Y_VERIFY(it != StoragePools.end());
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
