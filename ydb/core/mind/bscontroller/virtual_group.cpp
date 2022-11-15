#include "impl.h"
#include "config.h"

namespace NKikimr::NBsController {

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TAllocateVirtualGroup& cmd, TStatus& status) {
        if (!cmd.GetName()) {
            throw TExError() << "TAllocateVirtualGroup.Name must be set and be nonempty";
        } else if (!cmd.GetHiveId()) {
            throw TExError() << "TAllocateVirtualGroup.HiveId is not specified";
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
        group->HiveId = cmd.GetHiveId();

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
        if (!cmd.GetHiveId()) {
            throw TExError() << "TDecommitGroups.HiveId is not specified";
        }

        for (const TGroupId groupId : cmd.GetGroupIds()) {
            TGroupInfo *group = Groups.FindForUpdate(groupId);
            if (!group) {
                throw TExError() << "group not found" << TErrorParams::GroupId(groupId);
            } else if (group->DecommitStatus != NKikimrBlobStorage::TGroupDecommitStatus::NONE) {
                throw TExError() << "group is already being decommitted" << TErrorParams::GroupId(groupId);
            }

            group->DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::PENDING;
            group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::NEW;
            group->HiveId = cmd.GetHiveId();
            group->NeedAlter = true;

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
                        throw TExError() << "group not found" << TErrorParams::GroupId(begin->second)
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
                data->SetCount(numPhysicalGroups);
            }

            const bool success = config.SerializeToString(&group->BlobDepotConfig.ConstructInPlace());
            Y_VERIFY(success);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    class TBlobStorageController::TVirtualGroupSetupMachine : public TActorBootstrapped<TVirtualGroupSetupMachine> {
        TBlobStorageController *Self;
        const TActorId ControllerId;
        const TGroupId GroupId;

    private:
        class TTxUpdateGroup : public TTransactionBase<TBlobStorageController> {
            TVirtualGroupSetupMachine *Machine;

        public:
            TTxUpdateGroup(TVirtualGroupSetupMachine *machine)
                : TTransactionBase(machine->Self)
                , Machine(machine)
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                TGroupInfo *group = GetGroup(true);
                NIceDb::TNiceDb db(txc.DB);
                using T = Schema::Group;
                auto row = db.Table<T>().Key(group->ID);
#define PARAM(NAME) \
                if (const auto& cell = group->NAME) { \
                    row.Update<T::NAME>(*cell); \
                }
                PARAM(VirtualGroupState)
                PARAM(BlobDepotConfig)
                PARAM(BlobDepotId)
                PARAM(ErrorReason)
                PARAM(NeedAlter)
#undef PARAM
                if (group->SeenOperational) {
                    row.Update<T::SeenOperational>(true);
                }
                return true;
            }

            void Complete(const TActorContext&) override {
                TGroupInfo *group = GetGroup(false);
                Y_VERIFY(group->VirtualGroupSetupMachineId == Machine->SelfId());
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Bootstrap, 0, Machine->SelfId(), {}, nullptr, 0));
            }

        private:
            TGroupInfo *GetGroup(bool commitInProgress) {
                TGroupInfo *group = Machine->GetGroup();
                Y_VERIFY(group->CommitInProgress != commitInProgress);
                group->CommitInProgress = commitInProgress;
                return group;
            }
        };

        class TTxCommitDecommit : public TTransactionBase<TBlobStorageController> {
            TVirtualGroupSetupMachine *Machine;
            std::optional<TConfigState> State;

        public:
            TTxCommitDecommit(TVirtualGroupSetupMachine *machine)
                : TTransactionBase(machine->Self)
                , Machine(machine)
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                State.emplace(*Self, Self->HostRecords, TActivationContext::Now());
                State->CheckConsistency();
                TGroupInfo *group = State->Groups.FindForUpdate(Machine->GroupId);
                if (group && group->DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::PENDING) {
                    group->DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS;
                    group->ContentChanged = true;
                }
                State->CheckConsistency();
                TString error;
                if (State->Changed() && !Self->CommitConfigUpdates(*State, false, false, false, txc, &error)) {
                    State->Rollback();
                    State.reset();
                }
                return true;
            }

            void Complete(const TActorContext&) override {
                if (State) {
                    State->ApplyConfigUpdates();
                }
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
            Y_VERIFY(group->VirtualGroupState);

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
                    group->VirtualGroupSetupMachineId = {};
                    PassAway();
                    break;

                default:
                    Y_FAIL();
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        TActorId HivePipeId;
        TActorId BlobDepotPipeId;
        ui64 BlobDepotTabletId = 0;

        void HiveCreate(TGroupInfo *group) {
            Y_VERIFY(group->HiveId);
            HivePipeId = Register(NTabletPipe::CreateClient(SelfId(), *group->HiveId,
                NTabletPipe::TClientRetryPolicy::WithRetries()));

            NKikimrBlobDepot::TBlobDepotConfig config;
            Y_VERIFY(group->BlobDepotConfig);
            const bool success = config.ParseFromString(*group->BlobDepotConfig);
            Y_VERIFY(success);

            TChannelsBindings bindings;
            for (const auto& item : config.GetChannelProfiles()) {
                for (ui32 i = 0; i < item.GetCount(); ++i) {
                    NKikimrStoragePool::TChannelBind binding;
                    binding.SetStoragePoolName(item.GetStoragePoolName());
                    if (config.GetIsDecommittingGroup()) {
                        binding.SetPhysicalGroupsOnly(true);
                    }
                    bindings.push_back(std::move(binding));
                }
            }

            auto ev = std::make_unique<TEvHive::TEvCreateTablet>(Self->TabletID(), group->ID, TTabletTypes::BlobDepot, bindings);
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG00, "sending TEvCreateTablet", (TabletId, Self->TabletID()),
                (GroupId, group->ID), (HiveId, *group->HiveId), (Msg, ev->Record));

            NTabletPipe::SendData(SelfId(), HivePipeId, ev.release());
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG02, "received TEvClientConnected", (TabletId, Self->TabletID()),
                (Status, ev->Get()->Status), (ClientId, ev->Get()->ClientId), (HivePipeId, HivePipeId),
                (BlobDepotPipeId, BlobDepotPipeId));

            if (ev->Get()->Status != NKikimrProto::OK) {
                if (ev->Get()->ClientId == HivePipeId) {
                    HiveCreate(GetGroup());
                } else if (ev->Get()->ClientId == BlobDepotPipeId) {
                    ConfigureBlobDepot();
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
            } else if (clientId == BlobDepotPipeId) {
                BlobDepotPipeId = {};
            }
        }

        void Handle(TEvHive::TEvCreateTabletReply::TPtr ev) {
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG04, "received TEvCreateTabletReply", (TabletId, Self->TabletID()),
                (Msg, ev->Get()->Record));

            TGroupInfo *group = GetGroup();
            auto& record = ev->Get()->Record;
            if (record.GetStatus() == NKikimrProto::OK || record.GetStatus() == NKikimrProto::ALREADY) {
                BlobDepotTabletId = record.GetTabletID();
                Y_VERIFY(BlobDepotTabletId);
            } else {
                group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED;
                group->ErrorReason = SingleLineProto(record);
                Self->Execute(new TTxUpdateGroup(this));
            }

            NTabletPipe::CloseAndForgetClient(SelfId(), HivePipeId);
        }

        void Handle(TEvHive::TEvTabletCreationResult::TPtr ev) {
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG05, "received TEvTabletCreationResult", (TabletId, Self->TabletID()),
                (Msg, ev->Get()->Record));

            ConfigureBlobDepot();
        }

        void ConfigureBlobDepot() {
            TGroupInfo *group = GetGroup();
            const ui64 tabletId = group->BlobDepotId.GetOrElse(BlobDepotTabletId);
            BlobDepotPipeId = Register(NTabletPipe::CreateClient(SelfId(), tabletId,
                NTabletPipe::TClientRetryPolicy::WithRetries()));
            auto ev = std::make_unique<TEvBlobDepot::TEvApplyConfig>();
            Y_VERIFY(group->BlobDepotConfig);
            const bool success = ev->Record.MutableConfig()->ParseFromString(*group->BlobDepotConfig);
            Y_VERIFY(success);
            NTabletPipe::SendData(SelfId(), BlobDepotPipeId, ev.release());
        }

        void Handle(TEvBlobDepot::TEvApplyConfigResult::TPtr /*ev*/) {
            TGroupInfo *group = GetGroup();
            group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::WORKING;
            group->BlobDepotId = BlobDepotTabletId;
            group->NeedAlter = false; // race-check
            Self->Execute(new TTxUpdateGroup(this));

            if (group->DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::PENDING) {
                Self->Execute(new TTxCommitDecommit(this));
            }

            NTabletPipe::CloseAndForgetClient(SelfId(), BlobDepotPipeId);
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
            NTabletPipe::CloseClient(SelfId(), HivePipeId);
            NTabletPipe::CloseClient(SelfId(), BlobDepotPipeId);
            TActorBootstrapped::PassAway();
        }

        void StateFunc(STFUNC_SIG) {
            Y_UNUSED(ctx);
            if (Expired()) {
                return PassAway();
            }
            switch (const ui32 type = ev->GetTypeRewrite()) {
                cFunc(TEvents::TSystem::Poison, PassAway);
                cFunc(TEvents::TSystem::Bootstrap, Bootstrap);
                hFunc(TEvTabletPipe::TEvClientConnected, Handle);
                hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
                hFunc(TEvHive::TEvCreateTabletReply, Handle);
                hFunc(TEvHive::TEvTabletCreationResult, Handle);
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
            return !TlsActivationContext->Mailbox.FindActor(ControllerId.LocalId());
        }
    };

    void TBlobStorageController::CommitVirtualGroupUpdates(TConfigState& state) {
        for (const auto& [base, overlay] : state.Groups.Diff()) {
            TGroupInfo *group = overlay->second.Get();
            auto startSetupMachine = [&] {
                Y_VERIFY(group);
                if (group->VirtualGroupState && !group->VirtualGroupSetupMachineId) {
                    state.Callbacks.push_back(std::bind(&TThis::StartVirtualGroupSetupMachine, this, group));
                }
            };

            if (!base) { // new group was created
                startSetupMachine();
            } else if (!overlay->second) { // existing group was just deleted
                Y_VERIFY(!base->second->VirtualGroupSetupMachineId);
            } else if (overlay->second->NeedAlter.GetOrElse(false)) {
                startSetupMachine();
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

//            TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_DROP_DONOR; }

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                State.emplace(*Self, Self->HostRecords, TActivationContext::Now());
                State->CheckConsistency();
                Action(*State);
                State->CheckConsistency();
                TString error;
                if (State->Changed() && !Self->CommitConfigUpdates(*State, false, false, false, txc, &error)) {
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
                    return;
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
                }
            }

            void Complete(const TActorContext&) override {
                if (State) {
                    State->ApplyConfigUpdates();
                }
                auto ev = std::make_unique<TEvBlobStorage::TEvControllerGroupDecommittedResponse>(Status);
                if (ErrorReason) {
                    ev->Record.SetErrorReason(ErrorReason);
                }
                Self->Send(Ev->Sender, ev.release(), 0, Ev->Cookie);
            }
        };

        Execute(new TTxDecommitGroup(this, ev));
    }

} // NKikimr::NBsController
