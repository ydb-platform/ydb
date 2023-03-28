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
                if (group->HiveId != cmd.GetHiveId()) {
                    throw TExError() << "different hive specified for decommitting group" << TErrorParams::GroupId(groupId);
                }
                // group is already being decommitted -- make this operation idempotent
                continue;
            } else if (group->VirtualGroupState) {
                throw TExError() << "group is already virtual" << TErrorParams::GroupId(groupId);
            }

            group->DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::PENDING;
            group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::NEW;
            group->HiveId = cmd.GetHiveId();
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
                data->SetCount(Min<ui32>(250, numPhysicalGroups));
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
            std::optional<TConfigState> State;
            std::function<void(TGroupInfo&)> Callback;

        public:
            TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_GROUP; }

            TTxUpdateGroup(TVirtualGroupSetupMachine *machine, std::function<void(TGroupInfo&)>&& callback)
                : TTransactionBase(machine->Self)
                , Machine(machine)
                , Callback(std::move(callback))
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                State.emplace(*Self, Self->HostRecords, TActivationContext::Now());
                TGroupInfo *group = State->Groups.FindForUpdate(Machine->GroupId);
                Y_VERIFY(group);
                Callback(*group);
                group->CalculateGroupStatus();
                TString error;
                if (State->Changed() && !Self->CommitConfigUpdates(*State, true, true, true, txc, &error)) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSCVG08, "failed to commit update",
                        (VirtualGroupId, Machine->GroupId), (Error, error));
                    State->Rollback();
                    State.reset();
                }
                return true;
            }

            void Complete(const TActorContext&) override {
                TGroupInfo *group = Machine->GetGroup();
                Y_VERIFY(group->VirtualGroupSetupMachineId == Machine->SelfId());
                if (State) {
                    State->ApplyConfigUpdates();
                }
                TActivationContext::Send(new IEventHandleFat(TEvents::TSystem::Bootstrap, 0, Machine->SelfId(), {}, nullptr, 0));
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
            std::unordered_set<TString> names;
            auto invalidateEv = std::make_unique<TEvHive::TEvInvalidateStoragePools>();
            auto& record = invalidateEv->Record;

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

            auto& record = ev->Get()->Record;
            if (record.GetStatus() == NKikimrProto::OK || record.GetStatus() == NKikimrProto::ALREADY) {
                BlobDepotTabletId = record.GetTabletID();
                Y_VERIFY(BlobDepotTabletId);
            } else {
                Self->Execute(new TTxUpdateGroup(this, [&](TGroupInfo& group) {
                    group.VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED;
                    group.ErrorReason = TStringBuilder() << "failed to create BlobDepot tablet"
                        << " Reason# " << NKikimrHive::EErrorReason_Name(record.GetErrorReason())
                        << " Status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus());
                }));
            }

            NTabletPipe::CloseAndForgetClient(SelfId(), HivePipeId);
            ConfigureBlobDepot();
        }

        void Handle(TEvHive::TEvTabletCreationResult::TPtr ev) {
            STLOG(PRI_INFO, BS_CONTROLLER, BSCVG05, "received TEvTabletCreationResult", (TabletId, Self->TabletID()),
                (Msg, ev->Get()->Record));
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
            NTabletPipe::CloseAndForgetClient(SelfId(), BlobDepotPipeId);

            Self->Execute(new TTxUpdateGroup(this, [&](TGroupInfo& group) {
                group.VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::WORKING;
                group.BlobDepotId = BlobDepotTabletId;
                group.NeedAlter = false;
                if (group.DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::PENDING) {
                    group.DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS;
                    group.ContentChanged = true;
                }
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
                hFunc(TEvHive::TEvInvalidateStoragePoolsReply, Handle);
                hFunc(TEvHive::TEvReassignOnDecommitGroupReply, Handle);
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
                auto reply = std::make_unique<IEventHandleFat>(Ev->Sender, Self->SelfId(), ev.release(), 0, Ev->Cookie);
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
