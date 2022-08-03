#include "impl.h"
#include "config.h"

namespace NKikimr::NBsController {

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TAllocateVirtualGroup& cmd, TStatus& status) {
        const TString id = cmd.GetVirtualGroupPool();
        if (!id) {
            throw TExError() << "VirtualGroupPool can't be empty";
        }

        // update record generation to prevent races
        const ui64 nextGen = CheckGeneration(cmd, VirtualGroupPools.Get(), id);
        auto& map = VirtualGroupPools.Unshare();
        auto& item = map[id];
        item.Generation = nextGen;

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

        group->VirtualGroupPool = id;
        group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::CREATED;
        group->ParentDir = cmd.GetParentDir();
        group->Name = TStringBuilder() << "vgroup" << groupId.GetRaw();

        if (cmd.GetBlobDepotId()) {
            group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::WORKING;
            group->BlobDepotId = cmd.GetBlobDepotId();
            group->SeenOperational = true;
        }

        group->CalculateGroupStatus();

        NKikimrBlobDepot::TBlobDepotConfig config;
        config.SetOperationMode(NKikimrBlobDepot::EOperationMode::VirtualGroup);
        config.MutableChannelProfiles()->CopyFrom(cmd.GetChannelProfiles());

        const bool success = config.SerializeToString(&group->BlobDepotConfig.ConstructInPlace());
        Y_VERIFY(success);

        for (const TGroupId groupToDecommit : cmd.GetDecommitGroups()) {
            InitiateGroupDecommission(group->ID, groupToDecommit);
        }

        status.AddGroupId(group->ID);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDecommitGroups& cmd, TStatus& /*status*/) {
        for (const TGroupId groupToDecommit : cmd.GetDecommitGroups()) {
            InitiateGroupDecommission(cmd.GetVirtualGroupId(), groupToDecommit);
        }
    }

    void TBlobStorageController::TConfigState::InitiateGroupDecommission(ui32 virtualGroupId, ui32 groupToDecommit) {
        TGroupInfo *virtualGroup = Groups.FindForUpdate(virtualGroupId);
        if (!virtualGroup) {
            throw TExError() << "virtual group not found" << TErrorParams::GroupId(virtualGroupId);
        } else if (!virtualGroup->BlobDepotConfig) {
            throw TExError() << "virtual group blob depot config is not filled in" << TErrorParams::GroupId(virtualGroup->ID);
        }

        auto *group = Groups.FindForUpdate(groupToDecommit);
        if (!group) {
            throw TExError() << "group id for decomission is not found" << TErrorParams::GroupId(groupToDecommit);
        } else if (group->DecommitStatus != NKikimrBlobStorage::EGroupDecommitStatus::NONE) {
            throw TExError() << "group is already being decommitted" << TErrorParams::GroupId(group->ID);
        } else if (group->AssimilatorGroupId) {
            throw TExError() << "don't know how, but AssimilatorGroupId is already filled in" << TErrorParams::GroupId(group->ID);
        }
        group->DecommitStatus = NKikimrBlobStorage::EGroupDecommitStatus::PENDING;
        group->AssimilatorGroupId = virtualGroup->ID;
        group->ContentChanged = true; // advance group generation to push configs forcibly to all concerned nodes

        NKikimrBlobDepot::TBlobDepotConfig blobDepotConfig;
        if (!blobDepotConfig.ParseFromString(*virtualGroup->BlobDepotConfig)) {
            throw TExError() << "failed to parse virtual group blob depot config" << TErrorParams::GroupId(virtualGroup->ID);
        }
        blobDepotConfig.AddDecommittingGroups(groupToDecommit);
        if (!blobDepotConfig.SerializeToString(&*virtualGroup->BlobDepotConfig)) {
            throw TExError() << "failed to serialize virtual group blob depot config" << TErrorParams::GroupId(virtualGroup->ID);
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
                PARAM(ParentDir)
                PARAM(Name)
                PARAM(SchemeshardId)
                PARAM(BlobDepotConfig)
                PARAM(TxId)
                PARAM(PathId)
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
                (State, NKikimrBlobStorage::EVirtualGroupState_Name(*group->VirtualGroupState)));

            switch (*group->VirtualGroupState) {
                case NKikimrBlobStorage::EVirtualGroupState::CREATED:
                    IssueCreateOrAlterPathRequest(group, NKikimrSchemeOp::ESchemeOpCreateBlobDepot);
                    break;

                case NKikimrBlobStorage::EVirtualGroupState::WORKING:
                case NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED:
                    IssueNodeNotifications(group);
                    group->VirtualGroupSetupMachineId = {};
                    PassAway();
                    break;

                case NKikimrBlobStorage::EVirtualGroupState::WAIT_SCHEMESHARD_CREATE:
                    SubscribeToSchemeshard(group);
                    break;

                case NKikimrBlobStorage::EVirtualGroupState::WAIT_SCHEMESHARD_ALTER:
                    IssueCreateOrAlterPathRequest(group, NKikimrSchemeOp::ESchemeOpAlterBlobDepot);
                    group->NeedAlter = false;
                    break;

                default:
                    Y_FAIL();
            }
        }

        void IssueCreateOrAlterPathRequest(TGroupInfo *group, NKikimrSchemeOp::EOperationType op) {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG02, "IssueCreateOrAlterPathRequest", (GroupId, GroupId),
                (ParentDir, *group->ParentDir), (Name, *group->Name));

            auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
            auto& record = request->Record;

            Y_VERIFY(group->ParentDir);
            Y_VERIFY(group->Name);
            Y_VERIFY(group->BlobDepotConfig);

            auto *tx = record.MutableTransaction();
            auto *scheme = tx->MutableModifyScheme();
            scheme->SetWorkingDir(*group->ParentDir);
            scheme->SetOperationType(op);
            scheme->SetInternal(true);
            scheme->SetFailOnExist(false); // this operation has to be idempotent
            auto *bd = scheme->MutableBlobDepot();
            bd->SetName(*group->Name);
            const bool success = bd->MutableConfig()->ParseFromString(*group->BlobDepotConfig);
            Y_VERIFY(success);

            Send(MakeTxProxyID(), request.release());
        }

        void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr ev) {
            if (Expired()) {
                return PassAway();
            }

            TGroupInfo *group = GetGroup();

            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG01, "got TEvProposeTransactionStatus", (GroupId, GroupId),
                (ParentDir, group->ParentDir), (Name, group->Name), (Msg, ev->Get()->Record.DebugString()));

            const auto& record = ev->Get()->Record;
            if (record.GetSchemeShardStatus() == NKikimrScheme::StatusAccepted || record.GetSchemeShardStatus() == NKikimrScheme::StatusAlreadyExists) {
                group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::WAIT_SCHEMESHARD_CREATE;
                group->SchemeshardId = record.GetSchemeShardTabletId();
                group->TxId = record.GetTxId();
                group->PathId = record.GetPathId();
            } else {
                group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED;
                group->ErrorReason = record.GetSchemeShardReason();
            }
            group->CalculateGroupStatus();

            Self->Execute(new TTxUpdateGroup(this));
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        TActorId SchemeshardPipeId;

        void SubscribeToSchemeshard(TGroupInfo *group) {
            Y_VERIFY(group->SchemeshardId);
            Y_VERIFY(group->TxId);
            SchemeshardPipeId = Register(NTabletPipe::CreateClient(SelfId(), *group->SchemeshardId));
            NTabletPipe::SendData(SelfId(), SchemeshardPipeId, new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion(*group->TxId));
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG03, "TEvClientConnected", (GroupId, GroupId), (Msg, ev->Get()->ToString()));
        }

        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
            STLOG(PRI_NOTICE, BS_CONTROLLER, BSCVG04, "TEvClientDestroyed", (GroupId, GroupId), (Msg, ev->Get()->ToString()));
            SchemeshardPipeId = {};
            TActivationContext::Schedule(TDuration::Seconds(5), new IEventHandle(TEvents::TSystem::Bootstrap, 0, SelfId(), {}, nullptr, 0));
        }

        void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr ev) {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG05, "TEvNotifyTxCompletionRegistered", (GroupId, GroupId), (Msg, ev->Get()->ToString()));
        }

        void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr ev) {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG06, "TEvNotifyTxCompletionResult", (GroupId, GroupId), (Msg, ev->Get()->ToString()));

            if (Expired()) {
                return PassAway();
            }

            TGroupInfo *group = GetGroup();
            Y_VERIFY(group->SchemeshardId);
            Y_VERIFY(group->PathId);
            NTabletPipe::SendData(SelfId(), SchemeshardPipeId, new NSchemeShard::TEvSchemeShard::TEvDescribeScheme(
                *group->SchemeshardId, *group->PathId));
        }

        void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr ev) {
            const auto& record = ev->Get()->GetRecord();

            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCVG07, "TEvDescribeSchemeResult", (GroupId, GroupId),
                (Record, record));

            if (Expired()) {
                return PassAway();
            }

            TGroupInfo *group = GetGroup();

            const auto& desc = record.GetPathDescription().GetBlobDepotDescription();
            group->VirtualGroupState = NKikimrBlobStorage::EVirtualGroupState::WORKING;
            group->BlobDepotId = desc.GetTabletId();
            group->SeenOperational = true;
            group->CalculateGroupStatus();
            Y_VERIFY(*group->BlobDepotId);

            Self->Execute(new TTxUpdateGroup(this));
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
            NTabletPipe::CloseAndForgetClient(SelfId(), SchemeshardPipeId);
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateFunc,
            cFunc(TEvents::TSystem::Poison, PassAway);
            cFunc(TEvents::TSystem::Bootstrap, Bootstrap);
            hFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);

            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
        )

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

} // NKikimr::NBsController
