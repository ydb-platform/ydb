#include "distconf.h"
#include "node_warden_impl.h"

namespace NKikimr::NStorage {

    class TDistributedConfigKeeper::TInvokeRequestHandlerActor : public TActorBootstrapped<TInvokeRequestHandlerActor> {
        TDistributedConfigKeeper* const Self;
        const std::weak_ptr<TLifetimeToken> LifetimeToken;
        const std::weak_ptr<TScepter> Scepter;
        std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>> Event;
        const TActorId Sender;
        const ui64 Cookie;
        const TActorId RequestSessionId;

        TActorId ParentId;
        ui32 WaitingReplyFromNode = 0;

        using TQuery = NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot;
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

    public:
        TInvokeRequestHandlerActor(TDistributedConfigKeeper *self, std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>>&& ev)
            : Self(self)
            , LifetimeToken(Self->LifetimeToken)
            , Scepter(Self->Scepter)
            , Event(std::move(ev))
            , Sender(Event->Sender)
            , Cookie(Event->Cookie)
            , RequestSessionId(Event->InterconnectSession)
        {}

        void Bootstrap(TActorId parentId) {
            if (LifetimeToken.expired()) {
                return FinishWithError(TResult::ERROR, "distributed config keeper terminated");
            }

            STLOG(PRI_DEBUG, BS_NODE, NWDC42, "TInvokeRequestHandlerActor::Bootstrap", (Sender, Sender), (Cookie, Cookie),
                (SelfId, SelfId()), (Binding, Self->Binding), (RootState, Self->RootState));

            ParentId = parentId;
            Become(&TThis::StateFunc);

            if (auto scepter = Scepter.lock()) {
                ExecuteQuery();
            } else if (!Self->Binding) {
                FinishWithError(TResult::NO_QUORUM, "no quorum obtained");
            } else if (RequestSessionId) {
                FinishWithError(TResult::ERROR, "no double-hop invokes allowed");
            } else {
                const ui32 root = Self->Binding->RootNodeId;
                Send(MakeBlobStorageNodeWardenID(root), Event->Release(), IEventHandle::FlagSubscribeOnSession);
                const auto [it, inserted] = Subscriptions.try_emplace(root);
                Y_ABORT_UNLESS(inserted);
                WaitingReplyFromNode = root;
            }
        }

        void Handle(TEvNodeConfigInvokeOnRootResult::TPtr ev) {
            if (ev->HasEvent()) {
                Finish(Sender, SelfId(), ev->ReleaseBase().Release(), ev->Flags, Cookie);
            } else {
                Finish(ev->Type, ev->Flags, Sender, SelfId(), ev->ReleaseChainBuffer(), Cookie);
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Interconnect machinery

        THashMap<ui32, TActorId> Subscriptions;

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr ev) {
            const ui32 nodeId = ev->Get()->NodeId;
            if (const auto it = Subscriptions.find(nodeId); it != Subscriptions.end()) {
                it->second = ev->Sender;
            }
        }

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
            const ui32 nodeId = ev->Get()->NodeId;
            Subscriptions.erase(nodeId);
            if (nodeId == WaitingReplyFromNode) {
                FinishWithError(TResult::ERROR, "root node disconnected");
            }
            for (auto [begin, end] = NodeToVDisk.equal_range(nodeId); begin != end; ++begin) {
                OnVStatusError(begin->second);
            }
        }

        void UnsubscribeInterconnect() {
            for (auto it = Subscriptions.begin(); it != Subscriptions.end(); ) {
                const TActorId actorId = it->second ? it->second : TActivationContext::InterconnectProxy(it->first);
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, actorId, SelfId(), nullptr, 0));
                Subscriptions.erase(it++);
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Query execution logic

        void ExecuteQuery() {
            auto& record = Event->Get()->Record;
            STLOG(PRI_DEBUG, BS_NODE, NWDC43, "ExecuteQuery", (SelfId, SelfId()), (Record, record));
            switch (record.GetRequestCase()) {
                case TQuery::kUpdateConfig: {
                    auto *request = record.MutableUpdateConfig();

                    if (!RunCommonChecks()) {
                        return;
                    }

                    auto *config = request->MutableConfig();

                    if (auto error = ValidateConfig(*Self->StorageConfig)) {
                        return FinishWithError(TResult::ERROR, TStringBuilder() << "current config validation failed: " << *error);
                    } else if (auto error = ValidateConfigUpdate(*Self->StorageConfig, *config)) {
                        return FinishWithError(TResult::ERROR, TStringBuilder() << "config validation failed: " << *error);
                    }

                    return StartProposition(config);
                }

                case TQuery::kQueryConfig: {
                    auto ev = PrepareResult(TResult::OK, std::nullopt);
                    auto *record = &ev->Record;
                    auto *response = record->MutableQueryConfig();
                    if (Self->StorageConfig) {
                        response->MutableConfig()->CopyFrom(*Self->StorageConfig);
                    }
                    if (Self->CurrentProposedStorageConfig) {
                        response->MutableCurrentProposedStorageConfig()->CopyFrom(*Self->CurrentProposedStorageConfig);
                    }
                    return Finish(Sender, SelfId(), ev.release(), 0, Cookie);
                }

                case TQuery::kReassignGroupDisk:
                    return ReassignGroupDisk(record.GetReassignGroupDisk());

                case TQuery::kStaticVDiskSlain:
                    return StaticVDiskSlain(record.GetStaticVDiskSlain());

                case TQuery::kDropDonor:
                    return DropDonor(record.GetDropDonor());

                case TQuery::kReassignStateStorageNode:
                    return ReassignStateStorageNode(record.GetReassignStateStorageNode());

                case TQuery::kAdvanceGeneration:
                    return AdvanceGeneration();

                case TQuery::REQUEST_NOT_SET:
                    return FinishWithError(TResult::ERROR, "Request field not set");
            }

            FinishWithError(TResult::ERROR, "unhandled request");
        }

        void Handle(TEvNodeConfigGather::TPtr ev) {
            auto& record = ev->Get()->Record;
            STLOG(PRI_DEBUG, BS_NODE, NWDC44, "Handle(TEvNodeConfigGather)", (SelfId, SelfId()), (Record, record));
            switch (record.GetResponseCase()) {
                case TEvGather::kProposeStorageConfig: {
                    std::unique_ptr<TEvNodeConfigInvokeOnRootResult> ev;
                    if (auto error = Self->ProcessProposeStorageConfig(record.MutableProposeStorageConfig())) {
                        ev = PrepareResult(TResult::ERROR, *error);
                    } else {
                        ev = PrepareResult(TResult::OK, std::nullopt);
                    }
                    return Finish(Sender, SelfId(), ev.release(), 0, Cookie);
                }

                default:
                    return FinishWithError(TResult::ERROR, "unexpected Response case in resulting TEvGather");
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Reassign group disk logic

        void ReassignGroupDisk(const TQuery::TReassignGroupDisk& cmd) {
            if (!RunCommonChecks()) {
                return;
            }

            bool found = false;
            const TVDiskID vdiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());
            for (const auto& group : Self->StorageConfig->GetBlobStorageConfig().GetServiceSet().GetGroups()) {
                if (group.GetGroupID() == vdiskId.GroupID.GetRawId()) {
                    if (group.GetGroupGeneration() != vdiskId.GroupGeneration) {
                        return FinishWithError(TResult::ERROR, TStringBuilder() << "group generation mismatch"
                            << " GroupId# " << group.GetGroupID()
                            << " Generation# " << group.GetGroupGeneration()
                            << " VDiskId# " << vdiskId);
                    }
                    found = true;
                    if (!cmd.GetIgnoreGroupFailModelChecks()) {
                        IssueVStatusQueries(group);
                    }
                    break;
                }
            }
            if (!found) {
                return FinishWithError(TResult::ERROR, TStringBuilder() << "GroupId# " << vdiskId.GroupID << " not found");
            }

            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryBaseConfig);
        }

        THashMultiMap<ui32, TVDiskID> NodeToVDisk;
        THashMap<TActorId, TVDiskID> ActorToVDisk;
        std::optional<NKikimrBlobStorage::TBaseConfig> BaseConfig;
        THashSet<TVDiskID> PendingVDiskIds;
        TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
        std::optional<TBlobStorageGroupInfo::TGroupVDisks> SuccessfulVDisks;

        void IssueVStatusQueries(const NKikimrBlobStorage::TGroupInfo& group) {
            TStringStream err;
            GroupInfo = TBlobStorageGroupInfo::Parse(group, nullptr, &err);
            if (!GroupInfo) {
                return FinishWithError(TResult::ERROR, TStringBuilder() << "failed to parse group info: " << err.Str());
            }
            SuccessfulVDisks.emplace(&GroupInfo->GetTopology());

            for (ui32 i = 0, num = GroupInfo->GetTotalVDisksNum(); i < num; ++i) {
                const TVDiskID vdiskId = GroupInfo->GetVDiskId(i);
                const TActorId actorId = GroupInfo->GetActorId(i);
                const ui32 flags = IEventHandle::FlagTrackDelivery |
                    (actorId.NodeId() == SelfId().NodeId() ? 0 : IEventHandle::FlagSubscribeOnSession);
                STLOG(PRI_DEBUG, BS_NODE, NW53, "sending TEvVStatus", (SelfId, SelfId()), (VDiskId, vdiskId),
                    (ActorId, actorId));
                Send(actorId, new TEvBlobStorage::TEvVStatus(vdiskId), flags);
                if (actorId.NodeId() != SelfId().NodeId()) {
                    NodeToVDisk.emplace(actorId.NodeId(), vdiskId);
                }
                ActorToVDisk.emplace(actorId, vdiskId);
                PendingVDiskIds.emplace(vdiskId);
            }
        }

        void Handle(TEvBlobStorage::TEvVStatusResult::TPtr ev) {
            const auto& record = ev->Get()->Record;
            const TVDiskID vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
            STLOG(PRI_DEBUG, BS_NODE, NW54, "TEvVStatusResult", (SelfId, SelfId()), (Record, record), (VDiskId, vdiskId));
            if (!PendingVDiskIds.erase(vdiskId)) {
                return FinishWithError(TResult::ERROR, TStringBuilder() << "TEvVStatusResult VDiskID# " << vdiskId
                    << " is unexpected");
            }
            if (record.GetJoinedGroup() && record.GetReplicated()) {
                *SuccessfulVDisks |= {&GroupInfo->GetTopology(), vdiskId};
            }
            CheckReassignGroupDisk();
        }

        void Handle(TEvents::TEvUndelivered::TPtr ev) {
            if (const auto it = ActorToVDisk.find(ev->Sender); it != ActorToVDisk.end()) {
                Y_ABORT_UNLESS(ev->Get()->SourceType == TEvBlobStorage::EvVStatus);
                OnVStatusError(it->second);
            }
        }

        void OnVStatusError(TVDiskID vdiskId) {
            PendingVDiskIds.erase(vdiskId);
            CheckReassignGroupDisk();
        }

        void Handle(TEvNodeWardenBaseConfig::TPtr ev) {
            BaseConfig.emplace(std::move(ev->Get()->BaseConfig));
            CheckReassignGroupDisk();
        }

        void CheckReassignGroupDisk() {
            if (BaseConfig && PendingVDiskIds.empty()) {
                ReassignGroupDiskExecute();
            }
        }

        void ReassignGroupDiskExecute() {
            const auto& record = Event->Get()->Record;
            const auto& cmd = record.GetReassignGroupDisk();

            if (Scepter.expired()) {
                return FinishWithError(TResult::ERROR, "scepter lost during query execution");
            } else if (!RunCommonChecks()) {
                return;
            }

            STLOG(PRI_DEBUG, BS_NODE, NW55, "ReassignGroupDiskExecute", (SelfId, SelfId()));

            const auto& vdiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());

            ui64 maxSlotSize = 0;

            if (SuccessfulVDisks) {
                const auto& checker = GroupInfo->GetQuorumChecker();

                auto check = [&](auto failedVDisks, const char *base) {
                    bool wasDegraded = checker.IsDegraded(failedVDisks) && checker.CheckFailModelForGroup(failedVDisks);
                    failedVDisks |= {&GroupInfo->GetTopology(), vdiskId};

                    if (!checker.CheckFailModelForGroup(failedVDisks)) {
                        FinishWithError(TResult::ERROR, TStringBuilder()
                            << "ReassignGroupDisk would render group inoperable (" << base << ')');
                    } else if (!cmd.GetIgnoreDegradedGroupsChecks() && !wasDegraded && checker.IsDegraded(failedVDisks)) {
                        FinishWithError(TResult::ERROR, TStringBuilder()
                            << "ReassignGroupDisk would drive group into degraded state (" << base << ')');
                    } else {
                        return true;
                    }

                    return false;
                };

                if (!check(~SuccessfulVDisks.value(), "polling")) {
                    return;
                }

                // scan failed disks according to BS_CONTROLLER's data
                TBlobStorageGroupInfo::TGroupVDisks failedVDisks(&GroupInfo->GetTopology());
                for (const auto& vslot : BaseConfig->GetVSlot()) {
                    if (vslot.GetGroupId() != vdiskId.GroupID.GetRawId() || vslot.GetGroupGeneration() != vdiskId.GroupGeneration) {
                        continue;
                    }
                    if (!vslot.GetReady()) {
                        const TVDiskID vdiskId(TGroupId::FromProto(&vslot, &NKikimrBlobStorage::TBaseConfig::TVSlot::GetGroupId), vslot.GetGroupGeneration(), vslot.GetFailRealmIdx(),
                            vslot.GetFailDomainIdx(), vslot.GetVDiskIdx());
                        failedVDisks |= {&GroupInfo->GetTopology(), vdiskId};
                    }
                    if (vslot.HasVDiskMetrics()) {
                        const auto& m = vslot.GetVDiskMetrics();
                        if (m.HasAllocatedSize()) {
                            maxSlotSize = Max(maxSlotSize, m.GetAllocatedSize());
                        }
                    }
                }

                if (!check(failedVDisks, "BS_CONTROLLER state")) {
                    return;
                }
            }

            NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

            if (!config.HasBlobStorageConfig()) {
                return FinishWithError(TResult::ERROR, "no BlobStorageConfig defined");
            }
            const auto& bsConfig = config.GetBlobStorageConfig();

            if (!bsConfig.HasServiceSet()) {
                return FinishWithError(TResult::ERROR, "no ServiceSet defined");
            }
            const auto& ss = bsConfig.GetServiceSet();

            if (!bsConfig.HasAutoconfigSettings()) {
                return FinishWithError(TResult::ERROR, "no AutoconfigSettings defined");
            }
            const auto& settings = bsConfig.GetAutoconfigSettings();

            THashMap<TVDiskIdShort, NBsController::TPDiskId> replacedDisks;
            NBsController::TGroupMapper::TForbiddenPDisks forbid;
            for (const auto& vdisk : ss.GetVDisks()) {
                const TVDiskID currentVDiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
                if (!currentVDiskId.SameExceptGeneration(vdiskId)) {
                    continue;
                }
                if (currentVDiskId == vdiskId) {
                    NBsController::TPDiskId pdiskId;
                    if (cmd.HasPDiskId()) {
                        const auto& target = cmd.GetPDiskId();
                        pdiskId = {target.GetNodeId(), target.GetPDiskId()};
                    }
                    replacedDisks.emplace(vdiskId, pdiskId);
                } else {
                    Y_DEBUG_ABORT_UNLESS(vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY ||
                        vdisk.HasDonorMode());
                    const auto& loc = vdisk.GetVDiskLocation();
                    forbid.emplace(loc.GetNodeID(), loc.GetPDiskID());
                }
            }

            for (const auto& group : ss.GetGroups()) {
                if (group.GetGroupID() == vdiskId.GroupID.GetRawId()) {
                    try {
                        Self->AllocateStaticGroup(&config, vdiskId.GroupID.GetRawId(), vdiskId.GroupGeneration + 1,
                            TBlobStorageGroupType((TBlobStorageGroupType::EErasureSpecies)group.GetErasureSpecies()),
                            settings.GetGeometry(), settings.GetPDiskFilter(),
                            settings.HasPDiskType() ? std::make_optional(settings.GetPDiskType()) : std::nullopt,
                            replacedDisks, forbid, maxSlotSize,
                            &BaseConfig.value(), cmd.GetConvertToDonor(), cmd.GetIgnoreVSlotQuotaCheck(),
                            cmd.GetIsSelfHealReasonDecommit());
                    } catch (const TExConfigError& ex) {
                        STLOG(PRI_NOTICE, BS_NODE, NW49, "ReassignGroupDisk failed to allocate group", (SelfId, SelfId()),
                            (Config, config),
                            (BaseConfig, *BaseConfig),
                            (Error, ex.what()));
                        return FinishWithError(TResult::ERROR, TStringBuilder() << "failed to allocate group: " << ex.what());
                    }

                    config.SetGeneration(config.GetGeneration() + 1);
                    return StartProposition(&config);
                }
            }

            return FinishWithError(TResult::ERROR, TStringBuilder() << "group not found");
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // VDiskSlain/DropDonor logic

        void StaticVDiskSlain(const TQuery::TStaticVDiskSlain& cmd) {
            HandleDropDonorAndSlain(VDiskIDFromVDiskID(cmd.GetVDiskId()), cmd.GetVSlotId(), false);
        }

        void DropDonor(const TQuery::TDropDonor& cmd) {
            HandleDropDonorAndSlain(VDiskIDFromVDiskID(cmd.GetVDiskId()), cmd.GetVSlotId(), true);
        }

        void HandleDropDonorAndSlain(TVDiskID vdiskId, const NKikimrBlobStorage::TVSlotId& vslotId, bool isDropDonor) {
            if (!RunCommonChecks()) {
                return;
            }

            NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

            if (!config.HasBlobStorageConfig()) {
                return FinishWithError(TResult::ERROR, "no BlobStorageConfig defined");
            }
            auto *bsConfig = config.MutableBlobStorageConfig();

            if (!bsConfig->HasServiceSet()) {
                return FinishWithError(TResult::ERROR, "no ServiceSet defined");
            }
            auto *ss = bsConfig->MutableServiceSet();

            bool changes = false;
            ui32 pdiskUsageCount = 0;

            ui32 actualGroupGeneration = 0;
            for (const auto& group : ss->GetGroups()) {
                if (group.GetGroupID() == vdiskId.GroupID.GetRawId()) {
                    actualGroupGeneration = group.GetGroupGeneration();
                    break;
                }
            }
            Y_ABORT_UNLESS(0 < actualGroupGeneration && vdiskId.GroupGeneration < actualGroupGeneration);

            for (size_t i = 0; i < ss->VDisksSize(); ++i) {
                if (const auto& vdisk = ss->GetVDisks(i); vdisk.HasVDiskID() && vdisk.HasVDiskLocation()) {
                    const TVDiskID currentVDiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
                    if (!currentVDiskId.SameExceptGeneration(vdiskId) ||
                            vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
                        continue;
                    }

                    if (isDropDonor && !vdisk.HasDonorMode()) {
                        Y_ABORT_UNLESS(currentVDiskId.GroupGeneration == actualGroupGeneration);
                        auto *m = ss->MutableVDisks(i);
                        if (vdiskId.GroupGeneration) { // drop specific donor
                            for (size_t k = 0; k < m->DonorsSize(); ++k) {
                                const auto& donor = m->GetDonors(k);
                                const auto& loc = donor.GetVDiskLocation();
                                if (VDiskIDFromVDiskID(donor.GetVDiskId()) == vdiskId && loc.GetNodeID() == vslotId.GetNodeId() &&
                                        loc.GetPDiskID() == vslotId.GetPDiskId() && loc.GetVDiskSlotID() == vslotId.GetVSlotId()) {
                                    m->MutableDonors()->DeleteSubrange(k, 1);
                                    changes = true;
                                    break;
                                }
                            }
                        } else { // drop all of them
                            m->ClearDonors();
                            changes = true;
                        }
                        continue;
                    }

                    const auto& loc = vdisk.GetVDiskLocation();
                    if (loc.GetNodeID() != vslotId.GetNodeId() || loc.GetPDiskID() != vslotId.GetPDiskId()) {
                        continue;
                    }
                    ++pdiskUsageCount;

                    if (loc.GetVDiskSlotID() != vslotId.GetVSlotId()) {
                        continue;
                    }

                    Y_ABORT_UNLESS(currentVDiskId.GroupGeneration < actualGroupGeneration);

                    if (!isDropDonor) {
                        --pdiskUsageCount;
                        ss->MutableVDisks()->DeleteSubrange(i--, 1);
                        changes = true;
                    } else if (vdisk.HasDonorMode()) {
                        if (currentVDiskId == vdiskId || vdiskId.GroupGeneration == 0) {
                            auto *m = ss->MutableVDisks(i);
                            m->ClearDonorMode();
                            m->SetEntityStatus(NKikimrBlobStorage::EEntityStatus::DESTROY);
                            changes = true;
                        }
                    }
                }
            }

            if (!isDropDonor && !pdiskUsageCount) {
                for (size_t i = 0; i < ss->PDisksSize(); ++i) {
                    if (const auto& pdisk = ss->GetPDisks(i); pdisk.HasNodeID() && pdisk.HasPDiskID() &&
                            pdisk.GetNodeID() == vslotId.GetNodeId() && pdisk.GetPDiskID() == vslotId.GetPDiskId()) {
                        ss->MutablePDisks()->DeleteSubrange(i, 1);
                        changes = true;
                        break;
                    }
                }
            }

            if (!changes) {
                return Finish(Sender, SelfId(), PrepareResult(TResult::OK, std::nullopt).release(), 0, Cookie);
            }

            config.SetGeneration(config.GetGeneration() + 1);
            StartProposition(&config);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // State Storage operation

        void ReassignStateStorageNode(const TQuery::TReassignStateStorageNode& cmd) {
            if (!RunCommonChecks()) {
                return;
            }

            NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

            auto process = [&](const char *name, auto hasFunc, auto mutableFunc) {
                if (!(config.*hasFunc)()) {
                    FinishWithError(TResult::ERROR, TStringBuilder() << name << " configuration is not filled in");
                    return false;
                }

                auto *m = (config.*mutableFunc)();
                auto *ring = m->MutableRing();
                if (ring->RingSize() && ring->NodeSize()) {
                    FinishWithError(TResult::ERROR, TStringBuilder() << name << " incorrect configuration:"
                        " both Ring and Node fields are set");
                    return false;
                }

                const size_t numItems = Max(ring->RingSize(), ring->NodeSize());
                bool found = false;

                auto replace = [&](auto *ring, size_t i) {
                    if (ring->GetNode(i) == cmd.GetFrom()) {
                        if (found) {
                            FinishWithError(TResult::ERROR, TStringBuilder() << name << " ambiguous From node");
                            return false;
                        } else {
                            found = true;
                            ring->MutableNode()->Set(i, cmd.GetTo());
                        }
                    }
                    return true;
                };

                for (size_t i = 0; i < numItems; ++i) {
                    if (ring->RingSize()) {
                        const auto& r = ring->GetRing(i);
                        if (r.RingSize()) {
                            FinishWithError(TResult::ERROR, TStringBuilder() << name << " incorrect configuration:"
                                " Ring is way too nested");
                            return false;
                        }
                        const size_t numNodes = r.NodeSize();
                        for (size_t k = 0; k < numNodes; ++k) {
                            if (r.GetNode(k) == cmd.GetFrom() && !replace(ring->MutableRing(i), k)) {
                                return false;
                            }
                        }
                    } else {
                        if (ring->GetNode(i) == cmd.GetFrom() && !replace(ring, i)) {
                            return false;
                        }
                    }
                }
                if (!found) {
                    FinishWithError(TResult::ERROR, TStringBuilder() << name << " From node not found");
                    return false;
                }

                return true;
            };

#define F(NAME) \
            if (cmd.Get##NAME() && !process(#NAME, &NKikimrBlobStorage::TStorageConfig::Has##NAME##Config, \
                    &NKikimrBlobStorage::TStorageConfig::Mutable##NAME##Config)) { \
                return; \
            }
            F(StateStorage)
            F(StateStorageBoard)
            F(SchemeBoard)

            config.SetGeneration(config.GetGeneration() + 1);
            StartProposition(&config);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Configuration proposition

        void AdvanceGeneration() {
            if (RunCommonChecks()) {
                NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
                config.SetGeneration(config.GetGeneration() + 1);
                StartProposition(&config);
            }
        }

        void StartProposition(NKikimrBlobStorage::TStorageConfig *config) {
            config->MutablePrevConfig()->CopyFrom(*Self->StorageConfig);
            config->MutablePrevConfig()->ClearPrevConfig();
            UpdateFingerprint(config);

            if (auto error = ValidateConfigUpdate(*Self->StorageConfig, *config)) {
                STLOG(PRI_DEBUG, BS_NODE, NW51, "proposed config validation failed", (SelfId, SelfId()), (Error, *error),
                    (Config, config));
                return FinishWithError(TResult::ERROR, TStringBuilder() << "config validation failed: " << *error);
            }

            Self->CurrentProposedStorageConfig.emplace();
            Self->CurrentProposedStorageConfig->Swap(config);

            TEvScatter task;
            auto *propose = task.MutableProposeStorageConfig();
            propose->MutableConfig()->CopyFrom(*Self->CurrentProposedStorageConfig);

            return Self->IssueScatterTask(SelfId(), std::move(task));
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Query termination and result delivery

        bool RunCommonChecks() {
            if (!Self->StorageConfig) {
                FinishWithError(TResult::ERROR, "no agreed StorageConfig");
            } else if (Self->CurrentProposedStorageConfig) {
                FinishWithError(TResult::ERROR, "config proposition request in flight");
            } else if (Self->RootState != ERootState::RELAX) {
                FinishWithError(TResult::ERROR, "something going on with default FSM");
            } else if (auto error = ValidateConfig(*Self->StorageConfig)) {
                FinishWithError(TResult::ERROR, TStringBuilder() << "current config validation failed: " << *error);
            } else {
                return true;
            }
            return false;
        }

        std::unique_ptr<TEvNodeConfigInvokeOnRootResult> PrepareResult(TResult::EStatus status,
                std::optional<std::reference_wrapper<const TString>> errorReason) {
            auto ev = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
            auto *record = &ev->Record;
            record->SetStatus(status);
            if (errorReason) {
                record->SetErrorReason(*errorReason);
            }
            if (auto scepter = Scepter.lock()) {
                auto *s = record->MutableScepter();
                s->SetId(scepter->Id);
                s->SetNodeId(SelfId().NodeId());
            }
            return ev;
        }

        void FinishWithError(TResult::EStatus status, const TString& errorReason) {
            Finish(Sender, SelfId(), PrepareResult(status, errorReason).release(), 0, Cookie);
        }

        template<typename... TArgs>
        void Finish(TArgs&&... args) {
            auto handle = std::make_unique<IEventHandle>(std::forward<TArgs>(args)...);
            if (RequestSessionId) { // deliver response through interconnection session the request arrived from
                handle->Rewrite(TEvInterconnect::EvForward, RequestSessionId);
            }
            TActivationContext::Send(handle.release());
            PassAway();
        }

        void PassAway() override {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Gone, 0, ParentId, SelfId(), nullptr, 0));
            UnsubscribeInterconnect();
            TActorBootstrapped::PassAway();
        }

        STFUNC(StateFunc) {
            if (LifetimeToken.expired()) {
                return FinishWithError(TResult::ERROR, "distributed config keeper terminated");
            }
            STRICT_STFUNC_BODY(
                hFunc(TEvNodeConfigInvokeOnRootResult, Handle);
                hFunc(TEvNodeConfigGather, Handle);
                hFunc(TEvInterconnect::TEvNodeConnected, Handle);
                hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
                hFunc(TEvBlobStorage::TEvVStatusResult, Handle);
                hFunc(TEvents::TEvUndelivered, Handle);
                hFunc(TEvNodeWardenBaseConfig, Handle);
                cFunc(TEvents::TSystem::Poison, PassAway);
            )
        }
    };

    void TDistributedConfigKeeper::Handle(TEvNodeConfigInvokeOnRoot::TPtr ev) {
        std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>> evPtr(ev.Release());
        ChildActors.insert(RegisterWithSameMailbox(new TInvokeRequestHandlerActor(this, std::move(evPtr))));
    }

} // NKikimr::NStorage
