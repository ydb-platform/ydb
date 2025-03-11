#include "distconf.h"
#include "node_warden_impl.h"

#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/library/yaml_json/yaml_to_json.h>

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

        using TGatherCallback = std::function<std::optional<TString>(TEvGather*)>;
        ui64 NextScatterCookie = 1;
        THashMap<ui64, TGatherCallback> ScatterTasks;

        std::shared_ptr<TLifetimeToken> RequestHandlerToken = std::make_shared<TLifetimeToken>();

        NKikimrBlobStorage::TStorageConfig ProposedStorageConfig;

        std::optional<TString> NewYaml;
        std::optional<TString> VersionError;

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
                case TQuery::kUpdateConfig:
                    return UpdateConfig(record.MutableUpdateConfig());

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

                case TQuery::kFetchStorageConfig: {
                    const auto& request = record.GetFetchStorageConfig();
                    return FetchStorageConfig(request.GetManual(), request.GetMainConfig(), request.GetStorageConfig());
                }

                case TQuery::kReplaceStorageConfig:
                    return ReplaceStorageConfig(record.GetReplaceStorageConfig());

                case TQuery::kBootstrapCluster:
                    return BootstrapCluster(record.GetBootstrapCluster().GetSelfAssemblyUUID());

                case TQuery::REQUEST_NOT_SET:
                    return FinishWithError(TResult::ERROR, "Request field not set");
            }

            FinishWithError(TResult::ERROR, "unhandled request");
        }

        void IssueScatterTask(TEvScatter&& task, TGatherCallback callback) {
            const ui64 cookie = NextScatterCookie++;
            const auto [it, inserted] = ScatterTasks.try_emplace(cookie, std::move(callback));
            Y_ABORT_UNLESS(inserted);

            task.SetTaskId(RandomNumber<ui64>());
            task.SetCookie(cookie);
            Self->IssueScatterTask(SelfId(), std::move(task));
        }

        void Handle(TEvNodeConfigGather::TPtr ev) {
            auto& record = ev->Get()->Record;
            STLOG(PRI_DEBUG, BS_NODE, NWDC44, "Handle(TEvNodeConfigGather)", (SelfId, SelfId()), (Record, record));
            if (record.GetAborted()) {
                return FinishWithError(TResult::ERROR, "scatter task was aborted due to loss of quorum or other error");
            }

            const auto it = ScatterTasks.find(record.GetCookie());
            Y_ABORT_UNLESS(it != ScatterTasks.end());
            TGatherCallback callback = std::move(it->second);
            ScatterTasks.erase(it);

            if (auto error = callback(&record)) {
                FinishWithError(TResult::ERROR, std::move(*error));
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Configuration update

        void UpdateConfig(TQuery::TUpdateConfig *request) {
            if (!RunCommonChecks()) {
                return;
            }

            auto *config = request->MutableConfig();

            if (auto error = ValidateConfig(*Self->StorageConfig)) {
                return FinishWithError(TResult::ERROR, TStringBuilder() << "UpdateConfig current config validation failed: " << *error);
            } else if (auto error = ValidateConfigUpdate(*Self->StorageConfig, *config)) {
                return FinishWithError(TResult::ERROR, TStringBuilder() << "UpdateConfig config validation failed: " << *error);
            }

            StartProposition(config);
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
                STLOG(PRI_DEBUG, BS_NODE, NWDC73, "sending TEvVStatus", (SelfId, SelfId()), (VDiskId, vdiskId),
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
            STLOG(PRI_DEBUG, BS_NODE, NWDC74, "TEvVStatusResult", (SelfId, SelfId()), (Record, record), (VDiskId, vdiskId));
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

            if (!RunCommonChecks()) {
                return;
            } else if (!Self->SelfManagementEnabled) {
                return FinishWithError(TResult::ERROR, "self-management is not enabled");
            }

            STLOG(PRI_DEBUG, BS_NODE, NWDC75, "ReassignGroupDiskExecute", (SelfId, SelfId()));

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

            const auto& smConfig = config.GetSelfManagementConfig();

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
                            smConfig.GetGeometry(), smConfig.GetPDiskFilter(),
                            smConfig.HasPDiskType() ? std::make_optional(smConfig.GetPDiskType()) : std::nullopt,
                            replacedDisks, forbid, maxSlotSize,
                            &BaseConfig.value(), cmd.GetConvertToDonor(), cmd.GetIgnoreVSlotQuotaCheck(),
                            cmd.GetIsSelfHealReasonDecommit());
                    } catch (const TExConfigError& ex) {
                        STLOG(PRI_NOTICE, BS_NODE, NWDC76, "ReassignGroupDisk failed to allocate group", (SelfId, SelfId()),
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
        // Storage configuration YAML manipulation

        void FetchStorageConfig(bool manual, bool fetchMain, bool fetchStorage) {
            if (!Self->StorageConfig) {
                FinishWithError(TResult::ERROR, "no agreed StorageConfig");
            } else if (!Self->MainConfigFetchYaml) {
                FinishWithError(TResult::ERROR, "no stored YAML for storage config");
            } else {
                auto ev = PrepareResult(TResult::OK, std::nullopt);
                auto *record = &ev->Record;
                auto *res = record->MutableFetchStorageConfig();
                if (fetchMain) {
                    res->SetYAML(Self->MainConfigFetchYaml);
                }
                if (fetchStorage && Self->StorageConfigYaml) {
                    auto metadata = NYamlConfig::GetStorageMetadata(*Self->StorageConfigYaml);
                    metadata.Cluster = metadata.Cluster.value_or("unknown"); // TODO: fix this
                    metadata.Version = metadata.Version.value_or(0) + 1;
                    res->SetStorageYAML(NYamlConfig::ReplaceMetadata(*Self->StorageConfigYaml, metadata));
                }

                if (manual) {
                    // add BlobStorageConfig, NameserviceConfig, DomainsConfig into main/storage config
                }

                Finish(Sender, SelfId(), ev.release(), 0, Cookie);
            }
        }

        void ReplaceStorageConfig(const TQuery::TReplaceStorageConfig& request) {
            if (!RunCommonChecks()) {
                return;
            } else if (!Self->ConfigCommittedToConsole && Self->SelfManagementEnabled) {
                return FinishWithError(TResult::ERROR, "previous config has not been committed to Console yet");
            }

            NewYaml = request.HasYAML() ? std::make_optional(request.GetYAML()) : std::nullopt;

            auto newStorageYaml = request.HasStorageYAML() ? std::make_optional(request.GetStorageYAML()) : std::nullopt;

            auto switchDedicatedStorageSection = request.HasSwitchDedicatedStorageSection()
                ? std::make_optional(request.GetSwitchDedicatedStorageSection())
                : std::nullopt;

            const bool targetDedicatedStorageSection = switchDedicatedStorageSection.value_or(Self->StorageConfigYaml.has_value());

            if (switchDedicatedStorageSection) {
                // check that configs are explicitly defined when we are switching dual-config mode
                if (!NewYaml) {
                    return FinishWithError(TResult::ERROR, "main config must be specified when switching dedicated"
                        " storage section mode");
                } else if (*switchDedicatedStorageSection && !newStorageYaml) {
                    return FinishWithError(TResult::ERROR, "storage config must be specified when turning on dedicated"
                        " storage section mode");
                }
            }

            if (request.GetDedicatedStorageSectionConfigMode() != targetDedicatedStorageSection) {
                return FinishWithError(TResult::ERROR, "DedicatedStorageSectionConfigMode does not match target state");
            } else if (newStorageYaml && !targetDedicatedStorageSection) {
                // we are going to end up in single-config mode, but explicit storage yaml is provided
                return FinishWithError(TResult::ERROR, "unexpected dedicated storage config section in request");
            } else if (switchDedicatedStorageSection && *switchDedicatedStorageSection == Self->StorageConfigYaml.has_value()) {
                // this enable/disable command does not change the state
                return FinishWithError(TResult::ERROR, "dedicated storage config section is already in requested state");
            }

            TString state;
            NKikimrBlobStorage::TStorageConfig config(*Self->StorageConfig);
            std::optional<ui64> newExpectedStorageYamlVersion;

            if (config.HasExpectedStorageYamlVersion()) {
                newExpectedStorageYamlVersion.emplace(config.GetExpectedStorageYamlVersion());
            }

            std::optional<ui64> storageYamlVersion;
            std::optional<ui64> mainYamlVersion;

            try {
                auto load = [&](const TString& yaml, ui64& version, const char *expectedKind) {
                    state = TStringBuilder() << "loading " << expectedKind << " YAML";
                    NJson::TJsonValue json = NYaml::Yaml2Json(YAML::Load(yaml), true);

                    state = TStringBuilder() << "extracting " << expectedKind << " metadata";
                    if (!json.Has("metadata") || !json["metadata"].IsMap()) {
                        throw yexception() << "no metadata section";
                    }
                    auto& metadata = json["metadata"];
                    NYaml::ValidateMetadata(metadata);
                    if (!metadata.Has("kind") || metadata["kind"] != expectedKind) {
                        throw yexception() << "missing or invalid kind provided";
                    }
                    version = metadata["version"].GetUIntegerRobust();

                    state = TStringBuilder() << "validating " << expectedKind << " config section";
                    if (!json.Has("config") || !json["config"].IsMap()) {
                        throw yexception() << "missing config section";
                    }

                    return json;
                };

                NJson::TJsonValue main;
                NJson::TJsonValue storage;
                const NJson::TJsonValue *effective = nullptr;

                if (newStorageYaml) {
                    storage = load(*newStorageYaml, storageYamlVersion.emplace(), "StorageConfig");
                    newExpectedStorageYamlVersion = *storageYamlVersion + 1;
                    effective = &storage;
                }

                if (NewYaml) {
                    main = load(*NewYaml, mainYamlVersion.emplace(), "MainConfig");
                    if (!effective && !Self->StorageConfigYaml) {
                        effective = &main;
                    }
                }

                if (effective) {
                    state = "parsing final config";

                    NKikimrConfig::TAppConfig appConfig;
                    NYaml::Parse(*effective, NYaml::GetJsonToProtoConfig(), appConfig, true);

                    if (TString errorReason; !DeriveStorageConfig(appConfig, &config, &errorReason)) {
                        return FinishWithError(TResult::ERROR, TStringBuilder()
                            << "error while deriving StorageConfig: " << errorReason);
                    }
                }
            } catch (const std::exception& ex) {
                 return FinishWithError(TResult::ERROR, TStringBuilder() << "exception while " << state
                    << ": " << ex.what());
            }

            if (storageYamlVersion) {
                const ui64 expected = Self->StorageConfig->GetExpectedStorageYamlVersion();
                if (*storageYamlVersion != expected) {
                    VersionError = TStringBuilder()
                        << "storage config version must be increasing by one"
                        << " new version# " << *storageYamlVersion
                        << " expected version# " << expected;
                }
            }

            if (!VersionError && mainYamlVersion && Self->MainConfigYamlVersion) {
                // TODO(alexvru): we have to check version when migrating to self-managed mode
                const ui64 expected = Self->MainConfigYamlVersion
                    ? *Self->MainConfigYamlVersion + 1
                    : 0;
                if (*mainYamlVersion != expected) {
                    VersionError = TStringBuilder()
                        << "main config version must be increasing by one"
                        << " new version# " << *mainYamlVersion
                        << " expected version# " << expected;
                }
            }

            if (NewYaml) {
                if (const auto& error = UpdateConfigComposite(config, *NewYaml, std::nullopt)) {
                    return FinishWithError(TResult::ERROR, TStringBuilder() << "failed to update config yaml: " << *error);
                }
            }

            if (newExpectedStorageYamlVersion) {
                config.SetExpectedStorageYamlVersion(*newExpectedStorageYamlVersion);
            }

            if (newStorageYaml) {
                // make new compressed storage yaml section
                TString s;
                if (TStringOutput output(s); true) {
                    TZstdCompress zstd(&output);
                    zstd << *newStorageYaml;
                }
                config.SetCompressedStorageYaml(s);
            } else if (!targetDedicatedStorageSection) {
                config.ClearCompressedStorageYaml();
            }

            // advance the config generation
            config.SetGeneration(config.GetGeneration() + 1);

            if (auto error = ValidateConfig(*Self->StorageConfig)) {
                return FinishWithError(TResult::ERROR, TStringBuilder()
                    << "ReplaceStorageConfig current config validation failed: " << *error);
            } else if (auto error = ValidateConfigUpdate(*Self->StorageConfig, config)) {
                return FinishWithError(TResult::ERROR, TStringBuilder()
                    << "ReplaceStorageConfig config validation failed: " << *error);
            }

            // whether we are enabling distconf right now (by this operation)
            ProposedStorageConfig = std::move(config);

            if (!Self->SelfManagementEnabled && ProposedStorageConfig.GetSelfManagementConfig().GetEnabled()) {
                TryEnableDistconf();
            } else {
                IssueQueryToConsole(false);
            }
        }

        void IssueQueryToConsole(bool enablingDistconf) {
            if (VersionError) {
                FinishWithError(TResult::ERROR, *VersionError);
            } else if (Event->Get()->Record.GetReplaceStorageConfig().GetSkipConsoleValidation() || !NewYaml) {
                StartProposition(&ProposedStorageConfig);
            } else if (!Self->EnqueueConsoleConfigValidation(SelfId(), enablingDistconf, *NewYaml)) {
                FinishWithError(TResult::ERROR, "console pipe is not available");
            }
        }

        void TryEnableDistconf() {
            const ERootState prevState = std::exchange(Self->RootState, ERootState::IN_PROGRESS);
            Y_ABORT_UNLESS(prevState == ERootState::RELAX);

            TEvScatter task;
            task.MutableCollectConfigs();
            IssueScatterTask(std::move(task), [this](TEvGather *res) -> std::optional<TString> {
                Y_ABORT_UNLESS(Self->StorageConfig); // it can't just disappear

                const ERootState prevState = std::exchange(Self->RootState, ERootState::RELAX);
                Y_ABORT_UNLESS(prevState == ERootState::IN_PROGRESS);

                if (!res->HasCollectConfigs()) {
                    return "incorrect CollectConfigs response";
                } else if (Self->CurrentProposedStorageConfig) {
                    FinishWithError(TResult::RACE, "config proposition request in flight");
                } else if (Scepter.expired()) {
                    return "scepter lost during query execution";
                } else {
                    auto r = Self->ProcessCollectConfigs(res->MutableCollectConfigs(), std::nullopt);
                    return std::visit<std::optional<TString>>(TOverloaded{
                        [&](std::monostate&) -> std::optional<TString> {
                            if (r.IsDistconfDisabledQuorum) {
                                // distconf is disabled on the majority of nodes; we have just to replace configs
                                // and then to restart these nodes in order to enable it in future
                                auto ev = PrepareResult(TResult::CONTINUE_BSC, "proceed with BSC");
                                ev->Record.MutableReplaceStorageConfig()->SetAllowEnablingDistconf(true);
                                Finish(Sender, SelfId(), ev.release(), 0, Cookie);
                            } else {
                                // we can actually enable distconf with this query, so do it
                                IssueQueryToConsole(true);
                            }
                            return std::nullopt;
                        },
                        [&](TString& error) {
                            return std::move(error);
                        },
                        [&](NKikimrBlobStorage::TStorageConfig& /*proposedConfig*/) {
                            return "unexpected config proposition";
                        }
                    }, r.Outcome);
                }

                return std::nullopt; // no error or it is already processed
            });
        }

        void Handle(TEvBlobStorage::TEvControllerValidateConfigResponse::TPtr ev) {
            const auto& record = ev->Get()->Record;
            STLOG(PRI_DEBUG, BS_NODE, NWDC77, "received TEvControllerValidateConfigResponse", (SelfId, SelfId()),
                (InternalError, ev->Get()->InternalError), (Status, record.GetStatus()));

            if (ev->Get()->InternalError) {
                return FinishWithError(TResult::ERROR, TStringBuilder() << "failed to validate config through console: "
                    << *ev->Get()->InternalError);
            }

            switch (record.GetStatus()) {
                case NKikimrBlobStorage::TEvControllerValidateConfigResponse::IdPipeServerMismatch:
                    Self->DisconnectFromConsole();
                    Self->ConnectToConsole();
                    return FinishWithError(TResult::ERROR, TStringBuilder() << "console connection race detected: " << record.GetErrorReason());

                case NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigNotValid:
                    return FinishWithError(TResult::ERROR, TStringBuilder() << "console config validation failed: "
                        << record.GetErrorReason());

                case NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigIsValid:
                    if (const auto& error = UpdateConfigComposite(ProposedStorageConfig, *NewYaml, record.GetYAML())) {
                        return FinishWithError(TResult::ERROR, TStringBuilder() << "failed to update config yaml: " << *error);
                    }
                    return StartProposition(&ProposedStorageConfig);
            }
        }

        void BootstrapCluster(const TString& selfAssemblyUUID) {
            if (!RunCommonChecks()) {
                return;
            } else if (Self->StorageConfig->GetGeneration()) {
                if (Self->StorageConfig->GetSelfAssemblyUUID() == selfAssemblyUUID) { // repeated command, it's ok
                    return Finish(Sender, SelfId(), PrepareResult(TResult::OK, std::nullopt).release(), 0, Cookie);
                } else {
                    return FinishWithError(TResult::ERROR, "bootstrap on already bootstrapped cluster");
                }
            } else if (!selfAssemblyUUID) {
                return FinishWithError(TResult::ERROR, "SelfAssemblyUUID can't be empty");
            }

            const ERootState prevState = std::exchange(Self->RootState, ERootState::IN_PROGRESS);
            Y_ABORT_UNLESS(prevState == ERootState::RELAX);

            // issue scatter task to collect configs and then bootstrap cluster with specified cluster UUID
            auto done = [this, selfAssemblyUUID = TString(selfAssemblyUUID)](TEvGather *res) -> std::optional<TString> {
                Y_ABORT_UNLESS(res->HasCollectConfigs());
                Y_ABORT_UNLESS(Self->StorageConfig); // it can't just disappear
                if (Self->CurrentProposedStorageConfig) {
                    FinishWithError(TResult::RACE, "config proposition request in flight");
                    return std::nullopt;
                } else if (Self->StorageConfig->GetGeneration()) {
                    FinishWithError(TResult::RACE, "storage config generation regenerated while collecting configs");
                    return std::nullopt;
                }
                auto r = Self->ProcessCollectConfigs(res->MutableCollectConfigs(), selfAssemblyUUID);
                return std::visit<std::optional<TString>>(TOverloaded{
                    [&](std::monostate&) {
                        const ERootState prevState = std::exchange(Self->RootState, ERootState::RELAX);
                        Y_ABORT_UNLESS(prevState == ERootState::IN_PROGRESS);
                        Finish(Sender, SelfId(), PrepareResult(TResult::OK, std::nullopt).release(), 0, Cookie);
                        return std::nullopt;
                    },
                    [&](TString& error) {
                        const ERootState prevState = std::exchange(Self->RootState, ERootState::RELAX);
                        Y_ABORT_UNLESS(prevState == ERootState::IN_PROGRESS);
                        return error;
                    },
                    [&](NKikimrBlobStorage::TStorageConfig& proposedConfig) {
                        StartProposition(&proposedConfig, false);
                        return std::nullopt;
                    }
                }, r.Outcome);
            };

            TEvScatter task;
            task.MutableCollectConfigs();
            IssueScatterTask(std::move(task), std::move(done));
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

        void StartProposition(NKikimrBlobStorage::TStorageConfig *config, bool updateFields = true) {
            if (updateFields) {
                config->MutablePrevConfig()->CopyFrom(*Self->StorageConfig);
                config->MutablePrevConfig()->ClearPrevConfig();
                UpdateFingerprint(config);
            }

            if (auto error = ValidateConfigUpdate(*Self->StorageConfig, *config)) {
                STLOG(PRI_DEBUG, BS_NODE, NWDC78, "StartProposition config validation failed", (SelfId, SelfId()),
                    (Error, *error), (Config, config));
                return FinishWithError(TResult::ERROR, TStringBuilder()
                    << "StartProposition config validation failed: " << *error);
            }

            Self->CurrentProposedStorageConfig.emplace(std::move(*config));

            auto done = [&](TEvGather *res) -> std::optional<TString> {
                Y_ABORT_UNLESS(res->HasProposeStorageConfig());
                std::unique_ptr<TEvNodeConfigInvokeOnRootResult> ev;

                const ERootState prevState = std::exchange(Self->RootState, ERootState::RELAX);
                Y_ABORT_UNLESS(prevState == ERootState::IN_PROGRESS);

                if (auto error = Self->ProcessProposeStorageConfig(res->MutableProposeStorageConfig())) {
                    return error;
                }
                Finish(Sender, SelfId(), PrepareResult(TResult::OK, std::nullopt).release(), 0, Cookie);
                return std::nullopt;
            };

            TEvScatter task;
            auto *propose = task.MutableProposeStorageConfig();
            propose->MutableConfig()->CopyFrom(*Self->CurrentProposedStorageConfig);
            IssueScatterTask(std::move(task), done);

            Self->RootState = ERootState::IN_PROGRESS; // forbid any concurrent activity
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
            } else if (Scepter.expired()) {
                FinishWithError(TResult::ERROR, "scepter lost during query execution");
            } else {
                return true;
            }
            return false;
        }

        std::unique_ptr<TEvNodeConfigInvokeOnRootResult> PrepareResult(TResult::EStatus status,
                std::optional<TStringBuf> errorReason) {
            auto ev = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
            auto *record = &ev->Record;
            record->SetStatus(status);
            if (errorReason) {
                record->SetErrorReason(errorReason->data(), errorReason->size());
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
                hFunc(TEvBlobStorage::TEvControllerValidateConfigResponse, Handle);
            )
        }
    };

    void TDistributedConfigKeeper::Handle(TEvNodeConfigInvokeOnRoot::TPtr ev) {
        std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>> evPtr(ev.Release());
        ChildActors.insert(RegisterWithSameMailbox(new TInvokeRequestHandlerActor(this, std::move(evPtr))));
    }

} // NKikimr::NStorage
