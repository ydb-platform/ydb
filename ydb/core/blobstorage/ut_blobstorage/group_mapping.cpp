#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>
#include <ydb/core/mind/bscontroller/layout_helpers.h>

Y_UNIT_TEST_SUITE(GroupReconfiguration) {

    void SetupEnvForPropagationTest(std::unique_ptr<TEnvironmentSetup>& env, ui32 numDCs, ui32 numNodesInDC,
            ui32& groupId, ui32& fromNodeId, ui32& toNodeId, std::set<ui32>& nodesInGroup,
            std::vector<TInflightActor*>& actors, bool loadNodesWVDisks,
            NKikimrBlobStorage::TConfigRequest& reassignRequest) {
        const ui32 numNodes = numDCs * numNodesInDC + 1; // one node is reserved for bsc tablet
        TBlobStorageGroupType groupType = TBlobStorageGroupType::ErasureMirror3dc;

        env = std::make_unique<TEnvironmentSetup>(TEnvironmentSetup::TSettings{
            .NodeCount = numNodes,
            .Erasure = groupType,
            .ControllerNodeId = numNodes,
            .LocationGenerator = [&](ui32 nodeId) {
                NActorsInterconnect::TNodeLocation proto;
                proto.SetDataCenter(ToString((nodeId - 1) / numNodesInDC));
                proto.SetRack(ToString((nodeId - 1) % numNodesInDC));
                proto.SetUnit("1");
                return TNodeLocation(proto);
            },
        });

        const ui32 disksPerNode = 1;
        env->CreateBoxAndPool(disksPerNode, 1, numNodes - 1);
        env->Sim(TDuration::Seconds(30));

        // Get group info from BSC
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableQueryBaseConfig();
        auto response = env->Invoke(request);

        const auto& base = response.GetStatus(0).GetBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);
        const auto& group = base.GetGroup(0);

        groupId = group.GetGroupId();
        for (const auto& vslotId : group.GetVSlotId()) {
            nodesInGroup.insert(vslotId.GetNodeId());
        }

        // Make reassign request
        reassignRequest.Clear();
        auto* reassign = reassignRequest.AddCommand()->MutableReassignGroupDisk();
        for (const auto& slot : base.GetVSlot()) {
            fromNodeId = slot.GetVSlotId().GetNodeId();
            if (nodesInGroup.count(fromNodeId) != 0) {
                reassign->SetGroupId(slot.GetGroupId());
                reassign->SetGroupGeneration(slot.GetGroupGeneration());
                reassign->SetFailRealmIdx(slot.GetFailRealmIdx());
                reassign->SetFailDomainIdx(slot.GetFailDomainIdx());
                reassign->SetVDiskIdx(slot.GetVDiskIdx());
                break;
            }
        }

        for (const auto& pdisk : base.GetPDisk()) {
            toNodeId = pdisk.GetNodeId();
            if (nodesInGroup.count(toNodeId) == 0) {
                auto* target = reassign->MutableTargetPDiskId();
                target->SetNodeId(pdisk.GetNodeId());
                target->SetPDiskId(pdisk.GetPDiskId());
                break;
            }
        }

        // Start load actors
        for (ui32 nodeId = 1; nodeId < numNodes - 1; ++nodeId) {
            if (loadNodesWVDisks || (nodesInGroup.count(nodeId) > 0 && nodeId != toNodeId && nodeId != fromNodeId)) {
                TInflightActor* newActor = new TInflightActorPut({100, 1, TDuration::MilliSeconds(100), TGroupId::FromValue(groupId)}, 1_KB);
                actors.push_back(newActor);
                env->Runtime->Register(newActor, nodeId);
            }
        }
    }

    void VerifyCounters(std::vector<TInflightActor*>& actors, ui32 acceptableLoss = 5) {
        for (ui32 i = 0; i < actors.size(); ++i) {
            auto* actor = actors[i];
            ui32 oks = actor->ResponsesByStatus[NKikimrProto::OK];
            UNIT_ASSERT_GE_C(actor->RequestsSent + acceptableLoss, oks,
                    "RequestsSent# " << actor->RequestsSent << " recieved OKs# " << oks << " nodeId# " << i + 1);
        }
    }

    void VerifyConfigsAreSame(TEnvironmentSetup& env, std::set<ui32>& nodesToCheck, ui32 groupId) {
        ui32 nodeWithInfo;
        std::optional<NKikimrBlobStorage::TEvNodeWardenGroupInfo> localGroupInfo;

        for (ui32 nodeId : nodesToCheck) {
            auto edge = env.Runtime->AllocateEdgeActor(nodeId);

            env.Runtime->WrapInActorContext(edge, [&] {
                env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), edge,
                        new TEvNodeWardenQueryGroupInfo(groupId), nodeId));
            });
            auto res = env.WaitForEdgeActorEvent<TEvNodeWardenGroupInfo>(edge, true, TInstant::Max());
            if (!localGroupInfo) {
                localGroupInfo = NKikimrBlobStorage::TEvNodeWardenGroupInfo();
                localGroupInfo->CopyFrom(res->Get()->Record);
                nodeWithInfo = nodeId;
            } else {
                const auto& group1 = localGroupInfo->GetGroup();
                const auto& group2 = res->Get()->Record.GetGroup();
                if (!google::protobuf::util::MessageDifferencer::Equals(group1, group2)) {
                    TString ProtoStored;
                    TString ProtoReceived;
                    google::protobuf::TextFormat::PrintToString(group1, &ProtoStored);
                    google::protobuf::TextFormat::PrintToString(group2, &ProtoReceived);

                    UNIT_FAIL("Node1 Id# " << nodeWithInfo << " Node1 GroupInfo# {" << ProtoStored << "} "
                            << "Node2 Id# " << nodeId << " Node 2 GroupInfo# {" << ProtoReceived << "}");
                }
            }
        }
    }

    void TestPropagateNewConfigurationViaVDisks(bool requestsToNodesWVDisks) {
        const ui32 numDCs = 3;
        const ui32 numNodesInDC = 4;
        const ui32 numNodes = numDCs * numNodesInDC + 1;
        const ui32 bscNodeId = numNodes;

        ui32 groupId;
        std::set<ui32> nodesInGroup;
        ui32 fromNodeId;
        ui32 toNodeId;

        std::vector<TInflightActor*> actors;

        NKikimrBlobStorage::TConfigRequest request;
        NKikimrBlobStorage::TConfigResponse response;

        std::unique_ptr<TEnvironmentSetup> env;
        SetupEnvForPropagationTest(env, numDCs, numNodesInDC, groupId, fromNodeId, toNodeId, nodesInGroup, actors, requestsToNodesWVDisks, request);
        env->Sim(TDuration::Seconds(1));

        Cerr << "Reassign disk fromNodeId# " << fromNodeId << " toNodeId# " << toNodeId << Endl;
        response = env->Invoke(request);

        bool bscShutDown = false;
        env->Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->Sender.NodeId() == bscNodeId) {
                if (nodesInGroup.count(nodeId) > 0 && nodeId != fromNodeId && nodeId != toNodeId && nodeId != bscNodeId) {
                    if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerNodeServiceSetUpdate::EventType
                            && !std::exchange(bscShutDown, true)) {
                        Cerr << "Send configuration to nodeId# " << nodeId << Endl;
                        return true;
                    }
                    return !bscShutDown;
                }
                return false;
            }
            return true;
        };

        env->Sim(TDuration::Seconds(3));

        UNIT_ASSERT(bscShutDown);
        env->Sim(TDuration::Seconds(2));
        VerifyCounters(actors, 5);

        nodesInGroup.erase(fromNodeId);
        nodesInGroup.insert(toNodeId);
        VerifyConfigsAreSame(*env, nodesInGroup, groupId);
    }

    // FIXME serg-belyakov
    // Y_UNIT_TEST(PropagateNewConfigurationViaVDisks) {
    //     TestPropagateNewConfigurationViaVDisks(true);
    // }

    // TODO: KIKIMR-11627
    // Y_UNIT_TEST(PropagateNewConfigurationViaVDisksNoRequestsToNodesWVDisks) {
    //     TestPropagateNewConfigurationViaVDisks(false);
    // }

    void TestBsControllerDoesNotDisableGroup(bool requestsToNodesWVDisks) {
        const ui32 numDCs = 3;
        const ui32 numNodesInDC = 4;
        const ui32 numNodes = numDCs * numNodesInDC;
        const ui32 bscNodeId = numNodes + 1;

        ui32 groupId;
        std::set<ui32> nodesInGroup;
        ui32 fromNodeId;
        ui32 toNodeId;

        std::vector<TInflightActor*> actors;

        NKikimrBlobStorage::TConfigRequest request;
        NKikimrBlobStorage::TConfigResponse response;

        std::unique_ptr<TEnvironmentSetup> env;
        SetupEnvForPropagationTest(env, numDCs, numNodesInDC, groupId, fromNodeId, toNodeId, nodesInGroup, actors, requestsToNodesWVDisks, request);
        env->Sim(TDuration::Seconds(1));

        response = env->Invoke(request);

        std::array<bool, numNodes> passedMessages{true};

        env->Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->Sender.NodeId() == bscNodeId && nodeId != bscNodeId) {
                if (nodeId != fromNodeId && nodeId != toNodeId && std::exchange(passedMessages[nodeId - 1], false)) {
                    Cerr << "Send configuration to nodeId# " << nodeId << Endl;
                    return true;
                }
                return false;
            }
            return true;
        };

        Cerr << "Reassign disk fromNodeId# " << fromNodeId << " toNodeId# " << toNodeId << Endl;
        env->Sim(TDuration::Seconds(3));

       // UNIT_ASSERT(!passOne);
        env->Sim(TDuration::Seconds(2));
        VerifyCounters(actors, 5);
    }

    Y_UNIT_TEST(BsControllerDoesNotDisableGroup) {
        TestBsControllerDoesNotDisableGroup(true);
    }

    Y_UNIT_TEST(BsControllerDoesNotDisableGroupNoRequestsToNodesWVDisks) {
        TestBsControllerDoesNotDisableGroup(false);
    }

    void SetupEnvForReassignTest(ui32 numDCs, ui32 numNodesInDC, ui32 disksPerNode, ui32& numNodes, ui32 numGroups,
            ui32& groupId, TBlobStorageGroupType erasure, std::unique_ptr<TEnvironmentSetup>& env) {
        numNodes = numDCs * numNodesInDC + 1;
        env.reset(new TEnvironmentSetup({
            .NodeCount = numNodes,
            .Erasure = erasure,
            .ControllerNodeId = numNodes,
            .LocationGenerator = [&](ui32 nodeId) {
                NActorsInterconnect::TNodeLocation proto;
                proto.SetDataCenter(ToString((nodeId - 1) / numNodesInDC));
                proto.SetRack(ToString((nodeId - 1) % numNodesInDC));
                proto.SetUnit("1");
                return TNodeLocation(proto);
            },
        }));

        env->CreateBoxAndPool(disksPerNode, numGroups, numNodes);

        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableQueryBaseConfig();
        auto response = env->Invoke(request);

        const auto& base = response.GetStatus(0).GetBaseConfig();
        UNIT_ASSERT_GE(base.GroupSize(), 1);
        const auto& group = base.GetGroup(0);
        groupId = group.GetGroupId();

        env->Sim(TDuration::Seconds(30));
    }

    class TReassignActor : public TActorBootstrapped<TReassignActor> {
    public:
        TReassignActor(ui32 groupId, ui64 bsController, ui32 reassignRequests, TDuration reassignDelay)
            : GroupId(groupId)
            , BsController(bsController)
            , ReassignReqests(reassignRequests)
            , ReassignDelay(reassignDelay)
        {}

        ~TReassignActor() = default;

        void Bootstrap(const TActorContext&/* ctx*/) {
            ClientId = Register(NKikimr::NTabletPipe::CreateClient(SelfId(), BsController,
                    TTestActorSystem::GetPipeConfigWithRetries()));
            BootstrapImpl();
        }

    protected:
        ui32 GroupId;
        ui64 BsController;
        TActorId ClientId;

        ui32 ReassignReqests;
        TDuration ReassignDelay;

        bool Started = false;

    protected:
        virtual void BootstrapImpl() = 0;

        void SendFirstRequest() {

        }

        void SendRequest() {
            if (ReassignReqests > 0) {
                SendReassign();
                ReassignReqests -= 1;
                Schedule(ReassignDelay, new TEvents::TEvWakeup);
            } else {
                SendBscRequest();
            }
        }

        void HandleConnected() {
            if (!std::exchange(Started, true)) {
                SendRequest();
            }
        }

        virtual void SendReassign() = 0;

        virtual void SendBscRequest() {
        }
    };

    class TSelfHealActor : public TReassignActor {
    public:
        TSelfHealActor(ui32 groupId, ui64 bsController, TDuration& delay)
            : TReassignActor(groupId, bsController, 10'000, TDuration::MicroSeconds(1))
            , Delay(delay)
        {}

    private:
        void BootstrapImpl() override {
            Become(&TSelfHealActor::StateFunc);
        }
    private:
        TDuration& Delay;

    private:
        void SendReassign() override {
            NKikimrBlobStorage::TConfigRequest reassignRequest;
            auto* reassign = reassignRequest.AddCommand()->MutableReassignGroupDisk();
            reassign->SetGroupId(GroupId);
            reassign->SetFailRealmIdx(0);
            reassign->SetFailDomainIdx(0);
            reassign->SetVDiskIdx(0);

            auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
            ev->SelfHeal = true;
            ev->Record.MutableRequest()->CopyFrom(reassignRequest);
            NTabletPipe::SendData(SelfId(), ClientId, ev.release(), 0);
        }

        void SendBscRequest() override {
            Timer.Reset();
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerGetGroup>();
            ev->Record.AddGroupIDs(GroupId);
            NTabletPipe::SendData(SelfId(), ClientId, ev.release(), Max<ui64>());
        }

    private:
        void Handle(TEvBlobStorage::TEvControllerNodeServiceSetUpdate::TPtr/* ev*/) {
            Delay = TDuration::Seconds(Timer.Passed());
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvBlobStorage::TEvControllerNodeServiceSetUpdate, Handle);
            cFunc(TEvents::TSystem::Wakeup, SendRequest);
            cFunc(TEvTabletPipe::TEvClientConnected::EventType, HandleConnected);

            IgnoreFunc(TEvBlobStorage::TEvControllerConfigResponse);
        )

        THPTimer Timer;
    };

    Y_UNIT_TEST(BsControllerConfigurationRequestIsFastEnough) {
        const ui32 numDCs = 3;
        const ui32 numNodesInDC = 10;
        const ui32 disksPerNode = 1;
        ui32 numNodes;
        const ui32 numGroups = 1;
        ui32 groupId;
        std::unique_ptr<TEnvironmentSetup> env;
        SetupEnvForReassignTest(numDCs, numNodesInDC, disksPerNode, numNodes, numGroups, groupId,
                TBlobStorageGroupType::ErasureMirror3dc, env);

        std::vector<TDuration> delays(numNodes, TDuration::Max());
        const TDuration maxDelay = TDuration::Seconds(2);

        for (ui32 nodeId = 1; nodeId < numNodes; ++nodeId) {
            env->Runtime->Register(new TSelfHealActor(groupId, env->TabletId, delays[nodeId - 1]), nodeId);
        }

        env->Sim(TDuration::Seconds(30));

        for (ui32 i = 0; i < numNodes - 1; ++i) {
            UNIT_ASSERT_LE_C(delays[i], maxDelay, delays[i].ToString() << ' ' << maxDelay.ToString() << ' ' << i);
        }
    }

    class TSequentialReassigner : public TReassignActor {
    public:
        TSequentialReassigner(ui32 groupId, ui64 bsController, ui32 numNodes, TVDiskIdShort vdisk, bool target)
            : TReassignActor(groupId, bsController, 100, TDuration::MilliSeconds(1))
            , NumNodes(numNodes)
            , VDisk(vdisk)
            , TargetPDisk(target)
        {}

    private:
        void BootstrapImpl() override {
            Become(&TSequentialReassigner::StateFunc);
            SendReassign();
        }

    private:
        const ui32 NumNodes;
        ui32 TargetNode = 0;
        TVDiskIdShort VDisk;
        bool TargetPDisk = false;

    private:
        void SendReassign() override {
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
            auto* reassign = ev->Record.MutableRequest()->AddCommand()->MutableReassignGroupDisk();
            reassign->SetGroupId(GroupId);
            reassign->SetFailRealmIdx(VDisk.FailRealm);
            reassign->SetFailDomainIdx(VDisk.FailDomain);
            reassign->SetVDiskIdx(VDisk.VDisk);
            if (TargetPDisk) {
                auto* pdiskId = reassign->MutableTargetPDiskId();
                pdiskId->SetNodeId(TargetNode++ % NumNodes + 1);
                pdiskId->SetPDiskId(1);
            }

            NTabletPipe::SendData(SelfId(), ClientId, ev.release(), 0);
        }

    private:
        STRICT_STFUNC(StateFunc,
            cFunc(TEvents::TSystem::Wakeup, SendRequest);
            cFunc(TEvTabletPipe::TEvClientConnected::EventType, HandleConnected);
            IgnoreFunc(TEvBlobStorage::TEvControllerConfigResponse);
        )
    };

    void TestReassignsDoNotCauseErrorMessages(TBlobStorageGroupType erasure, ui32 numDCs, ui32 numNodesInDC) {
        const ui32 disksPerNode = 1;
        ui32 numNodes;
        const ui32 numGroups = 1;
        ui32 groupId;
        std::unique_ptr<TEnvironmentSetup> env;
        SetupEnvForReassignTest(numDCs, numNodesInDC, disksPerNode, numNodes, numGroups, groupId,
                erasure, env);

        const ui32 requests = 10;
        const ui32 requestsHuge = 10;
        const ui32 blobSize = 10;
        const ui32 hugeBlobSize = 4_MB;

        env->Runtime->Register(new TSequentialReassigner(groupId, env->TabletId, numNodes, TVDiskIdShort{0, 0, 0}, false), 1);
        env->Runtime->Register(new TSequentialReassigner(groupId, env->TabletId, numNodes, TVDiskIdShort{0, 1, 0}, true), 1);
        std::vector<TInflightActor*> actors;

        const std::vector<TString> actorTypes = {
            "Put", "HugePut", "Get", "HugeGet", "Patch", "HugePatch",
        };


        for (ui32 nodeId = 1; nodeId < numNodes; ++nodeId) {
            std::vector<TInflightActor*> newActors = {
                new TInflightActorPut({requests, 1, TDuration::MilliSeconds(1), TGroupId::FromValue(groupId)}, blobSize),
                new TInflightActorPut({requestsHuge, 1, TDuration::MilliSeconds(1), TGroupId::FromValue(groupId)}, hugeBlobSize),
                new TInflightActorGet({requests, 1, TDuration::MilliSeconds(1), TGroupId::FromValue(groupId)}, blobSize),
                new TInflightActorGet({requestsHuge, 1, TDuration::MilliSeconds(1), TGroupId::FromValue(groupId)}, hugeBlobSize),
                new TInflightActorPatch({requests, 1, TDuration::MilliSeconds(1), TGroupId::FromValue(groupId)}, blobSize),
                new TInflightActorPatch({requestsHuge, 1, TDuration::MilliSeconds(1), TGroupId::FromValue(groupId)}, hugeBlobSize),
            };
            // remember to add new actor type to actorTypes
            Y_ABORT_UNLESS(newActors.size() == actorTypes.size());
            for (auto* actor : newActors) {
                actors.push_back(actor);
                env->Runtime->Register(actor, nodeId);
            }
        }

        env->Sim(TDuration::Seconds(30));

        for (ui32 nodeId = 1; nodeId < numNodes; ++nodeId) {
            for (ui32 i = 0; i < actorTypes.size(); ++i) {
                auto* actor = actors[actorTypes.size() * (nodeId - 1) + i];
                ui32 errors = actor->ResponsesByStatus[NKikimrProto::ERROR];
                UNIT_ASSERT_VALUES_EQUAL_C(errors, 0, "NodeId# " << nodeId <<
                        " ActorType# " << actorTypes[i] << " errors# " << errors);
            }
        }
    }

    Y_UNIT_TEST(ReassignsDoNotCauseErrorMessagesMirror3dc) {
        TestReassignsDoNotCauseErrorMessages(TBlobStorageGroupType::ErasureMirror3dc, 3, 4);
    }

    Y_UNIT_TEST(ReassignsDoNotCauseErrorMessagesMirror3of4) {
        TestReassignsDoNotCauseErrorMessages(TBlobStorageGroupType::ErasureMirror3of4, 1, 12);
    }

    Y_UNIT_TEST(ReassignsDoNotCauseErrorMessagesBlock4Plus2) {
        TestReassignsDoNotCauseErrorMessages(TBlobStorageGroupType::Erasure4Plus2Block, 1, 12);
    }
}


Y_UNIT_TEST_SUITE(GroupAllocation) {
    Y_UNIT_TEST(HalfRacks) {
        const ui32 numNodes = 4;
        TBlobStorageGroupType groupType = TBlobStorageGroupType::Erasure4Plus2Block;
        const ui32 pdisksPerScope = 4;
        const ui32 domainLevelEnd = 51;
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = numNodes,
            .Erasure = groupType,
            .ControllerNodeId = numNodes,
            .LocationGenerator = [&](ui32 nodeId) {
                NActorsInterconnect::TNodeLocation proto;
                proto.SetDataCenter("A");
                proto.SetRack(std::to_string(nodeId));
                proto.SetUnit("1");
                return TNodeLocation(proto);
            },
        });

        {
            NKikimrBlobStorage::EPDiskType pdiskType = NKikimrBlobStorage::EPDiskType::ROT;
            NKikimrBlobStorage::TConfigRequest request;
            auto *cmd = request.AddCommand()->MutableDefineHostConfig();
            cmd->SetHostConfigId(1);
            for (std::string diskScope : {"scope-A", "scope-B"}) {
                for (ui32 pdiskIdx = 0; pdiskIdx < pdisksPerScope; ++pdiskIdx) {
                    auto *drive = cmd->AddDrive();
                    std::string pdiskName = diskScope + std::to_string(pdiskIdx);
                    drive->SetPath("SectorMap:" + pdiskName + ":1000");
                    drive->SetType(pdiskType);
                    drive->SetDiskScope(diskScope.data(), diskScope.size());
                }
            }

            auto *cmd1 = request.AddCommand()->MutableDefineBox();
            cmd1->SetBoxId(1);
            for (ui32 nodeId : env.Runtime->GetNodes()) {
                auto *host = cmd1->AddHost();
                host->MutableKey()->SetNodeId(nodeId);
                host->SetHostConfigId(1);
            }

            auto *cmd2 = request.AddCommand()->MutableDefineStoragePool();
            cmd2->SetBoxId(1);
            cmd2->SetStoragePoolId(1);
            cmd2->SetName(env.StoragePoolName);
            cmd2->SetKind(env.StoragePoolName);
            cmd2->SetErasureSpecies(TBlobStorageGroupType::ErasureSpeciesName(env.Settings.Erasure.GetErasure()));
            cmd2->SetVDiskKind("Default");
            cmd2->SetNumGroups(1);
            cmd2->AddPDiskFilter()->AddProperty()->SetType(pdiskType);

            NKikimrBlobStorage::TGroupGeometry* geometry = cmd2->MutableGeometry();
            geometry->SetNumFailRealms(1);
            geometry->SetNumFailDomainsPerFailRealm(8);
            geometry->SetNumVDisksPerFailDomain(1);
            geometry->SetRealmLevelBegin(10);
            geometry->SetRealmLevelEnd(20);
            geometry->SetDomainLevelBegin(10);
            geometry->SetDomainLevelEnd(domainLevelEnd);

            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        NKikimrBlobStorage::TBaseConfig base = env.FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);

        const ui32 groupId = base.GetGroup(0).GetGroupId();

        TGroupGeometryInfo geom = CreateGroupGeometry(groupType, 1, 8, 1, 10, 20, 10, domainLevelEnd);

        auto getPDiskScopes = [](const NKikimrBlobStorage::TBaseConfig& cfg) {
            std::map<std::pair<ui32, ui32>, TString> scopes;
            for (const auto& pdisk : cfg.GetPDisk()) {
                if (pdisk.HasDiskScope()) {
                    scopes.emplace(std::make_pair(pdisk.GetNodeId(), pdisk.GetPDiskId()), pdisk.GetDiskScope());
                }
            }
            return scopes;
        };

        auto checkLayout = [&](const NKikimrBlobStorage::TBaseConfig& cfg) {
            TString error;
            UNIT_ASSERT_C(CheckBaseConfigLayout(geom, cfg, true, error), error);

            auto scopes = getPDiskScopes(cfg);
            std::set<std::pair<ui32, ui32>> usedPDisks;
            std::set<std::pair<ui32, TString>> usedNodeScopePairs;
            for (const auto& vslot : cfg.GetVSlot()) {
                const auto key = std::make_pair(vslot.GetVSlotId().GetNodeId(), vslot.GetVSlotId().GetPDiskId());
                const auto it = scopes.find(key);
                UNIT_ASSERT_C(it != scopes.end(), "group VDisk resides on PDisk without DiskScope, nodeId# "
                        << key.first << " pdiskId# " << key.second);
                const TString& scope = it->second;
                usedPDisks.insert(key);
                const bool inserted = usedNodeScopePairs.emplace(key.first, scope).second;
                UNIT_ASSERT_C(inserted, "two group VDisks share the same node and DiskScope, nodeId# "
                        << key.first << " scope# " << scope);
            }
            UNIT_ASSERT_VALUES_EQUAL(usedPDisks.size(), 8);
            UNIT_ASSERT_VALUES_EQUAL(usedNodeScopePairs.size(), 8);
        };

        auto checkGroupStatus = [&]() {
            TActorId edge = env.Runtime->AllocateEdgeActor(1);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvStatus(TInstant::Max()));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(edge, true, TInstant::Max());
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        };

        env.Sim(TDuration::Seconds(30));

        checkLayout(base);
        checkGroupStatus();

        for (ui32 i = 0; i < 100; ++i) {
            base = env.FetchBaseConfig();
            const auto& group = base.GetGroup(0);
            const ui32 idx = RandomNumber<ui32>(group.VSlotIdSize());

            const NKikimrBlobStorage::TBaseConfig::TVSlot& vslot = base.GetVSlot(idx);

            NKikimrBlobStorage::TConfigRequest request;
            auto* reassign = request.AddCommand()->MutableReassignGroupDisk();
            reassign->SetGroupId(vslot.GetGroupId());
            reassign->SetGroupGeneration(vslot.GetGroupGeneration());
            reassign->SetFailRealmIdx(vslot.GetFailRealmIdx());
            reassign->SetFailDomainIdx(vslot.GetFailDomainIdx());
            reassign->SetVDiskIdx(vslot.GetVDiskIdx());

            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), "iteration# " << i << " " << response.GetErrorDescription());

            env.Sim(TDuration::Seconds(30));

            base = env.FetchBaseConfig();
            checkLayout(base);
            checkGroupStatus();
        }
    }

    // DomainLevelEnd values corresponding to fail_domain_type setting (see ydb/tools/cfg/types.py, DistinctionLevels)
    constexpr ui32 DomainLevelEndRack = 40;      // fail_domain_type: rack
    constexpr ui32 DomainLevelEndUnit = 50;      // fail_domain_type: body (unit)
    constexpr ui32 DomainLevelEndDiskScope = 60; // fail_domain_type: disk_scope
    constexpr ui32 DomainLevelEndDisk = 256;     // fail_domain_type: disk

    std::function<TNodeLocation(ui32)> MakeLocationGenerator() {
        return [](ui32 nodeId) {
            NActorsInterconnect::TNodeLocation proto;
            proto.SetDataCenter("A");
            proto.SetRack(std::to_string(nodeId));
            proto.SetUnit("1");
            return TNodeLocation(proto);
        };
    }

    NKikimrBlobStorage::TGroupGeometry MakeGeometry(ui32 domainLevelEnd) {
        NKikimrBlobStorage::TGroupGeometry geometry;
        geometry.SetNumFailRealms(1);
        geometry.SetNumFailDomainsPerFailRealm(8);
        geometry.SetNumVDisksPerFailDomain(1);
        geometry.SetRealmLevelBegin(10);
        geometry.SetRealmLevelEnd(20);
        geometry.SetDomainLevelBegin(10);
        geometry.SetDomainLevelEnd(domainLevelEnd);
        return geometry;
    }

    // scopes: list of {diskScope, numDrives}; empty diskScope means drives without DiskScope set
    void DefineHostConfig(NKikimrBlobStorage::TConfigRequest& request, ui32 hostConfigId,
            const std::vector<std::pair<TString, ui32>>& scopes) {
        auto *cmd = request.AddCommand()->MutableDefineHostConfig();
        cmd->SetHostConfigId(hostConfigId);
        for (const auto& [diskScope, numDrives] : scopes) {
            for (ui32 pdiskIdx = 0; pdiskIdx < numDrives; ++pdiskIdx) {
                auto *drive = cmd->AddDrive();
                const TString pdiskName = (diskScope ? diskScope : "noscope") + ToString(pdiskIdx);
                drive->SetPath("SectorMap:" + pdiskName + ":1000");
                drive->SetType(NKikimrBlobStorage::EPDiskType::ROT);
                if (diskScope) {
                    drive->SetDiskScope(diskScope);
                }
            }
        }
    }

    void DefineBox(NKikimrBlobStorage::TConfigRequest& request, TEnvironmentSetup& env,
            const std::function<ui32(ui32)>& hostConfigIdForNode) {
        auto *cmd = request.AddCommand()->MutableDefineBox();
        cmd->SetBoxId(1);
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            auto *host = cmd->AddHost();
            host->MutableKey()->SetNodeId(nodeId);
            host->SetHostConfigId(hostConfigIdForNode(nodeId));
        }
    }

    void DefineStoragePool(NKikimrBlobStorage::TConfigRequest& request, TEnvironmentSetup& env,
            ui32 storagePoolId, const TString& name, ui32 numGroups, ui32 domainLevelEnd) {
        auto *cmd = request.AddCommand()->MutableDefineStoragePool();
        cmd->SetBoxId(1);
        cmd->SetStoragePoolId(storagePoolId);
        cmd->SetName(name);
        cmd->SetKind(name);
        cmd->SetErasureSpecies(TBlobStorageGroupType::ErasureSpeciesName(env.Settings.Erasure.GetErasure()));
        cmd->SetVDiskKind("Default");
        cmd->SetNumGroups(numGroups);
        cmd->AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::EPDiskType::ROT);
        cmd->MutableGeometry()->CopyFrom(MakeGeometry(domainLevelEnd));
    }

    std::map<std::pair<ui32, ui32>, TString> GetPDiskScopes(const NKikimrBlobStorage::TBaseConfig& cfg) {
        std::map<std::pair<ui32, ui32>, TString> scopes;
        for (const auto& pdisk : cfg.GetPDisk()) {
            if (pdisk.HasDiskScope()) {
                scopes.emplace(std::make_pair(pdisk.GetNodeId(), pdisk.GetPDiskId()), pdisk.GetDiskScope());
            }
        }
        return scopes;
    }

    Y_UNIT_TEST(DiskScopeUniformDistribution) {
        const ui32 numNodes = 4;
        const ui32 disksPerNode = 4;
        const ui32 numGroups = 16;
        TBlobStorageGroupType groupType = TBlobStorageGroupType::Erasure4Plus2Block;
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = numNodes,
            .Erasure = groupType,
            .ControllerNodeId = numNodes,
            .LocationGenerator = MakeLocationGenerator(),
        });

        {
            NKikimrBlobStorage::TConfigRequest request;
            DefineHostConfig(request, 1, {{"scope-A", 2}, {"scope-B", 2}});
            DefineBox(request, env, [](ui32) { return 1; });
            DefineStoragePool(request, env, 1, env.StoragePoolName, numGroups, DomainLevelEndDiskScope);
            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        NKikimrBlobStorage::TBaseConfig base = env.FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), numGroups);
        UNIT_ASSERT_VALUES_EQUAL(base.PDiskSize(), numNodes * disksPerNode);

        TGroupGeometryInfo geom = CreateGroupGeometry(groupType, 1, 8, 1, 10, 20, 10, DomainLevelEndDiskScope);
        TString error;
        UNIT_ASSERT_C(CheckBaseConfigLayout(geom, base, true, error), error);

        const auto scopes = GetPDiskScopes(base);

        std::map<ui32, std::set<std::pair<ui32, TString>>> groupNodeScopePairs;
        std::map<std::pair<ui32, ui32>, ui32> slotsPerPDisk;
        for (const auto& vslot : base.GetVSlot()) {
            const auto key = std::make_pair(vslot.GetVSlotId().GetNodeId(), vslot.GetVSlotId().GetPDiskId());
            ++slotsPerPDisk[key];
            const auto it = scopes.find(key);
            UNIT_ASSERT_C(it != scopes.end(), "group VDisk resides on PDisk without DiskScope, nodeId# "
                    << key.first << " pdiskId# " << key.second);
            const bool inserted = groupNodeScopePairs[vslot.GetGroupId()].emplace(key.first, it->second).second;
            UNIT_ASSERT_C(inserted, "two VDisks of the same group share node and DiskScope, groupId# "
                    << vslot.GetGroupId() << " nodeId# " << key.first << " scope# " << it->second);
        }

        // every group occupies 8 distinct (node, scope) fail domains
        UNIT_ASSERT_VALUES_EQUAL(groupNodeScopePairs.size(), numGroups);
        for (const auto& [groupId, nodeScopePairs] : groupNodeScopePairs) {
            UNIT_ASSERT_VALUES_EQUAL_C(nodeScopePairs.size(), 8, "groupId# " << groupId);
        }

        // VDisks are distributed uniformly among all PDisks
        const ui32 expectedSlotsPerPDisk = numGroups * 8 / (numNodes * disksPerNode);
        UNIT_ASSERT_VALUES_EQUAL(slotsPerPDisk.size(), numNodes * disksPerNode);
        for (const auto& [pdisk, slots] : slotsPerPDisk) {
            UNIT_ASSERT_VALUES_EQUAL_C(slots, expectedSlotsPerPDisk, "uneven VDisk distribution, nodeId# "
                    << pdisk.first << " pdiskId# " << pdisk.second);
        }
    }

    Y_UNIT_TEST(AllFailDomainTypes) {
        const ui32 numNodes = 8;
        TBlobStorageGroupType groupType = TBlobStorageGroupType::Erasure4Plus2Block;
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = numNodes,
            .Erasure = groupType,
            .ControllerNodeId = numNodes,
            .LocationGenerator = MakeLocationGenerator(),
        });

        const std::vector<std::pair<TString, ui32>> pools = {
            {"disk_scope", DomainLevelEndDiskScope},
            {"unit", DomainLevelEndUnit},
            {"disk", DomainLevelEndDisk},
            {"rack", DomainLevelEndRack},
        };

        {
            NKikimrBlobStorage::TConfigRequest request;
            DefineHostConfig(request, 1, {{"scope-A", 2}, {"scope-B", 2}});
            DefineBox(request, env, [](ui32) { return 1; });
            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        for (size_t i = 0; i < pools.size(); ++i) {
            NKikimrBlobStorage::TConfigRequest request;
            DefineStoragePool(request, env, i + 1, "pool-" + pools[i].first, 1, pools[i].second);
            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), "failed to allocate group with fail_domain_type# "
                    << pools[i].first << " error# " << response.GetErrorDescription());
        }

        NKikimrBlobStorage::TBaseConfig base = env.FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), pools.size());

        std::map<ui32, ui32> groupToPool;
        for (const auto& group : base.GetGroup()) {
            groupToPool[group.GetGroupId()] = group.GetStoragePoolId();
        }

        const auto scopes = GetPDiskScopes(base);

        std::map<ui32, std::vector<std::pair<ui32, ui32>>> pdisksPerPool; // storagePoolId -> pdisks with group VDisks
        for (const auto& vslot : base.GetVSlot()) {
            const auto it = groupToPool.find(vslot.GetGroupId());
            UNIT_ASSERT(it != groupToPool.end());
            pdisksPerPool[it->second].emplace_back(vslot.GetVSlotId().GetNodeId(), vslot.GetVSlotId().GetPDiskId());
        }

        for (size_t i = 0; i < pools.size(); ++i) {
            const auto& [name, domainLevelEnd] = pools[i];
            const auto& pdisks = pdisksPerPool[i + 1];
            UNIT_ASSERT_VALUES_EQUAL_C(pdisks.size(), 8, "fail_domain_type# " << name);

            // verify layout of this pool's group against pool geometry
            NKikimrBlobStorage::TBaseConfig poolConfig = base;
            poolConfig.ClearVSlot();
            for (const auto& vslot : base.GetVSlot()) {
                if (groupToPool[vslot.GetGroupId()] == i + 1) {
                    poolConfig.AddVSlot()->CopyFrom(vslot);
                }
            }
            TGroupGeometryInfo geom = CreateGroupGeometry(groupType, 1, 8, 1, 10, 20, 10, domainLevelEnd);
            TString error;
            UNIT_ASSERT_C(CheckBaseConfigLayout(geom, poolConfig, true, error), "fail_domain_type# " << name
                    << " error# " << error);

            if (domainLevelEnd == DomainLevelEndDiskScope) {
                // disk_scope fail domain type must not allow two VDisks to share the same node and DiskScope
                std::set<std::pair<ui32, TString>> nodeScopePairs;
                for (const auto& pdisk : pdisks) {
                    const auto it = scopes.find(pdisk);
                    UNIT_ASSERT_C(it != scopes.end(), "group VDisk resides on PDisk without DiskScope, nodeId# "
                            << pdisk.first << " pdiskId# " << pdisk.second);
                    const bool inserted = nodeScopePairs.emplace(pdisk.first, it->second).second;
                    UNIT_ASSERT_C(inserted, "two VDisks of the same group share node and DiskScope, nodeId# "
                            << pdisk.first << " scope# " << it->second);
                }
            } else if (domainLevelEnd == DomainLevelEndRack) {
                // rack fail domain type must not allow two VDisks on the same node
                std::set<ui32> distinctNodes;
                for (const auto& pdisk : pdisks) {
                    distinctNodes.insert(pdisk.first);
                }
                UNIT_ASSERT_VALUES_EQUAL_C(distinctNodes.size(), pdisks.size(),
                        "two VDisks of the same group occupy the same node, fail_domain_type# " << name);
            }
        }
    }

    Y_UNIT_TEST(PartialDiskScopeMarkdown) {
        const ui32 numNodes = 4;
        TBlobStorageGroupType groupType = TBlobStorageGroupType::Erasure4Plus2Block;
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = numNodes,
            .Erasure = groupType,
            .ControllerNodeId = numNodes,
            .LocationGenerator = MakeLocationGenerator(),
        });

        {
            NKikimrBlobStorage::TConfigRequest request;
            DefineHostConfig(request, 1, {{"scope-A", 2}, {"scope-B", 2}}); // with disk scopes
            DefineHostConfig(request, 2, {{"", 4}}); // without disk scopes
            DefineBox(request, env, [&](ui32 nodeId) { return nodeId <= numNodes / 2 ? 1 : 2; });
            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        {
            // allocation with fail_domain_type: disk_scope is impossible: nodes without
            // DiskScope markdown provide only one fail domain each, 6 fail domains total
            NKikimrBlobStorage::TConfigRequest request;
            DefineStoragePool(request, env, 1, "pool-disk-scope", 1, DomainLevelEndDiskScope);
            auto response = env.Invoke(request);
            UNIT_ASSERT_C(!response.GetSuccess(), "group allocation with fail_domain_type# disk_scope must fail");
        }

        {
            // allocation with fail_domain_type: disk is possible: every PDisk is a separate fail domain
            NKikimrBlobStorage::TConfigRequest request;
            DefineStoragePool(request, env, 2, "pool-disk", 1, DomainLevelEndDisk);
            auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        NKikimrBlobStorage::TBaseConfig base = env.FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);

        TGroupGeometryInfo geom = CreateGroupGeometry(groupType, 1, 8, 1, 10, 20, 10, DomainLevelEndDisk);
        TString error;
        UNIT_ASSERT_C(CheckBaseConfigLayout(geom, base, true, error), error);

        std::set<std::pair<ui32, ui32>> usedPDisks;
        for (const auto& vslot : base.GetVSlot()) {
            const bool inserted = usedPDisks.emplace(vslot.GetVSlotId().GetNodeId(), vslot.GetVSlotId().GetPDiskId()).second;
            UNIT_ASSERT_C(inserted, "two VDisks of the same group occupy the same PDisk");
        }
        UNIT_ASSERT_VALUES_EQUAL(usedPDisks.size(), 8);
    }
}
