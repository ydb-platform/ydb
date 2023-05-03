#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(GroupReconfiguration) {

    class TBsProxyLoader : public TActorBootstrapped<TBsProxyLoader> {
    public:
        TBsProxyLoader(ui32 groupId, ui32& sendCounter, ui32& successCounter) 
            : GroupId(groupId)
            , SendCounter(sendCounter)
            , SuccessCounter(successCounter)
        {}

        void Bootstrap(const TActorContext&/* ctx*/) {
            Become(&TThis::StateFunc);
            HandleWakeup();
        }

    private:
        ui32 GroupId;
        ui32& SendCounter;
        ui32& SuccessCounter;

        constexpr static TDuration PutDelay = TDuration::MilliSeconds(2);

        // BsProxy requests parameters
        constexpr static ui64 TabletId = 1000;
        constexpr static ui64 Channel = 2;
        constexpr static ui64 BlobSize = 100;

        static ui32 Counter;

    private:
        void Handle(TEvBlobStorage::TEvPutResult::TPtr ev) {
            if (ev->Get()->Status == NKikimrProto::OK) {
                SuccessCounter += 1;
            }
            Schedule(PutDelay, new TEvents::TEvWakeup);
        }

        void HandleWakeup() {
            TString data = TString(BlobSize, '0');
            SendToBSProxy(SelfId(), GroupId, new TEvBlobStorage::TEvPut(
                    TLogoBlobID(TabletId, 1, 1, Channel, data.size(), Counter++), 
                    std::move(data),
                    TInstant::Max()));
            SendCounter += 1;
        }

        STRICT_STFUNC(StateFunc, {
            hFunc(TEvBlobStorage::TEvPutResult, Handle)
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup)
        })
    };

    ui32 TBsProxyLoader::Counter = 0;

    void SetupTesEnv(std::unique_ptr<TEnvironmentSetup>& env, ui32 numDCs, ui32 numNodesInDC,
            ui32& groupId, ui32& fromNodeId, ui32& toNodeId, std::set<ui32>& nodesInGroup,
            std::vector<std::pair<ui32, ui32>>& counters,
            bool loadNodesWVDisks, NKikimrBlobStorage::TConfigRequest& reassignRequest) {
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
        counters.resize(numNodes); // to avoid reallocation
        for (ui32 i = 0; i < numNodes - 1; ++i) {
            ui32 nodeId = i + 1;
            counters[i] = {0, 0};
            if (loadNodesWVDisks || (nodesInGroup.count(nodeId) > 0 && nodeId != toNodeId && nodeId != fromNodeId)) {
                env->Runtime->Register(new TBsProxyLoader(groupId, counters[i].first, counters[i].second), nodeId);
            }
        }
    }

    
    void VerifyCounters(std::vector<std::pair<ui32, ui32>>& counters, ui32 acceptableLoss = 5) {
        for (ui32 i = 0; i < counters.size(); ++i) {
            auto [sent, successes] = counters[i];
            UNIT_ASSERT_GE_C(successes + acceptableLoss, sent, "Sent puts number# " << sent
                    << " recieved successes number# " << successes
                    << " nodeId# " << i + 1);
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

        std::vector<std::pair<ui32, ui32>> counters;
    
        NKikimrBlobStorage::TConfigRequest request;
        NKikimrBlobStorage::TConfigResponse response;

        std::unique_ptr<TEnvironmentSetup> env;
        SetupTesEnv(env, numDCs, numNodesInDC, groupId, fromNodeId, toNodeId, nodesInGroup, counters, requestsToNodesWVDisks, request);
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
        VerifyCounters(counters, 5);

        nodesInGroup.erase(fromNodeId);
        nodesInGroup.insert(toNodeId);
        VerifyConfigsAreSame(*env, nodesInGroup, groupId);
    }

    Y_UNIT_TEST(PropagateNewConfigurationViaVDisks) {
        TestPropagateNewConfigurationViaVDisks(true);
    }

    // TODO: KIKIMR-11627
    // Y_UNIT_TEST(PropagateNewConfigurationViaVDisksNoRequestsToNodesWVDisks) {
    //     TestPropagateNewConfigurationViaVDisks(false);
    // }

    void TestBsControllerDoesNotDisableGroup(bool requestsToNodesWVDisks) {
        const ui32 numDCs = 3;
        const ui32 numNodesInDC = 4;
        const ui32 numNodes = numDCs * numNodesInDC;
        const ui32 bscNodeId = numNodes;

        ui32 groupId;
        std::set<ui32> nodesInGroup;
        ui32 fromNodeId;
        ui32 toNodeId;

        std::vector<std::pair<ui32, ui32>> counters;
    
        NKikimrBlobStorage::TConfigRequest request;
        NKikimrBlobStorage::TConfigResponse response;

        std::unique_ptr<TEnvironmentSetup> env;
        SetupTesEnv(env, numDCs, numNodesInDC, groupId, fromNodeId, toNodeId, nodesInGroup, counters, requestsToNodesWVDisks, request);
        env->Sim(TDuration::Seconds(1));

        response = env->Invoke(request);
        env->Sim(TDuration::MilliSeconds(1));
        

        std::array<bool, numNodes - 1> passedMessages{true};

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
        VerifyCounters(counters, 5);
    }

    Y_UNIT_TEST(BsControllerDoesNotDisableGroup) {
        TestBsControllerDoesNotDisableGroup(true);
    }

    Y_UNIT_TEST(BsControllerDoesNotDisableGroupNoRequestsToNodesWVDisks) {
        TestBsControllerDoesNotDisableGroup(false);
    }

    class TConfigurationRequestActor : public TActorBootstrapped<TConfigurationRequestActor> {
    public:
        TConfigurationRequestActor(ui32 groupId, ui64 bsController, TDuration& delay) 
            : GroupId(groupId)
            , BsController(bsController)
            , Delay(delay)
        {}

        void Bootstrap(const TActorContext&/* ctx*/) {
            Become(&TThis::StateFunc);
            ClientId = Register(NKikimr::NTabletPipe::CreateClient(SelfId(), BsController,
                    TTestActorSystem::GetPipeConfigWithRetries()));
        }

    private:
        ui32 GroupId;
        ui64 BsController;
        THPTimer Timer;
        TDuration& Delay;
        TActorId ClientId;

        ui32 ReassignReqests = 10'000;
        constexpr static TDuration ReassignDelay = TDuration::MicroSeconds(1);

    private:
        void Handle(TEvBlobStorage::TEvControllerNodeServiceSetUpdate::TPtr/* ev*/) {
            Delay = TDuration::Seconds(Timer.Passed());
        }

        void HandleWakeup() {
            Timer.Reset();
            if (ReassignReqests > 0) {
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
                ReassignReqests -= 1;
                Schedule(ReassignDelay, new TEvents::TEvWakeup);
            } else {
                Timer.Reset();
                auto ev = std::make_unique<TEvBlobStorage::TEvControllerGetGroup>();
                ev->Record.AddGroupIDs(GroupId);
                NTabletPipe::SendData(SelfId(), ClientId, ev.release(), 0);
            }
        }

        void HandleConnected() {
            HandleWakeup();
        }

        void Ignore() {
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvBlobStorage::TEvControllerNodeServiceSetUpdate, Handle)
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup)
            cFunc(TEvTabletPipe::TEvClientConnected::EventType, HandleConnected)

            cFunc(TEvBlobStorage::TEvControllerConfigResponse::EventType, Ignore)
        )
    };

    Y_UNIT_TEST(BsControllerConfigurationRequestIsFastEnough) {
        const ui32 numDCs = 3;
        const ui32 numNodesInDC = 10;
        const ui32 numNodes = numDCs * numNodesInDC + 1;
        const ui32 disksPerNode = 1;
        const ui32 numGroups = numDCs * numNodesInDC * disksPerNode * 9 / 8;

        TEnvironmentSetup env{TEnvironmentSetup::TSettings{
            .NodeCount = numNodes,
            .ControllerNodeId = numNodes,
            .LocationGenerator = [&](ui32 nodeId) {
                NActorsInterconnect::TNodeLocation proto;
                proto.SetDataCenter(ToString((nodeId - 1) / numNodesInDC));
                proto.SetRack(ToString((nodeId - 1) % numNodesInDC));
                proto.SetUnit("1");
                return TNodeLocation(proto);
            },
        }};

        env.CreateBoxAndPool(disksPerNode, numGroups, numNodes);

        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableQueryBaseConfig();
        auto response = env.Invoke(request);

        const auto& base = response.GetStatus(0).GetBaseConfig();
        UNIT_ASSERT_GE(base.GroupSize(), 1);
        const auto& group = base.GetGroup(0);
        ui32 groupId = group.GetGroupId();

        std::array<TDuration, numNodes> delays{TDuration::Max()};
        const TDuration maxDelay = TDuration::Seconds(2);

        for (ui32 nodeId = 1; nodeId < numNodes; ++nodeId) {
            env.Runtime->Register(new TConfigurationRequestActor(groupId, env.TabletId, delays[nodeId - 1]), nodeId);
        }

        env.Sim(TDuration::Seconds(30));

        for (ui32 i = 0; i < numNodes - 1; ++i) {
            UNIT_ASSERT_LE_C(delays[i], maxDelay, delays[i].ToString() << ' ' << maxDelay.ToString() << ' ' << i);
        }
    }
}
