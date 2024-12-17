#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/base/blobstorage_common.h>
#include "ut_helpers.h"

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

        response = env->Invoke(request);

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
