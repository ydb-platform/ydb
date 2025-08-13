#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

Y_UNIT_TEST_SUITE(GroupConfigurationPropagation) {

struct TTestCtx : public TTestCtxBase {
    TTestCtx(const TBlobStorageGroupType& erasure)
        : TTestCtxBase(TEnvironmentSetup::TSettings{
            .NodeCount = erasure.BlobSubgroupSize() * 2,
            .Erasure = erasure,
            .ControllerNodeId = erasure.BlobSubgroupSize() * 2,
        })
    {}

    static bool BlockBscResponses(ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerNodeServiceSetUpdate::EventType) {
            return false;
        }
        
        return true;
    }

    void AllocateEdgeActorOnNodeWOConfig() {
        std::set<ui32> nodesWOConfig = GetComplementNodeSet(NodesWithConfig);
        UNIT_ASSERT(!NodesWithConfig.empty());
        AllocateEdgeActorOnSpecificNode(*nodesWOConfig.begin());
        NodeWithProxy = Edge.NodeId();
    }

    void CheckStatus() {
        AllocateEdgeActorOnNodeWOConfig();
        auto res = GetGroupStatus(GroupId, WaitTime);
        UNIT_ASSERT(res);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ErrorReason);
    }

    void Setup() {
        Initialize();
        NodesWithConfig = GetNodesWithVDisks();
        NodesWithConfig.insert(Env->Settings.ControllerNodeId);
        AllocateEdgeActorOnNodeWOConfig();
        UNIT_ASSERT(NodeWithProxy != Env->Settings.ControllerNodeId);
        Env->Sim(TDuration::Minutes(10));
        Env->Runtime->FilterFunction = BlockBscResponses;
    }

    void StopNode() {
        Env->StopNode(NodeWithProxy);
    }

    void StartNode() {
        Env->StartNode(NodeWithProxy);
        AllocateEdgeActorOnNodeWOConfig();
    }

    void RestartNode() {
        StopNode();
        StartNode();
    }

    void UpdateGroupConfiguration() {
        NKikimrBlobStorage::TConfigRequest request;
        auto* reassign = request.AddCommand()->MutableReassignGroupDisk();
        for (const auto& slot : BaseConfig.GetVSlot()) {
            if (slot.GetGroupId() == GroupId) {
                reassign->SetGroupId(slot.GetGroupId());
                reassign->SetGroupGeneration(slot.GetGroupGeneration());
                reassign->SetFailRealmIdx(slot.GetFailRealmIdx());
                reassign->SetFailDomainIdx(slot.GetFailDomainIdx());
                reassign->SetVDiskIdx(slot.GetVDiskIdx());
                break;
            }
        }
        auto response = Env->Invoke(request);
        BaseConfig = Env->FetchBaseConfig();

        std::set<ui32> nodesWithVDisks = GetNodesWithVDisks();
        NodesWithConfig.insert(nodesWithVDisks.begin(), nodesWithVDisks.end());
        Env->Sim(TDuration::Seconds(1));
    }

    const TDuration WaitTime = TDuration::Seconds(1);
    ui32 NodeWithProxy;
    std::set<ui32> NodesWithConfig;
};

Y_UNIT_TEST(Simple) {
    TTestCtx ctx(TBlobStorageGroupType::Erasure4Plus2Block);
    ctx.Setup();
    ctx.CheckStatus();
}

Y_UNIT_TEST(Reassign) {
    TTestCtx ctx(TBlobStorageGroupType::Erasure4Plus2Block);
    ctx.Setup();
    ctx.UpdateGroupConfiguration();
    ctx.CheckStatus();
}

Y_UNIT_TEST(NodeRestart) {
    TTestCtx ctx(TBlobStorageGroupType::Erasure4Plus2Block);
    ctx.Setup();
    ctx.RestartNode();
    ctx.CheckStatus();
}

Y_UNIT_TEST(NodeRestartAndUpdate) {
    TTestCtx ctx(TBlobStorageGroupType::Erasure4Plus2Block);
    ctx.Setup();
    ctx.StopNode();
    ctx.UpdateGroupConfiguration();
    ctx.StartNode();
    ctx.CheckStatus();
}

Y_UNIT_TEST(BscRestart) {
    TTestCtx ctx(TBlobStorageGroupType::Erasure4Plus2Block);
    ctx.Setup();
    ctx.Env->RestartNode(ctx.Env->Settings.ControllerNodeId);
    ctx.CheckStatus();
}

}
