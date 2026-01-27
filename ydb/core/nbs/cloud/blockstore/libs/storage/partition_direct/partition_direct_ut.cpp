#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_actor.h>

using namespace NKikimr;
using namespace NYdb::NBS::NStorage::NPartitionDirect;
using namespace NYdb::NBS;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionDirectTest) {

    Y_UNIT_TEST(BasicWriteRead) {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;
        runtime->SetLogPriority(NKikimrServices::NBS_PARTITION, NActors::NLog::PRI_DEBUG);
        env.CreateBoxAndPool();
        env.Sim(TDuration::Seconds(30));

        {
            NKikimrBlobStorage::TConfigRequest request;
            auto *cmd = request.AddCommand()->MutableDefineDDiskPool();
            cmd->SetBoxId(1);
            cmd->SetName("ddp1");
            auto *g = cmd->MutableGeometry();
            g->SetRealmLevelBegin(10);
            g->SetRealmLevelEnd(20);
            g->SetDomainLevelBegin(10);
            g->SetDomainLevelEnd(40);
            g->SetNumFailRealms(1);
            g->SetNumFailDomainsPerFailRealm(5);
            g->SetNumVDisksPerFailDomain(1);
            cmd->AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::EPDiskType::ROT);
            cmd->SetNumDDiskGroups(3);
            auto res = env.Invoke(request);
            UNIT_ASSERT_C(res.GetSuccess(), res.GetErrorDescription());
        }

        auto partition = runtime->Register(new TPartitionActor(), 1);

        // wait ready
        env.Sim(TDuration::Seconds(5));
                
        const TActorId& edge = runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        {
            auto request = std::make_unique<NYdb::NBS::TEvService::TEvReadBlocksRequest>();
            request->Record.SetStartIndex(0);
            request->Record.SetBlocksCount(1);

            runtime->Send(new IEventHandle(partition, edge, request.release()), edge.NodeId());

            auto res = env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(edge, false);
            UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
            UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
            UNIT_ASSERT(res->Get()->Record.MutableBlocks()->GetBuffers(0) == TString(4096, 0));
        }

        auto expectedData = TString(1024, 'A') + TString(1024, 'B') + TString(1024, 'C') + TString(1024, 'D');

        {
            auto request = std::make_unique<NYdb::NBS::TEvService::TEvWriteBlocksRequest>();
            request->Record.SetStartIndex(1);
            request->Record.MutableBlocks()->AddBuffers(expectedData);

            runtime->Send(new IEventHandle(partition, edge, request.release()), edge.NodeId());

            auto res = env.WaitForEdgeActorEvent<TEvService::TEvWriteBlocksResponse>(edge, false);
            UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
        }

        {
            auto request = std::make_unique<NYdb::NBS::TEvService::TEvReadBlocksRequest>();
            request->Record.SetStartIndex(0);
            request->Record.SetBlocksCount(1);

            runtime->Send(new IEventHandle(partition, edge, request.release()), edge.NodeId());

            auto res = env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(edge, false);
            UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
            UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
            UNIT_ASSERT(res->Get()->Record.MutableBlocks()->GetBuffers(0) == TString(4096, 0));
        }

        {
            auto request = std::make_unique<NYdb::NBS::TEvService::TEvReadBlocksRequest>();
            request->Record.SetStartIndex(1);
            request->Record.SetBlocksCount(1);

            runtime->Send(new IEventHandle(partition, edge, request.release()), edge.NodeId());

            auto res = env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(edge, false);
            UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
            UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.MutableBlocks()->GetBuffers(0), expectedData);
        }
    }
}
