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

        // Wait for partition and direct block group to be ready
        env.Sim(TDuration::Seconds(5));
                
        const TActorId& edge = runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);

        // Read not writed block
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

        auto flushRequestsCount = 0;
        runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NDDisk::TEvFlushPersistentBuffer::EventType) {
                if (flushRequestsCount++ < 3) {
                    runtime->Schedule(
                        TDuration::Seconds(10),
                        ev.release(),
                        nullptr,
                        nodeId);

                    return false;
                }
            }

            return true;
        };

        auto expectedData = TString(1024, 'A') + TString(1024, 'B') + TString(1024, 'C') + TString(1024, 'D');
        {
            auto request = std::make_unique<NYdb::NBS::TEvService::TEvWriteBlocksRequest>();
            request->Record.SetStartIndex(1);
            request->Record.MutableBlocks()->AddBuffers(expectedData);

            runtime->Send(new IEventHandle(partition, edge, request.release()), edge.NodeId());

            auto res = env.WaitForEdgeActorEvent<TEvService::TEvWriteBlocksResponse>(edge, false);
            UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
        }

        // Read writed block from persistent buffer
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

        // Read not writed block
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

        env.Sim(TDuration::Seconds(12));
        
        // Read writed block from ddisk
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

    Y_UNIT_TEST(ShouldEraseBlockWithOutdatedLsnFromPersistentBuffer) {
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

        // Wait for partition and direct block group to be ready
        env.Sim(TDuration::Seconds(5));
                
        const TActorId& edge = runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);

        auto writeResponsesCount = 0;
        auto eraseRequestsCount = 0;
        runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case NDDisk::TEvWritePersistentBufferResult::EventType: {
                    if (writeResponsesCount++ < 3) {
                        runtime->Schedule(
                            TDuration::Seconds(10),
                            ev.release(),
                            nullptr,
                            nodeId);

                        return false;
                    }
                    break;
                }
                case NDDisk::TEvFlushPersistentBuffer::EventType: {
                    const auto& msg = *ev->Get<NDDisk::TEvFlushPersistentBuffer>();
                    if (!msg.Record.HasDDiskInstanceGuid()) {
                        ++eraseRequestsCount;
                    }
                    break;
                }
            }

            return true;
        };

        auto firstWriteData = TString(4096, 'A');
        {
            auto request = std::make_unique<NYdb::NBS::TEvService::TEvWriteBlocksRequest>();
            request->Record.SetStartIndex(1);
            request->Record.MutableBlocks()->AddBuffers(firstWriteData);

            runtime->Send(new IEventHandle(partition, edge, request.release()), edge.NodeId());
        }

        auto secondWriteData = TString(4096, 'B');
        {
            auto request = std::make_unique<NYdb::NBS::TEvService::TEvWriteBlocksRequest>();
            request->Record.SetStartIndex(1);
            request->Record.MutableBlocks()->AddBuffers(secondWriteData);

            runtime->Send(new IEventHandle(partition, edge, request.release()), edge.NodeId());
        }

        {
            auto res = env.WaitForEdgeActorEvent<TEvService::TEvWriteBlocksResponse>(edge, false);
            UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
        }

        {
            auto res = env.WaitForEdgeActorEvent<TEvService::TEvWriteBlocksResponse>(edge, false);
            UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
        }

        env.Sim(TDuration::Seconds(15));

        UNIT_ASSERT_VALUES_EQUAL(eraseRequestsCount, 3);

        // Read writed block from ddisk
        {
            auto request = std::make_unique<NYdb::NBS::TEvService::TEvReadBlocksRequest>();
            request->Record.SetStartIndex(1);
            request->Record.SetBlocksCount(1);

            runtime->Send(new IEventHandle(partition, edge, request.release()), edge.NodeId());

            auto res = env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(edge, false);
            UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
            UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.MutableBlocks()->GetBuffers(0), secondWriteData);
        }
    }
}
