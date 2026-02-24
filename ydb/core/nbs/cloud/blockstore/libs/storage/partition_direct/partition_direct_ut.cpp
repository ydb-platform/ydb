#include <ydb/core/nbs/cloud/blockstore/bootstrap/bootstrap.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct_actor.h>

// #include <ydb/core/blobstorage/ddisk/ddisk.h>
// #include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
// #include <ydb/core/util/actorsys_test/testactorsys.h>
// #include <ydb/core/protos/config.pb.h>
// #include <ydb/core/testlib/tablet_helpers.h>

// using namespace NKikimr;

// namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

// namespace {

// ////////////////////////////////////////////////////////////////////////////////

// const TString DDiskPoolName = "ddp1";
// const TString PersistentBufferDDiskPoolName = "ddp1";
// const ui64 PartitionTabletId = MakeTabletID(1, 0, 1);

// void SetupStorage(TEnvironmentSetup& env)
// {
//     env.CreateBoxAndPool();
//     env.Sim(TDuration::Seconds(30));

//     {
//         NKikimrBlobStorage::TConfigRequest request;
//         auto* cmd = request.AddCommand()->MutableDefineDDiskPool();
//         cmd->SetBoxId(1);
//         cmd->SetName(DDiskPoolName);
//         auto* g = cmd->MutableGeometry();
//         g->SetRealmLevelBegin(10);
//         g->SetRealmLevelEnd(20);
//         g->SetDomainLevelBegin(10);
//         g->SetDomainLevelEnd(40);
//         g->SetNumFailRealms(1);
//         g->SetNumFailDomainsPerFailRealm(5);
//         g->SetNumVDisksPerFailDomain(1);
//         cmd->AddPDiskFilter()->AddProperty()->SetType(
//             NKikimrBlobStorage::EPDiskType::ROT);
//         cmd->SetNumDDiskGroups(3);
//         auto res = env.Invoke(request);
//         UNIT_ASSERT_C(res.GetSuccess(), res.GetErrorDescription());
//     }

//     // Setup NBS service with storage config
//     NKikimrConfig::TNbsConfig nbsConfig;
//     auto* storageConfig = nbsConfig.MutableNbsStorageConfig();
//     storageConfig->SetDDiskPoolName(DDiskPoolName);
//     storageConfig->SetPersistentBufferDDiskPoolName(PersistentBufferDDiskPoolName);
//     CreateNbsService(nbsConfig);
//     StartNbsService();
// }

// NKikimrBlockStore::TVolumeConfig CreateVolumeConfig()
// {
//     NKikimrBlockStore::TVolumeConfig volumeConfig;
//     volumeConfig.SetDiskId("test-volume");
//     volumeConfig.SetBlockSize(4096);
//     volumeConfig.SetStoragePoolName(DDiskPoolName);
//     auto* partition = volumeConfig.AddPartitions();
//     partition->SetBlockCount(32768);
//     return volumeConfig;
// }

// ui64 CreatePartitionTablet(TEnvironmentSetup& env)
// {
//     // Create tablet like in SetupTablet()
//     env.Runtime->CreateTestBootstrapper(
//         TTestActorSystem::CreateTestTabletInfo(
//             PartitionTabletId,
//             TTabletTypes::Unknown,
//             env.Settings.Erasure.GetErasure(),
//             env.GroupId,
//             3),  // NumChannels
//         [](const TActorId& tablet, TTabletStorageInfo* info) -> IActor* {
//             return new TPartitionActor(tablet, info);
//         },
//         env.Settings.ControllerNodeId);

//     // Wait for tablet to boot
//     bool working = true;
//     env.Runtime->Sim(
//         [&] { return working; },
//         [&](IEventHandle& event) {
//             working = event.GetTypeRewrite() != TEvTablet::EvBoot;
//         });

//     // Send volume config update
//     auto volumeConfig = CreateVolumeConfig();
//     auto updateEvent = std::make_unique<NKikimr::TEvBlockStore::TEvUpdateVolumeConfig>();
//     updateEvent->Record.MutableVolumeConfig()->CopyFrom(volumeConfig);
//     updateEvent->Record.SetTxId(1);

//     const TActorId& edge = env.Runtime->AllocateEdgeActor(
//         env.Settings.ControllerNodeId,
//         __FILE__,
//         __LINE__);

//     env.Runtime->SendToPipe(
//         PartitionTabletId,
//         edge,
//         updateEvent.release(),
//         0,
//         TTestActorSystem::GetPipeConfigWithRetries());

//     // Wait for response
//     auto response = env.WaitForEdgeActorEvent<NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>(edge);
//     UNIT_ASSERT(response->Get()->Record.GetStatus() == NKikimrBlockStore::OK);

//     // Wait for partition to allocate DDisk group
//     env.Sim(TDuration::Seconds(5));

//     return PartitionTabletId;
// }

// TActorId GetLoadActorAdapterActorId(
//     TEnvironmentSetup& env,
//     ui64 partitionTabletId,
//     const TActorId& edge)
// {
//     auto request = std::make_unique<TEvService::TEvGetLoadActorAdapterActorIdRequest>();
//     env.Runtime->SendToPipe(
//         partitionTabletId,
//         edge,
//         request.release(),
//         0,
//         TTestActorSystem::GetPipeConfigWithRetries());

//     auto res = env.WaitForEdgeActorEvent<
//         TEvService::TEvGetLoadActorAdapterActorIdResponse>(edge, false);
//     UNIT_ASSERT(res);
//     UNIT_ASSERT(!res->Get()->ActorId.empty());
//     NActors::TActorId loadActorAdapter;
//     loadActorAdapter.Parse(
//         res->Get()->ActorId.data(),
//         res->Get()->ActorId.size());
//     return loadActorAdapter;
// }

// }   // namespace

// ////////////////////////////////////////////////////////////////////////////////

// Y_UNIT_TEST_SUITE(TPartitionDirectTest)
// {
//     Y_UNIT_TEST(BasicWriteRead)
//     {
//         TEnvironmentSetup env{{
//             .NodeCount = 8,
//             .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
//         }};
//         auto& runtime = env.Runtime;
//         runtime->SetLogPriority(
//             NKikimrServices::NBS_PARTITION,
//             NActors::NLog::PRI_DEBUG);

//         SetupStorage(env);

//         auto partition = CreatePartitionTablet(env);

//         const TActorId& edge = runtime->AllocateEdgeActor(
//             env.Settings.ControllerNodeId,
//             __FILE__,
//             __LINE__);

//         auto loadActorAdapter =
//             GetLoadActorAdapterActorId(env, partition, edge);

//         // Read not writed block
//         {
//             auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
//             request->Record.SetStartIndex(0);
//             request->Record.SetBlocksCount(1);

//             runtime->Send(
//                 new IEventHandle(loadActorAdapter, edge, request.release()),
//                 edge.NodeId());

//             auto res =
//                 env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
//                     edge,
//                     false);
//             UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
//             UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
//             UNIT_ASSERT(
//                 res->Get()->Record.MutableBlocks()->GetBuffers(0) ==
//                 TString(4096, 0));
//         }

//         auto syncRequestsCount = 0;
//         runtime->FilterFunction =
//             [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev)
//         {
//             if (ev->GetTypeRewrite() ==
//                 NDDisk::TEvSyncWithPersistentBuffer::EventType)
//             {
//                 if (syncRequestsCount++ < 3) {
//                     runtime->Schedule(
//                         TDuration::Seconds(10),
//                         ev.release(),
//                         nullptr,
//                         nodeId);

//                     return false;
//                 }
//             }

//             return true;
//         };

//         auto expectedData = TString(1024, 'A') + TString(1024, 'B') +
//                             TString(1024, 'C') + TString(1024, 'D');
//         {
//             auto request =
//                 std::make_unique<TEvService::TEvWriteBlocksRequest>();
//             request->Record.SetStartIndex(1);
//             request->Record.MutableBlocks()->AddBuffers(expectedData);

//             runtime->Send(
//                 new IEventHandle(loadActorAdapter, edge, request.release()),
//                 edge.NodeId());

//             auto res =
//                 env.WaitForEdgeActorEvent<TEvService::TEvWriteBlocksResponse>(
//                     edge,
//                     false);
//             UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
//         }

//         // Read written block from persistent buffer
//         {
//             auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
//             request->Record.SetStartIndex(1);
//             request->Record.SetBlocksCount(1);

//             runtime->Send(
//                 new IEventHandle(loadActorAdapter, edge, request.release()),
//                 edge.NodeId());

//             auto res =
//                 env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
//                     edge,
//                     false);
//             UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
//             UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
//             UNIT_ASSERT_VALUES_EQUAL(
//                 res->Get()->Record.MutableBlocks()->GetBuffers(0),
//                 expectedData);
//         }

//         // Read not written block
//         {
//             auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
//             request->Record.SetStartIndex(0);
//             request->Record.SetBlocksCount(1);

//             runtime->Send(
//                 new IEventHandle(loadActorAdapter, edge, request.release()),
//                 edge.NodeId());

//             auto res =
//                 env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
//                     edge,
//                     false);
//             UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
//             UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
//             UNIT_ASSERT(
//                 res->Get()->Record.MutableBlocks()->GetBuffers(0) ==
//                 TString(4096, 0));
//         }

//         env.Sim(TDuration::Seconds(60));

//         // Read written block from ddisk
//         {
//             auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
//             request->Record.SetStartIndex(1);
//             request->Record.SetBlocksCount(1);

//             runtime->Send(
//                 new IEventHandle(loadActorAdapter, edge, request.release()),
//                 edge.NodeId());

//             auto res =
//                 env.WaitForEdgeActorEvent<TEvService::TEvReadBlocksResponse>(
//                     edge,
//                     false);
//             UNIT_ASSERT(res->Get()->Record.MutableError()->GetCode() == S_OK);
//             UNIT_ASSERT(res->Get()->Record.MutableBlocks()->BuffersSize() == 1);
//             UNIT_ASSERT_VALUES_EQUAL(
//                 res->Get()->Record.MutableBlocks()->GetBuffers(0),
//                 expectedData);
//         }
//     }
// }

// }   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
