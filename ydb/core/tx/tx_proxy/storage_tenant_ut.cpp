#include "proxy_ut_helpers.h"

#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/library/actors/interconnect/interconnect.h>

#include <ydb/core/base/path.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/tablet/tablet_impl.h>


using namespace NKikimr;
using namespace NTxProxyUT;
using namespace NHelpers;

Y_UNIT_TEST_SUITE(TStorageTenantTest) {
    Y_UNIT_TEST(Boot) {
        TTestEnvWithPoolsSupport env;
        auto lsroot = env.GetClient().Ls("/");
        Print(lsroot);

        auto lsdomain = env.GetClient().Ls(env.GetRoot());
        Print(lsdomain);
        NTestLs::Finished(lsdomain);
    }

    Y_UNIT_TEST(LsLs) {
        TTestEnvWithPoolsSupport env(2, 2);
        auto storagePool = env.CreatePoolsForTenant("USER_0");

        {
            auto subdomain = GetSubDomainDeclareSetting("USER_0");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateSubdomain("/dc-1", subdomain));
        }

        NTestLs::InSubdomain(env.GetClient().Ls("/dc-1/USER_0"), NKikimrSchemeOp::EPathTypeSubDomain);
    }

    void CheckThatAllChannelsIsRightStoragePools(const NKikimrHive::TEvGetTabletStorageInfoResult& storageInfo, TStoragePools pools) {
        auto& info = storageInfo.GetInfo();
        for (auto& channel: info.GetChannels()) {
            TString poolName = channel.GetStoragePool();
            auto it = std::find_if(pools.begin(), pools.end(), [&] (const TStoragePool& pool) {return pool.GetName() == poolName;});
            UNIT_ASSERT(it != pools.end());
        }
    }

    Y_UNIT_TEST(CreateDummyTabletsInDifferentDomains) {
        TTestEnvWithPoolsSupport env(2, 2);

        auto first_storage_pools = env.CreatePoolsForTenant("USER_0");
        ui64 tablet_in_first_domain = CreateSubDomainAndTabletInside(env, "USER_0", 1, first_storage_pools);
        auto second_storage_pools = env.CreatePoolsForTenant("USER_1");
        ui64 tablet_in_second_domain = CreateSubDomainAndTabletInside(env, "USER_1", 2, second_storage_pools);

        CheckTableIsOfline(env, tablet_in_first_domain);
        CheckTableIsOfline(env, tablet_in_second_domain);

        env.GetTenants().Run("/dc-1/USER_0");
        CheckTableBecomeAlive(env, tablet_in_first_domain);
        CheckTableRunOnProperTenantNode(env, "/dc-1/USER_0", tablet_in_first_domain);

        env.GetTenants().Run("/dc-1/USER_1");
        CheckTableBecomeAlive(env, tablet_in_second_domain);
        CheckTableRunOnProperTenantNode(env, "/dc-1/USER_1", tablet_in_second_domain);

        env.GetTenants().Stop("/dc-1/USER_0");
        CheckTableBecomeOfline(env, tablet_in_first_domain);

        env.GetTenants().Stop("/dc-1/USER_1");
        CheckTableBecomeOfline(env, tablet_in_second_domain);

        env.GetTenants().Run("/dc-1/USER_0");
        env.GetTenants().Run("/dc-1/USER_1");
        CheckTableBecomeAlive(env, tablet_in_first_domain);
        CheckTableBecomeAlive(env, tablet_in_second_domain);

        CheckTableRunOnProperTenantNode(env, "/dc-1/USER_0", tablet_in_first_domain);
        CheckTableRunOnProperTenantNode(env, "/dc-1/USER_1", tablet_in_second_domain);

        NKikimrHive::TEvGetTabletStorageInfoResult firstStorageInfo;
        env.GetClient().GetTabletStorageInfoFromHive(&env.GetRuntime(), tablet_in_first_domain, firstStorageInfo);
        UNIT_ASSERT_VALUES_EQUAL(firstStorageInfo.GetStatus(), NKikimrProto::OK);
        CheckThatAllChannelsIsRightStoragePools(firstStorageInfo, first_storage_pools);

        NKikimrHive::TEvGetTabletStorageInfoResult secondStorageInfo;
        env.GetClient().GetTabletStorageInfoFromHive(&env.GetRuntime(), tablet_in_second_domain, secondStorageInfo);
        UNIT_ASSERT_VALUES_EQUAL(secondStorageInfo.GetStatus(), NKikimrProto::OK);
        CheckThatAllChannelsIsRightStoragePools(secondStorageInfo, second_storage_pools);
    }

    Y_UNIT_TEST(CreateTableInsideSubDomain) {
        TTestEnvWithPoolsSupport env(1, 2);
        auto storagePool = env.CreatePoolsForTenant("USER_0");

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0")));

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0", storagePool), TDuration::MilliSeconds(500)));

        env.GetTenants().Run("/dc-1/USER_0");

        auto tableDesc = GetTableSimpleDescription("SimpleTable");
        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_0", tableDesc),
                                 NMsgBusProxy::MSTATUS_OK);

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeTable);
        }
    }

    Y_UNIT_TEST(CreateTableInsideSubDomain2) {
        TTestEnvWithPoolsSupport env(1, 2);
        auto storagePools = env.CreatePoolsForTenant("USER_0");

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0")));

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0", storagePools), TDuration::MilliSeconds(500)));

        auto tableDesc = GetTableSimpleDescription("SimpleTable");
        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_0", tableDesc, TDuration::MilliSeconds(500)),
                                 NMsgBusProxy::MSTATUS_INPROGRESS);

        env.GetTenants().Run("/dc-1/USER_0");

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().WaitCreateTx(&env.GetRuntime(), "/dc-1/USER_0/SimpleTable", WaitTimeOut));

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeTable);
        }

        auto tablePortions = NTestLs::ExtractTablePartitions(env.GetClient().Ls("/dc-1/USER_0/SimpleTable"));
        for (NKikimrSchemeOp::TTablePartition& portion: tablePortions) {
            auto tabletId = portion.GetDatashardId();

            UNIT_ASSERT(env.GetClient().TabletExistsInHive(&env.GetRuntime(), tabletId));

            NKikimrHive::TEvGetTabletStorageInfoResult storageInfo;
            env.GetClient().GetTabletStorageInfoFromHive(&env.GetRuntime(), tabletId, storageInfo);
            UNIT_ASSERT_VALUES_EQUAL(storageInfo.GetStatus(), NKikimrProto::OK);

            CheckThatAllChannelsIsRightStoragePools(storageInfo, storagePools);
        }
    }

    Y_UNIT_TEST(CreateSolomonInsideSubDomain) {
        TTestEnvWithPoolsSupport env(1, 2);

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0")));

        auto storagePools = env.CreatePoolsForTenant("USER_0");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0", storagePools), TDuration::MilliSeconds(500)));

        env.GetTenants().Run("/dc-1/USER_0");

        const ui32 partsCount = 4;
        const ui32 channelProfile = 2;
        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateSolomon("/dc-1/USER_0", "Solomon", partsCount, channelProfile),
                                 NMsgBusProxy::MSTATUS_OK);

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0/Solomon");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeSolomonVolume);
        }

        env.GetTenants().Stop();
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteSubdomain("/dc-1", "USER_0"));
    }

    Y_UNIT_TEST(DeclareAndDefine) {
        TTestEnvWithPoolsSupport env(1, 1);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        auto storagePool = env.CreatePoolsForTenant("USER_0");

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        {
            auto subdomain = GetSubDomainDeclareSetting("USER_0");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateSubdomain("/dc-1", subdomain));
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeSubDomain);
        }

        {
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().MkDir("/dc-1/USER_0", "dir"));
            auto ls = env.GetClient().Ls("/dc-1/USER_0/dir");
            NTestLs::InSubdomain(ls);
        }

        env.GetTenants().Run("/dc-1/USER_0");


        {
            auto subdomain = GetSubDomainDefaultSetting("USER_0", storagePool);
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().AlterSubdomain("/dc-1", subdomain));
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::InSubdomainWithPools(ls, NKikimrSchemeOp::EPathTypeSubDomain);
        }

        {
            auto tableDesc = GetTableSimpleDescription("table1");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateTable("/dc-1/USER_0/dir", tableDesc));
            auto ls = env.GetClient().Ls("/dc-1/USER_0/dir/table1");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeTable);
        }

        {
            auto tableDesc = GetTableSimpleDescription("table2");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateTable("/dc-1/USER_0/dir", tableDesc));
            auto ls = env.GetClient().Ls("/dc-1/USER_0/dir/table2");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeTable);
        }

        SetRowInSimpletable(env, 42, 111, "/dc-1/USER_0/dir/table1");
        SetRowInSimpletable(env, 42, 111, "/dc-1/USER_0/dir/table2");

        {
            NKikimrMiniKQL::TResult result;
            env.GetClient().FlatQuery("("
                "(let row0_ '('('key (Uint64 '42))))"
                "(let cols_ '('value))"
                "(let select0_ (SelectRow '/dc-1/USER_0/dir/table1 row0_ cols_))"
                "(let select1_ (SelectRow '/dc-1/USER_0/dir/table2 row0_ cols_))"
                "(let ret_ (AsList"
                "    (SetResult 'res0_ select0_)"
                "    (SetResult 'res1_ select1_)"
                "))"
                "(return ret_)"
                ")", result);
        }
    }

    Y_UNIT_TEST(GenericCases) {
        TTestEnvWithPoolsSupport env(1, 1);
        auto storagePool = env.CreatePoolsForTenant("USER_0");

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0")));

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0", storagePool), TDuration::MilliSeconds(500)));

        env.GetTenants().Run("/dc-1/USER_0");

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeSubDomain);
        }

        {
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().MkDir("/dc-1/USER_0", "dir"));
            auto ls = env.GetClient().Ls("/dc-1/USER_0/dir");
            NTestLs::InSubdomain(ls);
        }

        {
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().MkDir("/dc-1", "dir"));
            auto ls = env.GetClient().Ls("/dc-1/dir");
            NTestLs::NotInSubdomain(ls);
        }

        {
            auto ls = env.GetClient().Ls("/dc-1");
            NTestLs::NotInSubdomain(ls);
        }

        {
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().MkDir("/dc-1/USER_0/dir", "dir_0"));
            auto ls = env.GetClient().Ls("/dc-1/USER_0/dir/dir_0");
            NTestLs::InSubdomain(ls);
        }

        {
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().MkDir("/dc-1/USER_0/dir", "dir_1"));
            auto ls = env.GetClient().Ls("/dc-1/USER_0/dir/dir_1");
            NTestLs::InSubdomain(ls);
        }

        {
            auto tableDesc = GetTableSimpleDescription("table");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateTable("/dc-1/USER_0/dir/dir_0", tableDesc));
            auto ls = env.GetClient().Ls("/dc-1/USER_0/dir/dir_0/table");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeTable);
        }

        {
            auto tableDesc = GetTableSimpleDescription("table");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateTable("/dc-1/USER_0/dir/dir_1", tableDesc));
            auto ls = env.GetClient().Ls("/dc-1/USER_0/dir/dir_1/table");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeTable);
        }

        SetRowInSimpletable(env, 42, 111, "/dc-1/USER_0/dir/dir_0/table");
        SetRowInSimpletable(env, 42, 111, "/dc-1/USER_0/dir/dir_1/table");

        {
            NKikimrMiniKQL::TResult result;
            env.GetClient().FlatQuery("("
                "(let row0_ '('('key (Uint64 '42))))"
                "(let cols_ '('value))"
                "(let select0_ (SelectRow '/dc-1/USER_0/dir/dir_0/table row0_ cols_))"
                "(let select1_ (SelectRow '/dc-1/USER_0/dir/dir_1/table row0_ cols_))"
                "(let ret_ (AsList"
                "    (SetResult 'res0_ select0_)"
                "    (SetResult 'res1_ select1_)"
                "))"
                "(return ret_)"
                ")", result);
        }
    }

    void CheckThatDSProxyReturnNoGroupIfTryBlock(TTestActorRuntime* runtime,
                                                 const NKikimrHive::TEvGetTabletStorageInfoResult& storageInfo,
                                                 ui32 nodeIdx = 0) {
        TIntrusivePtr<TTabletStorageInfo> info = TabletStorageInfoFromProto(storageInfo.GetInfo());

        TActorId edge = runtime->AllocateEdgeActor(nodeIdx);
        IActor* x = CreateTabletReqBlockBlobStorage(edge, info.Get(), Max<ui32>(), false);
        runtime->Register(x, nodeIdx);

//        TAutoPtr<IEventHandle> handle;
        TEvTabletBase::TEvBlockBlobStorageResult::TPtr response = runtime->GrabEdgeEventRethrow<TEvTabletBase::TEvBlockBlobStorageResult>(edge);
        Y_ABORT_UNLESS(response->Get()->TabletId == storageInfo.GetTabletID());
        UNIT_ASSERT_EQUAL(response->Get()->Status, NKikimrProto::NO_GROUP);
    }

    void CheckThatDSProxyReturnNoGroupIfCollect(TTestActorRuntime* runtime,
                                                const NKikimrHive::TEvGetTabletStorageInfoResult& storageInfo,
                                                ui32 nodeIdx = 0) {
        auto& info = storageInfo.GetInfo();
        auto& channel = info.GetChannels(0);
        auto& lastEntry = *channel.GetHistory().rbegin();
        ui32 group = lastEntry.GetGroupID();

        TActorId edge = runtime->AllocateEdgeActor(nodeIdx);

        const ui32 generation = Max<ui32>();
        auto event = TEvBlobStorage::TEvCollectGarbage::CreateHardBarrier(
                    storageInfo.GetTabletID(),        // tabletId
                    generation,                       // recordGeneration
                    generation,                       // perGenerationCounter
                    channel.GetChannel(),             // channel
                    generation,                       // collectGeneration
                    std::numeric_limits<ui32>::max(), // collectStep
                    TInstant::Max());                 // deadline

        TActorId nodeWarden = MakeBlobStorageNodeWardenID(runtime->GetNodeId(nodeIdx));
        runtime->Send(new IEventHandle(MakeBlobStorageProxyID(group), edge, event.Release(),
                                       IEventHandle::FlagForwardOnNondelivery, 0, &nodeWarden), nodeIdx);

        TEvBlobStorage::TEvCollectGarbageResult::TPtr response = runtime->GrabEdgeEventRethrow<TEvBlobStorage::TEvCollectGarbageResult>(edge);
        Y_ABORT_UNLESS(response->Get()->TabletId == storageInfo.GetTabletID());
        UNIT_ASSERT_EQUAL(response->Get()->Status, NKikimrProto::NO_GROUP);
    }
    ui32 GetNodeIdx(TBaseTestEnv &env, ui32 nodeId) {
        ui32 nodeIdx = 0;
        for (ui32 idx = 0; idx < env.GetRuntime().GetNodeCount(); ++idx) {
            if (env.GetRuntime().GetNodeId(idx) == nodeId) {
                nodeIdx = idx;
                break;
            }
        }
        return nodeIdx;
    }

    Y_UNIT_TEST(RemoveStoragePoolBeforeDroppingTablet) {
        TTestEnvWithPoolsSupport env(1, 2);
        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        auto storagePools = env.CreatePoolsForTenant("USER_0");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0")));
        env.GetTenants().Run("/dc-1/USER_0", 1);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0", storagePools)));
        env.GetTenants().Stop("/dc-1/USER_0");

        auto domaindescr = NTestLs::ExtractDomainDescription(env.GetClient().Ls("/dc-1/USER_0"));

        UNIT_ASSERT_VALUES_UNEQUAL(1, domaindescr.GetDomainKey().GetPathId());
        ui64 coordinator = domaindescr.GetProcessingParams().GetCoordinators(0);

        UNIT_ASSERT(env.GetClient().TabletExistsInHive(&env.GetRuntime(), coordinator));

        NKikimrHive::TEvGetTabletStorageInfoResult storageInfo;
        env.GetClient().GetTabletStorageInfoFromHive(&env.GetRuntime(), coordinator, storageInfo);
        UNIT_ASSERT_VALUES_EQUAL(storageInfo.GetStatus(), NKikimrProto::OK);

        CheckThatAllChannelsIsRightStoragePools(storageInfo, storagePools);

        for (auto& pool: storagePools) {
            env.GetClient().RemoveStoragePool(pool.GetName());
        }

        CheckThatDSProxyReturnNoGroupIfTryBlock(&env.GetRuntime(), storageInfo);
        CheckThatDSProxyReturnNoGroupIfCollect(&env.GetRuntime(), storageInfo);

        // there is no chance to run or boot it, no groups
        CheckTableIsOfline(env, coordinator);

        //activate dynamic nodes and ask dsproxy from them
        env.GetTenants().Run("/dc-1/USER_0", 1);

        //ask another dsproxy
        CheckThatDSProxyReturnNoGroupIfTryBlock(&env.GetRuntime(), storageInfo, 1);
        CheckThatDSProxyReturnNoGroupIfCollect(&env.GetRuntime(), storageInfo, 1);

        CheckTableIsOfline(env, coordinator);

        env.GetTenants().Stop();
        //delete subdomain with tablets
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteSubdomain("/dc-1/", "USER_0"));

        //tablet deleted logicaly
        while (env.GetClient().TabletExistsInHive(&env.GetRuntime(), coordinator)) {
            /*no op*/
        }

        //subscribe untill tablet deleted physically
        env.GetClient().GetTabletStorageInfoFromHive(&env.GetRuntime(), coordinator, storageInfo);
        UNIT_ASSERT_VALUES_EQUAL(storageInfo.GetStatus(), NKikimrProto::ERROR);

        //check that tablet is deleted physically
        UNIT_ASSERT(!env.GetClient().TabletExistsInHive(&env.GetRuntime(), coordinator, true));
    }

    Y_UNIT_TEST(RemoveStoragePoolAndCreateOneMore) {
        TTestEnvWithPoolsSupport env(1, 2);
        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        {// run and delete USER_0's pools
            auto storagePools = env.CreatePoolsForTenant("USER_0");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                     env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0")));
            env.GetTenants().Run("/dc-1/USER_0", 1);
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                     env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0", storagePools)));
            env.GetTenants().Stop("/dc-1/USER_0");
            auto domaindescr = NTestLs::ExtractDomainDescription(env.GetClient().Ls("/dc-1/USER_0"));

            UNIT_ASSERT_VALUES_UNEQUAL(1, domaindescr.GetDomainKey().GetPathId());
            ui64 coordinator = domaindescr.GetProcessingParams().GetCoordinators(0);
            UNIT_ASSERT(env.GetClient().TabletExistsInHive(&env.GetRuntime(), coordinator));

            NKikimrHive::TEvGetTabletStorageInfoResult storageInfo;
            env.GetClient().GetTabletStorageInfoFromHive(&env.GetRuntime(), coordinator, storageInfo);
            UNIT_ASSERT_VALUES_EQUAL(storageInfo.GetStatus(), NKikimrProto::OK);

            CheckThatAllChannelsIsRightStoragePools(storageInfo, storagePools);
            env.GetTenants().Run("/dc-1/USER_0", 1);
            CheckTableBecomeAlive(env, coordinator);

            for (auto& pool: storagePools) {
                env.GetClient().RemoveStoragePool(pool.GetName());
            }

            CheckTableBecomeOfline(env, coordinator);
        }

        {// run and check USER_1
            auto storagePools = env.CreatePoolsForTenant("USER_1");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                     env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_1")));
            env.GetTenants().Run("/dc-1/USER_1", 1);
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                     env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_1", storagePools)));
            env.GetTenants().Stop("/dc-1/USER_1");
            auto domaindescr = NTestLs::ExtractDomainDescription(env.GetClient().Ls("/dc-1/USER_1"));

            UNIT_ASSERT_VALUES_UNEQUAL(1, domaindescr.GetDomainKey().GetPathId());
            auto coordinator = domaindescr.GetProcessingParams().GetCoordinators(0);
            UNIT_ASSERT(env.GetClient().TabletExistsInHive(&env.GetRuntime(), coordinator));

            NKikimrHive::TEvGetTabletStorageInfoResult storageInfo;
            env.GetClient().GetTabletStorageInfoFromHive(&env.GetRuntime(), coordinator, storageInfo);
            UNIT_ASSERT_VALUES_EQUAL(storageInfo.GetStatus(), NKikimrProto::OK);

            CheckThatAllChannelsIsRightStoragePools(storageInfo, storagePools);
            env.GetTenants().Run("/dc-1/USER_1", 1);
            CheckTableBecomeAlive(env, coordinator);
        }
    }

    Y_UNIT_TEST(CopyTableAndConcurrentSplit) {
        TTestEnvWithPoolsSupport env(1, 2);

        SetSplitMergePartCountLimit(env.GetServer().GetRuntime(), -1);

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        auto storagePools = env.CreatePoolsForTenant("USER_0");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0")));
        env.GetTenants().Run("/dc-1/USER_0", 1);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0", storagePools)));

        auto tableDesc = GetTableSimpleDescription("SimpleTable");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateTable("/dc-1/USER_0", tableDesc));


        auto tablePortionsBefireSplit = NTestLs::ExtractTablePartitions(env.GetClient().Ls("/dc-1/USER_0/SimpleTable"));
        auto firstDataShardBefireSplit = tablePortionsBefireSplit.front().GetDatashardId();

        {
            UNIT_ASSERT(env.GetClient().TabletExistsInHive(&env.GetRuntime(), firstDataShardBefireSplit));

            NKikimrHive::TEvGetTabletStorageInfoResult storageInfo;
            env.GetClient().GetTabletStorageInfoFromHive(&env.GetRuntime(), firstDataShardBefireSplit, storageInfo);
            UNIT_ASSERT_VALUES_EQUAL(storageInfo.GetStatus(), NKikimrProto::OK);

            CheckThatAllChannelsIsRightStoragePools(storageInfo, storagePools);
        }

        env.GetClient().SplitTable("/dc-1/USER_0/SimpleTable", firstDataShardBefireSplit, 1000000, TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CopyTable("/dc-1/USER_0/", "copy", "/dc-1/USER_0/SimpleTable"));

        auto tablePortionsAfterSplit = NTestLs::ExtractTablePartitions(env.GetClient().Ls("/dc-1/USER_0/SimpleTable"));
        auto firstDataShardAfterSplit = tablePortionsAfterSplit.front().GetDatashardId();

        UNIT_ASSERT_VALUES_UNEQUAL(firstDataShardBefireSplit, firstDataShardAfterSplit);

        {
            UNIT_ASSERT(env.GetClient().TabletExistsInHive(&env.GetRuntime(), firstDataShardAfterSplit));

            NKikimrHive::TEvGetTabletStorageInfoResult storageInfo;
            env.GetClient().GetTabletStorageInfoFromHive(&env.GetRuntime(), firstDataShardAfterSplit, storageInfo);
            UNIT_ASSERT_VALUES_EQUAL(storageInfo.GetStatus(), NKikimrProto::OK);

            CheckThatAllChannelsIsRightStoragePools(storageInfo, storagePools);
        }

        auto copyTablePortions = NTestLs::ExtractTablePartitions(env.GetClient().Ls("/dc-1/USER_0/copy"));
        for (NKikimrSchemeOp::TTablePartition& portion: copyTablePortions) {
            auto tabletId = portion.GetDatashardId();

            UNIT_ASSERT(env.GetClient().TabletExistsInHive(&env.GetRuntime(), tabletId));

            NKikimrHive::TEvGetTabletStorageInfoResult storageInfo;
            env.GetClient().GetTabletStorageInfoFromHive(&env.GetRuntime(), tabletId, storageInfo);
            UNIT_ASSERT_VALUES_EQUAL(storageInfo.GetStatus(), NKikimrProto::OK);

            CheckThatAllChannelsIsRightStoragePools(storageInfo, storagePools);
        }
    }
}
