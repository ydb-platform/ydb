#include "proxy_ut_helpers.h"

#include <ydb/core/base/path.h>
#include <ydb/library/aclib/aclib.h>

using namespace NKikimr;
using namespace NTxProxyUT;
using namespace NHelpers;

void DeclareAndLs(TTestEnv& env) {
    {
        auto subdomain = GetSubDomainDeclareSetting("USER_0");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateExtSubdomain("/dc-1", subdomain));
    }

    {
        auto ls = env.GetClient().Ls("/dc-1/USER_0");
        NTestLs::IsUnavailable(ls);
    }

    {
        auto ls = env.GetClient().Ls(env.GetRoot());
        auto ver = NTestLs::ExtractPathVersion(ls);
        UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
        UNIT_ASSERT_VALUES_EQUAL(ver.Version, 5);
    }
}


void DeclareAndDrop(TTestEnv& env) {
    {
        auto subdomain = GetSubDomainDeclareSetting("USER_0");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateExtSubdomain("/dc-1", subdomain));
    }

    {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteSubdomain("/dc-1", "USER_0"));
    }

    {
        auto ls = env.GetClient().Ls(env.GetRoot());
        auto ver = NTestLs::ExtractPathVersion(ls);
        UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
        UNIT_ASSERT_VALUES_EQUAL(ver.Version, 7);
    }

}

void DeclareAndDefineWithoutNodes(TTestEnvWithPoolsSupport& env) {
    {
        auto subdomain = GetSubDomainDeclareSetting("USER_0");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateExtSubdomain("/dc-1", subdomain));
    }

    {
        auto storagePool = env.CreatePoolsForTenant("USER_0");
        auto subdomain = GetSubDomainDefaultSetting("USER_0", storagePool);
        subdomain.SetExternalSchemeShard(true);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS, env.GetClient().AlterExtSubdomain("/dc-1", subdomain, TDuration::MilliSeconds(500)));
    }

    {
        auto ls = env.GetClient().Ls("/dc-1/USER_0");
        NTestLs::IsUnavailable(ls);
    }

    {
        auto ls = env.GetClient().Ls(env.GetRoot());
        auto ver = NTestLs::ExtractPathVersion(ls);
        UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
        UNIT_ASSERT_VALUES_EQUAL(ver.Version, 5);
    }
}

void DeclareAndDefineWithNodes(TTestEnvWithPoolsSupport& env) {
    {
        auto subdomain = GetSubDomainDeclareSetting("USER_0");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateExtSubdomain("/dc-1", subdomain));
    }

    env.GetTenants().Run("/dc-1/USER_0");

    {
        auto storagePool = env.CreatePoolsForTenant("USER_0");
        auto subdomain = GetSubDomainDefaultSetting("USER_0", storagePool);
        subdomain.SetExternalSchemeShard(true);
        subdomain.SetExternalHive(true);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().AlterExtSubdomain("/dc-1", subdomain, WaitTimeOut));
    }

    {
        auto ls = env.GetClient().Ls("/dc-1/USER_0");
        NTestLs::IsExtSubdomain(ls); //root TSS
        auto ver = NTestLs::ExtractPathVersion(ls);
        UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
        UNIT_ASSERT_VALUES_EQUAL(ver.Version, (env.GetSettings().FeatureFlags.GetEnableAlterDatabaseCreateHiveFirst() ? 5 : 4));
    }

    {
        auto ls = env.GetClient().Ls(env.GetRoot());
        auto ver = NTestLs::ExtractPathVersion(ls);
        UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
        UNIT_ASSERT_VALUES_EQUAL(ver.Version, 5);
    }
}

void CreateTableInsideAndLs(TTestEnvWithPoolsSupport& env) {
    ui64 rootSchemeShard = Tests::ChangeStateStorage(Tests::SchemeRoot, 1);

    {
        auto subdomain_0 = GetSubDomainDeclareSetting("USER_0");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateExtSubdomain("/dc-1", subdomain_0));
    }

    env.GetTenants().Run("/dc-1/USER_0");

    {
        auto storagePool = env.CreatePoolsForTenant("USER_0");
        auto subdomain = GetSubDomainDefaultSetting("USER_0", storagePool);
        subdomain.SetExternalSchemeShard(true);
        subdomain.SetExternalHive(true);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().AlterExtSubdomain("/dc-1", subdomain));

        auto ls = env.GetClient().Ls("/dc-1/USER_0");
        NTestLs::IsExtSubdomain(ls); //root TSS
        auto ver = NTestLs::ExtractPathVersion(ls);
        UNIT_ASSERT_VALUES_UNEQUAL(ver.OwnerId, rootSchemeShard);
        UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
        UNIT_ASSERT_VALUES_EQUAL(ver.Version, (env.GetSettings().FeatureFlags.GetEnableAlterDatabaseCreateHiveFirst() ? 5 : 4));
    }

    {
        auto tableDesc = GetTableSimpleDescription("SimpleTable");
        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_0", tableDesc),
                                 NMsgBusProxy::MSTATUS_OK);

        auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
        NTestLs::IsTable(ls);
        auto ver = NTestLs::ExtractPathVersion(ls);
        UNIT_ASSERT_VALUES_UNEQUAL(ver.OwnerId, rootSchemeShard);
        UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
        UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
    }

    {
        auto ls = env.GetClient().Ls("/dc-1/USER_0");
        auto ver = NTestLs::ExtractPathVersion(ls);
        UNIT_ASSERT_VALUES_UNEQUAL(ver.OwnerId, rootSchemeShard);
        UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
        UNIT_ASSERT_VALUES_EQUAL(ver.Version, (env.GetSettings().FeatureFlags.GetEnableAlterDatabaseCreateHiveFirst() ? 7 : 6));
    }

    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteSubdomain("/dc-1", "USER_0"));

    {
        auto ls = env.GetClient().Ls("/dc-1");
        auto ver = NTestLs::ExtractPathVersion(ls);
        UNIT_ASSERT_VALUES_EQUAL(ver.OwnerId, rootSchemeShard);
        UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
        UNIT_ASSERT_VALUES_EQUAL(ver.Version, 7);
    }
}

void DeclareAndAlterPools(TTestEnvWithPoolsSupport& env) {
    auto storagePool = env.CreatePoolsForTenant("USER_0");

    UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

    {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateExtSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0")));

        auto ls = env.GetClient().Ls("/dc-1/USER_0");
        NTestLs::IsUnavailable(ls); //root TSS not ready yet
        ls = env.GetClient().Ls("/dc-1");
        NTestLs::HasChild(ls, "USER_0", NKikimrSchemeOp::EPathTypeExtSubDomain);
    }

    {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterExtSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0", storagePool)));

        auto ls = env.GetClient().Ls("/dc-1/USER_0");
        NTestLs::IsUnavailable(ls); //root TSS not ready yet
        ls = env.GetClient().Ls("/dc-1");
        NTestLs::HasChild(ls, "USER_0", NKikimrSchemeOp::EPathTypeExtSubDomain);
    }

    env.GetTenants().Run("/dc-1/USER_0");

    {
        auto subdomain = GetSubDomainDefaultSetting("USER_0");
        subdomain.SetExternalSchemeShard(true);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().AlterExtSubdomain("/dc-1", subdomain));

        auto ls = env.GetClient().Ls("/dc-1/USER_0");
        NTestLs::IsExtSubdomain(ls); //root TSS ready
        NTestLs::WithPools(ls);
    }

    {
        auto tableDesc = GetTableSimpleDescription("SimpleTable");
        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_0", tableDesc),
                                 NMsgBusProxy::MSTATUS_OK);

        auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
        NTestLs::IsTable(ls);
        auto ver = NTestLs::ExtractPathVersion(ls);
        UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
        UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
    }

    env.GetTenants().Stop("/dc-1/USER_0");

    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteSubdomain("/dc-1", "USER_0"));
}

void CreateTableInsideThenStopTenantAndForceDeleteSubDomain(TTestEnvWithPoolsSupport& env) {
    ui64 rootSchemeShard = Tests::ChangeStateStorage(Tests::SchemeRoot, 1);
    auto storagePool = env.CreatePoolsForTenant("USER_0");
    const ui32 triesNum = 3;
    TVector<ui64> schemeshards(triesNum, 0);
    for (ui32 x = 0; x < triesNum; ++x) {
        {
            auto subdomain_0 = GetSubDomainDeclareSetting("USER_0");
            subdomain_0.SetExternalHive(true);
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateExtSubdomain("/dc-1", subdomain_0));

            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsUnavailable(ls);  // extsubdomain is not ready yet

            ls = env.GetClient().Ls("/dc-1");
            NTestLs::ChildrenCount(ls, 1);
            NTestLs::HasChild(ls, "USER_0", NKikimrSchemeOp::EPathTypeExtSubDomain);
        }

        env.GetTenants().Run("/dc-1/USER_0");

        {
            auto subdomain = GetSubDomainDefaultSetting("USER_0", storagePool);
            subdomain.SetExternalHive(true);
            subdomain.SetExternalSchemeShard(true);
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().AlterExtSubdomain("/dc-1", subdomain));

            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsExtSubdomain(ls);  // extsubdomain's SS is ready
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_UNEQUAL(ver.OwnerId, rootSchemeShard);
            schemeshards[x] = ver.OwnerId;
            if (x) {
                UNIT_ASSERT_VALUES_UNEQUAL(schemeshards[x], schemeshards[x-1]);
            }
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, (env.GetSettings().FeatureFlags.GetEnableAlterDatabaseCreateHiveFirst() ? 5 : 4));
        }

        {
            auto tableDesc = GetTableSimpleDescription("SimpleTable");
            UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_0", tableDesc),
                                     NMsgBusProxy::MSTATUS_OK);

            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0/SimpleTable");
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NTestLs::PathType(ls, NKikimrSchemeOp::EPathTypeTable);
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.OwnerId, schemeshards[x]);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
        }

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsExtSubdomain(ls);
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.OwnerId, schemeshards[x]);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, (env.GetSettings().FeatureFlags.GetEnableAlterDatabaseCreateHiveFirst() ? 7 : 6));


            ls = env.GetClient().Ls("/dc-1");
            NTestLs::ChildrenCount(ls, 1);
            NTestLs::HasChild(ls, "USER_0", NKikimrSchemeOp::EPathTypeExtSubDomain);
        }

        env.GetTenants().Stop();

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteSubdomain("/dc-1", "USER_0"));

        {
            auto ls = env.GetClient().Ls("/dc-1");
            //NOTE: no need to check children count because extsubdomain root path
            // could still exist technically but in a state of being deleted --
            // -- all that counts is that path status: should be DoesNotExist
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.OwnerId, rootSchemeShard);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 7 + x * 4);

            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0");
            ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsDoesNotExist(ls); // extsubdomain root deleted
        }
    }
}

void CreateTableInsideAndDeleteTable(TTestEnvWithPoolsSupport& env) {
    ui64 rootSchemeShard = Tests::ChangeStateStorage(Tests::SchemeRoot, 1);
    auto storagePool = env.CreatePoolsForTenant("USER_0");
    const ui32 triesNum = 3;
    TVector<ui64> schemeshards(triesNum, 0);
    for (ui32 x = 0; x < triesNum; ++x) {
        {
            auto subdomain_0 = GetSubDomainDeclareSetting("USER_0");
            subdomain_0.SetExternalHive(true);
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateExtSubdomain("/dc-1", subdomain_0));

            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsUnavailable(ls); //root TSS not ready yet

            ls = env.GetClient().Ls("/dc-1");
            NTestLs::ChildrenCount(ls, 1);
            NTestLs::HasChild(ls, "USER_0", NKikimrSchemeOp::EPathTypeExtSubDomain);
        }

        env.GetTenants().Run("/dc-1/USER_0");

        {
            auto subdomain = GetSubDomainDefaultSetting("USER_0", storagePool);
            subdomain.SetExternalHive(true);
            subdomain.SetExternalSchemeShard(true);
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().AlterExtSubdomain("/dc-1", subdomain));

            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsExtSubdomain(ls); //root TSS ready
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_UNEQUAL(ver.OwnerId, rootSchemeShard);
            schemeshards[x] = ver.OwnerId;
            if (x) {
                UNIT_ASSERT_VALUES_UNEQUAL(schemeshards[x], schemeshards[x-1]);
            }
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 4);
        }

        {
            auto tableDesc = GetTableSimpleDescription("SimpleTable");
            UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_0", tableDesc),
                                     NMsgBusProxy::MSTATUS_OK);

            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0/SimpleTable");
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NTestLs::PathType(ls, NKikimrSchemeOp::EPathTypeTable);
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.OwnerId, schemeshards[x]);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);

            UNIT_ASSERT_VALUES_EQUAL(env.GetClient().DeleteTable("/dc-1/USER_0", "SimpleTable"),
                                     NMsgBusProxy::MSTATUS_OK);

            auto shards = NTestLs::ExtractTablePartitions(ls);
            for (auto shard : shards) {
                bool success = env.GetClient().WaitForTabletDown(&env.GetRuntime(), shard.GetDatashardId(), true, TDuration::Seconds(60));
                UNIT_ASSERT(success);
            }

        }
        {
            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0/SimpleTable");
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NTestLs::IsDoesNotExist(ls);
        }

        env.GetTenants().Stop();

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteSubdomain("/dc-1", "USER_0"));

        {
            auto ls = env.GetClient().Ls("/dc-1");
            NTestLs::NoChildren(ls);
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.OwnerId, rootSchemeShard);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 7 + x * 4);

            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0");
            ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsDoesNotExist(ls); //root TSS deleted
        }
    }
}

void CreateTableInsideAndAlterTable(TTestEnvWithPoolsSupport& env) {
    ui64 rootSchemeShard = Tests::ChangeStateStorage(Tests::SchemeRoot, 1);
    auto storagePool = env.CreatePoolsForTenant("USER_0");
    const ui32 triesNum = 3;
    TVector<ui64> schemeshards(triesNum, 0);
    for (ui32 x = 0; x < triesNum; ++x) {
        {
            auto subdomain_0 = GetSubDomainDeclareSetting("USER_0");
            subdomain_0.SetExternalHive(true);
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateExtSubdomain("/dc-1", subdomain_0));

            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsUnavailable(ls); //root TSS not ready yet

            ls = env.GetClient().Ls("/dc-1");
            NTestLs::ChildrenCount(ls, 1);
            NTestLs::HasChild(ls, "USER_0", NKikimrSchemeOp::EPathTypeExtSubDomain);
        }

        env.GetTenants().Run("/dc-1/USER_0");

        {
            auto subdomain = GetSubDomainDefaultSetting("USER_0", storagePool);
            subdomain.SetExternalHive(true);
            subdomain.SetExternalSchemeShard(true);
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().AlterExtSubdomain("/dc-1", subdomain));

            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsExtSubdomain(ls); //root TSS ready
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_UNEQUAL(ver.OwnerId, rootSchemeShard);
            schemeshards[x] = ver.OwnerId;
            if (x) {
                UNIT_ASSERT_VALUES_UNEQUAL(schemeshards[x], schemeshards[x-1]);
            }
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 4);
        }

        {
            auto tableDesc = GetTableSimpleDescription("SimpleTable");
            UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_0", tableDesc),
                                     NMsgBusProxy::MSTATUS_OK);

            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0/SimpleTable");
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NTestLs::PathType(ls, NKikimrSchemeOp::EPathTypeTable);
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.OwnerId, schemeshards[x]);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);

            NKikimrSchemeOp::TTableDescription description = NTestLs::ExtractTableDescription(ls);

            description.ClearColumns();
            description.ClearKeyColumnIds();
            description.ClearKeyColumnNames();
            description.ClearSplitBoundary();
            description.ClearTableIndexes();
            description.MutablePartitionConfig()->SetFollowerCount(1);

            UNIT_ASSERT_VALUES_EQUAL(env.GetClient().AlterTable("/dc-1/USER_0", description),
                                     NMsgBusProxy::MSTATUS_OK);
        }
        {
            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0/SimpleTable");
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NKikimrSchemeOp::TTableDescription description = NTestLs::ExtractTableDescription(ls);
            UNIT_ASSERT_VALUES_EQUAL(description.GetPartitionConfig().GetFollowerCount(), 1);
        }

        env.GetTenants().Stop();

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteSubdomain("/dc-1", "USER_0"));

        {
            auto ls = env.GetClient().Ls("/dc-1");
            NTestLs::NoChildren(ls);
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.OwnerId, rootSchemeShard);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 7 + x * 4);

            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0");
            ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsDoesNotExist(ls); //root TSS deleted
        }
    }
}


void CreateTableInsideAndAlterDomainAndTable(TTestEnvWithPoolsSupport& env) {
    ui64 rootSchemeShard = Tests::ChangeStateStorage(Tests::SchemeRoot, 1);
    auto storagePool = env.CreatePoolsForTenant("USER_0");
    const ui32 triesNum = 3;
    TVector<ui64> schemeshards(triesNum, 0);
    for (ui32 x = 0; x < triesNum; ++x) {
        {
            auto subdomain_0 = GetSubDomainDeclareSetting("USER_0");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateExtSubdomain("/dc-1", subdomain_0));

            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsUnavailable(ls);  // extsubdomain is not ready yet

            ls = env.GetClient().Ls("/dc-1");
            NTestLs::ChildrenCount(ls, 1);
            NTestLs::HasChild(ls, "USER_0", NKikimrSchemeOp::EPathTypeExtSubDomain);
        }

        env.GetTenants().Run("/dc-1/USER_0");

        {
            auto subdomain = GetSubDomainDefaultSetting("USER_0", storagePool);
            subdomain.SetExternalSchemeShard(true);
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().AlterExtSubdomain("/dc-1", subdomain));

            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsExtSubdomain(ls);  // extsubdomain's SS is ready
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_UNEQUAL(ver.OwnerId, rootSchemeShard);
            schemeshards[x] = ver.OwnerId;
            if (x) {
                UNIT_ASSERT_VALUES_UNEQUAL(schemeshards[x], schemeshards[x-1]);
            }
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 4);
        }

        {
            auto tableDesc = GetTableSimpleDescription("SimpleTable");
            tableDesc.MutablePartitionConfig()->SetFollowerCount(0);

            UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_0", tableDesc),
                                     NMsgBusProxy::MSTATUS_OK);

            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0/SimpleTable");
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NTestLs::PathType(ls, NKikimrSchemeOp::EPathTypeTable);
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.OwnerId, schemeshards[x]);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
        }
        {
            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0/SimpleTable");
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NKikimrSchemeOp::TTableDescription description = NTestLs::ExtractTableDescription(ls);
            UNIT_ASSERT_VALUES_EQUAL(description.GetPartitionConfig().GetFollowerCount(), 0);
        }

        {
            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0");
            auto ls = env.GetClient().Ls("/dc-1/USER_0");

            NKikimrSubDomains::TDomainDescription description = NTestLs::ExtractDomainDescription(ls);
            UNIT_ASSERT_VALUES_EQUAL(description.GetProcessingParams().GetHive(), 0);

            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 6);
        }

        {
            auto subdomain = GetSubDomainDefaultSetting("USER_0", storagePool);
            subdomain.SetExternalSchemeShard(true);
            subdomain.SetExternalHive(true);
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().AlterExtSubdomain("/dc-1", subdomain));

            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsExtSubdomain(ls);  // extsubdomain's SS is ready
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_UNEQUAL(ver.OwnerId, rootSchemeShard);
            schemeshards[x] = ver.OwnerId;
            if (x) {
                UNIT_ASSERT_VALUES_UNEQUAL(schemeshards[x], schemeshards[x-1]);
            }
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, (env.GetSettings().FeatureFlags.GetEnableAlterDatabaseCreateHiveFirst() ? 8 : 7));

            NKikimrSubDomains::TDomainDescription description = NTestLs::ExtractDomainDescription(ls);
            UNIT_ASSERT_VALUES_UNEQUAL(description.GetProcessingParams().GetHive(), 0);
        }

        {
            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0/SimpleTable");
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NTestLs::PathType(ls, NKikimrSchemeOp::EPathTypeTable);
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.OwnerId, schemeshards[x]);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);


            NKikimrSchemeOp::TTableDescription description = NTestLs::ExtractTableDescription(ls);

            description.ClearColumns();
            description.ClearKeyColumnIds();
            description.ClearKeyColumnNames();
            description.ClearSplitBoundary();
            description.ClearTableIndexes();
            description.MutablePartitionConfig()->SetFollowerCount(1);

            UNIT_ASSERT_VALUES_EQUAL(env.GetClient().AlterTable("/dc-1/USER_0", description),
                                     NMsgBusProxy::MSTATUS_OK);
        }
        {
            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0/SimpleTable");
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NKikimrSchemeOp::TTableDescription description = NTestLs::ExtractTableDescription(ls);
            UNIT_ASSERT_VALUES_EQUAL(description.GetPartitionConfig().GetFollowerCount(), 1);
        }

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteSubdomain("/dc-1", "USER_0"));

        {
            auto ls = env.GetClient().Ls("/dc-1");
            //NOTE: no need to check children count because extsubdomain root path
            // could still exist technically but in a state of being deleted --
            // -- all that counts is that path status: should be DoesNotExist
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.OwnerId, rootSchemeShard);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 7 + x * 4);

            env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0");
            ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::IsDoesNotExist(ls); // extsubdomain root deleted
        }

        env.GetTenants().Stop();
    }
}

void GenericCases(TTestEnvWithPoolsSupport& env) {
    UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                             env.GetClient().CreateExtSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0")));
    env.GetTenants().Run("/dc-1/USER_0");

    auto storagePools = env.CreatePoolsForTenant("USER_0");
    auto alter = GetSubDomainDefaultSetting("USER_0", storagePools);
    alter.SetExternalSchemeShard(true);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                             env.GetClient().AlterExtSubdomain("/dc-1", alter, WaitTimeOut));

    {
        auto ls = env.GetClient().Ls("/dc-1/USER_0");
        NTestLs::IsExtSubdomain(ls);
    }

    {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().MkDir("/dc-1/USER_0", "dir"));
        auto ls = env.GetClient().Ls("/dc-1/USER_0/dir");
        NTestLs::IsDir(ls);
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
        NTestLs::IsDir(ls);
    }

    {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().MkDir("/dc-1/USER_0/dir", "dir_1"));
        auto ls = env.GetClient().Ls("/dc-1/USER_0/dir/dir_1");
        NTestLs::IsDir(ls);
    }

    {
        auto tableDesc = GetTableSimpleDescription("table");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateTable("/dc-1/USER_0/dir/dir_0", tableDesc));
        auto ls = env.GetClient().Ls("/dc-1/USER_0/dir/dir_0/table");
        NTestLs::IsTable(ls);
    }

    {
        auto tableDesc = GetTableSimpleDescription("table");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateTable("/dc-1/USER_0/dir/dir_1", tableDesc));
        auto ls = env.GetClient().Ls("/dc-1/USER_0/dir/dir_1/table");
        NTestLs::IsTable(ls);
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

Y_UNIT_TEST_SUITE(TExtSubDomainTest) {
    Y_UNIT_TEST(DeclareAndLs) {
        TTestEnv env(1, 0);
        DeclareAndLs(env);
    }

    Y_UNIT_TEST(DeclareAndDrop) {
        TTestEnv env(1, 0);
        DeclareAndDrop(env);
    }

    Y_UNIT_TEST_FLAG(DeclareAndDefineWithoutNodes, AlterDatabaseCreateHiveFirst) {
        TTestEnvWithPoolsSupport env(1, 0, 2, AlterDatabaseCreateHiveFirst);
        DeclareAndDefineWithoutNodes(env);
    }

    Y_UNIT_TEST_FLAG(DeclareAndDefineWithNodes, AlterDatabaseCreateHiveFirst) {
        TTestEnvWithPoolsSupport env(1, 1, 2, AlterDatabaseCreateHiveFirst);
        DeclareAndDefineWithNodes(env);
    }

    Y_UNIT_TEST_FLAG(CreateTableInsideAndLs, AlterDatabaseCreateHiveFirst) {
        TTestEnvWithPoolsSupport env(1, 1, 2, AlterDatabaseCreateHiveFirst);
        CreateTableInsideAndLs(env);
    }

    Y_UNIT_TEST_FLAG(DeclareAndAlterPools, AlterDatabaseCreateHiveFirst) {
        TTestEnvWithPoolsSupport env(1, 1, 2, AlterDatabaseCreateHiveFirst);
        DeclareAndAlterPools(env);
    }

    Y_UNIT_TEST_FLAG(CreateTableInsideThenStopTenantAndForceDeleteSubDomain, AlterDatabaseCreateHiveFirst) {
        TTestEnvWithPoolsSupport env(1, 1, 2, AlterDatabaseCreateHiveFirst);
        CreateTableInsideThenStopTenantAndForceDeleteSubDomain(env);
    }

    Y_UNIT_TEST_FLAG(CreateTableInsideAndAlterDomainAndTable, AlterDatabaseCreateHiveFirst) {
        TTestEnvWithPoolsSupport env(1, 1, 2, AlterDatabaseCreateHiveFirst);
        CreateTableInsideAndAlterDomainAndTable(env);
    }

    Y_UNIT_TEST(GenericCases) {
        TTestEnvWithPoolsSupport env(1, 1, 2);
        GenericCases(env);
    }
}
