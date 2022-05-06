#include "proxy_ut_helpers.h"

#include <ydb/core/base/path.h>
#include <ydb/library/aclib/aclib.h>

using namespace NKikimr;
using namespace NTxProxyUT;
using namespace NHelpers;

Y_UNIT_TEST_SUITE(TSubDomainTest) {
    NKikimrSchemeOp::TTableDescription GetTableSimpleDescription(const TString &name) {
        NKikimrSchemeOp::TTableDescription tableDescr;
        tableDescr.SetName(name);
        {
            auto *c1 = tableDescr.AddColumns();
            c1->SetName("key");
            c1->SetType("Uint64");
        }
        {
            auto *c2 = tableDescr.AddColumns();
            c2->SetName("value");
            c2->SetType("Uint64");
        }
        tableDescr.SetUniformPartitionsCount(2);
        tableDescr.MutablePartitionConfig()->SetFollowerCount(2);
        *tableDescr.AddKeyColumnNames() = "key";
        return tableDescr;
    }

    void SetRowInSimpletable(TTestEnv& env, ui64 key, ui64 value, const TString &path) {
        NKikimrMiniKQL::TResult res;
        TString query = Sprintf("("
                                "(let row '('('key (Uint64 '%" PRIu64 "))))"
                                "(let myUpd '("
                                "    '('value (Uint64 '%" PRIu64 "))"
                                "))"
                                "(let pgmReturn (AsList"
                                "    (UpdateRow '%s row myUpd)"
                                "))"
                                "(return pgmReturn)"
                                ")", key, value, path.c_str());

        env.GetClient().FlatQuery(query, res);
    }

    Y_UNIT_TEST(Boot) {
        TTestEnv env;
        auto lsroot = env.GetClient().Ls("/");
        Print(lsroot);

        {
            auto ls = env.GetClient().Ls(env.GetRoot());
            Print(ls);
            NTestLs::Finished(ls);
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
        }
    }

    Y_UNIT_TEST(LsLs) {
        TTestEnv env;

        {
            auto subdomain = GetSubDomainDeclareSetting("USER_0", env.GetPools());
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateSubdomain("/dc-1", subdomain));
        }

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::Finished(ls, NKikimrSchemeOp::EPathTypeSubDomain);
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeSubDomain);
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
        }

        {
            auto ls = env.GetClient().Ls(env.GetRoot());
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 5);
        }
    }

    Y_UNIT_TEST(LsAltered) {
        TTestEnv env;

        {
            auto subdomain = GetSubDomainDeclareSetting("USER_0", env.GetPools());
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateSubdomain("/dc-1", subdomain));
        }

        {
            auto subdomain = GetSubDomainDefaultSetting("USER_0");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS, env.GetClient().AlterSubdomain("/dc-1", subdomain, TDuration::MilliSeconds(500)));
        }

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeSubDomain);
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3); // alter is in progress and isn't finish yet
        }

        {
            auto ls = env.GetClient().Ls(env.GetRoot());
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 5);
        }
    }

    Y_UNIT_TEST(CreateTablet) {
        TTestEnv env(3,2);

        ui64 owner = THash<TString>()("CreateTablet");
        ui64 index = 1;
        TAutoPtr<NMsgBusProxy::TBusResponse> resp = env.GetClient().HiveCreateTablet(
                    env.GetSettings().Domain, owner, index, TTabletTypes::Dummy, {}, {});
        NKikimrClient::TResponse& record = resp->Record;

        UNIT_ASSERT_VALUES_EQUAL(record.CreateTabletResultSize(), 1);
        const auto& result = record.GetCreateTabletResult(0);

        UNIT_ASSERT_EQUAL(
            NKikimrProto::OK,
            result.GetStatus());

        UNIT_ASSERT(
            env.GetClient().TabletExistsInHive(&env.GetRuntime(), result.GetTabletId()));

        UNIT_ASSERT(
             env.GetClient().WaitForTabletAlive(&env.GetRuntime(), result.GetTabletId(), true, WaitTimeOut));
    }

    Y_UNIT_TEST(CreateTabletForUnknownDomain) {
        TTestEnv env;

        ui64 owner = THash<TString>()("CreateTabletForUnknownDomain");
        ui64 index = 1;
        TVector<TSubDomainKey> allowed_domains;
        allowed_domains.push_back(TSubDomainKey(999, 999));

        TAutoPtr<NMsgBusProxy::TBusResponse> resp = env.GetClient().HiveCreateTablet(
                    env.GetSettings().Domain, owner, index, TTabletTypes::Dummy, {}, allowed_domains);
        NKikimrClient::TResponse& record = resp->Record;

        UNIT_ASSERT_VALUES_EQUAL(record.CreateTabletResultSize(), 1);
        const auto& result = record.GetCreateTabletResult(0);

        UNIT_ASSERT_EQUAL(
            NKikimrProto::OK,
            result.GetStatus());

        UNIT_ASSERT(
            env.GetClient().TabletExistsInHive(&env.GetRuntime(), result.GetTabletId()));

        UNIT_ASSERT(
            env.GetClient().WaitForTabletDown(&env.GetRuntime(), result.GetTabletId(), true, WaitTimeOut));

        UNIT_ASSERT_EQUAL(
            Max<ui32>(),
            env.GetClient().GetLeaderNode(&env.GetRuntime(), result.GetTabletId()));
    }

    Y_UNIT_TEST(CreateDummyTabletsInDifferentDomains) {
        TTestEnv env(1, 2);

        ui64 tablet_in_first_domain = CreateSubDomainAndTabletInside(env, "USER_0", 1);
        ui64 tablet_in_second_domain = CreateSubDomainAndTabletInside(env, "USER_1", 2);

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
    }

    Y_UNIT_TEST(StartAndStopTenanNode) {
        TTestEnv env(1, 1);

        auto subdomain_0 = GetSubDomainDeclareSetting("USER_0", env.GetPools());
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateSubdomain("/dc-1", subdomain_0));

        env.GetTenants().Run("/dc-1/USER_0");
        env.GetTenants().Stop();
    }

    Y_UNIT_TEST(CreateTableInsideAndForceDeleteSubDomain) {
        TTestEnv env(1, 1);

        auto subdomain_0 = GetSubDomainDeclareSetting("USER_0", env.GetPools());
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateSubdomain("/dc-1", subdomain_0));

        {
            auto subdomain = GetSubDomainDefaultSetting("USER_0");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS, env.GetClient().AlterSubdomain("/dc-1", subdomain, TDuration::MilliSeconds(500)));

            {
                auto ls = env.GetClient().Ls("/dc-1/USER_0");
                auto ver = NTestLs::ExtractPathVersion(ls);
                UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
                UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
            }
        }

        env.GetTenants().Run("/dc-1/USER_0");

        auto tableDesc = GetTableSimpleDescription("SimpleTable");
        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_0", tableDesc),
                                 NMsgBusProxy::MSTATUS_OK);

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeTable);
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 3);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
        }

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 6);
        }

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteSubdomain("/dc-1", "USER_0"));
    }

    Y_UNIT_TEST(CreateTableInsidetThenStopTenantAndForceDeleteSubDomain) {
        TTestEnv env(1, 1);

        auto subdomain_0 = GetSubDomainDeclareSetting("USER_0", env.GetPools());
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateSubdomain("/dc-1", subdomain_0));

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
        }

        env.GetTenants().Run("/dc-1/USER_0");

        {
            auto subdomain = GetSubDomainDefaultSetting("USER_0");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().AlterSubdomain("/dc-1", subdomain));
        }

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 4);
        }

        auto tableDesc = GetTableSimpleDescription("SimpleTable");
        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_0", tableDesc),
                                 NMsgBusProxy::MSTATUS_OK);

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeTable);
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 3);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
        }

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 6);
        }

        env.GetTenants().Stop();

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteSubdomain("/dc-1", "USER_0"));
    }

    Y_UNIT_TEST(DeleteTableAndThenForceDeleteSubDomain) {
        TTestEnv env(1, 1);

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0", env.GetPools())));

        env.GetTenants().Run("/dc-1/USER_0");

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0")));

        auto tableDesc = GetTableSimpleDescription("SimpleTable");
        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_0", tableDesc),
                                 NMsgBusProxy::MSTATUS_OK);

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeTable);
        }

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().DeleteTable("/dc-1/USER_0", "SimpleTable"));

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteSubdomain("/dc-1", "USER_0"));
    }

    Y_UNIT_TEST(StartTenanNodeAndStopAtDestructor) {
        TTestEnv env(1, 1);

        auto subdomain_0 = GetSubDomainDeclareSetting("USER_0", env.GetPools());
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateSubdomain("/dc-1", subdomain_0));

        env.GetTenants().Run("/dc-1/USER_0");
    }

    Y_UNIT_TEST(CreateTableInsideSubDomain) {
        TTestEnv env(1, 1);

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0", env.GetPools())));

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
        }

        env.GetTenants().Run("/dc-1/USER_0");

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0")));

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 4);
        }

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateTable("/dc-1/USER_0", GetTableSimpleDescription("SimpleTable")));

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 6);
        }

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeTable);
        }
    }

    Y_UNIT_TEST(FailIfAffectedSetNotInterior) {
        TTestEnv env(1, 2);

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0", env.GetPools())));
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0"), TDuration::MilliSeconds(500)));
        env.GetTenants().Run("/dc-1/USER_0");

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_1", env.GetPools())));
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_1"), TDuration::MilliSeconds(500)));
        env.GetTenants().Run("/dc-1/USER_1");

        auto tableDesc = GetTableSimpleDescription("SimpleTable");
        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_0", tableDesc),
                                 NMsgBusProxy::MSTATUS_OK);

        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_1", tableDesc),
                                 NMsgBusProxy::MSTATUS_OK);

        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1", tableDesc),
                                 NMsgBusProxy::MSTATUS_OK);

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0/SimpleTable");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeTable);
        }

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_1/SimpleTable");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeTable);
        }

        {
            auto ls = env.GetClient().Ls("/dc-1/SimpleTable");
            NTestLs::NotInSubdomain(ls, NKikimrSchemeOp::EPathTypeTable);
        }

        SetRowInSimpletable(env, 42, 111, "/dc-1/USER_0/SimpleTable");
        SetRowInSimpletable(env, 42, 222, "/dc-1/USER_1/SimpleTable");
        SetRowInSimpletable(env, 42, 333, "/dc-1/SimpleTable");

        {//transation intersect SubDomain and domain
            NKikimrMiniKQL::TResult res;
            Tests::TClient::TFlatQueryOptions opts;

            NKikimrClient::TResponse expectedResponse;
            expectedResponse.SetStatus(NMsgBusProxy::MSTATUS_ERROR);
            expectedResponse.SetProxyErrorCode(NTxProxy::TResultStatus::DomainLocalityError);

            env.GetClient().FlatQuery("("
                "(let row0_ '('('key (Uint64 '42))))"
                "(let cols_ '('value))"
                "(let select0_ (SelectRow '/dc-1/SimpleTable row0_ cols_))"
                "(let select1_ (SelectRow '/dc-1/USER_0/SimpleTable row0_ cols_))"
                "(let ret_ (AsList"
                "    (SetResult 'res0_ select0_)"
                "    (SetResult 'res1_ select1_)"
                "))"
                "(return ret_)"
                ")", opts, res, expectedResponse);
        }

        {//transation intersect SubDomain and another SubDomain
            NKikimrMiniKQL::TResult res;
            Tests::TClient::TFlatQueryOptions opts;

            NKikimrClient::TResponse expectedResponse;
            expectedResponse.SetStatus(NMsgBusProxy::MSTATUS_ERROR);
            expectedResponse.SetProxyErrorCode(NTxProxy::TResultStatus::DomainLocalityError);

            env.GetClient().FlatQuery("("
                "(let row0_ '('('key (Uint64 '42))))"
                "(let cols_ '('value))"
                "(let select0_ (SelectRow '/dc-1/USER_1/SimpleTable row0_ cols_))"
                "(let select1_ (SelectRow '/dc-1/USER_0/SimpleTable row0_ cols_))"
                "(let ret_ (AsList"
                "    (SetResult 'res0_ select0_)"
                "    (SetResult 'res1_ select1_)"
                "))"
                "(return ret_)"
                ")", opts, res, expectedResponse);
        }
    }


    Y_UNIT_TEST(GenericCases) {
        TTestEnv env(1, 1);

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0", env.GetPools())));
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0"), TDuration::MilliSeconds(500)));

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::InSubdomain(ls, NKikimrSchemeOp::EPathTypeSubDomain);
        }

        env.GetTenants().Run("/dc-1/USER_0");

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

    Y_UNIT_TEST(DatashardNotRunAtAllWhenSubDomainNodesIsStopped) {
        TTestEnv env(1, 1);

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0", env.GetPools())));

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0"), TDuration::MilliSeconds(500)));

        env.GetTenants().Run("/dc-1/USER_0");

        //create table
        auto tableDesc = GetTableSimpleDescription("table");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateTable("/dc-1/USER_0", tableDesc));

        auto partitions = NTestLs::ExtractTablePartitions(env.GetClient().Ls("/dc-1/USER_0/table"));
        ui64 somedatashard = partitions.back().GetDatashardId();

        //datashard run on dynamic node
        ui32 nodeId = env.GetClient().GetLeaderNode(&env.GetRuntime(), somedatashard);
        UNIT_ASSERT(env.GetTenants().IsActive("/dc-1/USER_0", nodeId));

        env.GetTenants().Stop("/dc-1/USER_0");
        UNIT_ASSERT(env.GetClient().WaitForTabletDown(&env.GetRuntime(), somedatashard, true, WaitTimeOut));

        // make sure that hive has seen tablet stopped
        ui32 leaderNodeForDataShard = 1;
        while (leaderNodeForDataShard != Max<ui32>()) {
            leaderNodeForDataShard = env.GetClient().GetLeaderNode(&env.GetRuntime(), somedatashard);
        }

        //datashard is not running
        UNIT_ASSERT_VALUES_EQUAL(Max<ui32>(), env.GetClient().GetLeaderNode(&env.GetRuntime(), somedatashard));

        //and there is no hope to run it up
        UNIT_ASSERT(!env.GetClient().WaitForTabletAlive(&env.GetRuntime(), somedatashard, true, WaitTimeOut));
    }

    Y_UNIT_TEST(DatashardRunAtOtherNodeWhenOneNodeIsStopped) {
        TTestEnv env(1, 2);

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0", env.GetPools())));

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0"), TDuration::MilliSeconds(500)));

        env.GetTenants().Run("/dc-1/USER_0", 2);
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetTenants().Size("/dc-1/USER_0"));

        //create table
        auto tableDesc = GetTableSimpleDescription("table");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateTable("/dc-1/USER_0", tableDesc));

        auto partitions = NTestLs::ExtractTablePartitions(env.GetClient().Ls("/dc-1/USER_0/table"));
        ui64 somedatashard = partitions.back().GetDatashardId();

        //datashard run on dynamic node
        ui32 nodeId = env.GetClient().GetLeaderNode(&env.GetRuntime(), somedatashard);
        UNIT_ASSERT(env.GetTenants().IsActive("/dc-1/USER_0", nodeId));

        //switch off the node
        env.GetTenants().FreeNode("/dc-1/USER_0", nodeId);

        //wait while datashards shutdown and wake up at second node
        env.GetClient().WaitForTabletDown(&env.GetRuntime(), somedatashard, true, WaitTimeOut);
        UNIT_ASSERT(env.GetClient().WaitForTabletAlive(&env.GetRuntime(), somedatashard, true, WaitTimeOut));

        ui32 newNodeId = env.GetClient().GetLeaderNode(&env.GetRuntime(), somedatashard);
        UNIT_ASSERT(env.GetTenants().IsActive("/dc-1/USER_0", newNodeId));
    }

    Y_UNIT_TEST(CoordinatorRunAtSubdomainNodeWhenAvailable) {
        TTestEnv env(1, 5);

        {
            auto ls = env.GetClient().Ls("/dc-1");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
        }

        {
            auto subdomain = GetSubDomainDeclareSetting("USER_0", env.GetPools());
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateSubdomain("/dc-1", subdomain));
        }

        {
            auto ls = env.GetClient().Ls("/dc-1");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 5);
        }

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
        }

        env.GetTenants().Run("/dc-1/USER_0", active_tenants_nodes);

        {
            auto subdomain = GetSubDomainDefaultSetting("USER_0");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().AlterSubdomain("/dc-1", subdomain));
        }

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 4);
        }

        auto domaindescr = NTestLs::ExtractDomainDescription(env.GetClient().Ls("/dc-1/USER_0"));

        UNIT_ASSERT_VALUES_UNEQUAL(1, domaindescr.GetDomainKey().GetPathId());
        ui64 coordinator = domaindescr.GetProcessingParams().GetCoordinators(0);

        //coordinator runs on dynamic node
        ui32 nodeId = env.GetClient().GetLeaderNode(&env.GetRuntime(), coordinator);
        UNIT_ASSERT_C(env.GetTenants().IsActive("/dc-1/USER_0", nodeId), "assert IsActive nodeId " << nodeId);

        //create table, if it is OK then tenant node awakes
        auto tableDesc = GetTableSimpleDescription("table");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateTable("/dc-1/USER_0", tableDesc));

        env.GetClient().MarkNodeInHive(&env.GetRuntime(), nodeId, false);
        env.GetClient().KickNodeInHive(&env.GetRuntime(), nodeId);
        env.GetClient().MarkNodeInHive(&env.GetRuntime(), nodeId, true);
        UNIT_ASSERT(env.GetClient().WaitForTabletAlive(&env.GetRuntime(), coordinator, true, WaitTimeOut));

        //coordinator runs on dynamic node
        ui32 newNodeId = env.GetClient().GetLeaderNode(&env.GetRuntime(), coordinator);
        UNIT_ASSERT_C(env.GetTenants().IsActive("/dc-1/USER_0", newNodeId), "assert IsActive nodeId " << nodeId);

        //coordinator no runs on static node, when dynamic is offline
        env.GetTenants().Stop("/dc-1/USER_0");

        UNIT_ASSERT(env.GetClient().WaitForTabletDown(&env.GetRuntime(), coordinator, true, WaitTimeOut));
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteSubdomain("/dc-1", "USER_0"));
        UNIT_ASSERT(!env.GetClient().TabletExistsInHive(&env.GetRuntime(), coordinator, false));
    }

    Y_UNIT_TEST(CoordinatorRunAtSubdomainNodeWhenAvailable2) {
        TTestEnv env(1, 5);

        {
            auto ls = env.GetClient().Ls("/dc-1");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 3);
        }

        {
            auto subdomain = GetSubDomainDeclareSetting("USER_0", env.GetPools());
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateSubdomain("/dc-1", subdomain));
        }

        {
            auto ls = env.GetClient().Ls("/dc-1");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 5);
        }

        {
            auto subdomain = GetSubDomainDefaultSetting("USER_0");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS, env.GetClient().AlterSubdomain("/dc-1", subdomain, TDuration::MilliSeconds(500)));
        }

        {
            auto tableDesc = GetTableSimpleDescription("table");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS, env.GetClient().CreateTable("/dc-1/USER_0", tableDesc, TDuration::Seconds(1)));
        }

        env.GetTenants().Run("/dc-1/USER_0", active_tenants_nodes);

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().WaitCreateTx(&env.GetRuntime(), "/dc-1/USER_0/table", WaitTimeOut));

        {
            auto ls = env.GetClient().Ls("/dc-1");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 1);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 5);
        }

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 6);
        }

        auto domaindescr = NTestLs::ExtractDomainDescription(env.GetClient().Ls("/dc-1/USER_0"));

        UNIT_ASSERT_VALUES_UNEQUAL(1, domaindescr.GetDomainKey().GetPathId());
        ui64 coordinator = domaindescr.GetProcessingParams().GetCoordinators(0);

        //coordinator runs on dynamic node
        ui32 nodeId = env.GetClient().GetLeaderNode(&env.GetRuntime(), coordinator);
        UNIT_ASSERT_C(env.GetTenants().IsActive("/dc-1/USER_0", nodeId), "assert IsActive nodeId " << nodeId);
    }

    Y_UNIT_TEST(UserAttributes) {
        TTestEnv env(1, 0);

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0", env.GetPools())));

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterUserAttributes("/dc-1", "USER_0", {{"AttrA1", "ValA1"}}));

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::HasUserAttributes(ls, {{"AttrA1", "ValA1"}});
        }
    }

    Y_UNIT_TEST(UserAttributesApplyIf) {
        TTestEnv env(1, 0);

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0", env.GetPools())));

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterUserAttributes("/dc-1", "USER_0", {{"AttrA1", "ValA1"}}));

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::HasUserAttributes(ls, {{"AttrA1", "ValA1"}});
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 4);
        }


        auto pathVer = env.GetClient().ExtractPathVersion(env.GetClient().Ls("/dc-1/USER_0"));
        UNIT_ASSERT_VALUES_EQUAL(pathVer.PathId, 2);
        UNIT_ASSERT_VALUES_EQUAL(pathVer.Version, 4);

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterUserAttributes("/dc-1", "USER_0", {{"AttrA2", "ValA2"}}, {"AttrA1"}, {pathVer}));

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::HasUserAttributes(ls, {{"AttrA2", "ValA2"}});
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 5);
        }

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_ERROR,
                                 env.GetClient().AlterUserAttributes("/dc-1", "USER_0", {{"AttrA3", "ValA3"}}, {"AttrA2"}, {pathVer}));

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::HasUserAttributes(ls, {{"AttrA2", "ValA2"}});
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 5);
        }

        pathVer = env.GetClient().ExtractPathVersion(env.GetClient().Ls("/dc-1/USER_0"));
        UNIT_ASSERT_VALUES_EQUAL(pathVer.PathId, 2);
        UNIT_ASSERT_VALUES_EQUAL(pathVer.Version, 5);

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterUserAttributes("/dc-1", "USER_0", {{"AttrA3", "ValA3"}}, {"AttrA2"}, {pathVer}));

        {
            auto ls = env.GetClient().Ls("/dc-1/USER_0");
            NTestLs::HasUserAttributes(ls, {{"AttrA3", "ValA3"}});
            auto ver = NTestLs::ExtractPathVersion(ls);
            UNIT_ASSERT_VALUES_EQUAL(ver.PathId, 2);
            UNIT_ASSERT_VALUES_EQUAL(ver.Version, 6);
        }
    }

    Y_UNIT_TEST(CheckAccessCopyTable) {
        TTestEnv env(1, 2);

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0", env.GetPools())));
        env.GetTenants().Run("/dc-1/USER_0", 1);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0")));
        env.GetClient().ModifyOwner("/dc-1", "USER_0", "user0@builtin");
        env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0");

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_1", env.GetPools())));
        env.GetTenants().Run("/dc-1/USER_1", 1);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_1")));
        env.GetClient().ModifyOwner("/dc-1", "USER_1", "user1@builtin");
        env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_1");


        {
            env.GetClient().SetSecurityToken("user0@builtin");

            Cerr << NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0")) << "\n";
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().MkDir("/dc-1/USER_0", "a"));
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().MkDir("/dc-1/USER_0/a", "b"));

            auto tableDesc = GetTableSimpleDescription("table");
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateTable("/dc-1/USER_0/a/b", tableDesc));

            NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0/a"));
            NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0/a/b"));
            NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0/a/b/table"));
        }

        {
            env.GetClient().SetSecurityToken("user0@builtin");

            //no rights to create table in /dc-1/USER_1
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_ERROR, env.GetClient().CopyTable("/dc-1/USER_1", "copy", "/dc-1/USER_0/a/b/table"));
        }
;
        {
            env.GetClient().SetSecurityToken("user1@builtin");

            //no rights to read table fom /dc-1/USER_0
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_ERROR, env.GetClient().CopyTable("/dc-1/USER_1", "copy", "/dc-1/USER_0/a/b/table"));
        }
    }

    Y_UNIT_TEST(ConsistentCopyTable) {
        TTestEnv env(1, 2);

        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0", env.GetPools())));
        env.GetTenants().Run("/dc-1/USER_0", 1);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0")));
        env.GetClient().ModifyOwner("/dc-1", "USER_0", "user0@builtin");
        env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_0");

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_1", env.GetPools())));
        env.GetTenants().Run("/dc-1/USER_1", 1);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_1")));
        env.GetClient().ModifyOwner("/dc-1", "USER_1", "user1@builtin");
        env.GetClient().RefreshPathCache(&env.GetRuntime(), "/dc-1/USER_1");


        {
            env.GetClient().SetSecurityToken("user0@builtin");

            Cerr << NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0")) << "\n";
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().MkDir("/dc-1/USER_0", "a"));
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().MkDir("/dc-1/USER_0/a", "b"));

            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                     env.GetClient().CreateTable("/dc-1/USER_0/", GetTableSimpleDescription("table")));
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                     env.GetClient().CreateTable("/dc-1/USER_0/a", GetTableSimpleDescription("table")));
            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                     env.GetClient().CreateTable("/dc-1/USER_0/a/b", GetTableSimpleDescription("table")));


            NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0/table"));
            NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0/a"));
            NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0/a/b/table"));
            NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0/a/b"));
            NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0/a/b/table"));
        }

        {
            env.GetClient().SetSecurityToken("user0@builtin");

            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().MkDir("/dc-1/USER_0", "backup"));

            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                     env.GetClient().ConsistentCopyTables(
                                        {{"/dc-1/USER_0/table", "/dc-1/USER_0/backup/table"},
                                         {"/dc-1/USER_0/a/table", "/dc-1/USER_0/backup/table_a"},
                                         {"/dc-1/USER_0/a/b/table", "/dc-1/USER_0/backup/table_a_b"}}));

            NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0/backup"));
            NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0/backup/table"));
            NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0/backup/table_a"));
            NTestLs::CheckStatus(env.GetClient().Ls("/dc-1/USER_0/backup/table_a_b"));
        }

        {
            env.GetClient().SetSecurityToken("user0@builtin");

            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_ERROR,
                                     env.GetClient().ConsistentCopyTables(
                                        {{"/dc-1/USER_0/table", "/dc-1/USER_1/table"},
                                         {"/dc-1/USER_0/a/table", "/dc-1/USER_1/table_a"}}));
        }

        {
            env.GetClient().SetSecurityToken("user1@builtin");

            UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_ERROR,
                                     env.GetClient().ConsistentCopyTables(
                                        {{"/dc-1/USER_0/table", "/dc-1/USER_1/table"},
                                         {"/dc-1/USER_0/a/table", "/dc-1/USER_1/table_a"}}));
        }
    }
}
