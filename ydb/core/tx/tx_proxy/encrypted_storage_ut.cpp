#include "proxy_ut_helpers.h"

#include <ydb/library/actors/interconnect/interconnect.h>

#include <ydb/core/base/path.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/tablet/tablet_impl.h>


using namespace NKikimr;
using namespace NTxProxyUT;
using namespace NHelpers;

class TTestEnvWithEncryptedPoolsSupport: public TBaseTestEnv {
public:
    TTestEnvWithEncryptedPoolsSupport(ui32 staticNodes = 1, ui32 dynamicNodes = 2)
    {
        Settings = new Tests::TServerSettings(PortManager.GetPort(3534));
        GetSettings().SetEnableMockOnSingleNode(false);

        GetSettings().SetNodeCount(staticNodes);
        GetSettings().SetDynamicNodeCount(dynamicNodes);
        ui32 encryptionMode = 1;
        GetSettings().AddStoragePool("encrypted", "", encryptionMode);

        for (ui32 nodeIdx = 0; nodeIdx < staticNodes + dynamicNodes; ++nodeIdx) {
            TString key = TStringBuilder() << "node_key_" << nodeIdx;
            GetSettings().SetKeyFor(nodeIdx, key);
        }

        Server = new Tests::TServer(Settings);

        Client = MakeHolder<Tests::TClient>(GetSettings());
        Tenants = MakeHolder<Tests::TTenants>(Server);

        SetLogging();
        InitRoot();
    }

    TStoragePools CreatePoolsForTenant(const TString& tenant) {
        TStoragePools result;
        for (auto& poolType: Settings->StoragePoolTypes) {
            auto& poolKind = poolType.first;
            result.emplace_back(GetClient().CreateStoragePool(poolKind, tenant), poolKind);
        }
        return result;
    }
};

Y_UNIT_TEST_SUITE(TStorageTenantTest) {
    Y_UNIT_TEST(Empty) {
    }

    Y_UNIT_TEST(CreateTableOutsideDatabaseFailToStartTabletsButDropIsOk) {
        return;

        TTestEnvWithEncryptedPoolsSupport env(1, 1);
        UNIT_ASSERT_VALUES_EQUAL("/dc-1", env.GetRoot());

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().CreateSubdomain("/dc-1", GetSubDomainDeclareSetting("USER_0")));

        env.GetTenants().Run("/dc-1/USER_0", 1);
        UNIT_ASSERT(env.GetTenants().IsActive("/dc-1/USER_0", 1));

        auto storagePool = env.CreatePoolsForTenant("USER_0");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 env.GetClient().AlterSubdomain("/dc-1", GetSubDomainDefaultSetting("USER_0", storagePool)));

        NTestLs::TPathVersion versionBefore = NTestLs::ExtractPathVersion(env.GetClient().Ls("/dc-1/USER_0"));

        auto tableDesc = GetTableSimpleDescription("SimpleTable");
        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/USER_0", tableDesc),
                                 NMsgBusProxy::MSTATUS_OK);

        NTestLs::TPathVersion versionAfter = NTestLs::ExtractPathVersion(env.GetClient().Ls("/dc-1/USER_0"));

        UNIT_ASSERT_VALUES_EQUAL(versionBefore.Version + 2, versionAfter.Version);

        ui64 maxTableId = 0;
        {
            auto tablePortions = NTestLs::ExtractTablePartitions(env.GetClient().Ls("/dc-1/USER_0/SimpleTable"));
            for (NKikimrSchemeOp::TTablePartition& portion: tablePortions) {
                auto tabletId = portion.GetDatashardId();
                maxTableId = Max(maxTableId, tabletId);
            }
        }
        UNIT_ASSERT(env.GetClient().TabletExistsInHive(&env.GetRuntime(), maxTableId));


        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().MkDir("/dc-1", "dir"),
                                NMsgBusProxy::MSTATUS_OK);

        versionBefore = NTestLs::ExtractPathVersion(env.GetClient().Ls("/dc-1/dir"));

        //unable to run tablet on storage pools which belong to someone else database due encryption
        UNIT_ASSERT_VALUES_EQUAL(env.GetClient().CreateTable("/dc-1/dir", tableDesc, TDuration::Seconds(1)),
                                 NMsgBusProxy::MSTATUS_INPROGRESS);

        versionAfter = NTestLs::ExtractPathVersion(env.GetClient().Ls("/dc-1/dir"));

        UNIT_ASSERT_VALUES_EQUAL(versionBefore.Version + 1, versionAfter.Version);

        UNIT_ASSERT(env.GetClient().TabletExistsInHive(&env.GetRuntime(), maxTableId+1));
        UNIT_ASSERT(env.GetClient().TabletExistsInHive(&env.GetRuntime(), maxTableId+2));

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().ForceDeleteUnsafe("/dc-1", "dir"));

        //tablet deleted logicaly
        while (env.GetClient().TabletExistsInHive(&env.GetRuntime(), maxTableId+1)) {
            /*no op*/
        }
        UNIT_ASSERT(!env.GetClient().TabletExistsInHive(&env.GetRuntime(), maxTableId+1));

        //tablet deleted logicaly
        while (env.GetClient().TabletExistsInHive(&env.GetRuntime(), maxTableId+2)) {
            /*no op*/
        }
        UNIT_ASSERT(!env.GetClient().TabletExistsInHive(&env.GetRuntime(), maxTableId+2));
    }

}
