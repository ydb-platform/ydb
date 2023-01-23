#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/testlib/test_client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>


// ad-hoc test parametrization support: only for single boolean flag
// taken from ydb/core/ut/common/kqp_ut_common.h:Y_UNIT_TEST_TWIN
//TODO: introduce general support for test parametrization?
#define Y_UNIT_TEST_FLAG(N, OPT)                                                                                   \
    template<bool OPT> void Test##N(NUnitTest::TTestContext&);                                                           \
    struct TTestRegistration##N {                                                                                  \
        TTestRegistration##N() {                                                                                   \
            TCurrentTest::AddTest(#N "-" #OPT "-false", static_cast<void (*)(NUnitTest::TTestContext&)>(&Test##N<false>), false); \
            TCurrentTest::AddTest(#N "-" #OPT "-true", static_cast<void (*)(NUnitTest::TTestContext&)>(&Test##N<true>), false);   \
        }                                                                                                          \
    };                                                                                                             \
    static TTestRegistration##N testRegistration##N;                                                               \
    template<bool OPT>                                                                                             \
    void Test##N(NUnitTest::TTestContext&)


namespace NKikimr {
namespace NTxProxyUT {

inline constexpr static bool SanIsOn() noexcept {
    return NSan::PlainOrUnderSanitizer(false, true);
}

inline constexpr static bool IsFat() noexcept {
    return SanIsOn() || NValgrind::ValgrindIsOn();
}

template <typename T>
T ChoiceFastOrFat(T fast, T fat) noexcept {
    if (IsFat()) {
        return fat;
    }
    return fast;
}

class TBaseTestEnv {
public:
    virtual ~TBaseTestEnv() {}

    const TString& GetRoot() {
        static const TString root = TString("/") + GetSettings().DomainName;
        return root;
    }

    Tests::TServerSettings& GetSettings() {
        return *Settings;
    }

    Tests::TServer& GetServer() {
        return *Server;
    }

    Tests::TClient& GetClient() {
        return *Client;
    }

    Tests::TTenants& GetTenants() {
        return *Tenants;
    }

    TTestActorRuntime& GetRuntime() {
        return *GetServer().GetRuntime();
    }

protected:
    TPortManager PortManager;
    Tests::TServerSettings::TPtr Settings;
    Tests::TServer::TPtr Server;
    THolder<Tests::TTenants> Tenants;
    THolder<Tests::TClient> Client;

    void SetLogging() {
        if (GetSettings().SupportsRedirect && Tests::IsServerRedirected())
            return;

        GetRuntime().SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        GetRuntime().SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_NOTICE);
        GetRuntime().SetLogPriority(NKikimrServices::LOCAL, NActors::NLog::PRI_NOTICE);

        GetRuntime().SetLogPriority(NKikimrServices::BS_CONTROLLER, NActors::NLog::PRI_NOTICE);
        //GetRuntime().SetLogPriority(NKikimrServices::BS_VDISK_BLOCK, NActors::NLog::PRI_DEBUG);
        //GetRuntime().SetLogPriority(NKikimrServices::BS_PROXY_DISCOVER, NActors::NLog::PRI_DEBUG);
        //GetRuntime().SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);

        //GetRuntime().SetLogPriority(NKikimrServices::TX_MEDIATOR, NActors::NLog::PRI_DEBUG);
        //GetRuntime().SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_DEBUG);

        GetRuntime().SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_TRACE);
        GetRuntime().SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_TRACE);
        GetRuntime().SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_DEBUG);
        GetRuntime().SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NActors::NLog::PRI_DEBUG);
        GetRuntime().SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NActors::NLog::PRI_DEBUG);
    }

    void InitRoot() {
        if (GetSettings().SupportsRedirect && Tests::IsServerRedirected())
            return;

        GetClient().InitRootScheme();
    }
};

class TTestEnv: public TBaseTestEnv {
public:
    TTestEnv(ui32 staticNodes = 1, ui32 dynamicNodes = 0)
    {
        Settings = new Tests::TServerSettings(PortManager.GetPort(3534));

        GetSettings().SetNodeCount(staticNodes);
        GetSettings().SetDynamicNodeCount(dynamicNodes);

        Server = new Tests::TServer(Settings);

        Client = MakeHolder<Tests::TClient>(GetSettings());
        Tenants = MakeHolder<Tests::TTenants>(Server);

        SetLogging();
        InitRoot();
    }

    TStoragePools GetPools() {
        TStoragePools pools;
        for (const auto& [kind, pool] : GetSettings().StoragePoolTypes) {
            pools.emplace_back(pool.GetName(), kind);
        }
        return pools;
    }
};

void Print(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);

namespace NTestLs {
    TString Finished(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, NKikimrSchemeOp::EPathType type = NKikimrSchemeOp::EPathTypeDir);
    TString IsUnavailable(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);
    TString IsDoesNotExist(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);

    TString IsSubdomain(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);
    TString IsExtSubdomain(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);
    TString WithPools(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);


    TString InSubdomain(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, NKikimrSchemeOp::EPathType type = NKikimrSchemeOp::EPathTypeDir);
    TString InSubdomainWithPools(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, NKikimrSchemeOp::EPathType type = NKikimrSchemeOp::EPathTypeDir);
    TString NotInSubdomain(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, NKikimrSchemeOp::EPathType type = NKikimrSchemeOp::EPathTypeDir);
    TString HasUserAttributes(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, TVector<std::pair<TString, TString>> attrs);
    NKikimrSubDomains::TDomainDescription ExtractDomainDescription(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);
    TVector<NKikimrSchemeOp::TTablePartition> ExtractTablePartitions(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);
    NKikimrSchemeOp::TTableDescription ExtractTableDescription(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);
    TString CheckStatus(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, NKikimrScheme::EStatus schemeStatus = NKikimrScheme::StatusSuccess);

    TString ChildrenCount(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, ui64 count);
    TString NoChildren(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);
    TString HasChild(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, TString name, NKikimrSchemeOp::EPathType type);

    TString PathType(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, NKikimrSchemeOp::EPathType type);
    TString IsDir(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);
    TString IsTable(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);



    using TPathVersion = Tests::TClient::TPathVersion;
    TPathVersion ExtractPathVersion(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);

    TVector<ui64> ExtractTableShards(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp);
}

namespace NHelpers {
const TDuration WaitTimeOut = TDuration::Seconds(ChoiceFastOrFat(10, 600));

template <typename T>
inline constexpr static T ChoiceThreadSanOrDefault(T tsan, T def) noexcept {
#if defined(_tsan_enabled_)
    Y_UNUSED(def);
    return tsan;
#else
    Y_UNUSED(tsan);
    return def;
#endif
}
const ui32 active_tenants_nodes = ChoiceThreadSanOrDefault(1, 5);

ui64 CreateSubDomainAndTabletInside(TBaseTestEnv &env, const TString& name, ui64 shard_index, const TStoragePools& pools = {});

void CheckTableIsOfline(TBaseTestEnv &env, ui64 tablet_id);
void CheckTableBecomeAlive(TBaseTestEnv &env, ui64 tablet_id);
void CheckTableBecomeOfline(TBaseTestEnv &env, ui64 tablet_id);
void CheckTableRunOnProperTenantNode(TBaseTestEnv &env, const TString& tenant, ui64 tablet_id);

NKikimrSubDomains::TSubDomainSettings GetSubDomainDeclareSetting(const TString &name, const TStoragePools &pools = {});
NKikimrSubDomains::TSubDomainSettings GetSubDomainDefaultSetting(const TString &name, const TStoragePools &pools = {});

NKikimrSchemeOp::TTableDescription GetTableSimpleDescription(const TString &name);
void SetRowInSimpletable(TBaseTestEnv& env, ui64 key, ui64 value, const TString &path);
}

class TTestEnvWithPoolsSupport: public TBaseTestEnv {
public:
    TTestEnvWithPoolsSupport(ui32 staticNodes = 1, ui32 dynamicNodes = 0, ui32 poolsCount = 2, bool enableAlterDatabaseCreateHiveFirst = true)
    {
        Settings = new Tests::TServerSettings(PortManager.GetPort(3534));
        GetSettings().SetEnableMockOnSingleNode(false);

        GetSettings().SetNodeCount(staticNodes);
        GetSettings().SetDynamicNodeCount(dynamicNodes);
        GetSettings().SetEnableAlterDatabaseCreateHiveFirst(enableAlterDatabaseCreateHiveFirst);
        GetSettings().SetEnableSystemViews(false);

        for (ui32 poolNum = 1; poolNum <= poolsCount; ++poolNum) {
            GetSettings().AddStoragePoolType("storage-pool-number-" + ToString(poolNum));
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

}}
