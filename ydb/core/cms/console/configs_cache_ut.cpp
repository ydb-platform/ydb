#include "ut_helpers.h"
#include "console_configs_manager.h"
#include "configs_cache.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/validators/registry.h>
#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/library/actors/interconnect/interconnect_impl.h>

#include <ydb/core/testlib/tenant_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/hostname.h>

namespace NKikimr {

using namespace NConsole;
using namespace NConsole::NUT;

namespace {

void Canonize(NKikimrConfig::TConfigVersion *version) {
    auto items = version->MutableItems();
    std::sort(items->begin(), items->end(), [](auto i1, auto i2) {
        if (i1.GetKind() != i2.GetKind())
            return i1.GetKind() < i2.GetKind();
        else if (i1.GetId() != i2.GetId())
            return i1.GetId() < i2.GetId();
        else
            return i1.GetGeneration() < i2.GetGeneration();
    });
}

TTenantTestConfig::TTenantPoolConfig DefaultTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {DOMAIN1_NAME, {1, 1, 1}} }},
        // NodeType
        "type1"
    };
    return res;
}

TTenantTestConfig DefaultTestConfig()
{
    TTenantTestConfig res = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, TVector<TString>()} }},
        // HiveId
        HIVE_ID,
        // FakeTenantSlotBroker
        true,
        // FakeSchemeShard
        false,
        // CreateConsole
        true,
        // Nodes {tenant_pool_config, data_center}
        {{
             {DefaultTenantPoolConfig()},
         }},
        // DataCenterCount
        1,
        // CreateConfigsDispatcher
        true
    };
    return res;
}

NKikimrConsole::TConfigItem ITEM_DOMAIN_LOG_1;
NKikimrConsole::TConfigItem ITEM_DOMAIN_LOG_2;
NKikimrConsole::TConfigItem ITEM_DOMAIN_TENANT_POOL_1;

void InitializeTestConfigItems()
{
    ITEM_DOMAIN_LOG_1 =
        MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                       NKikimrConfig::TAppConfig(), {}, {}, "", "", 1,
                       NKikimrConsole::TConfigItem::MERGE, "");

    ITEM_DOMAIN_LOG_2 =
        MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                       NKikimrConfig::TAppConfig(), {}, {}, "", "", 2,
                       NKikimrConsole::TConfigItem::OVERWRITE, "");

    ITEM_DOMAIN_TENANT_POOL_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::TenantPoolConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 1,
                         NKikimrConsole::TConfigItem::MERGE, "");
}

void AwaitForCondition(const TTenantTestRuntime &runtime, const std::function<bool()> &cond, TDuration simTimeout = TDuration::Max()) {
    TInstant deadline = runtime.GetCurrentTime() + simTimeout;
    while (!cond() && runtime.GetCurrentTime() < deadline) {
        Sleep(TDuration::MilliSeconds(100));
    }

    UNIT_ASSERT(cond());
}

struct TEvPrivate {
    enum EEv {
        EvSaveConfig = EventSpaceBegin(NKikimr::TKikimrEvents::ES_PRIVATE),
        EvEnd
    };

    struct TEvSaveConfig : public TEventLocal<TEvSaveConfig, EvSaveConfig> {
        TEvSaveConfig(NKikimrConfig::TAppConfig config)
            : Config(config) {}

        NKikimrConfig::TAppConfig Config;
    };
};

}

Y_UNIT_TEST_SUITE(TConfigsCacheTests) {
    Y_UNIT_TEST(TestNoNotificationIfConfigIsCached) {
        TTenantTestRuntime runtime(DefaultTestConfig());
        InitializeTestConfigItems();

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, MakeAddAction(ITEM_DOMAIN_LOG_1));

        TSaveCallback save = [](auto&) {
            UNIT_ASSERT(false);
        };

        TLoadCallback load = [](auto &config) {
            config.MutableLogConfig()->SetClusterName("cluster-1");
            auto item = config.MutableVersion()->AddItems();
            item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
            item->SetId(1);
            item->SetGeneration(1);
        };

        auto actor = new TConfigsCache(save, load);
        runtime.Register(actor);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvConsole::EvConfigSubscriptionResponse, 1);
        runtime.DispatchEvents(options);
    }

    Y_UNIT_TEST(TestFullConfigurationRestore) {
        TTenantTestRuntime runtime(DefaultTestConfig());
        InitializeTestConfigItems();

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, MakeAddAction(ITEM_DOMAIN_LOG_1));

        auto edge = runtime.AllocateEdgeActor();

        TSaveCallback save = [&runtime, &edge](auto &config) {
            runtime.Send(new IEventHandle(runtime.Sender, edge, new TEvPrivate::TEvSaveConfig(config)));
        };

        TLoadCallback load = [](auto&) {};

        auto actor = new TConfigsCache(save, load);
        runtime.Register(actor);

        NKikimrConfig::TAppConfig expected;
        expected.MutableLogConfig()->SetClusterName("cluster-1");

        auto item = expected.MutableVersion()->AddItems();
        item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        item->SetId(1);
        item->SetGeneration(1);

        TAutoPtr<IEventHandle> handle;
        auto saved = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvSaveConfig>(handle)->Config;

        UNIT_ASSERT_VALUES_EQUAL(expected.ShortDebugString(), saved.ShortDebugString());
    }

    Y_UNIT_TEST(TestConfigurationSaveOnNotification) {
        TTenantTestRuntime runtime(DefaultTestConfig());
        InitializeTestConfigItems();

        auto edge = runtime.AllocateEdgeActor();

        TSaveCallback save = [&runtime, &edge](auto &config) {
            runtime.Send(new IEventHandle(runtime.Sender, edge, new TEvPrivate::TEvSaveConfig(config)));
        };

        TLoadCallback load = [](auto&) {};

        auto actor = new TConfigsCache(save, load);
        runtime.Register(actor);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvConsole::EvConfigSubscriptionResponse, 1);
        runtime.DispatchEvents(options);

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, MakeAddAction(ITEM_DOMAIN_LOG_1));

        NKikimrConfig::TAppConfig expected;
        expected.MutableLogConfig()->SetClusterName("cluster-1");

        auto item = expected.MutableVersion()->AddItems();
        item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
        item->SetId(1);
        item->SetGeneration(1);

        TAutoPtr<IEventHandle> handle;
        auto saved = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvSaveConfig>(handle)->Config;

        UNIT_ASSERT_VALUES_EQUAL(expected.ShortDebugString(), saved.ShortDebugString());
    }

    Y_UNIT_TEST(TestOverwrittenConfigurationDoesntCauseNotification) {
        TTenantTestRuntime runtime(DefaultTestConfig());
        InitializeTestConfigItems();

        auto group = GetServiceCounters(runtime.GetDynamicCounters(0), "utils")->GetSubgroup("component", "configs_cache");
        auto inconsistent = group->GetCounter("InconsistentConfiguration");

        UNIT_ASSERT(*inconsistent == 0);

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");
        ITEM_DOMAIN_LOG_2.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-2");

        auto ids = CheckConfigure(runtime, Ydb::StatusIds::SUCCESS,
                                  MakeAddAction(ITEM_DOMAIN_LOG_1),
                                  MakeAddAction(ITEM_DOMAIN_LOG_2));

        AssignIds(ids, ITEM_DOMAIN_LOG_1, ITEM_DOMAIN_LOG_2);

        ui32 saves = 0;
        TSaveCallback save = [&saves](auto&) {
            saves++;
        };

        TLoadCallback load = [](auto&) {};

        auto actor = new TConfigsCache(save, load);
        runtime.Register(actor);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvConsole::EvConfigSubscriptionResponse, 1);
        runtime.DispatchEvents(options);

        AwaitForCondition(runtime, [&saves]() { return saves > 0; }, TDuration::MilliSeconds(5000));

        UNIT_ASSERT_EQUAL(saves, 1);

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-3");

        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, MakeModifyAction(ITEM_DOMAIN_LOG_1));

        Sleep(TDuration::MilliSeconds(300));

        UNIT_ASSERT_EQUAL(saves, 1);
    }

    Y_UNIT_TEST(TestConfigurationChangeSensor) {
        TTenantTestRuntime runtime(DefaultTestConfig());
        InitializeTestConfigItems();

        auto edge = runtime.AllocateEdgeActor();

        TSaveCallback save = [&runtime, &edge](auto &config) {
            runtime.Send(new IEventHandle(runtime.Sender, edge, new TEvPrivate::TEvSaveConfig(config)));
        };

        TLoadCallback load = [](auto&) {};

        auto actor = new TConfigsCache(save, load);
        runtime.Register(actor);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvConsole::EvConfigSubscriptionResponse, 1);
        runtime.DispatchEvents(options);

        auto group = GetServiceCounters(runtime.GetDynamicCounters(0), "utils")->GetSubgroup("component", "configs_cache");
        auto outdated = group->GetCounter("OutdatedConfiguration");

        UNIT_ASSERT(*outdated == 0);

        ITEM_DOMAIN_LOG_1.MutableConfig()->MutableLogConfig()->SetClusterName("cluster-1");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, MakeAddAction(ITEM_DOMAIN_LOG_1));

        NKikimrConfig::TAppConfig expected;
        expected.MutableLogConfig()->SetClusterName("cluster-1");
        {
            auto item = expected.MutableVersion()->AddItems();
            item->SetKind(NKikimrConsole::TConfigItem::LogConfigItem);
            item->SetId(1);
            item->SetGeneration(1);
        }

        TAutoPtr<IEventHandle> handle1;
        auto saved1 = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvSaveConfig>(handle1)->Config;

        Canonize(expected.MutableVersion());
        Canonize(saved1.MutableVersion());

        UNIT_ASSERT_VALUES_EQUAL(expected.ShortDebugString(), saved1.ShortDebugString());
        UNIT_ASSERT(*outdated == 0); // log config change doesn't require node restart

        ITEM_DOMAIN_TENANT_POOL_1.MutableConfig()->MutableTenantPoolConfig()->SetNodeType("nodeType");
        CheckConfigure(runtime, Ydb::StatusIds::SUCCESS, MakeAddAction(ITEM_DOMAIN_TENANT_POOL_1));

        auto tenantPool = expected.MutableTenantPoolConfig();
        tenantPool->SetNodeType("nodeType");
        {
            auto item = expected.MutableVersion()->AddItems();
            item->SetKind(NKikimrConsole::TConfigItem::TenantPoolConfigItem);
            item->SetId(2);
            item->SetGeneration(1);
        }

        TAutoPtr<IEventHandle> handle2;
        auto saved2 = runtime.GrabEdgeEventRethrow<TEvPrivate::TEvSaveConfig>(handle2)->Config;

        Canonize(expected.MutableVersion());
        Canonize(saved2.MutableVersion());

        UNIT_ASSERT_VALUES_EQUAL(expected.ShortDebugString(), saved2.ShortDebugString());
        UNIT_ASSERT(*outdated == 1);
    }
}
}
