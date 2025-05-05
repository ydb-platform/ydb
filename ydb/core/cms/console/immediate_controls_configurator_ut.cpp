#include "configs_dispatcher.h"
#include "immediate_controls_configurator.h"
#include "ut_helpers.h"

namespace NKikimr {

using namespace NConsole;
using namespace NConsole::NUT;

namespace {

TTenantTestConfig::TTenantPoolConfig StaticTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {DOMAIN1_NAME, {1, 1, 1}} }},
        // NodeType
        "type1"
    };
    return res;
}

TTenantTestConfig DefaultConsoleTestConfig()
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
                {StaticTenantPoolConfig()},
        }},
        // DataCenterCount
        1,
        // CreateConfigsDispatcher
        true
    };
    return res;
}

NKikimrConsole::TConfigItem ITEM_CONTROLS_DEFAULT;
NKikimrConsole::TConfigItem ITEM_CONTROLS1;
NKikimrConsole::TConfigItem ITEM_CONTROLS2;
NKikimrConsole::TConfigItem ITEM_CONTROLS2_RES;
NKikimrConsole::TConfigItem ITEM_CONTROLS_MAX;
NKikimrConsole::TConfigItem ITEM_CONTROLS_EXCEED_MAX;

void InitImmediateControlsConfigurator(TTenantTestRuntime &runtime)
{
    runtime.Register(CreateImmediateControlsConfigurator(runtime.GetAppData().Icb,
                                                         NKikimrConfig::TImmediateControlsConfig(),
                                                         /* allowExistingControls */ true));
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvConfigsDispatcher::EvSetConfigSubscriptionResponse, 1);
    runtime.DispatchEvents(options);

    ITEM_CONTROLS_DEFAULT
        = MakeConfigItem(NKikimrConsole::TConfigItem::ImmediateControlsConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 1,
                         NKikimrConsole::TConfigItem::OVERWRITE, "");
    {
        auto &cfg = *ITEM_CONTROLS_DEFAULT.MutableConfig()->MutableImmediateControlsConfig();
        cfg.MutableDataShardControls()->SetMaxTxInFly(15000);
        cfg.MutableDataShardControls()->SetDisableByKeyFilter(0);
        cfg.MutableDataShardControls()->SetMaxTxLagMilliseconds(300000);
        cfg.MutableDataShardControls()->SetCanCancelROWithReadSets(0);
        cfg.MutableTxLimitControls()->SetPerRequestDataSizeLimit(53687091200);
        cfg.MutableTxLimitControls()->SetPerShardReadSizeLimit(5368709120);
        cfg.MutableTxLimitControls()->SetPerShardIncomingReadSetSizeLimit(209715200);
    }

    ITEM_CONTROLS1
        = MakeConfigItem(NKikimrConsole::TConfigItem::ImmediateControlsConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 2,
                         NKikimrConsole::TConfigItem::OVERWRITE, "");
    {
        auto &cfg = *ITEM_CONTROLS1.MutableConfig()->MutableImmediateControlsConfig();
        cfg.MutableDataShardControls()->SetMaxTxInFly(3);
        cfg.MutableDataShardControls()->SetDisableByKeyFilter(1);
        cfg.MutableDataShardControls()->SetMaxTxLagMilliseconds(5);
        cfg.MutableDataShardControls()->SetCanCancelROWithReadSets(1);
        cfg.MutableTxLimitControls()->SetPerRequestDataSizeLimit(11);
        cfg.MutableTxLimitControls()->SetPerShardReadSizeLimit(12);
        cfg.MutableTxLimitControls()->SetPerShardIncomingReadSetSizeLimit(13);
    }

    ITEM_CONTROLS2
        = MakeConfigItem(NKikimrConsole::TConfigItem::ImmediateControlsConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 3,
                         NKikimrConsole::TConfigItem::OVERWRITE, "");
    {
        auto &cfg = *ITEM_CONTROLS2.MutableConfig()->MutableImmediateControlsConfig();
        cfg.MutableDataShardControls()->SetMaxTxInFly(3);
        cfg.MutableDataShardControls()->SetCanCancelROWithReadSets(1);
        cfg.MutableTxLimitControls()->SetPerRequestDataSizeLimit(11);
        cfg.MutableTxLimitControls()->SetPerShardIncomingReadSetSizeLimit(13);
    }

    ITEM_CONTROLS2_RES
        = MakeConfigItem(NKikimrConsole::TConfigItem::ImmediateControlsConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 4,
                         NKikimrConsole::TConfigItem::OVERWRITE, "");
    {
        auto &cfg = *ITEM_CONTROLS2_RES.MutableConfig()->MutableImmediateControlsConfig();
        cfg.MutableDataShardControls()->SetMaxTxInFly(3);
        cfg.MutableDataShardControls()->SetDisableByKeyFilter(0);
        cfg.MutableDataShardControls()->SetMaxTxLagMilliseconds(300000);
        cfg.MutableDataShardControls()->SetCanCancelROWithReadSets(1);
        cfg.MutableTxLimitControls()->SetPerRequestDataSizeLimit(11);
        cfg.MutableTxLimitControls()->SetPerShardReadSizeLimit(5368709120);
        cfg.MutableTxLimitControls()->SetPerShardIncomingReadSetSizeLimit(13);
    }

    ITEM_CONTROLS_MAX
        = MakeConfigItem(NKikimrConsole::TConfigItem::ImmediateControlsConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 5,
                         NKikimrConsole::TConfigItem::OVERWRITE, "");
    {
        auto &cfg = *ITEM_CONTROLS_MAX.MutableConfig()->MutableImmediateControlsConfig();
        cfg.MutableDataShardControls()->SetMaxTxInFly(100000);
        cfg.MutableDataShardControls()->SetDisableByKeyFilter(1);
        cfg.MutableDataShardControls()->SetMaxTxLagMilliseconds(2592000000ULL);
        cfg.MutableDataShardControls()->SetCanCancelROWithReadSets(1);
        cfg.MutableTxLimitControls()->SetPerRequestDataSizeLimit(256000000000000ULL);
        cfg.MutableTxLimitControls()->SetPerShardReadSizeLimit(107374182400ULL);
        cfg.MutableTxLimitControls()->SetPerShardIncomingReadSetSizeLimit(5368709120ULL);
    }

    ITEM_CONTROLS_EXCEED_MAX
        = MakeConfigItem(NKikimrConsole::TConfigItem::ImmediateControlsConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 6,
                         NKikimrConsole::TConfigItem::OVERWRITE, "");
    {
        auto &cfg = *ITEM_CONTROLS_EXCEED_MAX.MutableConfig()->MutableImmediateControlsConfig();
        cfg.MutableDataShardControls()->SetMaxTxInFly(1000000);
        cfg.MutableDataShardControls()->SetDisableByKeyFilter(10);
        cfg.MutableDataShardControls()->SetMaxTxLagMilliseconds(25920000000ULL);
        cfg.MutableDataShardControls()->SetCanCancelROWithReadSets(10);
        cfg.MutableTxLimitControls()->SetPerRequestDataSizeLimit(300000000000000ULL);
        cfg.MutableTxLimitControls()->SetPerShardReadSizeLimit(1073741824000ULL);
        cfg.MutableTxLimitControls()->SetPerShardIncomingReadSetSizeLimit(53687091200ULL);
    }
}

void WaitForUpdate(TTenantTestRuntime &runtime)
{
    struct TIsConfigNotificationProcessed {
        bool operator()(IEventHandle& ev)
        {
            if (ev.GetTypeRewrite() == NConsole::TEvConsole::EvConfigNotificationResponse) {
                auto &rec = ev.Get<NConsole::TEvConsole::TEvConfigNotificationResponse>()->Record;
                if (rec.GetConfigId().ItemIdsSize() != 1 || rec.GetConfigId().GetItemIds(0).GetId())
                    return true;
            }

            return false;
        }
    };

    TDispatchOptions options;
    options.FinalEvents.emplace_back(TIsConfigNotificationProcessed(), 1);
    runtime.DispatchEvents(options);
}

template <typename ...Ts>
void ConfigureAndWaitUpdate(TTenantTestRuntime &runtime,
                            Ts... args)
{
    auto *event = new TEvConsole::TEvConfigureRequest;
    CollectActions(event->Record, args...);

    runtime.SendToConsole(event);
    WaitForUpdate(runtime);
}

void CompareControls(TTenantTestRuntime &runtime,
                     const NKikimrConfig::TImmediateControlsConfig &cfg)
{
    auto icb = runtime.GetAppData().Icb;

    TControlWrapper wrapper;

    icb->RegisterSharedControl(wrapper, "DataShardControls.MaxTxInFly");
    UNIT_ASSERT_VALUES_EQUAL((ui64)(i64)wrapper, cfg.GetDataShardControls().GetMaxTxInFly());
    icb->RegisterSharedControl(wrapper, "DataShardControls.DisableByKeyFilter");
    UNIT_ASSERT_VALUES_EQUAL((ui64)(i64)wrapper, cfg.GetDataShardControls().GetDisableByKeyFilter());
    icb->RegisterSharedControl(wrapper, "DataShardControls.MaxTxLagMilliseconds");
    UNIT_ASSERT_VALUES_EQUAL((ui64)(i64)wrapper, cfg.GetDataShardControls().GetMaxTxLagMilliseconds());
    icb->RegisterSharedControl(wrapper, "DataShardControls.CanCancelROWithReadSets");
    UNIT_ASSERT_VALUES_EQUAL((ui64)(i64)wrapper, cfg.GetDataShardControls().GetCanCancelROWithReadSets());
    icb->RegisterSharedControl(wrapper, "TxLimitControls.PerRequestDataSizeLimit");
    UNIT_ASSERT_VALUES_EQUAL((ui64)(i64)wrapper, cfg.GetTxLimitControls().GetPerRequestDataSizeLimit());
    icb->RegisterSharedControl(wrapper, "TxLimitControls.PerShardReadSizeLimit");
    UNIT_ASSERT_VALUES_EQUAL((ui64)(i64)wrapper, cfg.GetTxLimitControls().GetPerShardReadSizeLimit());
    icb->RegisterSharedControl(wrapper, "TxLimitControls.PerShardIncomingReadSetSizeLimit");
    UNIT_ASSERT_VALUES_EQUAL((ui64)(i64)wrapper, cfg.GetTxLimitControls().GetPerShardIncomingReadSetSizeLimit());
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TImmediateControlsConfiguratorTests)
{
    Y_UNIT_TEST(TestControlsInitialization)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitImmediateControlsConfigurator(runtime);
        WaitForUpdate(runtime); // initial update

        CompareControls(runtime, ITEM_CONTROLS_DEFAULT.GetConfig().GetImmediateControlsConfig());
    }

    Y_UNIT_TEST(TestModifiedControls)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitImmediateControlsConfigurator(runtime);
        WaitForUpdate(runtime); // initial update

        ConfigureAndWaitUpdate(runtime,
                               MakeAddAction(ITEM_CONTROLS1));
        CompareControls(runtime, ITEM_CONTROLS1.GetConfig().GetImmediateControlsConfig());
    }

    Y_UNIT_TEST(TestResetToDefault)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitImmediateControlsConfigurator(runtime);
        WaitForUpdate(runtime); // initial update

        ConfigureAndWaitUpdate(runtime,
                               MakeAddAction(ITEM_CONTROLS1));
        CompareControls(runtime, ITEM_CONTROLS1.GetConfig().GetImmediateControlsConfig());

        ConfigureAndWaitUpdate(runtime,
                               MakeAddAction(ITEM_CONTROLS2));
        CompareControls(runtime, ITEM_CONTROLS2_RES.GetConfig().GetImmediateControlsConfig());

        ConfigureAndWaitUpdate(runtime,
                               MakeRemoveAction(1, 1),
                               MakeRemoveAction(2, 1));

        CompareControls(runtime, ITEM_CONTROLS_DEFAULT.GetConfig().GetImmediateControlsConfig());
    }

    Y_UNIT_TEST(TestMaxLimit)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitImmediateControlsConfigurator(runtime);
        WaitForUpdate(runtime); // initial update

        ConfigureAndWaitUpdate(runtime,
                               MakeAddAction(ITEM_CONTROLS_EXCEED_MAX));
        CompareControls(runtime, ITEM_CONTROLS_MAX.GetConfig().GetImmediateControlsConfig());
    }

    Y_UNIT_TEST(TestDynamicMap)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitImmediateControlsConfigurator(runtime);
        WaitForUpdate(runtime); // initial update

        NKikimrConsole::TConfigItem dynamicMapValue;
        {
            auto &cfg = *dynamicMapValue.MutableConfig()->MutableImmediateControlsConfig();
            auto *grpcControls = cfg.MutableGRpcControls();

            auto *requestConfigs = grpcControls->MutableRequestConfigs();
            auto &r = (*requestConfigs)["FooBar"];
            r.SetMaxInFlight(10);
        }

        ConfigureAndWaitUpdate(runtime, MakeAddAction(dynamicMapValue));

        auto icb = runtime.GetAppData().Icb;

        TControlWrapper wrapper;

        icb->RegisterSharedControl(wrapper, "GRpcControls.RequestConfigs.FooBar.MaxInFlight");
        UNIT_ASSERT_VALUES_EQUAL((ui64)(i64)wrapper, 10);
    }
}

} // namespace NKikimr
