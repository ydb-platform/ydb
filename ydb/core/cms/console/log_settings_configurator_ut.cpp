#include "configs_dispatcher.h"
#include "log_settings_configurator.h"
#include "ut_helpers.h"

namespace NKikimr {

using namespace NConsole;
using namespace NConsole::NUT;
using namespace NLog;

namespace {

TTenantTestConfig::TTenantPoolConfig TenantTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {TENANT1_1_NAME, {1, 1, 1}} }},
        // NodeType
        "type1"
    };
    return res;
}

TTenantTestConfig DefaultConsoleTestConfig()
{
    TTenantTestConfig res = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, { TENANT1_1_NAME } } }},
        // HiveId
        HIVE_ID,
        // FakeTenantSlotBroker
        true,
        // FakeSchemeShard
        true,
        // CreateConsole
        true,
        // Nodes {tenant_pool_config, data_center}
        {{
                {TenantTenantPoolConfig()},
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
NKikimrConsole::TConfigItem ITEM_TENANT1_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT1_LOG_2;
NKikimrConsole::TConfigItem ITEM_TENANT2_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT2_LOG_2;
NKikimrConsole::TConfigItem ITEM_TYPE1_LOG_1;
NKikimrConsole::TConfigItem ITEM_TYPE1_LOG_2;
NKikimrConsole::TConfigItem ITEM_TENANT1_TYPE1_LOG_1;
NKikimrConsole::TConfigItem ITEM_TENANT2_TYPE1_LOG_1;

TVector<TComponentSettings>
InitLogSettingsConfigurator(TTenantTestRuntime &runtime)
{
    runtime.Register(CreateLogSettingsConfigurator());
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvConfigsDispatcher::EvSetConfigSubscriptionResponse, 1);
    runtime.DispatchEvents(options);


    ITEM_DOMAIN_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 1,
                         NKikimrConsole::TConfigItem::MERGE, "");
    ITEM_DOMAIN_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "", 2,
                         NKikimrConsole::TConfigItem::MERGE, "");
    ITEM_TENANT1_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, TENANT1_1_NAME, "", 3,
                         NKikimrConsole::TConfigItem::MERGE, "");
    ITEM_TENANT1_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, TENANT1_1_NAME, "", 4,
                         NKikimrConsole::TConfigItem::MERGE, "");
    ITEM_TENANT2_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, TENANT1_2_NAME, "", 5,
                         NKikimrConsole::TConfigItem::MERGE, "");
    ITEM_TENANT2_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, TENANT1_2_NAME, "", 6,
                         NKikimrConsole::TConfigItem::MERGE, "");
    ITEM_TYPE1_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "type1", 7,
                         NKikimrConsole::TConfigItem::MERGE, "");
    ITEM_TYPE1_LOG_2
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, "", "type1", 8,
                         NKikimrConsole::TConfigItem::MERGE, "");
    ITEM_TENANT1_TYPE1_LOG_1
        = MakeConfigItem(NKikimrConsole::TConfigItem::LogConfigItem,
                         NKikimrConfig::TAppConfig(), {}, {}, TENANT1_1_NAME, "type1", 9,
                         NKikimrConsole::TConfigItem::MERGE, "");

    auto logSettings = runtime.GetLogSettings(0);

    // Reset all to defaults except CMS_CONFIGS.
    TString dummy;
    logSettings->SetLevel(logSettings->DefPriority,
                          InvalidComponent,
                          dummy);
    logSettings->SetSamplingLevel(logSettings->DefSamplingPriority,
                                  InvalidComponent,
                                  dummy);
    logSettings->SetSamplingRate(logSettings->DefSamplingRate,
                                 InvalidComponent,
                                 dummy);
    logSettings->SetLevel(PRI_TRACE,
                          NKikimrServices::CMS_CONFIGS,
                          dummy);

    // But enable CMS_CONFIGS

    TVector<TComponentSettings> result(logSettings->MaxVal + 1,
                                       TComponentSettings(0));
    for (EComponent i = logSettings->MinVal; i <= logSettings->MaxVal; ++i) {
        if (logSettings->IsValidComponent(i))
            result[i] = logSettings->GetComponentSettings(i);
    }
    return result;
}

void SetDefaults(NKikimrConsole::TConfigItem &item,
                 ui32 level,
                 ui32 samplingLevel,
                 ui32 samplingRate)
{
    auto &cfg = *item.MutableConfig()->MutableLogConfig();
    cfg.SetDefaultLevel(level);
    cfg.SetDefaultSamplingLevel(samplingLevel);
    cfg.SetDefaultSamplingRate(samplingRate);
}

void AddEntry(NKikimrConsole::TConfigItem &item,
              const TString &component,
              ui32 level,
              ui32 samplingLevel,
              ui32 samplingRate)
{
    auto &entry = *item.MutableConfig()->MutableLogConfig()->AddEntry();
    entry.SetComponent(component);
    if (level != Max<ui32>())
        entry.SetLevel(level);
    if (samplingLevel != Max<ui32>())
        entry.SetSamplingLevel(samplingLevel);
    if (samplingRate != Max<ui32>())
        entry.SetSamplingRate(samplingRate);
}

void SetDefaultLogConfig(NKikimrConsole::TConfigItem &item)
{
    SetDefaults(item, PRI_WARN, PRI_WARN, 0);
    AddEntry(item, "CMS_CONFIGS", PRI_TRACE, Max<ui32>(), Max<ui32>());
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

void CompareSettings(TTenantTestRuntime &runtime,
                     const TVector<TComponentSettings> &settings)
{
    auto logSettings = runtime.GetLogSettings(0);
    for (EComponent i = logSettings->MinVal; i <= logSettings->MaxVal; ++i) {
        if (!logSettings->IsValidComponent(i))
            continue;

        UNIT_ASSERT_VALUES_EQUAL(settings[i].Raw.X.Level,
                                 logSettings->GetComponentSettings(i).Raw.X.Level);
        UNIT_ASSERT_VALUES_EQUAL(settings[i].Raw.X.SamplingLevel,
                                 logSettings->GetComponentSettings(i).Raw.X.SamplingLevel);
        UNIT_ASSERT_VALUES_EQUAL(settings[i].Raw.X.SamplingRate,
                                 logSettings->GetComponentSettings(i).Raw.X.SamplingRate);
    }
}

TVector<ui64> GetTenantItemIds(TTenantTestRuntime &runtime,
                               const TString &tenant)
{
    auto *event = new TEvConsole::TEvGetConfigItemsRequest;
    event->Record.MutableTenantFilter()->AddTenants(tenant);

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetConfigItemsResponse>(handle);
    TVector<ui64> result;
    for (auto &item : reply->Record.GetConfigItems())
        result.push_back(item.GetId().GetId());
    return result;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TLogSettingsConfiguratorTests)
{
    Y_UNIT_TEST(TestNoChanges)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        auto settings = InitLogSettingsConfigurator(runtime);
        WaitForUpdate(runtime); // initial update

        SetDefaultLogConfig(ITEM_DOMAIN_LOG_1);
        ConfigureAndWaitUpdate(runtime,
                               MakeAddAction(ITEM_DOMAIN_LOG_1));
        CompareSettings(runtime, settings);
    }

    Y_UNIT_TEST(TestAddComponentEntries)
    {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        auto settings = InitLogSettingsConfigurator(runtime);
        WaitForUpdate(runtime); // initial update

        SetDefaultLogConfig(ITEM_DOMAIN_LOG_1);
        AddEntry(ITEM_DOMAIN_LOG_1, "CMS_CLUSTER", 5, Max<ui32>(), Max<ui32>());
        AddEntry(ITEM_DOMAIN_LOG_1, "CMS_CONFIGS", Max<ui32>(), 5, Max<ui32>());
        AddEntry(ITEM_DOMAIN_LOG_1, "CMS_TENANTS", Max<ui32>(), Max<ui32>(), 5);
        ConfigureAndWaitUpdate(runtime,
                               MakeAddAction(ITEM_DOMAIN_LOG_1));
        settings[NKikimrServices::CMS_CLUSTER].Raw.X.Level = 5;
        settings[NKikimrServices::CMS_CONFIGS].Raw.X.SamplingLevel = 5;
        settings[NKikimrServices::CMS_TENANTS].Raw.X.SamplingRate = 5;
        CompareSettings(runtime, settings);
    }

    Y_UNIT_TEST(TestRemoveComponentEntries)
    {
        NKikimrConfig::TAppConfig ext;
        auto& label = *ext.AddLabels();
        label.SetName("tenant");
        label.SetValue(TENANT1_1_NAME);
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), ext);
        auto settings = InitLogSettingsConfigurator(runtime);
        WaitForUpdate(runtime); // initial update

        SetDefaultLogConfig(ITEM_DOMAIN_LOG_1);
        AddEntry(ITEM_TENANT1_LOG_1, "CMS_CLUSTER", 5, Max<ui32>(), Max<ui32>());
        AddEntry(ITEM_TENANT1_LOG_1, "CMS_CONFIGS", Max<ui32>(), 5, Max<ui32>());
        AddEntry(ITEM_TENANT1_LOG_1, "CMS_TENANTS", Max<ui32>(), Max<ui32>(), 5);
        ConfigureAndWaitUpdate(runtime,
                               MakeAddAction(ITEM_DOMAIN_LOG_1),
                               MakeAddAction(ITEM_TENANT1_LOG_1));
        settings[NKikimrServices::CMS_CLUSTER].Raw.X.Level = 5;
        settings[NKikimrServices::CMS_CONFIGS].Raw.X.SamplingLevel = 5;
        settings[NKikimrServices::CMS_TENANTS].Raw.X.SamplingRate = 5;
        CompareSettings(runtime, settings);

        auto ids = GetTenantItemIds(runtime, TENANT1_1_NAME);
        AssignIds(ids, ITEM_TENANT1_LOG_1);

        ConfigureAndWaitUpdate(runtime,
                               MakeRemoveAction(ITEM_TENANT1_LOG_1));
        settings[NKikimrServices::CMS_CLUSTER].Raw.X.Level = 4;
        settings[NKikimrServices::CMS_CONFIGS].Raw.X.SamplingLevel = 4;
        settings[NKikimrServices::CMS_TENANTS].Raw.X.SamplingRate = 0;
        CompareSettings(runtime, settings);
    }

    Y_UNIT_TEST(TestChangeDefaults)
    {
        NKikimrConfig::TAppConfig ext;
        auto& label = *ext.AddLabels();
        label.SetName("tenant");
        label.SetValue(TENANT1_1_NAME);
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), ext);
        auto settings = InitLogSettingsConfigurator(runtime);
        WaitForUpdate(runtime); // initial update

        SetDefaultLogConfig(ITEM_DOMAIN_LOG_1);
        SetDefaults(ITEM_TENANT1_LOG_1, PRI_ALERT, PRI_ALERT, 10);
        ConfigureAndWaitUpdate(runtime,
                               MakeAddAction(ITEM_DOMAIN_LOG_1),
                               MakeAddAction(ITEM_TENANT1_LOG_1));
        for (auto &set : settings)
            set = TComponentSettings(PRI_ALERT, PRI_ALERT, 10);
        settings[NKikimrServices::CMS_CONFIGS].Raw.X.Level = PRI_TRACE;
        CompareSettings(runtime, settings);
    }
}

} // namespace NKikimr
