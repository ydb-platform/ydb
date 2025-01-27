#include "feature_flags_configurator.h"
#include "configs_dispatcher.h"
#include "ut_helpers.h"

#include <ydb/core/base/feature_flags_service.h>

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
        true,
        // RegisterFeatureFlagsConfigurator
        false,
    };
    return res;
}

const NKikimrConsole::TConfigItem ITEM_FEATURE_FLAGS_DEFAULT = []{
    auto item = MakeConfigItem(NKikimrConsole::TConfigItem::FeatureFlagsItem,
                               NKikimrConfig::TAppConfig(), {}, {}, "", "", 1,
                               NKikimrConsole::TConfigItem::OVERWRITE, "");
    auto& cfg = *item.MutableConfig()->MutableFeatureFlags();
    // Note: this matches the default config currently used in tests, subject to change
    cfg.SetEnableExternalHive(false);
    return item;
}();

const NKikimrConsole::TConfigItem ITEM_FEATURE_FLAGS_1 = []{
    auto item = MakeConfigItem(NKikimrConsole::TConfigItem::FeatureFlagsItem,
                               NKikimrConfig::TAppConfig(), {}, {}, "", "", 2,
                               NKikimrConsole::TConfigItem::MERGE, "");
    auto& cfg = *item.MutableConfig()->MutableFeatureFlags();
    cfg.SetEnableDataShardVolatileTransactions(false);
    return item;
}();

const NKikimrConsole::TConfigItem ITEM_FEATURE_FLAGS_2 = []{
    auto item = MakeConfigItem(NKikimrConsole::TConfigItem::FeatureFlagsItem,
                               NKikimrConfig::TAppConfig(), {}, {}, "", "", 3,
                               NKikimrConsole::TConfigItem::OVERWRITE, "");
    auto& cfg = *item.MutableConfig()->MutableFeatureFlags();
    cfg.SetEnableVolatileTransactionArbiters(false);
    return item;
}();

void InitFeatureFlagsConfigurator(TTenantTestRuntime& runtime) {
    runtime.RegisterService(
        MakeFeatureFlagsServiceID(),
        runtime.Register(CreateFeatureFlagsConfigurator()));
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvConfigsDispatcher::EvSetConfigSubscriptionResponse, 1);
    runtime.DispatchEvents(options);
}

void WaitForUpdate(TTenantTestRuntime& runtime) {
    struct TIsConfigNotificationProcessed {
        bool operator()(IEventHandle& ev) {
            if (ev.GetTypeRewrite() == NConsole::TEvConsole::EvConfigNotificationResponse) {
                auto& rec = ev.Get<NConsole::TEvConsole::TEvConfigNotificationResponse>()->Record;
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

template <class ...Ts>
void ConfigureAndWaitUpdate(
    TTenantTestRuntime& runtime, Ts&&... args)
{
    auto* event = new TEvConsole::TEvConfigureRequest;
    CollectActions(event->Record, std::forward<Ts>(args)...);

    runtime.SendToConsole(event);
    WaitForUpdate(runtime);
}

void CompareFeatureFlags(TTenantTestRuntime& runtime, const TString& expected) {
    NKikimrConfig::TFeatureFlags current = runtime.GetAppData().FeatureFlags;

    UNIT_ASSERT_VALUES_EQUAL(current.DebugString(), expected);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(FeatureFlagsConfiguratorTest) {

    Y_UNIT_TEST(TestFeatureFlagsUpdates) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        InitFeatureFlagsConfigurator(runtime);
        WaitForUpdate(runtime); // initial update

        CompareFeatureFlags(runtime,
            "EnableExternalHive: false\n"
            "EnableColumnStatistics: false\n"
            "EnableScaleRecommender: true\n");

        auto sender = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(MakeFeatureFlagsServiceID(), sender, new TEvFeatureFlags::TEvSubscribe()));

        // We must receive the first (spurious) notification
        runtime.GrabEdgeEventRethrow<TEvFeatureFlags::TEvChanged>(sender);

        ConfigureAndWaitUpdate(runtime, MakeAddAction(ITEM_FEATURE_FLAGS_DEFAULT));
        CompareFeatureFlags(runtime,
            "EnableExternalHive: false\n");

        // We must receive a notification (contents are not checked)
        runtime.GrabEdgeEventRethrow<TEvFeatureFlags::TEvChanged>(sender);

        ConfigureAndWaitUpdate(runtime, MakeAddAction(ITEM_FEATURE_FLAGS_1));
        CompareFeatureFlags(runtime,
            "EnableExternalHive: false\n"
            "EnableDataShardVolatileTransactions: false\n");

        // We must receive a notification on every change
        runtime.GrabEdgeEventRethrow<TEvFeatureFlags::TEvChanged>(sender);

        ConfigureAndWaitUpdate(runtime, MakeAddAction(ITEM_FEATURE_FLAGS_2));
        CompareFeatureFlags(runtime,
            "EnableVolatileTransactionArbiters: false\n");

        // We must receive a notification on every change
        runtime.GrabEdgeEventRethrow<TEvFeatureFlags::TEvChanged>(sender);
    }

} // Y_UNIT_TEST_SUITE(FeatureFlagsConfiguratorTest)

} // namespace NKikimr
