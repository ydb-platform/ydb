#include "ut_configs_provider_helpers.h"

namespace NKikimr::NConsole {
using namespace NTests;

Y_UNIT_TEST_SUITE(TConsoleNodeConfigCacheTests) {
    Y_UNIT_TEST(EquivalentNodesShareValidatedSerializedReply) {
        auto validator = MakeIntrusive<TCountingValidator>();
        TConfigsProviderFixture fixture(validator);
        fixture.SetItems({LogItem(1, "shared")});
        auto first = fixture.Request(Node(1, "host1", "tenant1", "type1"), {}, false, {}, 123);
        auto data = first->GetChainBuffer();
        UNIT_ASSERT_VALUES_EQUAL(first->Get()->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
        first->Get()->Record.MutableConfig()->MutableLogConfig()->SetClusterName("recipient mutation");

        auto second = fixture.Request(Node(2, "host2", "tenant2", "type2"), {}, false, {}, 456);
        UNIT_ASSERT_VALUES_EQUAL(second->GetChainBuffer().Get(), data.Get());
        UNIT_ASSERT_VALUES_EQUAL(second->Get()->Record.GetConfig().GetLogConfig().GetClusterName(), "shared");
        UNIT_ASSERT_VALUES_EQUAL(validator->Checks, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("NodeConfigCacheHits", true), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("NodeConfigCacheEntries"), 1);
    }

    Y_UNIT_TEST(ScopedKindsAndMergeOrderRemainDistinct) {
        auto validator = MakeIntrusive<TCountingValidator>();
        TConfigsProviderFixture fixture(validator);
        fixture.SetItems({
            LogItem(10, "domain-first", {}, {}, {}, {}, 1),
            LogItem(1, "domain-last", {}, {}, {}, {}, 2),
            LogItem(20, "node", {1}),
            LogItem(30, "host", {}, {"special"}),
            LogItem(40, "tenant", {}, {}, "tenant"),
            LogItem(50, "type", {}, {}, {}, "type"),
        });
        auto check = [&](const NKikimrConsole::TNodeAttributes& node, const TString& expected) {
            auto response = fixture.Request(node);
            const auto& config = response->Get()->Record.GetConfig();
            UNIT_ASSERT_VALUES_EQUAL(config.GetLogConfig().GetClusterName(), expected);
            UNIT_ASSERT_VALUES_EQUAL(config.GetVersion().GetItems(0).GetId(), 10);
            UNIT_ASSERT_VALUES_EQUAL(config.GetVersion().GetItems(1).GetId(), 1);
        };
        check(Node(2), "domain-last");
        check(Node(1), "node");
        check(Node(3, "special"), "host");
        check(Node(4, {}, "tenant"), "tenant");
        check(Node(5, {}, {}, "type"), "type");
        check(Node(6), "domain-last");
        UNIT_ASSERT_VALUES_EQUAL(validator->Checks, 5);
        auto empty = fixture.Request(Node(1), {NKikimrConsole::TConfigItem::NameserviceConfigItem});
        UNIT_ASSERT(!empty->Get()->Record.GetConfig().HasLogConfig());
        auto log = fixture.Request(Node(1), {NKikimrConsole::TConfigItem::LogConfigItem});
        UNIT_ASSERT_VALUES_EQUAL(log->Get()->Record.GetConfig().GetLogConfig().GetClusterName(), "node");
        UNIT_ASSERT_VALUES_EQUAL(validator->Checks, 6);
    }

    Y_UNIT_TEST(ConfigMutationsInvalidateEvenWithReusedVersions) {
        auto validator = MakeIntrusive<TCountingValidator>();
        TConfigsProviderFixture fixture(validator);
        fixture.Request(Node(1)); // Cache an empty initial index.
        fixture.SetItems({LogItem(1, "initial")});
        auto check = [&](const TString& expected) {
            auto response = fixture.Request(Node(1));
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetConfig().GetLogConfig().GetClusterName(), expected);
        };
        check("initial");
        TConfigModifications modified;
        modified.ModifiedItems.emplace(1, LogItem(1, "same-id-and-generation"));
        fixture.Update(modified);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("NodeConfigCacheEntries"), 0);
        check("same-id-and-generation");
        TConfigModifications removed;
        removed.RemovedItems.emplace(1, modified.ModifiedItems.at(1));
        fixture.Update(removed);
        check({});
        TConfigModifications added;
        added.AddedItems.push_back(LogItem(1, "re-added"));
        fixture.Update(added);
        check("re-added");
        UNIT_ASSERT_VALUES_EQUAL(validator->Checks, 5);
    }

    Y_UNIT_TEST(ValidationSettingsAndValidatorStateInvalidate) {
        auto validator = MakeIntrusive<TCountingValidator>();
        TConfigsProviderFixture fixture(validator);
        fixture.SetItems({LogItem(1, "reject")});
        auto check = [&](bool success) {
            auto response = fixture.Request(Node(100));
            const auto& record = response->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus().GetCode(),
                success ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::PRECONDITION_FAILED);
            if (!success) {
                UNIT_ASSERT(!record.HasConfig());
                UNIT_ASSERT_STRING_CONTAINS(record.GetStatus().GetReason(), "counting validator: rejected config");
            }
        };
        check(false);
        check(false);
        UNIT_ASSERT_VALUES_EQUAL(validator->Checks, 1);
        fixture.SetValidation(false);
        check(true);
        fixture.SetValidation(true);
        check(false);
        validator->Disable();
        check(true);
        validator->Enable();
        check(false);
        auto registry = TValidatorsRegistry::Instance();
        registry->DisableValidators();
        check(true);
        registry->EnableValidators();
        check(false);
        registry->DisableValidator("counting");
        check(true);
        registry->EnableValidator("counting");
        check(false);
        UNIT_ASSERT_VALUES_EQUAL(validator->Checks, 5);

        TValidatorsRegistry::DropInstance();
        auto replacement = MakeIntrusive<TCountingValidator>();
        auto newRegistry = TValidatorsRegistry::Instance();
        UNIT_ASSERT(newRegistry->AddValidator(replacement));
        newRegistry->LockValidators();
        check(false);
        UNIT_ASSERT_VALUES_EQUAL(replacement->Checks, 1);
    }

    Y_UNIT_TEST(ValidatorAbaDuringValidationDoesNotPopulateCache) {
        auto validator = MakeIntrusive<TCountingValidator>();
        TConfigsProviderFixture fixture(validator);
        fixture.SetItems({LogItem(1, "valid")});
        validator->ToggleDuringCheck = true;
        const auto state = validator->GetState();
        fixture.Request(Node(1));
        UNIT_ASSERT(validator->IsEnabled());
        UNIT_ASSERT_VALUES_EQUAL(validator->GetState(), state + 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("NodeConfigCacheEntries"), 0);
        fixture.Request(Node(2));
        fixture.Request(Node(3));
        UNIT_ASSERT_VALUES_EQUAL(validator->Checks, 2);
        const auto enabledState = validator->GetState();
        validator->Enable();
        UNIT_ASSERT_VALUES_EQUAL(validator->GetState(), enabledState);
    }

    Y_UNIT_TEST(YamlNegotiationAndAllYamlUpdatesInvalidate) {
        auto validator = MakeIntrusive<TCountingValidator>();
        TConfigsProviderFixture fixture(validator);
        fixture.SetItems({LogItem(1, "shared")});
        THashMap<TString, TDatabaseYamlConfig> databases = {
            {"tenant-a", {"database-a", 1}}, {"tenant-b", {"database-b", 1}}, {"", {"empty-tenant", 1}}
        };
        fixture.Send(new TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig(MainYaml(1), databases));
        fixture.Sync();
        auto plain = fixture.Request(Node(1));
        auto plainData = plain->GetChainBuffer();
        for (auto api : {std::optional<ui32>{}, std::optional<ui32>{2}}) {
            auto response = fixture.Request(Node(2), {}, true, api);
            UNIT_ASSERT_VALUES_EQUAL(response->GetChainBuffer().Get(), plainData.Get());
            UNIT_ASSERT(!response->Get()->Record.HasMainYamlConfig());
        }
        auto a = fixture.Request(Node(3, {}, "tenant-a"), {}, true, 1);
        auto aData = a->GetChainBuffer();
        UNIT_ASSERT_VALUES_EQUAL(a->Get()->Record.GetDatabaseYamlConfig(), "database-a");
        auto a2 = fixture.Request(Node(4, {}, "tenant-a"), {}, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(a2->GetChainBuffer().Get(), aData.Get());
        auto b = fixture.Request(Node(5, {}, "tenant-b"), {}, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(b->Get()->Record.GetDatabaseYamlConfig(), "database-b");
        auto missing = fixture.Request(Node(6, {}, "no-database"), {}, true, 1);
        UNIT_ASSERT(!missing->Get()->Record.HasDatabaseYamlConfig());
        auto empty = fixture.Request(Node(7), {}, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(empty->Get()->Record.GetDatabaseYamlConfig(), "empty-tenant");

        databases["tenant-a"] = {"database-a-new", 2};
        const TMap<ui64, TString> volatileConfigs = {{11, "selector_config: []\n"}};
        fixture.Send(new TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig(MainYaml(2), databases, volatileConfigs));
        fixture.Sync();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("NodeConfigCacheEntries"), 0);
        auto updated = fixture.Request(Node(8, {}, "tenant-a"), {}, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(updated->Get()->Record.GetMainYamlConfig(), MainYaml(2));
        UNIT_ASSERT_VALUES_EQUAL(updated->Get()->Record.GetDatabaseYamlConfig(), "database-a-new");
        UNIT_ASSERT_VALUES_EQUAL(updated->Get()->Record.VolatileConfigsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(updated->Get()->Record.GetVolatileConfigs(0).GetId(), 11);
        databases["tenant-a"] = {"database-only-update", 3};
        fixture.Send(new TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig(MainYaml(9), databases, volatileConfigs, "tenant-a"));
        fixture.Sync();
        auto databaseOnly = fixture.Request(Node(9, {}, "tenant-a"), {}, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(databaseOnly->Get()->Record.GetMainYamlConfig(), MainYaml(2));
        UNIT_ASSERT_VALUES_EQUAL(databaseOnly->Get()->Record.GetDatabaseYamlConfig(), "database-only-update");
    }

    Y_UNIT_TEST(EntryLimitAndLruPromotion) {
        auto validator = MakeIntrusive<TCountingValidator>();
        TConfigsProviderFixture fixture(validator);
        TVector<TConfigItem::TPtr> items;
        for (ui32 id = 1; id <= 129; ++id) {
            items.push_back(LogItem(id, ToString(id), {id}));
        }
        fixture.SetItems(items);
        for (ui32 id = 1; id <= 128; ++id) {
            fixture.Request(Node(id));
        }
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("NodeConfigCacheEntries"), 128);
        fixture.Request(Node(1)); // Promote the oldest, so node 2 is evicted next.
        fixture.Request(Node(129));
        fixture.Request(Node(1));
        fixture.Request(Node(2));
        UNIT_ASSERT_VALUES_EQUAL(validator->Checks, 130);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("NodeConfigCacheEntries"), 128);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("NodeConfigCacheHits", true), 2);
    }

    Y_UNIT_TEST(ByteBudgetEvictsAndOversizeRepliesBypassWithoutEviction) {
        auto validator = MakeIntrusive<TCountingValidator>();
        TConfigsProviderFixture fixture(validator);
        TVector<TConfigItem::TPtr> items;
        const TString payload(4 * 1024 * 1024, 'x');
        for (ui32 id = 1; id <= 17; ++id) {
            items.push_back(LogItem(id, payload, {id}));
        }
        fixture.SetItems(items);
        for (ui32 id = 1; id <= 17; ++id) {
            fixture.Request(Node(id));
        }
        UNIT_ASSERT(fixture.Counter("NodeConfigCacheBytes") <= 64 * 1024 * 1024);
        UNIT_ASSERT(fixture.Counter("NodeConfigCacheEntries") < 17);
        auto checks = validator->Checks;
        fixture.Request(Node(17));
        UNIT_ASSERT_VALUES_EQUAL(validator->Checks, checks);
        fixture.Request(Node(1));
        UNIT_ASSERT_VALUES_EQUAL(validator->Checks, checks + 1);

        // A huge YAML response must not evict a useful ordinary-config response.
        const TString hugeYaml = MainYaml(1) + "#" + TString(65 * 1024 * 1024, 'x') + "\n";
        fixture.Send(new TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig(hugeYaml));
        fixture.Sync();
        fixture.Request(Node(100)); // No matching 4MiB config, one small retained entry.
        const auto retainedBytes = fixture.Counter("NodeConfigCacheBytes");
        checks = validator->Checks;
        fixture.Request(Node(100), {}, true, 1);
        fixture.Request(Node(100), {}, true, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("NodeConfigCacheEntries"), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("NodeConfigCacheBytes"), retainedBytes);
        fixture.Request(Node(101));
        UNIT_ASSERT_VALUES_EQUAL(validator->Checks, checks + 2);
    }

    Y_UNIT_TEST(LargeNameserviceConfigIsValidatedOnceAcrossNodeIds) {
        auto validator = MakeIntrusive<TCountingNameserviceValidator>();
        TConfigsProviderFixture fixture(validator);
        fixture.SetItems({NameserviceItem(5310)});
        for (ui32 id = 1; id <= 32; ++id) {
            auto response = fixture.Request(Node(id));
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetConfig().GetNameserviceConfig().NodeSize(), 5310);
        }
        UNIT_ASSERT_VALUES_EQUAL(validator->Checks, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("NodeConfigCacheHits", true), 31);
    }
}

} // namespace NKikimr::NConsole
