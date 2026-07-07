#include <yql/essentials/minikql/runtime_settings/runtime_settings_hash.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_configuration.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

namespace {

TString DumpRuntimeSettingsToString(const TRuntimeSettings& settings) {
    TStringStream stream;
    TRuntimeSettingsConfiguration configuration(settings);
    configuration.SerializeStaticSettings([&](const TString&, const TString& value) {
        stream << value;
    });
    for (const auto& [module, settings] : settings.GetUdfSettings()) {
        stream << " " << module;
        for (const auto& [key, value] : settings) {
            stream << " " << key << " " << value;
        }
    }
    return stream.Str();
}

} // namespace

Y_UNIT_TEST_SUITE(TRuntimeSettingsHashTest) {

Y_UNIT_TEST(StableHashIsStable) {
    std::array<TRuntimeSettings::TPtr, 2> configs;
    for (auto& config : configs) {
        config = MakeRuntimeSettingsMutable();
        config->DatumValidation.Set(EDatumValidationMode::Cheap);
        config->SetUdfSetting("ModuleB", "Key2", "Val2");
        config->SetUdfSetting("ModuleA", "Key1", "Val1");
    }

    const auto hash1 = StableHashRuntimeSettings(*configs[0]);
    const auto hash2 = StableHashRuntimeSettings(*configs[1]);
    UNIT_ASSERT_VALUES_EQUAL(hash1, hash2);

    const auto emptyHash = StableHashRuntimeSettings(*MakeIntrusive<TRuntimeSettingsConfiguration>());
    UNIT_ASSERT_VALUES_UNEQUAL(hash1, emptyHash);
}

Y_UNIT_TEST(StableHashDiffersByContent) {
    auto config1 = MakeIntrusive<TRuntimeSettingsConfiguration>();
    config1->DatumValidation.Set(EDatumValidationMode::Cheap);

    auto config2 = MakeIntrusive<TRuntimeSettingsConfiguration>();
    config2->DatumValidation.Set(EDatumValidationMode::Expensive);

    UNIT_ASSERT_VALUES_UNEQUAL(StableHashRuntimeSettings(*config1), StableHashRuntimeSettings(*config2));
}

Y_UNIT_TEST(StableHashUdfSettingOrderIndependent) {
    auto config1 = MakeIntrusive<TRuntimeSettingsConfiguration>();
    config1->SetUdfSetting("ModuleA", "Key1", "Val1");
    config1->SetUdfSetting("ModuleA", "Key2", "Val2");
    config1->SetUdfSetting("ModuleB", "Key3", "Val3");

    auto config2 = MakeIntrusive<TRuntimeSettingsConfiguration>();
    config2->SetUdfSetting("ModuleB", "Key3", "Val3");
    config2->SetUdfSetting("ModuleA", "Key2", "Val2");
    config2->SetUdfSetting("ModuleA", "Key1", "Val1");

    UNIT_ASSERT_VALUES_EQUAL(StableHashRuntimeSettings(*config1), StableHashRuntimeSettings(*config2));
}

Y_UNIT_TEST(StableHashUdfModuleBoundaryIsDistinct) {
    auto config1 = MakeIntrusive<TRuntimeSettingsConfiguration>();
    config1->SetUdfSetting("1", "2", "3");
    config1->SetUdfSetting("1", "4", "5");
    config1->SetUdfSetting("1", "6", "7");
    config1->SetUdfSetting("1", "8", "9");

    auto config2 = MakeIntrusive<TRuntimeSettingsConfiguration>();
    config2->SetUdfSetting("1", "2", "3");
    config2->SetUdfSetting("4", "5", "6");
    config2->SetUdfSetting("7", "8", "9");
    UNIT_ASSERT_VALUES_EQUAL(DumpRuntimeSettingsToString(*config1), DumpRuntimeSettingsToString(*config2));
    UNIT_ASSERT_VALUES_UNEQUAL(StableHashRuntimeSettings(*config1), StableHashRuntimeSettings(*config2));
}

} // Y_UNIT_TEST_SUITE(TRuntimeSettingsHashTest)

} // namespace NYql
