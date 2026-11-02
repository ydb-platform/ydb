#include <yql/essentials/minikql/runtime_settings/runtime_settings_hash.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_configuration.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/string/cast.h>

namespace NYql {

namespace {

TString DumpRuntimeSettingsToString(const TRuntimeSettings& settings) {
    TStringStream stream;
    TRuntimeSettingsConfiguration::Get().SerializeStaticSettings(settings, [&](const TString&, const TString& value) {
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

Y_UNIT_TEST(ExplicitDefaultsAreDistinctFromUnset) {
    const auto unsetHash = StableHashRuntimeSettings(TRuntimeSettings{});
    auto check = [&](auto field) {
        TRuntimeSettings settings;
        auto& setting = settings.*field;
        setting.Set(setting.Get());
        UNIT_ASSERT_VALUES_UNEQUAL(StableHashRuntimeSettings(settings), unsetHash);
    };
    check(&TRuntimeSettings::DatumValidation);
    check(&TRuntimeSettings::TestHostSetting);
    check(&TRuntimeSettings::UdfProfileEnable);
    check(&TRuntimeSettings::UdfProfileMinTimeUs);
    check(&TRuntimeSettings::UdfProfileGraceCount);
    check(&TRuntimeSettings::UdfProfileExcludeModules);
    check(&TRuntimeSettings::UdfProfileHLLPrecision);
}

Y_UNIT_TEST(HostSettingPositionsAreDistinct) {
    for (const bool value : {false, true}) {
        TRuntimeSettings first;
        first.DatumValidation.Set(EDatumValidationMode::None);
        first.TestHostSetting.Set(value);

        TRuntimeSettings second;
        second.DatumValidation.Set(EDatumValidationMode::None);
        second.UdfProfileEnable.Set(value);

        const auto firstHash = StableHashRuntimeSettings(first);
        const auto secondHash = StableHashRuntimeSettings(second);
        UNIT_ASSERT_VALUES_UNEQUAL(firstHash, secondHash);
    }
}

Y_UNIT_TEST(ModuleSetOrderIsStable) {
    for (size_t count : {0, 1, 2, 16, 17, 80}) {
        TUdfModuleSet forward;
        TUdfModuleSet reverse;
        for (size_t index = 0; index < count; ++index) {
            forward.insert(ToString(index));
            reverse.insert(ToString(count - index - 1));
            reverse.insert(ToString(count - index - 1));
        }
        UNIT_ASSERT_VALUES_EQUAL(forward.size(), count);
        UNIT_ASSERT_VALUES_EQUAL(reverse.size(), count);
        TRuntimeSettings left;
        TRuntimeSettings right;
        left.UdfProfileExcludeModules.Set(forward);
        right.UdfProfileExcludeModules.Set(reverse);
        UNIT_ASSERT_VALUES_EQUAL(StableHashRuntimeSettings(left), StableHashRuntimeSettings(right));
    }
}

Y_UNIT_TEST(ModuleSetBoundariesAreDistinct) {
    TRuntimeSettings left;
    TRuntimeSettings right;
    left.UdfProfileExcludeModules.Set({"a", "bc"});
    right.UdfProfileExcludeModules.Set({"ab", "c"});
    UNIT_ASSERT_VALUES_UNEQUAL(StableHashRuntimeSettings(left), StableHashRuntimeSettings(right));
    left.UdfProfileExcludeModules.Set({"", "a"});
    right.UdfProfileExcludeModules.Set({"a"});
    UNIT_ASSERT_VALUES_UNEQUAL(StableHashRuntimeSettings(left), StableHashRuntimeSettings(right));
}

Y_UNIT_TEST(EmptyAndBinaryStringsAreDistinct) {
    THashSet<TRuntimeSettingsStableHash> hashes;
    for (const auto& value : {TString{}, TString("x"), TString("x\0", 2), TString("\0x", 2)}) {
        TRuntimeSettings settings;
        settings.SetUdfSetting("", "", value);
        UNIT_ASSERT(hashes.insert(StableHashRuntimeSettings(settings)).second);
    }
}

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

    const auto emptyHash = StableHashRuntimeSettings(*MakeRuntimeSettingsMutable());
    UNIT_ASSERT_VALUES_UNEQUAL(hash1, emptyHash);
}

Y_UNIT_TEST(StableHashDiffersByContent) {
    auto config1 = MakeRuntimeSettingsMutable();
    config1->DatumValidation.Set(EDatumValidationMode::Cheap);

    auto config2 = MakeRuntimeSettingsMutable();
    config2->DatumValidation.Set(EDatumValidationMode::Expensive);

    UNIT_ASSERT_VALUES_UNEQUAL(StableHashRuntimeSettings(*config1), StableHashRuntimeSettings(*config2));
}

Y_UNIT_TEST(StableHashUdfSettingOrderIndependent) {
    for (size_t count : {0, 1, 5, 10, 50}) {
        TRuntimeSettings forward;
        TRuntimeSettings reverse;
        for (size_t module = 0; module < count; ++module) {
            for (size_t setting = 0; setting < count; ++setting) {
                const auto reverseModule = count - module - 1;
                const auto reverseSetting = count - setting - 1;
                forward.SetUdfSetting(ToString(module), ToString(setting), ToString(module + setting));
                reverse.SetUdfSetting(ToString(reverseModule), ToString(reverseSetting), ToString(reverseModule + reverseSetting));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(StableHashRuntimeSettings(forward), StableHashRuntimeSettings(reverse));
    }
}

Y_UNIT_TEST(StableHashUdfModuleBoundaryIsDistinct) {
    auto config1 = MakeRuntimeSettingsMutable();
    config1->SetUdfSetting("1", "2", "3");
    config1->SetUdfSetting("1", "4", "5");
    config1->SetUdfSetting("1", "6", "7");
    config1->SetUdfSetting("1", "8", "9");

    auto config2 = MakeRuntimeSettingsMutable();
    config2->SetUdfSetting("1", "2", "3");
    config2->SetUdfSetting("4", "5", "6");
    config2->SetUdfSetting("7", "8", "9");
    UNIT_ASSERT_VALUES_EQUAL(DumpRuntimeSettingsToString(*config1), DumpRuntimeSettingsToString(*config2));
    UNIT_ASSERT_VALUES_UNEQUAL(StableHashRuntimeSettings(*config1), StableHashRuntimeSettings(*config2));
}

} // Y_UNIT_TEST_SUITE(TRuntimeSettingsHashTest)

} // namespace NYql
