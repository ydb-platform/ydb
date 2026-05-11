#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TRuntimeSettingsSerializationTest) {

Y_UNIT_TEST(Serialization) {
    auto config = MakeIntrusive<TRuntimeSettingsConfiguration>();
    config->DatumValidation.Set(true);
    config->TestHostSetting.Set(true);
    config->SetUdfSetting("MyModule", "Key", "Val");

    const TString data = SerializeRuntimeSettingsToString(*config);

    NProto::TRuntimeSettings proto;
    UNIT_ASSERT(proto.ParseFromString(data));

    THashMap<TString, TString> hostSettings;
    for (const auto& s : proto.GetHostSettings()) {
        hostSettings[s.GetName()] = s.GetValue();
    }
    UNIT_ASSERT_VALUES_EQUAL(proto.HostSettingsSize(), 2);
    UNIT_ASSERT_VALUES_EQUAL(hostSettings.at("DatumValidation"), "true");
    UNIT_ASSERT_VALUES_EQUAL(hostSettings.at("TestHostSetting"), "true");

    UNIT_ASSERT_VALUES_EQUAL(proto.UdfSettingsSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetUdfSettings(0).GetModule(), "MyModule");
    UNIT_ASSERT_VALUES_EQUAL(proto.GetUdfSettings(0).RuntimeSettingsSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(proto.GetUdfSettings(0).GetRuntimeSettings(0).GetName(), "Key");
    UNIT_ASSERT_VALUES_EQUAL(proto.GetUdfSettings(0).GetRuntimeSettings(0).GetValue(), "Val");
}

Y_UNIT_TEST(Deserialization) {
    NProto::TRuntimeSettings proto;
    auto* datumValidation = proto.AddHostSettings();
    datumValidation->SetName("DatumValidation");
    datumValidation->SetValue("true");
    auto* testHostSetting = proto.AddHostSettings();
    testHostSetting->SetName("TestHostSetting");
    testHostSetting->SetValue("true");

    auto* udfSettings = proto.AddUdfSettings();
    udfSettings->SetModule("MyModule");
    auto* udfSetting = udfSettings->AddRuntimeSettings();
    udfSetting->SetName("Key");
    udfSetting->SetValue("Val");

    TString data;
    UNIT_ASSERT(proto.SerializeToString(&data));

    auto config = CreateRuntimeSettingsFromString(data, TString{}, nullptr);

    UNIT_ASSERT_VALUES_EQUAL(config->DatumValidation.Get(), true);
    UNIT_ASSERT_VALUES_EQUAL(config->TestHostSetting.Get(), true);
    UNIT_ASSERT_VALUES_EQUAL(config->GetUdfSetting("MyModule", "Key"), "Val");
    UNIT_ASSERT_VALUES_EQUAL(config->GetUdfSetting("MyModule", "Key2"), "");
    UNIT_ASSERT_VALUES_EQUAL(config->GetUdfSetting("MyModule2", "Key"), "");
    UNIT_ASSERT_VALUES_EQUAL(config->GetUdfSetting("MyModule2", "Key2"), "");
}

Y_UNIT_TEST(HostSettingActivation50Percent) {
    NProto::TRuntimeSettings proto;
    auto* hostSetting = proto.AddHostSettings();
    hostSetting->SetName("DatumValidation");
    hostSetting->SetValue("true");
    hostSetting->MutableActivation()->SetPercentage(50);

    TString data;
    UNIT_ASSERT(proto.SerializeToString(&data));

    constexpr int Iterations = 10000;
    int activatedCount = 0;
    for (int i = 0; i < Iterations; ++i) {
        auto config = CreateRuntimeSettingsFromString(data, TString{}, nullptr);
        if (config->DatumValidation.Get()) {
            ++activatedCount;
        }
    }

    UNIT_ASSERT_GE(activatedCount, Iterations / 4);
    UNIT_ASSERT_LE(activatedCount, Iterations * 3 / 4);
}

Y_UNIT_TEST(UdfSettingActivation50Percent) {
    NProto::TRuntimeSettings proto;
    auto* udfSettings = proto.AddUdfSettings();
    udfSettings->SetModule("MyModule");
    auto* udfSetting = udfSettings->AddRuntimeSettings();
    udfSetting->SetName("Key");
    udfSetting->SetValue("Val");
    udfSetting->MutableActivation()->SetPercentage(50);

    TString data;
    UNIT_ASSERT(proto.SerializeToString(&data));

    constexpr int Iterations = 10000;
    int activatedCount = 0;
    for (int i = 0; i < Iterations; ++i) {
        auto config = CreateRuntimeSettingsFromString(data, TString{}, nullptr);
        if (!config->GetUdfSetting("MyModule", "Key").empty()) {
            ++activatedCount;
        }
    }

    UNIT_ASSERT_GE(activatedCount, Iterations / 4);
    UNIT_ASSERT_LE(activatedCount, Iterations * 3 / 4);
}

} // Y_UNIT_TEST_SUITE(TRuntimeSettingsSerializationTest)

} // namespace NYql
