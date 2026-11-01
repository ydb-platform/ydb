#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>
#include <yql/essentials/core/qplayer/storage/memory/yql_qstorage_memory.h>
#include <yql/essentials/providers/common/config/yql_dispatch.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/string/join.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TRuntimeSettingsSerializationTest) {

Y_UNIT_TEST(Serialization) {
    auto config = MakeRuntimeSettingsMutable();
    config->DatumValidation.Set(NYql::EDatumValidationMode::Cheap);
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
    UNIT_ASSERT_VALUES_EQUAL(hostSettings.at("DatumValidation"), "Cheap");
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
    datumValidation->SetValue("Cheap");
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

    auto config = CreateRuntimeSettingsFromString(data);

    UNIT_ASSERT_VALUES_EQUAL(config->DatumValidation.Get(), NYql::EDatumValidationMode::Cheap);
    UNIT_ASSERT_VALUES_EQUAL(config->TestHostSetting.Get(), true);
    UNIT_ASSERT_VALUES_EQUAL(config->GetUdfSetting("MyModule", "Key"), "Val");
    UNIT_ASSERT_VALUES_EQUAL(config->GetUdfSetting("MyModule", "Key2"), "");
    UNIT_ASSERT_VALUES_EQUAL(config->GetUdfSetting("MyModule2", "Key"), "");
    UNIT_ASSERT_VALUES_EQUAL(config->GetUdfSetting("MyModule2", "Key2"), "");
}

Y_UNIT_TEST(ModuleSetRoundTrip) {
    for (const auto value : {"ModuleC,ModuleA,ModuleB,ModuleA", "ModuleC|ModuleA;ModuleB ModuleA"}) {
        NProto::TRuntimeSettings proto;
        auto* setting = proto.AddHostSettings();
        setting->SetName("UdfProfileExcludeModules");
        setting->SetValue(value);
        const auto config = DeserializeRuntimeSettingsFromProto(proto);
        const auto& modules = config->UdfProfileExcludeModules.Get();
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", modules), "ModuleA,ModuleB,ModuleC");
        for (const TStringBuf module : {"ModuleA", "ModuleB", "ModuleC"}) {
            UNIT_ASSERT(modules.contains(module));
        }
        UNIT_ASSERT(!modules.contains(TStringBuf("Missing")));
        const auto serialized = SerializeRuntimeSettingsToProto(*config);
        UNIT_ASSERT_VALUES_EQUAL(serialized.HostSettingsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(serialized.GetHostSettings(0).GetValue(), "ModuleA,ModuleB,ModuleC");
    }
}

Y_UNIT_TEST(HostSettingNamesAreNotNormalized) {
    for (const auto name : {"TestHostSetting", "TESTHOSTSETTING", "test_host_setting", "tEsT_HoSt_SeTtInG_", "", "_TestHostSetting", "UnknownSetting", "Test__HostSetting"}) {
        NProto::TRuntimeSettings proto;
        auto* setting = proto.AddHostSettings();
        setting->SetName(name);
        setting->SetValue("true");
        const auto config = DeserializeRuntimeSettingsFromProto(proto);
        UNIT_ASSERT_VALUES_EQUAL(config->TestHostSetting.IsSet(), TStringBuf(name) == "TestHostSetting");
        UNIT_ASSERT_VALUES_EQUAL(setting->GetName(), name);
    }
}

Y_UNIT_TEST(DispatcherNameNormalization) {
    struct TTestSettings {
        NCommon::TConfSetting<bool> Value;
    };
    for (const bool normalizeSettingNames : {false, true}) {
        NCommon::TDispatcher<TTestSettings> dispatcher("", false, normalizeSettingNames);
        dispatcher.AddSetting("TestSetting", &TTestSettings::Value);
        for (const auto name : {"TestSetting", "TESTSETTING", "test_setting"}) {
            const bool matches = normalizeSettingNames || TStringBuf(name) == "TestSetting";
            TTestSettings settings;
            UNIT_ASSERT(dispatcher.Dispatch(settings, NCommon::ALL_CLUSTERS, name, TString("true"),
                                            NCommon::ISettingDispatcher::EStage::CONFIG, NCommon::GetDefaultErrorCallback(), {}));
            UNIT_ASSERT_VALUES_EQUAL(settings.Value.Get(NCommon::ALL_CLUSTERS).Defined(), matches);
            UNIT_ASSERT_VALUES_EQUAL(dispatcher.IsRuntime(name), matches);
        }
        TVector<TString> names;
        dispatcher.Enumerate([&](std::string_view name) { names.emplace_back(name); });
        UNIT_ASSERT_VALUES_EQUAL(names, TVector<TString>{"TestSetting"});
    }
}

Y_UNIT_TEST(DeserializationPreservesActivationValidation) {
    for (const bool host : {false, true}) {
        NProto::TRuntimeSettings proto;
        auto* setting = host ? proto.AddHostSettings() : proto.AddUdfSettings()->AddRuntimeSettings();
        setting->SetName("TestHostSetting");
        setting->SetValue("true");
        setting->MutableActivation()->SetPercentage(0);
        auto config = DeserializeRuntimeSettingsFromProto(proto);
        UNIT_ASSERT(!config->TestHostSetting.IsSet());
        UNIT_ASSERT(config->GetUdfSettings().empty());

        setting->MutableActivation()->SetPercentage(100);
        UNIT_ASSERT_EXCEPTION_CONTAINS(DeserializeRuntimeSettingsFromProto(proto), yexception, "Activation is not allowed");
    }
}

Y_UNIT_TEST(HostSettingActivation50Percent) {
    NProto::TRuntimeSettings proto;
    auto* hostSetting = proto.AddHostSettings();
    hostSetting->SetName("TestHostSetting");
    hostSetting->SetValue("true");
    hostSetting->MutableActivation()->SetPercentage(50);

    constexpr int Iterations = 10000;
    int activatedCount = 0;
    for (int i = 0; i < Iterations; ++i) {
        auto config = CreateRuntimeSettingsFromProto(proto, TString{}, nullptr, TQContext(), {});
        if (config->TestHostSetting.Get()) {
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

    constexpr int Iterations = 10000;
    int activatedCount = 0;
    for (int i = 0; i < Iterations; ++i) {
        auto config = CreateRuntimeSettingsFromProto(proto, TString{}, nullptr, TQContext(), {});
        if (!config->GetUdfSetting("MyModule", "Key").empty()) {
            ++activatedCount;
        }
    }

    UNIT_ASSERT_GE(activatedCount, Iterations / 4);
    UNIT_ASSERT_LE(activatedCount, Iterations * 3 / 4);
}

Y_UNIT_TEST(ActivationStatePreservedAfterQContextRoundTrip) {
    NProto::TRuntimeSettings proto;

    auto* hostSetting = proto.AddHostSettings();
    hostSetting->SetName("TestHostSetting");
    hostSetting->SetValue("true");
    hostSetting->MutableActivation()->SetPercentage(50);

    auto* udfSettings = proto.AddUdfSettings();
    udfSettings->SetModule("MyModule");
    auto* udfSetting = udfSettings->AddRuntimeSettings();
    udfSetting->SetName("Key");
    udfSetting->SetValue("Val");
    udfSetting->MutableActivation()->SetPercentage(50);

    constexpr int Iterations = 400;
    for (int i = 0; i < Iterations; ++i) {
        auto qStorage = MakeMemoryQStorage();

        auto writer = qStorage->MakeWriter("op", {});
        auto capturedConfig = CreateRuntimeSettingsFromProto(
            proto, TString{}, nullptr, TQContext(writer), {});
        writer->Commit().GetValueSync();

        const bool capturedHostActivated = capturedConfig->TestHostSetting.Get();
        const bool capturedUdfActivated = !capturedConfig->GetUdfSetting("MyModule", "Key").empty();

        auto replayedConfig = CreateRuntimeSettingsFromProto(
            proto, TString{}, nullptr, TQContext(qStorage->MakeReader("op", {})), {});

        UNIT_ASSERT_VALUES_EQUAL(capturedHostActivated, replayedConfig->TestHostSetting.Get());
        UNIT_ASSERT_VALUES_EQUAL(capturedUdfActivated, !replayedConfig->GetUdfSetting("MyModule", "Key").empty());
    }
}

} // Y_UNIT_TEST_SUITE(TRuntimeSettingsSerializationTest)

} // namespace NYql
