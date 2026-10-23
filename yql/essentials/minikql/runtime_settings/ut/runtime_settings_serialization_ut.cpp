#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>
#include <yql/essentials/core/qplayer/storage/memory/yql_qstorage_memory.h>

#include <library/cpp/testing/unittest/registar.h>

#include <future>
#include <vector>

namespace NYql {

Y_UNIT_TEST_SUITE(TRuntimeSettingsSerializationTest) {

Y_UNIT_TEST(ResolvedMetadataIsImmutableAndReused) {
    auto settings = MakeRuntimeSettingsMutable();
    settings->DatumValidation.Set(EDatumValidationMode::Cheap);
    settings->SetUdfSetting("Module", "Option", "value");
    const auto proto = SerializeRuntimeSettingsToProto(*settings);
    const auto first = ResolveRuntimeSettings(proto);
    const auto second = ResolveRuntimeSettings(proto);
    UNIT_ASSERT(first == second);
    UNIT_ASSERT_VALUES_EQUAL(first->StableHash, StableHashRuntimeSettings(*first->Settings));
    settings->DatumValidation.Set(EDatumValidationMode::Expensive);
    settings->SetUdfSetting("Module", "Option", "changed");
    UNIT_ASSERT_VALUES_EQUAL(first->Settings->DatumValidation.Get(), EDatumValidationMode::Cheap);
    UNIT_ASSERT_VALUES_EQUAL(first->Settings->GetUdfSetting("Module", "Option"), "value");
    const auto changed = ResolveRuntimeSettings(SerializeRuntimeSettingsToProto(*settings));
    UNIT_ASSERT_VALUES_UNEQUAL(first->StableHash, changed->StableHash);
}

Y_UNIT_TEST(ResolvedHashPreservesEncodingSemantics) {
    const auto defaults = ResolveRuntimeSettings(NProto::TRuntimeSettings{});
    UNIT_ASSERT_VALUES_EQUAL(defaults->StableHash, StableHashRuntimeSettings(*MakeRuntimeSettings()));
    NProto::TRuntimeSettings explicitDefault;
    auto* setting = explicitDefault.AddHostSettings();
    setting->SetName("TestHostSetting");
    setting->SetValue("false");
    const auto explicitSettings = ResolveRuntimeSettings(explicitDefault);
    // Explicit defaults have historically differed from absent settings in the
    // stable hash. Keep exactly the existing deserializer/hash behavior.
    UNIT_ASSERT_VALUES_EQUAL(explicitSettings->StableHash,
                             StableHashRuntimeSettings(*DeserializeRuntimeSettingsFromProto(explicitDefault)));
    UNIT_ASSERT_VALUES_EQUAL(explicitSettings->Settings->TestHostSetting.Get(), false);
}

Y_UNIT_TEST(ResolvedMetadataDoesNotCacheActivationDecisions) {
    NProto::TRuntimeSettings proto;
    auto* setting = proto.AddHostSettings();
    setting->SetName("TestHostSetting");
    setting->SetValue("true");
    setting->MutableActivation()->SetPercentage(0);
    const auto first = ResolveRuntimeSettings(proto);
    const auto second = ResolveRuntimeSettings(proto);
    UNIT_ASSERT(first != second);
    UNIT_ASSERT(!first->Settings->TestHostSetting.Get());
    setting->ClearActivation();
    const auto activated = ResolveRuntimeSettings(proto);
    UNIT_ASSERT(activated->Settings->TestHostSetting.Get());
    UNIT_ASSERT(activated == ResolveRuntimeSettings(proto));
    setting->SetValue("not-a-bool");
    UNIT_ASSERT_EXCEPTION(ResolveRuntimeSettings(proto), yexception);
}

Y_UNIT_TEST(ResolvedMetadataConcurrentIsolation) {
    std::vector<std::future<void>> threads;
    for (ui32 thread = 0; thread < 8; ++thread) {
        threads.push_back(std::async(std::launch::async, [thread] {
            for (ui32 i = 0; i < 200; ++i) {
                auto settings = MakeRuntimeSettingsMutable();
                settings->SetUdfSetting("Concurrent", "Key", ToString((i + thread) % 7));
                const auto proto = SerializeRuntimeSettingsToProto(*settings);
                const auto metadata = ResolveRuntimeSettings(proto);
                UNIT_ASSERT_VALUES_EQUAL(metadata->Settings->GetUdfSetting("Concurrent", "Key"),
                                         ToString((i + thread) % 7));
                UNIT_ASSERT_VALUES_EQUAL(metadata->StableHash, StableHashRuntimeSettings(*settings));
            }
        }));
    }
    for (auto& thread : threads) {
        thread.get();
    }
}

Y_UNIT_TEST(ResolvedMetadataOutlivesEvictionAndLargeSettingsBypass) {
    NProto::TRuntimeSettings proto;
    auto* udf = proto.AddUdfSettings();
    udf->SetModule("Eviction");
    auto* setting = udf->AddRuntimeSettings();
    setting->SetName("Key");
    setting->SetValue("original");
    const auto retained = ResolveRuntimeSettings(proto);
    for (ui32 i = 0; i < 1200; ++i) {
        setting->SetValue(ToString(i));
        ResolveRuntimeSettings(proto);
    }
    UNIT_ASSERT_VALUES_EQUAL(retained->Settings->GetUdfSetting("Eviction", "Key"), "original");
    setting->SetValue(TString(20000, 'x'));
    const auto large = ResolveRuntimeSettings(proto);
    UNIT_ASSERT_VALUES_EQUAL(large->Settings->GetUdfSetting("Eviction", "Key").size(), 20000);
    UNIT_ASSERT_VALUES_EQUAL(large->StableHash,
                             StableHashRuntimeSettings(*DeserializeRuntimeSettingsFromProto(proto)));
}

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
