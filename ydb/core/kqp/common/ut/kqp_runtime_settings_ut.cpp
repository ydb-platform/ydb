#include <ydb/core/kqp/common/kqp_runtime_settings.h>

#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NKqp;

namespace {

constexpr TStringBuf DateTime2ModuleName = "DateTime2";
constexpr TStringBuf WriteOffsetWithColonAvailableSinceSetting = "MakeWriteOffsetWithColonAvailableSince";
constexpr TStringBuf WriteOffsetWithColonAvailableSinceValue = "2025.01";

} // namespace

Y_UNIT_TEST_SUITE(TKqpRuntimeSettings) {
    Y_UNIT_TEST(DefaultPinsWriteOffsetWithColon) {
        TKqpRuntimeSettings defaults;
        const auto& settings = defaults.Get();
        UNIT_ASSERT_VALUES_EQUAL(
            settings->GetUdfSetting(DateTime2ModuleName, WriteOffsetWithColonAvailableSinceSetting),
            WriteOffsetWithColonAvailableSinceValue);

        auto proto = NYql::SerializeRuntimeSettingsToProto(*settings);
        auto restored = NYql::DeserializeRuntimeSettingsFromProto(proto);
        UNIT_ASSERT_VALUES_EQUAL(
            restored->GetUdfSetting(DateTime2ModuleName, WriteOffsetWithColonAvailableSinceSetting),
            WriteOffsetWithColonAvailableSinceValue);
    }

    Y_UNIT_TEST(EnsureFillsEmptyProtoOnce) {
        TKqpRuntimeSettings defaults;
        NYql::NProto::TRuntimeSettings proto;
        defaults.ApplyTo(proto);
        defaults.ApplyTo(proto);
        UNIT_ASSERT_VALUES_EQUAL(proto.UdfSettingsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetUdfSettings(0).GetModule(), DateTime2ModuleName);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetUdfSettings(0).RuntimeSettingsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetUdfSettings(0).GetRuntimeSettings(0).GetName(), WriteOffsetWithColonAvailableSinceSetting);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetUdfSettings(0).GetRuntimeSettings(0).GetValue(), WriteOffsetWithColonAvailableSinceValue);
    }

    Y_UNIT_TEST(EnsureKeepsOtherUdfSettings) {
        TKqpRuntimeSettings defaults;
        NYql::NProto::TRuntimeSettings proto;
        auto* other = proto.AddUdfSettings();
        other->SetModule("Other");
        auto* setting = other->AddRuntimeSettings();
        setting->SetName("Key");
        setting->SetValue("Val");

        defaults.ApplyTo(proto);
        UNIT_ASSERT_VALUES_EQUAL(proto.UdfSettingsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetUdfSettings(0).GetModule(), "Other");
        UNIT_ASSERT_VALUES_EQUAL(proto.GetUdfSettings(1).GetModule(), DateTime2ModuleName);
    }

    Y_UNIT_TEST(EnsureKeepsExplicitValue) {
        TKqpRuntimeSettings defaults;
        NYql::NProto::TRuntimeSettings proto;
        auto* udf = proto.AddUdfSettings();
        udf->SetModule(TString(DateTime2ModuleName));
        auto* setting = udf->AddRuntimeSettings();
        setting->SetName(TString(WriteOffsetWithColonAvailableSinceSetting));
        setting->SetValue("2025.05");

        defaults.ApplyTo(proto);
        UNIT_ASSERT_VALUES_EQUAL(proto.UdfSettingsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetUdfSettings(0).GetRuntimeSettings(0).GetValue(), "2025.05");
    }

    Y_UNIT_TEST(EnsureReplacesEmptyValue) {
        TKqpRuntimeSettings defaults;
        NYql::NProto::TRuntimeSettings proto;
        auto* udf = proto.AddUdfSettings();
        udf->SetModule(TString(DateTime2ModuleName));
        auto* setting = udf->AddRuntimeSettings();
        setting->SetName(TString(WriteOffsetWithColonAvailableSinceSetting));

        defaults.ApplyTo(proto);
        UNIT_ASSERT_VALUES_EQUAL(proto.UdfSettingsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetUdfSettings(0).RuntimeSettingsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetUdfSettings(0).GetRuntimeSettings(0).GetValue(), WriteOffsetWithColonAvailableSinceValue);
    }
}
