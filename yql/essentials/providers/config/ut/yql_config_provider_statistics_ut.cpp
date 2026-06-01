#include <yql/essentials/providers/config/yql_config_provider.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/ast/yql_expr.h>

#include <library/cpp/yson/writer.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

namespace NYql {

namespace {

TString CollectStatisticsToString(IDataProvider& provider) {
    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text);
    provider.CollectStatistics(writer, /*totalOnly=*/false);
    return out.Str();
}

} // namespace

Y_UNIT_TEST_SUITE(TConfigProviderStatisticsTest) {

Y_UNIT_TEST(NullConfigProducesNoStatistics) {
    TTypeAnnotationContext typesCtx;
    TExprContext exprCtx;
    auto provider = CreateConfigProvider(typesCtx, nullptr, "testuser", {}, false);

    UNIT_ASSERT(provider->Initialize(exprCtx));

    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text);
    UNIT_ASSERT(!provider->CollectStatistics(writer, false));
}

Y_UNIT_TEST(RuntimeSettingWithoutActivationProducesNoStatistics) {
    TTypeAnnotationContext typesCtx;
    TExprContext exprCtx;

    TGatewaysConfig config;
    auto* setting = config.MutableRuntimeSettings()->AddHostSettings();
    setting->SetName("DatumValidation");
    setting->SetValue("true");

    auto provider = CreateConfigProvider(typesCtx, &config, "testuser", {}, false);
    UNIT_ASSERT(provider->Initialize(exprCtx));

    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text);
    UNIT_ASSERT(!provider->CollectStatistics(writer, false));
}

Y_UNIT_TEST(RuntimeSettingWithMatchingActivationAppearsInStatistics) {
    TTypeAnnotationContext typesCtx;
    TExprContext exprCtx;

    TGatewaysConfig config;
    auto* setting = config.MutableRuntimeSettings()->AddHostSettings();
    setting->SetName("TestHostSetting");
    setting->SetValue("true");
    setting->MutableActivation()->AddIncludeUsers("testuser");

    auto provider = CreateConfigProvider(typesCtx, &config, "testuser", {}, false);
    UNIT_ASSERT(provider->Initialize(exprCtx));

    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text);
    UNIT_ASSERT(provider->CollectStatistics(writer, false));
    UNIT_ASSERT(out.Str().Contains("Activation:RuntimeSetting/TestHostSetting"));
}

Y_UNIT_TEST(RuntimeSettingWithNonMatchingActivationProducesNoStatistics) {
    TTypeAnnotationContext typesCtx;
    TExprContext exprCtx;

    TGatewaysConfig config;
    auto* setting = config.MutableRuntimeSettings()->AddHostSettings();
    setting->SetName("TestHostSetting");
    setting->SetValue("true");
    setting->MutableActivation()->AddIncludeUsers("otheruser");

    auto provider = CreateConfigProvider(typesCtx, &config, "testuser", {}, false);
    UNIT_ASSERT(provider->Initialize(exprCtx));

    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text);
    UNIT_ASSERT(!provider->CollectStatistics(writer, false));
}

Y_UNIT_TEST(OnlyActivatedRuntimeSettingsAppearInStatistics) {
    TTypeAnnotationContext typesCtx;
    TExprContext exprCtx;

    TGatewaysConfig config;
    auto* runtimeSettings = config.MutableRuntimeSettings();

    {
        auto* setting = runtimeSettings->AddHostSettings();
        setting->SetName("DatumValidation");
        setting->SetValue("true");
        // no activation — never recorded in statistics
    }
    {
        auto* setting = runtimeSettings->AddHostSettings();
        setting->SetName("TestHostSetting");
        setting->SetValue("true");
        setting->MutableActivation()->AddIncludeUsers("testuser");
    }

    auto provider = CreateConfigProvider(typesCtx, &config, "testuser", {}, false);
    UNIT_ASSERT(provider->Initialize(exprCtx));

    const TString statistics = CollectStatisticsToString(*provider);
    UNIT_ASSERT(statistics.Contains("Activation:RuntimeSetting/TestHostSetting"));
    UNIT_ASSERT(!statistics.Contains("Activation:RuntimeSetting/DatumValidation"));
}

Y_UNIT_TEST(CoreFlagWithMatchingActivationAppearsInStatistics) {
    TTypeAnnotationContext typesCtx;
    TExprContext exprCtx;

    TGatewaysConfig config;
    auto* coreFlag = config.MutableYqlCore()->AddFlags();
    coreFlag->SetName("UseBlocks");
    coreFlag->MutableActivation()->AddIncludeUsers("testuser");

    auto provider = CreateConfigProvider(typesCtx, &config, "testuser", {}, false);
    UNIT_ASSERT(provider->Initialize(exprCtx));

    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text);
    UNIT_ASSERT(provider->CollectStatistics(writer, false));
    UNIT_ASSERT(out.Str().Contains("Activation:UseBlocks"));
}

Y_UNIT_TEST(CoreFlagWithNonMatchingActivationProducesNoStatistics) {
    TTypeAnnotationContext typesCtx;
    TExprContext exprCtx;

    TGatewaysConfig config;
    auto* coreFlag = config.MutableYqlCore()->AddFlags();
    coreFlag->SetName("UseBlocks");
    coreFlag->MutableActivation()->AddIncludeUsers("otheruser");

    auto provider = CreateConfigProvider(typesCtx, &config, "testuser", {}, false);
    UNIT_ASSERT(provider->Initialize(exprCtx));

    TStringStream out;
    NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Text);
    UNIT_ASSERT(!provider->CollectStatistics(writer, false));
}

Y_UNIT_TEST(BothCoreAndRuntimeActivationsAppearInStatistics) {
    TTypeAnnotationContext typesCtx;
    TExprContext exprCtx;

    TGatewaysConfig config;

    auto* coreFlag = config.MutableYqlCore()->AddFlags();
    coreFlag->SetName("UseBlocks");
    coreFlag->MutableActivation()->AddIncludeUsers("testuser");

    auto* setting = config.MutableRuntimeSettings()->AddHostSettings();
    setting->SetName("TestHostSetting");
    setting->SetValue("true");
    setting->MutableActivation()->AddIncludeUsers("testuser");

    auto provider = CreateConfigProvider(typesCtx, &config, "testuser", {}, false);
    UNIT_ASSERT(provider->Initialize(exprCtx));

    const TString statistics = CollectStatisticsToString(*provider);
    UNIT_ASSERT(statistics.Contains("Activation:UseBlocks"));
    UNIT_ASSERT(statistics.Contains("Activation:RuntimeSetting/TestHostSetting"));
}

} // Y_UNIT_TEST_SUITE(TConfigProviderStatisticsTest)

} // namespace NYql
