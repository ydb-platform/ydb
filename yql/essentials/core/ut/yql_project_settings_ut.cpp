#include <yql/essentials/core/project_settings/yql_project_settings.h>

#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/config/yql_config_provider.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {
namespace {

class TAcceptingProjectSettingsProvider: public TDataProviderBase {
public:
    explicit TAcceptingProjectSettingsProvider(TString accepted)
        : Accepted_(std::move(accepted))
    {
    }

    TStringBuf GetName() const override {
        return "test";
    }
    bool AcceptsProjectSetting(const TProjectSettings::TPragma& pragma) const override {
        return pragma.Name == Accepted_;
    }

private:
    const TString Accepted_;
};

} // namespace

Y_UNIT_TEST_SUITE(TProjectSettingsTest) {

Y_UNIT_TEST(DefaultIsEmpty) {
    TProjectSettings settings;
    UNIT_ASSERT(settings.Pragmas.empty());
    UNIT_ASSERT(!settings.FindPragma("yt.StaticPool"));
}

Y_UNIT_TEST(AddSingleValuePragma) {
    TProjectSettings settings;
    settings.AddPragma("yt.StaticPool", "my-pool");

    const auto* pragma = settings.FindPragma("yt.StaticPool");
    UNIT_ASSERT(pragma);
    UNIT_ASSERT_VALUES_EQUAL(pragma->Name, "yt.StaticPool");
    UNIT_ASSERT_VALUES_EQUAL(pragma->Value.size(), 1U);
    UNIT_ASSERT_VALUES_EQUAL(pragma->Value[0], "my-pool");
}

Y_UNIT_TEST(AddMultiValuePragma) {
    TProjectSettings settings;
    settings.AddPragma("yt.StaticPool", TVector<TString>{"pool-1", "pool-2"});

    const auto* pragma = settings.FindPragma("yt.StaticPool");
    UNIT_ASSERT(pragma);
    UNIT_ASSERT_VALUES_EQUAL(pragma->Value.size(), 2U);
    UNIT_ASSERT_VALUES_EQUAL(pragma->Value[0], "pool-1");
    UNIT_ASSERT_VALUES_EQUAL(pragma->Value[1], "pool-2");
}

Y_UNIT_TEST(AddEmptyValuePragma) {
    TProjectSettings settings;
    settings.AddPragma("yt.StaticPool", TVector<TString>{});

    const auto* pragma = settings.FindPragma("yt.StaticPool");
    UNIT_ASSERT(pragma);
    UNIT_ASSERT(pragma->Value.empty());
}

Y_UNIT_TEST(AddEmptyNamePragma) {
    TProjectSettings settings;
    settings.AddPragma(TString{}, "value");

    const auto* pragma = settings.FindPragma(TStringBuf{});
    UNIT_ASSERT(pragma);
    UNIT_ASSERT_VALUES_EQUAL(pragma->Name, TString{});
    UNIT_ASSERT_VALUES_EQUAL(pragma->Value.size(), 1U);
    UNIT_ASSERT_VALUES_EQUAL(pragma->Value[0], "value");
}

Y_UNIT_TEST(FindReturnsFirstMatch) {
    TProjectSettings settings;
    settings.AddPragma("yt.StaticPool", "first");
    settings.AddPragma("yt.StaticPool", "second");

    const auto* pragma = settings.FindPragma("yt.StaticPool");
    UNIT_ASSERT(pragma);
    UNIT_ASSERT_VALUES_EQUAL(pragma->Value.size(), 1U);
    UNIT_ASSERT_VALUES_EQUAL(pragma->Value[0], "first");
}

} // Y_UNIT_TEST_SUITE(TProjectSettingsTest)

Y_UNIT_TEST_SUITE(TProjectSettingsValidation) {
Y_UNIT_TEST(ProgramValidatesTranslatorAndProviderSettingsTogether) {
    auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    TVector<TDataProviderInitializer> providers = {[](const auto&...) {
        TDataProviderInfo info;
        info.Names.insert("custom");
        info.Source = MakeIntrusive<TAcceptingProjectSettingsProvider>("custom.Option");
        return info;
    }};
    TProgramFactory factory(/*useRepeatableRandomAndTimeProviders=*/true, registry.Get(), 0, providers, "test");
    factory.SetLanguageVersion(GetMaxLangVersion());
    TProjectSettings settings;
    settings.AddPragma("TablePathPrefix", "prefix/");
    settings.AddPragma("ClassicDivision", "false");
    settings.AddPragma("custom.Option", "value");
    settings.AddPragma("config.flags", TVector<TString>{"LLVM_OFF"});
    factory.SetProjectSettings(settings);
    NSQLTranslation::TTranslationSettings translation;
    translation.PgParser = true;
    auto program = factory.Create("test", "SELECT 1", "accepted");
    program->SetStrictConfigValidation(true);
    UNIT_ASSERT(program->ParseSql(translation));
    UNIT_ASSERT(program->Compile("test"));
    settings.AddPragma("typo.Option", "value");
    factory.SetProjectSettings(settings);
    program = factory.Create("test", "SELECT 1", "rejected");
    program->SetStrictConfigValidation(true);
    UNIT_ASSERT(program->ParseSql(translation));
    UNIT_ASSERT(!program->Compile("test"));
    UNIT_ASSERT_STRING_CONTAINS(program->Issues().ToString(), "typo.Option");
}

Y_UNIT_TEST(EachSettingNeedsOneAcceptingComponent) {
    TTypeAnnotationContext types;
    types.StrictConfigValidation = true;
    types.ProjectSettings.AddPragma("ClassicDivision", "false");
    types.AcceptedSqlProjectSettings.insert("ClassicDivision");
    types.ProjectSettings.AddPragma("cluster.Option", "value");
    types.ProjectSettings.AddPragma("sink.Option", "value");
    types.DataSourceMap["unrelated"] = MakeIntrusive<TAcceptingProjectSettingsProvider>("unrelated.Option");
    types.DataSourceMap["accepting"] = MakeIntrusive<TAcceptingProjectSettingsProvider>("cluster.Option");
    types.DataSinks.push_back(MakeIntrusive<TAcceptingProjectSettingsProvider>("sink.Option"));
    TExprContext ctx;
    UNIT_ASSERT(types.ValidateProjectSettings(ctx));
    UNIT_ASSERT(ctx.IssueManager.GetIssues().Empty());
    types.ProjectSettings.AddPragma("unknown.Option", "value");
    types.ProjectSettings.AddPragma("other.Unknown", "other-value");
    UNIT_ASSERT(!types.ValidateProjectSettings(ctx));
    const auto& issues = ctx.IssueManager.GetIssues();
    UNIT_ASSERT_VALUES_EQUAL(issues.Size(), 2);
    UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "unknown.Option");
    UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "other.Unknown");
    UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "values: other-value");
    for (const auto& issue : issues) {
        UNIT_ASSERT_VALUES_EQUAL(issue.GetCode(), static_cast<ui32>(TIssuesIds::DEFAULT_ERROR));
        UNIT_ASSERT(issue.GetSeverity() == TSeverityIds::S_ERROR);
    }
}

Y_UNIT_TEST(TranslatorProjectSettingsRespectAllowlist) {
    auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    for (bool pg : {false, true}) {
        TProgramFactory factory(/*useRepeatableRandomAndTimeProviders=*/true, registry.Get(), 0, {}, "test");
        factory.SetLanguageVersion(GetMaxLangVersion());
        TProjectSettings settings;
        settings.AddPragma(pg ? "ClassicDivision" : "ApplicationName", pg ? TVector<TString>{"invalid"} : TVector<TString>{"one", "two"});
        factory.SetProjectSettings(settings);
        auto program = factory.Create("test", "SELECT 1", "invalid");
        program->SetStrictConfigValidation(true);
        NSQLTranslation::TTranslationSettings translation;
        translation.PgParser = pg;
        UNIT_ASSERT(program->ParseSql(translation));
        UNIT_ASSERT_VALUES_EQUAL_C(program->Compile("test"), pg, program->Issues().ToString());
        if (!pg) {
            UNIT_ASSERT_STRING_CONTAINS(program->Issues().ToString(), "ApplicationName");
        }
    }
}

Y_UNIT_TEST(NonStrictValidationIgnoresUnknownSettings) {
    TTypeAnnotationContext types;
    types.ProjectSettings.AddPragma("unknown.Option", "value");
    TExprContext ctx;
    UNIT_ASSERT(types.ValidateProjectSettings(ctx));
    UNIT_ASSERT(ctx.IssueManager.GetIssues().Empty());
}

Y_UNIT_TEST(SettingCanBelongToAnotherProviderUsingConfigCluster) {
    TTypeAnnotationContext types;
    types.StrictConfigValidation = true;
    types.ProjectSettings.AddPragma("config.StaticPool", "LLVM_OFF");
    const auto provider = CreateConfigProvider(types, /*config=*/nullptr, "test", {});
    TExprContext ctx;
    UNIT_ASSERT(provider->Initialize(ctx));
    UNIT_ASSERT(ctx.IssueManager.GetIssues().Empty());
    types.DataSourceMap["config"] = provider;
    types.DataSourceMap["yt"] = MakeIntrusive<TAcceptingProjectSettingsProvider>("config.StaticPool");
    UNIT_ASSERT(types.ValidateProjectSettings(ctx));
}

Y_UNIT_TEST(ConfigProviderReportsAllowedFlags) {
    TTypeAnnotationContext types;
    const auto provider = CreateConfigProvider(types, /*config=*/nullptr, "test", {});
    UNIT_ASSERT(provider->AcceptsProjectSetting({.Name = "config.flags", .Value = {"LLVM_OFF"}}));
    UNIT_ASSERT(!provider->AcceptsProjectSetting({.Name = "config.flags", .Value = {"NodesAllocationLimit", "1"}}));
    UNIT_ASSERT(!provider->AcceptsProjectSetting({.Name = "config.flags", .Value = {}}));
    UNIT_ASSERT(!provider->AcceptsProjectSetting({.Name = "other.flags", .Value = {"LLVM_OFF"}}));
}

Y_UNIT_TEST(PgTranslatorReportsRecognizedSettings) {
    const auto translator = NSQLTranslationPG::MakeTranslator();
    UNIT_ASSERT(translator->AcceptsProjectSetting({.Name = "TablePathPrefix", .Value = {"prefix/"}}));
    UNIT_ASSERT(!translator->AcceptsProjectSetting({.Name = "Unknown", .Value = {"value"}}));
}
} // Y_UNIT_TEST_SUITE(TProjectSettingsValidation)

} // namespace NYql
