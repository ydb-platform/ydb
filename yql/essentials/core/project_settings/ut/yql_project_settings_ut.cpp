#include <yql/essentials/core/project_settings/convert/convert.h>
#include <yql/essentials/core/project_settings/proto/project_settings.pb.h>

#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/config/yql_config_provider.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/node/node_io.h>

#include <utility>

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

Y_UNIT_TEST(ConversionsPreserveSettings) {
    auto settings = ProjectSettingsFromJson(R"({
        "DefaultCluster":"cluster", "LangVer":"2025.01",
        "Attributes":{"text":"\"\\\n", "nested":{"key":42}, "list":[true,null]}
    })");
    settings.AddPragma("Repeated", TVector<TString>{});
    settings.AddPragma("Repeated", TVector<TString>{"", "value"});
    settings.AddPragma("", TVector<TString>{""});
    const auto expected = ProjectSettingsToYson(settings);
    NProto::TProjectSettings proto;
    proto.SetDefaultCluster("cluster");
    proto.SetLangVer("2025.01");
    proto.SetAttributes(NYT::NodeToYsonString(settings.Attributes));
    proto.AddPragmas()->SetName("Repeated");
    auto* repeated = proto.AddPragmas();
    repeated->SetName("Repeated");
    repeated->AddValue("");
    repeated->AddValue("value");
    auto* emptyName = proto.AddPragmas();
    emptyName->SetName("");
    emptyName->AddValue("");
    UNIT_ASSERT(ProjectSettingsToYson(ProjectSettingsFromProto(proto)) == expected);
    UNIT_ASSERT(ProjectSettingsToYson(ProjectSettingsFromJson(ProjectSettingsToJson(settings))) == expected);
    UNIT_ASSERT(ProjectSettingsToYson(ProjectSettingsFromYson(expected)) == expected);
}

Y_UNIT_TEST(EmptyProtoSettings) {
    const NProto::TProjectSettings proto;
    UNIT_ASSERT(ProjectSettingsToYson(ProjectSettingsFromProto(proto)) == ProjectSettingsToYson({}));
}

Y_UNIT_TEST(InvalidProtoSettings) {
    NProto::TProjectSettings proto;
    proto.SetLangVer("invalid");
    UNIT_ASSERT_EXCEPTION(ProjectSettingsFromProto(proto), yexception);
    proto.ClearLangVer();
    proto.SetAttributes("[]");
    UNIT_ASSERT_EXCEPTION(ProjectSettingsFromProto(proto), yexception);
    proto.SetAttributes("{broken");
    UNIT_ASSERT_EXCEPTION(ProjectSettingsFromProto(proto), yexception);
}

Y_UNIT_TEST(ExtendedSettingsRoundTrip) {
    const auto settings = ProjectSettingsFromJson(R"({
        "DefaultCluster":"project_cluster", "LangVer":"2025.01",
        "Attributes":{"label":"project", "nested":{"key":42}, "list":[true, null]}
    })");
    UNIT_ASSERT_VALUES_EQUAL(settings.DefaultCluster, "project_cluster");
    UNIT_ASSERT_VALUES_EQUAL(settings.LangVer, MinLangVersion);
    const auto restored = ProjectSettingsFromYson(ProjectSettingsToYson(settings));
    UNIT_ASSERT_VALUES_EQUAL(restored.DefaultCluster, settings.DefaultCluster);
    UNIT_ASSERT_VALUES_EQUAL(restored.LangVer, settings.LangVer);
    UNIT_ASSERT(restored.Attributes == settings.Attributes);
    const auto empty = ProjectSettingsFromYson(ProjectSettingsToYson({}));
    UNIT_ASSERT(empty.DefaultCluster.empty());
    UNIT_ASSERT(!empty.LangVer);
    UNIT_ASSERT(empty.Attributes.AsMap().empty());
}

Y_UNIT_TEST(InvalidExtendedSettings) {
    for (const TStringBuf json : {
             R"({"DefaultCluster":[]})", R"({"LangVer":"invalid"})",
             R"({"LangVer":202501})", R"({"Attributes":[]})"}) {
        UNIT_ASSERT_EXCEPTION(ProjectSettingsFromJson(json), yexception);
    }
}

Y_UNIT_TEST(ConversionErrorsIncludeContext) {
    const TVector<std::pair<TStringBuf, TStringBuf>> cases = {
        {"[]", "root"},
        {R"({"DefaultCluster":[]})", "DefaultCluster"},
        {R"({"LangVer":42})", "LangVer"},
        {R"({"Attributes":[]})", "Attributes"},
        {R"({"Pragmas":{}})", "Pragmas"},
        {R"({"Pragmas":[{}]})", "Pragmas.Value"},
        {R"({"Pragmas":[{"Value":42}]})", "Pragmas.Value"},
        {R"({"Pragmas":[{"Value":[42]}]})", "Pragmas.Value"},
        {R"({"Pragmas":[{"Value":[]}]})", "Pragmas.Name"},
        {R"({"Pragmas":[{"Value":[],"Name":42}]})", "Pragmas.Name"},
    };
    for (const auto& [json, field] : cases) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(ProjectSettingsFromJson(json), yexception,
                                       TString("Failed to parse project settings field ") + field);
    }
    UNIT_ASSERT_EXCEPTION_CONTAINS(ProjectSettingsFromJson("{"), yexception, "Failed to parse project settings JSON:");
    NProto::TProjectSettings proto;
    proto.SetAttributes("{broken");
    UNIT_ASSERT_EXCEPTION_CONTAINS(ProjectSettingsFromProto(proto), yexception, "Failed to parse project settings protobuf Attributes:");
}

Y_UNIT_TEST(DefaultClusterAndUsePrecedence) {
    auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    TProgramFactory factory(/*useRepeatableRandomAndTimeProviders=*/true, registry.Get(), 0, {}, "test");
    TProjectSettings settings;
    settings.DefaultCluster = "project_cluster";
    factory.SetProjectSettings(settings);
    NSQLTranslation::TTranslationSettings translation;
    translation.ClusterMapping = {{"project_cluster", "yt"}, {"query_cluster", "yt"}, {"config_cluster", "yt"}};
    translation.DefaultCluster = "config_cluster";
    auto program = factory.Create("test", "SELECT * FROM Input", "project");
    program->SetStrictConfigValidation(true);
    UNIT_ASSERT_C(program->ParseSql(translation), program->Issues().ToString());
    UNIT_ASSERT_STRING_CONTAINS(program->AstRoot()->ToString(), "project_cluster");
    program = factory.Create("test", "USE query_cluster; SELECT * FROM Input", "query");
    program->SetStrictConfigValidation(true);
    UNIT_ASSERT_C(program->ParseSql(translation), program->Issues().ToString());
    UNIT_ASSERT_STRING_CONTAINS(program->AstRoot()->ToString(), "query_cluster");
}

Y_UNIT_TEST(UnknownDefaultClusterReportsIssue) {
    auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    TProgramFactory factory(/*useRepeatableRandomAndTimeProviders=*/true, registry.Get(), 0, {}, "test");
    TProjectSettings settings;
    settings.DefaultCluster = "unknown_project_cluster";
    factory.SetProjectSettings(settings);
    NSQLTranslation::TTranslationSettings translation;
    translation.ClusterMapping = {{"query_cluster", "yt"}};
    for (const TStringBuf query : {"SELECT 1", "SELECT * FROM Input", "USE query_cluster; SELECT * FROM Input"}) {
        for (bool strict : {false, true}) {
            auto program = factory.Create("test", TString(query), "unknown-cluster");
            program->SetStrictConfigValidation(strict);
            UNIT_ASSERT(!program->ParseSql(translation));
            UNIT_ASSERT_STRING_CONTAINS(program->Issues().ToString(), "Unknown default cluster: unknown_project_cluster");
        }
    }
}

Y_UNIT_TEST(StrictValidationChecksProjectLanguageVersion) {
    auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    TProgramFactory factory(/*useRepeatableRandomAndTimeProviders=*/true, registry.Get(), 0, {}, "test");
    factory.SetLanguageVersion(MinLangVersion);
    factory.SetMaxLanguageVersion(MinLangVersion);
    for (auto version : {MinLangVersion, GetMaxLangVersion()}) {
        TProjectSettings settings;
        settings.LangVer = version;
        factory.SetProjectSettings(settings);
        for (bool strict : {false, true}) {
            auto program = factory.Create("test", "SELECT 1", "strict-version");
            program->SetStrictConfigValidation(strict);
            const bool accepted = !strict || version == MinLangVersion;
            UNIT_ASSERT_VALUES_EQUAL(program->ParseSql(), accepted);
            if (!accepted) {
                UNIT_ASSERT_STRING_CONTAINS(program->Issues().ToString(), "Invalid project LangVer:");
            }
            program = factory.Create("test", "SELECT 1", "raised-version-limit");
            program->SetStrictConfigValidation(strict);
            program->SetMaxLanguageVersion(GetMaxLangVersion());
            UNIT_ASSERT_C(program->ParseSql(), program->Issues().ToString());
        }
    }
}

Y_UNIT_TEST(LanguageVersionValidationPreservesBothIssues) {
    auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    TProgramFactory factory(/*useRepeatableRandomAndTimeProviders=*/true, registry.Get(), 0, {}, "test");
    factory.SetLanguageVersion(GetMaxLangVersion());
    factory.SetMaxLanguageVersion(MinLangVersion);
    TProjectSettings settings;
    settings.LangVer = GetMaxLangVersion();
    factory.SetProjectSettings(settings);
    auto program = factory.Create("test", "SELECT 1", "both-invalid-versions");
    program->SetStrictConfigValidation(true);
    UNIT_ASSERT(!program->ParseSql());
    const auto& issues = program->Issues();
    UNIT_ASSERT_VALUES_EQUAL(issues.Size(), 2);
    auto issue = issues.begin();
    UNIT_ASSERT_STRING_CONTAINS(issue->GetMessage(), "maximum version is");
    UNIT_ASSERT(!issue->GetMessage().Contains("Invalid project LangVer"));
    ++issue;
    UNIT_ASSERT_STRING_CONTAINS(issue->GetMessage(), "Invalid project LangVer:");
}

Y_UNIT_TEST(OperationAttributesAndLanguageVersion) {
    auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    TLangVersion actualVersion = UnknownLangVersion;
    NYT::TNode actualAttrs;
    TVector<TDataProviderInitializer> providers = {[&](const auto&, const auto&, const auto&, const auto&, const auto&, const auto& types,
                                                       const auto&, const auto& options, const auto&...) {
        actualVersion = types->LangVer;
        UNIT_ASSERT(options.AttrsYson);
        actualAttrs = NYT::NodeFromYsonString(*options.AttrsYson);
        return TDataProviderInfo{};
    }};
    TProgramFactory factory(/*useRepeatableRandomAndTimeProviders=*/true, registry.Get(), 0, providers, "test");
    TProjectSettings settings;
    settings.LangVer = MinLangVersion;
    settings.Attributes = NYT::TNode()("shared", "project")("project_only", 1)("nested", NYT::TNode()("project", 1));
    factory.SetProjectSettings(settings);
    auto program = factory.Create("test", "SELECT 1", "project");
    UNIT_ASSERT(program->ParseSql());
    UNIT_ASSERT(program->Compile("test"));
    UNIT_ASSERT_VALUES_EQUAL(actualVersion, MinLangVersion);
    UNIT_ASSERT(actualAttrs == settings.Attributes);
    factory.SetLanguageVersion(GetMaxLangVersion());
    program = factory.Create("test", "SELECT 1", "operation");
    program->SetOperationAttrsYson("{shared=operation;operation_only=2;nested={operation=2;};}");
    UNIT_ASSERT(program->ParseSql());
    UNIT_ASSERT(program->Compile("test"));
    UNIT_ASSERT_VALUES_EQUAL(actualVersion, GetMaxLangVersion());
    UNIT_ASSERT_VALUES_EQUAL(actualAttrs["shared"].AsString(), "operation");
    UNIT_ASSERT_VALUES_EQUAL(actualAttrs["project_only"].AsInt64(), 1);
    UNIT_ASSERT_VALUES_EQUAL(actualAttrs["operation_only"].AsInt64(), 2);
    UNIT_ASSERT(!actualAttrs["nested"].HasKey("project"));
    UNIT_ASSERT_VALUES_EQUAL(actualAttrs["nested"]["operation"].AsInt64(), 2);
    factory.SetLanguageVersion(UnknownLangVersion);
    program = factory.Create("test", "SELECT 1", "unknown");
    UNIT_ASSERT(program->ParseSql());
    UNIT_ASSERT(program->Compile("test"));
    UNIT_ASSERT_VALUES_EQUAL(actualVersion, UnknownLangVersion);
}

Y_UNIT_TEST(EmptyProjectLeavesOperationAttributesUnset) {
    auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    bool initialized = false;
    TVector<TDataProviderInitializer> providers = {[&](const auto&, const auto&, const auto&, const auto&, const auto&, const auto&,
                                                       const auto&, const auto& options, const auto&...) {
        initialized = true;
        UNIT_ASSERT(!options.AttrsYson);
        return TDataProviderInfo{};
    }};
    TProgramFactory factory(/*useRepeatableRandomAndTimeProviders=*/true, registry.Get(), 0, providers, "test");
    auto program = factory.Create("test", "SELECT 1", "empty");
    UNIT_ASSERT(program->ParseSql());
    UNIT_ASSERT(program->Compile("test"));
    UNIT_ASSERT(initialized);
}

Y_UNIT_TEST(FromJsonPreservesPragmas) {
    const auto settings = ProjectSettingsFromJson(R"({"Pragmas":[
        {"Name":"UnicodeLiterals","Value":[]},
        {"Name":"config.flags","Value":["LLVM", "OFF"]},
        {"Name":"UnicodeLiterals","Value":[""]}
    ]})");
    UNIT_ASSERT_VALUES_EQUAL(settings.Pragmas.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(settings.Pragmas[0].Name, "UnicodeLiterals");
    UNIT_ASSERT(settings.Pragmas[0].Value.empty());
    UNIT_ASSERT_VALUES_EQUAL(settings.Pragmas[1].Name, "config.flags");
    UNIT_ASSERT_VALUES_EQUAL(settings.Pragmas[1].Value, (TVector<TString>{"LLVM", "OFF"}));
    UNIT_ASSERT_VALUES_EQUAL(settings.Pragmas[2].Name, "UnicodeLiterals");
    UNIT_ASSERT_VALUES_EQUAL(settings.Pragmas[2].Value, TVector<TString>{""});
}

Y_UNIT_TEST(FromJsonEmptySettings) {
    UNIT_ASSERT(ProjectSettingsFromJson("{}").Pragmas.empty());
    UNIT_ASSERT(ProjectSettingsFromJson(R"({"Pragmas":[]})").Pragmas.empty());
}

Y_UNIT_TEST(FromJsonRejectsInvalidSettings) {
    for (const TStringBuf json : {
             "", "{invalid}", "[]", "null",
             R"({"Pragmas":{}})",
             R"({"Pragmas":[{}]})",
             R"({"Pragmas":[{"Name":1,"Value":[]}]})",
             R"({"Pragmas":[{"Name":"UnicodeLiterals","Value":[true]}]})",
             R"({"Pragmas":[{"Name":"UnicodeLiterals","Value":"true"}]})"}) {
        UNIT_ASSERT_EXCEPTION(ProjectSettingsFromJson(json), yexception);
    }
}

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
