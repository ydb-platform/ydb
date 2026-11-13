#include <yql/essentials/providers/config/yql_config_provider.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/ast/yql_expr.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {
namespace {

void AddCoreFlag(TGatewaysConfig& config, TStringBuf name, const TVector<TString>& args = {}) {
    auto* flag = config.MutableYqlCore()->AddFlags();
    flag->SetName(TString(name));
    for (const auto& arg : args) {
        flag->AddArgs(arg);
    }
}

IGraphTransformer::TStatus ApplyPragma(IDataProvider& provider, TExprContext& ctx,
                                       TStringBuf name, const TVector<TString>& args = {}) {
    const auto pos = ctx.AppendPosition(TPosition(1, 1));
    TExprNode::TListType children = {
        ctx.NewWorld(pos),
        ctx.NewCallable(pos, "DataSource", {ctx.NewAtom(pos, "config")}),
        ctx.NewAtom(pos, name),
    };
    for (const auto& arg : args) {
        children.push_back(ctx.NewAtom(pos, arg));
    }

    auto input = ctx.NewCallable(pos, "Configure!", std::move(children));
    TExprNode::TPtr output;
    return provider.GetConfigurationTransformer().Transform(input, output, ctx);
}

} // namespace

Y_UNIT_TEST_SUITE(TConfigProviderFlagsTest) {
Y_UNIT_TEST(PartialTypeCheckToleratesMissingFullResultDataSink) {
    TTypeAnnotationContext types;
    TExprContext ctx;
    auto provider = CreateConfigProvider(types, /*config=*/nullptr, "testuser", {}, /*forPartialTypeCheck=*/true);
    UNIT_ASSERT(provider->Initialize(ctx));

    const auto status = ApplyPragma(*provider, ctx, "FullResultDataSink", {"yt"});
    UNIT_ASSERT_C(status == IGraphTransformer::TStatus::Ok, ctx.IssueManager.GetIssues().ToString());
    UNIT_ASSERT(ctx.IssueManager.GetIssues().Empty());
    UNIT_ASSERT(types.FullResultDataSink.empty());
}

Y_UNIT_TEST(FullResultDataSinkRejectsMissingProvider) {
    TTypeAnnotationContext types;
    TExprContext ctx;
    auto provider = CreateConfigProvider(types, /*config=*/nullptr, "testuser", {});
    UNIT_ASSERT(provider->Initialize(ctx));

    UNIT_ASSERT(ApplyPragma(*provider, ctx, "FullResultDataSink", {"yt"}) == IGraphTransformer::TStatus::Error);
    UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "Unknown datasink for full result provider: yt");
    UNIT_ASSERT(types.FullResultDataSink.empty());
}

Y_UNIT_TEST(FullResultDataSinkValidatesArgumentCount) {
    for (bool partial : {false, true}) {
        for (const auto& args : {TVector<TString>{}, TVector<TString>{"yt", "extra"}}) {
            TTypeAnnotationContext types;
            TExprContext ctx;
            auto provider = CreateConfigProvider(types, /*config=*/nullptr, "testuser", {}, partial);
            UNIT_ASSERT(provider->Initialize(ctx));

            UNIT_ASSERT(ApplyPragma(*provider, ctx, "FullResultDataSink", args) == IGraphTransformer::TStatus::Error);
            UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "Expected 1 argument");
            UNIT_ASSERT(types.FullResultDataSink.empty());
        }
    }
}

Y_UNIT_TEST(ProjectSettingsOverrideConfigAndQueryOverridesProject) {
    TTypeAnnotationContext types;
    types.ProjectSettings.AddPragma("config.flags", TVector<TString>{"UdfSupportsYield", "true"});
    types.ProjectSettings.AddPragma("yt.UnknownSetting", "ignored");
    types.ProjectSettings.AddPragma("UnknownSqlPragma", "ignored");
    TExprContext ctx;
    TGatewaysConfig config;
    AddCoreFlag(config, "UdfSupportsYield", {"false"});
    auto provider = CreateConfigProvider(types, &config, "testuser", {});

    UNIT_ASSERT_C(provider->Initialize(ctx), ctx.IssueManager.GetIssues().ToString());
    UNIT_ASSERT(types.UdfSupportsYield);
    UNIT_ASSERT(ApplyPragma(*provider, ctx, "UdfSupportsYield", {"false"}) == IGraphTransformer::TStatus::Ok);
    UNIT_ASSERT(!types.UdfSupportsYield);
}

Y_UNIT_TEST(InvalidProjectSettingsAreIgnoredWithoutStrictValidation) {
    TTypeAnnotationContext types;
    types.ProjectSettings.AddPragma("config.flags", TVector<TString>{"UnknownSetting", "value"});
    types.ProjectSettings.AddPragma("config.flags", TVector<TString>{"UseTableMetaFromGraph", "true"});
    types.ProjectSettings.AddPragma("config.flags", TVector<TString>{"Diagnostics", "true"});
    TExprContext ctx;
    auto provider = CreateConfigProvider(types, /*config=*/nullptr, "testuser", {});

    UNIT_ASSERT(provider->Initialize(ctx));
    UNIT_ASSERT(!types.UseTableMetaFromGraph);
    UNIT_ASSERT(!types.Diagnostics);
    UNIT_ASSERT(ctx.IssueManager.GetIssues().Empty());
}

Y_UNIT_TEST(ProjectSettingsAllowOnlyWhitelistedFlags) {
    const TVector<TVector<TString>> settings = {
        {"ValidateUdf", "None"},
        {"LLVM_OFF"},
        {"LLVM"},
        {"LLVM", "OFF"},
        {"UdfSupportsYield", "false"},
        {"GeobaseDownloadUrl", ""},
    };
    for (const auto& values : settings) {
        TTypeAnnotationContext types;
        types.StrictConfigValidation = true;
        types.ProjectSettings.AddPragma("config.flags", values);
        TExprContext ctx;
        auto provider = CreateConfigProvider(types, /*config=*/nullptr, "testuser", {});
        UNIT_ASSERT_C(provider->Initialize(ctx), ctx.IssueManager.GetIssues().ToString());
        UNIT_ASSERT(ctx.IssueManager.GetIssues().Empty());
    }
}

Y_UNIT_TEST(StrictValidationRejectsUnknownAndSystemProjectSettings) {
    for (const auto& name : {"UnknownSetting", "UseTableMetaFromGraph", "NodesAllocationLimit", "DisableStrictTableProps", "Diagnostics"}) {
        TTypeAnnotationContext types;
        types.StrictConfigValidation = true;
        types.ProjectSettings.AddPragma("config.flags", TVector<TString>{name, "true"});
        TExprContext ctx;
        auto provider = CreateConfigProvider(types, /*config=*/nullptr, "testuser", {});

        types.DataSourceMap["config"] = provider;
        UNIT_ASSERT(!(provider->Initialize(ctx) && types.ValidateProjectSettings(ctx)));
        UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), name);
        UNIT_ASSERT_VALUES_EQUAL(ctx.IssueManager.GetIssues().Size(), 1);
        const auto& issue = *ctx.IssueManager.GetIssues().begin();
        UNIT_ASSERT_VALUES_EQUAL(issue.GetCode(), static_cast<ui32>(TIssuesIds::DEFAULT_ERROR));
        UNIT_ASSERT(issue.GetSeverity() == TSeverityIds::S_ERROR);
    }
}

Y_UNIT_TEST(ProjectSettingsRespectSettingPolicy) {
    for (bool strict : {false, true}) {
        TTypeAnnotationContext types;
        types.StrictConfigValidation = strict;
        types.ProjectSettings.AddPragma("config.flags", TVector<TString>{"UdfSupportsYield", "true"});
        TExprContext ctx;
        const auto originalLimit = types.UdfSupportsYield;
        auto provider = CreateConfigProvider(types, /*config=*/nullptr, "testuser", [](TStringBuf) { return false; });
        if (strict) {
            UNIT_ASSERT(!provider->Initialize(ctx));
            UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "UdfSupportsYield");
        } else {
            UNIT_ASSERT(provider->Initialize(ctx));
            UNIT_ASSERT(ctx.IssueManager.GetIssues().Empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(types.UdfSupportsYield, originalLimit);
    }
}

Y_UNIT_TEST(ProjectFlagsRequireFlagName) {
    for (bool strict : {false, true}) {
        for (const TVector<TString>& values : {TVector<TString>{}, TVector<TString>{""}}) {
            TTypeAnnotationContext types;
            types.StrictConfigValidation = strict;
            types.ProjectSettings.AddPragma("config.flags", values);
            TExprContext ctx;
            auto provider = CreateConfigProvider(types, /*config=*/nullptr, "testuser", {});
            if (strict) {
                types.DataSourceMap["config"] = provider;
                UNIT_ASSERT(provider->Initialize(ctx));
                UNIT_ASSERT(!types.ValidateProjectSettings(ctx));
                UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "config.flags");
            } else {
                UNIT_ASSERT(provider->Initialize(ctx));
                UNIT_ASSERT(ctx.IssueManager.GetIssues().Empty());
            }
        }
    }
}

Y_UNIT_TEST(ProjectFlagsPassAllArgumentsToHandler) {
    TTypeAnnotationContext types;
    types.ProjectSettings.AddPragma("config.flags", TVector<TString>{"ValidateUdf", "None", "extra"});
    TExprContext ctx;
    auto provider = CreateConfigProvider(types, /*config=*/nullptr, "testuser", {});

    UNIT_ASSERT(!provider->Initialize(ctx));
    UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "Expected 1 argument");
}

Y_UNIT_TEST(ProjectSettingsRejectOtherConfigPragmas) {
    for (bool strict : {false, true}) {
        for (const auto& name : {"config.NodesAllocationLimit", "config.Flags", "config.flags.extra", "config."}) {
            TTypeAnnotationContext types;
            types.StrictConfigValidation = strict;
            types.ProjectSettings.AddPragma(name, TVector<TString>{"NodesAllocationLimit", "12345"});
            TExprContext ctx;
            const auto originalLimit = ctx.NodesAllocationLimit;
            auto provider = CreateConfigProvider(types, /*config=*/nullptr, "testuser", {});
            if (strict) {
                types.DataSourceMap["config"] = provider;
                UNIT_ASSERT(provider->Initialize(ctx));
                UNIT_ASSERT(!types.ValidateProjectSettings(ctx));
                UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), name);
            } else {
                UNIT_ASSERT(provider->Initialize(ctx));
                UNIT_ASSERT(ctx.IssueManager.GetIssues().Empty());
            }
            UNIT_ASSERT_VALUES_EQUAL(ctx.NodesAllocationLimit, originalLimit);
        }
    }
}

Y_UNIT_TEST(UnknownConfigFlagsAreIgnored) {
    TTypeAnnotationContext types;
    TExprContext ctx;
    TGatewaysConfig config;
    AddCoreFlag(config, "UnknownCoreFlag");
    AddCoreFlag(config, "UnknownCoreFlagWithArgs", {"value", "extra"});
    AddCoreFlag(config, "NodesAllocationLimit", {"12345"});
    auto provider = CreateConfigProvider(types, &config, "testuser", {});

    UNIT_ASSERT_C(provider->Initialize(ctx), ctx.IssueManager.GetIssues().ToString());
    UNIT_ASSERT(ctx.IssueManager.GetIssues().Empty());
    UNIT_ASSERT_VALUES_EQUAL(ctx.NodesAllocationLimit, 12345);
}

Y_UNIT_TEST(UnknownPragmaIsRejected) {
    TTypeAnnotationContext types;
    TExprContext ctx;
    auto provider = CreateConfigProvider(types, /*config=*/nullptr, "testuser", {});
    UNIT_ASSERT(provider->Initialize(ctx));

    UNIT_ASSERT(ApplyPragma(*provider, ctx, "UnknownCoreFlag") == IGraphTransformer::TStatus::Error);
    UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "Unsupported command: UnknownCoreFlag");
}

Y_UNIT_TEST(StrictConfigValidationRejectsUnknownFlagOnInit) {
    TTypeAnnotationContext types;
    types.StrictConfigValidation = true;
    TExprContext ctx;
    TGatewaysConfig config;
    AddCoreFlag(config, "UnknownCoreFlag");
    auto provider = CreateConfigProvider(types, &config, "testuser", {});

    UNIT_ASSERT(!provider->Initialize(ctx));
    UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "Unsupported command: UnknownCoreFlag");
}

Y_UNIT_TEST(StrictConfigValidationAcceptsKnownFlagOnInit) {
    TTypeAnnotationContext types;
    types.StrictConfigValidation = true;
    TExprContext ctx;
    TGatewaysConfig config;
    AddCoreFlag(config, "NodesAllocationLimit", {"12345"});
    auto provider = CreateConfigProvider(types, &config, "testuser", {});

    UNIT_ASSERT_C(provider->Initialize(ctx), ctx.IssueManager.GetIssues().ToString());
    UNIT_ASSERT(ctx.IssueManager.GetIssues().Empty());
    UNIT_ASSERT_VALUES_EQUAL(ctx.NodesAllocationLimit, 12345);
}
} // Y_UNIT_TEST_SUITE(TConfigProviderFlagsTest)

} // namespace NYql
