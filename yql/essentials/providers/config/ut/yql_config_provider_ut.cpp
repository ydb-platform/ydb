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
