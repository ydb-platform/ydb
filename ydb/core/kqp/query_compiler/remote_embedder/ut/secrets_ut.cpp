#include <ydb/core/kqp/query_compiler/remote_embedder/secrets.h>

#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

NYql::TExprNode::TPtr Apply(NYql::TExprContext& ctx, TStringBuf function, const NYql::TExprNode::TPtr& secret) {
    const auto pos = ctx.AppendPosition({});
    return ctx.NewCallable(pos, "Apply", {
        ctx.NewCallable(pos, "Udf", {ctx.NewAtom(pos, function)}),
        ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "model")}),
        ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "http://localhost/v1")}),
        secret,
        ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "text")})});
}

} // namespace

Y_UNIT_TEST_SUITE(TRemoteEmbedderSecrets) {
    Y_UNIT_TEST(CollectsAndResolvesSecretReferences) {
        NYql::TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        const auto secret = ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "/Root/openai_token")});
        const auto scalar = Apply(ctx, "RemoteEmbedder.ApplyRemoteEmbedder", secret);
        const auto stream = Apply(ctx, "RemoteEmbedder.ApplyRemoteEmbedderStream", secret);
        const auto program = ctx.NewList(pos, {scalar, stream});
        THashMap<TString, TString> params;
        FillRemoteEmbedderSecureParams(program, params);
        UNIT_ASSERT_VALUES_EQUAL(params.size(), 1);
        auto parser = NYql::CreateStructuredTokenParser(params.at("/Root/openai_token"));
        TSet<TString> names;
        parser.ListReferences(names);
        UNIT_ASSERT_VALUES_EQUAL(names.size(), 1);
        UNIT_ASSERT(names.contains("/Root/openai_token"));
        const auto resolved = NYql::CreateStructuredTokenParser(parser.ToBuilder().RemoveSecrets().ReplaceReferences({{"/Root/openai_token", "resolved-token"}}).ToJson());
        UNIT_ASSERT(resolved.HasIAMToken());
        UNIT_ASSERT_VALUES_EQUAL(resolved.GetIAMToken(), "resolved-token");

    }

    Y_UNIT_TEST(IgnoresOtherUdfs) {
        NYql::TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        const auto secret = ctx.NewCallable(pos, "Parameter", {ctx.NewAtom(pos, "dynamic")});
        THashMap<TString, TString> params;
        FillRemoteEmbedderSecureParams(Apply(ctx, "Other.ApplyRemoteEmbedder", secret), params);
        UNIT_ASSERT(params.empty());
    }

    Y_UNIT_TEST(RejectsDynamicAndEmptySecretNames) {
        NYql::TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        THashMap<TString, TString> params;
        const auto parameter = ctx.NewCallable(pos, "Parameter", {ctx.NewAtom(pos, "dynamic")});
        const auto empty = ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "")});
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            FillRemoteEmbedderSecureParams(Apply(ctx, "RemoteEmbedder.ApplyRemoteEmbedder", parameter), params),
            yexception, "AuthSecretName must be a non-empty String literal");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            FillRemoteEmbedderSecureParams(Apply(ctx, "RemoteEmbedder.ApplyRemoteEmbedderStream", empty), params),
            yexception, "AuthSecretName must be a non-empty String literal");
    }
}

} // namespace NKikimr::NKqp
