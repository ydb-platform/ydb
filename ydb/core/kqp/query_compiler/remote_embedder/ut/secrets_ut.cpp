#include <ydb/core/kqp/query_compiler/remote_embedder/secrets.h>

#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

NYql::TExprNode::TPtr Apply(NYql::TExprContext& ctx, TStringBuf function, const NYql::TExprNode::TPtr& secret, bool stream = false) {
    const auto pos = ctx.AppendPosition({});
    return ctx.NewCallable(pos, "Apply", {
        ctx.NewCallable(pos, "Udf", {ctx.NewAtom(pos, function)}),
        stream ? ctx.NewArgument(pos, "input") : ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "text")}),
        ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "model")}),
        ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "http://localhost/v1")}),
        secret});
}

} // namespace

Y_UNIT_TEST_SUITE(TRemoteEmbedderSecrets) {
    Y_UNIT_TEST(CollectsAndResolvesSecretReferences) {
        NYql::TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        const auto secret = ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "/Root/openai_token")});
        const auto scalar = Apply(ctx, "Knn.ApplyRemoteEmbedder", secret);
        const auto stream = Apply(ctx, "Knn.ApplyRemoteEmbedder", secret, /*stream=*/true);
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

    Y_UNIT_TEST(CollectsPositionalAndNamedSecretArguments) {
        NYql::TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        const auto secret = ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "secret")});
        const auto apply = Apply(ctx, "Knn.ApplyRemoteEmbedder", secret);
        NYql::TExprNode::TListType args(apply->Children().begin() + 1, apply->Children().end());
        const auto namedOption = ctx.NewCallable(pos, "NamedApply", {
            apply->HeadPtr(), ctx.NewList(pos, NYql::TExprNode::TListType(args)),
            ctx.NewCallable(pos, "AsStruct", {ctx.NewList(pos, {ctx.NewAtom(pos, "MaxRetries"), ctx.NewCallable(pos, "Uint32", {ctx.NewAtom(pos, "0")})})})});
        args.pop_back();
        const auto namedSecret = ctx.NewCallable(pos, "NamedApply", {
            apply->HeadPtr(), ctx.NewList(pos, std::move(args)),
            ctx.NewCallable(pos, "AsStruct", {ctx.NewList(pos, {ctx.NewAtom(pos, "AuthSecretName"), secret})})});
        for (const auto& call : {namedOption, namedSecret}) {
            THashMap<TString, TString> params;
            UNIT_ASSERT(ValidateRemoteEmbedderSecretNames(call, ctx));
            FillRemoteEmbedderSecureParams(call, params);
            UNIT_ASSERT_VALUES_EQUAL(params.size(), 1);
            UNIT_ASSERT(params.contains("secret"));
        }
    }

    Y_UNIT_TEST(RejectsDynamicAndEmptySecretNames) {
        NYql::TExprContext ctx;
        const auto pos = ctx.AppendPosition({});
        THashMap<TString, TString> params;
        const auto parameter = ctx.NewCallable(pos, "Parameter", {ctx.NewAtom(pos, "dynamic")});
        const auto empty = ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "")});
        const auto scalar = Apply(ctx, "Knn.ApplyRemoteEmbedder", parameter);
        const auto stream = Apply(ctx, "Knn.ApplyRemoteEmbedder", empty, /*stream=*/true);
        UNIT_ASSERT(!ValidateRemoteEmbedderSecretNames(ctx.NewList(pos, {scalar, stream}), ctx));
        UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "AuthSecretName must be a non-empty String literal");
        FillRemoteEmbedderSecureParams(ctx.NewList(pos, {scalar, stream}), params);
        UNIT_ASSERT(params.empty());
    }
}

} // namespace NKikimr::NKqp
