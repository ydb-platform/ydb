#include <ydb/core/kqp/common/pg_policy/pg_policy.h>

#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NKikimr::NKqp {
namespace {

NYql::TExprNode::TPtr Compile(TStringBuf expression, NYql::TExprContext& ctx) {
    auto ast = NYql::ParseAst(TStringBuilder() << "((return " << expression << "))");
    UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());

    NYql::TExprNode::TPtr expr;
    UNIT_ASSERT_C(NYql::CompileExpr(*ast.Root, expr, ctx, nullptr, nullptr), ctx.IssueManager.GetIssues().ToString());
    return expr;
}

NYql::TExprNode::TPtr Annotate(TStringBuf expression, NYql::TExprContext& ctx, bool forKqp, bool windowNewPipeline = true) {
    auto expr = Compile(expression, ctx);
    auto types = MakeIntrusive<NYql::TTypeAnnotationContext>();
    types->WindowNewPipeline = windowNewPipeline;
    NYql::TTypeAnnCallableFactory factory;
    if (forKqp) {
        factory = CreateKqpTypeAnnCallableFactory(*types);
    }
    auto transformer = NYql::TTransformationPipeline(types, factory)
        .AddTypeAnnotationTransformer()
        .Build();
    if (NYql::SyncTransform(*transformer, expr, ctx) != NYql::IGraphTransformer::TStatus::Ok) {
        return nullptr;
    }
    return expr;
}

void CheckRejected(TStringBuf expression) {
    NYql::TExprContext ctx;
    auto expr = Annotate(expression, ctx, true);
    UNIT_ASSERT_C(!expr, expression);
    UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "PostgreSQL types are not supported");
}

} // namespace

Y_UNIT_TEST_SUITE(TKqpPgPolicy) {

Y_UNIT_TEST(FullPgCompilerWithoutKqpPolicySupportsPgExpressions) {
    for (TStringBuf expression : {"(PgType 'int4)", "(PgCall 'version '())", "(PgCall 'abs '() (Int32 '1))",
                                 "(ToPg (Int32 '1))", "(BlockToPg (InstanceOf (ScalarType (DataType 'Int32))))",
                                 "(BlockPgResolvedCall 'version '89 '())", "(ParseType 'pgint4)",
                                 "(ParseType '\"List<pgint4>\")"}) {
        NYql::TExprContext ctx;
        auto expr = Annotate(expression, ctx, false);
        UNIT_ASSERT_C(expr, TStringBuilder() << expression << ": " << ctx.IssueManager.GetIssues().ToString());
        UNIT_ASSERT(expr->GetTypeAnn());
    }
}

Y_UNIT_TEST(KqpPolicyRejectsPgType) {
    CheckRejected("(PgType 'int4)");
}

Y_UNIT_TEST(KqpPolicyRejectsPgCallWithoutArguments) {
    CheckRejected("(PgCall 'version '())");
}

Y_UNIT_TEST(KqpPolicyRejectsPgCallWithNativeArgument) {
    CheckRejected("(PgCall 'abs '() (Int32 '1))");
}

Y_UNIT_TEST(KqpPolicyRejectsToPg) {
    CheckRejected("(ToPg (Int32 '1))");
}

Y_UNIT_TEST(KqpPolicyRejectsBlockToPg) {
    CheckRejected("(BlockToPg (InstanceOf (ScalarType (DataType 'Int32))))");
}

Y_UNIT_TEST(KqpPolicyRejectsBlockPgCallWithoutArguments) {
    CheckRejected("(BlockPgResolvedCall 'version '89 '())");
}

Y_UNIT_TEST(KqpPolicyRejectsParsedPgType) {
    CheckRejected("(ParseType 'pgint4)");
}

Y_UNIT_TEST(KqpPolicyRejectsParsedNestedPgType) {
    CheckRejected("(ParseType '\"List<pgint4>\")");
}

Y_UNIT_TEST(KqpPolicyRejectsPgTypeFromQueueResource) {
    for (bool forKqp : {false, true}) {
        NYql::TExprContext ctx;
        auto expr = Annotate("(QueuePeek (InstanceOf (ResourceType 'TResourceQueue:pgint4)) (Uint64 '0))", ctx, forKqp, false);
        if (forKqp) {
            UNIT_ASSERT(!expr);
            UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "PostgreSQL types are not supported");
        } else {
            UNIT_ASSERT_C(expr, ctx.IssueManager.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(expr->GetTypeAnn()->GetKind(), NYql::ETypeAnnotationKind::Optional);
            UNIT_ASSERT_VALUES_EQUAL(expr->GetTypeAnn()->Cast<NYql::TOptionalExprType>()->GetItemType()->GetKind(), NYql::ETypeAnnotationKind::Pg);
        }
    }
}

Y_UNIT_TEST(KqpPolicySupportsNativeTypeFromQueueResource) {
    NYql::TExprContext ctx;
    auto expr = Annotate("(QueuePeek (InstanceOf (ResourceType 'TResourceQueue:Int32)) (Uint64 '0))", ctx, true, false);
    UNIT_ASSERT_C(expr, ctx.IssueManager.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(expr->GetTypeAnn()->GetKind(), NYql::ETypeAnnotationKind::Optional);
    UNIT_ASSERT_VALUES_EQUAL(expr->GetTypeAnn()->Cast<NYql::TOptionalExprType>()->GetItemType()->GetKind(), NYql::ETypeAnnotationKind::Data);
}

Y_UNIT_TEST(KqpPolicySupportsParsedNativeTypes) {
    NYql::TExprContext ctx;
    auto expr = Annotate("(ParseType '\"List<Int32>\")", ctx, true);
    UNIT_ASSERT_C(expr, ctx.IssueManager.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(expr->GetTypeAnn()->GetKind(), NYql::ETypeAnnotationKind::Type);
    const auto* parsedType = expr->GetTypeAnn()->Cast<NYql::TTypeExprType>()->GetType();
    UNIT_ASSERT_VALUES_EQUAL(parsedType->GetKind(), NYql::ETypeAnnotationKind::List);
    UNIT_ASSERT_VALUES_EQUAL(parsedType->Cast<NYql::TListExprType>()->GetItemType()->GetKind(), NYql::ETypeAnnotationKind::Data);
}

Y_UNIT_TEST(GeneratedNativeLambdaWithPgExpressionIsRejected) {
    for (bool forKqp : {false, true}) {
        NYql::TExprContext ctx;
        auto types = MakeIntrusive<NYql::TTypeAnnotationContext>();
        auto lambda = Compile("(lambda '(row) (FromPg (PgCall 'abs '() (Int32 '1))))", ctx);
        const auto* rowType = ctx.MakeType<NYql::TStructExprType>(TVector<const NYql::TItemExprType*>{});
        UNIT_ASSERT(NYql::UpdateLambdaAllArgumentsTypes(lambda, {rowType}, ctx));
        const bool annotated = forKqp
            ? InstantAnnotateKqpTypes(lambda, ctx, *types)
            : NYql::InstantAnnotateTypes(lambda, ctx, false, *types);
        if (forKqp) {
            UNIT_ASSERT(!annotated);
            UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "PostgreSQL types are not supported");
        } else {
            UNIT_ASSERT_C(annotated, ctx.IssueManager.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(lambda->GetTypeAnn()->GetKind(), NYql::ETypeAnnotationKind::Optional);
            UNIT_ASSERT_VALUES_EQUAL(lambda->GetTypeAnn()->Cast<NYql::TOptionalExprType>()->GetItemType()->GetKind(), NYql::ETypeAnnotationKind::Data);
        }
    }
}

Y_UNIT_TEST(InstantAnnotationSupportsNativeLambda) {
    NYql::TExprContext ctx;
    auto types = MakeIntrusive<NYql::TTypeAnnotationContext>();
    auto lambda = Compile("(lambda '(row) (Int32 '1))", ctx);
    const auto* rowType = ctx.MakeType<NYql::TStructExprType>(TVector<const NYql::TItemExprType*>{});
    UNIT_ASSERT(NYql::UpdateLambdaAllArgumentsTypes(lambda, {rowType}, ctx));
    UNIT_ASSERT_C(InstantAnnotateKqpTypes(lambda, ctx, *types), ctx.IssueManager.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(lambda->GetTypeAnn()->GetKind(), NYql::ETypeAnnotationKind::Data);
}

Y_UNIT_TEST(KqpPolicySupportsNativeTypes) {
    NYql::TExprContext ctx;
    auto expr = Annotate("(Int32 '1)", ctx, true);
    UNIT_ASSERT_C(expr, ctx.IssueManager.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(expr->GetTypeAnn()->GetKind(), NYql::ETypeAnnotationKind::Data);
}

Y_UNIT_TEST(KqpPolicySupportsFromPgWithNativeArgument) {
    for (TStringBuf expression : {"(FromPg (Bool 'true))", "(FromPg (Int32 '1))"}) {
        NYql::TExprContext ctx;
        auto expr = Annotate(expression, ctx, true);
        UNIT_ASSERT_C(expr, TStringBuilder() << expression << ": " << ctx.IssueManager.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(expr->GetTypeAnn()->GetKind(), NYql::ETypeAnnotationKind::Data);
    }
}

} // Y_UNIT_TEST_SUITE(TKqpPgPolicy)

} // namespace NKikimr::NKqp
