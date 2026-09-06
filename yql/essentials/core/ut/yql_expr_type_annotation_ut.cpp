#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/type_ann/type_ann_sql.h>
#include <yql/essentials/ast/yql_expr.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

#include <functional>

namespace NYql {

namespace {

TExprNode::TPtr MakeDecimalNode(int precision, int scale, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    // clang-format off
    auto expr = ctx.Builder(TPositionHandle())
        .Callable("Decimal")
            .Atom(0, "0")
            .Atom(1, ToString(precision))
            .Atom(2, ToString(scale))
        .Seal()
        .Build();
    // clang-format on
    ctx.RepeatTransformCounter = 0;
    if (!InstantAnnotateTypes(expr, ctx, /*wholeProgram=*/false, typesCtx)) {
        return nullptr;
    }
    return expr;
}

using TCommonTypeFn = std::function<const TTypeAnnotationNode*(TPositionHandle,
                                                               const TTypeAnnotationNode*,
                                                               const TTypeAnnotationNode*,
                                                               TExprContext&,
                                                               const TTypeAnnotationContext&,
                                                               bool)>;

TString DescribeDecimal(const TTypeAnnotationNode* type) {
    const auto* params = static_cast<const TDataExprParamsType*>(type->Cast<TDataExprType>());
    return TStringBuilder() << "Decimal(" << params->GetParamOne() << "," << params->GetParamTwo() << ")";
}

void CheckExpectedCommonType(TExprNode::TPtr node1, TExprNode::TPtr node2, TExprContext& ctx, TTypeAnnotationContext& typesCtx, TExprNode::TPtr expected = {}) {
    YQL_ENSURE(node1);
    YQL_ENSURE(node2);

    if (!expected) {
        const auto* common = CommonType<true>(TPositionHandle(), node1->GetTypeAnn(), node2->GetTypeAnn(), ctx, typesCtx, /*warn=*/false);
        UNIT_ASSERT_C(!common, "Expected CommonType to return null for "
                                   << DescribeDecimal(node1->GetTypeAnn()) << " and "
                                   << DescribeDecimal(node2->GetTypeAnn()) << " but got non-null");
        return;
    }

    const auto* common = CommonType<true>(TPositionHandle(), node1->GetTypeAnn(), node2->GetTypeAnn(), ctx, typesCtx, /*warn=*/false);
    UNIT_ASSERT_C(common, "CommonType returned null for "
                              << DescribeDecimal(node1->GetTypeAnn()) << " and "
                              << DescribeDecimal(node2->GetTypeAnn()));

    UNIT_ASSERT_C(IsSameAnnotation(*common, *expected->GetTypeAnn()),
                  "CommonType for " << DescribeDecimal(node1->GetTypeAnn())
                                    << " and " << DescribeDecimal(node2->GetTypeAnn())
                                    << " expected " << DescribeDecimal(expected->GetTypeAnn())
                                    << " but got " << DescribeDecimal(common));
}

void CheckCommonTypeBothCastable(TCommonTypeFn commonTypeFn, const TString& pipelineName) {
    constexpr int minDecimalTypePart = 0;
    constexpr int maxDecimalTypePart = 35;

    TExprContext ctx;
    TTypeAnnotationContext typesCtx;
    typesCtx.DecimalConversionMode = EDecimalConversionMode::WithCommonTypeFixup;

    for (int i = minDecimalTypePart; i <= maxDecimalTypePart; ++i) {
        for (int j = minDecimalTypePart; j <= maxDecimalTypePart; ++j) {
            for (int v = minDecimalTypePart; v <= maxDecimalTypePart; ++v) {
                for (int b = minDecimalTypePart; b <= maxDecimalTypePart; ++b) {
                    auto node1 = MakeDecimalNode(i, j, ctx, typesCtx);
                    if (!node1) {
                        continue;
                    }
                    auto node2 = MakeDecimalNode(v, b, ctx, typesCtx);
                    if (!node2) {
                        continue;
                    }

                    const auto* common = commonTypeFn(TPositionHandle(),
                                                      node1->GetTypeAnn(), node2->GetTypeAnn(), ctx, typesCtx, false);
                    if (!common) {
                        continue;
                    }

                    auto status1 = TrySilentConvertTo(node1, *node1->GetTypeAnn(), *common, ctx, typesCtx);
                    auto status2 = TrySilentConvertTo(node2, *node2->GetTypeAnn(), *common, ctx, typesCtx);
                    const auto* params = static_cast<const TDataExprParamsType*>(common->Cast<TDataExprType>());
                    UNIT_ASSERT_C(status1.Level != IGraphTransformer::TStatus::Error && status2.Level != IGraphTransformer::TStatus::Error,
                                  pipelineName << " returned non-null but cast failed."
                                               << " Left: Decimal(" << i << "," << j << ")"
                                               << ", Right: Decimal(" << v << "," << b << ")"
                                               << ", common: Decimal("
                                               << params->GetParamOne() << "," << params->GetParamTwo() << ")"
                                               << ", status1: " << status1.Level
                                               << ", status2: " << status2.Level);
                }
            }
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TYqlTryConvertToDecimal) {

Y_UNIT_TEST(CommonTypeBothCastable) {
    CheckCommonTypeBothCastable(CommonType<true>, "CommonType");
}

Y_UNIT_TEST(CommonTypeExpectedResults) {
    TExprContext ctx;
    TTypeAnnotationContext typesCtx;
    typesCtx.DecimalConversionMode = EDecimalConversionMode::WithCommonTypeFixup;

    auto dec = [&](int precision, int scale) {
        return MakeDecimalNode(precision, scale, ctx, typesCtx);
    };
    auto check = [&](TExprNode::TPtr node1, TExprNode::TPtr node2, TExprNode::TPtr expected) {
        CheckExpectedCommonType(std::move(node1), std::move(node2), ctx, typesCtx, std::move(expected));
    };

    // Same types — result is the same type
    check(dec(1, 0), dec(1, 0), dec(1, 0));
    check(dec(1, 1), dec(1, 1), dec(1, 1));
    check(dec(10, 5), dec(10, 5), dec(10, 5));
    check(dec(35, 0), dec(35, 0), dec(35, 0));
    check(dec(35, 35), dec(35, 35), dec(35, 35));

    // Different scales, precision fits
    check(dec(1, 0), dec(1, 1), dec(2, 1));
    check(dec(5, 2), dec(7, 4), dec(7, 4));
    check(dec(2, 1), dec(3, 2), dec(3, 2));
    check(dec(10, 0), dec(1, 1), dec(11, 1));

    // Precision at boundary (whole + scale = MaxPrecision)
    check(dec(18, 0), dec(17, 17), dec(35, 17));
    check(dec(17, 0), dec(18, 18), dec(35, 18));
    check(dec(20, 2), dec(18, 17), dec(35, 17));

    // Commutativity: order should not matter
    check(dec(1, 1), dec(1, 0), dec(2, 1));
    check(dec(7, 4), dec(5, 2), dec(7, 4));
    check(dec(17, 17), dec(18, 0), dec(35, 17));

    // CommonType returns null — precision overflow
    check(dec(20, 8), dec(35, 6), {});
    check(dec(34, 16), dec(33, 18), {});
    check(dec(35, 7), dec(35, 6), {});
    check(dec(35, 17), dec(35, 18), {});
    check(dec(35, 0), dec(35, 35), {});
    check(dec(35, 1), dec(35, 34), {});
    check(dec(1, 0), dec(35, 35), {});
    check(dec(18, 0), dec(18, 18), {});
}

} // Y_UNIT_TEST_SUITE(TYqlTryConvertToDecimal)

Y_UNIT_TEST_SUITE(TYqlSqlExprEquivalence) {

TExprNode::TPtr Lambda(
    TExprContext& ctx,
    TExprNode::TPtr argument,
    TExprNode::TPtr body)
{
    return ctx.NewLambda(
        TPositionHandle{}, ctx.NewArguments(TPositionHandle{}, {std::move(argument)}), std::move(body));
}

Y_UNIT_TEST(AlphaRenamingPreservesCapturedRowAndFreeVariableIdentity) {
    TExprContext ctx;
    const auto leftRow = ctx.NewArgument(TPositionHandle{}, "row");
    const auto rightRow = ctx.NewArgument(TPositionHandle{}, "renamed_row");
    const auto leftArg = ctx.NewArgument(TPositionHandle{}, "value");
    const auto rightArg = ctx.NewArgument(TPositionHandle{}, "renamed_value");
    const auto left = Lambda(ctx, leftArg, ctx.NewList(TPositionHandle{}, {leftRow, leftArg}));
    const auto right = Lambda(ctx, rightArg, ctx.NewList(TPositionHandle{}, {rightRow, rightArg}));
    const auto captured = Lambda(ctx, rightArg, ctx.NewList(TPositionHandle{}, {rightRow, rightRow}));
    const auto equal = [&](const auto& first, const auto& second) {
        return NTypeAnnImpl::NDetail::SqlExprsEqual(
            *first, *second, *leftRow, *rightRow);
    };

    UNIT_ASSERT(equal(left, right));
    UNIT_ASSERT(!equal(left, captured));
    UNIT_ASSERT(equal(leftArg, leftArg));
    UNIT_ASSERT(!equal(leftArg, rightArg));
    // A right-side binder must not capture an identical free-variable node.
    UNIT_ASSERT(!equal(Lambda(ctx, leftArg, rightArg), Lambda(ctx, rightArg, rightArg)));
}

Y_UNIT_TEST(SharedLambdaAtDifferentDepthDoesNotConfuseLocalAndCapturedArguments) {
    TExprContext ctx;
    const auto leftRow = ctx.NewArgument(TPositionHandle{}, "row");
    const auto rightRow = ctx.NewArgument(TPositionHandle{}, "row");
    const auto leftLocal = ctx.NewArgument(TPositionHandle{}, "local");
    const auto leftOuter = ctx.NewArgument(TPositionHandle{}, "outer");
    const auto rightFirst = ctx.NewArgument(TPositionHandle{}, "first");
    const auto rightOuter = ctx.NewArgument(TPositionHandle{}, "outer");
    const auto rightLocal = ctx.NewArgument(TPositionHandle{}, "local");
    const auto sharedIdentity = Lambda(ctx, leftLocal, leftLocal);
    const auto left = ctx.NewList(TPositionHandle{}, {
        sharedIdentity,
        Lambda(ctx, leftOuter, sharedIdentity),
    });
    const auto makeRight = [&](bool captureOuter) {
        return ctx.NewList(TPositionHandle{}, {
            Lambda(ctx, rightFirst, rightFirst),
            Lambda(ctx, rightOuter, Lambda(
                ctx, rightLocal, captureOuter ? rightOuter : rightLocal)),
        });
    };
    const auto equivalent = makeRight(false);
    const auto different = makeRight(true);

    UNIT_ASSERT(NTypeAnnImpl::NDetail::SqlExprsEqual(
        *left, *equivalent, *leftRow, *rightRow));
    UNIT_ASSERT(!NTypeAnnImpl::NDetail::SqlExprsEqual(
        *left, *different, *leftRow, *rightRow));
    UNIT_ASSERT(!NTypeAnnImpl::NDetail::SqlExprsEqual(
        *different, *left, *rightRow, *leftRow));
}

Y_UNIT_TEST(SharedLeftNodeIsComparedWithEveryRightNode) {
    TExprContext ctx;
    const auto leftRow = ctx.NewArgument(TPositionHandle{}, "row");
    const auto rightRow = ctx.NewArgument(TPositionHandle{}, "row");
    const auto shared = ctx.NewAtom(TPositionHandle{}, "same");
    const auto left = ctx.NewList(TPositionHandle{}, {shared, shared});
    const auto right = ctx.NewList(TPositionHandle{}, {
        ctx.NewAtom(TPositionHandle{}, "same"), ctx.NewAtom(TPositionHandle{}, "different")});
    UNIT_ASSERT(!NTypeAnnImpl::NDetail::SqlExprsEqual(
        *left, *right, *leftRow, *rightRow));
}

Y_UNIT_TEST(InnerBinderShadowsEitherSideOfTheEnclosingRowAlias) {
    TExprContext ctx;
    const auto leftRow = ctx.NewArgument(TPositionHandle{}, "row");
    const auto rightRow = ctx.NewArgument(TPositionHandle{}, "row");
    const auto local = ctx.NewArgument(TPositionHandle{}, "local");
    const auto captured = Lambda(ctx, local, leftRow);
    const auto shadowed = Lambda(ctx, rightRow, rightRow);

    // Whole-graph validation rejects duplicate declarations, but the internal
    // matcher also handles partial graphs without assuming it has run already.
    UNIT_ASSERT(!NTypeAnnImpl::NDetail::SqlExprsEqual(
        *captured, *shadowed, *leftRow, *rightRow));
    UNIT_ASSERT(!NTypeAnnImpl::NDetail::SqlExprsEqual(
        *shadowed, *captured, *rightRow, *leftRow));
}

} // Y_UNIT_TEST_SUITE(TYqlSqlExprEquivalence)

} // namespace NYql
