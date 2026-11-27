#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <yql/essentials/core/langver/feature.gen.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/yql_library_compiler.h>
#include <yql/essentials/core/yql_module_helpers.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/ast/yql_ast_annotation.h>
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

Y_UNIT_TEST_SUITE(TDecimalConversionMode) {

Y_UNIT_TEST(BeforeCutoffUsesConfiguredMode) {
    TTypeAnnotationContext typesCtx;
    typesCtx.LangVer = NFeature::AlwaysWithFixDecimalConversionMode.MinLangVer - 1;
    typesCtx.UpdateDecimalConversionMode(EDecimalConversionMode::WithoutCommonTypeFixup);

    UNIT_ASSERT_VALUES_EQUAL(
        typesCtx.GetDecimalConversionMode(),
        EDecimalConversionMode::WithoutCommonTypeFixup);
}

Y_UNIT_TEST(AtCutoffAlwaysUsesFixup) {
    TTypeAnnotationContext typesCtx;
    typesCtx.LangVer = NFeature::AlwaysWithFixDecimalConversionMode.MinLangVer;
    typesCtx.UpdateDecimalConversionMode(EDecimalConversionMode::WithoutCommonTypeFixup);

    UNIT_ASSERT_VALUES_EQUAL(
        typesCtx.GetDecimalConversionMode(),
        EDecimalConversionMode::WithCommonTypeFixup);
}

Y_UNIT_TEST(AfterCutoffAlwaysUsesFixup) {
    TTypeAnnotationContext typesCtx;
    typesCtx.LangVer = NFeature::AlwaysWithFixDecimalConversionMode.MinLangVer + 1;
    typesCtx.UpdateDecimalConversionMode(EDecimalConversionMode::WithoutCommonTypeFixup);

    UNIT_ASSERT_VALUES_EQUAL(
        typesCtx.GetDecimalConversionMode(),
        EDecimalConversionMode::WithCommonTypeFixup);
}

} // Y_UNIT_TEST_SUITE(TDecimalConversionMode)

Y_UNIT_TEST_SUITE(TMatchRecognizeTypeAnnotation) {

Y_UNIT_TEST(PropagatesUniversalDefinePredicate) {
    const TStringBuf program = R"((
        (let result (MatchRecognizeDefines
            (StructType '('"a" (DataType 'Bool)))
            '('"P")
            '('"P")
            (lambda '(rows matched index) (InstanceOf (UniversalType)))))
        (return result)
    ))";

    auto ast = ParseAst(program);
    UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());

    TExprContext ctx;
    TExprNode::TPtr expr;
    UNIT_ASSERT_C(CompileExpr(*ast.Root, expr, ctx, nullptr, nullptr), ctx.IssueManager.GetIssues().ToString());

    TTypeAnnotationContext typesCtx;
    UNIT_ASSERT_C(InstantAnnotateTypes(expr, ctx, /*wholeProgram=*/false, typesCtx), ctx.IssueManager.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(expr->GetTypeAnn()->GetKind(), ETypeAnnotationKind::Universal);
}

} // Y_UNIT_TEST_SUITE(TMatchRecognizeTypeAnnotation)

Y_UNIT_TEST_SUITE(TLazyBindTypeAnnotation) {

struct TLazyBindFixture {
    TExprContext LibraryContext;
    TExprContext Context;
    TTypeAnnotationContext Types;
    TExprNode::TPtr Expression;

    TLazyBindFixture() {
        TLibraryCohesion library;
        const TString source = R"((
            (let value (Int32 '42))
            (let identity (lambda '(value) value))
            (let nested (LazyBind '/lib/test 'value))
            (let applied (Apply identity value))
            (let invalid (Apply identity))
            (let atom 'Int32)
            (export value) (export identity) (export nested) (export applied) (export atom)
            (export invalid)
        ))";
        UNIT_ASSERT_C(CompileLibrary({nullptr, nullptr, nullptr}, "/lib/test.yqls", source, LibraryContext, library, false),
                      LibraryContext.IssueManager.GetIssues().ToString());
        TModulesTable modules;
        modules.emplace("/lib/test", std::move(library.Exports));
        Types.Modules = std::make_shared<TModuleResolver>(NSQLTranslation::TTranslators(nullptr, nullptr, nullptr),
                                                          std::move(modules), LibraryContext.NextUniqueId, THashMap<TString, TString>{}, NSQLTranslation::TExtendedSqlFlags{});
        Context.NextUniqueId = LibraryContext.NextUniqueId;
    }

    bool Annotate(TStringBuf expression) {
        const auto ast = ParseAst(TStringBuilder() << "((return " << expression << "))");
        UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());
        UNIT_ASSERT_C(CompileExpr(*ast.Root, Expression, Context, nullptr, nullptr), Context.IssueManager.GetIssues().ToString());
        return InstantAnnotateTypes(Expression, Context, /*wholeProgram=*/false, Types);
    }
};

Y_UNIT_TEST(ValueAndNestedBinding) {
    for (TStringBuf symbol : {"value", "nested", "applied"}) {
        TLazyBindFixture fixture;
        UNIT_ASSERT_C(fixture.Annotate(TStringBuilder() << "(LazyBind '/lib/test '" << symbol << ")"),
                      fixture.Context.IssueManager.GetIssues().ToString());
        UNIT_ASSERT(fixture.Expression->IsCallable("Int32"));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Expression->Head().Content(), "42");
    }
}

Y_UNIT_TEST(LambdaWithDifferentArgumentTypes) {
    TLazyBindFixture fixture;
    UNIT_ASSERT_C(fixture.Annotate(R"('(
        (Apply (LazyBind '/lib/test 'identity) (Int32 '1))
        (Apply (LazyBind '/lib/test 'identity) (String 'hello))
    ))"), fixture.Context.IssueManager.GetIssues().ToString());
    UNIT_ASSERT(fixture.Expression->Child(0)->IsCallable("Int32"));
    UNIT_ASSERT(fixture.Expression->Child(1)->IsCallable("String"));
}

Y_UNIT_TEST(UnselectedBranchIsNotResolved) {
    TLazyBindFixture fixture;
    UNIT_ASSERT_C(fixture.Annotate(R"((MatchType (DataType 'Int32)
        'Int32 (lambda '() (LazyBind '/lib/test 'value))
        (lambda '() (LazyBind '/missing 'missing))))"), fixture.Context.IssueManager.GetIssues().ToString());
    UNIT_ASSERT(fixture.Expression->IsCallable("Int32"));
}

Y_UNIT_TEST(AtomBinding) {
    TLazyBindFixture fixture;
    UNIT_ASSERT_C(fixture.Annotate("(DataType (LazyBind '/lib/test 'atom))"), fixture.Context.IssueManager.GetIssues().ToString());
    UNIT_ASSERT(fixture.Expression->IsCallable("DataType"));
    UNIT_ASSERT_VALUES_EQUAL(fixture.Expression->Head().Content(), "Int32");
}

Y_UNIT_TEST(NamedLambdaApplication) {
    TLazyBindFixture fixture;
    UNIT_ASSERT_C(fixture.Annotate("(NamedApply (LazyBind '/lib/test 'identity) '((Int32 '42)) (AsStruct))"),
                  fixture.Context.IssueManager.GetIssues().ToString());
    UNIT_ASSERT(fixture.Expression->IsCallable("Int32"));
}

Y_UNIT_TEST(WithoutResolverUsesUniversalType) {
    for (TStringBuf expression : {"(LazyBind '/lib/test 'value)", "(Apply (LazyBind '/lib/test 'identity) (Int32 '1))"}) {
        TLazyBindFixture fixture;
        fixture.Types.Modules.reset();
        UNIT_ASSERT_C(fixture.Annotate(expression), fixture.Context.IssueManager.GetIssues().ToString());
        UNIT_ASSERT(fixture.Expression->GetTypeAnn());
        UNIT_ASSERT(fixture.Expression->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal);
    }
}

Y_UNIT_TEST(InvalidBinding) {
    for (TStringBuf expression : {"(LazyBind '/missing 'value)", "(LazyBind '/lib/test 'missing)",
                                  "(LazyBind '/lib/test)", "(LazyBind (Int32 '1) 'value)", "(LazyBind '/lib/test (Int32 '1))",
                                  "(Apply (LazyBind '/lib/test 'identity))",
                                  "(Apply (LazyBind '/lib/test 'identity) (Int32 '1) (Int32 '2))"}) {
        TLazyBindFixture fixture;
        UNIT_ASSERT(!fixture.Annotate(expression));
        UNIT_ASSERT(!fixture.Context.IssueManager.GetIssues().Empty());
    }
}

Y_UNIT_TEST(InvalidImportedApplication) {
    TLazyBindFixture fixture;
    UNIT_ASSERT(!fixture.Annotate("(LazyBind '/lib/test 'invalid)"));
    UNIT_ASSERT_STRING_CONTAINS(fixture.Context.IssueManager.GetIssues().ToString(),
                                "Minimum arguments count: 1, maximum arguments count: 1, but provided 0");
    UNIT_ASSERT(fixture.Context.LazyBindings.empty());
    fixture.Context.IssueManager.Reset();
    UNIT_ASSERT(!ImportCached({}, "/lib/test", "invalid", fixture.Context, fixture.Types));
    UNIT_ASSERT_STRING_CONTAINS(fixture.Context.IssueManager.GetIssues().ToString(),
                                "Minimum arguments count: 1, maximum arguments count: 1, but provided 0");
    UNIT_ASSERT(fixture.Context.LazyBindings.empty());
}

Y_UNIT_TEST(CachedCloneIsQueryLocal) {
    TLazyBindFixture fixture;
    auto first = ImportCached({}, "/lib/test", "identity", fixture.Context, fixture.Types);
    UNIT_ASSERT(first);
    const auto count = fixture.Context.ExprNodes.size();
    auto second = ImportCached({}, "/lib/test", "identity", fixture.Context, fixture.Types);
    UNIT_ASSERT(first == second);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Context.ExprNodes.size(), count);
    TExprContext other(fixture.Context.NextUniqueId);
    auto separate = ImportCached({}, "/lib/test", "identity", other, fixture.Types);
    UNIT_ASSERT(first != separate);
    fixture.Context.Reset();
    auto afterReset = ImportCached({}, "/lib/test", "identity", fixture.Context, fixture.Types);
    UNIT_ASSERT(first != afterReset);
}

Y_UNIT_TEST(ExportsShareClonedSubgraphs) {
    TLazyBindFixture fixture;
    auto value = ImportCached({}, "/lib/test", "value", fixture.Context, fixture.Types);
    auto applied = ImportCached({}, "/lib/test", "applied", fixture.Context, fixture.Types);
    UNIT_ASSERT(value);
    UNIT_ASSERT(value == applied);
    UNIT_ASSERT(applied->IsCallable("Int32"));
}

} // Y_UNIT_TEST_SUITE(TLazyBindTypeAnnotation)

} // namespace NYql
