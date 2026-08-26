#include "yql_expr_optimize.h"
#include "yql_expr_type_annotation.h"
#include "yql_opt_range.h"
#include "yql_opt_rewrite_io.h"
#include "yql_opt_proposed_by_data.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/ast/yql_ast_annotation.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/type_ann/type_ann_core.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

namespace {

TExprNode::TPtr MakePresenceRangeFor(
    TExprContext& ctx,
    TStringBuf operation,
    const TTypeAnnotationNode* keyType)
{
    const auto pos = TPositionHandle();
    auto value = ctx.NewCallable(pos, "Void", {});
    value->SetTypeAnn(ctx.MakeType<TVoidExprType>());
    auto type = ExpandType(pos, *keyType, ctx);
    type->SetTypeAnn(ctx.MakeType<TTypeExprType>(keyType));
    return ctx.NewCallable(pos, "RangeFor", {
        ctx.NewAtom(pos, operation),
        std::move(value),
        std::move(type),
    });
}

const TExprNode& AssertSingleRange(const TExprNode::TPtr& result) {
    UNIT_ASSERT(result->IsCallable("AsRange"));
    UNIT_ASSERT_VALUES_EQUAL(result->ChildrenSize(), 1);
    const auto& range = result->Head();
    UNIT_ASSERT(range.IsList());
    UNIT_ASSERT_VALUES_EQUAL(range.ChildrenSize(), 2);
    return range;
}

void AssertBoundaryFlag(
    const TExprNode& range,
    size_t boundaryIndex,
    TStringBuf expected)
{
    const auto& boundary = *range.Child(boundaryIndex);
    UNIT_ASSERT(boundary.IsList());
    UNIT_ASSERT_VALUES_EQUAL(boundary.ChildrenSize(), 2);
    const auto& flag = *boundary.Child(1);
    UNIT_ASSERT(flag.IsCallable("Int32"));
    UNIT_ASSERT_VALUES_EQUAL(flag.ChildrenSize(), 1);
    UNIT_ASSERT(flag.Head().IsAtom(expected));
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TOptimizeYqlExpr) {
Y_UNIT_TEST(CombineAtoms) {
    const auto s = "(\n"
                   "(let x (Combine '11 '333 '7))\n"
                   "(return x)\n"
                   ")\n";

    const auto astRes = ParseAst(s);
    UNIT_ASSERT(astRes.IsOk());
    TExprContext exprCtx;
    TExprNode::TPtr exprRoot;
    UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));
    UNIT_ASSERT_EQUAL(IGraphTransformer::TStatus::Repeat, ExpandApply(exprRoot, exprRoot, exprCtx).Level);

    auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, /*refAtoms=*/true);
    auto strRes = ast.Root->ToString(TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
    UNIT_ASSERT(strRes.find("(return '113337)") != TString::npos);
}

Y_UNIT_TEST(RecursiveLambda) {
    const auto s =
        R"(
            (
            (let f**k (lambda '(x l) (+ x (Apply l x l))))
            (return (Apply f**k (Uint32 '1) f**k))
            )
        )";

    const auto astRes = ParseAst(s);
    UNIT_ASSERT(astRes.IsOk());
    TExprContext exprCtx;
    TExprNode::TPtr exprRoot;
    UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));

    for (size_t i = 0U; i < 0x100; ++i) {
        UNIT_ASSERT_EQUAL(IGraphTransformer::TStatus::Repeat, ExpandApply(exprRoot, exprRoot, exprCtx).Level);
    }

    const auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, /*refAtoms=*/true);
    const auto strRes = ast.Root->ToString(TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
    UNIT_ASSERT_EQUAL(0x101, std::count(strRes.cbegin(), strRes.cend(), '+'));
}

Y_UNIT_TEST(ApplyWideLambda) {
    const auto s =
        R"(
            (
            (let wide (lambda '(x y) (+ x y) (* x y) x y))
            (return '('1 (Apply wide (Int32 '3) (Int32 '7)) '9))
            )
        )";

    const auto astRes = ParseAst(s);
    UNIT_ASSERT(astRes.IsOk());
    TExprContext exprCtx;
    TExprNode::TPtr exprRoot;
    UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));
    UNIT_ASSERT_EQUAL(IGraphTransformer::TStatus::Repeat, ExpandApply(exprRoot, exprRoot, exprCtx).Level);

    auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, /*refAtoms=*/true);
    auto strRes = ast.Root->ToString(TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
    UNIT_ASSERT(strRes.find("(return '('1 (+ $1 $2) (* $1 $2) $1 $2 '9))") != TString::npos);
}

Y_UNIT_TEST(ApplyThinLambda) {
    const auto s =
        R"(
            (
            (let wide (lambda '(x y)))
            (return '('1 (Apply wide (Int32 '3) (Int32 '7)) '9))
            )
        )";

    const auto astRes = ParseAst(s);
    UNIT_ASSERT(astRes.IsOk());
    TExprContext exprCtx;
    TExprNode::TPtr exprRoot;
    UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));
    UNIT_ASSERT_EQUAL(IGraphTransformer::TStatus::Repeat, ExpandApply(exprRoot, exprRoot, exprCtx).Level);

    auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, /*refAtoms=*/true);
    auto strRes = ast.Root->ToString(TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
    UNIT_ASSERT(strRes.find("(return '('1 '9))") != TString::npos);
}

Y_UNIT_TEST(ApplyDeepLambda) {
    const auto s = "# program\n"
                   "(\n"
                   "(let x (Uint64 '42))\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "(let l (lambda '(y) (block '(\n"
                   "\n"
                   "(let l (lambda '(y) (+ x y)))\n"
                   "\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "(return (Apply l (+ y x)))))))\n"
                   "\n"
                   "(let res_sink (DataSink 'result))\n"
                   "(let resKey (Apply l (Int64 '7)))\n"
                   "(let world (Write! world res_sink (Key) resKey '('('type))))\n"
                   "(let world (Commit! world res_sink))\n"
                   "(return world)\n"
                   ")\n";

    const auto astRes = ParseAst(s);
    UNIT_ASSERT(astRes.IsOk());
    TExprContext exprCtx;
    TExprNode::TPtr exprRoot;
    UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));

    while (true) {
        const auto ret = ExpandApply(exprRoot, exprRoot, exprCtx);
        if (ret.Level != IGraphTransformer::TStatus::Repeat) {
            UNIT_ASSERT_EQUAL(ret.Level, IGraphTransformer::TStatus::Ok);
            break;
        }
    }

    auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, /*refAtoms=*/true);
    auto strRes = ast.Root->ToString(TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
    UNIT_ASSERT_EQUAL(strRes.find("lambda"), TString::npos);
}

Y_UNIT_TEST(Nth) {
    const auto s = "(\n"
                   "(let x '('11 '333 '7))\n"
                   "(return (Nth x '2))\n"
                   ")\n";

    const auto astRes = ParseAst(s);
    UNIT_ASSERT(astRes.IsOk());
    TExprContext exprCtx;
    TExprNode::TPtr exprRoot;
    UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));
    UNIT_ASSERT_EQUAL(IGraphTransformer::TStatus::Repeat, ExpandApply(exprRoot, exprRoot, exprCtx).Level);

    auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, /*refAtoms=*/true);
    auto strRes = ast.Root->ToString(TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
    UNIT_ASSERT(strRes.find("(return '7)") != TString::npos);
}

Y_UNIT_TEST(NthLargeIndex) {
    const auto s = "(\n"
                   "(let x '('11 '333 '7))\n"
                   "(return (Nth x '3))\n"
                   ")\n";

    const auto astRes = ParseAst(s);
    UNIT_ASSERT(astRes.IsOk());
    TExprContext exprCtx;
    TExprNode::TPtr exprRoot;
    UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));
    UNIT_ASSERT_EQUAL(IGraphTransformer::TStatus::Error, ExpandApply(exprRoot, exprRoot, exprCtx).Level);
    UNIT_ASSERT_VALUES_EQUAL("<main>:3:17: Error: Index too large: (3 >= 3).\n", exprCtx.IssueManager.GetIssues().ToString());
}

Y_UNIT_TEST(NthWrongIndex) {
    const auto s = "(\n"
                   "(let x '('11 '333 '7))\n"
                   "(return (Nth x 'Z))\n"
                   ")\n";

    const auto astRes = ParseAst(s);
    UNIT_ASSERT(astRes.IsOk());
    TExprContext exprCtx;
    TExprNode::TPtr exprRoot;
    UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));
    UNIT_ASSERT_EQUAL(IGraphTransformer::TStatus::Error, ExpandApply(exprRoot, exprRoot, exprCtx).Level);
    UNIT_ASSERT_VALUES_EQUAL("<main>:3:17: Error: Index 'Z' isn't UI32.\n", exprCtx.IssueManager.GetIssues().ToString());
}

Y_UNIT_TEST(NthArg) {
    const auto s = "(\n"
                   "(let x (NthArg '1 (+ '37 '42)))\n"
                   "(return x)\n"
                   ")\n";

    const auto astRes = ParseAst(s);
    UNIT_ASSERT(astRes.IsOk());
    TExprContext exprCtx;
    TExprNode::TPtr exprRoot;
    UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));
    UNIT_ASSERT_EQUAL(IGraphTransformer::TStatus::Repeat, ExpandApply(exprRoot, exprRoot, exprCtx).Level);

    auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, /*refAtoms=*/true);
    auto strRes = ast.Root->ToString(TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
    UNIT_ASSERT(strRes.find("(return '42)") != TString::npos);
}

Y_UNIT_TEST(NthArgLargeIndex) {
    const auto s = "(\n"
                   "(let x (NthArg '2 (- '37 '42)))\n"
                   "(return x)\n"
                   ")\n";

    const auto astRes = ParseAst(s);
    UNIT_ASSERT(astRes.IsOk());
    TExprContext exprCtx;
    TExprNode::TPtr exprRoot;
    UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));
    UNIT_ASSERT_EQUAL(IGraphTransformer::TStatus::Error, ExpandApply(exprRoot, exprRoot, exprCtx).Level);
    UNIT_ASSERT_VALUES_EQUAL("<main>:2:17: Error: Index too large: (2 >= 2).\n", exprCtx.IssueManager.GetIssues().ToString());
}

Y_UNIT_TEST(NthArgWrongIndex) {
    const auto s = "(\n"
                   "(let x (NthArg 'bad (* '37 '42)))\n"
                   "(return x)\n"
                   ")\n";

    const auto astRes = ParseAst(s);
    UNIT_ASSERT(astRes.IsOk());
    TExprContext exprCtx;
    TExprNode::TPtr exprRoot;
    UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));
    UNIT_ASSERT_EQUAL(IGraphTransformer::TStatus::Error, ExpandApply(exprRoot, exprRoot, exprCtx).Level);
    UNIT_ASSERT_VALUES_EQUAL("<main>:2:17: Error: Index 'bad' isn't UI32.\n", exprCtx.IssueManager.GetIssues().ToString());
}

Y_UNIT_TEST(NthArgNotCallable) {
    const auto s = "(\n"
                   "(let x (NthArg '0 'bad))\n"
                   "(return x)\n"
                   ")\n";

    const auto astRes = ParseAst(s);
    UNIT_ASSERT(astRes.IsOk());
    TExprContext exprCtx;
    TExprNode::TPtr exprRoot;
    UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));
    UNIT_ASSERT_EQUAL(IGraphTransformer::TStatus::Error, ExpandApply(exprRoot, exprRoot, exprCtx).Level);
    UNIT_ASSERT_VALUES_EQUAL("<main>:2:20: Error: Expected callable, but got: Atom\n", exprCtx.IssueManager.GetIssues().ToString());
}

Y_UNIT_TEST(RangeForExistsOnRequiredKeyIsFullAndNotExistsIsEmpty) {
    TExprContext exprCtx;

    for (const auto slot : {EDataSlot::Int64, EDataSlot::Double}) {
        const auto* keyType = exprCtx.MakeType<TDataExprType>(slot);
        const auto exists = ExpandRangeFor(
            MakePresenceRangeFor(exprCtx, "Exists", keyType), exprCtx);
        const auto& range = AssertSingleRange(exists);
        AssertBoundaryFlag(range, 0, "0");
        AssertBoundaryFlag(range, 1, "0");

        const auto notExists = ExpandRangeFor(
            MakePresenceRangeFor(exprCtx, "NotExists", keyType), exprCtx);
        UNIT_ASSERT(notExists->IsCallable("RangeEmpty"));
        UNIT_ASSERT_VALUES_EQUAL(notExists->ChildrenSize(), 1);

        if (slot == EDataSlot::Double) {
            UNIT_ASSERT_C(
                !FindNode(exists, [](const TExprNode::TPtr& node) {
                    return node->IsAtom("nan");
                }),
                "Exists over a required floating-point key must include NaN");
        }
    }
}

Y_UNIT_TEST(RangeForExistsRetainsOptionalAndPgNullRanges) {
    TExprContext exprCtx;
    const auto* int64Type = exprCtx.MakeType<TDataExprType>(EDataSlot::Int64);
    const auto* optionalType = exprCtx.MakeType<TOptionalExprType>(int64Type);
    const auto* pgType = exprCtx.MakeType<TPgExprType>(
        NPg::LookupType("int4").TypeId);
    const TTypeAnnotationNode* keyTypes[] = {optionalType, pgType};

    for (const auto* keyType : keyTypes) {
        const auto exists = ExpandRangeFor(
            MakePresenceRangeFor(exprCtx, "Exists", keyType), exprCtx);
        const auto& existsRange = AssertSingleRange(exists);
        AssertBoundaryFlag(existsRange, 0, "0");
        AssertBoundaryFlag(existsRange, 1, "0");
        UNIT_ASSERT(existsRange.Head().Head().IsCallable("Just"));

        const auto notExists = ExpandRangeFor(
            MakePresenceRangeFor(exprCtx, "NotExists", keyType), exprCtx);
        const auto& notExistsRange = AssertSingleRange(notExists);
        AssertBoundaryFlag(notExistsRange, 0, "1");
        AssertBoundaryFlag(notExistsRange, 1, "1");
        UNIT_ASSERT(notExistsRange.Head().Head().IsCallable("Just"));
        UNIT_ASSERT_EQUAL(
            notExistsRange.Child(0)->Child(0),
            notExistsRange.Child(1)->Child(0));
    }
}
} // Y_UNIT_TEST_SUITE(TOptimizeYqlExpr)

} // namespace NYql
