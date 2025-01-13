#include "yql_expr_optimize.h"
#include "yql_opt_rewrite_io.h"
#include "yql_opt_proposed_by_data.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/ast/yql_ast_annotation.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/type_ann/type_ann_core.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

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

        auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, true);
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

        for (size_t i = 0U; i < 0x100; ++i)
            UNIT_ASSERT_EQUAL(IGraphTransformer::TStatus::Repeat, ExpandApply(exprRoot, exprRoot, exprCtx).Level);

        const auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, true);
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

        auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, true);
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

        auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, true);
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

        auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, true);
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

        auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, true);
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

        auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, true);
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
}

} // namespace NYql
