#include "yql.h"

#include "cluster.h"
#include "table.h"

#define USE_CURRENT_UDF_ABI_VERSION
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/services/yql_eval_expr.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/iterator/iterate_keys.h>

namespace NSQLComplete {

    THashSet<TString> TYqlContext::Clusters() const {
        auto keys = IterateKeys(TablesByCluster);
        return {keys.begin(), keys.end()};
    }

    TMaybe<TYqlContext> IYqlAnalysis::Analyze(NYql::TAstNode& root, NYql::TIssues& issues) const {
        NYql::TExprContext ctx;
        NYql::TExprNode::TPtr expr;
        if (!NYql::CompileExpr(root, expr, ctx, /* resolver = */ nullptr, /* urlListerManager = */ nullptr)) {
            for (NYql::TIssue issue : ctx.IssueManager.GetIssues()) {
                issues.AddIssue(std::move(issue));
            }
            return Nothing();
        }
        return Analyze(expr, ctx);
    }

    namespace {

        class TYqlAnalysis: public IYqlAnalysis {
        public:
            TYqlAnalysis()
                : FunctionRegistry_(
                      NKikimr::NMiniKQL::CreateFunctionRegistry(
                          NKikimr::NMiniKQL::CreateBuiltinRegistry()))
                , Types_(MakeIntrusive<NYql::TTypeAnnotationContext>())
            {
            }

            TYqlContext Analyze(NYql::TExprNode::TPtr root, NYql::TExprContext& ctx) const override {
                root = Optimized(std::move(root), ctx);

                TYqlContext yqlCtx;

                yqlCtx.TablesByCluster = CollectTablesByCluster(*root);

                for (TString cluster : CollectClusters(*root)) {
                    Y_UNUSED(yqlCtx.TablesByCluster[std::move(cluster)]);
                }

                return yqlCtx;
            }

        private:
            NYql::TExprNode::TPtr Optimized(NYql::TExprNode::TPtr expr, NYql::TExprContext& ctx) const {
                constexpr size_t AttemptsLimit = 128;

                for (size_t i = 0; i < AttemptsLimit; ++i) {
                    auto status = NYql::EvaluateExpression(expr, expr, *Types_, ctx, *FunctionRegistry_);
                    if (status.Level != NYql::IGraphTransformer::TStatus::Repeat) {
                        Y_ENSURE(status == NYql::IGraphTransformer::TStatus::Ok, "" << status);
                        return expr;
                    }
                }

                ythrow yexception() << "Optimization was not converged after "
                                    << AttemptsLimit << " attempts";
            }

            static void Print(IOutputStream& out, const NYql::TExprNode& root, NYql::TExprContext& ctx) {
                auto ast = ConvertToAst(root, ctx, NYql::TExprAnnotationFlags::None, true);
                ast.Root->PrettyPrintTo(out, NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);
            }

            TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FunctionRegistry_;
            NYql::TTypeAnnotationContextPtr Types_;
        };

    } // namespace

    IYqlAnalysis::TPtr MakeYqlAnalysis() {
        return new TYqlAnalysis();
    }

} // namespace NSQLComplete
