#include "dqs_opt.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy_finalizing.h>
#include <ydb/library/yql/dq/opt/dq_opt_build.h>
#include <ydb/library/yql/dq/opt/dq_opt_peephole.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

#include <ydb/library/yql/utils/log/log.h>

#include <util/string/split.h>

#define PERFORM_RULE(func, ...)                                            \
    do {                                                                   \
        if (auto result = func(__VA_ARGS__); result.Raw() != node.Raw()) { \
            node = result;                                                 \
            return node.Ptr();                                             \
        }                                                                  \
    } while (0)

namespace NYql::NDqs {
    using namespace NYql;
    using namespace NYql::NDq;
    using namespace NYql::NNodes;

    using TStatus = IGraphTransformer::TStatus;

    THolder<IGraphTransformer> CreateDqsWrapListsOptTransformer() {
        return CreateFunctorTransformer(
            [&](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> TStatus {
                YQL_ENSURE(input->GetTypeAnn() != nullptr);
                YQL_ENSURE(input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List);
                if (TExprBase(input).Maybe<TDqCnUnionAll>()) {
                    return TStatus(TStatus::ELevel::Ok, false);
                }

                output = Build<TDqCnUnionAll>(ctx, input->Pos()) // clang-format off
                    .Output()
                        .Stage<TDqStage>()
                            .Inputs()
                                .Build()
                            .Program()
                                .Args({})
                                .Body<TCoIterator>() 
                                    .List(input) 
                                    .Build()
                                .Build()
                            .Settings(TDqStageSettings().BuildNode(ctx, input->Pos()))
                            .Build()
                        .Index()
                            .Build("0")
                        .Build()
                    .Done().Ptr(); // clang-format on

                return TStatus(TStatus::ELevel::Repeat, true);
            });
    }

    THolder<NYql::IGraphTransformer> CreateDqsBuildTransformer() {
        return CreateFunctorTransformer([](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> TStatus {
            TExprBase node(input);
            if (node.Maybe<TDqCnResult>()) {
                return TStatus::Ok;
            }

            if (!node.Maybe<TDqCnUnionAll>()) {
                ctx.AddError(TIssue(input->Pos(ctx), "Last connection must be union all"));
                output = input;
                return TStatus::Error;
            }

            output = Build<TDqCnResult>(ctx, input->Pos()) // clang-format off
              .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(node.Cast<TDqCnUnionAll>())
                        .Build()
                    .Program()
                        .Args({"row"})
                        .Body("row") 
                        .Build() 
                    .Settings(TDqStageSettings().BuildNode(ctx, node.Pos()))
                    .Build() 
                .Index()
                    .Build("0")
                .Build() 
              .ColumnHints() // TODO: set column hints
                .Build()
              .Done().Ptr(); // clang-format on
            return TStatus(IGraphTransformer::TStatus::Repeat, true);
        });
    }

    THolder<IGraphTransformer> CreateDqsRewritePhyCallablesTransformer() {
        return CreateFunctorTransformer([](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            TOptimizeExprSettings optSettings{nullptr};
            optSettings.VisitLambdas = true;
            return OptimizeExprEx(input, output,
                [](const TExprNode::TPtr& inputExpr, TExprContext& ctx, IOptimizationContext&) {
                    TExprBase node{inputExpr};
                    PERFORM_RULE(DqPeepholeRewriteCrossJoin, node, ctx);
                    PERFORM_RULE(DqPeepholeRewriteJoinDict, node, ctx);
                    PERFORM_RULE(DqPeepholeRewriteMapJoin, node, ctx);
                    PERFORM_RULE(DqPeepholeRewritePureJoin, node, ctx); 
                    PERFORM_RULE(DqPeepholeRewriteReplicate, node, ctx);
                    return inputExpr;
                }, ctx, optSettings);
        });
    }

    namespace NPeephole {

        class TDqsPeepholeTransformer: public TSyncTransformerBase {
        public:
            TDqsPeepholeTransformer(THolder<IGraphTransformer>&& typeAnnTransformer,
                                    TTypeAnnotationContext& typesCtx)
                : TypeAnnTransformer(std::move(typeAnnTransformer))
                , TypesCtx(typesCtx)
            {
            }

            TStatus DoTransform(TExprNode::TPtr inputExpr, TExprNode::TPtr& outputExpr, TExprContext& ctx) final {
                if (Optimized) {
                    outputExpr = inputExpr;
                    return TStatus::Ok;
                }

                auto transformer = CreateDqsRewritePhyCallablesTransformer();
                auto status = InstantTransform(*transformer, inputExpr, ctx);
                if (status.Level != TStatus::Ok) {
                    ctx.AddError(TIssue(ctx.GetPosition(inputExpr->Pos()), TString("Peephole optimization failed for Dq stage")));
                    return TStatus::Error;
                }

                bool hasNonDeterministicFunctions = false;
                status = PeepHoleOptimizeNode<true>(inputExpr, inputExpr, ctx, TypesCtx, TypeAnnTransformer.Get(), hasNonDeterministicFunctions);
                if (status.Level != TStatus::Ok) {
                    ctx.AddError(TIssue(ctx.GetPosition(inputExpr->Pos()), TString("Peephole optimization failed for Dq stage")));
                    return TStatus::Error;
                }

                outputExpr = inputExpr;
                Optimized = true;
                return TStatus::Ok;
            }

            void Rewind() final {
                Optimized = false;
            }

        private:
            THolder<IGraphTransformer> TypeAnnTransformer;
            TTypeAnnotationContext& TypesCtx;
            bool Optimized = false;
        };
    }

    THolder<IGraphTransformer> CreateDqsPeepholeTransformer(THolder<IGraphTransformer>&& typeAnnTransformer, TTypeAnnotationContext& typesCtx) {
        return MakeHolder<NPeephole::TDqsPeepholeTransformer>(std::move(typeAnnTransformer), typesCtx);
    }

    THolder<IGraphTransformer> CreateDqsFinalizingOptTransformer() {
        return CreateFunctorTransformer(
            [](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                auto status = NDq::DqReplicateStageMultiOutput(input, output, ctx);
                if (status.Level != TStatus::Error && input != output) {
                    YQL_CVLOG(NLog::ELevel::INFO, NLog::EComponent::ProviderDq) << "DqReplicateStageMultiOutput";
                }
                return status;
            });
    }
}
