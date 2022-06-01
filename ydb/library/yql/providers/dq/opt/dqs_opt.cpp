#include "dqs_opt.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy_finalizing.h>
#include <ydb/library/yql/dq/opt/dq_opt_build.h>
#include <ydb/library/yql/dq/opt/dq_opt_peephole.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_mem_info.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/string/split.h>

#define PERFORM_RULE(func, ...)                                            \
    do {                                                                   \
        if (auto result = func(__VA_ARGS__); result.Raw() != node.Raw()) { \
            YQL_CLOG(DEBUG, ProviderDq) << #func;                          \
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
                    PERFORM_RULE(DqPeepholeDropUnusedInputs, node, ctx);
                    return inputExpr;
                }, ctx, optSettings);
        });
    }

    THolder<IGraphTransformer> CreateDqsReplacePrecomputesTransformer(TTypeAnnotationContext* typesCtx, const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry) {
        return CreateFunctorTransformer([typesCtx, funcRegistry](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            TProcessedNodesSet ignoreNodes;
            VisitExpr(input, [&](const TExprNode::TPtr& node) {
                if (node != input && (TDqReadWrapBase::Match(node.Get()) || TDqPhyPrecompute::Match(node.Get()))) {
                    ignoreNodes.insert(node->UniqueId());
                    return false;
                }
                return true;
            });

            TOptimizeExprSettings settings(typesCtx);
            settings.ProcessedNodes = &ignoreNodes;

            NKikimr::NMiniKQL::TScopedAlloc alloc;
            NKikimr::NMiniKQL::TTypeEnvironment env(alloc);
            NKikimr::NMiniKQL::TProgramBuilder pgmBuilder(env, *funcRegistry);
            NKikimr::NMiniKQL::TMemoryUsageInfo memInfo("Precompute");
            NKikimr::NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo);

            return OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                if (TDqStageBase::Match(node.Get())) {
                    auto stage = TDqStageBase(node);
                    TNodeOnNodeOwnedMap replaces;
                    std::vector<TCoArgument> newArgs;
                    std::vector<TExprNode::TPtr> newInputs;
                    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
                        const auto& input = stage.Inputs().Item(i);
                        if (input.Maybe<TDqPhyPrecompute>() && input.Ref().HasResult()) {
                            auto yson = input.Ref().GetResult().Content();
                            auto dataNode = NYT::NodeFromYsonString(yson);
                            YQL_ENSURE(dataNode.IsList() && !dataNode.AsList().empty());
                            dataNode = dataNode[0];
                            TStringStream err;
                            NKikimr::NMiniKQL::TType* mkqlType = NCommon::BuildType(*input.Ref().GetTypeAnn(), pgmBuilder, err);
                            if (!mkqlType) {
                                ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Failed to process " << TDqPhyPrecompute::CallableName() << " type: " << err.Str()));
                                return nullptr;
                            }

                            auto value = NCommon::ParseYsonNodeInResultFormat(holderFactory, dataNode, mkqlType, &err);
                            if (!value) {
                                ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Failed to parse " << TDqPhyPrecompute::CallableName() << " value: " << err.Str()));
                                return nullptr;
                            }

                            replaces[stage.Program().Args().Arg(i).Raw()] = NCommon::ValueToExprLiteral(input.Ref().GetTypeAnn(), *value, ctx, input.Pos());
                        } else {
                            newArgs.push_back(stage.Program().Args().Arg(i));
                            newInputs.push_back(input.Ptr());
                        }
                    }

                    if (!replaces.empty()) {
                        YQL_CLOG(DEBUG, ProviderDq) << "DqsReplacePrecomputes";
                        auto children = stage.Ref().ChildrenList();
                        children[TDqStageBase::idx_Program] = Build<TCoLambda>(ctx, stage.Program().Pos())
                            .Args(newArgs)
                            .Body(ctx.ReplaceNodes(stage.Program().Body().Ptr(), replaces))
                            .Done().Ptr();
                        children[TDqStageBase::idx_Inputs] = ctx.NewList(stage.Inputs().Pos(), std::move(newInputs));
                        return ctx.ChangeChildren(stage.Ref(), std::move(children));
                    }
                }
                return node;
            }, ctx, settings);
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
