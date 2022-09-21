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
            return result.Ptr();                                             \
        }                                                                  \
    } while (0)

namespace NYql::NDqs {
    using namespace NYql;
    using namespace NYql::NDq;
    using namespace NYql::NNodes;

    using TStatus = IGraphTransformer::TStatus;

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
                    PERFORM_RULE(DqPeepholeRewriteLength, node, ctx);
                    return inputExpr;
                }, ctx, optSettings);
        });
    }

    THolder<IGraphTransformer> CreateDqsReplacePrecomputesTransformer(TTypeAnnotationContext* typesCtx, const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry) {
        return CreateFunctorTransformer([typesCtx, funcRegistry](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> TStatus {
            TOptimizeExprSettings settings(typesCtx);
            settings.VisitChecker = [&](const TExprNode& node) {
                return input.Get() == &node || (!TDqReadWrapBase::Match(&node) && !TDqPhyPrecompute::Match(&node));
            };
            settings.VisitStarted = true;

            NKikimr::NMiniKQL::TScopedAlloc alloc;
            NKikimr::NMiniKQL::TTypeEnvironment env(alloc);
            NKikimr::NMiniKQL::TProgramBuilder pgmBuilder(env, *funcRegistry);
            NKikimr::NMiniKQL::TMemoryUsageInfo memInfo("Precompute");
            NKikimr::NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo);

            auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
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

            if (status.Level != TStatus::Ok) {
                return status;
            }

            auto precomputes = FindNodes(output,
                [](const TExprNode::TPtr& node) {
                    return !TDqReadWrapBase::Match(node.Get());
                },
                [] (const TExprNode::TPtr& node) {
                    return TDqPhyPrecompute::Match(node.Get()) && node->HasResult();
                }
            );

            if (!precomputes.empty()) {
                TNodeOnNodeOwnedMap replaces;
                for (auto node: precomputes) {
                    auto yson = node->GetResult().Content();
                    auto dataNode = NYT::NodeFromYsonString(yson);
                    YQL_ENSURE(dataNode.IsList() && !dataNode.AsList().empty());
                    dataNode = dataNode[0];
                    TStringStream err;
                    NKikimr::NMiniKQL::TType* mkqlType = NCommon::BuildType(*node->GetTypeAnn(), pgmBuilder, err);
                    if (!mkqlType) {
                        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Failed to process " << TDqPhyPrecompute::CallableName() << " type: " << err.Str()));
                        return TStatus::Error;
                    }

                    auto value = NCommon::ParseYsonNodeInResultFormat(holderFactory, dataNode, mkqlType, &err);
                    if (!value) {
                        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Failed to parse " << TDqPhyPrecompute::CallableName() << " value: " << err.Str()));
                        return TStatus::Error;
                    }
                    replaces[node.Get()] = NCommon::ValueToExprLiteral(node->GetTypeAnn(), *value, ctx, node->Pos());
                }
                TOptimizeExprSettings settings(typesCtx);
                settings.VisitStarted = true;
                YQL_CLOG(DEBUG, ProviderDq) << "DqsReplacePrecomputes";
                return RemapExpr(output, output, replaces, ctx, settings);
            }

            return TStatus::Ok;
        });
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
