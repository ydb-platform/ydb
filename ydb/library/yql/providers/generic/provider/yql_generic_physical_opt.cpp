#include "yql_generic_provider_impl.h"
#include "yql_generic_predicate_pushdown.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/pushdown/collection.h>
#include <ydb/library/yql/providers/common/pushdown/predicate_node.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NYql {

    using namespace NNodes;

    namespace {

        struct TPushdownSettings: public NPushdown::TSettings {
            TPushdownSettings()
                : NPushdown::TSettings(NLog::EComponent::ProviderGeneric)
            {
                using EFlag = NPushdown::TSettings::EFeatureFlag;
                Enable(EFlag::ExpressionAsPredicate | EFlag::ArithmeticalExpressions | EFlag::ImplicitConversionToInt64);
            }
        };

        class TGenericPhysicalOptProposalTransformer: public TOptimizeTransformerBase {
        public:
            TGenericPhysicalOptProposalTransformer(TGenericState::TPtr state)
                : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderGeneric, {})
                , State_(state)
            {
#define HNDL(name) "PhysicalOptimizer-" #name, Hndl(&TGenericPhysicalOptProposalTransformer::name)
                AddHandler(0, &TCoLeft::Match, HNDL(TrimReadWorld));
                AddHandler(0, &TCoNarrowMap::Match, HNDL(ReadZeroColumns));
                AddHandler(0, &TCoFlatMap::Match, HNDL(PushFilterToReadTable));
                AddHandler(0, &TCoFlatMap::Match, HNDL(PushFilterToDqSourceWrap));
#undef HNDL
            }

            TMaybeNode<TExprBase> TrimReadWorld(TExprBase node, TExprContext& ctx) const {
                Y_UNUSED(ctx);

                const auto& maybeRead = node.Cast<TCoLeft>().Input().Maybe<TGenReadTable>();
                if (!maybeRead) {
                    return node;
                }

                return TExprBase(maybeRead.Cast().World().Ptr());
            }

            TMaybeNode<TExprBase> ReadZeroColumns(TExprBase node, TExprContext& ctx) const {
                const auto& narrow = node.Maybe<TCoNarrowMap>();
                if (const auto& wide = narrow.Cast().Input().Maybe<TDqReadWideWrap>()) {
                    if (const auto& maybe = wide.Cast().Input().Maybe<TGenReadTable>()) {
                        if (!wide.Cast()
                                 .Ref()
                                 .GetTypeAnn()
                                 ->Cast<TFlowExprType>()
                                 ->GetItemType()
                                 ->Cast<TMultiExprType>()
                                 ->GetSize()) {
                            const auto& read = maybe.Cast();

                            // Get table metadata
                            const auto [tableMeta, issue] = State_->GetTable(
                                read.DataSource().Cluster().Value(),
                                read.Table().Value(),
                                ctx.GetPosition(node.Pos()));
                            if (issue.has_value()) {
                                ctx.AddError(issue.value());
                                return node;
                            }

                            const auto structType = tableMeta.value()->ItemType;
                            YQL_ENSURE(structType->GetSize());
                            auto columns =
                                ctx.NewList(read.Pos(), {ctx.NewAtom(read.Pos(), GetLightColumn(*structType)->GetName())});

                            // clang-format off
                            return Build<TCoNarrowMap>(ctx, narrow.Cast().Pos())
                                .Input<TDqReadWideWrap>()
                                    .InitFrom(wide.Cast())
                                    .Input<TGenReadTable>()
                                        .InitFrom(read)
                                        .Columns(std::move(columns))
                                    .Build()
                                .Build()
                                .Lambda()
                                    .Args({"stub"})
                                    .Body<TCoAsStruct>().Build()
                                .Build()
                            .Done();
                            // clang-format on
                        }
                    }
                }

                return node;
            }

            static NPushdown::TPredicateNode SplitForPartialPushdown(const NPushdown::TPredicateNode& predicateTree,
                                                                     TExprContext& ctx, TPositionHandle pos)
            {
                if (predicateTree.CanBePushed) {
                    return predicateTree;
                }

                if (predicateTree.Op != NPushdown::EBoolOp::And) {
                    return NPushdown::TPredicateNode(); // Not valid, => return the same node from optimizer
                }

                std::vector<NPushdown::TPredicateNode> pushable;
                for (auto& predicate : predicateTree.Children) {
                    if (predicate.CanBePushed) {
                        pushable.emplace_back(predicate);
                    }
                }
                NPushdown::TPredicateNode predicateToPush;
                predicateToPush.SetPredicates(pushable, ctx, pos);
                return predicateToPush;
            }

            TMaybeNode<TCoLambda> MakePushdownPredicate(const TCoLambda& lambda, TExprContext& ctx, const TPositionHandle& pos) const {
                auto lambdaArg = lambda.Args().Arg(0).Ptr();

                YQL_CLOG(TRACE, ProviderGeneric) << "Push filter. Initial filter lambda: " << NCommon::ExprToPrettyString(ctx, lambda.Ref());

                auto maybeOptionalIf = lambda.Body().Maybe<TCoOptionalIf>();
                if (!maybeOptionalIf.IsValid()) { // Nothing to push
                    return {};
                }

                TCoOptionalIf optionalIf = maybeOptionalIf.Cast();
                NPushdown::TPredicateNode predicateTree(optionalIf.Predicate());
                NPushdown::CollectPredicates(optionalIf.Predicate(), predicateTree, lambdaArg.Get(), TExprBase(lambdaArg), TPushdownSettings());
                YQL_ENSURE(predicateTree.IsValid(), "Collected filter predicates are invalid");

                NPushdown::TPredicateNode predicateToPush = SplitForPartialPushdown(predicateTree, ctx, pos);
                if (!predicateToPush.IsValid()) {
                    return {};
                }

                // clang-format off
                auto newFilterLambda = Build<TCoLambda>(ctx, pos)
                    .Args({"filter_row"})
                    .Body<TExprApplier>()
                        .Apply(predicateToPush.ExprNode.Cast())
                        .With(TExprBase(lambdaArg), "filter_row")
                        .Build()
                    .Done();
                // clang-format on

                YQL_CLOG(INFO, ProviderGeneric) << "Push filter lambda: " << NCommon::ExprToPrettyString(ctx, *newFilterLambda.Ptr());
                return newFilterLambda;
            }

            TMaybeNode<TExprBase> PushFilterToReadTable(TExprBase node, TExprContext& ctx) const {
                if (!State_->Configuration->UsePredicatePushdown.Get().GetOrElse(TGenericSettings::TDefault::UsePredicatePushdown)) {
                    return node;
                }

                auto flatmap = node.Cast<TCoFlatMap>();
                auto maybeRight = flatmap.Input().Maybe<TCoRight>();
                if (!maybeRight) {
                    return node;
                }
                auto maybeGenericRead = maybeRight.Cast().Input().Maybe<TGenReadTable>();
                if (!maybeGenericRead) {
                    return node;
                }

                TGenReadTable genericRead = maybeGenericRead.Cast();
                if (!IsEmptyFilterPredicate(genericRead.FilterPredicate())) {
                    YQL_CLOG(TRACE, ProviderGeneric) << "Push filter. Lambda is already not empty";
                    return node;
                }

                auto newFilterLambda = MakePushdownPredicate(flatmap.Lambda(), ctx, node.Pos());
                if (!newFilterLambda) {
                    return node;
                }

                // clang-format off
                return Build<TCoFlatMap>(ctx, flatmap.Pos())
                    .InitFrom(flatmap) // Leave existing filter in flatmap for the case of not applying predicate in connector
                    .Input<TCoRight>()
                        .Input<TGenReadTable>()
                            .InitFrom(genericRead)
                            .FilterPredicate(newFilterLambda.Cast())
                            .Build()
                        .Build()
                    .Done();
                // clang-format on
            }

            TMaybeNode<TExprBase> PushFilterToDqSourceWrap(TExprBase node, TExprContext& ctx) const {
                if (!State_->Configuration->UsePredicatePushdown.Get().GetOrElse(TGenericSettings::TDefault::UsePredicatePushdown)) {
                    return node;
                }

                auto flatmap = node.Cast<TCoFlatMap>();
                auto maybeSourceWrap = flatmap.Input().Maybe<TDqSourceWrap>();
                if (!maybeSourceWrap) {
                    return node;
                }

                TDqSourceWrap sourceWrap = maybeSourceWrap.Cast();
                auto maybeGenericSourceSettings = sourceWrap.Input().Maybe<TGenSourceSettings>();
                if (!maybeGenericSourceSettings) {
                    return node;
                }

                TGenSourceSettings genericSourceSettings = maybeGenericSourceSettings.Cast();
                if (!IsEmptyFilterPredicate(genericSourceSettings.FilterPredicate())) {
                    YQL_CLOG(TRACE, ProviderGeneric) << "Push filter. Lambda is already not empty";
                    return node;
                }

                auto newFilterLambda = MakePushdownPredicate(flatmap.Lambda(), ctx, node.Pos());
                if (!newFilterLambda) {
                    return node;
                }

                // clang-format off
                return Build<TCoFlatMap>(ctx, flatmap.Pos())
                    .InitFrom(flatmap) // Leave existing filter in flatmap for the case of not applying predicate in connector
                    .Input<TDqSourceWrap>()
                        .InitFrom(sourceWrap)
                        .Input<TGenSourceSettings>()
                            .InitFrom(genericSourceSettings)
                            .FilterPredicate(newFilterLambda.Cast())
                            .Build()
                        .Build()
                    .Done();
                // clang-format on
            }

        private:
            const TGenericState::TPtr State_;
        };
    }

    THolder<IGraphTransformer> CreateGenericPhysicalOptProposalTransformer(TGenericState::TPtr state) {
        return MakeHolder<TGenericPhysicalOptProposalTransformer>(state);
    }

} // namespace NYql
