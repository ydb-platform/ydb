#include "yql_generic_provider_impl.h"
#include "yql_generic_predicate_pushdown.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/pushdown/collection.h>
#include <ydb/library/yql/providers/common/pushdown/physical_opt.h>
#include <ydb/library/yql/providers/common/pushdown/predicate_node.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

namespace NYql {

    using namespace NNodes;

    namespace {

        struct TPushdownSettings: public NPushdown::TSettings {
            TPushdownSettings()
                : NPushdown::TSettings(NLog::EComponent::ProviderGeneric)
            {
                using EFlag = NPushdown::TSettings::EFeatureFlag;
                Enable(
                    EFlag::ExpressionAsPredicate | 
                    EFlag::ArithmeticalExpressions | 
                    EFlag::ImplicitConversionToInt64 | 
                    EFlag::DateTimeTypes |
                    EFlag::TimestampCtor |
                    EFlag::StringTypes |
                    EFlag::LikeOperator |
                    EFlag::JustPassthroughOperators | // To pushdown REGEXP over String column
                    EFlag::FlatMapOverOptionals | // To pushdown REGEXP over Utf8 column
                    EFlag::ToStringFromStringExpressions // To pushdown REGEXP over Utf8 column
                );
                EnableFunction("Re2.Grep");  // For REGEXP pushdown
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
                            const auto [tableMeta, issues] = State_->GetTable(
                                TGenericState::TTableAddress(
                                    TString(read.DataSource().Cluster().Value()),
                                    TString(read.Table().Name().Value())
                                )
                            );
                            if (issues) {
                                for (const auto& issue : issues) {
                                    ctx.AddError(issue);
                                }
                                return node;
                            }

                            const auto structType = tableMeta->ItemType;
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

                auto newFilterLambda = NPushdown::MakePushdownPredicate(flatmap.Lambda(), ctx, node.Pos(), TPushdownSettings());
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

                auto newFilterLambda = NPushdown::MakePushdownPredicate(flatmap.Lambda(), ctx, node.Pos(), TPushdownSettings());
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
    } // namespace

    THolder<IGraphTransformer> CreateGenericPhysicalOptProposalTransformer(TGenericState::TPtr state) {
        return MakeHolder<TGenericPhysicalOptProposalTransformer>(state);
    }

} // namespace NYql
