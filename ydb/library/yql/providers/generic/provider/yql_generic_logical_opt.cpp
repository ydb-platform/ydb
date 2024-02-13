#include "yql_generic_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

    using namespace NNodes;

    namespace {

        class TGenericLogicalOptProposalTransformer: public TOptimizeTransformerBase {
        public:
            TGenericLogicalOptProposalTransformer(TGenericState::TPtr state)
                : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderGeneric, {})
                , State_(state)
            {
#define HNDL(name) "LogicalOptimizer-" #name, Hndl(&TGenericLogicalOptProposalTransformer::name)
                AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembers));
                AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersOverDqWrap));
                AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersOverDqSourceWrap));
#undef HNDL
            }

            TMaybeNode<TExprBase> ExtractMembers(TExprBase node, TExprContext& ctx) const {
                const auto extract = node.Cast<TCoExtractMembers>();
                const auto input = extract.Input();
                const auto read = input.Maybe<TCoRight>().Input().Maybe<TGenReadTable>();
                if (!read) {
                    return node;
                }

                // clang-format off
                return Build<TCoRight>(ctx, extract.Pos())
                    .Input<TGenReadTable>()
                        .InitFrom(read.Cast())
                        .Columns(extract.Members())
                    .Build()
                    .Done();
                // clang-format on
            }

            TMaybeNode<TExprBase> ExtractMembersOverDqWrap(TExprBase node, TExprContext& ctx) const {
                auto extract = node.Cast<TCoExtractMembers>();
                auto input = extract.Input();
                auto read = input.Maybe<TDqReadWrap>().Input().Maybe<TGenReadTable>();
                if (!read) {
                    return node;
                }

                // clang-format off
                return Build<TDqReadWrap>(ctx, node.Pos())
                    .InitFrom(input.Cast<TDqReadWrap>())
                    .Input<TGenReadTable>()
                        .InitFrom(read.Cast())
                        .Columns(extract.Members())
                    .Build()
                    .Done();
                // clang-format on
            }

            TMaybeNode<TExprBase> ExtractMembersOverDqSourceWrap(TExprBase node, TExprContext& ctx) const {
                const auto extract = node.Cast<TCoExtractMembers>();
                const auto input = extract.Input();
                const auto read = input.Maybe<TDqSourceWrap>().Input().Maybe<TGenSourceSettings>();
                if (!read) {
                    return node;
                }

                // clang-format off
                return Build<TDqSourceWrap>(ctx, node.Pos())
                    .Input<TGenSourceSettings>()
                        .InitFrom(read.Cast())
                        .Columns(extract.Members())
                    .Build()
                    .DataSource(input.Cast<TDqSourceWrap>().DataSource())
                    .RowType(ExpandType(node.Pos(), GetSeqItemType(*extract.Ref().GetTypeAnn()), ctx))
                    .Done();
                // clang-format on
            }

        private:
            const TGenericState::TPtr State_;
        };

    }

    THolder<IGraphTransformer> CreateGenericLogicalOptProposalTransformer(TGenericState::TPtr state) {
        return MakeHolder<TGenericLogicalOptProposalTransformer>(state);
    }

} // namespace NYql
