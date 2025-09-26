#include "yql_generic_provider_impl.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <yql/essentials/utils/log/log.h>

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
                AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersRead<TCoRight>));
                AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersRead<TDqReadWrap>));
                AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersSource<TDqSourceWrap>));
                AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersSource<TDqLookupSourceWrap>));
#undef HNDL
            }

            template <class TDqReadWrap>
            TMaybeNode<TExprBase> ExtractMembersRead(TExprBase node, TExprContext& ctx) const {
                const auto extract = node.Cast<TCoExtractMembers>();
                const auto input = extract.Input();
                const auto read = input.Maybe<TDqReadWrap>().Input().template Maybe<TGenReadTable>();
                if (!read) {
                    return node;
                }

                // clang-format off
                return Build<TDqReadWrap>(ctx, extract.Pos())
                    .InitFrom(input.Cast<TDqReadWrap>())
                    .template Input<TGenReadTable>()
                        .InitFrom(read.Cast())
                        .Columns(extract.Members())
                    .Build()
                    .Done();
                // clang-format on
            }

            template <class TDqSourceWrap>
            TMaybeNode<TExprBase> ExtractMembersSource(TExprBase node, TExprContext& ctx) const {
                const auto extract = node.Cast<TCoExtractMembers>();
                const auto input = extract.Input();
                const auto read = input.Maybe<TDqSourceWrap>().Input().template Maybe<TGenSourceSettings>();
                if (!read) {
                    return node;
                }

                // clang-format off
                return Build<TDqSourceWrap>(ctx, node.Pos())
                    .InitFrom(input.Cast<TDqSourceWrap>())
                    .template Input<TGenSourceSettings>()
                        .InitFrom(read.Cast())
                        .Columns(extract.Members())
                    .Build()
                    .RowType(ExpandType(node.Pos(), GetSeqItemType(*extract.Ref().GetTypeAnn()), ctx))
                    .Done();
                // clang-format on
            }

        private:
            const TGenericState::TPtr State_;
        };

    } // namespace

    THolder<IGraphTransformer> CreateGenericLogicalOptProposalTransformer(TGenericState::TPtr state) {
        return MakeHolder<TGenericLogicalOptProposalTransformer>(state);
    }

} // namespace NYql
