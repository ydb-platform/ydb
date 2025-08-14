#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/provider/yql_solomon_provider.h>
#include <ydb/library/yql/providers/solomon/scheme/yql_solomon_scheme.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>

namespace NYql {

using namespace NNodes;

namespace {

class TSolomonLogicalOptProposalTransformer: public TOptimizeTransformerBase {
public:
    explicit TSolomonLogicalOptProposalTransformer(TSolomonState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderGeneric, {})
        , State_(state)
    {
#define HNDL(name) "LogicalOptimizer-" #name, Hndl(&TSolomonLogicalOptProposalTransformer::name)
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersOverDqSource));
#undef HNDL
    }

    TMaybeNode<TExprBase> ExtractMembersOverDqSource(TExprBase node, TExprContext& ctx) const {
        const auto& extract = node.Cast<TCoExtractMembers>();
        const auto& maybeDqSource = extract.Input().Maybe<TDqSourceWrap>();
        if (!maybeDqSource) {
            return node;
        }

        const auto& dqSource = maybeDqSource.Cast();
        if (dqSource.DataSource().Category() != SolomonProviderName) {
            return node;
        }

        const auto& maybeSoSourceSettings = dqSource.Input().Maybe<TSoSourceSettings>();
        if (!maybeSoSourceSettings) {
            return node;
        }

        TSet<TStringBuf> extractMembers;
        for (auto member : extract.Members()) {
            extractMembers.insert(member.Value());
        }

        const auto& systemColumns = maybeSoSourceSettings.SystemColumns().Cast();
        const auto& labelNames = maybeSoSourceSettings.LabelNames().Cast();
        const auto& labelNameAliases = maybeSoSourceSettings.LabelNameAliases().Cast();
        YQL_ENSURE(labelNames.Size() == labelNameAliases.Size());

        TVector<TCoAtom> newSystemColumns;
        TVector<TCoAtom> newLabelNames;
        TVector<TCoAtom> newLabelNameAliases;

        newSystemColumns.reserve(extractMembers.size());
        newLabelNames.reserve(extractMembers.size());

        for (const auto& atom : systemColumns) {
            if (TString column = atom.StringValue(); extractMembers.contains(column)) {
                newSystemColumns.push_back(Build<TCoAtom>(ctx, node.Pos()).Value(column).Done());
            }
        }

        for (size_t i = 0; i < labelNames.Size(); ++i) {
            if (TString column = labelNames.Item(i).StringValue(); extractMembers.contains(column)) {
                newLabelNames.push_back(Build<TCoAtom>(ctx, node.Pos()).Value(column).Done());
                newLabelNameAliases.push_back(Build<TCoAtom>(ctx, node.Pos()).Value(labelNameAliases.Item(i).StringValue()).Done());
            }
        }

        const auto rowType = node.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

        TVector<const TItemExprType*> newColumnTypes;
        newColumnTypes.reserve(extractMembers.size());
        for (const auto& item : rowType->GetItems()) {
            if (extractMembers.contains(item->GetName())) {
                newColumnTypes.push_back(ctx.MakeType<TItemExprType>(item->GetName(), item->GetItemType()));
            }
        }

        auto newRowTypeNode = ExpandType(node.Pos(), *ctx.MakeType<TStructExprType>(newColumnTypes), ctx);

        return Build<TDqSourceWrap>(ctx, dqSource.Pos())
            .InitFrom(dqSource)
            .Input<TSoSourceSettings>()
                .InitFrom(maybeSoSourceSettings.Cast())
                .RowType(newRowTypeNode)
                .SystemColumns<TCoAtomList>()
                    .Add(std::move(newSystemColumns))
                    .Build()
                .LabelNames<TCoAtomList>()
                    .Add(std::move(newLabelNames))
                    .Build()
                .LabelNameAliases<TCoAtomList>()
                    .Add(std::move(newLabelNameAliases))
                    .Build()
                .Build()
            .RowType(newRowTypeNode)
            .Done();
    }

private:
    const TSolomonState::TPtr State_;
};

} // namespace

THolder<IGraphTransformer> CreateSolomonLogicalOptProposalTransformer(TSolomonState::TPtr state) {
    return MakeHolder<TSolomonLogicalOptProposalTransformer>(state);
}

} // namespace NYql
