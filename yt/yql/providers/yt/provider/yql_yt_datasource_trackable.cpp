#include "yql_yt_provider_impl.h"

#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>

#include <yql/essentials/core/yql_graph_transformer.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYtDataSourceTrackableCleanupTransformer : public TAsyncCallbackTransformer<TYtDataSourceTrackableCleanupTransformer> {
public:
    TYtDataSourceTrackableCleanupTransformer(TYtState::TPtr state)
        : State_(state) {
    }

    std::pair<TStatus, TAsyncTransformCallbackFuture>
    CallbackTransform(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        Y_UNUSED(ctx);
        output = input;
        auto options = IYtGateway::TUnlockTablesOptions(State_->SessionId)
            .Config(State_->Configuration->GetSettingsForNode(*input));

        YQL_ENSURE(input->IsList());
        input->ForEachChild([this, &options](const TExprNode& node) {
            auto maybeTable = TMaybeNode<TYtTable>(&node);
            YQL_ENSURE(maybeTable);
            auto table = maybeTable.Cast();

            IYtGateway::TUnlockTablesOptions::TUnlockTable unlockTable;
            unlockTable.Cluster = TString{table.Cluster().Value()};
            unlockTable.Epoch = TEpochInfo::Parse(table.Epoch().Ref()).GetOrElse(0);
            if (NYql::GetSetting(table.Settings().Ref(), EYtSettingType::Anonymous)) {
                auto tableLabel = TString{TYtTableInfo::GetTableLabel(table)};
                unlockTable.Path = State_->AnonymousLabels.Value(std::make_pair(unlockTable.Cluster, tableLabel), TString());
                unlockTable.Anonymous = true;
                YQL_ENSURE(unlockTable.Path, "Unaccounted anonymous table: " << unlockTable.Cluster << '.' << tableLabel);
            } else {
                unlockTable.Path = TString{table.Name().Value()};
                unlockTable.Anonymous = false;
            }

            options.Tables().push_back(unlockTable);
        });

        auto future = State_->Gateway->UnlockTables(std::move(options));

        return WrapFuture(future,
            [](const IYtGateway::TUnlockTablesResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                Y_UNUSED(res);
                return ctx.NewWorld(input->Pos());
        });
    }
private:
    TYtState::TPtr State_;
};

}

THolder<IGraphTransformer> CreateYtDataSourceTrackableNodesCleanupTransformer(TYtState::TPtr state) {
    return THolder(new TYtDataSourceTrackableCleanupTransformer(state));
}

} // NYql
