#include "yql_yt_provider_impl.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>

#include <ydb/library/yql/core/yql_graph_transformer.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYtDataSinkTrackableCleanupTransformer : public TAsyncCallbackTransformer<TYtDataSinkTrackableCleanupTransformer> {
public:
    TYtDataSinkTrackableCleanupTransformer(TYtState::TPtr state)
        : State_(state) {
    }

    std::pair<TStatus, TAsyncTransformCallbackFuture>
    CallbackTransform(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        Y_UNUSED(ctx);
        output = input;
        auto options = IYtGateway::TDropTrackablesOptions(State_->SessionId)
            .Config(State_->Configuration->GetSettingsForNode(*input));


        YQL_ENSURE(input->IsList());
        input->ForEachChild([&options](const TExprNode& node)
                            {
                                YQL_ENSURE(node.IsList());
                                YQL_ENSURE(node.ChildrenSize() == 2);
                                auto ytDataSink = TMaybeNode<TYtDSink>(node.Child(0));
                                YQL_ENSURE(ytDataSink);
                                auto ytOutTable = TMaybeNode<TYtOutTable>(node.Child(1));
                                YQL_ENSURE(ytOutTable);

                                IYtGateway::TDropTrackablesOptions::TClusterAndPath clusterAndPath;
                                clusterAndPath.Path = TString{ytOutTable.Cast().Name().Value()};
                                clusterAndPath.Cluster = TString{ytDataSink.Cast().Cluster().Value()};

                                options.Pathes().push_back(clusterAndPath);
                            });

        auto future = State_->Gateway->DropTrackables(std::move(options));

        return WrapFuture(future,
                          [](const IYtGateway::TDropTrackablesResult& res, const TExprNode::TPtr& input, TExprContext& ctx) {
                              Y_UNUSED(res);
                              return ctx.NewWorld(input->Pos());
                          });
    }
private:
    TYtState::TPtr State_;
};

}

THolder<IGraphTransformer> CreateYtDataSinkTrackableNodesCleanupTransformer(TYtState::TPtr state) {
    return THolder(new TYtDataSinkTrackableCleanupTransformer(state));
}

} // NYql
