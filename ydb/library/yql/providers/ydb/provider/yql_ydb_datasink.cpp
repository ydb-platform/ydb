#include "yql_ydb_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYdbDataSinkProvider : public TDataProviderBase {
public:
    TYdbDataSinkProvider(TYdbState::TPtr state)
        : State_(state)
        , TypeAnnotationTransformer_(CreateYdbDataSinkTypeAnnotationTransformer(State_))
        , ExecutionTransformer_(CreateYdbDataSinkExecTransformer(State_))
        , LogicalOptProposalTransformer_(CreateYdbLogicalOptProposalTransformer(State_))
        , PhysicalOptProposalTransformer_(CreateYdbPhysicalOptProposalTransformer(State_))
    {
    }

    TStringBuf GetName() const override {
        return YdbProviderName;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(TCoWrite::CallableName())) {
            return TYdbDataSink::Match(node.Child(1));
        }

        return TypeAnnotationTransformer_->CanParse(node);
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer_;
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *ExecutionTransformer_;
    }

    bool CanExecute(const TExprNode& node) override {
        return ExecutionTransformer_->CanExec(node);
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSink::CallableName())) {
            if (node.Head().Content() == YdbProviderName) {
                if (const auto clusterName = node.Child(1)->Content(); State_->Configuration->HasCluster(clusterName)) {
                    cluster = clusterName;
                    return true;
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()), TStringBuilder() << "Unknown cluster name: " << clusterName));
                    return false;
                }
            }
        }
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Ydb DataSink parameters"));
        return false;
    }

    IGraphTransformer& GetLogicalOptProposalTransformer() override {
        return *LogicalOptProposalTransformer_;
    }

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return *PhysicalOptProposalTransformer_;
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        YQL_CLOG(INFO, ProviderYdb) << "Rewrite " << node->Content();
        const TCoWrite write(node);
        TYdbKey key;
        YQL_ENSURE(key.Extract(*node->Child(2), ctx), "Failed to extract table key.");
        const auto settings = NCommon::ParseWriteTableSettings(TExprList(node->Child(4)), ctx);
        return Build<TYdbWriteTable>(ctx, node->Pos())
            .World(write.World())
            .DataSink(write.DataSink().Ptr())
            .Table().Build(key.GetTablePath())
            .Input(node->Child(3))
            .Mode(settings.Mode.Cast())
            .Settings(settings.Other)
            .Done()
            .Ptr();
    }

    TExprNode::TPtr GetClusterInfo(const TString& cluster, TExprContext& ctx) override {
        const auto find = State_->Configuration->Clusters.find(cluster);
        if (State_->Configuration->Clusters.cend() == find) {
            return {};
        }

        const auto& config = find->second.Raw;

        TPositionHandle pos;

        TVector<TExprBase> locators;
        const auto& grpcData = config.GetGrpc();
        for (size_t index = 0; index < grpcData.LocatorsSize(); ++index) {
            locators.push_back(Build<TCoAtom>(ctx, pos)
                .Value(grpcData.GetLocators(index))
                .Done());
        }

        return Build<NNodes::TYdbClusterConfig>(ctx, pos)
            .Locators<TCoAtomList>().Add(locators).Build()
            .TvmId<TCoAtom>().Build(ToString(config.GetTvmId()))
            .Done().Ptr();
    }

private:
    const TYdbState::TPtr State_;
    const THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    const THolder<TExecTransformerBase> ExecutionTransformer_;
    const THolder<IGraphTransformer> LogicalOptProposalTransformer_;
    const THolder<IGraphTransformer> PhysicalOptProposalTransformer_;
};

}

TIntrusivePtr<IDataProvider> CreateYdbDataSink(TYdbState::TPtr state) {
    return new TYdbDataSinkProvider(state);
}

} // namespace NYql
