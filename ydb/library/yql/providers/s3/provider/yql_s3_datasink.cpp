#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TS3DataSinkProvider : public TDataProviderBase {
public:
    TS3DataSinkProvider(TS3State::TPtr state)
        : State_(state)
        , TypeAnnotationTransformer_(CreateS3DataSinkTypeAnnotationTransformer(State_))
        , ExecutionTransformer_(CreateS3DataSinkExecTransformer(State_))
        , LogicalOptProposalTransformer_(CreateS3LogicalOptProposalTransformer(State_))
    {
    }

    TStringBuf GetName() const override {
        return S3ProviderName;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(TCoWrite::CallableName())) {
            return TS3DataSink::Match(node.Child(1));
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
            if (node.Head().Content() == S3ProviderName) {
                if (const auto clusterName = node.Child(1)->Content(); State_->Configuration->HasCluster(clusterName)) {
                    cluster = clusterName;
                    return true;
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()), TStringBuilder() << "Unknown cluster name: " << clusterName));
                    return false;
                }
            }
        }
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid S3 DataSink parameters"));
        return false;
    }

    IGraphTransformer& GetLogicalOptProposalTransformer() override {
        return *LogicalOptProposalTransformer_;
    }
private:
    const TS3State::TPtr State_;
    const THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    const THolder<TExecTransformerBase> ExecutionTransformer_;
    const THolder<IGraphTransformer> LogicalOptProposalTransformer_;
};

}

TIntrusivePtr<IDataProvider> CreateS3DataSink(TS3State::TPtr state) {
    return new TS3DataSinkProvider(state);
}

} // namespace NYql
