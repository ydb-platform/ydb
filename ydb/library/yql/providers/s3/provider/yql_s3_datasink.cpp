#include "yql_s3_provider_impl.h"
#include "yql_s3_dq_integration.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

void ScanPlanDependencies(const TExprNode::TPtr& input, TExprNode::TListType& children) {
    VisitExpr(input, [&children](const TExprNode::TPtr& node) {
        if (node->IsCallable("DqCnResult")) {
            children.push_back(node->HeadPtr());
            return false;
        }

        return true;
    });
}

class TS3DataSinkProvider : public TDataProviderBase {
public:
    TS3DataSinkProvider(TS3State::TPtr state)
        : State_(state)
        , TypeAnnotationTransformer_(CreateS3DataSinkTypeAnnotationTransformer(State_))
        , ExecutionTransformer_(CreateS3DataSinkExecTransformer(State_))
        , LogicalOptProposalTransformer_(CreateS3LogicalOptProposalTransformer(State_))
        , PhysicalOptProposalTransformer_(CreateS3PhysicalOptProposalTransformer(State_))
        , DqIntegration_(CreateS3DqIntegration(State_))
    {}
private:
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

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return *PhysicalOptProposalTransformer_;
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

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& write, TExprContext& ctx) override {
        const TS3Write w(write);
        auto settings = write->Tail().ChildrenList();

        TExprNode::TPtr format = ExtractFormat(settings);
        if (!format) {
            ctx.AddError(TIssue(ctx.GetPosition(write->Pos()), "Missing format - please use WITH FORMAT when writing into S3"));
            return nullptr;
        }

        return Build<TS3WriteObject>(ctx, w.Pos())
                .World(w.World())
                .DataSink(w.DataSink())
                .Target<TS3Target>()
                    .Path(write->Child(2U)->Head().Tail().HeadPtr())
                    .Format(std::move(format))
                    .Settings(ctx.NewList(w.Pos(), std::move(settings)))
                    .Build()
                .Input(write->ChildPtr(3))
            .Done().Ptr();
    }

    void GetOutputs(const TExprNode& node, TVector<TPinInfo>& outputs) override {
        if (const auto& maybeOp = TMaybeNode<TS3WriteObject>(&node)) {
            const auto& op = maybeOp.Cast();
            outputs.push_back(TPinInfo(nullptr, op.DataSink().Raw(), op.Target().Raw(), op.DataSink().Cluster().StringValue() + '.' + op.Target().Path().StringValue(), false));
        }
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool) override {
        if (CanExecute(node)) {
            children.push_back(node.HeadPtr());

            if (TS3WriteObject::Match(&node)) {
                ScanPlanDependencies(node.ChildPtr(TS3WriteObject::idx_Input), children);
            }

            return true;
        }

        return false;
    }

    IDqIntegration* GetDqIntegration() override {
        return DqIntegration_.Get();
    }

    const TS3State::TPtr State_;
    const THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    const THolder<TExecTransformerBase> ExecutionTransformer_;
    const THolder<IGraphTransformer> LogicalOptProposalTransformer_;
    const THolder<IGraphTransformer> PhysicalOptProposalTransformer_;
    const THolder<IDqIntegration> DqIntegration_;
};

}

TIntrusivePtr<IDataProvider> CreateS3DataSink(TS3State::TPtr state) {
    return new TS3DataSinkProvider(std::move(state));
}

} // namespace NYql
