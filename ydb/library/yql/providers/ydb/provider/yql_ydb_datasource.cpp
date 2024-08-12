#include "yql_ydb_provider_impl.h"
#include "yql_ydb_dq_integration.h"

#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYdbDataSourceProvider : public TDataProviderBase {
public:
    TYdbDataSourceProvider(
        TYdbState::TPtr state,
        NYdb::TDriver driver)
        : State_(state)
        , IODiscoveryTransformer_(CreateYdbIODiscoveryTransformer(State_))
        , LoadMetaDataTransformer_(CreateYdbLoadTableMetadataTransformer(State_, driver))
        , CallableExecutionTransformer_(CreateYdbSourceCallableExecutionTransformer(State_))
        , TypeAnnotationTransformer_(CreateYdbDataSourceTypeAnnotationTransformer(State_))
        , DqIntegration_(CreateYdbDqIntegration(State_))
    {}

    TStringBuf GetName() const override {
        return YdbProviderName;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSource::CallableName())) {
            if (node.Child(0)->Content() == YdbProviderName) {
                auto clusterName = node.Child(1)->Content();
                if (!State_->Configuration->HasCluster(clusterName)) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()), TStringBuilder() <<
                        "Unknown cluster name: " << clusterName));
                    return false;
                }
                cluster = clusterName;
                return true;
            }
        }
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Ydb DataSource parameters"));
        return false;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(TCoRead::CallableName())) {
            return NNodes::TYdbDataSource::Match(node.Child(1));
        }
        return TypeAnnotationTransformer_->CanParse(node);
    }

    IGraphTransformer& GetIODiscoveryTransformer() override {
        return *IODiscoveryTransformer_;
    }

    IGraphTransformer& GetLoadTableMetadataTransformer() override {
        return *LoadMetaDataTransformer_;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer_;
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *CallableExecutionTransformer_;
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        YQL_CLOG(INFO, ProviderYdb) << "RewriteIO";
        return node;
    }

    bool CanPullResult(const TExprNode& node, TSyncMap& syncList, bool& canRef) override {
        Y_UNUSED(syncList);
        canRef = false;

        if (node.IsCallable(TCoRight::CallableName())) {
            if (node.Head().IsCallable(TYdbReadTableScheme::CallableName())) {
                return true;
            }
        }

        return false;
    }

    bool CanExecute(const TExprNode& node) override {
        if (node.IsCallable(TYdbReadTableScheme::CallableName())) {
            return true;
        }

        return false;
    }

    void GetRequiredChildren(const TExprNode& node, TExprNode::TListType& children) override {
        if (CanExecute(node)) {
            children.push_back(node.ChildPtr(0));
        }
    }

    const THashMap<TString, TString>* GetClusterTokens() override {
        return &State_->Configuration->Tokens;
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(compact);

        for (auto& child : node.Children()) {
            children.push_back(child.Get());
        }

        if (TMaybeNode<TYdbReadTable>(&node)) {
            return true;
        }
        return false;
    }

    ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>& inputs, bool withLimits) override {
        Y_UNUSED(withLimits);
        if (auto maybeRead = TMaybeNode<TYdbReadTable>(&node)) {
            if (auto maybeTable = maybeRead.Table()) {
                TStringBuilder tableNameBuilder;
                if (auto dataSource = maybeRead.DataSource().Maybe<NNodes::TYdbDataSource>()) {
                    auto cluster = dataSource.Cast().Cluster();
                    tableNameBuilder << cluster.Value() << ".";
                }
                tableNameBuilder  << '`' << maybeTable.Cast().Value() << '`';
                inputs.push_back(TPinInfo(maybeRead.DataSource().Raw(), nullptr, maybeTable.Cast().Raw(), tableNameBuilder, false));
                return 1;
            }
        }
        return 0;
    }

    IDqIntegration* GetDqIntegration() override {
        return DqIntegration_.Get();
    }


private:
    const TYdbState::TPtr State_;
    const THolder<IGraphTransformer> IODiscoveryTransformer_;
    const THolder<IGraphTransformer> LoadMetaDataTransformer_;
    const THolder<IGraphTransformer> CallableExecutionTransformer_;
    const THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    const THolder<IDqIntegration> DqIntegration_;
};

}

TIntrusivePtr<IDataProvider> CreateYdbDataSource(
    TYdbState::TPtr state,
    NYdb::TDriver driver) {
    return new TYdbDataSourceProvider(state, driver);
}

} // namespace NYql
