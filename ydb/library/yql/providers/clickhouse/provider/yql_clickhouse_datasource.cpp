#include "yql_clickhouse_provider_impl.h"
#include "yql_clickhouse_dq_integration.h"

#include <ydb/library/yql/providers/clickhouse/expr_nodes/yql_clickhouse_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TClickHouseDataSource : public TDataProviderBase {
public:
    TClickHouseDataSource(TClickHouseState::TPtr state, IHTTPGateway::TPtr gateway)
        : State_(state)
        , IODiscoveryTransformer_(CreateClickHouseIODiscoveryTransformer(State_))
        , LoadMetaDataTransformer_(CreateClickHouseLoadTableMetadataTransformer(State_, std::move(gateway)))
        , TypeAnnotationTransformer_(CreateClickHouseDataSourceTypeAnnotationTransformer(State_))
        , DqIntegration_(CreateClickHouseDqIntegration(State_))
    {}

    TStringBuf GetName() const override {
        return ClickHouseProviderName;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSource::CallableName())) {
            if (node.Child(0)->Content() == ClickHouseProviderName) {
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
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid ClickHouse DataSource parameters"));
        return false;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(TCoRead::CallableName())) {
            return TClDataSource::Match(node.Child(1));
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

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        YQL_CLOG(INFO, ProviderClickHouse) << "RewriteIO";
        return node;
    }

    const THashMap<TString, TString>* GetClusterTokens() override {
        return &State_->Configuration->Tokens;
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(compact);

        for (auto& child : node.Children()) {
            children.push_back(child.Get());
        }

        if (TMaybeNode<TClReadTable>(&node)) {
            return true;
        }
        return false;
    }

    ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>& inputs, bool withLimits) override {
        Y_UNUSED(withLimits);
        if (auto maybeRead = TMaybeNode<TClReadTable>(&node)) {
            if (auto maybeTable = maybeRead.Table()) {
                TStringBuilder tableNameBuilder;
                if (auto dataSource = maybeRead.DataSource().Maybe<TClDataSource>()) {
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
    const TClickHouseState::TPtr State_;
    const THolder<IGraphTransformer> IODiscoveryTransformer_;
    const THolder<IGraphTransformer> LoadMetaDataTransformer_;
    const THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    const THolder<IDqIntegration> DqIntegration_;
};

}

TIntrusivePtr<IDataProvider> CreateClickHouseDataSource(TClickHouseState::TPtr state, IHTTPGateway::TPtr gateway) {
    return new TClickHouseDataSource(std::move(state), std::move(gateway));
}

} // namespace NYql
