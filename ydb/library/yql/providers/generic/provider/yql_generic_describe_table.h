#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include "yql_generic_state.h"

namespace NYql {

class TGenericDescribeTableTransformer : public TGraphTransformerBase {
    struct TTableDescription {
        using TPtr = std::shared_ptr<TTableDescription>;

        NYql::TGenericDataSourceInstance DataSourceInstance;
        std::optional<NConnector::NApi::TSchema> Schema;
        // Issues that could occur at any phase of network interaction with Connector
        TIssues Issues; 
    };

    using TTableDescriptionMap =
        std::unordered_map<TGenericState::TTableAddress, TTableDescription::TPtr, THash<TGenericState::TTableAddress>>;

public:
    explicit TGenericDescribeTableTransformer(TGenericState::TPtr state);

public:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) final {
        return AsyncFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

    void Rewind() final;

private:
    TIssues DescribeTableFromConnector(const TGenericState::TTableAddress& tableAddress,
                                       std::vector<NThreading::TFuture<void>>& handles);

    TIssues FillDescribeTableRequest(NConnector::NApi::TDescribeTableRequest& request,
                                    const TGenericClusterConfig& clusterConfig, const TString& tablePath);

    void FillCredentials(NConnector::NApi::TDescribeTableRequest& request,
                        const TGenericClusterConfig& clusterConfig);

    void FillTypeMappingSettings(NConnector::NApi::TDescribeTableRequest& request);

private:
    const TGenericState::TPtr State_;
    TTableDescriptionMap TableDescriptions_;
    NThreading::TFuture<void> AsyncFuture_;
};

} // namespace NYql
