#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include "yql_generic_state.h"

namespace NYql {

class TGenericListTransformer : public TGraphTransformerBase {
    struct TListResponse {
        using TPtr = std::shared_ptr<TListResponse>;

        std::vector<NConnector::NApi::TSplit> Splits;
        TIssues Issues; 
    };

    using TListResponseMap =
        std::unordered_map<TGenericState::TTableAddress, TListResponse::TPtr, THash<TGenericState::TTableAddress>>;

public:
    explicit TGenericListTransformer(TGenericState::TPtr state);

public:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) final {
        return AsyncFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

    void Rewind() final;

private:
    TIssues ListTableFromConnector(const TGenericState::TTableAddress& tableAddress,
                                   NConnector::NApi::TSelect select,
                                   std::vector<NThreading::TFuture<void>>& handles);

private: 
    const TGenericState::TPtr State_;
    TListResponseMap ListResponses_;
    NThreading::TFuture<void> AsyncFuture_;
};

} // NYql
