#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include "yql_generic_state.h"

namespace NYql {

///
/// Optimization process contains several transformers in a pipeline which are called
/// consequently. If a DoTransform returns repeat, e.g. it changes expression, the process
/// is starting again from the very begining, but with a new input. That is why transformer
/// can be called multiple times. The purpose of this transformer is to find TGenSourceSettings
/// nodes which were created during previous transformations and perform ListSplit request.
/// ListSplit transformer has to work after where clause has been pushed; otherwise
/// in a ReadSplit request, which ultimately occurs after pushdown, connector could receive
/// where clause that is differ from ListSplit's.
///
/// The order of transformations calls:
///
/// 1. TGenericPhysicalOptProposalTransformer::PushFilterToReadTable pushdowns predicate into
///    a TGenReadTable.
///
/// 2. BuildKqlQuery creates TGenSourceSettings.
///
/// 3. TKqpConstantFoldingTransformer folds const expression in a pushdown predicate.
///
/// 4. TGenericListSplitTransformer performs a ListSplit request.
///
class TGenericListSplitTransformer : public TGraphTransformerBase {
    struct TListSplitRequestData {
        TString Key;
        NConnector::NApi::TSelect Select;
        TGenericState::TTableAddress TableAddress;
    };

    struct TListResponse {
        using TPtr = std::shared_ptr<TListResponse>;

        std::vector<NConnector::NApi::TSplit> Splits;
        TIssues Issues; 
    };

    using TListResponseMap =
        std::unordered_map<TString, TListResponse::TPtr>;

public:
    explicit TGenericListSplitTransformer(TGenericState::TPtr state);

public:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) final {
        return AsyncFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

    void Rewind() final;

private:
    TIssues ListSplitsFromConnector(const TListSplitRequestData& data, std::vector<NThreading::TFuture<void>>& handles);

private: 
    const TGenericState::TPtr State_;
    TListResponseMap ListResponses_;
    NThreading::TFuture<void> AsyncFuture_;
};

} // NYql
