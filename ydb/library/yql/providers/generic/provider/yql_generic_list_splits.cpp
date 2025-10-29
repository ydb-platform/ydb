#include "yql_generic_list_splits.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/ast/yql_expr.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_utils.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_predicate_pushdown.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/error.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>
#include <ydb/core/external_sources/iceberg_fields.h>
#include <library/cpp/json/json_reader.h>

namespace NYql {

using namespace NNodes;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

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
///    a TGenReadTable node.
///
/// 2. TKqpConstantFoldingTransformer folds const expression in a pushdown predicate.
///
/// 3. TGenericListSplitTransformer performs a ListSplit request on a TGenSourceSettings node.
///
class TGenericListSplitTransformer : public TGraphTransformerBase {
    struct TListSplitRequestData {
        TSelectKey Key;
        NConnector::NApi::TSelect Select;
        TGenericState::TTableAddress TableAddress;
    };

    struct TListResponse {
        using TPtr = std::shared_ptr<TListResponse>;

        std::vector<NConnector::NApi::TSplit> Splits;
        TIssues Issues;
    };

    using TListResponseMap =
        std::unordered_map<TSelectKey, TListResponse::TPtr, TSelectKeyHash>;

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

TGenericListSplitTransformer::TGenericListSplitTransformer(TGenericState::TPtr state)
    : State_(std::move(state))
{ }

IGraphTransformer::TStatus TGenericListSplitTransformer::DoTransform(TExprNode::TPtr input,
                                                                     TExprNode::TPtr& output,
                                                                     TExprContext& ctx) {
    output = input;

    const auto& readsSettings = FindNodes(input, [&](const TExprNode::TPtr& node) {
        if (const auto maybeSettings = TMaybeNode<TGenSourceSettings>(node)) {
            return true;
        }
        
        return false;
    });

    if (readsSettings.empty()) {
        return TStatus::Ok;
    }

    std::unordered_map<TSelectKey, TListSplitRequestData, TSelectKeyHash> pendingRequests;

    // Iterate over all settings in the input expression, create ListSplit request if needed
    for (const auto& r : readsSettings) {
        const TGenSourceSettings settings(r);

        const auto& tableName = settings.Table().StringValue();
        const auto& clusterName = settings.Cluster().StringValue();

        auto tableAddress = TGenericState::TTableAddress(clusterName, tableName);
        auto table = State_->GetTable(tableAddress);

        if (!table.first) {
            ctx.AddError(TIssue(table.second.ToString()));
            return TStatus::Error;
        }

        // Grab select from a read table query
        NConnector::NApi::TSelect select;
        FillSelectFromGenSourceSettings(select, settings, ctx, table.first);

        auto selectKey = tableAddress.MakeKeyFor(select);

        // The one sql query could contain multiple selects, e.g.
        // join, subquery etc. If splits has been already acquired for a
        // select, skip it
        if (table.first->HasSplitsForSelect(selectKey)) {
            continue;
        }

        auto v = TListSplitRequestData{selectKey, std::move(select), tableAddress};
        pendingRequests.emplace(selectKey, v);
    }

    if (pendingRequests.empty()) {
        return TStatus::Ok;
    }

    std::vector<NThreading::TFuture<void>> handles;
    handles.reserve(pendingRequests.size());
    ListResponses_.reserve(pendingRequests.size());

    for (auto& k : pendingRequests) {
        auto tIssues = ListSplitsFromConnector(k.second, handles);
        if (!tIssues.Empty()) {
            ctx.AddError(TIssue(tIssues.ToString()));
            return TStatus::Error;
        }
    }

    if (handles.empty()) {
        return TStatus::Ok;
    }

    AsyncFuture_ = NThreading::WaitExceptionOrAll(handles);
    return TStatus::Async;
}

TIssues TGenericListSplitTransformer::ListSplitsFromConnector(const TListSplitRequestData& data,
                                                              std::vector<NThreading::TFuture<void>>& handles) {
    auto table = State_->GetTable(data.TableAddress);

    if (!table.first) {
        return table.second;
    }

    // Preserve data source instance for the further usage
    auto emplaceIt = ListResponses_.emplace(data.Key, std::make_shared<TListResponse>());
    auto desc = emplaceIt.first->second;

    // Call ListSplits
    NConnector::NApi::TListSplitsRequest request;
    *request.mutable_selects()->Add() = data.Select;

    auto promise = NThreading::NewPromise();
    handles.emplace_back(promise.GetFuture());

    Y_ENSURE(State_->GenericClient);

    State_->GenericClient->ListSplits(request).Subscribe([desc, promise, data]
        (const NConnector::TListSplitsStreamIteratorAsyncResult f3) mutable {
        NConnector::TListSplitsStreamIteratorAsyncResult f4(f3);
        auto streamIterResult = f4.ExtractValueSync();

        // Check transport error
        if (!streamIterResult.Status.Ok()) {
            desc->Issues.AddIssue(TStringBuilder()
                << "Call ListSplits for table: " << data.TableAddress.ToString()
                << " with select: " << streamIterResult.Status.ToDebugString());
            promise.SetValue();
            return;
        }

        Y_ENSURE(streamIterResult.Iterator);

        auto drainer =
            NConnector::MakeListSplitsStreamIteratorDrainer(std::move(streamIterResult.Iterator));

        // Pass drainer to the callback because we want him to stay alive until the callback is called
        drainer->Run().Subscribe([desc, promise, data, drainer]
            (const NThreading::TFuture<NConnector::TListSplitsStreamIteratorDrainer::TBuffer>& f5) mutable {
            NThreading::TFuture<NConnector::TListSplitsStreamIteratorDrainer::TBuffer> f6(f5);
            auto drainerResult = f6.ExtractValueSync();

            // check transport and logical errors
            if (drainerResult.Issues) {
                auto msg = TStringBuilder()
                    << "Call ListSplits for table: " << data.TableAddress.ToString() << " with select: "
                    << data.Select.what().DebugString()
                    << data.Select.from().DebugString()
                    << data.Select.where().DebugString();

                TIssue dstIssue(msg);

                for (const auto& srcIssue : drainerResult.Issues) {
                    dstIssue.AddSubIssue(MakeIntrusive<TIssue>(srcIssue));
                }

                desc->Issues.AddIssue(std::move(dstIssue));
                promise.SetValue();
                return;
            }

            // Collect all the splits from every response into a single vector
            for (auto&& response : drainerResult.Responses) {
                std::transform(std::make_move_iterator(response.mutable_splits()->begin()),
                               std::make_move_iterator(response.mutable_splits()->end()),
                               std::back_inserter(desc->Splits),
                               [](auto&& split) { return std::move(split); });
            }

            promise.SetValue();
        });
    });

    return TIssues();
}

IGraphTransformer::TStatus TGenericListSplitTransformer::DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    AsyncFuture_.GetValue();
    output = input;

    /* 
     * Search for a TGenSourceSettings. TGenSourceSettings contains the same data as during
     * DoTransform method call
     */
    const auto& readsSettings = FindNodes(input, [&](const TExprNode::TPtr& node) {
        if (const auto maybeSettings = TMaybeNode<TGenSourceSettings>(node)) {
            return true;
        }

        return false;
    });

    if (readsSettings.empty()) {
        return TStatus::Ok;
    }

    // Iterate over all settings in the input expression, check Connector responses
    for (const auto& r : readsSettings) {
        const TGenSourceSettings settings(r);

        const auto& tableName = settings.Table().StringValue();
        const auto& clusterName = settings.Cluster().StringValue();

        auto tableAddress = TGenericState::TTableAddress(clusterName, tableName);
        auto table = State_->GetTable(tableAddress);

        if (!table.first) {
            ctx.AddError(TIssue(table.second.ToString()));
            return TStatus::Error;
        }

        // Grab select from a read table query
        NConnector::NApi::TSelect select;
        FillSelectFromGenSourceSettings(select, settings, ctx, table.first);
        auto selectKey = tableAddress.MakeKeyFor(select);

        // If splits for a similar select for this table was created skip it
        if (table.first->HasSplitsForSelect(selectKey)) {
            continue;
        }

        // Find appropriate response
        auto iter = ListResponses_.find(selectKey);

        if (iter == ListResponses_.end()) {
            auto msg = TStringBuilder()
                << "Connector response not found for table: " << tableAddress.ToString() << " and select: "
                << select.what().DebugString()
                << select.from().DebugString()
                << select.where().DebugString();

            ctx.AddError(TIssue(ctx.GetPosition(settings.Pos()), msg));
            return TStatus::Error;
        }

        auto& result = iter->second;

        // If errors occurred during network interaction with Connector, return them
        if (result->Issues) {
            for (const auto& issue : result->Issues) {
                ctx.AddError(issue);
            }

            return TStatus::Error;
        }

        Y_ENSURE(!result->Splits.empty());

        if (auto issue = State_->AttachSplitsToTable(tableAddress, selectKey, result->Splits); issue) {
            ctx.AddError(*issue);
            return TStatus::Error;
        }
    }

    return TStatus::Ok;
}

void TGenericListSplitTransformer::Rewind() {
    ListResponses_.clear();
    AsyncFuture_ = {};
}

THolder<TGraphTransformerBase> CreateGenericListSplitTransformer(TGenericState::TPtr state) {
    return MakeHolder<TGenericListSplitTransformer>(std::move(state));
}

} // NYql
