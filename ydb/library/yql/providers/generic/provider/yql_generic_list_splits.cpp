#include "yql_generic_provider_impl.h"
#include "yql_generic_list_splits.h"

#include <library/cpp/json/json_reader.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/error.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <yql/essentials/utils/log/log.h>
#include <ydb/core/external_sources/iceberg_fields.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_predicate_pushdown.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/utils.h>

namespace NYql {

using namespace NNodes;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

TGenericListSplitTransformer::TGenericListSplitTransformer(TGenericState::TPtr state)
    : State_(std::move(state))
{ }

///
/// Fill columns in a select query
///
void FillColumns(NConnector::NApi::TSelect& select, const TGenReadTable& reader, NYql::NConnector::NApi::TSchema schema) {
    auto items = select.mutable_what()->mutable_items();
    auto columns = reader.Columns().Ptr();

    if (!columns->IsList()) {
        const auto rowType = reader.Ref()
            .GetTypeAnn()
                ->Cast<TTupleExprType>()
                ->GetItems()
            .back()
                ->Cast<TListExprType>()
                ->GetItemType();

        const auto& exp = rowType->Cast<TStructExprType>()->GetItems();

        for (auto item : exp) {
            auto column = items->Add()->mutable_column();
            column->mutable_name()->assign(item->GetName());
            auto type = NConnector::GetColumnTypeByName(schema, TString(item->GetName()));
            *column->mutable_type() = type;
        }

        return;
    }

    for (size_t i = 0; i < columns->ChildrenSize(); i++) {
        auto cc = columns->Child(i);
        Y_ENSURE(cc->IsAtom());

        auto column = items->Add()->mutable_column();
        auto columnName = TString(cc->Content());
        column->mutable_name()->assign(columnName);

        // assign column type
        auto type = NConnector::GetColumnTypeByName(schema, columnName);
        *column->mutable_type() = type;
    }
}

///
/// Fill where clause in a select query
///
void FillPredicate(NConnector::NApi::TSelect& select, const TGenReadTable& reader, TExprContext& ctx) {
    auto predicate = reader.FilterPredicate();

    if (IsEmptyFilterPredicate(predicate)) {
        return;
    }

    TStringBuilder err;

    if (!SerializeFilterPredicate(ctx, reader.FilterPredicate(), select.mutable_where()->mutable_filter_typed(), err)) {
        throw yexception() << "Failed to serialize filter predicate for source: " << err;
    }
}

///
/// Make an unique key for a select created for a table
///
TString MakeKeyFor(const TGenericState::TTableAddress& table, const NConnector::NApi::TSelect& select) {
    return TStringBuilder() << table.ClusterName << GetSelectKey(select);
}

IGraphTransformer::TStatus TGenericListSplitTransformer::DoTransform(TExprNode::TPtr input,
                                                                TExprNode::TPtr& output,
                                                                TExprContext& ctx) {
    output = input;

    const auto& reads = FindNodes(input, [&](const TExprNode::TPtr& node) {
        if (const auto maybeRead = TMaybeNode<TGenReadTable>(node)) {
            Y_ENSURE(maybeRead.Cast().DataSource().Category().Value() == GenericProviderName);
            return true;
        }
        
        return false;
    });

    if (reads.empty()) {
        return TStatus::Ok;
    }

    std::unordered_map<TString, TListSplitRequestData> pendingRequests;

    // Iterate over all read table queries in the input expression, create ListSplit request if needed
    for (const auto& r : reads) {
        const TGenReadTable read(r);
        const auto clusterName = read.DataSource().Cluster().StringValue();
        const auto tableName = read.Table().Name().StringValue();
        auto tableAddress = TGenericState::TTableAddress(clusterName, tableName);
        auto table = State_->GetTable(tableAddress);

        if (!table.first) {
            ctx.AddError(TIssue(table.second.ToString()));
            return TStatus::Error;
        }

        // Grab select from a read table query
        NConnector::NApi::TSelect select;

        *select.mutable_data_source_instance() = table.first->DataSourceInstance;
        select.mutable_from()->set_table(tableName);

        FillColumns(select, read, table.first->Schema);
        FillPredicate(select, read, ctx);

        // Splits has been already acquired for such select, skip it
        if (table.first->HasSplitsForSelect(select)) {
            continue;
        }

        auto k = MakeKeyFor(tableAddress, select);
        auto v = TListSplitRequestData{k, std::move(select), tableAddress};
        pendingRequests.emplace(k, v);
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
                    << "Call ListSplits for table: " << data.TableAddress.ToString() << " with select";

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

    const auto& reads = FindNodes(input, [&](const TExprNode::TPtr& node) {
        if (const auto maybeRead = TMaybeNode<TGenReadTable>(node)) {
            Y_ENSURE(maybeRead.Cast().DataSource().Category().Value() == GenericProviderName);
            return true;
        }

        return false;
    });

    if (reads.empty()) {
        return TStatus::Ok;
    }

    // Iterate over all read table queries in the input expression, check Connector responses
    for (const auto& r : reads) {
        const TGenReadTable genRead(r);
        const auto clusterName = genRead.DataSource().Cluster().StringValue();
        const auto tableName = genRead.Table().Name().StringValue();
        const TGenericState::TTableAddress tableAddress{clusterName, tableName};
        auto table = State_->GetTable(tableAddress);

        if (!table.first) {
            ctx.AddError(TIssue(table.second.ToString()));
            return TStatus::Error;
        }

        // Grab select from a read table query
        NConnector::NApi::TSelect select;

        *select.mutable_data_source_instance() = table.first->DataSourceInstance;
        select.mutable_from()->set_table(tableName);

        FillColumns(select, genRead, table.first->Schema);
        FillPredicate(select, genRead, ctx);

        // If splits for a similar select for this table was created skip it
        if (table.first->HasSplitsForSelect(select)) {
            continue;
        }

        // Find appropriate response
        auto iter = ListResponses_.find(MakeKeyFor(tableAddress, select));

        if (iter == ListResponses_.end()) {
            auto msg = TStringBuilder()
                << "Connector response not found for table: " << tableAddress.ToString()
                << " and select: " << select.DebugString();

            ctx.AddError(TIssue(ctx.GetPosition(genRead.Pos()), msg));
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

        Y_ENSURE(result->Splits.size() > 0);

        if (auto issue = State_->AttachSplitsToTable(tableAddress, select, result->Splits); issue) {
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

} // NYql
