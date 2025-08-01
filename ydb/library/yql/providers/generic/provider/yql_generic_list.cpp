#include "yql_generic_provider_impl.h"
#include "yql_generic_list.h"

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

TGenericListTransformer::TGenericListTransformer(TGenericState::TPtr state) 
    : State_(std::move(state))
{ }

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

        for(auto item : exp) {
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

IGraphTransformer::TStatus TGenericListTransformer::DoTransform(TExprNode::TPtr input,
                                                                TExprNode::TPtr& output,
                                                                TExprContext& ctx) {
    output = input;

    std::unordered_map<TListResponseMap::key_type, NConnector::NApi::TSelect, TListResponseMap::hasher> pendingTables;

    const auto& reads = FindNodes(input, [&](const TExprNode::TPtr& node) {
        if (const auto maybeRead = TMaybeNode<TGenReadTable>(node)) {
            return maybeRead.Cast().DataSource().Category().Value() == GenericProviderName;
        }
        return false;
    });

    if (!reads.empty()) {
        for (const auto& r : reads) {
            const TGenReadTable read(r);
            const auto clusterName = read.DataSource().Cluster().StringValue();
            const auto tableName = read.Table().Name().StringValue();
            auto tableAddress = TGenericState::TTableAddress(clusterName, tableName);
            auto table = State_->GetTable(tableAddress);

            if (!table.first) {
                ctx.AddError(TIssue(table.second.ToString()));
                return TStatus::Error;;
            }

            NConnector::NApi::TSelect select;

            *select.mutable_data_source_instance() = table.first->DataSourceInstance;
            select.mutable_from()->set_table(tableName);

            FillColumns(select, read, table.first->Schema);

            auto predicate = read.FilterPredicate();

            if (!IsEmptyFilterPredicate(predicate)) {
                TStringBuilder err;

                if (!SerializeFilterPredicate(ctx, read.FilterPredicate(), select.mutable_where()->mutable_filter_typed(), err)) {
                    throw yexception() << "Failed to serialize filter predicate for source: " << err;
                }
            }

            pendingTables.emplace(tableAddress, std::move(select));
        }
    }

    std::vector<NThreading::TFuture<void>> handles;
    handles.reserve(pendingTables.size());
    ListResponses_.reserve(pendingTables.size());

    for (const auto& k : pendingTables) {
        auto tIssues = ListTableFromConnector(k.first, std::move(k.second), handles);
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

TIssues TGenericListTransformer::ListTableFromConnector(const TGenericState::TTableAddress& tableAddress,
                                                        NConnector::NApi::TSelect select,
                                                        std::vector<NThreading::TFuture<void>>& handles) {
    auto table = State_->GetTable(tableAddress);

    if (!table.first) {
        return table.second;
    }

    // preserve data source instance for the further usage
    auto emplaceIt = ListResponses_.emplace(tableAddress, std::make_shared<TListResponse>());
    auto desc = emplaceIt.first->second;

    // Call ListSplits
    NConnector::NApi::TListSplitsRequest request;
    *request.mutable_selects()->Add() = select;

    auto promise = NThreading::NewPromise();
    handles.emplace_back(promise.GetFuture());

    Y_ENSURE(State_->GenericClient);

    State_->GenericClient->ListSplits(request).Subscribe([desc, promise, tableAddress]
        (const NConnector::TListSplitsStreamIteratorAsyncResult f3) mutable {
        NConnector::TListSplitsStreamIteratorAsyncResult f4(f3);
        auto streamIterResult = f4.ExtractValueSync();

        // Check transport error
        if (!streamIterResult.Status.Ok()) {
            desc->Issues.AddIssue(TStringBuilder()
                << "Call ListSplits for table " << tableAddress.ToString() << ": "
                << streamIterResult.Status.ToDebugString());
            promise.SetValue();
            return;
        }

        Y_ENSURE(streamIterResult.Iterator);

        auto drainer =
            NConnector::MakeListSplitsStreamIteratorDrainer(std::move(streamIterResult.Iterator));

        // pass drainer to the callback because we want him to
        // stay alive until the callback is called    
        drainer->Run().Subscribe([desc, promise, tableAddress, drainer]
            (const NThreading::TFuture<NConnector::TListSplitsStreamIteratorDrainer::TBuffer>& f5) mutable {
            NThreading::TFuture<NConnector::TListSplitsStreamIteratorDrainer::TBuffer> f6(f5);
            auto drainerResult = f6.ExtractValueSync();

            // check transport and logical errors
            if (drainerResult.Issues) {
                TIssue dstIssue(TStringBuilder() << "Call ListSplits for table " << tableAddress.ToString());

                for (const auto& srcIssue : drainerResult.Issues) {
                    dstIssue.AddSubIssue(MakeIntrusive<TIssue>(srcIssue));
                };

                desc->Issues.AddIssue(std::move(dstIssue));
                promise.SetValue();
                return;
            }

            // collect all the splits from every response into a single vector
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

IGraphTransformer::TStatus TGenericListTransformer::DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    AsyncFuture_.GetValue();
    output = input;

    const auto& reads = FindNodes(input, [&](const TExprNode::TPtr& node) {
        if (const auto maybeRead = TMaybeNode<TGenReadTable>(node)) {
            return maybeRead.Cast().DataSource().Category().Value() == GenericProviderName;
        }
        return false;
    });

    // Iterate over all the requested tables, check Connector responses
    for (const auto& r : reads) {
        const TGenReadTable genRead(r);
        const auto clusterName = genRead.DataSource().Cluster().StringValue();
        const auto tableName = genRead.Table().Name().StringValue();
        const TGenericState::TTableAddress tableAddress{clusterName, tableName};

        // Find appropriate response
        auto iter = ListResponses_.find(tableAddress);

        if (iter == ListResponses_.end()) {
            ctx.AddError(TIssue(ctx.GetPosition(genRead.Pos()), TStringBuilder()
                                                                    << "Connector response not found for table "
                                                                    << tableAddress.ToString()));

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

        if (!State_->AttachSplitsToTable(tableAddress, result->Splits)) {
            ctx.AddError(TIssue("Failed to attach splits to a table metadata"));
            return TStatus::Error;
        }

    }

    return TStatus::Ok;
}

// clang-format off

void TGenericListTransformer::Rewind() {
    ListResponses_.clear();
    AsyncFuture_ = {};
}

} // NYql
