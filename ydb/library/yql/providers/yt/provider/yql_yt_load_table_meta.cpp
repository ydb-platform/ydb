#include "yql_yt_provider_impl.h"
#include "yql_yt_gateway.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/lib/row_spec/yql_row_spec.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <library/cpp/threading/future/async.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/algorithm.h>
#include <util/string/join.h>

#include <utility>

namespace NYql {

namespace {

using namespace NNodes;

class TYtLoadTableMetadataTransformer : public TGraphTransformerBase {
private:
    struct TLoadContext: public TThrRefBase {
        using TPtr = TIntrusivePtr<TLoadContext>;

        ui32 Epoch = 0;
        IYtGateway::TTableInfoResult Result;
        TVector<std::pair<std::pair<TString, TString>, TYtTableDescription*>> Tables;
    };
public:
    TYtLoadTableMetadataTransformer(TYtState::TPtr state)
        : State_(state)
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (ctx.Step.IsDone(TExprStep::LoadTablesMetadata)) {
            return TStatus::Ok;
        }

        ui32 loadEpoch = 0;
        size_t settingsVer = 0;
        if (State_->LoadEpochMetadata.Defined()) {
            std::tie(loadEpoch, settingsVer) = *State_->LoadEpochMetadata;
            State_->LoadEpochMetadata.Clear();
        }

        LoadCtx = MakeIntrusive<TLoadContext>();
        LoadCtx->Epoch = loadEpoch;
        IYtGateway::TGetTableInfoOptions opts{State_->SessionId};

        if (0 != loadEpoch) {
            auto p = State_->EpochDependencies.FindPtr(loadEpoch);
            YQL_ENSURE(p && !p->empty());
            for (auto& clusterAndTable: *p) {
                TYtTableDescription& tableDesc = State_->TablesData->GetModifTable(clusterAndTable.first, clusterAndTable.second, loadEpoch);
                TString tableName = clusterAndTable.second;
                if (tableDesc.IsAnonymous) {
                    tableName = State_->AnonymousLabels.Value(std::make_pair(clusterAndTable.first, tableName), TString());
                    YQL_ENSURE(tableName, "Unaccounted anonymous table: " << clusterAndTable.first << '.' << clusterAndTable.second);
                }
                opts.Tables().push_back(IYtGateway::TTableReq()
                    .Cluster(clusterAndTable.first)
                    .Table(tableName)
                    .Intents(tableDesc.Intents)
                    .InferSchemaRows(0)
                    .ForceInferSchema(false)
                    .Anonymous(tableDesc.IsAnonymous)
                    .IgnoreYamrDsv(State_->Configuration->IgnoreYamrDsv.Get().GetOrElse(false))
                    .IgnoreWeakSchema(State_->Configuration->IgnoreWeakSchema.Get().GetOrElse(false))
                );
                LoadCtx->Tables.emplace_back(clusterAndTable, &tableDesc);
            }
        } else {
            for (auto& clusterAndTable: State_->TablesData->GetAllZeroEpochTables()) {
                TYtTableDescription& tableDesc = State_->TablesData->GetModifTable(clusterAndTable.first, clusterAndTable.second, loadEpoch);
                if (tableDesc.Meta) {
                    if (!tableDesc.HasWriteLock && HasModifyIntents(tableDesc.Intents)) {
                        TStringBuilder msg;
                        msg << "Table " << clusterAndTable.second.Quote()
                            << " is locked exclusively after taking snapshot lock."
                            << " This may lead query failure if YQL detected concurrent table modification before taking exclusive lock."
                            << " Add WITH XLOCK hint to all reads from this table to resolve this warning";
                        if (!ctx.AddWarning(YqlIssue(TPosition(), EYqlIssueCode::TIssuesIds_EIssueCode_YT_LATE_TABLE_XLOCK, msg))) {
                            return TStatus::Error;
                        }
                    }

                    // skip if table already has loaded metadata and has only read intents
                    if (State_->Types->IsReadOnly || State_->Types->UseTableMetaFromGraph || tableDesc.HasWriteLock || !HasModifyIntents(tableDesc.Intents)) {
                        // Intents/views can be updated since evaluation phase
                        if (!tableDesc.FillViews(
                            clusterAndTable.first, clusterAndTable.second, ctx,
                            State_->Types->Modules.get(), State_->Types->UrlListerManager.Get(), *State_->Types->RandomProvider,
                            State_->Configuration->ViewIsolation.Get().GetOrElse(false),
                            State_->Types->UdfResolver
                        )) {
                            return TStatus::Error;
                        }
                        continue;
                    }
                }
                TString tableName = clusterAndTable.second;
                if (tableDesc.IsAnonymous) {
                    tableName = State_->AnonymousLabels.Value(std::make_pair(clusterAndTable.first, tableName), TString());
                    YQL_ENSURE(tableName, "Unaccounted anonymous table: " << clusterAndTable.first << '.' << clusterAndTable.second);
                }
                opts.Tables().push_back(IYtGateway::TTableReq()
                    .Cluster(clusterAndTable.first)
                    .Table(tableName)
                    .LockOnly(bool(tableDesc.Meta))
                    .Intents(tableDesc.Intents)
                    .InferSchemaRows(tableDesc.InferSchemaRows)
                    .ForceInferSchema(tableDesc.ForceInferSchema)
                    .Anonymous(tableDesc.IsAnonymous)
                    .IgnoreYamrDsv(State_->Configuration->IgnoreYamrDsv.Get().GetOrElse(false))
                    .IgnoreWeakSchema(State_->Configuration->IgnoreWeakSchema.Get().GetOrElse(false))
                );
                LoadCtx->Tables.emplace_back(clusterAndTable, &tableDesc);
            }
        }

        if (opts.Tables().empty()) {
            return TStatus::Ok;
        }

        auto config = loadEpoch ? State_->Configuration->GetSettingsVer(settingsVer) : State_->Configuration->Snapshot();
        opts.Config(config).ReadOnly(State_->Types->IsReadOnly).Epoch(loadEpoch);

        auto future = State_->Gateway->GetTableInfo(std::move(opts));
        auto loadCtx = LoadCtx;

        AsyncFuture = future.Apply(
            [loadCtx](const NThreading::TFuture<IYtGateway::TTableInfoResult>& completedFuture) {
                loadCtx->Result = completedFuture.GetValue();
                YQL_ENSURE(!loadCtx->Result.Success() || loadCtx->Result.Data.size() == loadCtx->Tables.size());
            });

        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        return AsyncFuture;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        // Raise errors if any
        AsyncFuture.GetValue();

        LoadCtx->Result.ReportIssues(ctx.IssueManager);
        if (!LoadCtx->Result.Success()) {
            LoadCtx.Reset();
            return TStatus::Error;
        }

        ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
        THashMap<std::pair<TString, TString>, TYtTableDescription*> tableDescrs;
        for (size_t i = 0 ; i < LoadCtx->Result.Data.size(); ++i) {
            tableDescrs.emplace(LoadCtx->Tables[i]);

            TString cluster = LoadCtx->Tables[i].first.first;
            TString tableName = LoadCtx->Tables[i].first.second;
            TYtTableDescription& tableDesc = *LoadCtx->Tables[i].second;

            if (LoadCtx->Result.Data[i].WriteLock) {
                tableDesc.HasWriteLock = true;
            }

            TIssueScopeGuard issueScope(ctx.IssueManager, [tableName]() {
                return MakeIntrusive<TIssue>(TStringBuilder() << "Table " << tableName);
            });

            if (auto meta = LoadCtx->Result.Data[i].Meta) {
                tableDesc.Meta = meta;
                const auto schemaAttrs = std::initializer_list<TStringBuf>{YqlRowSpecAttribute, SCHEMA_ATTR_NAME, READ_SCHEMA_ATTR_NAME, INFER_SCHEMA_ATTR_NAME};
                if (AnyOf(schemaAttrs, [&tableDesc](TStringBuf attr) { return tableDesc.Meta->Attrs.contains(attr); })) {
                    auto rowSpec = MakeIntrusive<TYqlRowSpecInfo>();
                    if (!rowSpec->Parse(tableDesc.Meta->Attrs, ctx)) {
                        return TStatus::Error;
                    }
                    if (!State_->Configuration->UseNativeDescSort.Get().GetOrElse(false) && rowSpec->ClearNativeDescendingSort()) {
                        if (!ctx.AddWarning(YqlIssue(TPosition(), EYqlIssueCode::TIssuesIds_EIssueCode_YT_NATIVE_DESC_SORT_IGNORED, "Native descending sort is ignored"))) {
                            return TStatus::Error;
                        }
                    }
                    // Some sanity checks
                    if (tableDesc.RowSpec && tableDesc.RowSpec->IsSorted()) {
                        YQL_ENSURE(rowSpec->IsSorted(), "Bad predicted sort for the table '"
                            << cluster << "." << tableName << "' (epoch=" << LoadCtx->Epoch << "), expected sorted, but actually not.");
                        YQL_ENSURE(rowSpec->SortedBy.size() >= tableDesc.RowSpec->SortedBy.size()
                            && Equal(tableDesc.RowSpec->SortedBy.begin(), tableDesc.RowSpec->SortedBy.end(), rowSpec->SortedBy.begin()),
                            "Bad predicted SortedBy for the table '"
                            << cluster << "." << tableName << "' (epoch=" << LoadCtx->Epoch << "), expected: [" << JoinSeq(",", tableDesc.RowSpec->SortedBy)
                            << "], actual: [" << JoinSeq(",", rowSpec->SortedBy) << ']');
                        YQL_ENSURE(rowSpec->SortMembers.size() >= tableDesc.RowSpec->SortMembers.size()
                            && Equal(tableDesc.RowSpec->SortMembers.begin(), tableDesc.RowSpec->SortMembers.end(), rowSpec->SortMembers.begin()),
                            "Bad predicted SortMembers for the table '"
                            << cluster << "." << tableName << "' (epoch=" << LoadCtx->Epoch << "), expected: [" << JoinSeq(",", tableDesc.RowSpec->SortMembers)
                            << "], actual: [" << JoinSeq(",", rowSpec->SortMembers) << ']');
                    }
                    tableDesc.RowSpec = rowSpec;
                    ForEach(std::begin(schemaAttrs), std::end(schemaAttrs), [&tableDesc](TStringBuf attr) { tableDesc.Meta->Attrs.erase(attr); });

                    if (LoadCtx->Epoch != 0 && tableDesc.RawRowType) {
                        // Some sanity checks
                        YQL_ENSURE(IsSameAnnotation(*tableDesc.RawRowType, *tableDesc.RowSpec->GetType()), "Scheme diff: " << GetTypeDiff(*tableDesc.RawRowType, *tableDesc.RowSpec->GetType()));
                    }
                }
                if (auto rowSpecAttr = tableDesc.Meta->Attrs.FindPtr(TString{YqlRowSpecAttribute}.append("_qb2"))) {
                    auto rowSpec = MakeIntrusive<TYqlRowSpecInfo>();
                    if (!rowSpec->Parse(*rowSpecAttr, ctx)) {
                        return TStatus::Error;
                    }
                    tableDesc.QB2RowSpec = rowSpec;
                    tableDesc.Meta->Attrs.erase(TString{YqlRowSpecAttribute}.append("_qb2"));
                }

                if (0 == LoadCtx->Epoch) {
                    if (!tableDesc.Fill(
                        cluster, tableName, ctx,
                        State_->Types->Modules.get(), State_->Types->UrlListerManager.Get(), *State_->Types->RandomProvider,
                        State_->Configuration->ViewIsolation.Get().GetOrElse(false),
                        State_->Types->UdfResolver
                    )) {
                        return TStatus::Error;
                    }
                }
            }

            if (auto stat = LoadCtx->Result.Data[i].Stat) {
                tableDesc.Stat = stat;
                if (HasReadIntents(tableDesc.Intents)) {
                    const auto& securityTagsSet = tableDesc.Stat->SecurityTags;
                    const TString tmpFolder = GetTablesTmpFolder(*State_->Configuration);
                    if (!securityTagsSet.empty() && tmpFolder.empty()) {
                        TStringBuilder msg;
                        msg << "Table " << cluster << "." << tableName
                            << " contains sensitive data, but is used with the default tmp folder."
                            << " This may lead to sensitive data being leaked, consider using a protected tmp folder with the TmpFolder pragma.";
                        auto issue = YqlIssue(TPosition(), EYqlIssueCode::TIssuesIds_EIssueCode_YT_SECURE_DATA_IN_COMMON_TMP, msg);
                        if (State_->Configuration->ForceTmpSecurity.Get().GetOrElse(false)) {
                            ctx.AddError(issue);
                            return TStatus::Error;
                        } else {
                            if (!ctx.AddWarning(issue)) {
                                return TStatus::Error;
                            }
                        }
                    }
                }
            }
        }

        TStatus status = TStatus::Ok;
        if (LoadCtx->Epoch) {
            TNodeMap<TYtTableDescription*> tableNodes;
            VisitExpr(input, [&](const TExprNode::TPtr& node) {
                TString cluster;
                if (auto maybeTable = TMaybeNode<TYtTable>(node)) {
                    TYtTable table = maybeTable.Cast();
                    if (auto p = tableDescrs.FindPtr(std::make_pair(TString{table.Cluster().Value()}, TString{table.Name().Value()}))) {
                        if (TEpochInfo::Parse(table.Epoch().Ref()).GetOrElse(0) == LoadCtx->Epoch) {
                            tableNodes.emplace(node.Get(), *p);
                        }
                    }
                    return false;
                }
                return true;
            });

            TOptimizeExprSettings settings(nullptr);
            settings.VisitChanges = true;
            settings.VisitStarted = true;
            status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                auto it = tableNodes.find(node.Get());
                if (it != tableNodes.cend()) {
                    TYtTableInfo table = node;

                    const TYtTableDescription& tableDesc = *it->second;
                    table.Stat = tableDesc.Stat;
                    table.Meta = tableDesc.Meta;
                    table.RowSpec = tableDesc.RowSpec;

                    return table.ToExprNode(ctx, node->Pos()).Ptr();
                }
                return node;
            }, ctx, settings);
            if (input != output) {
                status = status.Combine(TStatus(TStatus::Repeat, true));
            }
        }

        LoadCtx.Reset();
        return status;
    }

    void Rewind() final {
        LoadCtx.Reset();
        AsyncFuture = {};
    }

private:
    TYtState::TPtr State_;
    TLoadContext::TPtr LoadCtx;
    NThreading::TFuture<void> AsyncFuture;
};

} // namespace

THolder<IGraphTransformer> CreateYtLoadTableMetadataTransformer(TYtState::TPtr state) {
    return THolder(new TYtLoadTableMetadataTransformer(state));
}

} // namespace NYql
