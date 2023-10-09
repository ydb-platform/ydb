#include "yql_ydb_provider_impl.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <ydb/public/lib/experimental/ydb_clickhouse_internal.h>

namespace NYql {

using namespace NNodes;
using namespace NYdb::NClickhouseInternal;

namespace {

const TTypeAnnotationNode* MakeYqlType(NYdb::TTypeParser&& type, TExprContext& ctx) {
    switch (type.GetKind()) {
        case NYdb::TTypeParser::ETypeKind::Optional: {
            type.OpenOptional();
            return ctx.MakeType<TOptionalExprType>(MakeYqlType(std::move(type), ctx));
        }
        case NYdb::TTypeParser::ETypeKind::Decimal: {
            const auto decimal = type.GetDecimal();
            return ctx.MakeType<TDataExprParamsType>(NUdf::GetDataSlot(static_cast<NUdf::TDataTypeId>(type.GetPrimitive())), ToString(decimal.Precision), ToString(decimal.Scale));
        }
        case NYdb::TTypeParser::ETypeKind::Primitive:
            if (const auto typeId = static_cast<NUdf::TDataTypeId>(type.GetPrimitive()); NUdf::TDataType<NUdf::TDecimal>::Id == typeId)
                return ctx.MakeType<TDataExprParamsType>(NUdf::EDataSlot::Decimal, "22", "9");
            else
                return ctx.MakeType<TDataExprType>(NUdf::GetDataSlot(static_cast<NUdf::TDataTypeId>(type.GetPrimitive())));
        default:
            return nullptr;
    }
}

NUdf::TDataTypeId GetYqlTypeId(NYdb::TTypeParser&& type, TExprContext& ctx) {
    switch (type.GetKind()) {
        case NYdb::TTypeParser::ETypeKind::Optional: {
            type.OpenOptional();
            return GetYqlTypeId(std::move(type), ctx);
        }
        case NYdb::TTypeParser::ETypeKind::Decimal:
        case NYdb::TTypeParser::ETypeKind::Primitive:
            return static_cast<NUdf::TDataTypeId>(type.GetPrimitive());
        default:
            return 0;
    }
}

class TYdbLoadTableMetadataTransformer : public TGraphTransformerBase {

using TCluster2ClientPerSnapshotHandle = std::unordered_map<std::string_view, std::pair<TMetaClient, std::optional<TCreateSnapshotHandleResult>>>;
using TTableKey2DescribeTableResult = std::unordered_map<std::pair<TString, TString>, std::optional<TDescribeTableResult>, THash<std::pair<TString, TString>>>;

public:
    TYdbLoadTableMetadataTransformer(TYdbState::TPtr state, NYdb::TDriver driver)
        : Driver_(std::move(driver)), State_(std::move(state))
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (ctx.Step.IsDone(TExprStep::LoadTablesMetadata)) {
            return TStatus::Ok;
        }

        Y_ABORT_UNLESS(Clients_, "Clients_ was destroyed");
        Y_ABORT_UNLESS(PendingTables_, "PendingTables_ was destroyed");

        std::unordered_map<TString, std::unordered_set<TString>> tablesFromCluster;

        TOptimizeExprSettings settings{ nullptr };
        settings.VisitChanges = true;
        const auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            const TExprBase nodeExpr(node);
            if (const auto left = nodeExpr.Maybe<TCoLeft>()) {
                if (const auto read = left.Input().Maybe<TYdbReadTable>()) {
                    if (const auto world = read.World().Cast().Ptr()) {
                        return world;
                    }
                }
            }

            if (!nodeExpr.Maybe<TYdbRead>()) {
                return node;
            }

            const TYdbRead read(node);
            if (read.DataSource().Category().Value() != YdbProviderName) {
                return node;
            }

            TIssueScopeGuard issueScopeRead(ctx.IssueManager, [&]() {
                return MakeIntrusive<TIssue>(ctx.GetPosition(read.Pos()), TStringBuilder() << "At function: " << TCoRead::CallableName());
            });

            TYdbKey key;
            if (!key.Extract(read.FreeArgs().Get(2).Ref(), ctx))
                return nullptr;

            const auto& cluster = read.DataSource().Cluster().StringValue();
            const auto& database = State_->Configuration->Clusters[cluster].Database;
            auto table = TString(key.GetTablePath());
            if (table.front() != '/')
                table = database + '/' + table;
            const auto& tableKey = std::make_pair(cluster, table);
            if (State_->Tables.cend() == State_->Tables.find(tableKey)) {
                PendingTables_->emplace(tableKey, std::nullopt);
                tablesFromCluster[tableKey.first].emplace(tableKey.second);
            }

            switch (key.GetKeyType()) {
            case TYdbKey::Type::Table:
                return Build<TYdbReadTable>(ctx, read.Pos())
                        .World(read.World())
                        .DataSource(read.DataSource())
                        .Table().Value(table).Build()
                        .Columns<TCoVoid>().Build()
                    .Done().Ptr();
            case TYdbKey::Type::TableScheme:
                return Build<TYdbReadTableScheme>(ctx, read.Pos())
                        .World(read.World())
                        .DataSource(read.DataSource())
                        .Table().Value(table).Build()
                    .Done().Ptr();
            default:
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Unsupported key type for Ydb."));
                return nullptr;
            }
        }, ctx, settings);

        if (status == TStatus::Error || PendingTables_->empty()) {
            return status;
        }

        std::vector<NThreading::TFuture<void>> handles;
        handles.reserve(PendingTables_->size() + tablesFromCluster.size());

        for (const auto& item : tablesFromCluster) {
            const auto& cluster = item.first;
            TString token = State_->Configuration->Tokens.at(cluster);

            const auto& config = State_->Configuration->Clusters[cluster];

            std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(State_->CredentialsFactory, token, config.AddBearerToToken);
            const auto ins = Clients_->emplace(cluster, std::pair<TMetaClient, std::optional<TCreateSnapshotHandleResult>>{{Driver_, NYdb::TCommonClientSettings()
                .Database(config.Database)
                .DiscoveryEndpoint(config.Endpoint)
                .SslCredentials(NYdb::TSslCredentials(config.Secure))
                .CredentialsProviderFactory(credentialsProviderFactory)
                .DiscoveryMode(NYdb::EDiscoveryMode::Async)}, std::nullopt});

            YQL_CLOG(INFO, ProviderYdb) << "Take snapshot for: `" << cluster << "` from " << config.Endpoint;

            std::optional<TCreateSnapshotHandleResult>& snapshot = ins.first->second.second;
            std::weak_ptr<TCluster2ClientPerSnapshotHandle> clients = Clients_;
            handles.emplace_back(ins.first->second.first.CreateSnapshotHandle(TVector<TString>(item.second.cbegin(), item.second.cend())).Apply([clients, &snapshot]
                (const NThreading::TFuture<TCreateSnapshotHandleResult>& future) {
                    if (future.HasException()) {
                        const_cast<NThreading::TFuture<TCreateSnapshotHandleResult>&>(future).ExtractValue();
                    } else if (clients.lock()) {
                        snapshot.emplace(const_cast<NThreading::TFuture<TCreateSnapshotHandleResult>&>(future).ExtractValue());
                    }
                }));
        }

        for (auto& pair : *PendingTables_) {
            const auto find = Clients_->find(pair.first.first);
            const auto& table = pair.first.second;
            YQL_CLOG(INFO, ProviderYdb) << "Load table meta for: `" << table << "`";

            std::optional<TDescribeTableResult>& meta = pair.second;
            std::weak_ptr<TTableKey2DescribeTableResult> pendingTables = PendingTables_;
            handles.emplace_back(find->second.first.GetTableDescription(table, true).Apply([pendingTables, &meta]
                (const NThreading::TFuture<TDescribeTableResult>& future) {
                    if (future.HasException()) {
                        const_cast<NThreading::TFuture<TDescribeTableResult>&>(future).ExtractValue();
                    } else if (pendingTables.lock()) {
                        meta.emplace(const_cast<NThreading::TFuture<TDescribeTableResult>&>(future).ExtractValue());
                    }
                }));
        }

        if (handles.empty()) {
            return TStatus::Ok;
        }

        AsyncFuture_ = NThreading::WaitExceptionOrAll(handles);
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) final {
        return AsyncFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        YQL_ENSURE(AsyncFuture_.HasValue());
        output = input;

        Y_ABORT_UNLESS(Clients_, "Clients_ was destroyed");
        Y_ABORT_UNLESS(PendingTables_, "PendingTables_ was destroyed");

        bool failed = false;
        std::unordered_map<std::string_view, TSnapshotHandle> snapshots(Clients_->size());
        for (auto& client : *Clients_) {
            auto& snapshot = *client.second.second;
            if (snapshot.IsSuccess()) {
                snapshots.emplace(client.first, snapshot.ExtractResult());
            } else {
                failed = true;
                const auto& config = State_->Configuration->Clusters[TString(client.first)];
                ctx.AddError(TIssue({}, TStringBuilder() << "Failed to take snapshot for: `" << client.first << "`, endpoint: " << config.Endpoint << ", status: " << snapshot.GetStatus()));
                for (const auto& issue : snapshot.GetIssues())
                    ctx.AddError(issue);
            }
        }

        for (auto& pair : *PendingTables_) {
            const auto& result = *pair.second;
            auto& meta = State_->Tables[pair.first];
            meta.Snapshot = snapshots[pair.first.first];
            if (result.IsSuccess()) {
                const auto& tableDescription = result.GetDescription();
                const auto& columns = tableDescription.columns();
                std::unordered_map<std::string_view, NUdf::TDataTypeId> map(columns.size());
                TVector<const TItemExprType*> items;
                items.reserve(columns.size());
                std::transform(columns.cbegin(), columns.cend(), std::back_inserter(items), [&map, &ctx] (const Ydb::Table::ColumnMeta& col) {
                    map.emplace(col.Getname(), GetYqlTypeId(NYdb::TTypeParser(col.Gettype()), ctx));
                    return ctx.MakeType<TItemExprType>(ctx.AppendString(col.Getname()), MakeYqlType(NYdb::TTypeParser(col.Gettype()), ctx));
                });
                meta.ItemType = ctx.MakeType<TStructExprType>(items);

                const auto& pKey = tableDescription.primary_key();
                meta.KeyTypes.reserve(pKey.size());
                std::transform(pKey.cbegin(), pKey.cend(), std::back_inserter(meta.KeyTypes), [&map] (const std::string_view& col) { return map[col]; });

                const auto& partitions = tableDescription.partitions();
                YQL_CLOG(INFO, ProviderYdb) << "Table " << pair.first.second << " has " << partitions.size() << " partitions by " << tableDescription.primary_key();
                TString endKey;
                meta.Partitions.reserve(partitions.size());
                for (const auto& part : partitions) {
                    meta.Partitions.emplace_back(std::array<TString,2U>{{std::move(endKey), part.end_key()}});
                    endKey = meta.Partitions.back().back();
                }
                meta.Partitions.back().back().clear();
            } else {
                failed = true;
                ctx.AddError(TIssue({}, TStringBuilder() << "Failed to load table metadata for: `" << pair.first.second << ", status: " << result.GetStatus() << "`\n"));
                for (const auto& issue : result.GetIssues())
                    ctx.AddError(issue);
            }
        }

        Clients_->clear();
        PendingTables_->clear();
        return failed ? TStatus::Error : TStatus::Ok;
    }

    void Rewind() final {
        Clients_ = std::make_shared<TCluster2ClientPerSnapshotHandle>();
        PendingTables_ = std::make_shared<TTableKey2DescribeTableResult>();
        AsyncFuture_ = {};
    }
private:
    const NYdb::TDriver Driver_;
    const TYdbState::TPtr State_;
    std::shared_ptr<TCluster2ClientPerSnapshotHandle> Clients_ = std::make_shared<TCluster2ClientPerSnapshotHandle>();
    std::shared_ptr<TTableKey2DescribeTableResult > PendingTables_ = std::make_shared<TTableKey2DescribeTableResult>();
    NThreading::TFuture<void> AsyncFuture_;
};

}

THolder<IGraphTransformer> CreateYdbLoadTableMetadataTransformer(TYdbState::TPtr state, NYdb::TDriver driver) {
    return MakeHolder<TYdbLoadTableMetadataTransformer>(state, driver);
}

} // namespace NYql
