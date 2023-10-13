#include "yql_generic_provider_impl.h"

#include <library/cpp/json/json_reader.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_type_string.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/error.h>

namespace NYql {
    using namespace NNodes;
    using namespace NKikimr;
    using namespace NKikimr::NMiniKQL;

    struct TGenericTableDescription {
        TGenericTableDescription() = delete;

        TGenericTableDescription(const NConnector::NApi::TDataSourceInstance& dsi,
                                 NConnector::TDescribeTableResult::TPtr&& result)
            : DataSourceInstance(dsi)
            , Result(std::move(result))
        {
        }

        NConnector::NApi::TDataSourceInstance DataSourceInstance;
        NConnector::TDescribeTableResult::TPtr Result;
    };

    class TGenericLoadTableMetadataTransformer: public TGraphTransformerBase {
        using TMapType = std::unordered_map<TGenericState::TTableAddress, TGenericTableDescription, THash<TGenericState::TTableAddress>>;

    public:
        TGenericLoadTableMetadataTransformer(TGenericState::TPtr state)
            : State_(std::move(state))
        {
        }

        TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
            output = input;

            if (ctx.Step.IsDone(TExprStep::LoadTablesMetadata)) {
                return TStatus::Ok;
            }

            std::unordered_set<TMapType::key_type, TMapType::hasher> pendingTables;
            const auto& reads = FindNodes(input,
                                          [&](const TExprNode::TPtr& node) {
                                              if (const auto maybeRead = TMaybeNode<TGenRead>(node)) {
                                                  return maybeRead.Cast().DataSource().Category().Value() == GenericProviderName;
                                              }
                                              return false;
                                          });
            if (!reads.empty()) {
                for (const auto& r : reads) {
                    const TGenRead read(r);
                    if (!read.FreeArgs().Get(2).Ref().IsCallable("MrTableConcat")) {
                        ctx.AddError(
                            TIssue(ctx.GetPosition(read.FreeArgs().Get(0).Pos()), TStringBuilder() << "Expected key"));
                        return TStatus::Error;
                    }

                    const auto maybeKey = TExprBase(read.FreeArgs().Get(2).Ref().HeadPtr()).Maybe<TCoKey>();
                    if (!maybeKey) {
                        ctx.AddError(
                            TIssue(ctx.GetPosition(read.FreeArgs().Get(0).Pos()), TStringBuilder() << "Expected key"));
                        return TStatus::Error;
                    }

                    const auto& keyArg = maybeKey.Cast().Ref().Head();
                    if (!keyArg.IsList() || keyArg.ChildrenSize() != 2U || !keyArg.Head().IsAtom("table") ||
                        !keyArg.Tail().IsCallable(TCoString::CallableName())) {
                        ctx.AddError(
                            TIssue(ctx.GetPosition(keyArg.Pos()), TStringBuilder() << "Expected single table name"));
                        return TStatus::Error;
                    }

                    const auto clusterName = read.DataSource().Cluster().StringValue();
                    const auto tableName = TString(keyArg.Tail().Head().Content());
                    if (pendingTables.insert(TGenericState::TTableAddress(clusterName, tableName)).second) {
                        YQL_CLOG(INFO, ProviderGeneric)
                            << "Loading table meta for: `" << clusterName << "`.`" << tableName << "`";
                    }
                }
            }

            std::vector<NThreading::TFuture<void>> handles;
            handles.reserve(pendingTables.size());
            Results_.reserve(pendingTables.size());

            for (const auto& item : pendingTables) {
                NConnector::NApi::TDescribeTableRequest request;

                const auto& clusterName = item.first;
                const auto it = State_->Configuration->ClusterNamesToClusterConfigs.find(clusterName);
                YQL_ENSURE(State_->Configuration->ClusterNamesToClusterConfigs.cend() != it, "cluster not found: " << clusterName);

                const auto& clusterConfig = it->second;
                const auto dataSourceKind = clusterConfig.GetKind();

                auto dsi = request.mutable_data_source_instance();
                dsi->mutable_endpoint()->CopyFrom(clusterConfig.GetEndpoint());
                dsi->set_kind(dataSourceKind);
                dsi->mutable_credentials()->CopyFrom(clusterConfig.GetCredentials());
                dsi->set_use_tls(clusterConfig.GetUseSsl());
                dsi->set_protocol(clusterConfig.GetProtocol());

                // for backward compability full path can be used (cluster_name.`db_name.table`)
                // TODO: simplify during https://st.yandex-team.ru/YQ-2494
                const auto& tablePath = item.second;
                const auto& dbNameFromConfig = clusterConfig.GetDatabaseName();
                TStringBuf dbNameTarget, tableName;
                auto isFullPath = TStringBuf(tablePath).TrySplit('.', dbNameTarget, tableName);

                if (!dbNameFromConfig.empty()) {
                    dbNameTarget = dbNameFromConfig;
                    if (!isFullPath) {
                        tableName = tablePath;
                    }
                } else if (!isFullPath) {
                    tableName = tablePath;
                    switch (dataSourceKind) {
                        case NYql::NConnector::NApi::CLICKHOUSE:
                            dbNameTarget = "default";
                            break;
                        case NYql::NConnector::NApi::POSTGRESQL:
                            dbNameTarget = "postgres";
                            break;
                        default:
                            ythrow yexception() << "Unexpected data source kind: '"
                                                << NYql::NConnector::NApi::EDataSourceKind_Name(dataSourceKind) << "'";
                    }
                } // else take database name from table path

                dsi->set_database(TString(dbNameTarget));
                request.set_table(TString(tableName));

                // NOTE: errors will be checked further in DoApplyAsyncChanges
                Results_.emplace(item, TGenericTableDescription(request.data_source_instance(), State_->GenericClient->DescribeTable(request)));

                // FIXME: for the sake of simplicity, asynchronous workflow is broken now. Fix it some day.
                auto promise = NThreading::NewPromise();
                handles.emplace_back(promise.GetFuture());
                promise.SetValue();
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
            AsyncFuture_.GetValue();

            const auto& reads = FindNodes(input, [&](const TExprNode::TPtr& node) {
                if (const auto maybeRead = TMaybeNode<TGenRead>(node)) {
                    return maybeRead.Cast().DataSource().Category().Value() == GenericProviderName;
                }
                return false;
            });

            TNodeOnNodeOwnedMap replaces(reads.size());
            bool hasErrors = false;

            for (const auto& r : reads) {
                const TGenRead read(r);
                const auto clusterName = read.DataSource().Cluster().StringValue();
                const auto& keyArg = TExprBase(read.FreeArgs().Get(2).Ref().HeadPtr()).Cast<TCoKey>().Ref().Head();
                const auto tableName = TString(keyArg.Tail().Head().Content());

                const auto it = Results_.find(TGenericState::TTableAddress(clusterName, tableName));
                if (Results_.cend() != it) {
                    const auto& result = it->second.Result;
                    const auto& error = result->Error;
                    if (NConnector::ErrorIsSuccess(error)) {
                        TGenericState::TTableMeta tableMeta;
                        tableMeta.Schema = result->Schema;
                        tableMeta.DataSourceInstance = it->second.DataSourceInstance;

                        const auto& parse = ParseTableMeta(tableMeta.Schema, clusterName, tableName, ctx, tableMeta.ColumnOrder);

                        if (parse) {
                            tableMeta.ItemType = parse;
                            if (const auto ins = replaces.emplace(read.Raw(), TExprNode::TPtr()); ins.second) {
                                // clang-format off
                                ins.first->second = Build<TGenReadTable>(ctx, read.Pos())
                                    .World(read.World())
                                    .DataSource(read.DataSource())
                                    .Table().Value(tableName).Build()
                                    .Columns<TCoVoid>().Build()
                                .Done().Ptr();
                                // clang-format on
                            }
                            State_->AddTable(clusterName, tableName, std::move(tableMeta));
                        } else {
                            hasErrors = true;
                            break;
                        }
                    } else {
                        NConnector::ErrorToExprCtx(error, ctx, ctx.GetPosition(read.Pos()),
                                                   TStringBuilder()
                                                       << "Loading metadata for table: " << clusterName << '.' << tableName);
                        hasErrors = true;
                        break;
                    }
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(read.Pos()),
                                        TStringBuilder() << "Not found result for " << clusterName << '.' << tableName));
                    hasErrors = true;
                    break;
                }
            }

            if (hasErrors) {
                return TStatus::Error;
            }

            return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings(nullptr));
        }

        void Rewind() final {
            Results_.clear();
            AsyncFuture_ = {};
        }

    private:
        const TStructExprType* ParseTableMeta(const NConnector::NApi::TSchema& schema,
                                              const std::string_view& cluster,
                                              const std::string_view& table, TExprContext& ctx,
                                              TVector<TString>& columnOrder) try {
            TVector<const TItemExprType*> items;

            auto columns = schema.columns();
            if (columns.empty()) {
                ctx.AddError(TIssue({}, TStringBuilder() << "Table " << cluster << '.' << table << " doesn't exist."));
                return nullptr;
            }

            for (auto i = 0; i < columns.size(); i++) {
                // Make type annotation
                NYdb::TTypeParser parser(columns.Get(i).type());
                auto typeAnnotation = NFq::MakeType(parser, ctx);

                // Create items from graph
                items.emplace_back(ctx.MakeType<TItemExprType>(columns.Get(i).name(), typeAnnotation));
                columnOrder.emplace_back(columns.Get(i).name());
            }
            // FIXME: handle on Connector's side?
            return ctx.MakeType<TStructExprType>(items);
        } catch (std::exception&) {
            ctx.AddError(TIssue({}, TStringBuilder() << "Failed to parse table metadata: " << CurrentExceptionMessage()));
            return nullptr;
        }

    private:
        const TGenericState::TPtr State_;

        TMapType Results_;
        NThreading::TFuture<void> AsyncFuture_;
    };

    THolder<IGraphTransformer> CreateGenericLoadTableMetadataTransformer(TGenericState::TPtr state) {
        return MakeHolder<TGenericLoadTableMetadataTransformer>(std::move(state));
    }

} // namespace NYql
