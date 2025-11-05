#include "yql_generic_dq_integration.h"
#include "yql_generic_mkql_compiler.h"
#include "yql_generic_predicate_pushdown.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/utils.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/providers/generic/proto/partition.pb.h>
#include <ydb/library/yql/providers/generic/proto/source.pb.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_utils.h>
#include <ydb/library/yql/utils/plan/plan_utils.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/providers/common/dq/yql_dq_integration_impl.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

    using namespace NNodes;

    namespace {

        TString GetSourceType(NYql::TGenericDataSourceInstance dsi) {
            switch (dsi.kind()) {
                case NYql::EGenericDataSourceKind::CLICKHOUSE:
                    return "ClickHouseGeneric";
                case NYql::EGenericDataSourceKind::POSTGRESQL:
                    return "PostgreSqlGeneric";
                case NYql::EGenericDataSourceKind::MYSQL:
                    return "MySqlGeneric";
                case NYql::EGenericDataSourceKind::YDB:
                    return "YdbGeneric";
                case NYql::EGenericDataSourceKind::GREENPLUM:
                    return "GreenplumGeneric";
                case NYql::EGenericDataSourceKind::MS_SQL_SERVER:
                    return "MsSQLServerGeneric";
                case NYql::EGenericDataSourceKind::ORACLE:
                    return "OracleGeneric";
                case NYql::EGenericDataSourceKind::LOGGING:
                    return "LoggingGeneric";
                case NYql::EGenericDataSourceKind::ICEBERG:
                    return "IcebergGeneric";
                case NYql::EGenericDataSourceKind::REDIS:
                    return "RedisGeneric";
                case NYql::EGenericDataSourceKind::PROMETHEUS:
                    return "PrometheusGeneric";
                case NYql::EGenericDataSourceKind::MONGO_DB:
                    return "MongoDBGeneric";
                case NYql::EGenericDataSourceKind::OPENSEARCH:
                    return "OpenSearchGeneric";
                default:
                    throw yexception() << "Data source kind is unknown or not specified";
            }
        }

        class TGenericDqIntegration: public TDqIntegrationBase {
        public:
            TGenericDqIntegration(TGenericState::TPtr state)
                : State_(state)
            {
            }

            bool CanRead(const TExprNode& read, TExprContext&, bool) override {
                return TGenReadTable::Match(&read);
            }

            TMaybe<ui64> EstimateReadSize(ui64 /*dataSizePerJob*/, ui32 /*maxTasksPerStage*/, const TVector<const TExprNode*>& read,
                                          TExprContext&) override {
                if (AllOf(read, [](const auto val) { return TGenReadTable::Match(val); })) {
                    return 0ul; // TODO: return real size
                }
                return Nothing();
            }

            TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx, const TWrapReadSettings&) override {
                if (const auto maybeGenReadTable = TMaybeNode<TGenReadTable>(read)) {
                    const auto genReadTable = maybeGenReadTable.Cast();
                    YQL_ENSURE(genReadTable.Ref().GetTypeAnn(), "No type annotation for node " << genReadTable.Ref().Content());
                    const auto tokenName = TString("cluster:default_") += genReadTable.DataSource().Cluster().StringValue();
                    const auto rowType = genReadTable.Ref()
                                             .GetTypeAnn()
                                             ->Cast<TTupleExprType>()
                                             ->GetItems()
                                             .back()
                                             ->Cast<TListExprType>()
                                             ->GetItemType();
                    auto columns = genReadTable.Columns().Ptr();
                    if (!columns->IsList()) {
                        const auto pos = columns->Pos();
                        const auto& items = rowType->Cast<TStructExprType>()->GetItems();
                        TExprNode::TListType cols;
                        cols.reserve(items.size());
                        std::transform(items.cbegin(), items.cend(), std::back_inserter(cols),
                                       [&](const TItemExprType* item) { return ctx.NewAtom(pos, item->GetName()); });
                        columns = ctx.NewList(pos, std::move(cols));
                    }

                    // clang-format off
                    return Build<TDqSourceWrap>(ctx, read->Pos())
                        .Input<TGenSourceSettings>()
                            .World(genReadTable.World())
                            .Cluster(genReadTable.DataSource().Cluster())
                            .Table(genReadTable.Table().Name())
                            .Token<TCoSecureParam>()
                                .Name().Build(tokenName)
                                .Build()
                            .Columns(std::move(columns))
                            .FilterPredicate(genReadTable.FilterPredicate())
                            .Build()
                        .RowType(ExpandType(genReadTable.Pos(), *rowType, ctx))
                        .DataSource(genReadTable.DataSource().Cast<TCoDataSource>())
                        .Done().Ptr();
                    // clang-format on
                }
                return read;
            }

            ///
            /// Fill a select from a dq source
            ///
            void FillSelect(NConnector::NApi::TSelect& select, const TDqSource& source, TExprContext& ctx) {
                const auto maybeSettings = source.Settings().Maybe<TGenSourceSettings>();

                if (!maybeSettings) {
                    return;
                }

                const auto settings = maybeSettings.Cast();
                const auto& tableName = settings.Table().StringValue();
                const auto& clusterName = source.DataSource().Cast<TGenDataSource>().Cluster().StringValue();
                auto [tableMeta, issues] = State_->GetTable({clusterName, tableName});

                if (issues) {
                    throw yexception() << "Get table metadata: " << issues.ToOneLineString();
                }

                FillSelectFromGenSourceSettings(select, settings, ctx, tableMeta);
            }

            ui64 Partition(
                const TExprNode& node,
                TVector<TString>& partitions,
                TString*,
                TExprContext& ctx,
                const TPartitionSettings& partitionSettings) override {
                auto maybeDqSource = TMaybeNode<TDqSource>(&node);
                if (!maybeDqSource) {
                    return 0;
                }

                auto srcSettings = maybeDqSource.Cast().Settings();
                auto maybeGenSourceSettings = TMaybeNode<TGenSourceSettings>(srcSettings.Raw());
                Y_ENSURE(maybeGenSourceSettings);
                auto genSourceSettings = maybeGenSourceSettings.Cast();

                const TGenericState::TTableAddress tableAddress{
                    genSourceSettings.Cluster().StringValue(),
                    genSourceSettings.Table().StringValue()};

                // Extract table metadata from provider state>.
                auto [tableMeta, issues] = State_->GetTable(tableAddress);
                if (issues) {
                    for (const auto& issue : issues) {
                        ctx.AddError(issue);
                    }

                    return 0;
                }

                NConnector::NApi::TSelect select;
                FillSelect(select, TDqSource(&node), ctx);

                auto selectKey = tableAddress.MakeKeyFor(select);
                auto splits = tableMeta->GetSplitsForSelect(selectKey);
                const size_t totalSplits = splits.size();

                partitions.clear();

                if (totalSplits <= partitionSettings.MaxPartitions) {
                    // If there are not too many splits, simply make a single-split partitions.
                    for (size_t i = 0; i < totalSplits; i++) {
                        Generic::TPartition partition;
                        *partition.add_splits() = splits[i];
                        TString partitionStr;
                        YQL_ENSURE(partition.SerializeToString(&partitionStr), "Failed to serialize partition");
                        partitions.emplace_back(std::move(partitionStr));
                    }
                } else {
                    // If the number of splits is greater than the partitions limit,
                    // we have to make split batches in each partition.
                    size_t splitsPerPartition = (totalSplits / partitionSettings.MaxPartitions - 1) + 1;

                    for (size_t i = 0; i < totalSplits; i += splitsPerPartition) {
                        Generic::TPartition partition;
                        for (size_t j = i; j < i + splitsPerPartition && j < totalSplits; j++) {
                            *partition.add_splits() = splits[j];
                        }
                        TString partitionStr;
                        YQL_ENSURE(partition.SerializeToString(&partitionStr), "Failed to serialize partition");
                        partitions.emplace_back(std::move(partitionStr));
                    }
                }

                // TODO: check what's the meaning of this value
                return 0;
            }

            void FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings,
                                    TString& sourceType, size_t, TExprContext& ctx) override {
                const TDqSource dqSource(&node);
                const auto maybeSettings = dqSource.Settings().Maybe<TGenSourceSettings>();

                if (!maybeSettings) {
                    return;
                }

                const auto settings = maybeSettings.Cast();
                const auto& clusterName = dqSource.DataSource().Cast<TGenDataSource>().Cluster().StringValue();
                const auto& tableName = settings.Table().StringValue();
                const auto& clusterConfig = State_->Configuration->ClusterNamesToClusterConfigs[clusterName];
                const auto& endpoint = clusterConfig.endpoint();

                Generic::TSource source;

                YQL_CLOG(INFO, ProviderGeneric)
                    << "Filling source settings"
                    << ": cluster: " << clusterName
                    << ", table: " << tableName
                    << ", endpoint: " << endpoint.ShortDebugString();

                auto [tableMeta, issues] = State_->GetTable({clusterName, tableName});

                if (issues) {
                    throw yexception() << "Get table metadata: " << issues.ToOneLineString();
                }

                // prepare select
                auto select = source.mutable_select();
                FillSelect(*select, dqSource, ctx);
                
                // We set token name to the protobuf message that will be received
                // by the read actor during the execution phase.
                // It will use token name to extract credentials from the secureParams.
                const TString tokenName(settings.Token().Maybe<TCoSecureParam>().Name().Cast());
                source.SetTokenName(tokenName);

                // preserve source description for read actor
                protoSettings.PackFrom(source);
                sourceType = GetSourceType(select->data_source_instance());
            }

            bool FillSourcePlanProperties(const NNodes::TExprBase& node, TMap<TString, NJson::TJsonValue>& properties) override {
                if (!node.Maybe<TDqSource>()) {
                    return false;
                }

                auto source = node.Cast<TDqSource>();
                if (!source.Settings().Maybe<TGenSourceSettings>()) {
                    return false;
                }

                const TGenSourceSettings settings = source.Settings().Cast<TGenSourceSettings>();
                const TString& clusterName = source.DataSource().Cast<TGenDataSource>().Cluster().StringValue();
                const TString& tableName = settings.Table().StringValue();
                properties["Table"] = tableName;
                auto [tableMeta, issue] = State_->GetTable({clusterName, tableName});
                if (!issue) {
                    const NYql::TGenericDataSourceInstance& dataSourceInstance = tableMeta->DataSourceInstance;
                    switch (dataSourceInstance.kind()) {
                        case NYql::EGenericDataSourceKind::CLICKHOUSE:
                            properties["SourceType"] = "ClickHouse";
                            break;
                        case NYql::EGenericDataSourceKind::POSTGRESQL:
                            properties["SourceType"] = "PostgreSql";
                            break;
                        case NYql::EGenericDataSourceKind::MYSQL:
                            properties["SourceType"] = "MySql";
                            break;
                        case NYql::EGenericDataSourceKind::YDB:
                            properties["SourceType"] = "Ydb";
                            break;
                        case NYql::EGenericDataSourceKind::GREENPLUM:
                            properties["SourceType"] = "Greenplum";
                            break;
                        case NYql::EGenericDataSourceKind::MS_SQL_SERVER:
                            properties["SourceType"] = "MsSQLServer";
                            break;
                        case NYql::EGenericDataSourceKind::ORACLE:
                            properties["SourceType"] = "Oracle";
                            break;
                        case NYql::EGenericDataSourceKind::LOGGING:
                            properties["SourceType"] = "Logging";
                            break;
                        case NYql::EGenericDataSourceKind::ICEBERG:
                            properties["SourceType"] = "Iceberg";
                            break;
                        case NYql::EGenericDataSourceKind::MONGO_DB:
                            properties["SourceType"] = "MongoDB";
                            break;
                        case NYql::EGenericDataSourceKind::REDIS:
                            properties["SourceType"] = "Redis";
                            break;
                        case NYql::EGenericDataSourceKind::PROMETHEUS:
                            properties["SourceType"] = "Prometheus";
                            break;
                        case NYql::EGenericDataSourceKind::OPENSEARCH:
                            properties["SourceType"] = "OpenSearch";
                            break;
                        case NYql::EGenericDataSourceKind::DATA_SOURCE_KIND_UNSPECIFIED:
                            break;
                        default:
                            properties["SourceType"] = NYql::EGenericDataSourceKind_Name(dataSourceInstance.kind());
                            break;
                    }

                    if (const TString& database = dataSourceInstance.database()) {
                        properties["Database"] = database;
                    }

                    switch (dataSourceInstance.protocol()) {
                        case NYql::EGenericProtocol::NATIVE:
                            properties["Protocol"] = "Native";
                            break;
                        case NYql::EGenericProtocol::HTTP:
                            properties["Protocol"] = "Http";
                            break;
                        case NYql::EGenericProtocol::PROTOCOL_UNSPECIFIED:
                        default:
                            properties["Protocol"] = NYql::EGenericProtocol_Name(dataSourceInstance.protocol());
                            break;
                    }
                }
                if (settings.Columns().Size()) {
                    auto& columns = properties["ReadColumns"];
                    for (const TCoAtom col : settings.Columns()) {
                        columns.AppendValue(col.StringValue());
                    }
                }
                if (auto predicate = settings.FilterPredicate(); !IsEmptyFilterPredicate(predicate)) {
                    properties["Filter"] = NPlanUtils::PrettyExprStr(predicate);
                }
                return true;
            }

            void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
                RegisterDqGenericMkqlCompilers(compiler, State_);
            }

            void FillLookupSourceSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sourceType) override {
                const TDqLookupSourceWrap wrap(&node);
                const auto settings = wrap.Input().Cast<TGenSourceSettings>();

                const auto& clusterName = wrap.DataSource().Cast<TGenDataSource>().Cluster().StringValue();
                const auto& tableName = settings.Table().StringValue();
                const auto& clusterConfig = State_->Configuration->ClusterNamesToClusterConfigs[clusterName];
                const auto& endpoint = clusterConfig.endpoint();

                YQL_CLOG(INFO, ProviderGeneric)
                    << "Filling lookup source settings"
                    << ": cluster: " << clusterName
                    << ", table: " << tableName
                    << ", endpoint: " << endpoint.ShortDebugString();

                auto [tableMeta, issues] = State_->GetTable({clusterName, tableName});
                if (issues) {
                    throw yexception() << "Get table metadata: " << issues.ToOneLineString();
                }

                Generic::TLookupSource source;
                source.set_table(tableName);
                *source.mutable_data_source_instance() = tableMeta->DataSourceInstance;

                // We set token name to the protobuf message that will be received
                // by the lookup actor during the execution phase.
                // It will use token name to extract credentials from the secureParams.
                const TString tokenName(settings.Token().Maybe<TCoSecureParam>().Name().Cast());
                source.SetTokenName(tokenName);

                // preserve source description for read actor
                protoSettings.PackFrom(source);
                sourceType = GetSourceType(source.data_source_instance());
            }

        private:
            const TGenericState::TPtr State_;
        };

    } // namespace

    THolder<IDqIntegration> CreateGenericDqIntegration(TGenericState::TPtr state) {
        return MakeHolder<TGenericDqIntegration>(state);
    }

} // namespace NYql
