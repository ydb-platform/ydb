#include "yql_generic_dq_integration.h"

#include "yql_generic_mkql_compiler.h"
#include "yql_generic_predicate_pushdown.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/common/dq/yql_dq_integration_impl.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/providers/generic/proto/range.pb.h>
#include <ydb/library/yql/providers/generic/proto/source.pb.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/utils.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/plan/plan_utils.h>

namespace NYql {

    using namespace NNodes;

    namespace {

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

            TExprNode::TPtr WrapRead(const TDqSettings&, const TExprNode::TPtr& read, TExprContext& ctx) override {
                if (const auto maybeGenReadTable = TMaybeNode<TGenReadTable>(read)) {
                    const auto genReadTable = maybeGenReadTable.Cast();
                    YQL_ENSURE(genReadTable.Ref().GetTypeAnn(), "No type annotation for node " << genReadTable.Ref().Content());
                    const auto token = TString("cluster:default_") += genReadTable.DataSource().Cluster().StringValue();
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
                            .Cluster(genReadTable.DataSource().Cluster())
                            .Table(genReadTable.Table())
                            .Token<TCoSecureParam>()
                                .Name().Build(token)
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

            ui64 Partition(const TDqSettings&, size_t, const TExprNode&, TVector<TString>& partitions, TString*, TExprContext&,
                           bool) override {
                partitions.clear();
                Generic::TRange range;
                partitions.emplace_back();
                TStringOutput out(partitions.back());
                range.Save(&out);
                return 0ULL;
            }

            void FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings,
                                    TString& sourceType) override {
                const TDqSource source(&node);
                if (const auto maybeSettings = source.Settings().Maybe<TGenSourceSettings>()) {
                    const auto settings = maybeSettings.Cast();
                    const auto& clusterName = source.DataSource().Cast<TGenDataSource>().Cluster().StringValue();
                    const auto& table = settings.Table().StringValue();
                    const auto& token = settings.Token().Name().StringValue();
                    const auto& endpoint = State_->Configuration->ClusterNamesToClusterConfigs[clusterName].endpoint();

                    Generic::TSource srcDesc;
                    srcDesc.set_token(token);

                    // for backward compability full path can be used (cluster_name.`db_name.table`)
                    // TODO: simplify during https://st.yandex-team.ru/YQ-2494
                    TStringBuf db, dbTable;
                    if (!TStringBuf(table).TrySplit('.', db, dbTable)) {
                        dbTable = table;
                    }

                    YQL_CLOG(INFO, ProviderGeneric)
                        << "Filling source settings"
                        << ": cluster: " << clusterName
                        << ", table: " << table
                        << ", endpoint: " << endpoint.ShortDebugString();

                    const auto& columns = settings.Columns();

                    auto [tableMeta, issue] = State_->GetTable(clusterName, table);
                    if (issue.has_value()) {
                        ythrow yexception() << "Get table metadata: " << issue.value();
                    }

                    // prepare select
                    auto select = srcDesc.mutable_select();
                    select->mutable_from()->set_table(TString(dbTable));
                    select->mutable_data_source_instance()->CopyFrom(tableMeta.value()->DataSourceInstance);

                    auto items = select->mutable_what()->mutable_items();
                    for (size_t i = 0; i < columns.Size(); i++) {
                        // assign column name
                        auto column = items->Add()->mutable_column();
                        auto columnName = columns.Item(i).StringValue();
                        column->mutable_name()->assign(columnName);

                        // assign column type
                        auto type = NConnector::GetColumnTypeByName(tableMeta.value()->Schema, columnName);
                        column->mutable_type()->CopyFrom(type);
                    }

                    if (auto predicate = settings.FilterPredicate(); !IsEmptyFilterPredicate(predicate)) {
                        TStringBuilder err;
                        if (!SerializeFilterPredicate(predicate, select->mutable_where()->mutable_filter_typed(), err)) {
                            ythrow yexception() << "Failed to serialize filter predicate for source: " << err;
                        }
                    }

                    // store data source instance
                    srcDesc.mutable_data_source_instance()->CopyFrom(tableMeta.value()->DataSourceInstance);

                    // preserve source description for read actor
                    protoSettings.PackFrom(srcDesc);

                    switch (srcDesc.data_source_instance().kind()) {
                        case NYql::NConnector::NApi::CLICKHOUSE:
                            sourceType = "ClickHouseGeneric";
                            break;
                        case NYql::NConnector::NApi::POSTGRESQL:
                            sourceType = "PostgreSqlGeneric";
                            break;
                        default:
                            ythrow yexception() << "Data source kind is unknown or not specified";
                            break;
                    }
                }
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
                const TString& table = settings.Table().StringValue();
                properties["Table"] = table;
                auto [tableMeta, issue] = State_->GetTable(clusterName, table);
                if (!issue) {
                    const NConnector::NApi::TDataSourceInstance& dataSourceInstance = tableMeta.value()->DataSourceInstance;
                    switch (dataSourceInstance.kind()) {
                        case NConnector::NApi::CLICKHOUSE:
                            properties["SourceType"] = "ClickHouse";
                            break;
                        case NConnector::NApi::POSTGRESQL:
                            properties["SourceType"] = "PostgreSql";
                            break;
                        case NConnector::NApi::DATA_SOURCE_KIND_UNSPECIFIED:
                            break;
                        default:
                            properties["SourceType"] = NConnector::NApi::EDataSourceKind_Name(dataSourceInstance.kind());
                            break;
                    }

                    if (const TString& database = dataSourceInstance.database()) {
                        properties["Database"] = database;
                    }

                    switch (dataSourceInstance.protocol()) {
                        case NConnector::NApi::NATIVE:
                            properties["Protocol"] = "Native";
                            break;
                        case NConnector::NApi::HTTP:
                            properties["Protocol"] = "Http";
                            break;
                        case NConnector::NApi::PROTOCOL_UNSPECIFIED:
                            break;
                        default:
                            properties["Protocol"] = NConnector::NApi::EProtocol_Name(dataSourceInstance.protocol());
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

        private:
            const TGenericState::TPtr State_;
        };

    }

    THolder<IDqIntegration> CreateGenericDqIntegration(TGenericState::TPtr state) {
        return MakeHolder<TGenericDqIntegration>(state);
    }

}
