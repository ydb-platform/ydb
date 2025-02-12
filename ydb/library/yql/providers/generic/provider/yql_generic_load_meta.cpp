#include "yql_generic_provider_impl.h"

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

namespace NYql {
    using namespace NNodes;
    using namespace NKikimr;
    using namespace NKikimr::NMiniKQL;

    class TGenericLoadTableMetadataTransformer: public TGraphTransformerBase {
        struct TTableDescription {
            using TPtr = std::shared_ptr<TTableDescription>;

            NYql::TGenericDataSourceInstance DataSourceInstance;

            std::optional<NConnector::NApi::TSchema> Schema;
            std::vector<NConnector::NApi::TSplit> Splits;

            // Issues that could occure at any phase of network interaction with Connector
            TIssues Issues; 
        };

        using TTableDescriptionMap =
            std::unordered_map<TGenericState::TTableAddress, TTableDescription::TPtr, THash<TGenericState::TTableAddress>>;

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

            std::unordered_set<TTableDescriptionMap::key_type, TTableDescriptionMap::hasher> pendingTables;
            const auto& reads = FindNodes(input, [&](const TExprNode::TPtr& node) {
                if (const auto maybeRead = TMaybeNode<TGenRead>(node)) {
                    return maybeRead.Cast().DataSource().Category().Value() == GenericProviderName;
                }
                return false;
            });

            if (!reads.empty()) {
                for (const auto& r : reads) {
                    const TGenRead read(r);
                    if (!read.FreeArgs().Get(2).Ref().IsCallable("MrTableConcat")) {
                        ctx.AddError(TIssue(ctx.GetPosition(read.FreeArgs().Get(0).Pos()), "Expected key"));
                        return TStatus::Error;
                    }

                    const auto maybeKey = TExprBase(read.FreeArgs().Get(2).Ref().HeadPtr()).Maybe<TCoKey>();
                    if (!maybeKey) {
                        ctx.AddError(TIssue(ctx.GetPosition(read.FreeArgs().Get(0).Pos()), "Expected key"));
                        return TStatus::Error;
                    }

                    const auto& keyArg = maybeKey.Cast().Ref().Head();
                    if (!keyArg.IsList() || keyArg.ChildrenSize() != 2U || !keyArg.Head().IsAtom("table") ||
                        !keyArg.Tail().IsCallable(TCoString::CallableName())) {
                        ctx.AddError(TIssue(ctx.GetPosition(keyArg.Pos()), "Expected single table name"));
                        return TStatus::Error;
                    }

                    const auto clusterName = read.DataSource().Cluster().StringValue();
                    const auto tableName = TString(keyArg.Tail().Head().Content());
                    if (pendingTables.insert(TGenericState::TTableAddress(clusterName, tableName)).second) {
                        YQL_CLOG(INFO, ProviderGeneric) << "Loading table meta for: `" << clusterName << "`.`" << tableName << "`";
                    }
                }
            }

            std::vector<NThreading::TFuture<void>> handles;
            handles.reserve(pendingTables.size());
            TableDescriptions_.reserve(pendingTables.size());

            for (const auto& tableAddress : pendingTables) {
                LoadTableMetadataFromConnector(tableAddress, handles);
            }

            if (handles.empty()) {
                return TStatus::Ok;
            }

            AsyncFuture_ = NThreading::WaitExceptionOrAll(handles);
            return TStatus::Async;
        }
    
    private:
        void LoadTableMetadataFromConnector(
            const TGenericState::TTableAddress& tableAddress,
            std::vector<NThreading::TFuture<void>>& handles
        ) {
            const auto it = State_->Configuration->ClusterNamesToClusterConfigs.find(tableAddress.ClusterName);
            YQL_ENSURE(State_->Configuration->ClusterNamesToClusterConfigs.cend() != it, "cluster not found: " << tableAddress.ClusterName);

            NConnector::NApi::TDescribeTableRequest request;
            FillDescribeTableRequest(request, it->second, tableAddress.TableName);

            auto promise = NThreading::NewPromise();
            handles.emplace_back(promise.GetFuture());

            // preserve data source instance for the further usage
            auto emplaceIt = TableDescriptions_.emplace(std::make_pair(tableAddress, std::make_shared<TTableDescription>()));
            auto desc = emplaceIt.first->second;
            desc->DataSourceInstance = request.data_source_instance();

            Y_ENSURE(State_->GenericClient);
            State_->GenericClient->DescribeTable(request).Apply(
                [desc, tableAddress, promise, client = State_->GenericClient](
                    const NConnector::TDescribeTableAsyncResult& f1) mutable {
                    NConnector::TDescribeTableAsyncResult f2(f1);
                    auto result = f2.ExtractValueSync();

                    // Check transport error
                    if (!result.Status.Ok()) {
                        desc->Issues.AddIssue(TStringBuilder() << "Call DescribeTable for table "<< tableAddress.String() << ": " << result.Status.ToDebugString());
                        promise.SetValue();
                        return; 
                    } 

                    // Check logical error
                    if (!NConnector::IsSuccess(*result.Response)) {
                        desc->Issues.AddIssues(NConnector::ErrorToIssues( 
                            result.Response->error(),
                            TStringBuilder() << "Call DescribeTable for table "<< tableAddress.String() << ": "
                        ));
                        promise.SetValue();
                        return;
                    }

                    // Preserve schema for the further usage
                    desc->Schema = result.Response->schema();

                    // Call ListSplits
                    NConnector::NApi::TListSplitsRequest request;
                    auto select = request.add_selects();
                    select->mutable_data_source_instance()->CopyFrom(desc->DataSourceInstance); 
                    select->mutable_from()->set_table(tableAddress.TableName);

                    client->ListSplits(request).Apply(
                        [desc, promise, tableAddress](const NConnector::TListSplitsStreamIteratorAsyncResult f3) mutable {
                            NConnector::TListSplitsStreamIteratorAsyncResult f4(f3);
                            auto streamIterResult = f4.ExtractValueSync();

                            // Check transport error
                            if (!streamIterResult.Status.Ok()) {
                                desc->Issues.AddIssue(streamIterResult.Status.ToDebugString());
                                promise.SetValue();
                                return; 
                            } 

                            Y_ENSURE(streamIterResult.Iterator);

                            auto& iterator = streamIterResult.Iterator;
                            while(true) {
                                auto result = iterator->ReadNext().GetValueSync();

                                // Check transport error
                                if (!result.Status.Ok()) {
                                    // It could be either EOF (== success), or unexpected error  
                                    if (!NConnector::GrpcStatusEndOfStream(result.Status)) {
                                        desc->Issues.AddIssue(
                                            TStringBuilder() << "Call ListSplits for table " << tableAddress.String() << ": " << result.Status.ToDebugString());
                                    }

                                    promise.SetValue();
                                    return; 
                                }

                                Y_ENSURE(result.Response);

                                if (!NConnector::IsSuccess(*result.Response)) {
                                    desc->Issues.AddIssues(
                                        NConnector::ErrorToIssues(
                                            result.Response->error(),
                                            TStringBuilder() << "Call ListSplits for table "<< tableAddress.String() << ": "
                                        )
                                    );
                                    promise.SetValue();
                                    return;
                                }

                                std::transform(
                                    std::make_move_iterator(result.Response->splits().begin()),
                                    std::make_move_iterator(result.Response->splits().end()),
                                    std::back_inserter(desc->Splits),
                                    [](auto&& split) { return std::move(split); }
                                );
                            }
                        }
                    );
                }
            );
        }

    public:
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

            // Iterate over all the requested tables, check Connector responses
            for (const auto& r: reads) {
                const TGenRead genRead(r);
                const auto clusterName = genRead.DataSource().Cluster().StringValue();
                const auto& keyArg = TExprBase(genRead.FreeArgs().Get(2).Ref().HeadPtr()).Cast<TCoKey>().Ref().Head();
                const auto tableName = TString(keyArg.Tail().Head().Content());
                const TGenericState::TTableAddress tableAddress{clusterName, tableName};

                // Find appropriate response
                auto iter = TableDescriptions_.find(tableAddress);
                if (iter == TableDescriptions_.end()) {
                    ctx.AddError(
                        TIssue(ctx.GetPosition(genRead.Pos()), 
                                TStringBuilder() << "Connector response not found for table " << tableAddress.String())
                    );

                    return TStatus::Error;
                }

                const auto& result = iter->second;

                // If errors occured during network interaction with Connector, return them
                if (result->Issues) {
                    for (const auto& issue: result->Issues) {
                        ctx.AddError(issue);
                    }

                    return TStatus::Error;
                }

                Y_ENSURE(result->Schema);
                Y_ENSURE(result->Splits.size() > 0);

                TGenericState::TTableMeta tableMeta;
                tableMeta.Schema = *result->Schema;
                tableMeta.DataSourceInstance = result->DataSourceInstance;

                // Parse table schema
                ParseTableMeta(ctx, ctx.GetPosition(genRead.Pos()), tableAddress, tableMeta);
                
                // Fill AST for each requested table
                if (const auto ins = replaces.emplace(genRead.Raw(), TExprNode::TPtr()); ins.second) {
                    ins.first->second = MakeTableMetaNode(ctx, genRead, tableName, result->Splits);
                }

                // Save table metadata into provider state
                State_->AddTable(tableAddress, std::move(tableMeta));
            }

            return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings(nullptr));
        }

        void Rewind() final {
            TableDescriptions_.clear();
            AsyncFuture_ = {};
        }

    private:
        TIssues ParseTableMeta(
            TExprContext& ctx,
            const TPosition& pos,
            const TGenericState::TTableAddress& tableAddress,
            TGenericState::TTableMeta& tableMeta
        ) try {
            TVector<const TItemExprType*> items;

            const auto& columns = tableMeta.Schema.columns();
            if (columns.empty()) {
                TIssues issues;
                issues.AddIssue(TIssue(pos, TStringBuilder() << "Table " << tableAddress.String() << " doesn't exist."));
                return issues;
            }

            for (const auto& column: columns) {
                // Make type annotation
                NYdb::TTypeParser parser(column.type());
                auto typeAnnotation = NFq::MakeType(parser, ctx);

                // Create items from graph
                items.emplace_back(ctx.MakeType<TItemExprType>(column.name(), typeAnnotation));
                tableMeta.ColumnOrder.emplace_back(column.name());
            }

            tableMeta.ItemType = ctx.MakeType<TStructExprType>(items);
            return TIssues{};
        } catch (std::exception&) {
            TIssues issues;
            issues.AddIssue(TIssue(pos, TStringBuilder() << "Failed to parse table metadata: " << CurrentExceptionMessage()));
            return issues;
        }

        TExprNode::TPtr MakeTableMetaNode(
            TExprContext& ctx,
            const TGenRead& read,
            const TString& tableName,
            const std::vector<NConnector::NApi::TSplit>& srcSplits
        ) {
            // clang-format off
            auto row = Build<TCoArgument>(ctx, read.Pos())
                .Name("row")
                .Done();

            auto emptyPredicate = Build<TCoLambda>(ctx, read.Pos())
                .Args({row})
                .Body<TCoBool>()
                    .Literal().Build("true")
                    .Build()
                .Done().Ptr();

            TVector<TCoAtom> splitDescriptions;
            for (auto& split : srcSplits) {
                splitDescriptions.push_back(Build<TCoAtom>(ctx, read.Pos()).Value(split.description()).Done());
            }
            auto dstSplits = Build<TCoAtomList>(ctx, read.Pos()).Add(splitDescriptions).Done();

            auto table = Build<TGenTable>(ctx, read.Pos())
                .Name().Value(tableName).Build()
                .Splits(std::move(dstSplits))
            .Done();

            return Build<TGenReadTable>(ctx, read.Pos())
                .World(read.World())
                .DataSource(read.DataSource())
                .Table(table)
                .Columns<TCoVoid>().Build()
                .FilterPredicate(emptyPredicate)
            .Done().Ptr();
            // clang-format on
        }

        void FillDescribeTableRequest(NConnector::NApi::TDescribeTableRequest& request, const TGenericClusterConfig& clusterConfig,
                                      const TString& tablePath) {
            const auto dataSourceKind = clusterConfig.GetKind();
            auto dsi = request.mutable_data_source_instance();
            *dsi->mutable_endpoint() = clusterConfig.GetEndpoint();
            dsi->set_kind(dataSourceKind);
            dsi->set_use_tls(clusterConfig.GetUseSsl());
            dsi->set_protocol(clusterConfig.GetProtocol());
            FillCredentials(request, clusterConfig);
            FillTypeMappingSettings(request);
            FillDataSourceOptions(request, clusterConfig);
            FillTablePath(request, clusterConfig, tablePath);
        }

        void FillCredentials(NConnector::NApi::TDescribeTableRequest& request, const TGenericClusterConfig& clusterConfig) {
            auto dsi = request.mutable_data_source_instance();

            // If login/password is provided, just copy them into request:
            // connector will use Basic Auth to access external data sources.
            if (clusterConfig.GetCredentials().Hasbasic()) {
                *dsi->mutable_credentials() = clusterConfig.GetCredentials();
                return;
            }

            // If there are no Basic Auth parameters, two options can be considered:

            // 1. Client provided own IAM-token to access external data source
            auto iamToken = State_->Types->Credentials->FindCredentialContent(
                "default_" + clusterConfig.name(),
                "default_generic",
                clusterConfig.GetToken());
            if (iamToken) {
                *dsi->mutable_credentials()->mutable_token()->mutable_value() = iamToken;
                *dsi->mutable_credentials()->mutable_token()->mutable_type() = "IAM";
                return;
            }

            // 2. Client provided service account creds that must be converted into IAM-token
            Y_ENSURE(State_->CredentialsFactory, "CredentialsFactory is not initialized");

            auto structuredTokenJSON = TStructuredTokenBuilder().SetServiceAccountIdAuth(
                                                                    clusterConfig.GetServiceAccountId(),
                                                                    clusterConfig.GetServiceAccountIdSignature())
                                           .ToJson();

            Y_ENSURE(structuredTokenJSON, "empty structured token");

            // Create provider or get existing one.
            // It's crucial to reuse providers because their construction implies synchronous IO.
            auto providersIt = State_->CredentialProviders.find(clusterConfig.name());
            if (providersIt == State_->CredentialProviders.end()) {
                auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(
                    State_->CredentialsFactory,
                    structuredTokenJSON,
                    false);

                providersIt = State_->CredentialProviders.emplace(
                                                             std::make_pair(clusterConfig.name(), credentialsProviderFactory->CreateProvider()))
                                  .first;
            }

            iamToken = providersIt->second->GetAuthInfo();
            Y_ENSURE(iamToken, "empty IAM token");

            *dsi->mutable_credentials()->mutable_token()->mutable_value() = iamToken;
            *dsi->mutable_credentials()->mutable_token()->mutable_type() = "IAM";
        }

        template <typename T>
        void SetSchema(T& request, const TGenericClusterConfig& clusterConfig) {
            TString schema;
            const auto it = clusterConfig.GetDataSourceOptions().find("schema");
            if (it != clusterConfig.GetDataSourceOptions().end()) {
                schema = it->second;
            }
            if (!schema) {
                schema = "public";
            }

            request.set_schema(schema);
        }

        void SetOracleServiceName(NYql::TOracleDataSourceOptions& options, const TGenericClusterConfig& clusterConfig) {
            const auto it = clusterConfig.GetDataSourceOptions().find("service_name");
            if (it != clusterConfig.GetDataSourceOptions().end()) {
                options.set_service_name(it->second);
            }
        }

        void SetLoggingFolderId(NYql::TLoggingDataSourceOptions& options, const TGenericClusterConfig& clusterConfig) {
            const auto it = clusterConfig.GetDataSourceOptions().find("folder_id");
            if (it != clusterConfig.GetDataSourceOptions().end()) {
                options.set_folder_id(it->second);
            }
        }

        void FillDataSourceOptions(NConnector::NApi::TDescribeTableRequest& request, const TGenericClusterConfig& clusterConfig) {
            const auto dataSourceKind = clusterConfig.GetKind();
            switch (dataSourceKind) {
                case NYql::EGenericDataSourceKind::CLICKHOUSE:
                    break;
                case NYql::EGenericDataSourceKind::YDB:
                    break;
                case NYql::EGenericDataSourceKind::MYSQL:
                    break;
                case NYql::EGenericDataSourceKind::GREENPLUM: {
                    auto* options = request.mutable_data_source_instance()->mutable_gp_options();
                    SetSchema(*options, clusterConfig);
                } break;
                case NYql::EGenericDataSourceKind::MS_SQL_SERVER:
                    break;
                case NYql::EGenericDataSourceKind::POSTGRESQL: {
                    auto* options = request.mutable_data_source_instance()->mutable_pg_options();
                    SetSchema(*options, clusterConfig);
                } break;
                case NYql::EGenericDataSourceKind::ORACLE: {
                    auto* options = request.mutable_data_source_instance()->mutable_oracle_options();
                    SetOracleServiceName(*options, clusterConfig);
                } break;
                case NYql::EGenericDataSourceKind::LOGGING: {
                    auto* options = request.mutable_data_source_instance()->mutable_logging_options();
                    SetLoggingFolderId(*options, clusterConfig);
                } break;
                default:
                    ythrow yexception() << "Unexpected data source kind: '" << NYql::EGenericDataSourceKind_Name(dataSourceKind)
                                        << "'";
            }
        }

        void FillTypeMappingSettings(NConnector::NApi::TDescribeTableRequest& request) {
            const TString dateTimeFormat =
                State_->Configuration->DateTimeFormat.Get().GetOrElse(TGenericSettings::TDefault::DateTimeFormat);
            if (dateTimeFormat == "string") {
                request.mutable_type_mapping_settings()->set_date_time_format(NConnector::NApi::STRING_FORMAT);
            } else if (dateTimeFormat == "YQL") {
                request.mutable_type_mapping_settings()->set_date_time_format(NConnector::NApi::YQL_FORMAT);
            } else {
                ythrow yexception() << "Unexpected date/time format: '" << dateTimeFormat << "'";
            }
        }

        void FillTablePath(NConnector::NApi::TDescribeTableRequest& request, const TGenericClusterConfig& clusterConfig,
                           const TString& tablePath) {
            request.mutable_data_source_instance()->set_database(clusterConfig.GetDatabaseName());
            request.set_table(tablePath);
        }

    private:
        const TGenericState::TPtr State_;

        TTableDescriptionMap TableDescriptions_;

        NThreading::TFuture<void> AsyncFuture_;
    };

    THolder<IGraphTransformer> CreateGenericLoadTableMetadataTransformer(TGenericState::TPtr state) {
        return MakeHolder<TGenericLoadTableMetadataTransformer>(std::move(state));
    }

} // namespace NYql
