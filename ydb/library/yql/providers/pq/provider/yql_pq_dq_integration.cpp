#include "yql_pq_dq_integration.h"
#include "yql_pq_settings.h"
#include "yql_pq_helpers.h"
#include "yql_pq_mkql_compiler.h"
#include "yql_pq_topic_key_parser.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/dq/runtime/streaming/partition_key.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_predicate_pushdown.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/providers/common/dq/yql_dq_integration_impl.h>
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <util/string/builder.h>

#include <string_view>

namespace NYql {

namespace {

using namespace NNodes;
using namespace std::literals::string_view_literals;

class TPqDqIntegration : public TDqIntegrationBase {
    static constexpr ui64 DefaultMaxPartitions = 10000;

public:
    explicit TPqDqIntegration(const TPqState::TPtr& state)
        : State_(state.Get())
    {}

    ui64 PartitionTopicRead(
        const TPqTopic& topic,
        size_t maxPartitions,
        TVector<TString>& partitions,
        bool streamingTopicRead,
        const std::unordered_set<ui64>& predicatePartitions) {
        size_t topicPartitionsCount = 0;
        for (auto kv : topic.Props()) {
            auto key = kv.Name().Value();
            if (key == PartitionsCountProp) {
                topicPartitionsCount = FromString(kv.Value().Ref().Content());
            }
        }
        YQL_ENSURE(topicPartitionsCount > 0);
        if (!streamingTopicRead && !maxPartitions) {
            maxPartitions = 1;      // Reading in table mode - 1 task by default.
        }
        if (!maxPartitions) {
            maxPartitions = DefaultMaxPartitions;
        }

        if (predicatePartitions.empty()) {      // read all partitions
            const size_t tasks = Min(maxPartitions, topicPartitionsCount);
            partitions.reserve(tasks);
            for (size_t i = 0; i < tasks; ++i) {
                NPq::NProto::TDqReadTaskParams params;
                auto* partitioningParams = params.AddPartitioningParams();
                partitioningParams->SetTopicPartitionsCount(topicPartitionsCount);
                partitioningParams->SetEachTopicPartitionGroupId(i);
                partitioningParams->SetDqPartitionsCount(tasks);
                YQL_CLOG(DEBUG, ProviderPq) << "Create DQ reading partition " << params;

                TString serializedParams;
                YQL_ENSURE(params.SerializeToString(&serializedParams));
                partitions.emplace_back(std::move(serializedParams));
            }
        } else {    // read only predicate partitions
            const size_t tasks = Min(maxPartitions, predicatePartitions.size());
            auto predicatePartitionIt = predicatePartitions.begin();
            partitions.reserve(tasks);
            size_t allocatedPartitions = 0;

            for (size_t i = 0; i < tasks; ++i) {
                NPq::NProto::TDqReadTaskParams params;
                size_t partitionsPerTask = (predicatePartitions.size() - allocatedPartitions) / (tasks - i);
                for (size_t k = 0; k < partitionsPerTask; ++k) {
                    auto* partitioningParams = params.AddPartitioningParams();
                    partitioningParams->SetTopicPartitionsCount(topicPartitionsCount);
                    auto partition = *predicatePartitionIt;
                    partitioningParams->SetEachTopicPartitionGroupId(partition);
                    partitioningParams->SetDqPartitionsCount(topicPartitionsCount);
                    YQL_CLOG(DEBUG, ProviderPq) << "Create DQ reading partition " << params;
                    predicatePartitionIt++;
                }
                allocatedPartitions += partitionsPerTask;
                TString serializedParams;
                YQL_ENSURE(params.SerializeToString(&serializedParams));
                partitions.emplace_back(std::move(serializedParams));
            }
        }
        return 0;
    }

    template<typename TContainer>
    bool GetPartitions(const TExprNode& node, TContainer& partitions) {
        partitions.clear();
        if (!node.IsCallable("AsList")) {
            return false;
        }
        for (ui32 j = 0; j < node.ChildrenSize(); ++j) {
            if (!node.Child(j)->IsCallable("Uint64")) {
                return false;
            }
            partitions.insert(FromString<ui64>(node.Child(j)->Child(0)->Content()));
        }
        return true;
    }

    ui64 Partition(const TExprNode& node, TVector<TString>& partitions, TString*, TExprContext&, const TPartitionSettings& settings) override {
        if (auto maybePqRead = TMaybeNode<TPqReadTopic>(&node)) {
            return PartitionTopicRead(maybePqRead.Cast().Topic(), settings.MaxPartitions, partitions, true, {});
        }
        if (auto maybeDqSource = TMaybeNode<TDqSource>(&node)) {
            auto srcSettings = maybeDqSource.Cast().Settings();
            if (auto maybeTopicSource = TMaybeNode<TDqPqTopicSource>(srcSettings.Raw())) {
                TDqPqTopicSource topicSource = maybeTopicSource.Cast();
                bool streamingTopicRead = State_->StreamingTopicsReadByDefault;
                size_t const settingsCount = topicSource.Settings().Size();
                for (size_t i = 0; i < settingsCount; ++i) {
                    TCoNameValueTuple setting = topicSource.Settings().Item(i);
                    const TStringBuf name = Name(setting);
                    if (name != StreamingTopicRead) {
                        continue;
                    }
                    streamingTopicRead = FromString<bool>(Value(setting));
                    break;
                }
                std::unordered_set<ui64> predicatePartitions;
                bool success = GetPartitions(*topicSource.Partitions().Ptr(), predicatePartitions);
                if (!success) {
                    YQL_CLOG(WARN, ProviderPq) << "Failed to get predicate partitions";
                }
                return PartitionTopicRead(topicSource.Topic(), settings.MaxPartitions, partitions, streamingTopicRead, predicatePartitions);
            }
        }
        return 0;
    }

    TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx, const TWrapReadSettings& wrSettings) override {
        if (const auto& maybePqReadTopic = TMaybeNode<TPqReadTopic>(read)) {
            if (wrSettings.WatermarksMode.Defined()) { // fq only
                const auto& watermarksMode = *wrSettings.WatermarksMode;
                State_->EnableWatermarks = watermarksMode == "default";
                State_->EnableWatermarksAdvanced = watermarksMode == "advanced";
            }

            const auto& pqReadTopic = maybePqReadTopic.Cast();
            YQL_ENSURE(pqReadTopic.Ref().GetTypeAnn(), "No type annotation for node " << pqReadTopic.Ref().Content());

            const auto pqTopic = pqReadTopic.Topic();

            const auto rowType = pqReadTopic.Ref().GetTypeAnn()
                ->Cast<TTupleExprType>()->GetItems().back()->Cast<TListExprType>()
                ->GetItemType()->Cast<TStructExprType>();
            const auto& clusterName = pqReadTopic.DataSource().Cluster().StringValue();
            const auto token = "cluster:default_" + clusterName;

            const auto& typeItems = pqTopic.RowSpec().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>()->GetItems();
            const auto pos = read->Pos();

            TExprNode::TListType colNames;
            colNames.reserve(typeItems.size());
            std::transform(typeItems.cbegin(), typeItems.cend(), std::back_inserter(colNames),
                [&](const TItemExprType* item) {
                    return ctx.NewAtom(pos, item->GetName());
                });
            auto columnNames = ctx.NewList(pos, std::move(colNames));

            const auto maybeSettings = BuildTopicReadSettings(pqReadTopic, ctx, wrSettings);
            if (!maybeSettings) {
                return {};
            }
            const auto settings = maybeSettings.Cast();

            const bool useSharedReading = AnyOf(settings, [](const TCoNameValueTuple& setting) {
                return Name(setting) == SharedReading && FromString<bool>(Value(setting));
            });

            const auto maybeWatermark = pqReadTopic.Watermark().Maybe<TCoLambda>();

            TMaybeNode<TCoAtom> watermarkSerialized;
            if (maybeWatermark) {
                const auto watermark = maybeWatermark.Cast();

                TStringBuilder err;
                NYql::NConnector::NApi::TExpression watermarkExprProto;
                if (!NYql::SerializeWatermarkExpr(ctx, watermark, &watermarkExprProto, err)) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Failed to serialize Watermark Expr to proto: " + err));
                    return {};
                }

                TString serializedWatermarkExpr;
                if (!watermarkExprProto.SerializeToString(&serializedWatermarkExpr)) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Failed to serialize Watermark Expr to string"));
                    return {};
                }

                watermarkSerialized = Build<TCoAtom>(ctx, watermark.Pos()).Value(serializedWatermarkExpr).Done();
            }

            const auto expandedRowType = ExpandType(pqReadTopic.Pos(), *rowType, ctx);

            TExprBase result = Build<TDqSourceWrap>(ctx, pos)
                .Input<TDqPqTopicSource>()
                    .World(pqReadTopic.World())
                    .Topic(pqTopic)
                    .Columns(std::move(columnNames))
                    .Settings(settings)
                    .Token<TCoSecureParam>()
                        .Name().Build(token)
                        .Build()
                    .FilterPredicate().Value(TString()).Build()  // Empty predicate by default <=> WHERE TRUE
                    .RowType(expandedRowType)
                    .Partitions<TCoVoid>().Build()
                    .OffsetPredicate().Value(TString()).Build()  // Empty predicate by default <=> WHERE TRUE
                    .WriteTimePredicate().Value(TString()).Build()  // Empty predicate by default <=> WHERE TRUE
                    .WatermarkExpr(maybeWatermark)
                    .WatermarkSerialized(watermarkSerialized)
                    .Build()
                .RowType(expandedRowType)
                .DataSource(pqReadTopic.DataSource().Cast<TCoDataSource>())
                .Settings(BuildDqSourceWrapSettings(pqReadTopic, pos, ctx))
                .Done();

            if (maybeWatermark && State_->EnableWatermarksAdvanced && !useSharedReading) {
                const auto watermark = maybeWatermark.Cast();

                const auto eventTimeAndDelay = SplitWatermarkExpr(watermark, *State_, ctx);
                if (!eventTimeAndDelay) {
                    return {};
                }
                const auto [eventTimeExtractor, _] = *eventTimeAndDelay;

                auto watermarkSettingsBuilder = Build<TCoNameValueTupleList>(ctx, pos);
                for (const auto& nameValue : settings) {
                    if (const auto name = nameValue.Name().Value();
                        WatermarksLateArrivalDelayUsSetting == name) {
                        watermarkSettingsBuilder.Add<TCoNameValueTuple>().InitFrom(nameValue).Build();
                    } else if (WatermarksGranularityUsSetting == name) {
                        watermarkSettingsBuilder.Add<TCoNameValueTuple>().InitFrom(nameValue).Build();
                    } else if (WatermarksIdleTimeoutUsSetting == name) {
                        watermarkSettingsBuilder.Add<TCoNameValueTuple>().InitFrom(nameValue).Build();
                    }
                }
                for (const auto& nameValue : pqTopic.Props()) {
                    if (const auto name = nameValue.Name().Value();
                        FederatedClustersProp == name) {
                        auto federatedClusters = nameValue.Value().Cast<TDqPqFederatedClusterList>();

                        TVector<TCoAtom> newFederatedClusters;
                        for (const auto& federatedCluster : federatedClusters) {
                            const auto cluster = federatedCluster.Name();
                            const auto partitionsCount = federatedCluster.PartitionsCount();

                            TString newFederatedCluster;
                            TStringOutput ss(newFederatedCluster);
                            ss << NDq::TPartitionKey {
                                .Cluster = TString{cluster.Value()},
                                .PartitionId = partitionsCount ? FromString<ui32>(partitionsCount.Cast().Value()) : 0,
                            };
                            newFederatedClusters.push_back(Build<TCoAtom>(ctx, federatedClusters.Pos()).Value(newFederatedCluster).Done());
                        }
                        watermarkSettingsBuilder.Add<TCoNameValueTuple>()
                            .Name<TCoAtom>().Build(name)
                            .Value<TCoAtomList>().Add(newFederatedClusters).Build()
                            .Build();
                    }
                }
                const TCoNameValueTupleList watermarkSettings = watermarkSettingsBuilder.Done();

                // The partition id metadata column is exposed at the expr level under the user-facing
                // __ydb_ name when system columns are forbidden, and under the legacy _yql_sys_ name otherwise.
                const auto partitionIdDescriptor = GetPqMetaFieldDescriptorByKey(
                    "partition_id",
                    State_->AddTransparentPrefixToTransparentSystemColumns,
                    State_->EnableUserAttributesInTopicQuery,
                    State_->ForbidYqlSysColumnsAndSystemMetadata
                );
                if (!partitionIdDescriptor) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Cannot bind partition_id metadata to column"));
                    return {};
                }
                const auto clusterDescriptor = GetPqMetaFieldDescriptorByKey(
                    "cluster",
                    State_->AddTransparentPrefixToTransparentSystemColumns,
                    State_->EnableUserAttributesInTopicQuery,
                    State_->ForbidYqlSysColumnsAndSystemMetadata
                );
                if (!clusterDescriptor) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Cannot bind cluster metadata to column"));
                    return {};
                }
                const auto writeTimeDescriptor = GetPqMetaFieldDescriptorByKey(
                    "write_time",
                    State_->AddTransparentPrefixToTransparentSystemColumns,
                    State_->EnableUserAttributesInTopicQuery,
                    State_->ForbidYqlSysColumnsAndSystemMetadata
                );
                if (!writeTimeDescriptor) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Cannot bind write_time metadata to column"));
                    return {};
                }

                result = Build<TDqPhyWatermarkGenerator>(ctx, pos)
                    .Input(result)
                    .WatermarkExtractor(eventTimeExtractor)
                    .PartitionKeyExtractor<TCoLambda>()
                        .Args({"arg"})
                        .Body<TCoAsStruct>()
                            .Add<TCoNameValueTuple>()
                                .Name<TCoAtom>().Build("cluster")
                                .Value<TCoMember>()
                                    .Struct("arg")
                                    .Name().Build(clusterDescriptor->SysColumn)
                                    .Build()
                                .Build()
                            .Add<TCoNameValueTuple>()
                                .Name<TCoAtom>().Build("partition_id")
                                .Value<TCoMember>()
                                    .Struct("arg")
                                    .Name().Build(partitionIdDescriptor->SysColumn)
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                    .WriteTimeExtractor<TCoLambda>()
                        .Args({"arg"})
                        .Body<TCoMember>()
                            .Struct("arg")
                            .Name().Build(writeTimeDescriptor->SysColumn)
                            .Build()
                        .Build()
                    .WatermarkSettings(watermarkSettings.Ptr())
                    .PartitionKeys<TCoVoid>().Build()
                    .Done();
            }

            return result.Ptr();
        }
        return read;
    }

    TMaybe<bool> CanWrite(const TExprNode& write, TExprContext&) override {
        return TPqWriteTopic::Match(&write);
    }

    TExprNode::TPtr WrapWrite(const TExprNode::TPtr& writeNode, TExprContext& ctx) override {
        TExprBase writeExpr(writeNode);
        const auto write = writeExpr.Cast<TPqWriteTopic>();

        return Build<TPqInsert>(ctx, write.Pos())
            .World(write.World())
            .DataSink(write.DataSink())
            .Topic(write.Topic())
            .Input(write.Input())
            .Done().Ptr();
    }

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
        RegisterDqPqMkqlCompilers(compiler);
    }

    static TStringBuf Name(const TCoNameValueTuple& nameValue) {
        return nameValue.Name().Value();
    }

    static TStringBuf Value(const TCoNameValueTuple& nameValue) {
        if (TMaybeNode<TExprBase> maybeValue = nameValue.Value()) {
            const TExprNode& value = maybeValue.Cast().Ref();
            YQL_ENSURE(value.IsAtom());
            return value.Content();
        }

        return {};
    }

    static NPq::NProto::EClusterType ToClusterType(NYql::TPqClusterConfig::EClusterType t) {
        switch (t) {
        case NYql::TPqClusterConfig::CT_UNSPECIFIED:
            return NPq::NProto::Unspecified;
        case NYql::TPqClusterConfig::CT_PERS_QUEUE:
            return NPq::NProto::PersQueue;
        case NYql::TPqClusterConfig::CT_DATA_STREAMS:
            return NPq::NProto::DataStreams;
        }
    }

    TMaybe<TSourceWatermarksSettings> ExtractSourceWatermarksSettings(const TExprNode& /*node*/, const ::google::protobuf::Any& protoSettings, const TString& sourceType) override {
        YQL_ENSURE(sourceType == "PqSource");
        YQL_ENSURE(protoSettings.Is<NPq::NProto::TDqPqTopicSource>());
        NYql::NPq::NProto::TDqPqTopicSource srcDesc;
        if (!protoSettings.UnpackTo(&srcDesc)) {
            return Nothing();
        }
        if (!srcDesc.HasWatermarks()) {
            return Nothing();
        }
        TSourceWatermarksSettings watermarksSettings;
        const auto& watermarks = srcDesc.GetWatermarks();
        if (watermarks.HasIdleTimeoutUs()) {
            watermarksSettings.IdleTimeoutUs = watermarks.GetIdleTimeoutUs();
        }
        return watermarksSettings;
    }

    void FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sourceType, size_t, TExprContext& ctx) override {
        if (auto maybeDqSource = TMaybeNode<TDqSource>(&node)) {
            auto settings = maybeDqSource.Cast().Settings();
            if (auto maybeTopicSource = TMaybeNode<TDqPqTopicSource>(settings.Raw())) {
                NPq::NProto::TDqPqTopicSource srcDesc;
                TDqPqTopicSource topicSource = maybeTopicSource.Cast();

                TPqTopic topic = topicSource.Topic();
                const TStringBuf cluster = topic.Cluster().Value();
                const auto* clusterDesc = State_->Configuration->ClustersConfigurationSettings.FindPtr(cluster);
                YQL_ENSURE(clusterDesc, "Unknown cluster " << cluster);
                srcDesc.SetClusterType(ToClusterType(clusterDesc->ClusterType));
                auto topicPath = topic.Path().Value();
                auto topicDatabase = topic.Database().Value();
                if (clusterDesc->ClusterType == NYql::TPqClusterConfig::CT_PERS_QUEUE && topicDatabase == "/Root") {
                    auto pos = topicPath.find('/');
                    Y_ENSURE(pos != TStringBuf::npos);
                    srcDesc.SetTopicPath(TString(topicPath.substr(pos + 1)));
                    srcDesc.SetDatabase("/logbroker-federation/" + TString(topicPath.substr(0, pos)));
                } else {
                    srcDesc.SetTopicPath(TString(topicPath));
                    srcDesc.SetDatabase(TString(topicDatabase));
                }
                srcDesc.SetDatabaseId(clusterDesc->DatabaseId);

                const TStructExprType* fullRowType = topicSource.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                srcDesc.SetRowType(NCommon::WriteTypeToYson(fullRowType, NYT::NYson::EYsonFormat::Text));

                if (const auto& types = State_->Types) {
                    if (const auto& optLLVM = types->OptLLVM) {
                        srcDesc.SetEnabledLLVM(!optLLVM->empty() && *optLLVM != "OFF");
                    }
                }

                if (const auto& bufferSize = State_->Configuration->ReadSessionBufferBytes.Get()) {
                    srcDesc.SetReadSessionBufferBytes(*bufferSize);
                }

                std::optional<NDqProto::TDqIntegrationCommonSettings> commonSettings;
                if (const auto maxPartitionReadSkew = State_->Configuration->MaxPartitionReadSkew.Get()) {
                    *srcDesc.MutableMaxPartitionReadSkew() = NProtoInterop::CastToProto(*maxPartitionReadSkew);

                    NDqProto::TDqControlPlaneActorSettings aggregatorSettings;
                    aggregatorSettings.SetType("PqInfoAggregator");
                    commonSettings = NDqProto::TDqIntegrationCommonSettings();
                    YQL_ENSURE(commonSettings->MutableStageControlPlaneActors()->emplace("ControlPlane/PqSourcePartitionBalancerAggregatorId", aggregatorSettings).second);
                }

                bool sharedReading = false;
                bool skipErrors = false;
                bool streamingTopicRead = State_->StreamingTopicsReadByDefault;
                TString format;
                const TExprNode* userSchemaColumnsSetting = nullptr;
                size_t const settingsCount = topicSource.Settings().Size();
                for (size_t i = 0; i < settingsCount; ++i) {
                    TCoNameValueTuple setting = topicSource.Settings().Item(i);
                    const TStringBuf name = Name(setting);
                    if (name == ConsumerSetting) {
                        srcDesc.SetConsumerName(TString(Value(setting)));
                    } else if (name == EndpointSetting) {
                        srcDesc.SetEndpoint(TString(Value(setting)));
                    } else if (name == SharedReading) {
                        sharedReading = FromString<bool>(Value(setting));
                    } else if (name == ReconnectPeriod) {
                        srcDesc.SetReconnectPeriod(TString(Value(setting)));
                    } else if (name == ReadGroup) {
                        srcDesc.SetReadGroup(TString(Value(setting)));
                    } else if (name == Format) {
                        format = TString(Value(setting));
                    } else if (name == UseSslSetting) {
                        srcDesc.SetUseSsl(FromString<bool>(Value(setting)));
                    } else if (name == AddBearerToTokenSetting) {
                        srcDesc.SetAddBearerToToken(FromString<bool>(Value(setting)));
                    } else if (name == WatermarksEnableSetting) {
                        srcDesc.MutableWatermarks()->SetEnabled(FromString<bool>(Value(setting)));
                    } else if (name == WatermarksGranularityUsSetting) {
                        srcDesc.MutableWatermarks()->SetGranularityUs(FromString<ui64>(Value(setting)));
                    } else if (name == WatermarksLateArrivalDelayUsSetting) {
                        srcDesc.MutableWatermarks()->SetLateArrivalDelayUs(FromString<ui64>(Value(setting)));
                    } else if (name == WatermarksIdleTimeoutUsSetting) {
                        srcDesc.MutableWatermarks()->SetIdleTimeoutUs(FromString<ui64>(Value(setting)));
                    } else if (name == WatermarksIdlePartitionsSetting) {
                        srcDesc.MutableWatermarks()->SetIdlePartitionsEnabled(FromString<bool>(Value(setting)));
                    } else if (name == SkipJsonErrors) {
                        skipErrors = FromString<bool>(Value(setting));
                    } else if (name == StreamingTopicRead) {
                        streamingTopicRead = FromString<bool>(Value(setting));
                    } else if (name == PartitionsBalancingIdleTimeoutUsSetting) {
                        *srcDesc.MutablePartitionsBalancingIdleTimeout() = NProtoInterop::CastToProto(TDuration::MicroSeconds(FromString<ui64>(Value(setting))));
                    } else if (name == UserSchemaColumnsSetting) {
                        if (TMaybeNode<TExprBase> maybeList = setting.Value()) {
                            userSchemaColumnsSetting = maybeList.Cast().Raw();
                        }
                    }
                }

                if (format == "csv"sv && userSchemaColumnsSetting) {
                    YQL_ENSURE(userSchemaColumnsSetting->IsList(), "UserSchemaColumns must be a list of atoms");
                    for (ui32 j = 0; j < userSchemaColumnsSetting->ChildrenSize(); ++j) {
                        YQL_ENSURE(userSchemaColumnsSetting->Child(j)->IsAtom(), "UserSchemaColumns must be a list of atoms");
                        srcDesc.AddUserSchemaColumns(TString(userSchemaColumnsSetting->Child(j)->Content()));
                    }
                }

                srcDesc.SetStopAtCurrentEndOffsets(!streamingTopicRead);

                for (auto prop : topic.Props()) {
                    const TStringBuf name = Name(prop);
                    if (name == FederatedClustersProp) {
                        auto clusterList = prop.Value().Cast<TDqPqFederatedClusterList>();
                        for (auto cluster : clusterList) {
                            auto federatedCluster = srcDesc.AddFederatedClusters();
                            federatedCluster->SetName(cluster.Name().StringValue());
                            federatedCluster->SetEndpoint(cluster.Endpoint().StringValue());
                            federatedCluster->SetDatabase(cluster.Database().StringValue());
                            if (cluster.PartitionsCount()) {
                                federatedCluster->SetPartitionsCount(FromString<ui32>(cluster.PartitionsCount().Cast().StringValue()));
                            }
                        }
                    }
                }

                srcDesc.SetFormat(format);
                srcDesc.SetUseActorSystemThreadsInTopicClient(State_->UseActorSystemThreadsInTopicClient);

                if (auto maybeToken = TMaybeNode<TCoSecureParam>(topicSource.Token().Raw())) {
                    srcDesc.MutableToken()->SetName(TString(maybeToken.Cast().Name().Value()));
                }

                if (clusterDesc->ClusterType == NYql::TPqClusterConfig::CT_PERS_QUEUE) {
                    YQL_ENSURE(srcDesc.GetConsumerName(), "No consumer specified for PersQueue cluster");
                }

                for (const auto metadata : topic.Metadata()) {
                    const auto sysColumnName = metadata.Value().Maybe<TCoAtom>().Cast().StringValue();
                    if (State_->ForbidYqlSysColumnsAndSystemMetadata && SkipPqSystemPrefix(sysColumnName)) {
                        continue;
                    }
                    // For __ydb_-prefixed columns, map to the corresponding _yql_sys_ name
                    // so the read actor can find the right extractor
                    if (auto oldName = YdbSysColumnToOldSysColumn(sysColumnName, State_->AddTransparentPrefixToTransparentSystemColumns)) {
                        // Only add the _yql_sys_ version if it's not already present
                        bool alreadyPresent = false;
                        for (size_t i = 0; i < static_cast<size_t>(srcDesc.MetadataFieldsSize()); ++i) {
                            if (srcDesc.GetMetadataFields(i) == *oldName) {
                                alreadyPresent = true;
                                break;
                            }
                        }
                        if (!alreadyPresent) {
                            srcDesc.AddMetadataFields(*oldName);
                        }
                    } else {
                        srcDesc.AddMetadataFields(sysColumnName);
                    }
                }

                const auto rowSchema = topic.RowSpec().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                for (const auto& item : rowSchema->GetItems()) {
                    srcDesc.AddColumns(TString(item->GetName()));
                    srcDesc.AddColumnTypes(NCommon::WriteTypeToYson(item->GetItemType(), NYT::NYson::EYsonFormat::Text));
                }

                NYql::NConnector::NApi::TPredicate predicateProto;
                auto serializedProto = topicSource.FilterPredicate().Ref().Content();
                YQL_ENSURE(predicateProto.ParseFromString(serializedProto));
                TString filterPredicateSql = NYql::FormatPredicate(predicateProto);

                NPq::NProto::TOffsetPredicate offsetPredicates;
                auto offsetSerialized = topicSource.OffsetPredicate().Ref().Content();
                YQL_ENSURE(offsetPredicates.ParseFromString(offsetSerialized));

                NPq::NProto::TWriteTimePredicate writeTimePredicate;
                auto writeTimeSerialized = topicSource.WriteTimePredicate().Ref().Content();
                YQL_ENSURE(writeTimePredicate.ParseFromString(writeTimeSerialized));

                if (!streamingTopicRead) {
                    *srcDesc.MutableOffsetPredicate() = offsetPredicates;
                    *srcDesc.MutableWriteTimePredicate() = writeTimePredicate;
                }
                if (sharedReading) {
                    srcDesc.SetPredicate(filterPredicateSql);
                    srcDesc.SetSharedReading(true);
                }
                srcDesc.SetSkipJsonErrors(skipErrors);

                if (!streamingTopicRead) {
                    srcDesc.MutableDisposition()->mutable_oldest();
                } else {
                    *srcDesc.MutableDisposition() = State_->Disposition;
                }

                for (const auto& [label, value] : State_->TaskSensorLabels) {
                    auto taskSensorLabel = srcDesc.AddTaskSensorLabel();
                    taskSensorLabel->SetLabel(label);
                    taskSensorLabel->SetValue(value);
                }
                for (auto nodeId : State_->NodeIds) {
                    srcDesc.AddNodeIds(nodeId);
                }

                TString watermarkExprSql;
                if (const auto maybeWatermarkSerialized = topicSource.WatermarkSerialized()) {
                    const auto serializedWatermarkExpr = maybeWatermarkSerialized.Cast().Ref().Content();
                    if (!serializedWatermarkExpr.empty()) {
                        NYql::NConnector::NApi::TExpression watermarkExprProto;
                        YQL_ENSURE(watermarkExprProto.ParseFromString(serializedWatermarkExpr));
                        watermarkExprSql = NYql::FormatExpression(watermarkExprProto);
                        srcDesc.SetWatermarkExpr(watermarkExprSql);
                    }
                }

                if (commonSettings) {
                    commonSettings->MutableSettings()->PackFrom(srcDesc);
                    protoSettings.PackFrom(*commonSettings);
                } else {
                    protoSettings.PackFrom(srcDesc);
                }

                if (sharedReading && !filterPredicateSql.empty()) {
                    if (filterPredicateSql.size() > 4000) {
                        filterPredicateSql = filterPredicateSql.substr(0, 4000) + "...";
                    }
                    ctx.AddWarning(TIssue(ctx.GetPosition(node.Pos()), "Row dispatcher will use the predicate: " + filterPredicateSql));
                }
                if (sharedReading && !watermarkExprSql.empty()) {
                    ctx.AddWarning(TIssue(ctx.GetPosition(node.Pos()), "Row dispatcher will use watermark expr: " + watermarkExprSql));
                }
                sourceType = "PqSource";
            }
        }
    }

    void FillSinkSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sinkType) override {
        if (auto maybeDqSink = TMaybeNode<TDqSink>(&node)) {
            auto settings = maybeDqSink.Cast().Settings();
            if (auto maybeTopicSink = TMaybeNode<TDqPqTopicSink>(settings.Raw())) {
                NPq::NProto::TDqPqTopicSink sinkDesc;
                TDqPqTopicSink topicSink = maybeTopicSink.Cast();

                TPqTopic topic = topicSink.Topic();
                const TStringBuf cluster = topic.Cluster().Value();
                const auto* clusterDesc = State_->Configuration->ClustersConfigurationSettings.FindPtr(cluster);
                YQL_ENSURE(clusterDesc, "Unknown cluster " << cluster);
                sinkDesc.SetClusterType(ToClusterType(clusterDesc->ClusterType));
                auto topicPath = topic.Path().Value();
                auto topicDatabase = topic.Database().Value();
                if (clusterDesc->ClusterType == NYql::TPqClusterConfig::CT_PERS_QUEUE && topicDatabase == "/Root") {
                    auto pos = topicPath.find('/');
                    Y_ENSURE(pos != TStringBuf::npos);
                    sinkDesc.SetTopicPath(TString(topicPath.substr(pos + 1)));
                    sinkDesc.SetDatabase("/logbroker-federation/" + TString(topicPath.substr(0, pos)));
                } else {
                    sinkDesc.SetTopicPath(TString(topicPath));
                    sinkDesc.SetDatabase(TString(topicDatabase));
                }

                sinkDesc.SetUseActorSystemThreadsInTopicClient(State_->UseActorSystemThreadsInTopicClient);

                size_t const settingsCount = topicSink.Settings().Size();
                for (size_t i = 0; i < settingsCount; ++i) {
                    TCoNameValueTuple setting = topicSink.Settings().Item(i);
                    const TStringBuf name = Name(setting);
                    if (name == EndpointSetting) {
                        sinkDesc.SetEndpoint(TString(Value(setting)));
                    } else if (name == UseSslSetting) {
                        sinkDesc.SetUseSsl(FromString<bool>(Value(setting)));
                    } else if (name == AddBearerToTokenSetting) {
                        sinkDesc.SetAddBearerToToken(FromString<bool>(Value(setting)));
                    }
                }

                if (auto maybeToken = TMaybeNode<TCoSecureParam>(topicSink.Token().Raw())) {
                    sinkDesc.MutableToken()->SetName(TString(maybeToken.Cast().Name().Value()));
                }

                if (auto maybeEnableDeduplication = State_->Configuration->EnableDeduplication.Get()) {
                    sinkDesc.SetEnableDeduplication(*maybeEnableDeduplication);
                }

                protoSettings.PackFrom(sinkDesc);
                sinkType = "PqSink";
            }
        }
    }

    bool CanRead(const TExprNode& read, TExprContext&, bool) override {
        return TPqReadTopic::Match(&read);
    }

    TMaybe<ui64> EstimateReadSize(ui64 /*dataSizePerJob*/, ui32 /*maxTasksPerStage*/, const TVector<const TExprNode*>& read, TExprContext&) override {
        if (AllOf(read, [](const auto val) { return TPqReadTopic::Match(val); })) {
            return 0ul; // TODO: return real size
        }
        return Nothing();
    }

public:
    TMaybeNode<TCoNameValueTupleList> BuildTopicReadSettings(
        const TPqReadTopic& pqReadTopic,
        TExprContext& ctx,
        const IDqIntegration::TWrapReadSettings& wrSettings
    ) const {
        const auto pos = pqReadTopic.Pos();
        const auto& cluster = pqReadTopic.DataSource().Cluster().StringValue();
        const auto format = pqReadTopic.Format().Ref().Content();
        const auto& settings = pqReadTopic.Settings();
        const auto maybeWatermark = pqReadTopic.Watermark().Maybe<TCoLambda>();

        TVector<TCoNameValueTuple> props;

        if (auto consumer = State_->Configuration->Consumer.Get()) {
            Add(props, ConsumerSetting, *consumer, pos, ctx);
        }

        auto clusterConfiguration = GetClusterConfiguration(cluster);

        Add(props, EndpointSetting, clusterConfiguration->Endpoint, pos, ctx);

        if (clusterConfiguration->AddBearerToToken) {
            Add(props, AddBearerToTokenSetting, "1", pos, ctx);
        }

        bool streamingTopicReadEnabled = State_->StreamingTopicsReadByDefault;
        TMaybe<ui64> watermarksGranularityUs;
        TMaybe<ui64> watermarksIdleTimeoutUs;
        TMaybe<ui64> watermarksLateArrivalDelayUs;
        bool skipJsonErrors = false;
        bool withSharedReading = false;

        for (const auto& setting : settings.Raw()->Children()) {
            const auto settingName = setting->Child(0)->Content();
            if ("skip.json.errors" == settingName || "sharedreadingskipjsonerrors" == settingName) {
                if (setting->ChildrenSize() != 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Expected `SHARED_READING_SKIP_JSON_ERRORS` = value"));
                    return {};
                }
                const auto settingValue = setting->Child(1);
                if (!EnsureAtom(*settingValue, ctx)) {
                    return {};
                }
                if (!TryFromString<bool>(settingValue->Content(), skipJsonErrors)) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "`SHARED_READING_SKIP_JSON_ERRORS` must be boolean type"));
                    return {};
                }
                if (!skipJsonErrors) {
                    continue;
                }
                Add(props, SkipJsonErrors, ToString(skipJsonErrors), pos, ctx);
            } else if ("watermarkgranularity" == settingName) {
                if (setting->ChildrenSize() != 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Expected WATERMARK_GRANULARITY = value"));
                    return {};
                }
                const auto settingValue = setting->Child(1);
                if (!EnsureAtom(*settingValue, ctx)) {
                    return {};
                }
                const auto out = NKikimr::NMiniKQL::ValueFromString(NUdf::EDataSlot::Interval, settingValue->Content());
                if (!out) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()),
                        TStringBuilder() << "Invalid value " << settingValue->Content() << " for WATERMARK_GRANULARITY"));
                    return {};
                }

                watermarksGranularityUs = out.Get<ui64>();
            } else if ("watermarkidletimeout" == settingName) {
                if (setting->ChildrenSize() != 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Expected WATERMARK_IDLE_TIMEOUT = value"));
                    return {};
                }
                const auto settingValue = setting->Child(1);
                if (!EnsureAtom(*settingValue, ctx)) {
                    return {};
                }
                const auto out = NKikimr::NMiniKQL::ValueFromString(NUdf::EDataSlot::Interval, settingValue->Content());
                if (!out) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()),
                        TStringBuilder() << "Invalid value " << settingValue->Content() << " for WATERMARK_IDLE_TIMEOUT"));
                    return {};
                }

                watermarksIdleTimeoutUs = out.Get<ui64>();
            } else if ("streaming" == settingName) {
                if (const auto parseResult = TTopicKeyParser::ParseStreamingTopicRead(*setting, ctx)) {
                    bool withStreamingValue = *parseResult;
                    if (State_->StreamingTopicsReadByDefault && !withStreamingValue) {
                        ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Table topic reading is not supported in streaming query now, please use WITH (STREAMING = \"TRUE\") after topic name to read from topics in streaming mode"));
                        return nullptr;
                    }
                    if (!State_->StreamingTopicsReadByDefault && withStreamingValue) {
                        TIssue tableTopicReadWarning(ctx.GetPosition(pqReadTopic.Pos()), "Streaming topic reading (without checkpoints) use for debugging purposes only");
                        tableTopicReadWarning.Severity = TSeverityIds::S_WARNING;
                        ctx.AddWarning(tableTopicReadWarning);
                    }
                    streamingTopicReadEnabled = withStreamingValue;
                } else {
                    return {};
                }
            } else if ("sharedreading" == settingName) {
                if (setting->ChildrenSize() != 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Expected `SHARED_READING` = value"));
                    return {};
                }
                const auto settingValue = setting->Child(1);
                if (!EnsureAtom(*settingValue, ctx)) {
                    return {};
                }
                if (!TryFromString<bool>(settingValue->Content(), withSharedReading)) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "`SHARED_READING` must be boolean type"));
                    return {};
                }
            }
        }

        bool useSharedReading = UseSharedReading(clusterConfiguration, pqReadTopic, withSharedReading, ctx);
        if (!useSharedReading && skipJsonErrors) {
            ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "`SHARED_READING_SKIP_JSON_ERRORS` is supported only in shared reading mode"));
            return {};
        }

        if (!useSharedReading && maybeWatermark) {
            const auto watermark = maybeWatermark.Cast();

            const auto eventTimeAndDelay = SplitWatermarkExpr(watermark, *State_, ctx);
            if (!eventTimeAndDelay) {
                return {};
            }
            std::tie(std::ignore, watermarksLateArrivalDelayUs) = *eventTimeAndDelay;
        }
        
        Add(props, SharedReading, ToString(useSharedReading), pos, ctx);
        Add(props, ReconnectPeriod, ToString(clusterConfiguration->ReconnectPeriod), pos, ctx);
        Add(props, Format, format, pos, ctx);
        Add(props, ReadGroup, clusterConfiguration->ReadGroup, pos, ctx);
        if (clusterConfiguration->UseSsl) {
            Add(props, UseSslSetting, "1", pos, ctx);
        }
        if (!streamingTopicReadEnabled) {
            ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Finite topic reading is not supported now, please use WITH (STREAMING = \"TRUE\") after topic name to read from topics in streaming mode"));
            return nullptr;
        }
        if (streamingTopicReadEnabled != State_->StreamingTopicsReadByDefault) {
            Add(props, StreamingTopicRead, ToString(streamingTopicReadEnabled), pos, ctx);
        }

        if (State_->Configuration->MaxPartitionReadSkew.Get() && !State_->EnableStreamingPartitionBalancing) {
            ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Streaming partition balancing is disabled. Please contact your system administrator to enable it"));
            return {};
        }

        if (State_->Configuration->MaxPartitionReadSkew.Get() && !streamingTopicReadEnabled) {
            ctx.AddWarning(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Partitions balancing is not supported with table mode. Partitions balancing settings will be ignored"));
        }

        if (State_->Configuration->MaxPartitionReadSkew.Get()) {
            Add(props, PartitionsBalancingIdleTimeoutUsSetting, ToString(watermarksIdleTimeoutUs.GetOrElse(TDuration::Minutes(1).MicroSeconds())), pos, ctx);
        }

        if ((State_->EnableWatermarks || State_->EnableWatermarksAdvanced) && maybeWatermark) {
            Add(props, WatermarksEnableSetting, ToString((State_->EnableWatermarks && !State_->EnableWatermarksAdvanced) || useSharedReading), pos, ctx);
            Add(props, WatermarksGranularityUsSetting,
                ToString(watermarksGranularityUs.GetOrElse(TDuration::MilliSeconds(wrSettings.WatermarksGranularityMs.GetOrElse(TDqSettings::TDefault::WatermarksGranularityMs)).MicroSeconds())), pos, ctx);
            Add(props, WatermarksLateArrivalDelayUsSetting,
                ToString(watermarksLateArrivalDelayUs.GetOrElse(TDuration::MilliSeconds(wrSettings.WatermarksLateArrivalDelayMs.GetOrElse(TDqSettings::TDefault::WatermarksLateArrivalDelayMs)).MicroSeconds())), pos, ctx);

            if (wrSettings.WatermarksEnableIdlePartitions.GetOrElse(true)) {
                if (wrSettings.WatermarksEnableIdlePartitions.Defined() && !watermarksIdleTimeoutUs) {
                    watermarksIdleTimeoutUs = TDuration::MilliSeconds(wrSettings.WatermarksIdleTimeoutMs.GetOrElse(TDqSettings::TDefault::WatermarksIdleTimeoutMs)).MicroSeconds();
                }
                if (watermarksIdleTimeoutUs) {
                    Add(props, WatermarksIdlePartitionsSetting, ToString(true), pos, ctx);
                    Add(props, WatermarksIdleTimeoutUsSetting, ToString(*watermarksIdleTimeoutUs), pos, ctx);
                }
            } else {
                if (watermarksIdleTimeoutUs) {
                    ctx.AddWarning(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "WATERMARK_IDLE_TIMEOUT specified, but watermarks idle partitions explicitly disabled"));
                }
            }
        } else {
            if (maybeWatermark) {
                ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "WATERMARK expression specified, but watermarks are disabled"));
                return {};
            }
            if (watermarksGranularityUs) {
                ctx.AddWarning(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "WATERMARK_GRANULARITY specified, but watermarks are disabled"));
            }
            if (watermarksIdleTimeoutUs) {
                ctx.AddWarning(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "WATERMARK_IDLE_TIMEOUT specified, but watermarks are disabled"));
            }
        }

        if (format == "csv"sv) {
            const auto maybeUserSchema = pqReadTopic.UserSchemaColumns();
            YQL_ENSURE(maybeUserSchema, "PqReadTopic csv: UserSchemaColumns is required");
            TExprNode::TPtr usc = maybeUserSchema.Cast().Ptr();
            YQL_ENSURE(usc->ChildrenSize() > 0);
            YQL_ENSURE(EnsureTupleOfAtoms(*usc, ctx), "PqReadTopic csv: UserSchemaColumns must contain only column name atoms");
            props.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(UserSchemaColumnsSetting)
                .Value(std::move(usc))
                .Done());
        }

        return Build<TCoNameValueTupleList>(ctx, pos)
            .Add(props)
            .Done();
    }

    NNodes::TCoNameValueTupleList BuildDqSourceWrapSettings(const TPqReadTopic& pqReadTopic, TPositionHandle pos, TExprContext& ctx) const {
        TVector<TCoNameValueTuple> settings;
        settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build("format")
            .Value(pqReadTopic.Format())
            .Done());

        TExprNode::TListType metadataFieldsList;
        if (!State_->ForbidYqlSysColumnsAndSystemMetadata) {
            for (const auto& sysColumn : GetAllowedPqMetaSysColumns(
                     State_->AddTransparentPrefixToTransparentSystemColumns,
                     State_->EnableUserAttributesInTopicQuery))
            {
                metadataFieldsList.push_back(ctx.NewAtom(pos, sysColumn));
            }
        }

        // Also add __ydb_-prefixed system columns
        for (const auto& ydbColumn : GetAllowedYdbSysColumns(State_->EnableUserAttributesInTopicQuery)) {
            metadataFieldsList.push_back(ctx.NewAtom(pos, ydbColumn));
        }

        settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build("metadataColumns")
            .Value(ctx.NewList(pos, std::move(metadataFieldsList)))
            .Done());

        // Like S3: UserSchemaColumns in formatSettings — immutable userschema/file column order for csv CH parser (not projection Columns).
        TExprNode::TPtr formatSettingsNode = pqReadTopic.Settings().Ptr();
        if (pqReadTopic.Format().Ref().Content() == TStringBuf("csv")) {
            const auto maybeUserSchema = pqReadTopic.UserSchemaColumns();
            YQL_ENSURE(maybeUserSchema, "PqReadTopic csv: UserSchemaColumns is required");
            TExprNode::TPtr usc = maybeUserSchema.Cast().Ptr();
            YQL_ENSURE(usc->IsList() && !TCoVoid::Match(usc.Get()));
            YQL_ENSURE(usc->ChildrenSize() > 0);
            YQL_ENSURE(EnsureTupleOfAtoms(*usc, ctx), "PqReadTopic csv: UserSchemaColumns must contain only column name atoms");
            TExprNode::TListType merged = formatSettingsNode->ChildrenList();
            merged.push_back(ctx.NewList(pos, {
                ctx.NewAtom(pos, UserSchemaColumnsSetting),
                ctx.NewList(pos, usc->ChildrenList()),
            }));
            formatSettingsNode = ctx.NewList(pos, std::move(merged));
        }

        settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build("formatSettings")
            .Value(std::move(formatSettingsNode))
            .Done());

        TVector<TCoNameValueTuple> innerSettings;
        if (pqReadTopic.Compression() != "") {
            innerSettings.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build("compression")
                .Value(pqReadTopic.Compression())
                .Done());
        }

        const auto clusterConfiguration = GetClusterConfiguration(pqReadTopic.DataSource().Cluster().StringValue());
        
        bool withSharedReading = false;
        const auto& readTopicSettings = pqReadTopic.Settings();
        for (const auto& setting : readTopicSettings.Raw()->Children()) {
            const auto settingName = setting->Child(0)->Content();
            if ("sharedreading" != settingName) {
                continue;
            }
            const auto settingValue = setting->Child(1);
            if (!EnsureAtom(*settingValue, ctx)) {
                continue;
            }
            TryFromString<bool>(settingValue->Content(), withSharedReading);
        }
        Add(innerSettings, SharedReading, ToString(UseSharedReading(clusterConfiguration, pqReadTopic, withSharedReading, ctx)), pos, ctx);

        if (!innerSettings.empty()) {
            settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build("settings")
                .Value<TCoNameValueTupleList>()
                    .Add(innerSettings)
                    .Build()
                .Done());
        }

        return Build<TCoNameValueTupleList>(ctx, pos)
            .Add(settings)
            .Done();
    }

    const TPqClusterConfigurationSettings* GetClusterConfiguration(const TString& cluster) const {
        const auto clusterConfiguration = State_->Configuration->ClustersConfigurationSettings.FindPtr(cluster);
        if (!clusterConfiguration) {
            ythrow yexception() << "Unknown pq cluster \"" << cluster << "\"";
        }
        return clusterConfiguration;
    }

    bool UseSharedReading(const TPqClusterConfigurationSettings* clusterConfiguration, const TPqReadTopic& pqReadTopic, bool withSharedReading, TExprContext& ctx) const {
        std::string_view format = pqReadTopic.Format().Ref().Content();
        const auto& settings = pqReadTopic.Settings();
        bool streamingTopicReadEnabled = State_->StreamingTopicsReadByDefault;

        for (const auto& setting : settings.Raw()->Children()) {
            const auto settingName = setting->Child(0)->Content();
            if ("streaming" != settingName) {
                continue;
            }
            if (const auto parseResult = TTopicKeyParser::ParseStreamingTopicRead(*setting, ctx)) {
                streamingTopicReadEnabled = *parseResult;
            }
        }
        bool useSharedReading = (clusterConfiguration->SharedReading || withSharedReading) && (format == "json_each_row" || format == "raw");
        if (!streamingTopicReadEnabled && useSharedReading) {
            ctx.AddWarning(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Table topic reading is not supported with sharing reading mode. Reading without shared reading will be used."));
            useSharedReading = false;
        }
        return useSharedReading;
    }

    bool FillSourcePlanProperties(const NNodes::TExprBase& node, TMap<TString, NJson::TJsonValue>& properties) override {
        if (!node.Maybe<TDqSource>()) {
            return false;
        }
        auto source = node.Cast<TDqSource>();
        auto settings = source.Settings();
        auto maybeTopicSource = TMaybeNode<TDqPqTopicSource>(settings.Raw());

        if (!maybeTopicSource) {
            return false;
        }
        TDqPqTopicSource topicSource = maybeTopicSource.Cast();

        NYql::NConnector::NApi::TPredicate predicateProto;
        auto serializedProto = topicSource.FilterPredicate().Ref().Content();
        YQL_ENSURE(predicateProto.ParseFromString(serializedProto));
        TString filterPredicateSql = NYql::FormatPredicate(predicateProto);

        NPq::NProto::TOffsetPredicate offsetPredicates;
        auto offsetSerialized = topicSource.OffsetPredicate().Ref().Content();
        YQL_ENSURE(offsetPredicates.ParseFromString(offsetSerialized));

        NPq::NProto::TWriteTimePredicate writeTimePredicate;
        auto writeTimeSerialized = topicSource.WriteTimePredicate().Ref().Content();
        YQL_ENSURE(writeTimePredicate.ParseFromString(writeTimeSerialized));

        std::set<ui64> predicatePartitions;
        bool hasPredicatePartitions = GetPartitions(*topicSource.Partitions().Ptr(), predicatePartitions);

        if (!filterPredicateSql.empty()) {
            properties["Filter (shared reading)"] = filterPredicateSql;
        }
        if (offsetPredicates.ItemSize() > 0) {
            const auto& item = offsetPredicates.GetItem(0);
            properties["Offsets"] = TStringBuilder() << "["
                << (item.HasBegin() ? ToString(item.GetBegin()) : "_") << ", "
                << (item.HasEnd() ? ToString(item.GetEnd()) : "_") << ")";
        }
        if (writeTimePredicate.ItemSize() > 0) {
            const auto& item = writeTimePredicate.GetItem(0);
            properties["WriteTime"] = TStringBuilder() << "["
                << (item.HasBegin() ? ToString(TInstant::MicroSeconds(item.GetBegin())) : "_") << ", "
                << (item.HasEnd() ? ToString(TInstant::MicroSeconds(item.GetEnd())) : "_") << ")";
        }
        if (hasPredicatePartitions && !predicatePartitions.empty()) {
            if (predicatePartitions.size() == 1) {
                properties["Partitions"] = TStringBuilder() << "[" << ToString(*predicatePartitions.begin()) << "]";
            } else {
                properties["Partitions"] = TStringBuilder() << "["
                    << ToString(*predicatePartitions.begin())
                    << ((predicatePartitions.size() > 2) ? ", ..., " : ", ")
                    << ToString(*predicatePartitions.rbegin()) << "]";
            }
        }
        return true;
    }

private:
    TPqState* State_; // State owns dq integration, so back reference must be not smart.
};

} // anonymous namespace

THolder<IDqIntegration> CreatePqDqIntegration(const TPqState::TPtr& state) {
    return MakeHolder<TPqDqIntegration>(state);
}

} // namespace NYql
