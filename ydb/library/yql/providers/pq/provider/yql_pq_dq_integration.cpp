#include "yql_pq_dq_integration.h"
#include "yql_pq_helpers.h"
#include "yql_pq_mkql_compiler.h"
#include "yql_pq_topic_key_parser.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
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
#include <yql/essentials/providers/common/dq/yql_dq_integration_impl.h>
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <util/string/builder.h>

namespace NYql {

using namespace NNodes;

namespace {

class TPqDqIntegration : public TDqIntegrationBase {
public:
    explicit TPqDqIntegration(const TPqState::TPtr& state)
        : State_(state.Get())
    {}

    ui64 PartitionTopicRead(const TPqTopic& topic, size_t maxPartitions, TVector<TString>& partitions) {
        size_t topicPartitionsCount = 0;
        for (auto kv : topic.Props()) {
            auto key = kv.Name().Value();
            if (key == PartitionsCountProp) {
                topicPartitionsCount = FromString(kv.Value().Ref().Content());
            }
        }
        YQL_ENSURE(topicPartitionsCount > 0);

        const size_t tasks = Min(maxPartitions, topicPartitionsCount);
        partitions.reserve(tasks);
        for (size_t i = 0; i < tasks; ++i) {
            NPq::NProto::TDqReadTaskParams params;
            auto* partitioningParams = params.MutablePartitioningParams();
            partitioningParams->SetTopicPartitionsCount(topicPartitionsCount);
            partitioningParams->SetEachTopicPartitionGroupId(i);
            partitioningParams->SetDqPartitionsCount(tasks);
            YQL_CLOG(DEBUG, ProviderPq) << "Create DQ reading partition " << params;

            TString serializedParams;
            YQL_ENSURE(params.SerializeToString(&serializedParams));
            partitions.emplace_back(std::move(serializedParams));
        }
        return 0;
    }

    ui64 Partition(const TExprNode& node, TVector<TString>& partitions, TString*, TExprContext&, const TPartitionSettings& settings) override {
        if (auto maybePqRead = TMaybeNode<TPqReadTopic>(&node)) {
            return PartitionTopicRead(maybePqRead.Cast().Topic(), settings.MaxPartitions, partitions);
        }
        if (auto maybeDqSource = TMaybeNode<TDqSource>(&node)) {
            auto srcSettings = maybeDqSource.Cast().Settings();
            if (auto topicSource = TMaybeNode<TDqPqTopicSource>(srcSettings.Raw())) {
                return PartitionTopicRead(topicSource.Cast().Topic(), settings.MaxPartitions, partitions);
            }
        }
        return 0;
    }

    TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx, const TWrapReadSettings& wrSettings) override {
        if (const auto& maybePqReadTopic = TMaybeNode<TPqReadTopic>(read)) {
            const auto& pqReadTopic = maybePqReadTopic.Cast();
            YQL_ENSURE(pqReadTopic.Ref().GetTypeAnn(), "No type annotation for node " << pqReadTopic.Ref().Content());

            const auto rowType = pqReadTopic.Ref().GetTypeAnn()
                ->Cast<TTupleExprType>()->GetItems().back()->Cast<TListExprType>()
                ->GetItemType()->Cast<TStructExprType>();
            const auto& clusterName = pqReadTopic.DataSource().Cluster().StringValue();
            const auto token = "cluster:default_" + clusterName;

            const auto& typeItems = pqReadTopic.Topic().RowSpec().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>()->GetItems();
            const auto pos = read->Pos();

            TExprNode::TListType colNames;
            colNames.reserve(typeItems.size());
            std::transform(typeItems.cbegin(), typeItems.cend(), std::back_inserter(colNames),
                [&](const TItemExprType* item) {
                    return ctx.NewAtom(pos, item->GetName());
                });
            auto columnNames = ctx.NewList(pos, std::move(colNames));

            auto settings = BuildTopicReadSettings(pqReadTopic, ctx, wrSettings);
            if (!settings) {
                return {};
            }

            TString serializedWatermarkExpr;
            if (const auto maybeWatermark = pqReadTopic.Watermark()) {
                const auto watermark = maybeWatermark.Cast();

                TStringBuilder err;
                NYql::NConnector::NApi::TExpression watermarkExprProto;
                if (!NYql::SerializeWatermarkExpr(ctx, watermark, &watermarkExprProto, err)) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Failed to serialize Watermark Expr to proto: " + err));
                    return {};
                }
                if (!watermarkExprProto.SerializeToString(&serializedWatermarkExpr)) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Failed to serialize Watermark Expr to string"));
                    return {};
                }
            }

            return Build<TDqSourceWrap>(ctx, pos)
                .Input<TDqPqTopicSource>()
                    .World(pqReadTopic.World())
                    .Topic(pqReadTopic.Topic())
                    .Columns(std::move(columnNames))
                    .Settings(std::move(settings))
                    .Token<TCoSecureParam>()
                        .Name().Build(token)
                        .Build()
                    .FilterPredicate().Value(TString()).Build()  // Empty predicate by default <=> WHERE TRUE
                    .RowType(ExpandType(pqReadTopic.Pos(), *rowType, ctx))
                    .Watermark().Value(serializedWatermarkExpr).Build()
                    .Build()
                .RowType(ExpandType(pqReadTopic.Pos(), *rowType, ctx))
                .DataSource(pqReadTopic.DataSource().Cast<TCoDataSource>())
                .Settings(BuildDqSourceWrapSettings(pqReadTopic, pos, ctx))
                .Done().Ptr();
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
                        srcDesc.MutableWatermarks()->SetEnabled(true);
                    } else if (name == WatermarksGranularityUsSetting) {
                        srcDesc.MutableWatermarks()->SetGranularityUs(FromString<ui64>(Value(setting)));
                    } else if (name == WatermarksLateArrivalDelayUsSetting) {
                        srcDesc.MutableWatermarks()->SetLateArrivalDelayUs(FromString<ui64>(Value(setting)));
                    } else if (name == WatermarksIdleTimeoutUsSetting) {
                        srcDesc.MutableWatermarks()->SetIdleTimeoutUs(FromString<ui64>(Value(setting)));
                    } else if (name == WatermarksIdlePartitionsSetting) {
                        srcDesc.MutableWatermarks()->SetIdlePartitionsEnabled(true);
                    } else if (name == SkipJsonErrors) {
                        skipErrors = FromString<bool>(Value(setting));
                    } else if (name == StreamingTopicRead) {
                        streamingTopicRead = FromString<bool>(Value(setting));
                    } else if (name == PartitionsBalancingIdleTimeoutUsSetting) {
                        *srcDesc.MutablePartitionsBalancingIdleTimeout() = NProtoInterop::CastToProto(TDuration::MicroSeconds(FromString<ui64>(Value(setting))));
                    }
                }

                YQL_ENSURE(streamingTopicRead, "Finite topic reading is not supported");

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
                    srcDesc.AddMetadataFields(metadata.Value().Maybe<TCoAtom>().Cast().StringValue());
                }

                const auto rowSchema = topic.RowSpec().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                for (const auto& item : rowSchema->GetItems()) {
                    srcDesc.AddColumns(TString(item->GetName()));
                    srcDesc.AddColumnTypes(NCommon::WriteTypeToYson(item->GetItemType(), NYT::NYson::EYsonFormat::Text));
                }

                NYql::NConnector::NApi::TPredicate predicateProto;
                auto serializedProto = topicSource.FilterPredicate().Ref().Content();
                YQL_ENSURE(predicateProto.ParseFromString(serializedProto));

                TString predicateSql = NYql::FormatPredicate(predicateProto);
                if (sharedReading) {
                    srcDesc.SetPredicate(predicateSql);
                    srcDesc.SetSharedReading(true);
                }
                srcDesc.SetSkipJsonErrors(skipErrors);

                *srcDesc.MutableDisposition() = State_->Disposition;

                for (const auto& [label, value] : State_->TaskSensorLabels) {
                    auto taskSensorLabel = srcDesc.AddTaskSensorLabel();
                    taskSensorLabel->SetLabel(label);
                    taskSensorLabel->SetValue(value);
                }
                for (auto nodeId : State_->NodeIds) {
                    srcDesc.AddNodeIds(nodeId);
                }

                NYql::NConnector::NApi::TExpression watermarkExprProto;
                auto serializedWatermarkExpr = topicSource.Watermark().Ref().Content();
                YQL_ENSURE(watermarkExprProto.ParseFromString(serializedWatermarkExpr));
                TString watermarkExprSql = NYql::FormatExpression(watermarkExprProto);
                srcDesc.SetWatermarkExpr(watermarkExprSql);

                if (commonSettings) {
                    commonSettings->MutableSettings()->PackFrom(srcDesc);
                    protoSettings.PackFrom(*commonSettings);
                } else {
                    protoSettings.PackFrom(srcDesc);
                }

                if (sharedReading && !predicateSql.empty()) {
                    ctx.AddWarning(TIssue(ctx.GetPosition(node.Pos()), "Row dispatcher will use the predicate: " + predicateSql));
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

private:
    // Extract watermark delay from fixed-format expression:
    // WITH ( ...
    //   WATERMARK = (SystemMetadata('write_time') - Interval('PT5S'))
    // Only used (and useful) for non-shared-reading pq source
    // (in this case, flexible watermark expression is not implented)
    static TMaybe<ui64> ExtractWatermarkDelay(const TCoLambda& watermark) {
        if (watermark.Args().Size() != 1) {
            return Nothing();
        }
        const auto arg = watermark.Args().Arg(0);
        const auto body = watermark.Body();
        const auto maybeSub = body.Maybe<TCoSub>();
        if (!maybeSub) {
            return Nothing();
        }
        const auto sub = maybeSub.Cast();
        {
            const auto maybeMember = sub.Left().Maybe<TCoMember>();
            if (!maybeMember) {
                return Nothing();
            }
            const auto member = maybeMember.Cast();
            if (const auto& maybeArg = member.Struct().Maybe<TCoArgument>()) {
                if (maybeArg.Cast().Name() != arg.Name()) {
                    return Nothing();
                }
            }
            if (!IsIn({"_yql_sys_tsp_write_time", "_yql_sys_write_time"}, member.Name())) {
                return Nothing();
            }
        }
        {
            auto maybeInterval = sub.Right().Maybe<TCoInterval>();
            if (!maybeInterval) {
                return Nothing();
            }
            auto interval = maybeInterval.Cast();
            return TryFromString<ui64>(interval.Literal().Value());
        }
    }

public:
    TExprNode::TPtr BuildTopicReadSettings(
        const TPqReadTopic& pqReadTopic,
        TExprContext& ctx,
        const IDqIntegration::TWrapReadSettings& wrSettings
    ) const {
        const auto pos = pqReadTopic.Pos();
        const auto& cluster = pqReadTopic.DataSource().Cluster().StringValue();
        const auto format = pqReadTopic.Format().Ref().Content();
        const auto& settings = pqReadTopic.Settings();
        const auto maybeWatermark = pqReadTopic.Watermark();

        TVector<TCoNameValueTuple> props;

        if (auto consumer = State_->Configuration->Consumer.Get()) {
            Add(props, ConsumerSetting, *consumer, pos, ctx);
        }

        auto clusterConfiguration = GetClusterConfiguration(cluster);

        Add(props, EndpointSetting, clusterConfiguration->Endpoint, pos, ctx);
        const bool useSharedReading = UseSharedReading(clusterConfiguration, format);
        Add(props, SharedReading, ToString(useSharedReading), pos, ctx);
        Add(props, ReconnectPeriod, ToString(clusterConfiguration->ReconnectPeriod), pos, ctx);
        Add(props, Format, format, pos, ctx);
        Add(props, ReadGroup, clusterConfiguration->ReadGroup, pos, ctx);

        if (clusterConfiguration->UseSsl) {
            Add(props, UseSslSetting, "1", pos, ctx);
        }

        if (clusterConfiguration->AddBearerToToken) {
            Add(props, AddBearerToTokenSetting, "1", pos, ctx);
        }

        bool streamingTopicReadEnabled = State_->StreamingTopicsReadByDefault;
        TMaybe<TString> watermarksLateEventsPolicy;
        TMaybe<ui64> watermarksGranularityUs;
        TMaybe<ui64> watermarksIdleTimeoutUs;
        TMaybe<ui64> watermarksLateArrivalDelayUs;
        if (!useSharedReading && maybeWatermark) {
            watermarksLateArrivalDelayUs = ExtractWatermarkDelay(maybeWatermark.Cast());
            if (!watermarksLateArrivalDelayUs) {
                ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Unrecognized watermark expression, flexible watermark expressions are only implemented in shared reading mode, please use WATERMARK = (SystemMetadata('write_time') - Interval('PT5S'))"));
                return {};
            }
        }
        for (const auto& setting : settings.Raw()->Children()) {
            const auto settingName = setting->Child(0)->Content();
            if ("skip.json.errors" == settingName) {
                if (setting->ChildrenSize() != 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Expected `skip.json.errors` = value"));
                    return {};
                }
                const auto settingValue = setting->Child(1);
                if (!EnsureAtom(*settingValue, ctx)) {
                    return {};
                }
                bool skipJsonErrors = true;
                if (!TryFromString<bool>(settingValue->Content(), skipJsonErrors)) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "`skip.json.errors` must be boolean type"));
                    return {};
                }
                if (!skipJsonErrors) {
                    continue;
                }
                if (!useSharedReading) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "`skip.json.errors` is supported only in shared reading mode"));
                    return {};
                }

                Add(props, SkipJsonErrors, ToString(skipJsonErrors), pos, ctx);
            } else if ("watermarkadjustlateevents" == settingName) {
                if (setting->ChildrenSize() > 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Expected WATERMARK_ADJUST_LATE_EVENTS (= false|true)"));
                    return {};
                }
                bool watermarkAdjustLateEvents = true;
                if (setting->ChildrenSize() == 2) {
                    const auto settingValue = setting->Child(1);
                    if (!EnsureAtom(*settingValue, ctx)) {
                        return {};
                    }
                    if (!TryFromString<bool>(settingValue->Content(), watermarkAdjustLateEvents)) {
                        ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "WATERMARK_ADJUST_LATE_EVENTS must be boolean type"));
                        return {};
                    }
                }
                if (!watermarkAdjustLateEvents) {
                    continue;
                }
                if (!watermarksLateEventsPolicy.Empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()),
                        TStringBuilder() << "Cannot adjust and " << *watermarksLateEventsPolicy << " late events at the same time"));
                    return {};
                }

                watermarksLateEventsPolicy = "adjust";
            } else if ("watermarkdroplateevents" == settingName) {
                if (setting->ChildrenSize() > 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Expected WATERMARK_DROP_LATE_EVENTS (= false|true)"));
                    return {};
                }
                bool watermarkDropLateEvents = true;
                if (setting->ChildrenSize() == 2) {
                    const auto settingValue = setting->Child(1);
                    if (!EnsureAtom(*settingValue, ctx)) {
                        return {};
                    }
                    if (!TryFromString<bool>(settingValue->Content(), watermarkDropLateEvents)) {
                        ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "WATERMARK_DROP_LATE_EVENTS must be boolean type"));
                        return {};
                    }
                }
                if (!watermarkDropLateEvents) {
                    continue;
                }
                if (!watermarksLateEventsPolicy.Empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()),
                        TStringBuilder() << "Cannot drop and " << *watermarksLateEventsPolicy << " late events at the same time"));
                    return {};
                }

                watermarksLateEventsPolicy = "drop";
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
                    streamingTopicReadEnabled = *parseResult;
                    Add(props, StreamingTopicRead, ToString(*parseResult), pos, ctx);
                } else {
                    return {};
                }
            }
        }

        if (!streamingTopicReadEnabled) {
            ctx.AddError(TIssue(ctx.GetPosition(pqReadTopic.Pos()), "Finite topic reading is not supported now, please use WITH (STREAMING = \"TRUE\") after topic name to read from topics in streaming mode"));
            return nullptr;
        }

        if (State_->Configuration->MaxPartitionReadSkew.Get()) {
            Add(props, PartitionsBalancingIdleTimeoutUsSetting, ToString(watermarksIdleTimeoutUs.GetOrElse(TDuration::Minutes(1).MicroSeconds())), pos, ctx);
        }

        if (wrSettings.WatermarksMode.GetOrElse("") == "default" && maybeWatermark) {
            Add(props, WatermarksEnableSetting, ToString(true), pos, ctx);
            Add(props, WatermarksGranularityUsSetting,
                ToString(watermarksGranularityUs.GetOrElse(TDuration::MilliSeconds(wrSettings.WatermarksGranularityMs.GetOrElse(TDqSettings::TDefault::WatermarksGranularityMs)).MicroSeconds())), pos, ctx);
            Add(props, WatermarksLateArrivalDelayUsSetting,
                ToString(watermarksLateArrivalDelayUs.GetOrElse(TDuration::MilliSeconds(wrSettings.WatermarksLateArrivalDelayMs.GetOrElse(TDqSettings::TDefault::WatermarksLateArrivalDelayMs)).MicroSeconds())), pos, ctx);

            const auto lateEventsPolicy = watermarksLateEventsPolicy
                .GetOrElse("adjust");
            Add(props, WatermarksLateEventsPolicySetting, lateEventsPolicy, pos, ctx);

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

        return Build<TCoNameValueTupleList>(ctx, pos)
            .Add(props)
            .Done().Ptr();
    }

    NNodes::TCoNameValueTupleList BuildDqSourceWrapSettings(const TPqReadTopic& pqReadTopic, TPositionHandle pos, TExprContext& ctx) const {
        TVector<TCoNameValueTuple> settings;
        settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build("format")
            .Value(pqReadTopic.Format())
            .Done());

        TExprNode::TListType metadataFieldsList;
        for (const auto& sysColumn : AllowedPqMetaSysColumns(State_->AllowTransparentSystemColumns)) {
            metadataFieldsList.push_back(ctx.NewAtom(pos, sysColumn));
        }

        settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build("metadataColumns")
            .Value(ctx.NewList(pos, std::move(metadataFieldsList)))
            .Done());

        settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build("formatSettings")
            .Value(std::move(pqReadTopic.Settings()))
            .Done());

        TVector<TCoNameValueTuple> innerSettings;
        if (pqReadTopic.Compression() != "") {
            innerSettings.push_back(Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build("compression")
                .Value(pqReadTopic.Compression())
                .Done());
        }

        const auto clusterConfiguration = GetClusterConfiguration(pqReadTopic.DataSource().Cluster().StringValue());
        Add(innerSettings, SharedReading, ToString(UseSharedReading(clusterConfiguration, pqReadTopic.Format().Ref().Content())), pos, ctx);

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

    static bool UseSharedReading(const TPqClusterConfigurationSettings* clusterConfiguration, std::string_view format) {
        return clusterConfiguration->SharedReading && (format == "json_each_row" || format == "raw");
    }

private:
    TPqState* State_; // State owns dq integration, so back reference must be not smart.
};

} // anonymous namespace

THolder<IDqIntegration> CreatePqDqIntegration(const TPqState::TPtr& state) {
    return MakeHolder<TPqDqIntegration>(state);
}

} // namespace NYql
