#include "yql_solomon_dq_integration.h"
#include "yql_solomon_mkql_compiler.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/protos/actors.pb.h>
#include <yql/essentials/ast/yql_expr.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/providers/common/dq/yql_dq_integration_impl.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/actors/dq_solomon_metrics_queue.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/client/solomon_accessor_client.h>

#include <util/string/builder.h>

namespace NYql {

using namespace NNodes;

namespace {

bool ExtractSettingValue(const TExprNode& value, TStringBuf settingName, TExprContext& ctx, TStringBuf& settingValue) {
    if (value.IsAtom()) {
        settingValue = value.Content();
        return true;
    }

    if (!value.IsCallable({ "String", "Utf8" })) {
        ctx.AddError(TIssue(ctx.GetPosition(value.Pos()), TStringBuilder() << settingName << " must be literal value"));
        return false;
    }
    settingValue = value.Head().Content();
    return true;
}

NSo::NProto::ESolomonClusterType MapClusterType(TSolomonClusterConfig::ESolomonClusterType clusterType) {
    switch (clusterType) {
        case TSolomonClusterConfig::SCT_SOLOMON:
            return NSo::NProto::ESolomonClusterType::CT_SOLOMON;
        case TSolomonClusterConfig::SCT_MONITORING:
            return NSo::NProto::ESolomonClusterType::CT_MONITORING;
        default:
            YQL_ENSURE(false, "Invalid cluster type " << ToString<ui32>(clusterType));
    }
}

void FillScheme(const TTypeAnnotationNode& itemType, NSo::NProto::TDqSolomonShardScheme& scheme) {
    int index = 0;
    for (const TItemExprType* structItem : itemType.Cast<TStructExprType>()->GetItems()) {
        const auto itemName = structItem->GetName();
        const TDataExprType* itemType = nullptr;

        bool isOptionalUnused = false;
        YQL_ENSURE(IsDataOrOptionalOfData(structItem->GetItemType(), isOptionalUnused, itemType), "Failed to unwrap optional type");

        const auto dataType = NUdf::GetDataTypeInfo(itemType->GetSlot());

        NSo::NProto::TDqSolomonSchemeItem schemeItem;
        schemeItem.SetKey(TString(itemName));
        schemeItem.SetIndex(index++);
        schemeItem.SetDataTypeId(dataType.TypeId);

        if (dataType.Features & NUdf::DateType || dataType.Features & NUdf::TzDateType) {
            *scheme.MutableTimestamp() = std::move(schemeItem);
        } else if (dataType.Features & NUdf::NumericType) {
            scheme.MutableSensors()->Add(std::move(schemeItem));
        } else if (dataType.Features & NUdf::StringType) {
            scheme.MutableLabels()->Add(std::move(schemeItem));
        } else {
            YQL_ENSURE(false, "Invalid data type for monitoring sink: " << dataType.Name);
        }
    }
}

class TSolomonDqIntegration: public TDqIntegrationBase {
public:
    explicit TSolomonDqIntegration(const TSolomonState::TPtr& state)
        : State_(state.Get())
    {
    }

    ui64 Partition(const TExprNode& node, TVector<TString>& partitions, TString*, TExprContext&, const TPartitionSettings& settings) override {
        const TDqSource dqSource(&node);

        if (const auto maybeSettings = dqSource.Settings().Maybe<TSoSourceSettings>()) {
            const auto soSourceSettings = maybeSettings.Cast();
            if (!soSourceSettings.Selectors().StringValue().empty()) {
                for (size_t i = 0; i < settings.MaxPartitions; ++i) {
                    partitions.push_back(TStringBuilder() << "partition" << i);
                }
                return 0;
            }
        }

        partitions.push_back("partition");
        return 0;
    }

    bool CanRead(const TExprNode& read, TExprContext&, bool) override {
        return TSoReadObject::Match(&read);
    }

    TMaybe<ui64> EstimateReadSize(ui64 /*dataSizePerJob*/, ui32 /*maxTasksPerStage*/, const TVector<const TExprNode*>& read, TExprContext&) override {
        if (AllOf(read, [](const auto val) { return TSoReadObject::Match(val); })) {
            return 0ul; // TODO: return real size
        }
        return Nothing();
    }

    TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx, const TWrapReadSettings&) override {
        if (const auto& maybeSoReadObject = TMaybeNode<TSoReadObject>(read)) {
            const auto& soReadObject = maybeSoReadObject.Cast();
            YQL_ENSURE(soReadObject.Ref().GetTypeAnn(), "No type annotation for node " << soReadObject.Ref().Content());

            const auto& clusterName = soReadObject.DataSource().Cluster().StringValue();

            const auto token = "cluster:default_" + clusterName;
            YQL_CLOG(INFO, ProviderSolomon) << "Wrap " << read->Content() << " with token: " << token;

            auto settings = soReadObject.Object().Settings();
            auto& settingsRef = settings.Ref();
            TInstant from = TInstant::Now() - TDuration::Hours(1);
            TInstant to = TInstant::Now();
            TString program;
            TString selectors;
            std::optional<bool> downsamplingDisabled;
            std::optional<TString> downsamplingAggregation;
            std::optional<TString> downsamplingFill;
            std::optional<ui32> downsamplingGridSec;

            for (auto i = 0U; i < settingsRef.ChildrenSize(); ++i) {
                if (settingsRef.Child(i)->Head().IsAtom("from"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settingsRef.Child(i)->Tail(), settingsRef.Child(i)->Head().Content(), ctx, value)) {
                        return {};
                    }
                    if (!TInstant::TryParseIso8601(value, from)) {
                        ctx.AddError(TIssue(ctx.GetPosition(settingsRef.Child(i)->Head().Pos()), "couldn't parse `from`, use Iso8601 format, e.g. 2025-03-12T14:40:39Z"));
                        return {};
                    }
                    continue;
                }
                if (settingsRef.Child(i)->Head().IsAtom("to"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settingsRef.Child(i)->Tail(), settingsRef.Child(i)->Head().Content(), ctx, value)) {
                        return {};
                    }
                    if (!TInstant::TryParseIso8601(value, to)) {
                        ctx.AddError(TIssue(ctx.GetPosition(settingsRef.Child(i)->Head().Pos()), "couldn't parse `to`, use Iso8601 format, e.g. 2025-03-12T14:40:39Z"));
                        return {};
                    }
                    continue;
                }
                if (settingsRef.Child(i)->Head().IsAtom("program"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settingsRef.Child(i)->Tail(), settingsRef.Child(i)->Head().Content(), ctx, value)) {
                        return {};
                    }

                    program = value;
                    continue;
                }
                if (settingsRef.Child(i)->Head().IsAtom("selectors"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settingsRef.Child(i)->Tail(), settingsRef.Child(i)->Head().Content(), ctx, value)) {
                        return {};
                    }

                    selectors = value;
                    continue;
                }
                if (settingsRef.Child(i)->Head().IsAtom("downsampling.disabled"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settingsRef.Child(i)->Tail(), settingsRef.Child(i)->Head().Content(), ctx, value)) {
                        return {};
                    }
                    bool boolValue;
                    if (!TryFromString<bool>(value, boolValue)) {
                        ctx.AddError(TIssue(ctx.GetPosition(settingsRef.Child(i)->Head().Pos()), TStringBuilder() << "downsampling.disabled must be true or false, but has " << value));
                        return {};
                    }

                    downsamplingDisabled = boolValue;
                    continue;
                }
                if (settingsRef.Child(i)->Head().IsAtom("downsampling.aggregation"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settingsRef.Child(i)->Tail(), settingsRef.Child(i)->Head().Content(), ctx, value)) {
                        return {};
                    }
                    if (!IsIn({ "AVG"sv, "COUNT"sv, "DEFAULT_AGGREGATION"sv, "LAST"sv, "MAX"sv, "MIN"sv, "SUM"sv }, value)) {
                        ctx.AddError(TIssue(ctx.GetPosition(settingsRef.Child(i)->Head().Pos()), TStringBuilder() << "downsampling.aggregation must be one of AVG, COUNT, DEFAULT_AGGREGATION, LAST, MAX, MIN, SUM, but has " << value));
                        return {};
                    }
                    downsamplingAggregation = value;
                    continue;
                }
                if (settingsRef.Child(i)->Head().IsAtom("downsampling.fill"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settingsRef.Child(i)->Tail(), settingsRef.Child(i)->Head().Content(), ctx, value)) {
                        return {};
                    }
                    if (!IsIn({ "NONE"sv, "NULL"sv, "PREVIOUS"sv }, value)) {
                        ctx.AddError(TIssue(ctx.GetPosition(settingsRef.Child(i)->Head().Pos()), TStringBuilder() << "downsampling.fill must be one of NONE, NULL, PREVIOUS, but has " << value));
                        return {};
                    }
                    downsamplingFill = value;
                    continue;
                }
                if (settingsRef.Child(i)->Head().IsAtom("downsampling.gridinterval"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settingsRef.Child(i)->Tail(), "downsampling.grid_interval"sv, ctx, value)) {
                        return {};
                    }
                    ui32 intValue = 0;
                    if (!TryFromString(value, intValue)) {
                        ctx.AddError(TIssue(ctx.GetPosition(settingsRef.Child(i)->Head().Pos()), TStringBuilder() << "downsampling.grid_interval must be positive number, but has " << value));
                        return {};
                    }
                    downsamplingGridSec = intValue;
                    continue;
                }

                ctx.AddError(TIssue(ctx.GetPosition(settingsRef.Child(i)->Head().Pos()), TStringBuilder() << "Unknown setting " << settingsRef.Child(i)->Head().Content()));
                return {};
            }

            if (downsamplingDisabled.has_value() && *downsamplingDisabled) {
                if (downsamplingAggregation || downsamplingFill || downsamplingGridSec) {
                    ctx.AddError(TIssue(ctx.GetPosition(settingsRef.Pos()), "downsampling.disabled must be false if downsampling.aggregation, downsampling.fill or downsamplig.grid_interval is specified"));
                    return {};
                }
            } else {
                downsamplingDisabled = false;
                if (!downsamplingAggregation) {
                    downsamplingAggregation = "AVG";
                }
                if (!downsamplingFill) {
                    downsamplingFill = "PREVIOUS";
                }
                if (!downsamplingGridSec) {
                    downsamplingGridSec = 15;
                }
            }

            return Build<TDqSourceWrap>(ctx, read->Pos())
                .Input<TSoSourceSettings>()
                    .World(soReadObject.World())
                    .Project(soReadObject.Object().Project())
                    .Token<TCoSecureParam>()
                        .Name().Build(token)
                        .Build()
                    .RowType(soReadObject.RowType())
                    .SystemColumns(soReadObject.SystemColumns())
                    .LabelNames(soReadObject.LabelNames())
                    .RequiredLabelNames(soReadObject.RequiredLabelNames())
                    .From<TCoAtom>().Build(from.ToStringUpToSeconds())
                    .To<TCoAtom>().Build(to.ToStringUpToSeconds())
                    .Selectors<TCoAtom>().Build(selectors)
                    .Program<TCoAtom>().Build(program)
                    .DownsamplingDisabled<TCoBool>().Literal().Build(*downsamplingDisabled ? "true" : "false").Build()
                    .DownsamplingAggregation<TCoAtom>().Build(downsamplingAggregation ? *downsamplingAggregation : "")
                    .DownsamplingFill<TCoAtom>().Build(downsamplingFill ? *downsamplingFill : "")
                    .DownsamplingGridSec<TCoUint32>().Literal().Build(ToString(downsamplingGridSec ? *downsamplingGridSec : 0)).Build()
                    .Build()
                .DataSource(soReadObject.DataSource().Cast<TCoDataSource>())
                .RowType(soReadObject.RowType())
                .Settings(settings)
                .Done().Ptr();
        }
        return read;
    }

    TMaybe<bool> CanWrite(const TExprNode& write, TExprContext&) override {
        return TSoWrite::Match(&write);
    }

    void FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sourceType, size_t maxTasksPerStage, TExprContext&) override {
        const TDqSource dqSource(&node);
        const auto maybeSettings = dqSource.Settings().Maybe<TSoSourceSettings>();
        if (!maybeSettings) {
            return;
        }

        const auto settings = maybeSettings.Cast();
        const auto& cluster = dqSource.DataSource().Cast<TSoDataSource>().Cluster().StringValue();
        const auto* clusterDesc = State_->Configuration->ClusterConfigs.FindPtr(cluster);
        YQL_ENSURE(clusterDesc, "Unknown cluster " << cluster);
        NSo::NProto::TDqSolomonSource source;
        source.SetEndpoint(clusterDesc->GetCluster());
        source.SetProject(settings.Project().StringValue());

        source.SetClusterType(MapClusterType(clusterDesc->GetClusterType()));
        source.SetUseSsl(clusterDesc->GetUseSsl());
        source.SetFrom(TInstant::ParseIso8601(settings.From().StringValue()).Seconds());
        source.SetTo(TInstant::ParseIso8601(settings.To().StringValue()).Seconds());

        auto& sourceSettings = *source.MutableSettings();

        for (const auto& attr : clusterDesc->settings()) {
            sourceSettings.insert({ attr.name(), attr.value() });
        }
        
        auto selectors = settings.Selectors().StringValue();
        if (!selectors.empty()) {
            source.SetSelectors(selectors);
        }

        auto program = settings.Program().StringValue();
        if (!program.empty()) {
            source.SetProgram(program);
        }

        auto& downsampling = *source.MutableDownsampling();
        const bool isDisabled = FromString<bool>(settings.DownsamplingDisabled().Literal().Value());
        downsampling.SetDisabled(isDisabled);
        downsampling.SetAggregation(settings.DownsamplingAggregation().StringValue());
        downsampling.SetFill(settings.DownsamplingFill().StringValue());
        const ui32 gridIntervalSec = FromString<ui32>(settings.DownsamplingGridSec().Literal().Value());
        downsampling.SetGridMs(gridIntervalSec * 1000);

        source.MutableToken()->SetName(settings.Token().Name().StringValue());

        THashSet<TString> uniqueColumns;
        for (const auto& c : settings.SystemColumns()) {
            const auto& columnAsString = c.StringValue();
            uniqueColumns.insert(columnAsString);
            source.AddSystemColumns(columnAsString);
        }

        for (const auto& c : settings.LabelNames()) {
            const auto& columnAsString = c.StringValue();
            if (!uniqueColumns.insert(columnAsString).second) {
                throw yexception() << "Column " << columnAsString << " already registered";
            }
            source.AddLabelNames(columnAsString);
        }

        for (const auto& c : settings.RequiredLabelNames()) {
            const auto& labelAsString = c.StringValue();
            source.AddRequiredLabelNames(labelAsString);
        }

        auto& solomonSettings = State_->Configuration;

        auto metricsQueuePageSize = solomonSettings->MetricsQueuePageSize.Get().OrElse(5000);
        sourceSettings.insert({"metricsQueuePageSize", ToString(metricsQueuePageSize)});

        auto metricsQueuePrefetchSize = solomonSettings->MetricsQueuePrefetchSize.Get().OrElse(10000);
        sourceSettings.insert({"metricsQueuePrefetchSize", ToString(metricsQueuePrefetchSize)});

        auto metricsQueueBatchCountLimit = solomonSettings->MetricsQueueBatchCountLimit.Get().OrElse(250);
        sourceSettings.insert({"metricsQueueBatchCountLimit", ToString(metricsQueueBatchCountLimit)});

        auto solomonClientDefaultReplica = solomonSettings->SolomonClientDefaultReplica.Get().OrElse("sas");
        sourceSettings.insert({"solomonClientDefaultReplica", ToString(solomonClientDefaultReplica)});

        auto computeActorBatchSize = solomonSettings->ComputeActorBatchSize.Get().OrElse(1000);
        sourceSettings.insert({"computeActorBatchSize", ToString(computeActorBatchSize)});

        if (!selectors.empty()) {
            auto providerFactory = CreateCredentialsProviderFactoryForStructuredToken(State_->CredentialsFactory, State_->Configuration->Tokens.at(cluster));
            auto credentialsProvider = providerFactory->CreateProvider();
            
            NDq::TDqSolomonReadParams readParams{ .Source = source };

            auto metricsQueueActor = NActors::TActivationContext::ActorSystem()->Register(
                NDq::CreateSolomonMetricsQueueActor(
                    maxTasksPerStage,
                    readParams,
                    credentialsProvider
                ),
                NActors::TMailboxType::HTSwap,
                State_->ExecutorPoolId
            );

            NActorsProto::TActorId protoId;
            ActorIdToProto(metricsQueueActor, &protoId);
            TString stringId;
            google::protobuf::TextFormat::PrintToString(protoId, &stringId);

            source.MutableSettings()->insert({"metricsQueueActor", stringId});
        }

        protoSettings.PackFrom(source);
        sourceType = "SolomonSource";
    }

    void FillSinkSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sinkType) override {
        const auto maybeDqSink = TMaybeNode<TDqSink>(&node);
        if (!maybeDqSink) {
            return;
        }
        const auto dqSink = maybeDqSink.Cast();

        const auto settings = dqSink.Settings();
        const auto maybeShard = TMaybeNode<TSoShard>(settings.Raw());
        if (!maybeShard) {
            return;
        }

        const TSoShard shard = maybeShard.Cast();

        const auto solomonCluster = shard.SolomonCluster().StringValue();
        const auto* clusterDesc = State_->Configuration->ClusterConfigs.FindPtr(solomonCluster);
        YQL_ENSURE(clusterDesc, "Unknown cluster " << solomonCluster);

        NSo::NProto::TDqSolomonShard shardDesc;
        shardDesc.SetEndpoint(clusterDesc->GetCluster());
        shardDesc.SetProject(shard.Project().StringValue());
        shardDesc.SetCluster(shard.Cluster().StringValue());
        shardDesc.SetService(shard.Service().StringValue());

        shardDesc.SetClusterType(MapClusterType(clusterDesc->GetClusterType()));
        shardDesc.SetUseSsl(clusterDesc->GetUseSsl());

        const TTypeAnnotationNode* itemType = shard.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        FillScheme(*itemType, *shardDesc.MutableScheme());

        if (auto maybeToken = shard.Token()) {
            shardDesc.MutableToken()->SetName(TString(maybeToken.Cast().Name().Value()));
        }

        protoSettings.PackFrom(shardDesc);
        sinkType = "SolomonSink";
    }

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
        RegisterDqSolomonMkqlCompilers(compiler);
    }

private:
    TSolomonState* State_; // State owns dq integration, so back reference must be not smart.
};

}

THolder<IDqIntegration> CreateSolomonDqIntegration(const TSolomonState::TPtr& state) {
    return MakeHolder<TSolomonDqIntegration>(state);
}

}
