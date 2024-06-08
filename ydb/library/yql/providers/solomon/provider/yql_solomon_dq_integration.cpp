#include "yql_solomon_dq_integration.h"
#include "yql_solomon_mkql_compiler.h"
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/dq/yql_dq_integration_impl.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>

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

const TTypeAnnotationNode* GetItemType(const TExprNode& node) {
    const TTypeAnnotationNode* typeAnn = node.GetTypeAnn();
    switch (typeAnn->GetKind()) {
        case ETypeAnnotationKind::Flow:
            return typeAnn->Cast<TFlowExprType>()->GetItemType();
        case ETypeAnnotationKind::Stream:
            return typeAnn->Cast<TStreamExprType>()->GetItemType();
        default: break;
    }
    YQL_ENSURE(false, "Invalid solomon sink type " << typeAnn->GetKind());
    return nullptr;
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

    ui64 Partition(const TDqSettings&, size_t maxPartitions, const TExprNode& node, TVector<TString>& partitions, TString*, TExprContext&, bool) override {
        Y_UNUSED(maxPartitions);
        Y_UNUSED(node);
        Y_UNUSED(partitions);
        partitions.push_back("zz_partition");
        return 0;
    }

    bool CanRead(const TExprNode& read, TExprContext&, bool) override {
        return TSoReadObject::Match(&read);
    }

    TMaybe<ui64> EstimateReadSize(ui64 /*dataSizePerJob*/, ui32 /*maxTasksPerStage*/, const TVector<const TExprNode*>&, TExprContext&) override {
        YQL_ENSURE(false, "Unimplemented");
    }

    TExprNode::TPtr WrapRead(const TDqSettings&, const TExprNode::TPtr& read, TExprContext& ctx) override {
        if (const auto& maybeSoReadObject = TMaybeNode<TSoReadObject>(read)) {
            const auto& soReadObject = maybeSoReadObject.Cast();
            YQL_ENSURE(soReadObject.Ref().GetTypeAnn(), "No type annotation for node " << soReadObject.Ref().Content());

            const auto& clusterName = soReadObject.DataSource().Cluster().StringValue();

            const auto token = "cluster:default_" + clusterName;
            YQL_CLOG(INFO, ProviderS3) << "Wrap " << read->Content() << " with token: " << token;

            auto& settings = soReadObject.Object().Settings().Ref();
            TString from;
            TString to;
            TString program;
            bool downsamplingDisabled = false;
            TString downsamplingAggregation;
            TString downsamplingFill;
            ui32 downsamplingGridSec = 15;

            for (auto i = 0U; i < settings.ChildrenSize(); ++i) {
                if (settings.Child(i)->Head().IsAtom("from"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settings.Child(i)->Tail(), "from"sv, ctx, value)) {
                        return {};
                    }

                    from = value;
                    continue;
                }
                if (settings.Child(i)->Head().IsAtom("to"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settings.Child(i)->Tail(), "to"sv, ctx, value)) {
                        return {};
                    }

                    to = value;
                    continue;
                }
                if (settings.Child(i)->Head().IsAtom("program"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settings.Child(i)->Tail(), "program"sv, ctx, value)) {
                        return {};
                    }

                    program = value;
                    continue;
                }
                if (settings.Child(i)->Head().IsAtom("downsampling.disabled"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settings.Child(i)->Tail(), "downsampling.disabled"sv, ctx, value)) {
                        return {};
                    }
                    downsamplingDisabled = FromString<bool>(value);
                    continue;
                }
                if (settings.Child(i)->Head().IsAtom("downsampling.gridaggregation"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settings.Child(i)->Tail(), "downsampling.grid_aggregation"sv, ctx, value)) {
                        return {};
                    }
                    // todo: validate value
                    downsamplingAggregation = value;
                    continue;
                }
                if (settings.Child(i)->Head().IsAtom("downsampling.fill"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settings.Child(i)->Tail(), "downsampling.fill"sv, ctx, value)) {
                        return {};
                    }
                    // todo: validate value
                    downsamplingFill = value;
                    continue;
                }
                if (settings.Child(i)->Head().IsAtom("downsampling.gridinterval"sv)) {
                    TStringBuf value;
                    if (!ExtractSettingValue(settings.Child(i)->Tail(), "downsampling.grid_interval"sv, ctx, value)) {
                        return {};
                    }
                    ui32 intValue = 0;
                    if (!TryFromString(value, intValue)) {
                        ctx.AddError(TIssue(ctx.GetPosition(settings.Child(i)->Head().Pos()), TStringBuilder() << "downsampling.grid_interval must be positive number, but has " << value));
                        return {};
                    }
                    downsamplingGridSec = intValue;
                    continue;
                }
            }

            const auto rowType = soReadObject.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back()->Cast<TListExprType>()->GetItemType();
            return Build<TDqSourceWrap>(ctx, read->Pos())
                .Input<TSoSourceSettings>()
                    .Token<TCoSecureParam>()
                        .Name().Build(token)
                        .Build()
                    .SystemColumns(soReadObject.SystemColumns())
                    .LabelNames(soReadObject.LabelNames())
                    .From<TCoAtom>().Build(from)
                    .To<TCoAtom>().Build(to)
                    .Program<TCoAtom>().Build(program)
                    .DownsamplingDisabled<TCoBool>().Literal().Build(downsamplingDisabled ? "true" : "false").Build()
                    .DownsamplingAggregation<TCoAtom>().Build(downsamplingAggregation)
                    .DownsamplingFill<TCoAtom>().Build(downsamplingFill)
                    .DownsamplingGridSec<TCoAtom>().Build(ToString(downsamplingGridSec))
                    .Build()
                .DataSource(soReadObject.DataSource().Cast<TCoDataSource>())
                .RowType(ExpandType(soReadObject.Pos(), *rowType, ctx))
                .Settings(&settings)
                .Done().Ptr();
        }
        return read;
    }

    TMaybe<bool> CanWrite(const TExprNode& write, TExprContext&) override {
        return TSoWrite::Match(&write);
    }

    void FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sourceType, size_t) override {
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
        source.SetProject("yq");

        source.SetClusterType(MapClusterType(clusterDesc->GetClusterType()));
        source.SetUseSsl(clusterDesc->GetUseSsl());
        source.SetFrom(TInstant::ParseIso8601("2023-11-09T14:40:39Z").Seconds());
        source.SetTo(TInstant::ParseIso8601("2023-11-10T14:45:39Z").Seconds());
        source.SetProgram("{execpool=User,activity=YQ_STORAGE_PROXY,sensor=ActorsAliveByActivity}");

        auto& downsampling = *source.MutableDownsampling();
        downsampling.SetDisabled(false);
        downsampling.SetAggregation("MAX");
        downsampling.SetFill("PREVIOUS");
        downsampling.SetGridMs(15 * 1000);

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

        const TTypeAnnotationNode* itemType = GetItemType(node);
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
