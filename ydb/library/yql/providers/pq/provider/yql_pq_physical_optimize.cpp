#include "yql_pq_provider_impl.h"
#include "yql_pq_helpers.h"
#include "yql_pq_settings.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/runtime/streaming/partition_key.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <yql/essentials/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

namespace {

using namespace NNodes;
using namespace NDq;

TCoAtomList GetWatermarkMetadataColumns(TPositionHandle pos, const TPqState& state, TExprContext& ctx) {
    static constexpr auto RequiredMetadataColumns = std::to_array<std::string_view>({
        "cluster",
        "partition_id",
        "write_time",
    });

    auto result = Build<TCoAtomList>(ctx, pos);
    for (const auto& key : RequiredMetadataColumns) {
        const auto descriptor = GetPqMetaFieldDescriptorByKey(
            TString(key),
            state.AddTransparentPrefixToTransparentSystemColumns,
            state.EnableUserAttributesInTopicQuery,
            state.ForbidYqlSysColumnsAndSystemMetadata
        );
        YQL_ENSURE(descriptor, "Unexpected pq metadata key: " << key);
        result.Add<TCoAtom>()
            .Value(descriptor->SysColumn)
            .Build();
    }
    return result.Done();
}

TMaybeNode<TExprBase> ParseISO8601(TCoAtom input, TExprContext& ctx) {
    const auto buf = input.Value();

    const auto out = NKikimr::NMiniKQL::ValueFromString(NKikimr::NUdf::EDataSlot::Interval, buf);
    if (!out) {
        ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
            << "Expected interval in ISO 8601 format, got " << TString(buf).Quote()));
        return {};
    }

    return Build<TCoAtom>(ctx, input.Pos())
        .Value(ToString(out.Get<ui64>()))
        .Done();
}

TMaybeNode<TCoNameValueTupleList> BuildDqWatermarkSettings(
    TCoNameValueTupleList settings,
    TCoNameValueTupleList topicProps,
    TCoLambda watermark,
    const TPqState& state,
    TExprContext& ctx
) {
    auto result = Build<TCoNameValueTupleList>(ctx, settings.Pos());

    for (const auto nameValue : settings) {
        auto name = nameValue.Name().Value();
        auto value = nameValue.Value();

        if (name == "watermarkgranularity") {
            name = WatermarksGranularityUsSetting;
            value = ParseISO8601(value.Cast<TCoAtom>(), ctx);
            if (!value) {
                return {};
            }
        } else if (name == "watermarklatearrivaldelay") {
            name = WatermarksLateArrivalDelayUsSetting;
            value = ParseISO8601(value.Cast<TCoAtom>(), ctx);
            if (!value) {
                return {};
            }
        } else if (name == "watermarkidletimeout") {
            name = WatermarksIdleTimeoutUsSetting;
            value = ParseISO8601(value.Cast<TCoAtom>(), ctx);
            if (!value) {
                return {};
            }
        } else {
            continue;
        }

        result.Add<TCoNameValueTuple>()
            .Name<TCoAtom>().Build(name)
            .Value(value)
            .Build();
    }

    for (const auto nameValue : topicProps) {
        auto name = nameValue.Name().Value();
        auto value = nameValue.Value();

        if (name == FederatedClustersProp) {
            const auto federatedClusters = value.Cast<TDqPqFederatedClusterList>();

            auto newValue = Build<TCoAtomList>(ctx, value.Cast().Pos());
            for (const auto& federatedCluster : federatedClusters) {
                const auto cluster = federatedCluster.Name();
                const auto partitionsCount = federatedCluster.PartitionsCount();

                TString serializedPartitionKey;
                TStringOutput out(serializedPartitionKey);
                out << NDq::TPartitionKey {
                    .Cluster = TString{cluster.Value()},
                    .PartitionId = partitionsCount ? FromString<ui32>(partitionsCount.Cast().Value()) : 0,
                };

                newValue.Add<TCoAtom>()
                    .Value(serializedPartitionKey)
                    .Build();
            }
            value = newValue.Done();
        } else {
            continue;
        }

        result.Add<TCoNameValueTuple>()
            .Name<TCoAtom>().Build(name)
            .Value(value)
            .Build();
    }

    const auto eventTimeAndDelay = SplitWatermarkExpr(watermark, state, ctx);
    if (!eventTimeAndDelay) {
        return {};
    }
    const auto [_, delay] = *eventTimeAndDelay;

    result.Add<TCoNameValueTuple>()
        .Name<TCoAtom>().Build(WatermarksLateArrivalDelayUsSetting)
        .Value<TCoAtom>()
            .Value(ToString(delay))
            .Build()
        .Build();

    return result.Done();
}

class TPqPhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    explicit TPqPhysicalOptProposalTransformer(TPqState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderPq, {})
        , State_(std::move(state))
    {
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TPqPhysicalOptProposalTransformer::name)
        AddHandler(0, &TCoLeft::Match, HNDL(TrimReadWorld));
        AddHandler(0, &TPqWriteTopic::Match, HNDL(PqWriteTopic));
        AddHandler(0, &TPqInsert::Match, HNDL(PqInsert));
        AddHandler(0, &TCoWatermarkGenerator::Match, HNDL(RewriteWatermarkGenerator));
        AddHandler(0, &TPqParsingWrap::Match, HNDL(RewriteParsingWrap));
        AddHandler(0, &TDqPqPhyParsingWrap::Match, HNDL(PushParsingWrapToStage));
#undef HNDL

        SetGlobal(0); // Stage 0 of this optimizer is global => we can remap nodes.
    }

    TMaybeNode<TExprBase> TrimReadWorld(TExprBase node, TExprContext& ctx) const {
        Y_UNUSED(ctx);

        const auto& maybeRead = node.Cast<TCoLeft>().Input().Maybe<TPqReadTopic>();
        if (!maybeRead) {
            return node;
        }

        return TExprBase(maybeRead.Cast().World().Ptr());
    }

    TMaybeNode<TExprBase> RewriteWatermarkGenerator(TExprBase node, TExprContext& ctx) const {
        const auto watermarkGenerator = node.Cast<TCoWatermarkGenerator>();

        const auto maybeParsingWrap = watermarkGenerator.Input().Maybe<TPqParsingWrap>();
        if (!maybeParsingWrap) {
            const auto metadataColumns = GetWatermarkMetadataColumns(watermarkGenerator.Pos(), *State_, ctx);

            return Build<TCoRemovePrefixMembers>(ctx, watermarkGenerator.Pos())
                .Input<TCoWatermarkGenerator>()
                    .InitFrom(watermarkGenerator)
                    .Input<TPqParsingWrap>()
                        .Input(watermarkGenerator.Input())
                        .Lambda(BuildIdentityLambda(watermarkGenerator.Pos(), ctx))
                        .MetadataColumns(metadataColumns)
                        .Build()
                    .Build()
                .Prefixes<TCoAtomList>()
                    .Add<TCoAtom>().Build("__ydb_watermark_")
                    .Build()
                .Done();
        }
        const auto parsingWrap = maybeParsingWrap.Cast();

        const auto maybeRight = parsingWrap.Input().Maybe<TCoRight>();
        if (!maybeRight) {
            return node;
        }
        const auto right = maybeRight.Cast();

        const auto maybePqReadTopic = right.Input().Maybe<TPqReadTopic>();
        if (!maybePqReadTopic) {
            ctx.AddError(TIssue(ctx.GetPosition(maybeRight.Cast().Input().Pos()), "Unsupported source category: expected PQ topic"));
            return {};
        }
        const auto pqReadTopic = maybePqReadTopic.Cast();

        const auto eventTimeAndDelay = SplitWatermarkExpr(watermarkGenerator.WatermarkExtractor(), *State_, ctx);
        if (!eventTimeAndDelay) {
            return {};
        }
        const auto [eventTimeExtractor, _] = *eventTimeAndDelay;

        const auto maybeWatermarkSettings = BuildDqWatermarkSettings(
            watermarkGenerator.WatermarkSettings(),
            pqReadTopic.Topic().Props(),
            watermarkGenerator.WatermarkExtractor(),
            *State_,
            ctx
        );
        if (!maybeWatermarkSettings) {
            return {};
        }
        const auto watermarkSettings = maybeWatermarkSettings.Cast();

        return Build<TDqPhyWatermarkGenerator>(ctx, watermarkGenerator.Pos())
            .Input<TDqPqPhyParsingWrap>()
                .Input(parsingWrap.Input())
                .Lambda(parsingWrap.Lambda())
                .MetadataColumns(parsingWrap.MetadataColumns())
                .Build()
            .WatermarkExtractor(eventTimeExtractor)
            .PartitionKeyExtractor<TCoLambda>()
                .Args({"arg"})
                .Body<TCoAsStruct>()
                    .Add<TCoNameValueTuple>()
                        .Name<TCoAtom>().Build("cluster")
                        .Value<TCoMember>()
                            .Struct("arg")
                            .Name().Build("__ydb_watermark_cluster")
                            .Build()
                        .Build()
                    .Add<TCoNameValueTuple>()
                        .Name<TCoAtom>().Build("partition_id")
                        .Value<TCoMember>()
                            .Struct("arg")
                            .Name().Build("__ydb_watermark_partition_id")
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .WriteTimeExtractor<TCoLambda>()
                .Args({"arg"})
                .Body<TCoMember>()
                    .Struct("arg")
                    .Name().Build("__ydb_watermark_write_time")
                    .Build()
                .Build()
            .WatermarkSettings(watermarkSettings)
            .PartitionKeys<TCoVoid>().Build()
            .Done();
    }

    TMaybeNode<TExprBase> RewriteParsingWrap(TExprBase node, TExprContext& ctx) const {
        const auto parsingWrap = node.Cast<TPqParsingWrap>();
        const auto input = parsingWrap.Input();

        if (const auto maybeRight = input.Maybe<TCoRight>()) {
            if (!maybeRight.Cast().Input().Maybe<TPqReadTopic>()) {
                ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), "Unsupported source category: expected PQ topic"));
                return {};
            }

            return node;
        }

        if (input.Ref().IsCallable({
            TCoExtractMembers::CallableName(),
            TCoRemoveSystemMembers::CallableName(),
            TCoRemovePrefixMembers::CallableName(),
            TCoFlatMap::CallableName(),
            TCoOrderedFlatMap::CallableName(),
        })) {
            auto arg = Build<TCoArgument>(ctx, input.Pos())
                .Name("stream")
                .Done();

            auto innerLambda = Build<TCoLambda>(ctx, input.Pos())
                .Args({arg})
                .Body(ctx.ChangeChild(input.Ref(), 0, arg.Ptr()))
                .Done();

            return Build<TPqParsingWrap>(ctx, parsingWrap.Pos())
                .InitFrom(parsingWrap)
                .Input(input.Ref().HeadPtr())
                .Lambda(ctx.FuseLambdas(parsingWrap.Lambda().Ref(), innerLambda.Ref()))
                .Done();
        }

        if (input.Ref().Content().starts_with("Dq")) {
            return node;
        }

        ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
            << "Unsupported node between PQ source and WATERMARK: "
            << input.Ref().Content()));
        return {};
    }

    TMaybeNode<TExprBase> PushParsingWrapToStage(
        TExprBase node,
        TExprContext& ctx,
        IOptimizationContext& optCtx,
        const TGetParents& getParents
    ) const {
        const auto parsingWrap = node.Cast<TDqPqPhyParsingWrap>();

        const auto maybeConnection = parsingWrap.Input().Maybe<TDqCnUnionAll>();
        if (!maybeConnection) {
            return node;
        }
        const auto connection = maybeConnection.Cast();

        if (!IsSingleConsumerConnection(connection, *getParents())) {
            return node;
        }

        const auto lambda = Build<TCoLambda>(ctx, parsingWrap.Pos())
            .Args({"arg"})
            .Body<TCoToFlow>()
                .Input<TDqPqPhyParsingWrap>()
                    .InitFrom(parsingWrap)
                    .Input<TCoFromFlow>()
                        .Input("arg")
                        .Build()
                    .Build()
                .Build()
            .Done();

        const auto result = DqPushLambdaToStageUnionAll(connection, lambda, {}, ctx, optCtx);
        if (!result) {
            return node;
        }
        return result.Cast();
    }

    NNodes::TCoNameValueTupleList BuildTopicWriteSettings(const TString& cluster, TPositionHandle pos, TExprContext& ctx) const {
        TVector<TCoNameValueTuple> props;

        auto clusterConfiguration = State_->Configuration->ClustersConfigurationSettings.FindPtr(cluster);
        if (!clusterConfiguration) {
            ythrow yexception() << "Unknown pq cluster \"" << cluster << "\"";
        }

        Add(props, EndpointSetting, clusterConfiguration->Endpoint, pos, ctx);
        if (clusterConfiguration->UseSsl) {
            Add(props, UseSslSetting, "1", pos, ctx);
        }

        if (clusterConfiguration->AddBearerToToken) {
            Add(props, AddBearerToTokenSetting, "1", pos, ctx);
        }

        return Build<TCoNameValueTupleList>(ctx, pos)
            .Add(props)
            .Done();
    }

    TMaybeNode<TExprBase> PqWriteTopic(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const {
        auto write = node.Cast<TPqWriteTopic>();
        if (!TDqCnUnionAll::Match(write.Input().Raw())) { // => this code is not for RTMR mode.
            return node;
        }

        const auto& topicNode = write.Topic();
        const TString cluster(topicNode.Cluster().Value());

        const TParentsMap* parentsMap = getParents();
        auto dqUnion = write.Input().Cast<TDqCnUnionAll>();
        if (!NDq::IsSingleConsumerConnection(dqUnion, *parentsMap)) {
            return node;
        }

        const auto* topicMeta = State_->FindTopicMeta(topicNode);
        if (!topicMeta) {
            ctx.AddError(TIssue(ctx.GetPosition(write.Pos()), TStringBuilder() << "Unknown topic `" << topicNode.Cluster().StringValue() << "`.`"
                                << topicNode.Path().StringValue() << "`"));
            return nullptr;
        }

        YQL_CLOG(INFO, ProviderPq) << "Optimize PqWriteTopic `" << topicNode.Cluster().StringValue() << "`.`" << topicNode.Path().StringValue() << "`";

        auto dqPqTopicSinkSettingsBuilder = Build<TDqPqTopicSink>(ctx, write.Pos());
        dqPqTopicSinkSettingsBuilder.Topic(topicNode);
        dqPqTopicSinkSettingsBuilder.Settings(BuildTopicWriteSettings(cluster, write.Pos(), ctx));
        dqPqTopicSinkSettingsBuilder.Token<TCoSecureParam>().Name().Build("cluster:default_" + cluster).Build();
        auto dqPqTopicSinkSettings = dqPqTopicSinkSettingsBuilder.Done();

        auto dqSink = Build<TDqSink>(ctx, write.Pos())
            .DataSink(write.DataSink())
            .Settings(dqPqTopicSinkSettings)
            .Index(dqUnion.Output().Index())
            .Done();

        TDqStage inputStage = dqUnion.Output().Stage().Cast<TDqStage>();

        auto outputsBuilder = Build<TDqStageOutputsList>(ctx, topicNode.Pos());
        if (inputStage.Outputs()) {
            outputsBuilder.InitFrom(inputStage.Outputs().Cast());
        }
        outputsBuilder.Add(dqSink);

        auto dqStageWithSink = Build<TDqStage>(ctx, inputStage.Pos())
            .InitFrom(inputStage)
            .Outputs(outputsBuilder.Done())
            .Done();

        auto dqQueryBuilder = Build<TDqQuery>(ctx, write.Pos());
        dqQueryBuilder.World(write.World());
        dqQueryBuilder.SinkStages().Add(dqStageWithSink).Build();

        optCtx.RemapNode(inputStage.Ref(), dqStageWithSink.Ptr());

        return dqQueryBuilder.Done();
    }

    TMaybeNode<TExprBase> PqInsert(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const {
        const auto insert = node.Cast<TPqInsert>();
        const auto& topicNode = insert.Topic();
        const TString cluster(topicNode.Cluster().Value());
        if (!State_->FindTopicMeta(topicNode)) {
            ctx.AddError(TIssue(ctx.GetPosition(insert.Pos()), TStringBuilder() << "Unknown topic `" << cluster << "`.`" << topicNode.Path().StringValue() << "`"));
            return nullptr;
        }

        auto dqSinkBuilder = Build<TDqSink>(ctx, insert.Pos())
            .DataSink(insert.DataSink())
            .Settings<TDqPqTopicSink>()
                .Topic(topicNode)
                .Settings(BuildTopicWriteSettings(cluster, insert.Pos(), ctx))
                .Token<TCoSecureParam>()
                    .Name()
                        .Value(TStringBuilder() << "cluster:default_" << cluster)
                        .Build()
                    .Build()
                .Build();

        const auto input = insert.Input();
        if (IsDqPureExpr(input)) {
            YQL_CLOG(INFO, ProviderPq) << "Optimize PqInsert `" << cluster << "`.`" << topicNode.Path().StringValue() << "`, build pure stage with sink";

            const auto dqSink = dqSinkBuilder
                .Index().Build(0)
                .Done();

            return Build<TDqStage>(ctx, insert.Pos())
                .Inputs().Build()
                .Program<TCoLambda>()
                    .Args({})
                    .Body<TCoToFlow>()
                        .Input(input)
                        .Build()
                    .Build()
                .Outputs()
                    .Add(dqSink)
                    .Build()
                .Settings().Build()
                .Done();
        }

        if (!TDqCnUnionAll::Match(input.Raw())) {
            return node;
        }

        const auto dqUnion = input.Cast<TDqCnUnionAll>();
        if (!NDq::IsSingleConsumerConnection(dqUnion, *getParents())) {
            return node;
        }

        YQL_CLOG(INFO, ProviderPq) << "Optimize PqInsert `" << cluster << "`.`" << topicNode.Path().StringValue() << "`, push into existing stage";

        const auto dqUnionOutput = dqUnion.Output();
        const auto dqSink = dqSinkBuilder
            .Index(dqUnionOutput.Index())
            .Done();

        const auto inputStage = dqUnionOutput.Stage().Cast<TDqStage>();

        auto outputsBuilder = Build<TDqStageOutputsList>(ctx, topicNode.Pos());
        if (const auto outputs = inputStage.Outputs()) {
            outputsBuilder.InitFrom(outputs.Cast());
            YQL_ENSURE(inputStage.Program().Body().Maybe<TDqReplicate>(), "Can not push multiple async outputs into stage without TDqReplicate");
        }
        outputsBuilder.Add(dqSink);

        const auto dqStageWithSink = Build<TDqStage>(ctx, inputStage.Pos())
            .InitFrom(inputStage)
            .Outputs(outputsBuilder.Done())
            .Done();

        optCtx.RemapNode(inputStage.Ref(), dqStageWithSink.Ptr());

        const auto dqResult = Build<TCoNth>(ctx, dqStageWithSink.Pos())
            .Tuple(dqStageWithSink)
            .Index(dqUnionOutput.Index())
            .Done();

        return ctx.NewList(dqStageWithSink.Pos(), {dqResult.Ptr()});
    }

private:
    TPqState::TPtr State_;
};

} // anonymous namespace

THolder<IGraphTransformer> CreatePqPhysicalOptProposalTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqPhysicalOptProposalTransformer>(std::move(state));
}

} // namespace NYql
