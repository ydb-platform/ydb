#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>

#include <util/generic/size_literals.h>

namespace NYql {

namespace {

using namespace NNodes;
using namespace NDq;

TExprNode::TPtr FindChild(const TExprNode& settings, TStringBuf name) {
    for (auto i = 0U; i < settings.ChildrenSize(); ++i) {
        if (settings.Child(i)->Head().IsAtom(name)) {
            return settings.ChildPtr(i);
        }
    }

    return {};
}

TExprNode::TPtr GetPartitionBy(const TExprNode& settings) {
    return FindChild(settings, "partitionedby"sv);
}

TExprNode::TPtr GetCompression(const TExprNode& settings) {
    return FindChild(settings, "compression"sv);
}

TExprNode::TPtr GetCsvDelimiter(const TExprNode& settings) {
    return FindChild(settings, "csvdelimiter"sv);
}

TExprNode::TPtr GetDateTimeFormatName(const TExprNode& settings) {
    return FindChild(settings, "data.datetime.formatname"sv);
}

TExprNode::TPtr GetDateTimeFormat(const TExprNode& settings) {
    return FindChild(settings, "data.datetime.format"sv);
}

TExprNode::TPtr GetTimestampFormatName(const TExprNode& settings) {
    return FindChild(settings, "data.timestamp.formatname"sv);
}

TExprNode::TPtr GetTimestampFormat(const TExprNode& settings) {
    return FindChild(settings, "data.timestamp.format"sv);
}

TExprNode::TListType GetPartitionKeys(const TExprNode::TPtr& partBy) {
    if (partBy) {
        auto children = partBy->ChildrenList();
        children.erase(children.cbegin());
        return children;
    }

    return {};
}

TString GetExtension(const std::string_view& format, const std::string_view& compression) {
    static const std::unordered_map<std::string_view, std::string_view> formatsMap = {
        {"csv_with_names"sv, "csv"sv},
        {"tsv_with_names"sv, "tsv"sv},
        {"raw"sv, "bin"sv},
        {"json_list"sv, "json"sv},
        {"json_each_row"sv, "json"sv},
        {"parquet"sv, "parquet"sv}
    };

    static const std::unordered_map<std::string_view, std::string_view> compressionsMap = {
        {"gzip"sv, "gz"sv},
        {"zstd"sv, "zst"sv},
        {"lz4"sv, "lz4"sv},
        {"bzip2"sv, "bz2"sv},
        {"brotli"sv, "br"sv},
        {"xz"sv, "xz"sv}
    };

    TStringBuilder extension;
    if (const auto it = formatsMap.find(format); formatsMap.cend() != it) {
        extension << '.' << it->second;
    }

    if (const auto it = compressionsMap.find(compression); compressionsMap.cend() != it) {
        extension << '.' << it->second;
    }
    return extension;
}

class TS3PhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    explicit TS3PhysicalOptProposalTransformer(TS3State::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderS3, {})
        , State_(std::move(state))
    {
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TS3PhysicalOptProposalTransformer::name)
        AddHandler(0, &TCoLeft::Match, HNDL(TrimReadWorld));
        if (!State_->Configuration->WriteThroughDqIntegration) {
            AddHandler(0, &TS3WriteObject::Match, HNDL(S3WriteObject));
        }
        AddHandler(0, &TS3Insert::Match, HNDL(S3Insert));
#undef HNDL
    }

    TMaybeNode<TExprBase> TrimReadWorld(TExprBase node, TExprContext& ctx) const {
        Y_UNUSED(ctx);

        const auto& maybeRead = node.Cast<TCoLeft>().Input().Maybe<TS3ReadObject>();
        if (!maybeRead) {
            return node;
        }

        return TExprBase(maybeRead.Cast().World().Ptr());
    }

    TMaybe<TDqStage> BuildSinkStage(TPositionHandle writePos, TS3DataSink dataSink, TS3Target target, TExprBase input, TExprContext& ctx, const TGetParents& getParents) const {
        const auto& cluster = dataSink.Cluster().StringValue();
        const auto token = "cluster:default_" + cluster;
        const auto& settings = target.Settings().Ref();
        auto partBy = GetPartitionBy(settings);
        auto keys = GetPartitionKeys(partBy);

        auto sinkSettingsBuilder = Build<TExprList>(ctx, target.Pos());
        if (partBy)
            sinkSettingsBuilder.Add(std::move(partBy));

        auto compression = GetCompression(settings);
        const auto& extension = GetExtension(target.Format().Value(), compression ? compression->Tail().Content() : ""sv);
        if (compression)
            sinkSettingsBuilder.Add(std::move(compression));

        auto sinkOutputSettingsBuilder = Build<TExprList>(ctx, target.Pos());
        if (auto csvDelimiter = GetCsvDelimiter(settings)) {
            sinkOutputSettingsBuilder.Add(std::move(csvDelimiter));
        }

        bool hasDateTimeFormat = false;
        bool hasDateTimeFormatName = false;
        bool hasTimestampFormat = false;
        bool hasTimestampFormatName = false;
        if (auto dateTimeFormatName = GetDateTimeFormatName(settings)) {
            sinkOutputSettingsBuilder.Add(std::move(dateTimeFormatName));
            hasDateTimeFormatName = true;
        }

        if (auto dateTimeFormat = GetDateTimeFormat(settings)) {
            sinkOutputSettingsBuilder.Add(std::move(dateTimeFormat));
            hasDateTimeFormat = true;
        }

        if (auto timestampFormatName = GetTimestampFormatName(settings)) {
            sinkOutputSettingsBuilder.Add(std::move(timestampFormatName));
            hasTimestampFormatName = true;
        }

        if (auto timestampFormat = GetTimestampFormat(settings)) {
            sinkOutputSettingsBuilder.Add(std::move(timestampFormat));
            hasTimestampFormat = true;
        }

        if (!hasDateTimeFormat && !hasDateTimeFormatName) {
            TExprNode::TListType pair;
            pair.push_back(ctx.NewAtom(target.Pos(), "data.datetime.formatname"));
            pair.push_back(ctx.NewAtom(target.Pos(), "POSIX"));
            sinkOutputSettingsBuilder.Add(ctx.NewList(target.Pos(), std::move(pair)));
        }

        if (!hasTimestampFormat && !hasTimestampFormatName) {
            TExprNode::TListType pair;
            pair.push_back(ctx.NewAtom(target.Pos(), "data.timestamp.formatname"));
            pair.push_back(ctx.NewAtom(target.Pos(), "POSIX"));
            sinkOutputSettingsBuilder.Add(ctx.NewList(target.Pos(), std::move(pair)));
        }

        const TStringBuf format = target.Format();
        if (format != "raw" && format != "json_list") { // multipart
            {
                TExprNode::TListType pair;
                pair.push_back(ctx.NewAtom(target.Pos(), "multipart"));
                pair.push_back(ctx.NewAtom(target.Pos(), "true"));
                sinkSettingsBuilder.Add(ctx.NewList(target.Pos(), std::move(pair)));
            }
            {
                TExprNode::TListType pair;
                pair.push_back(ctx.NewAtom(target.Pos(), "file_size_limit"));
                size_t fileSize = 50_MB;
                if (const auto& maxObjectSize = State_->Configuration->MaxOutputObjectSize.Get()) {
                    fileSize = *maxObjectSize;
                }
                pair.push_back(ctx.NewAtom(target.Pos(), ToString(fileSize)));
                sinkOutputSettingsBuilder.Add(ctx.NewList(target.Pos(), std::move(pair)));
            }
        }

        if (!FindNode(input.Ptr(), [] (const TExprNode::TPtr& node) { return node->IsCallable(TCoDataSource::CallableName()); })) {
            YQL_CLOG(INFO, ProviderS3) << "Rewrite pure S3WriteObject `" << cluster << "`.`" << target.Path().StringValue() << "` as stage with sink.";
            return keys.empty() ?
                Build<TDqStage>(ctx, writePos)
                    .Inputs().Build()
                    .Program<TCoLambda>()
                        .Args({})
                        .Body<TS3SinkOutput>()
                            .Input<TCoToFlow>()
                                .Input(input)
                                .Build()
                            .Format(target.Format())
                            .KeyColumns().Build()
                            .Settings(sinkOutputSettingsBuilder.Done())
                            .Build()
                        .Build()
                    .Outputs<TDqStageOutputsList>()
                        .Add<TDqSink>()
                            .DataSink(dataSink)
                            .Index().Value("0").Build()
                            .Settings<TS3SinkSettings>()
                                .Path(target.Path())
                                .Settings(sinkSettingsBuilder.Done())
                                .Token<TCoSecureParam>()
                                    .Name().Build(token)
                                    .Build()
                                .Extension().Value(extension).Build()
                                .Build()
                            .Build()
                        .Build()
                    .Settings().Build()
                    .Done()
                    :
                    Build<TDqStage>(ctx, writePos)
                        .Inputs()
                            .Add<TDqCnHashShuffle>()
                                .Output<TDqOutput>()
                                    .Stage<TDqStage>()
                                        .Inputs().Build()
                                        .Program<TCoLambda>()
                                            .Args({})
                                            .Body<TCoToFlow>()
                                                .Input(input)
                                                .Build()
                                            .Build()
                                        .Settings().Build()
                                        .Build()
                                    .Index().Value("0", TNodeFlags::Default).Build()
                                    .Build()
                                .KeyColumns().Add(keys).Build()
                                .Build()
                            .Build()
                        .Program<TCoLambda>()
                            .Args({"in"})
                            .Body<TS3SinkOutput>()
                                .Input("in")
                                .Format(target.Format())
                                .KeyColumns().Add(keys).Build()
                                .Settings(sinkOutputSettingsBuilder.Done())
                                .Build()
                            .Build()
                        .Outputs<TDqStageOutputsList>()
                            .Add<TDqSink>()
                                .DataSink(dataSink)
                                .Index().Value("0", TNodeFlags::Default).Build()
                                .Settings<TS3SinkSettings>()
                                    .Path(target.Path())
                                    .Settings(sinkSettingsBuilder.Done())
                                    .Token<TCoSecureParam>()
                                        .Name().Build(token)
                                        .Build()
                                    .Extension().Value(extension).Build()
                                    .Build()
                                .Build()
                            .Build()
                        .Settings().Build()
                        .Done();
        }

        if (!TDqCnUnionAll::Match(input.Raw())) {
            return Nothing();
        }

        const TParentsMap* parentsMap = getParents();
        const auto dqUnion = input.Cast<TDqCnUnionAll>();
        if (!NDq::IsSingleConsumerConnection(dqUnion, *parentsMap)) {
            return Nothing();
        }

        YQL_CLOG(INFO, ProviderS3) << "Rewrite S3WriteObject `" << cluster << "`.`" << target.Path().StringValue() << "` as sink.";

        const auto inputStage = dqUnion.Output().Stage().Cast<TDqStage>();

        const auto sink = Build<TDqSink>(ctx, writePos)
            .DataSink(dataSink)
            .Index(dqUnion.Output().Index())
            .Settings<TS3SinkSettings>()
                .Path(target.Path())
                .Settings(sinkSettingsBuilder.Done())
                .Token<TCoSecureParam>()
                    .Name().Build(token)
                    .Build()
                .Extension().Value(extension).Build()
                .Build()
            .Done();

        auto outputsBuilder = Build<TDqStageOutputsList>(ctx, target.Pos());
        if (inputStage.Outputs() && keys.empty()) {
            outputsBuilder.InitFrom(inputStage.Outputs().Cast());
        }
        outputsBuilder.Add(sink);

        if (keys.empty()) {
            const auto outputBuilder = Build<TS3SinkOutput>(ctx, target.Pos())
                .Input(inputStage.Program().Body().Ptr())
                .Format(target.Format())
                .KeyColumns().Add(std::move(keys)).Build()
                .Settings(sinkOutputSettingsBuilder.Done())
                .Done();

            return Build<TDqStage>(ctx, writePos)
                .InitFrom(inputStage)
                .Program(ctx.DeepCopyLambda(inputStage.Program().Ref(), outputBuilder.Ptr()))
                .Outputs(outputsBuilder.Done())
                .Done();
        } else {
            return Build<TDqStage>(ctx, writePos)
                .Inputs()
                    .Add<TDqCnHashShuffle>()
                        .Output<TDqOutput>()
                            .Stage(inputStage)
                            .Index(dqUnion.Output().Index())
                            .Build()
                        .KeyColumns().Add(keys).Build()
                        .Build()
                    .Build()
                .Program<TCoLambda>()
                    .Args({"in"})
                    .Body<TS3SinkOutput>()
                        .Input("in")
                        .Format(target.Format())
                        .KeyColumns().Add(std::move(keys)).Build()
                        .Settings(sinkOutputSettingsBuilder.Done())
                        .Build()
                    .Build()
                .Settings().Build()
                .Outputs(outputsBuilder.Done())
                .Done();
        }
    }

    TMaybeNode<TExprBase> S3Insert(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto insert = node.Cast<TS3Insert>();
        TMaybe<TDqStage> stage
            = BuildSinkStage(node.Pos(),
                insert.DataSink(),
                insert.Target(),
                insert.Input(),
                ctx,
                getParents);

        if (stage) {
            return *stage;
        } else {
            return node;
        }
    }

    TMaybeNode<TExprBase> S3WriteObject(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto write = node.Cast<TS3WriteObject>();
        TMaybe<TDqStage> stage
            = BuildSinkStage(node.Pos(),
                write.DataSink(),
                write.Target(),
                write.Input(),
                ctx,
                getParents);

        if (stage) {
            return Build<TDqQuery>(ctx, write.Pos())
                .World(write.World())
                .SinkStages()
                    .Add(*stage)
                    .Build()
                .Done();
        } else {
            return node;
        }
    }

    TMaybeNode<TExprBase> S3WriteObject1(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        const auto& write = node.Cast<TS3WriteObject>();
        const auto& targetNode = write.Target();
        const auto& cluster = write.DataSink().Cluster().StringValue();
        const auto token = "cluster:default_" + cluster;
        const auto& settings = write.Target().Settings().Ref();
        auto partBy = GetPartitionBy(settings);
        auto keys = GetPartitionKeys(partBy);

        auto sinkSettingsBuilder = Build<TExprList>(ctx, targetNode.Pos());
        if (partBy)
            sinkSettingsBuilder.Add(std::move(partBy));

        auto compression = GetCompression(settings);
        const auto& extension = GetExtension(write.Target().Format().Value(), compression ? compression->Tail().Content() : ""sv);
        if (compression)
            sinkSettingsBuilder.Add(std::move(compression));

        auto sinkOutputSettingsBuilder = Build<TExprList>(ctx, targetNode.Pos());
        if (auto csvDelimiter = GetCsvDelimiter(settings)) {
            sinkOutputSettingsBuilder.Add(std::move(csvDelimiter));
        }

        bool hasDateTimeFormat = false;
        bool hasDateTimeFormatName = false;
        bool hasTimestampFormat = false;
        bool hasTimestampFormatName = false;
        if (auto dateTimeFormatName = GetDateTimeFormatName(settings)) {
            sinkOutputSettingsBuilder.Add(std::move(dateTimeFormatName));
            hasDateTimeFormatName = true;
        }

        if (auto dateTimeFormat = GetDateTimeFormat(settings)) {
            sinkOutputSettingsBuilder.Add(std::move(dateTimeFormat));
            hasDateTimeFormat = true;
        }

        if (auto timestampFormatName = GetTimestampFormatName(settings)) {
            sinkOutputSettingsBuilder.Add(std::move(timestampFormatName));
            hasTimestampFormatName = true;
        }

        if (auto timestampFormat = GetTimestampFormat(settings)) {
            sinkOutputSettingsBuilder.Add(std::move(timestampFormat));
            hasTimestampFormat = true;
        }

        if (!hasDateTimeFormat && !hasDateTimeFormatName) {
            TExprNode::TListType pair;
            pair.push_back(ctx.NewAtom(targetNode.Pos(), "data.datetime.formatname"));
            pair.push_back(ctx.NewAtom(targetNode.Pos(), "POSIX"));
            sinkOutputSettingsBuilder.Add(ctx.NewList(targetNode.Pos(), std::move(pair)));
        }

        if (!hasTimestampFormat && !hasTimestampFormatName) {
            TExprNode::TListType pair;
            pair.push_back(ctx.NewAtom(targetNode.Pos(), "data.timestamp.formatname"));
            pair.push_back(ctx.NewAtom(targetNode.Pos(), "POSIX"));
            sinkOutputSettingsBuilder.Add(ctx.NewList(targetNode.Pos(), std::move(pair)));
        }

        const TStringBuf format = targetNode.Format();
        if (format != "raw" && format != "json_list") { // multipart
            {
                TExprNode::TListType pair;
                pair.push_back(ctx.NewAtom(targetNode.Pos(), "multipart"));
                pair.push_back(ctx.NewAtom(targetNode.Pos(), "true"));
                sinkSettingsBuilder.Add(ctx.NewList(targetNode.Pos(), std::move(pair)));
            }
            {
                TExprNode::TListType pair;
                pair.push_back(ctx.NewAtom(targetNode.Pos(), "file_size_limit"));
                size_t fileSize = 50_MB;
                if (const auto& maxObjectSize = State_->Configuration->MaxOutputObjectSize.Get()) {
                    fileSize = *maxObjectSize;
                }
                pair.push_back(ctx.NewAtom(targetNode.Pos(), ToString(fileSize)));
                sinkOutputSettingsBuilder.Add(ctx.NewList(targetNode.Pos(), std::move(pair)));
            }
        }

        if (!FindNode(write.Input().Ptr(), [] (const TExprNode::TPtr& node) { return node->IsCallable(TCoDataSource::CallableName()); })) {
            YQL_CLOG(INFO, ProviderS3) << "Rewrite pure S3WriteObject `" << cluster << "`.`" << targetNode.Path().StringValue() << "` as stage with sink.";
            return keys.empty() ?
                Build<TDqQuery>(ctx, write.Pos())
                    .World(write.World())
                    .SinkStages()
                        .Add<TDqStage>()
                            .Inputs().Build()
                            .Program<TCoLambda>()
                                .Args({})
                                .Body<TS3SinkOutput>()
                                    .Input<TCoToFlow>()
                                        .Input(write.Input())
                                        .Build()
                                    .Format(write.Target().Format())
                                    .KeyColumns().Build()
                                    .Settings(sinkOutputSettingsBuilder.Done())
                                    .Build()
                                .Build()
                            .Outputs<TDqStageOutputsList>()
                                .Add<TDqSink>()
                                    .DataSink(write.DataSink())
                                    .Index().Value("0").Build()
                                    .Settings<TS3SinkSettings>()
                                        .Path(write.Target().Path())
                                        .Settings(sinkSettingsBuilder.Done())
                                        .Token<TCoSecureParam>()
                                            .Name().Build(token)
                                            .Build()
                                        .Extension().Value(extension).Build()
                                        .Build()
                                    .Build()
                                .Build()
                            .Settings().Build()
                            .Build()
                        .Build()
                    .Done():
                Build<TDqQuery>(ctx, write.Pos())
                    .World(write.World())
                    .SinkStages()
                        .Add<TDqStage>()
                            .Inputs()
                                .Add<TDqCnHashShuffle>()
                                    .Output<TDqOutput>()
                                        .Stage<TDqStage>()
                                            .Inputs().Build()
                                            .Program<TCoLambda>()
                                                .Args({})
                                                .Body<TCoToFlow>()
                                                    .Input(write.Input())
                                                    .Build()
                                                .Build()
                                            .Settings().Build()
                                            .Build()
                                        .Index().Value("0", TNodeFlags::Default).Build()
                                        .Build()
                                    .KeyColumns().Add(keys).Build()
                                    .Build()
                                .Build()
                            .Program<TCoLambda>()
                                .Args({"in"})
                                .Body<TS3SinkOutput>()
                                    .Input("in")
                                    .Format(write.Target().Format())
                                    .KeyColumns().Add(keys).Build()
                                    .Settings(sinkOutputSettingsBuilder.Done())
                                    .Build()
                                .Build()
                            .Outputs<TDqStageOutputsList>()
                                .Add<TDqSink>()
                                    .DataSink(write.DataSink())
                                    .Index().Value("0", TNodeFlags::Default).Build()
                                    .Settings<TS3SinkSettings>()
                                        .Path(write.Target().Path())
                                        .Settings(sinkSettingsBuilder.Done())
                                        .Token<TCoSecureParam>()
                                            .Name().Build(token)
                                            .Build()
                                        .Extension().Value(extension).Build()
                                        .Build()
                                    .Build()
                                .Build()
                            .Settings().Build()
                            .Build()
                        .Build()
                    .Done();
        }

        if (!TDqCnUnionAll::Match(write.Input().Raw())) {
            return node;
        }

        const TParentsMap* parentsMap = getParents();
        const auto dqUnion = write.Input().Cast<TDqCnUnionAll>();
        if (!NDq::IsSingleConsumerConnection(dqUnion, *parentsMap)) {
            return node;
        }

        YQL_CLOG(INFO, ProviderS3) << "Rewrite S3WriteObject `" << cluster << "`.`" << targetNode.Path().StringValue() << "` as sink.";

        const auto inputStage = dqUnion.Output().Stage().Cast<TDqStage>();

        const auto sink = Build<TDqSink>(ctx, write.Pos())
            .DataSink(write.DataSink())
            .Index(dqUnion.Output().Index())
            .Settings<TS3SinkSettings>()
                .Path(write.Target().Path())
                .Settings(sinkSettingsBuilder.Done())
                .Token<TCoSecureParam>()
                    .Name().Build(token)
                    .Build()
                .Extension().Value(extension).Build()
                .Build()
            .Done();

        auto outputsBuilder = Build<TDqStageOutputsList>(ctx, targetNode.Pos());
        if (inputStage.Outputs() && keys.empty()) {
            outputsBuilder.InitFrom(inputStage.Outputs().Cast());
        }
        outputsBuilder.Add(sink);

        if (keys.empty()) {
            const auto outputBuilder = Build<TS3SinkOutput>(ctx, targetNode.Pos())
                .Input(inputStage.Program().Body().Ptr())
                .Format(write.Target().Format())
                .KeyColumns().Add(std::move(keys)).Build()
                .Settings(sinkOutputSettingsBuilder.Done())
                .Done();

            return Build<TDqQuery>(ctx, write.Pos())
                .World(write.World())
                .SinkStages()
                    .Add<TDqStage>()
                        .InitFrom(inputStage)
                        .Program(ctx.DeepCopyLambda(inputStage.Program().Ref(), outputBuilder.Ptr()))
                        .Outputs(outputsBuilder.Done())
                        .Build()
                    .Build()
                .Done();
        } else {
            return Build<TDqQuery>(ctx, write.Pos())
                .World(write.World())
                .SinkStages()
                    .Add<TDqStage>()
                        .Inputs()
                            .Add<TDqCnHashShuffle>()
                                .Output<TDqOutput>()
                                    .Stage(inputStage)
                                    .Index(dqUnion.Output().Index())
                                    .Build()
                                .KeyColumns().Add(keys).Build()
                                .Build()
                            .Build()
                        .Program<TCoLambda>()
                            .Args({"in"})
                            .Body<TS3SinkOutput>()
                                .Input("in")
                                .Format(write.Target().Format())
                                .KeyColumns().Add(std::move(keys)).Build()
                                .Settings(sinkOutputSettingsBuilder.Done())
                                .Build()
                            .Build()
                        .Settings().Build()
                        .Outputs(outputsBuilder.Done())
                        .Build()
                    .Build()
                .Done();
        }
    }

private:
    const TS3State::TPtr State_;
};

} // namespace

THolder<IGraphTransformer> CreateS3PhysicalOptProposalTransformer(TS3State::TPtr state) {
    return MakeHolder<TS3PhysicalOptProposalTransformer>(std::move(state));
}

} // namespace NYql
