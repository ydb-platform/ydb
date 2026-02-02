#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <yql/essentials/utils/log/log.h>

#include <util/generic/size_literals.h>

namespace NYql {

namespace {

using namespace NNodes;
using namespace NDq;

TExprNode::TPtr GetPartitionBy(const TExprNode& settings) {
    return GetSetting(settings, "partitionedby"sv);
}

TExprNode::TPtr GetCompression(const TExprNode& settings) {
    return GetSetting(settings, "compression"sv);
}

TExprNode::TPtr GetCsvDelimiter(const TExprNode& settings) {
    return GetSetting(settings, "csvdelimiter"sv);
}

TExprNode::TPtr GetDateTimeFormatName(const TExprNode& settings) {
    return GetSetting(settings, "data.datetime.formatname"sv);
}

TExprNode::TPtr GetDateTimeFormat(const TExprNode& settings) {
    return GetSetting(settings, "data.datetime.format"sv);
}

TExprNode::TPtr GetTimestampFormatName(const TExprNode& settings) {
    return GetSetting(settings, "data.timestamp.formatname"sv);
}

TExprNode::TPtr GetTimestampFormat(const TExprNode& settings) {
    return GetSetting(settings, "data.timestamp.format"sv);
}

TExprNode::TPtr GetDateFormat(const TExprNode& settings) {
    return GetSetting(settings, "data.date.format"sv);
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

        SetGlobal(0); // Stage 0 of this optimizer is global => we can remap nodes.
    }

    TMaybeNode<TExprBase> TrimReadWorld(TExprBase node, TExprContext& ctx) const {
        Y_UNUSED(ctx);

        const auto& maybeRead = node.Cast<TCoLeft>().Input().Maybe<TS3ReadObject>();
        if (!maybeRead) {
            return node;
        }

        return TExprBase(maybeRead.Cast().World().Ptr());
    }

    TExprNode::TPtr BuildSinkStage(TPositionHandle writePos, TS3DataSink dataSink, TS3Target target, TExprBase input,
        TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents, bool wrapWithNth) const
    {
        const auto& cluster = dataSink.Cluster().StringValue();
        const auto token = "cluster:default_" + cluster;
        const auto& settings = target.Settings().Ref();
        auto partBy = GetPartitionBy(settings);
        auto keys = GetPartitionKeys(partBy);

        auto sinkSettingsBuilder = Build<TExprList>(ctx, target.Pos());
        if (partBy) {
            sinkSettingsBuilder.Add(std::move(partBy));
        }

        auto compression = GetCompression(settings);
        const auto& extension = GetExtension(target.Format().Value(), compression ? compression->Tail().Content() : ""sv);
        if (compression) {
            sinkSettingsBuilder.Add(std::move(compression));
        }

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

        if (auto dateFormat = GetDateFormat(settings)) {
            sinkOutputSettingsBuilder.Add(std::move(dateFormat));
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
                const auto fileSize = State_->Configuration->MaxOutputObjectSize.GetOrDefault();
                pair.push_back(ctx.NewAtom(target.Pos(), ToString(fileSize)));
                sinkOutputSettingsBuilder.Add(ctx.NewList(target.Pos(), std::move(pair)));
            }
        }

        const auto maybeUnionAll = input.Maybe<TDqCnUnionAll>();
        const bool pureDqExpr = IsDqPureExpr(input);
        if (!pureDqExpr && !maybeUnionAll) {
            // Wait until union all build for non pure DQ stage
            return {};
        }

        if (maybeUnionAll && !NDq::IsSingleConsumerConnection(maybeUnionAll.Cast(), *getParents())) {
            // Wait until union will be split with DqReplicate
            return {};
        }

        std::vector<TCoArgument> stageArgs;
        TNodeOnNodeOwnedMap stageArgsReplaces;
        TExprNode::TPtr stageBody;

        if (pureDqExpr) {
            // Build stage with one sink from scratch
            stageBody = Build<TCoToFlow>(ctx, writePos)
                .Input(input)
                .Done().Ptr();
        } else if (!keys.empty()) {
            // Build external stage with one sink
            stageArgs.emplace_back(Build<TCoArgument>(ctx, writePos)
                .Name("in")
                .Done());
            stageBody = stageArgs.back().Ptr();
        } else {
            // Push sink into existing stage and rebuild stage lambda
            const auto& program = maybeUnionAll.Cast().Output().Stage().Program();
            stageBody = program.Body().Ptr();

            const auto& args = program.Args();
            for (size_t i = 0; i < args.Size(); ++i) {
                const auto& oldArg = args.Arg(i);
                stageArgs.emplace_back(ctx.NewArgument(oldArg.Pos(), TStringBuilder() << "arg_" << i));
                YQL_ENSURE(stageArgsReplaces.emplace(oldArg.Raw(), stageArgs.back().Ptr()).second);
            }
        }

        const auto* structType = input.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        if (TString error; !UseBlocksSink(format, keys, structType, State_->Configuration, error)){
            // Build CH udf serializer
            YQL_ENSURE(!error, "Got block sink error");
            stageBody = Build<TS3SinkOutput>(ctx, writePos)
                .Input(stageBody)
                .Format(target.Format())
                .KeyColumns()
                    .Add(keys)
                    .Build()
                .Settings(sinkOutputSettingsBuilder.Done())
                .Done().Ptr();
        } else {
            // Build arrow block serializer
            YQL_ENSURE(format == "parquet");
            YQL_ENSURE(keys.empty());

            TExprNode::TListType pair;
            pair.push_back(ctx.NewAtom(target.Pos(), "block_output"));
            pair.push_back(ctx.NewAtom(target.Pos(), "true"));
            sinkSettingsBuilder.Add(ctx.NewList(target.Pos(), std::move(pair)));

            stageBody = Build<TCoToFlow>(ctx, writePos)
                .Input(stageBody)
                .Done().Ptr();

            TVector<TString> columns;
            columns.reserve(structType->GetSize());
            for (const auto& item : structType->GetItems()) {
                columns.emplace_back(item->GetName());
            }
            stageBody = MakeExpandMap(writePos, columns, stageBody, ctx);

            stageBody = Build<TCoWideToBlocks>(ctx, writePos)
                .Input<TCoFromFlow>()
                    .Input(stageBody)
                    .Build()
                .Done().Ptr();
        }

        const auto stageProgram = Build<TCoLambda>(ctx, writePos)
            .Args(stageArgs)
            .Body(ctx.ReplaceNodes(std::move(stageBody), stageArgsReplaces))
            .Done();

        const auto sinkSettings = Build<TS3SinkSettings>(ctx, writePos)
            .Path(target.Path())
            .Settings(sinkSettingsBuilder.Done())
            .Token<TCoSecureParam>()
                .Name().Build(token)
                .Build()
            .Extension().Value(extension).Build()
            .RowType(ExpandType(writePos, *structType, ctx))
            .Done();

        if (pureDqExpr) {
            YQL_CLOG(INFO, ProviderS3) << "Rewrite pure S3WriteObject `" << cluster << "`.`" << target.Path().StringValue() << "` as stage with sink.";

            const auto stageOutputs = Build<TDqStageOutputsList>(ctx, target.Pos())
                .Add<TDqSink>()
                    .DataSink(dataSink)
                    .Index().Value("0", TNodeFlags::Default).Build()
                    .Settings(sinkSettings)
                    .Build()
                .Done();

            return keys.empty()
                ? Build<TDqStage>(ctx, writePos)
                    .Inputs().Build()
                    .Program(stageProgram)
                    .Outputs(stageOutputs)
                    .Settings().Build()
                    .Done().Ptr()
                : Build<TDqStage>(ctx, writePos)
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
                    .Program(stageProgram)
                    .Outputs(stageOutputs)
                    .Settings().Build()
                    .Done().Ptr();
        }

        const auto dqUnionOutput = maybeUnionAll.Cast().Output();

        if (keys.empty()) {
            YQL_CLOG(INFO, ProviderS3) << "Rewrite S3WriteObject `" << cluster << "`.`" << target.Path().StringValue() << "` and push sink into existing stage.";

            const auto dqSink = Build<TDqSink>(ctx, writePos)
                .DataSink(dataSink)
                .Index(dqUnionOutput.Index())
                .Settings(sinkSettings)
                .Done();

            const auto inputStage = dqUnionOutput.Stage().Cast<TDqStage>();

            auto outputsBuilder = Build<TDqStageOutputsList>(ctx, writePos);
            if (const auto outputs = inputStage.Outputs()) {
                outputsBuilder.InitFrom(outputs.Cast());
                YQL_ENSURE(inputStage.Program().Body().Maybe<TDqReplicate>(), "Can not push multiple async outputs into stage without TDqReplicate");
            }
            outputsBuilder.Add(dqSink);

            const auto dqStageWithSink = Build<TDqStage>(ctx, inputStage.Pos())
                .InitFrom(inputStage)
                .Program(stageProgram)
                .Outputs(outputsBuilder.Done())
                .Done();

            optCtx.RemapNode(inputStage.Ref(), dqStageWithSink.Ptr());

            if (!wrapWithNth) {
                return dqStageWithSink.Ptr();
            }

            const auto dqResult = Build<TCoNth>(ctx, dqStageWithSink.Pos())
                .Tuple(dqStageWithSink)
                .Index(dqUnionOutput.Index())
                .Done();

            return ctx.NewList(dqStageWithSink.Pos(), {dqResult.Ptr()});
        }

        YQL_CLOG(INFO, ProviderS3) << "Rewrite S3WriteObject `" << cluster << "`.`" << target.Path().StringValue() << "` as sink in external stage.";
        YQL_ENSURE(!keys.empty());

        return Build<TDqStage>(ctx, writePos)
            .Inputs()
                .Add<TDqCnHashShuffle>()
                    .Output(dqUnionOutput)
                    .KeyColumns()
                        .Add(keys)
                        .Build()
                    .Build()
                .Build()
            .Program(stageProgram)
            .Settings().Build()
            .Outputs()
                .Add<TDqSink>()
                    .DataSink(dataSink)
                    .Index()
                        .Build(0)
                    .Settings(sinkSettings)
                    .Build()
                .Build()
            .Done().Ptr();
    }

    TMaybeNode<TExprBase> S3Insert(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const {
        auto insert = node.Cast<TS3Insert>();
        const auto resultPtr = BuildSinkStage(node.Pos(),
            insert.DataSink(),
            insert.Target(),
            insert.Input(),
            ctx,
            optCtx,
            getParents,
            /* wrapWithNth */ true
        );

        if (resultPtr) {
            return resultPtr;
        } else {
            return node;
        }
    }

    TMaybeNode<TExprBase> S3WriteObject(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const {
        auto write = node.Cast<TS3WriteObject>();
        const auto resultPtr = BuildSinkStage(node.Pos(),
            write.DataSink(),
            write.Target(),
            write.Input(),
            ctx,
            optCtx,
            getParents,
            /* wrapWithNth */ false
        );

        if (resultPtr) {
            const auto maybeStage = TMaybeNode<TDqStage>(resultPtr);
            YQL_ENSURE(maybeStage);
            return Build<TDqQuery>(ctx, write.Pos())
                .World(write.World())
                .SinkStages()
                    .Add(maybeStage.Cast())
                    .Build()
                .Done();
        } else {
            return node;
        }
    }

private:
    const TS3State::TPtr State_;
};

} // anonymous namespace

THolder<IGraphTransformer> CreateS3PhysicalOptProposalTransformer(TS3State::TPtr state) {
    return MakeHolder<TS3PhysicalOptProposalTransformer>(std::move(state));
}

} // namespace NYql
