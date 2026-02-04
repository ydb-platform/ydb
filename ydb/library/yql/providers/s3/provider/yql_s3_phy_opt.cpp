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
        const auto maybeUnionAll = input.Maybe<TDqCnUnionAll>();
        const bool pureDqExpr = IsDqPureExpr(input);
        if (!pureDqExpr && !maybeUnionAll) {
            // Wait until union all build for non pure DQ stage
            return {};
        }

        const auto& parents = *getParents();
        if (maybeUnionAll && !NDq::IsSingleConsumerConnection(maybeUnionAll.Cast(), parents)) {
            // Wait until union will be split with DqReplicate
            return {};
        }

        // Build DQ sink settings and S3 format serializer
        const auto& structType = *input.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        auto sink = BuildS3Sink(writePos, dataSink, target, structType, ctx);

        // Build DQ stage program with S3 serializer
        std::vector<TCoArgument> stageArgs;
        TNodeOnNodeOwnedMap stageArgsReplaces;
        TExprNode::TPtr stageBody;

        const auto& keys = GetPartitionKeys(GetPartitionBy(target.Settings().Ref()));
        if (!keys.empty()) {
            // Build external stage with one sink
            stageArgs.emplace_back(Build<TCoArgument>(ctx, writePos)
                .Name("in")
                .Done());
            stageBody = sink.BuildSerializer(stageArgs.back().Ptr(), ctx);
        } else if (pureDqExpr) {
            // Build stage with one sink from scratch
            stageBody = sink.BuildSerializer(Build<TCoToFlow>(ctx, writePos)
                .Input(input)
                .Done().Ptr(), ctx);
        } else {
            // Push sink into existing stage and rebuild stage lambda
            const auto& stage = maybeUnionAll.Cast().Output().Stage();
            const auto& program = stage.Program();
            const auto& args = program.Args();

            stageArgs.reserve(args.Size());
            stageArgsReplaces.reserve(args.Size());
            for (size_t i = 0; i < args.Size(); ++i) {
                auto newArg = Build<TCoArgument>(ctx, writePos)
                    .Name(TStringBuilder() << "in_" << i)
                    .Done();

                stageArgs.emplace_back(newArg);
                YQL_ENSURE(stageArgsReplaces.emplace(args.Arg(i).Raw(), newArg.Ptr()).second);
            }

            const auto& currentBody = program.Body();
            if (const auto branchesCount = GetStageOutputsCount(stage); branchesCount == 1) {
                // Just add serializer over stage body
                stageBody = sink.BuildSerializer(currentBody.Ptr(), ctx);
            } else {
                // Fuse serializer with corresponding DQ replicate output

                const auto& maybeDqReplicate = program.Body().Maybe<TDqReplicate>();
                YQL_ENSURE(maybeDqReplicate, "Can not push S3 sink into stage with multi output without DQ replicate");
                const auto& dqReplicate = maybeDqReplicate.Cast();

                TVector<TExprNode::TPtr> newBranchLambdas;
                newBranchLambdas.reserve(branchesCount);

                const ui64 outputIndex = FromString(maybeUnionAll.Cast().Output().Index().Value());
                for (ui32 i = 0; i < branchesCount; ++i) {
                    const auto& branchLambda = dqReplicate.Arg(/* input */ 1 + i).Cast<TCoLambda>();
                    YQL_ENSURE(branchLambda.Args().Size() == 1);

                    TExprNode::TPtr newBranchProgram;
                    if (i == outputIndex) {
                        const auto newArg = Build<TCoArgument>(ctx, writePos)
                            .Name(TStringBuilder() << "in_dq_replicate_" << i)
                            .Done();

                        newBranchProgram = Build<TCoLambda>(ctx, writePos)
                            .Args({newArg})
                            .Body(ctx.ReplaceNode(sink.BuildSerializer(branchLambda.Body().Ptr(), ctx), branchLambda.Args().Arg(0).Ref(), newArg.Ptr()))
                            .Done().Ptr();
                    } else {
                        newBranchProgram = branchLambda.Ptr();
                    }

                    newBranchLambdas.emplace_back(std::move(newBranchProgram));
                }

                stageBody = Build<TDqReplicate>(ctx, writePos)
                    .Input(dqReplicate.Input())
                    .FreeArgs()
                        .Add(newBranchLambdas)
                        .Build()
                    .Done().Ptr();
            }
        }

        const auto stageProgram = Build<TCoLambda>(ctx, writePos)
            .Args(stageArgs)
            .Body(ctx.ReplaceNodes(std::move(stageBody), stageArgsReplaces))
            .Done();

        // Build DQ stage with sink and corresponding connections

        const auto cluster = dataSink.Cluster().Value();
        if (pureDqExpr) {
            YQL_CLOG(INFO, ProviderS3) << "Rewrite pure S3WriteObject `" << cluster << "`.`" << target.Path().Value() << "` as stage with sink.";

            auto inputsBuilder = Build<TExprList>(ctx, writePos);
            if (!keys.empty()) {
                inputsBuilder.Add<TDqCnHashShuffle>()
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
                        .Index().Build(0)
                        .Build()
                    .KeyColumns()
                        .Add(keys)
                        .Build()
                    .Build();
            }

            return Build<TDqStage>(ctx, writePos)
                .Inputs(inputsBuilder.Done())
                .Program(stageProgram)
                .Outputs()
                    .Add(sink.BuildSink(0, ctx, writePos))
                    .Build()
                .Settings().Build()
                .Done().Ptr();
        }

        const auto dqUnionOutput = maybeUnionAll.Cast().Output();
        if (keys.empty()) {
            YQL_CLOG(INFO, ProviderS3) << "Rewrite S3WriteObject `" << cluster << "`.`" << target.Path().StringValue() << "` and push sink into existing stage.";

            const auto inputStage = dqUnionOutput.Stage().Cast<TDqStage>();

            auto outputsBuilder = Build<TDqStageOutputsList>(ctx, writePos);
            if (const auto outputs = inputStage.Outputs()) {
                outputsBuilder.InitFrom(outputs.Cast());
                YQL_ENSURE(inputStage.Program().Body().Maybe<TDqReplicate>(), "Can not push multiple async outputs into stage without TDqReplicate");
            }
            outputsBuilder.Add(sink.BuildSink(FromString<ui64>(dqUnionOutput.Index().Value()), ctx, writePos));

            const auto dqStageWithSink = Build<TDqStage>(ctx, inputStage.Pos())
                .InitFrom(inputStage)
                .Program(stageProgram)
                .Outputs(outputsBuilder.Done())
                .Done();

            // Because stage type annotation was changed due to S3 serializer
            // we should replace stage parent nodes (they type annotation should not change)
            if (const auto parentsIt = parents.find(inputStage.Raw()); parentsIt != parents.end()) {
                for (const auto* parent : parentsIt->second) {
                    if (parent == dqUnionOutput.Raw()) {
                        continue;
                    }

                    TExprNode::TListType newChildren;
                    newChildren.reserve(parent->ChildrenSize());

                    for (const auto& child : parent->Children()) {
                        if (child.Get() == inputStage.Raw()) {
                            newChildren.emplace_back(dqStageWithSink.Ptr());
                        } else {
                            newChildren.emplace_back(child);
                        }
                    }

                    optCtx.RemapNode(*parent, ctx.ChangeChildren(*parent, std::move(newChildren)));
                }
            }

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
                .Add(sink.BuildSink(0, ctx, writePos))
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
        }

        return node;
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

        if (!resultPtr) {
            return node;
        }

        const auto maybeStage = TMaybeNode<TDqStage>(resultPtr);
        YQL_ENSURE(maybeStage);

        return Build<TDqQuery>(ctx, write.Pos())
            .World(write.World())
            .SinkStages()
                .Add(maybeStage.Cast())
                .Build()
            .Done();
    }

private:
    struct TS3SinkInfo {
        TS3SinkInfo(const TS3SinkSettings& dqSinkSettings, TExprNode::TPtr serializer, const TCoArgument& serializerArgument, const TS3DataSink& dataSink)
            : DqSinkSettings(dqSinkSettings)
            , Serializer(std::move(serializer))
            , SerializerArgument(serializerArgument)
            , DataSink(dataSink)
        {}

        TExprNode::TPtr BuildSerializer(TExprNode::TPtr input, TExprContext& ctx) {
            YQL_ENSURE(Serializer, "Can not build serializer twice");
            auto result = ctx.ReplaceNode(std::move(Serializer), SerializerArgument.Ref(), std::move(input));
            Serializer = nullptr;
            return result;
        }

        TDqSink BuildSink(ui64 index, TExprContext& ctx, TPositionHandle pos) const {
            return Build<TDqSink>(ctx, pos)
                .Index().Build(index)
                .DataSink(DataSink)
                .Settings(DqSinkSettings)
                .Done();
        }

    private:
        const TS3SinkSettings DqSinkSettings;
        TExprNode::TPtr Serializer;
        const TCoArgument SerializerArgument;
        const TS3DataSink DataSink;
    };

    TS3SinkInfo BuildS3Sink(TPositionHandle writePos, const TS3DataSink& dataSink, const TS3Target& target, const NYql::TStructExprType& rowType, TExprContext& ctx) const {
        const auto& settings = target.Settings().Ref();
        auto sinkSettingsBuilder = Build<TExprList>(ctx, target.Pos());
        if (auto partBy = GetPartitionBy(settings)) {
            sinkSettingsBuilder.Add(std::move(partBy));
        }

        const auto compression = GetCompression(settings);
        if (compression) {
            sinkSettingsBuilder.Add(compression);
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

        TExprNode::TPtr serializer;
        auto argument = Build<TCoArgument>(ctx, writePos)
            .Name("serializer_input")
            .Done();

        const auto& keys = GetPartitionKeys(GetPartitionBy(target.Settings().Ref()));
        if (TString error; !UseBlocksSink(format, keys, &rowType, State_->Configuration, error)){
            // Build CH udf serializer
            YQL_ENSURE(!error, "Got block sink error: " << error);
            serializer = Build<TS3SinkOutput>(ctx, writePos)
                .Input(argument)
                .Format().Build(format)
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

            serializer = Build<TCoToFlow>(ctx, writePos)
                .Input(argument)
                .Done().Ptr();

            TVector<TString> columns;
            columns.reserve(rowType.GetSize());
            for (const auto& item : rowType.GetItems()) {
                columns.emplace_back(item->GetName());
            }
            serializer = MakeExpandMap(writePos, columns, serializer, ctx);

            serializer = Build<TCoWideToBlocks>(ctx, writePos)
                .Input<TCoFromFlow>()
                    .Input(serializer)
                    .Build()
                .Done().Ptr();
        }

        auto sinkSettings = Build<TS3SinkSettings>(ctx, writePos)
            .Path(target.Path())
            .Settings(sinkSettingsBuilder.Done())
            .Token<TCoSecureParam>()
                .Name().Build(TStringBuilder() << "cluster:default_" << dataSink.Cluster().Value())
                .Build()
            .Extension().Build(GetExtension(format, compression ? compression->Tail().Content() : ""sv))
            .RowType(ExpandType(writePos, rowType, ctx))
            .Done();

        return TS3SinkInfo(sinkSettings, serializer, argument, dataSink);
    }

    const TS3State::TPtr State_;
};

} // anonymous namespace

THolder<IGraphTransformer> CreateS3PhysicalOptProposalTransformer(TS3State::TPtr state) {
    return MakeHolder<TS3PhysicalOptProposalTransformer>(std::move(state));
}

} // namespace NYql
