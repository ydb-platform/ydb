#include "yql_dq_exectransformer.h"

#include <ydb/library/yql/providers/dq/provider/yql_dq_datasource.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_state.h>

#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/transform/yql_exec.h>
#include <ydb/library/yql/providers/common/transform/yql_lazy_init.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>

#include <ydb/library/yql/providers/dq/opt/dqs_opt.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/dq/actors/proto_builder.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/providers/dq/planner/execution_planner.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_control.h>

#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <ydb/library/yql/dq/tasks/dq_task_program.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt_build.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/core/services/yql_out_transformers.h>

#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/aligned_page_pool.h>

#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/threading/future/future.h>

#include <util/system/env.h>
#include <util/generic/size_literals.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/folder/dirut.h>

#include <memory>
#include <vector>

namespace NYql {

using namespace NCommon;
using namespace NKikimr::NMiniKQL;
using namespace NNodes;
using namespace NThreading;

namespace {

using TUploadList = IDqGateway::TUploadList;

// TODO: move this to separate file
class TLocalExecutor: public TCounters
{
public:
    TLocalExecutor(const TDqStatePtr& state)
        : State(state)
    { }

    // TODO: move this to separate thread-pool
    IDqGateway::TResult Execute(TPosition pos, const TString& lambda, const TVector<TString>& columns,
        const THashMap<TString, TString>& secureParams, const IDataProvider::TFillSettings& fillSettings)
    {
        try {
            return ExecuteUnsafe(lambda, columns, secureParams, fillSettings);
        } catch (const NKikimr::TMemoryLimitExceededException& e) {
            auto res = ResultFromError<IDqGateway::TResult>(TStringBuilder()
                << "DQ computation exceeds the memory limit " << State->Settings->MemoryLimit.Get().GetOrElse(0) << ". Try to increase the limit using PRAGMA dq.MemoryLimit", pos);
            return res;
        } catch (const std::exception& e) {
            return ResultFromException<IDqGateway::TResult>(e, pos);
        } catch (...) {
            auto res = ResultFromError<IDqGateway::TResult>(CurrentExceptionMessage(), pos);
            res.SetStatus(TIssuesIds::UNEXPECTED);
            return res;
        }
    }

    IDqGateway::TResult ExecuteUnsafe(const TString& lambda, const TVector<TString>& columns,
        const THashMap<TString, TString>& secureParams, const IDataProvider::TFillSettings& fillSettings)
    {
        auto t = TInstant::Now();
        IDqGateway::TResult result;
        NDqProto::TDqTask task;
        task.SetId(0);
        task.SetStageId(0);

        auto& program = *task.MutableProgram();
        program.SetRuntimeVersion(NYql::NDqProto::ERuntimeVersion::RUNTIME_VERSION_YQL_1_0);
        program.SetRaw(lambda);

        auto outputDesc = task.AddOutputs();
        outputDesc->MutableMap();

        auto channelDesc = outputDesc->AddChannels();
        channelDesc->SetId(0);
        channelDesc->SetSrcTaskId(1);
        channelDesc->SetDstTaskId(0);

        // TODO: remove this
        auto deterministicMode = !!GetEnv("YQL_DETERMINISTIC_MODE");
        auto timeProvider = deterministicMode
            ? CreateDeterministicTimeProvider(10000000)
            : CreateDefaultTimeProvider();
        auto randomProvider = deterministicMode
            ? CreateDeterministicRandomProvider(1)
            : State->RandomProvider;

        auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(
            __LOCATION__,
            NKikimr::TAlignedPagePoolCounters(),
            State->FunctionRegistry->SupportsSizedAllocators(),
            false);
        NDq::TDqTaskRunnerContext executionContext;
        executionContext.FuncRegistry = State->FunctionRegistry;

        executionContext.ComputationFactory = State->ComputationFactory;
        executionContext.RandomProvider = randomProvider.Get();
        executionContext.TimeProvider = timeProvider.Get();
        executionContext.FuncProvider = State->TransformFactory({}, executionContext.FuncRegistry);

        NDq::TDqTaskRunnerMemoryLimits limits;
        limits.ChannelBufferSize = 10_MB;
        limits.OutputChunkMaxSize = 2_MB;

        NDq::TDqTaskRunnerSettings settings;
        settings.OptLLVM = "OFF"; // Don't use LLVM for local execution
        settings.SecureParams = secureParams;
        settings.StatsMode = NDqProto::DQ_STATS_MODE_BASIC;
        auto runner = NDq::MakeDqTaskRunner(alloc, executionContext, settings, {});
        auto runnerSettings = NDq::TDqTaskSettings(&task);

        {
            auto guard = runner->BindAllocator(State->Settings->MemoryLimit.Get().GetOrElse(0));
            NDq::TDqTaskRunnerExecutionContextDefault execCtx;
            runner->Prepare(runnerSettings, limits, execCtx);
        }

        TVector<NDq::TDqSerializedBatch> rows;
        ui64 totalSize = 0;
        ui64 totalRows = 0;
        {
            auto guard = runner->BindAllocator(State->Settings->MemoryLimit.Get().GetOrElse(0));
            YQL_CLOG(DEBUG, ProviderDq) << " NDq::ERunStatus " << runner->Run();

            NDq::ERunStatus status;
            while ((status = runner->Run()) == NDq::ERunStatus::PendingOutput || status == NDq::ERunStatus::Finished) {
                if (!fillSettings.Discard) {
                    NDq::TDqSerializedBatch data;
                    while (runner->GetOutputChannel(0)->Pop(data)) {
                        totalSize += data.Size();
                        totalRows += data.RowCount();
                        rows.push_back(std::move(data));
                        if (!fillSettings.Discard) {
                            if (fillSettings.AllResultsBytesLimit && totalSize >= *fillSettings.AllResultsBytesLimit) {
                                result.Truncated = true;
                                break;
                            }
                            if (fillSettings.RowsLimitPerWrite && totalRows >= *fillSettings.RowsLimitPerWrite) {
                                result.Truncated = true;
                                break;
                            }
                        }

                        data = {};
                    }
                }
                if (status == NDq::ERunStatus::Finished || result.Truncated) {
                    break;
                }
            }

            YQL_ENSURE(status == NDq::ERunStatus::Finished || status == NDq::ERunStatus::PendingOutput || result.Truncated);
        }

        auto serializedResultType = GetSerializedResultType(lambda);
        NYql::NDqs::TProtoBuilder protoBuilder(serializedResultType, columns);

        bool ysonTruncated = false;
        result.Data = protoBuilder.BuildYson(std::move(rows), fillSettings.AllResultsBytesLimit.GetOrElse(Max<ui64>()),
            fillSettings.RowsLimitPerWrite.GetOrElse(Max<ui64>()), &ysonTruncated);

        result.Truncated = result.Truncated || ysonTruncated;
        AddCounter("LocalRun", TInstant::Now() - t);

        FlushStatisticsToState();

        result.SetSuccess();

        return result;
    }

private:
    void FlushStatisticsToState() {
        TOperationStatistics statistics;
        FlushCounters(statistics);

        TGuard<TMutex> lock(State->Mutex);
        if (!statistics.Entries.empty()) {
            State->Statistics[State->MetricId++] = statistics;
        }
    }

    TDqStatePtr State;
};

struct TUploadCache {
    THashMap<TString, TFileLinkPtr> FileLinks;
    THashMap<TString, TString> ModulesMapping;

    using TPtr = std::shared_ptr<TUploadCache>;
};

struct TPublicIds {
    THashMap<ui32, ui32> AllPublicIds;
    THashMap<ui64, ui32> Stage2publicId;
    size_t GraphsCount = 0;

    using TPtr = std::shared_ptr<TPublicIds>;
};

NDq::EChannelMode GetConfiguredChannelMode(const TDqStatePtr& state, const TTypeAnnotationContext& typesCtx) {
    const bool useWideChannels = state->Settings->UseWideChannels.Get().GetOrElse(typesCtx.BlockEngineMode != EBlockEngineMode::Disable);
    const TMaybe<bool> useChannelBlocks = state->Settings->UseWideBlockChannels.Get();
    NDq::EChannelMode mode;
    if (!useWideChannels) {
        mode = NDq::EChannelMode::CHANNEL_SCALAR;
    } else if (useChannelBlocks.Defined()) {
        mode = *useChannelBlocks ? NDq::EChannelMode::CHANNEL_WIDE_FORCE_BLOCK : NDq::EChannelMode::CHANNEL_WIDE_SCALAR;
    } else {
        switch (typesCtx.BlockEngineMode) {
            case NYql::EBlockEngineMode::Auto:
                mode = NDq::EChannelMode::CHANNEL_WIDE_AUTO_BLOCK;
                break;
            case NYql::EBlockEngineMode::Force:
                mode = NDq::EChannelMode::CHANNEL_WIDE_FORCE_BLOCK;
                break;
            case NYql::EBlockEngineMode::Disable:
                mode = NDq::EChannelMode::CHANNEL_WIDE_SCALAR;
                break;
        }
    }
    return mode;
}

struct TDqsPipelineConfigurator : public IPipelineConfigurator {
public:
    TDqsPipelineConfigurator(const TDqStatePtr& state, const THashMap<TString, TString>& providerParams)
        : State_(state)
        , ProviderParams_(providerParams)
        , UniqIntegrations_(GetUniqueIntegrations(*State_->TypeCtx))
    {}

private:
    void AfterCreate(TTransformationPipeline*) const final {}

    void AfterTypeAnnotation(TTransformationPipeline* pipeline) const final {
        // First truncate graph by calculated precomputes
        pipeline->Add(NDqs::CreateDqsReplacePrecomputesTransformer(*pipeline->GetTypeAnnotationContext()), "ReplacePrecomputes");

        // Then apply provider specific transformers on truncated graph
        std::for_each(UniqIntegrations_.cbegin(), UniqIntegrations_.cend(), [&](const auto dqInt) {
            dqInt->ConfigurePeepholePipeline(true, ProviderParams_, pipeline);
        });

        TTypeAnnotationContext& typesCtx = *pipeline->GetTypeAnnotationContext();
        if (State_->Settings->UseBlockReader.Get().GetOrElse(typesCtx.BlockEngineMode != EBlockEngineMode::Disable)) {
            pipeline->Add(NDqs::CreateDqsRewritePhyBlockReadOnDqIntegrationTransformer(typesCtx), "ReplaceWideReadsWithBlock");
        }
        NDq::EChannelMode mode = GetConfiguredChannelMode(State_, typesCtx);
        pipeline->Add(
            NDq::CreateDqBuildPhyStagesTransformer(!State_->Settings->SplitStageOnDqReplicate.Get().GetOrElse(TDqSettings::TDefault::SplitStageOnDqReplicate), typesCtx, mode),
            "BuildPhy");
        pipeline->Add(NDqs::CreateDqsRewritePhyCallablesTransformer(*pipeline->GetTypeAnnotationContext()), "RewritePhyCallables");
    }

    void AfterOptimize(TTransformationPipeline* pipeline) const final {
        std::for_each(UniqIntegrations_.cbegin(), UniqIntegrations_.cend(), [&](const auto dqInt) {
            dqInt->ConfigurePeepholePipeline(false, ProviderParams_, pipeline);
        });
    }

private:
    TDqStatePtr State_;
    THashMap<TString, TString> ProviderParams_;
    std::unordered_set<IDqIntegration*> UniqIntegrations_;
};

TExprNode::TPtr DqMarkBlockStage(const TDqStatePtr& state, const TPublicIds::TPtr& publicIds, const TDqPhyStage& stage, TExprContext& ctx) {
    using NDq::TDqStageSettings;
    TDqStageSettings settings = NDq::TDqStageSettings::Parse(stage);
    if (settings.BlockStatus.Defined()) {
        return stage.Ptr();
    }

    TExprNode::TPtr root = stage.Program().Body().Ptr();

    // scalar channel as output
    if (root->IsCallable("FromFlow")) {
        root = root->HeadPtr();
    }
    if (root->IsCallable("WideFromBlocks")) {
        root = root->HeadPtr();
    }

    const TTypeAnnotationNode* nodeType = root->GetTypeAnn();
    YQL_ENSURE(nodeType);
    auto blockStatus = IsWideSequenceBlockType(*nodeType) ? TDqStageSettings::EBlockStatus::Full : TDqStageSettings::EBlockStatus::None;
    bool stop = false;

    VisitExpr(root, [&](const TExprNode::TPtr& node) {
        if (stop || node->IsLambda() || node->IsArgument()) {
            return false;
        }

        if (node->IsCallable("WideToBlocks") && node->Head().IsCallable("ToFlow") && node->Head().Head().IsArgument()) {
            // scalar channel as input
            return false;
        }

        const TTypeAnnotationNode* nodeType = node->GetTypeAnn();
        YQL_ENSURE(nodeType);

        if (nodeType->GetKind() != ETypeAnnotationKind::Stream && nodeType->GetKind() != ETypeAnnotationKind::Flow) {
            return false;
        }

        const bool isBlock = IsWideSequenceBlockType(*nodeType);
        if (blockStatus == TDqStageSettings::EBlockStatus::Full && !isBlock ||
            blockStatus == TDqStageSettings::EBlockStatus::None && isBlock)
        {
            blockStatus = TDqStageSettings::EBlockStatus::Partial;
        }

        if (blockStatus == TDqStageSettings::EBlockStatus::Partial) {
            stop = true;
            return false;
        }

        return true;
    });

    auto publicId = publicIds->Stage2publicId.find(settings.LogicalId);
    TString publicIdMsg;
    if (publicId != publicIds->Stage2publicId.end()) {
        publicIdMsg = TStringBuilder() << " (public id #" << publicId->second << ")";
        auto p = TOperationProgress(TString(DqProviderName), publicId->second, TOperationProgress::EState::InProgress);
        switch (blockStatus) {
        case TDqStageSettings::EBlockStatus::None: p.BlockStatus = TOperationProgress::EOpBlockStatus::None; break;
        case TDqStageSettings::EBlockStatus::Partial: p.BlockStatus = TOperationProgress::EOpBlockStatus::Partial; break;
        case TDqStageSettings::EBlockStatus::Full: p.BlockStatus = TOperationProgress::EOpBlockStatus::Full; break;
        }
        state->ProgressWriter(p);
    }

    YQL_CLOG(INFO, CoreDq) << "Setting block status for stage #" << settings.LogicalId << publicIdMsg << " to " << ToString(blockStatus);
    return Build<TDqPhyStage>(ctx, stage.Pos())
        .InitFrom(stage)
        .Settings(settings.SetBlockStatus(blockStatus).BuildNode(ctx, stage.Settings().Pos()))
        .Done().Ptr();
}

struct TDqsFinalPipelineConfigurator : public IPipelineConfigurator {
public:
    explicit TDqsFinalPipelineConfigurator(const TDqStatePtr& state, const TPublicIds::TPtr& publicIds)
        : State_(state)
        , PublicIds_(publicIds)
    {
    }
private:
    void AfterCreate(TTransformationPipeline*) const final {}

    void AfterTypeAnnotation(TTransformationPipeline*) const final {}

    void AfterOptimize(TTransformationPipeline* pipeline) const final {
        auto typeCtx = pipeline->GetTypeAnnotationContext();
        NDq::EChannelMode mode = GetConfiguredChannelMode(State_, *typeCtx);
        pipeline->Add(NDq::CreateDqBuildWideBlockChannelsTransformer(*typeCtx, mode),
            "DqBuildWideBlockChannels",
            TIssuesIds::DEFAULT_ERROR);
        pipeline->Add(CreateFunctorTransformer(
            [typeCtx, state = State_, publicIds = PublicIds_](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                TOptimizeExprSettings optSettings{typeCtx.Get()};
                optSettings.VisitLambdas = false;
                return OptimizeExpr(input, output, [state, publicIds](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                    TExprBase expr{node};
                    if (auto stage = expr.Maybe<TDqPhyStage>()) {
                        return DqMarkBlockStage(state, publicIds, stage.Cast(), ctx);
                    }
                    return node;
                }, ctx, optSettings);
            }
        ),
        "DqMarkBlockStages",
        TIssuesIds::DEFAULT_ERROR);
    }
    const TDqStatePtr State_;
    const TPublicIds::TPtr PublicIds_;
};

class TSimpleSkiffConverter : public ISkiffConverter {
public:
    TString ConvertNodeToSkiff(const TDqStatePtr /*state*/, const IDataProvider::TFillSettings& /*fillSettings*/, const NYT::TNode& /*rowSpec*/, const NYT::TNode& /*item*/, const TVector<TString>& /*columns*/) override {
        Y_ABORT("not implemented");
    }

    TYtType ParseYTType(const TExprNode& /*node*/, TExprContext& /*ctx*/, const TMaybe<NYql::TColumnOrder>& /*columns*/) override {
        Y_ABORT("not implemented");
    }
};

class TDqExecTransformer: public TExecTransformerBase, TCounters
{
public:
    TDqExecTransformer(const TDqStatePtr& state, const ISkiffConverterPtr& skiffConverter)
        : State(state)
        , SkiffConverter(skiffConverter)
        , ExecPrecomputeState_(MakeIntrusive<TExecPrecomputeState>())
        , UploadCache_(std::make_shared<TUploadCache>())
    {
        AddHandler({TStringBuf("Result")}, RequireNone(), Hndl(&TDqExecTransformer::HandleResult));
        AddHandler({TStringBuf("Pull")}, RequireNone(), Hndl(&TDqExecTransformer::HandlePull));
        AddHandler({TDqCnResult::CallableName()}, RequireNone(), Pass());
        AddHandler({TDqQuery::CallableName()}, RequireFirst(), Pass());
    }

    void Rewind() override {
        ExecPrecomputeState_ = MakeIntrusive<TExecPrecomputeState>();
        UploadCache_ = std::make_shared<TUploadCache>();

        TExecTransformerBase::Rewind();
    }

private:
    struct TExecPrecomputeState : public TThrRefBase {
        TAdaptiveLock Lock;

        struct TItem : public TIntrusiveListItem<TItem> {
            TExprNode* Node = nullptr;
            TAsyncTransformCallback Callback;
        };

        using TQueueType = TIntrusiveListWithAutoDelete<TExecPrecomputeState::TItem, TDelete>;
        TQueueType Completed;
        TNodeMap<NThreading::TFuture<void>> PrecomputeFutures; // Precompute node -> future
    };

    using TExecPrecomputeStatePtr = TIntrusivePtr<TExecPrecomputeState>;

    void GetResultType(TString* type, TVector<TString>* columns, const TExprNode& resOrPull, const TExprNode& resOrPullInput) const
    {
        *columns = NCommon::GetResOrPullColumnHints(resOrPull);
        if (columns->empty()) {
            *columns = NCommon::GetStructFields(resOrPullInput.GetTypeAnn());
        }

        if (NCommon::HasResOrPullOption(resOrPull, "type")) {
            TStringStream typeYson;
            NYson::TYsonWriter typeWriter(&typeYson);
            NCommon::WriteResOrPullType(typeWriter, resOrPullInput.GetTypeAnn(), TColumnOrder(*columns));
            *type = typeYson.Str();
        }
    }

    TExprNode::TPtr WrapLambdaBody(int& level, TExprNode::TPtr node, TExprContext& ctx) const {
        const auto kind = node->GetTypeAnn()->GetKind();
        const bool data = kind != ETypeAnnotationKind::Flow && kind != ETypeAnnotationKind::List && kind != ETypeAnnotationKind::Stream && kind != ETypeAnnotationKind::Optional;
        level = data ? 1 : 0;
        return ctx.WrapByCallableIf(kind != ETypeAnnotationKind::Stream, "ToStream", ctx.WrapByCallableIf(data, "Just", std::move(node)));
    }

    std::tuple<TString, TString> GetPathAndObjectId(const TString& path, const TString& objectId, const TString& md5) const {
        if (path.StartsWith(NKikimr::NMiniKQL::StaticModulePrefix)
            || !State->Settings->EnableStrip.Get() || !State->Settings->EnableStrip.Get().GetOrElse(false))
        {
            UploadCache_->ModulesMapping.emplace(objectId, path);
            return std::make_tuple(path, objectId);
        }

        TFileLinkPtr& fileLink = UploadCache_->FileLinks[objectId];
        if (!fileLink) {
            fileLink = State->FileStorage->PutFileStripped(path, md5);
        }

        UploadCache_->ModulesMapping.emplace(objectId  + DqStrippedSuffied, path);

        return std::make_tuple(fileLink->GetPath(), objectId + DqStrippedSuffied);
    }

    std::tuple<TString, TString> GetPathAndObjectId(const TFilePathWithMd5& pathWithMd5) const {
        if (pathWithMd5.Md5.empty()) {
            YQL_CLOG(WARN, ProviderDq) << "Empty md5 for " << pathWithMd5.Path;
        }
        return GetPathAndObjectId(pathWithMd5.Path,
            (pathWithMd5.Md5.empty() && !pathWithMd5.Path.StartsWith(NKikimr::NMiniKQL::StaticModulePrefix))
            ? MD5::File(pathWithMd5.Path) /* used for local run only */
            : pathWithMd5.Md5,
            pathWithMd5.Md5);
    }

    bool BuildUploadList(
        TUploadList* uploadList,
        bool localRun,
        TString* lambda,
        TTypeEnvironment& typeEnv,
        TUserDataTable& files) const
    {
        auto root = DeserializeRuntimeNode(*lambda, typeEnv);
        TExploringNodeVisitor explorer;
        explorer.Walk(root.GetNode(), typeEnv);
        auto ret = BuildUploadList(uploadList, localRun, explorer, typeEnv, files);
        *lambda = SerializeRuntimeNode(root, typeEnv);
        return ret;
    }

    bool BuildUploadList(
        TUploadList* uploadList,
        bool localRun,
        TExploringNodeVisitor& explorer,
        TTypeEnvironment& typeEnv,
        TUserDataTable& files) const
    {
        if (!localRun) {
            for (const auto& file : files) {
                const auto& fileName = file.first.Alias();
                const auto& block = file.second;
                if (fileName == NCommon::PgCatalogFileName || block.Usage.Test(EUserDataBlockUsage::PgExt)) {
                    auto f = IDqGateway::TFileResource();
                    auto filePath = block.FrozenFile->GetPath().GetPath();
                    f.SetLocalPath(RealPath(filePath));
                    f.SetName(fileName);
                    f.SetObjectId(block.FrozenFile->GetMd5());
                    f.SetObjectType(IDqGateway::TFileResource::EUSER_FILE);
                    f.SetSize(block.FrozenFile->GetSize());
                    uploadList->emplace(f);
                }
            }
        }

        if (!State->Settings->_SkipRevisionCheck.Get().GetOrElse(false)) {
            if (State->VanillaJobPath.empty()) {
                auto f = IDqGateway::TFileResource();
                f.SetName("dq_vanilla_job.lite");
                f.SetObjectId(GetProgramCommitId());
                f.SetObjectType(IDqGateway::TFileResource::EEXE_FILE);
                uploadList->emplace(f);
            } else {
                auto f = IDqGateway::TFileResource();
                f.SetName("dq_vanilla_job.lite");
                TString path = State->VanillaJobPath;
                TString objectId = GetProgramCommitId();
                std::tie(path, objectId) = GetPathAndObjectId(path, objectId, State->VanillaJobMd5);
                f.SetObjectId(objectId);
                f.SetLocalPath(path);
                f.SetObjectType(IDqGateway::TFileResource::EEXE_FILE);
                f.SetSize(TFile(path, OpenExisting | RdOnly).GetLength());
                uploadList->emplace(f);
            }
        }
        bool fallbackFlag = false;
        for (TNode* node : explorer.GetNodes()) {
            node->Freeze(typeEnv);

            if (node->GetType()->IsCallable()) {
                auto& callable = static_cast<NKikimr::NMiniKQL::TCallable&>(*node);
                if (!callable.HasResult()) {
                    const auto& callableType = callable.GetType();
                    const auto& name = callableType->GetNameStr();
                    if (name == TStringBuf("FolderPath"))
                    {
                        const TString folderName(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
                        auto blocks = TUserDataStorage::FindUserDataFolder(files, folderName);
                        MKQL_ENSURE(blocks, "Folder not found: " << folderName);
                        for ( const auto& b : *blocks) {
                            auto block = b.second;
                            auto filePath = block->FrozenFile->GetPath().GetPath();
                            auto fullFileName = localRun ? filePath : TUserDataStorage::MakeRelativeName(b.first.Alias());
                            YQL_CLOG(DEBUG, ProviderDq) << "Path resolve " << filePath << "|"<< fullFileName;
                            // validate
                            switch (block->Type) {
                                case EUserDataType::URL:
                                case EUserDataType::PATH:
                                case EUserDataType::RAW_INLINE_DATA: {
                                    break;
                                }
                                default:
                                    YQL_ENSURE(false, "Unknown block type " << block->Type);
                            }
                            // filePath, fileName, md5
                            auto f = IDqGateway::TFileResource();
                            f.SetLocalPath(filePath);
                            f.SetName(fullFileName);
                            f.SetObjectId(block->FrozenFile->GetMd5());
                            f.SetSize(block->FrozenFile->GetSize());
                            f.SetObjectType(IDqGateway::TFileResource::EUSER_FILE);
                            uploadList->emplace(f);
                        }
                        const TProgramBuilder pgmBuilder(typeEnv, *State->FunctionRegistry);
                        auto result = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(folderName);
                        result.Freeze();
                        if (result.GetNode() != node) {
                            callable.SetResult(result, typeEnv);
                        }
                    } else if (name == TStringBuf("FileContent") || name == TStringBuf("FilePath")) {
                        const TString fileName(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());

                        auto block = TUserDataStorage::FindUserDataBlock(files, fileName);
                        MKQL_ENSURE(block, "File not found: " << fileName);

                        auto filePath = block->FrozenFile->GetPath().GetPath();
                        auto fullFileName = localRun ? filePath : fileName;

                        const TProgramBuilder pgmBuilder(typeEnv, *State->FunctionRegistry);
                        TRuntimeNode result;
                        bool doUpload = false;
                        switch (block->Type) {
                            case EUserDataType::URL:
                            case EUserDataType::PATH: {
                                TString content = fullFileName;
                                if (name == TStringBuf("FileContent")) {
                                    if (block->FrozenFile->GetSize() < MaxFileReadSize) {
                                        content = TFileInput(block->FrozenFile->GetPath()).ReadAll();
                                    } else {
                                        TCallableBuilder builder(typeEnv, TStringBuf("FileContentJob"), callable.GetType()->GetReturnType(), false);
                                        builder.Add(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(fullFileName));
                                        result = TRuntimeNode(builder.Build(), false);
                                        doUpload = true;
                                    }
                                }
                                if (!result)
                                    result = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(content);
                                break;
                            }
                            case EUserDataType::RAW_INLINE_DATA: {
                                TString content = (name == TStringBuf("FilePath"))
                                    ? fullFileName
                                    : block->Data;
                                result = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(content);
                                break;
                            }
                            default:
                                YQL_ENSURE(false, "Unknown block type " << block->Type);
                        }
                        result.Freeze();
                        if (result.GetNode() != node) {
                            callable.SetResult(result, typeEnv);
                        }
                        if (name == TStringBuf("FilePath") || doUpload) {
                            // filePath, fileName, md5
                            auto f = IDqGateway::TFileResource();
                            f.SetLocalPath(filePath);
                            f.SetName(fullFileName);
                            f.SetObjectId(block->FrozenFile->GetMd5());
                            f.SetObjectType(IDqGateway::TFileResource::EUSER_FILE);
                            f.SetSize(block->FrozenFile->GetSize());
                            uploadList->emplace(f);
                        }
                    } else if (name == TStringBuf("Udf") || name == TStringBuf("ScriptUdf")) {
                        const TString udfName(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
                        const auto moduleName = ModuleName(udfName);
                        TString customUdfPrefix;
                        const auto& udfModules = State->TypeCtx->UdfModules;
                        auto it = udfModules.find(moduleName);
                        if (it != udfModules.end()) {
                            customUdfPrefix = it->second.Prefix;
                        }

                        YQL_CLOG(DEBUG, ProviderDq) << "Try to resolve " << moduleName;
                        TMaybe<TFilePathWithMd5> udfPathWithMd5 = State->TypeCtx->UdfResolver->GetSystemModulePath(moduleName);
                        YQL_ENSURE(udfPathWithMd5.Defined());

                        TString filePath, objectId;
                        std::tie(filePath, objectId) = GetPathAndObjectId(*udfPathWithMd5);

                        YQL_CLOG(DEBUG, ProviderDq) << "File|Md5 " << filePath << "|" << objectId;

                        if (!filePath.StartsWith(NKikimr::NMiniKQL::StaticModulePrefix)) {
                            auto f = IDqGateway::TFileResource();
                            f.SetLocalPath(filePath);
                            f.SetName(ToString(moduleName));
                            f.SetObjectId(objectId);
                            f.SetObjectType(IDqGateway::TFileResource::EUDF_FILE);
                            f.SetSize(TFile(filePath, OpenExisting | RdOnly).GetLength());
                            f.SetCustomUdfPrefix(customUdfPrefix);
                            uploadList->emplace(f);
                        }

                        if (moduleName == TStringBuf("Geo")) {
                            TString fileName = "/home/geodata6.bin";
                            auto block = TUserDataStorage::FindUserDataBlock(files, fileName);
                            MKQL_ENSURE(block, "File not found: " << fileName);
                            auto f = IDqGateway::TFileResource();
                            f.SetLocalPath(block->FrozenFile->GetPath().GetPath());
                            f.SetName(fileName);
                            f.SetObjectId(block->FrozenFile->GetMd5());
                            f.SetObjectType(IDqGateway::TFileResource::EUSER_FILE);
                            f.SetSize(block->FrozenFile->GetSize());
                            uploadList->emplace(f);
                        }
                    }
                }
            }
        }

        i64 sizeSum = 0;
        for (const auto& f : *uploadList) {
            sizeSum += f.GetSize();
        }

        i64 dataLimit = static_cast<i64>(State->Settings->_MaxAttachmentsSize.Get().GetOrElse(TDqSettings::TDefault::MaxAttachmentsSize));
        if (sizeSum > dataLimit) {
            YQL_CLOG(WARN, ProviderDq) << "Too much data: " << sizeSum << " > " << dataLimit;
            fallbackFlag = true;
        }

        return fallbackFlag;
    }

    TStatusCallbackPair GetLambda(
        TString* lambda,
        bool* untrustedUdfFlag,
        TUploadList* uploadList,
        const TExprNode::TPtr& resInput, TExprContext& ctx,
        bool hasGraphParams,
        bool enableLocalRun) const
    {
        auto input = Build<TDqPhyStage>(ctx, resInput->Pos())
            .Inputs()
                .Build()
            .Program<TCoLambda>()
                .Args({})
                .Body(resInput)
            .Build()
            .Settings().Build()
        .Done().Ptr();

        // copy-paste {
        TUserDataTable crutches = State->TypeCtx->UserDataStorageCrutches;
        TUserDataTable files;
        StartCounter("FreezeUsedFiles");
        if (const auto filesRes = NCommon::FreezeUsedFiles(*input, files, *State->TypeCtx, ctx, [](const TString&){return true;}, crutches); filesRes.first.Level != TStatus::Ok) {
            if (filesRes.first.Level != TStatus::Error) {
                YQL_CLOG(DEBUG, ProviderDq) << "Freezing files for " << input->Content() << " (UniqueId=" << input->UniqueId() << ")";
            }
            return filesRes;
        }
        FlushCounter("FreezeUsedFiles");
        // copy-paste }

        TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), State->FunctionRegistry->SupportsSizedAllocators());
        TTypeEnvironment typeEnv(alloc);
        NCommon::TMkqlCommonCallableCompiler compiler;

        {
            auto block = MeasureBlock("BuildProgram");
            auto programLambda = TDqPhyStage(input).Program();

            TVector<TExprBase> fakeReads;
            auto paramsType = NDq::CollectParameters(programLambda, ctx);
            NDq::TSpillingSettings spillingSettings{State->Settings->GetEnabledSpillingNodes()};
            *lambda = NDq::BuildProgram(
                programLambda, *paramsType, compiler, typeEnv, *State->FunctionRegistry,
                ctx, fakeReads, spillingSettings);
        }

        auto block = MeasureBlock("RuntimeNodeVisitor");

        auto root = DeserializeRuntimeNode(*lambda, typeEnv);

        TExploringNodeVisitor explorer;
        explorer.Walk(root.GetNode(), typeEnv);
        *untrustedUdfFlag = false;

        for (TNode* node : explorer.GetNodes()) {
            if (node->GetType()->IsCallable()) {
                auto& callable = static_cast<NKikimr::NMiniKQL::TCallable&>(*node);
                if (!callable.HasResult()) {
                    const auto& callableType = callable.GetType();
                    const auto& name = callableType->GetNameStr();

                    if (name == TStringBuf("Udf") || name == TStringBuf("ScriptUdf")) {
                        const TString udfName(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
                        const auto moduleName = ModuleName(udfName);

                        *untrustedUdfFlag = *untrustedUdfFlag ||
                            callable.GetType()->GetName() == TStringBuf("ScriptUdf") ||
                            !State->FunctionRegistry->IsLoadedUdfModule(moduleName) ||
                            moduleName == TStringBuf("Geo");
                    }
                }
            }
        }

        const bool localRun = enableLocalRun && (!State->DqGateway || (!*untrustedUdfFlag && !State->TypeCtx->ForceDq && !hasGraphParams));
        bool fallbackFlag = BuildUploadList(uploadList, localRun, explorer, typeEnv, files);

        if (fallbackFlag) {
            YQL_CLOG(DEBUG, ProviderDq) << "Fallback: " << NCommon::ExprToPrettyString(ctx, *input);
            return Fallback();
        } else {
            *lambda = SerializeRuntimeNode(root, typeEnv);

            return SyncStatus(TStatus::Ok);
        }
    }

    static TStatus FallbackCallback(const TDqStatePtr& state, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx)
    {
        if (state->Metrics) {
            state->Metrics->IncCounter("dq", "Fallback");
        }
        state->Statistics[state->MetricId++].Entries.push_back(TOperationStatistics::TEntry("Fallback", 0, 0, 0, 0, 1));

        YQL_ENSURE(input->ChildrenSize() > 0, "Node: " << NCommon::ExprToPrettyString(ctx, *input));
        TExprNode::TPtr resFill = input->TailPtr();
        output = input;
        if (resFill->IsCallable("ResFill!")) {
            // TODO: change result provider to remove this if
            ui32 inMemoryIndex;
            for (inMemoryIndex = 0; inMemoryIndex < resFill->ChildrenSize(); ++inMemoryIndex) {
                if (resFill->ChildPtr(inMemoryIndex)->IsAtom(DqProviderName)) {
                    break;
                }
            }

            YQL_ENSURE(inMemoryIndex != resFill->ChildrenSize(), "Node: " << NCommon::ExprToPrettyString(ctx, *input));
            YQL_ENSURE(!state->TypeCtx->AvailablePureResultDataSources.empty());
            YQL_ENSURE(state->TypeCtx->AvailablePureResultDataSources.front() != DqProviderName);

            auto newAtom = ctx.NewAtom(input->Pos(), state->TypeCtx->AvailablePureResultDataSources.front());
            resFill->Child(inMemoryIndex)->SetState(TExprNode::EState::ExecutionComplete);
            resFill = ctx.ChangeChild(*resFill, inMemoryIndex, std::move(newAtom));

            input->Child(input->ChildrenSize()-1)->SetState(TExprNode::EState::ExecutionComplete);
            output = ctx.ChangeChild(*input, input->ChildrenSize()-1, std::move(resFill));
            return TStatus::Repeat;
        } else {
            YQL_ENSURE(!state->TypeCtx->AvailablePureResultDataSources.empty());
            YQL_ENSURE(state->TypeCtx->AvailablePureResultDataSources.front() != DqProviderName);

            TStringStream out;
            NYson::TYsonWriter writer((IOutputStream*)&out);
            writer.OnBeginMap();
            writer.OnKeyedItem("FallbackProvider");
            writer.OnRaw(state->TypeCtx->AvailablePureResultDataSources.front());
            writer.OnEndMap();

            output->SetResult(ctx.NewAtom(input->Pos(), out.Str()));
            return TStatus::Ok;
        }
    }

    TStatusCallbackPair Fallback() const {
        auto callback = TAsyncTransformCallback([state = State] (const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            return FallbackCallback(state, input, output, ctx);
        });
        return std::make_pair(TStatus::Async, NThreading::MakeFuture(callback));
    }

    TStatusCallbackPair HandleResult(const TExprNode::TPtr& input, TExprContext& ctx) {
        YQL_CLOG(DEBUG, ProviderDq) << "Executing " << input->Content() << " (UniqueId=" << input->UniqueId() << ")";

        if (State->ExternalUser && !State->Settings->_EnablePorto.Get().GetOrElse(TDqSettings::TDefault::EnablePorto)) {
            return Fallback();
        }

        TInstant startTime = TInstant::Now();
        ui64 executionTimeout = State->Settings->_LiteralTimeout.Get().GetOrElse(TDqSettings::TDefault::LiteralTimeout);

        try {
            auto result = TResult(input);

            THashMap<TString, TString> resSettings;
            for (auto s: result.Settings()) {
                if (auto val = s.Value().Maybe<TCoAtom>()) {
                    resSettings.emplace(s.Name().Value(), val.Cast().Value());
                }
            }

            auto statusPair = HandlePrecomputes(result, resSettings, ctx, executionTimeout);
            if (statusPair.first.Level != TStatus::Ok) {
                return statusPair;
            }

            IDataProvider::TFillSettings fillSettings = NCommon::GetFillSettings(result.Ref());
            auto settings = State->Settings->WithFillSettings(fillSettings);
            if (!settings->_RowsLimitPerWrite.Get() && !settings->_AllResultsBytesLimit.Get()) {
                settings->_AllResultsBytesLimit = 64_MB;
            }

            TPublicIds::TPtr publicIds = std::make_shared<TPublicIds>();
            int level;
            TExprNode::TPtr resInput = WrapLambdaBody(level, result.Input().Ptr(), ctx);
            {
                auto block = MeasureBlock("PeepHole");
                if (const auto status = PeepHole(resInput, resInput, ctx, resSettings, publicIds); status.Level != TStatus::Ok) {
                    return SyncStatus(status);
                }
            }

            THashMap<TString, TString> secureParams;
            NCommon::FillSecureParams(resInput, *State->TypeCtx, secureParams);

            auto graphParams = GatherGraphParams(resInput);
            bool hasGraphParams = !graphParams.empty();

            TString type;
            TVector<TString> columns;
            GetResultType(&type, &columns, result.Ref(), result.Input().Ref());

            VisitExpr(result.Ptr(), [&](const TExprNode::TPtr& node) {
                const TExprBase expr(node);
                if (expr.Maybe<TResFill>()) {
                    if (auto publicId = State->TypeCtx->TranslateOperationId(node->UniqueId())) {
                        publicIds->AllPublicIds.emplace(*publicId, 0U);
                    }
                }
                return true;
            });
            IDqGateway::TDqProgressWriter progressWriter = MakeDqProgressWriter(publicIds);
            bool enableFullResultWrite = settings->EnableFullResultWrite.Get().GetOrElse(false);
            if (enableFullResultWrite) {
                const auto type = result.Input().Ref().GetTypeAnn();
                const auto integration = GetDqIntegrationForFullResTable(State);
                enableFullResultWrite = type->GetKind() == ETypeAnnotationKind::List
                    && type->Cast<TListExprType>()->GetItemType()->GetKind() == ETypeAnnotationKind::Struct
                    && !fillSettings.Discard
                    && State->DqGateway
                    && integration
                    && integration->PrepareFullResultTableParams(result.Ref(), ctx, graphParams, secureParams);
                settings->EnableFullResultWrite = enableFullResultWrite;
            }

            TString lambda;
            bool untrustedUdfFlag;
            TUploadList uploadList;

            bool enableLocalRun = true;

            NThreading::TFuture<IDqGateway::TResult> future;
            bool localRun = false;
            // try to prepare lambda with localRun 'on' and 'off'
            for (int i = 0; i < 2 && !future.Initialized(); i++) {
                uploadList.clear();

                auto lambdaResult = GetLambda(
                    &lambda,
                    &untrustedUdfFlag,
                    &uploadList,
                    resInput,
                    ctx,
                    hasGraphParams,
                    enableLocalRun);

                if (lambdaResult.first.Level == TStatus::Error) {
                    if (State->Settings->FallbackPolicy.Get().GetOrElse(EFallbackPolicy::Default) == EFallbackPolicy::Never
                        || State->TypeCtx->ForceDq)
                    {
                        return SyncError();
                    }
                    return Fallback();
                }
                if (lambdaResult.first.Level != TStatus::Ok) {
                    return lambdaResult;
                }

                auto executionPlanner = THolder<IDqsExecutionPlanner>(
                    new TDqsSingleExecutionPlanner(
                        lambda, NActors::TActorId(),
                        NActors::TActorId(1, 0, 1, 0),
                        result.Input().Ref().GetTypeAnn()));
                auto& tasks = executionPlanner->GetTasks();
                Yql::DqsProto::TTaskMeta taskMeta;
                tasks[0].MutableMeta()->UnpackTo(&taskMeta);
                for (const auto& file : uploadList) {
                    *taskMeta.AddFiles() = file;
                }
                tasks[0].MutableMeta()->PackFrom(taskMeta);

                // bool executeUdfLocallyIfPossible ?
                localRun = enableLocalRun
                    && (!State->DqGateway
                        || (!untrustedUdfFlag && !State->TypeCtx->ForceDq && !hasGraphParams));
                if (localRun) {
                    auto result = TLocalExecutor(State).Execute(
                        ctx.GetPosition(input->Pos()), lambda, columns, secureParams, fillSettings);
                    if (enableFullResultWrite && result.Success() && result.Truncated) {
                        enableLocalRun = false; continue;
                    } else {
                        future = NThreading::MakeFuture<IDqGateway::TResult>(std::move(result));
                    }
                } else {
                    if (State->Metrics) {
                        State->Metrics->IncCounter("dq", "HandleResult");
                    }
                    State->Statistics[State->MetricId++].Entries.push_back(TOperationStatistics::TEntry("HandleResult", 0, 0, 0, 0, 1));

                    graphParams["Evaluation"] = ToString(!ctx.Step.IsDone(TExprStep::ExprEval));
                    future = State->ExecutePlan(
                        State->SessionId, executionPlanner->GetPlan(), columns, secureParams, graphParams,
                        settings, progressWriter, UploadCache_->ModulesMapping, fillSettings.Discard, executionTimeout);
                }
            }

            if (State->Metrics) {
                State->Metrics->IncCounter("dq", localRun
                    ? "InMemory"
                    : "Remote");
            }

            YQL_ENSURE(future.Initialized());

            FlushStatisticsToState();

            TString skiffType;
            NYT::TNode rowSpec;
            if (fillSettings.Format == IDataProvider::EResultFormat::Skiff) {
                auto parsedYtType =  SkiffConverter->ParseYTType(result.Input().Ref(), ctx, TColumnOrder(columns));

                type = parsedYtType.Type;
                rowSpec = parsedYtType.RowSpec;
                skiffType = parsedYtType.SkiffType;
            }

            return WrapFutureCallback<false>(future, [localRun, startTime, type, rowSpec, skiffType, fillSettings, level, settings, enableFullResultWrite, columns, graphParams, state = State, skiffConverter = SkiffConverter](const IDqGateway::TResult& res, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                YQL_CLOG(DEBUG, ProviderDq) << state->SessionId <<  " WrapFutureCallback";

                auto duration = TInstant::Now() - startTime;
                YQL_CLOG(INFO, ProviderDq) << "Execution Result complete, duration: " << duration;
                if (state->Metrics) {
                    state->Metrics->SetCounter("dq", "TotalExecutionTime", duration.MilliSeconds());
                    state->Metrics->SetCounter(
                        "dq",
                        localRun
                            ? "InMemoryExecutionTime"
                            : "RemoteExecutionTime",
                        duration.MilliSeconds());
                }

                state->Statistics[state->MetricId++] = res.Statistics;

                if (res.Fallback) {
                    if (state->Settings->FallbackPolicy.Get().GetOrElse(EFallbackPolicy::Default) == EFallbackPolicy::Never || state->TypeCtx->ForceDq) {
                        auto issues = TIssues{TIssue(ctx.GetPosition(input->Pos()), "Gateway Error").SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_WARNING)};
                        issues.AddIssues(res.Issues());
                        ctx.AssociativeIssues.emplace(input.Get(), std::move(issues));
                        return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Error);
                    }

                    YQL_CLOG(DEBUG, ProviderDq) << "Fallback from gateway: " << NCommon::ExprToPrettyString(ctx, *input);
                    TIssue warning(ctx.GetPosition(input->Pos()), "DQ cannot execute the query");
                    warning.Severity = TSeverityIds::S_INFO;
                    ctx.IssueManager.RaiseIssue(warning);

                    if (res.ForceFallback) {
                        state->Metrics->IncCounter("dq", "ForceFallback");
                    }
                    return FallbackCallback(state, input, output, ctx);
                }

                output = input;
                input->SetState(TExprNode::EState::ExecutionComplete);

                TStringStream out;
                NYson::TYsonWriter writer((IOutputStream*)&out);
                writer.OnBeginMap();

                if (skiffType) {
                    writer.OnKeyedItem("SkiffType");
                    writer.OnRaw(skiffType, ::NYson::EYsonType::Node);

                    writer.OnKeyedItem("Columns");
                    writer.OnBeginList();
                    for (auto& column: columns) {
                        writer.OnListItem();
                        writer.OnStringScalar(column);
                    }
                    writer.OnEndList();
                }

                if (type) {
                    writer.OnKeyedItem("Type");
                    writer.OnRaw(type);
                }

                writer.OnKeyedItem("Data");
                auto item = NYT::NodeFromYsonString(res.Data);
                for (int i = 0; i < level; ++i) {
                    item = item.AsList().at(0);
                }
                auto raw = NYT::NodeToYsonString(item);

                const bool truncated = res.Truncated;
                const ui64 rowsCount = res.RowsCount;

                if (truncated && item.IsList()) {
                    ui64 bytes = 0;
                    ui64 rows = 0;
                    switch (fillSettings.Format) {
                        case IDataProvider::EResultFormat::Yson: {
                            writer.OnBeginList();
                            for (auto& node : item.AsList()) {
                                raw = NYT::NodeToYsonString(node);
                                bytes += raw.size();
                                rows += 1;
                                writer.OnListItem();
                                writer.OnRaw(raw);
                                if (fillSettings.AllResultsBytesLimit && bytes >= *fillSettings.AllResultsBytesLimit) {
                                    break;
                                }
                                if (fillSettings.RowsLimitPerWrite && rows >= *fillSettings.RowsLimitPerWrite) {
                                    break;
                                }
                            }
                            writer.OnEndList();
                            break;
                        }
                        case IDataProvider::EResultFormat::Skiff: {
                            writer.OnStringScalar(skiffConverter->ConvertNodeToSkiff(state, fillSettings, rowSpec, item, columns));
                            break;
                        }
                        default: {
                            YQL_LOG_CTX_THROW yexception() << "Invalid result type: " << fillSettings.Format;
                        }
                    }

                    if (enableFullResultWrite) {
                        writer.OnKeyedItem("Ref");
                        writer.OnBeginList();
                        writer.OnListItem();
                        const auto integration = GetDqIntegrationForFullResTable(state);
                        YQL_ENSURE(integration);
                        integration->WriteFullResultTableRef(writer, columns, graphParams);
                        writer.OnEndList();
                    }
                    writer.OnKeyedItem("Truncated");
                    writer.OnBooleanScalar(true);
                } else if (truncated) {
                    switch (fillSettings.Format) {
                        case IDataProvider::EResultFormat::Yson: {
                            writer.OnRaw("[]");
                            break;
                        }
                        case IDataProvider::EResultFormat::Skiff: {
                            writer.OnStringScalar("");
                            break;
                        }
                        default: {
                            YQL_LOG_CTX_THROW yexception() << "Invalid result type: " << fillSettings.Format;
                        }
                    }
                    writer.OnKeyedItem("Truncated");
                    writer.OnBooleanScalar(true);
                } else {
                    switch (fillSettings.Format) {
                        case IDataProvider::EResultFormat::Yson: {
                            writer.OnRaw(raw);
                            break;
                        }
                        case IDataProvider::EResultFormat::Skiff: {
                            writer.OnStringScalar(skiffConverter->ConvertNodeToSkiff(state, fillSettings, rowSpec, item, columns));
                            break;
                        }
                        default: {
                            YQL_LOG_CTX_THROW yexception() << "Invalid result type: " << fillSettings.Format;
                        }
                    }
                }

                if (rowsCount) {
                    writer.OnKeyedItem("RowsCount");
                    writer.OnUint64Scalar(rowsCount);
                }

                writer.OnEndMap();

                input->SetResult(ctx.NewAtom(input->Pos(), out.Str()));
                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Ok);
            }, "");

        } catch (...) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), CurrentExceptionMessage()));
            return SyncError();
        }
    }

    IGraphTransformer::TStatus FallbackWithMessage(const TExprNode& node, TIssue issue, TExprContext& ctx, bool isRoot) {
        if (State->Metrics) {
            State->Metrics->IncCounter("dq", "Fallback");
        }
        State->Statistics[State->MetricId++].Entries.push_back(TOperationStatistics::TEntry("Fallback", 0, 0, 0, 0, 1));
        if (isRoot) {
            ctx.AssociativeIssues.emplace(&node, TIssues{std::move(issue)});
        } else {
            ctx.IssueManager.RaiseIssue(issue);
        }
        return IGraphTransformer::TStatus::Error;
    }

    IGraphTransformer::TStatus FallbackWithMessage(const TExprNode& node, const TString& message, TExprContext& ctx, bool isRoot) {
        return FallbackWithMessage(node, TIssue(ctx.GetPosition(node.Pos()), message).SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_WARNING), ctx, isRoot);
    }

    IGraphTransformer::TStatus FallbackWithMessage(const TExprNode& node, const TFallbackError& error, TExprContext& ctx, bool isRoot) {
        if (auto issue = error.GetIssue()) {
            return FallbackWithMessage(node, *issue, ctx, isRoot);
        } else {
            return FallbackWithMessage(node, error.what(), ctx, isRoot);
        }
    }

    TPublicIds::TPtr GetPublicIds(const TExprNode::TPtr& root) const {
        TPublicIds::TPtr publicIds = std::make_shared<TPublicIds>();
        VisitExpr(root, [&](const TExprNode::TPtr& node) {
            if (TResTransientBase::Match(node.Get()) || TDqReadWrapBase::Match(node.Get())
                || TCoLeft::Match(node.Get()) || TCoRight::Match(node.Get())) {
                return false;
            } else if (TDqConnection::Match(node.Get())) {
                if (const auto publicId = State->TypeCtx->TranslateOperationId(node->UniqueId())) {
                    publicIds->AllPublicIds.emplace(*publicId, 0U);
                }
            } else if (TDqPhyPrecompute::Match(node.Get())) {
                if (node->HasResult()) {
                    return false;
                }
            } else if (const auto& maybeStage = TMaybeNode<TDqStage>(node.Get())) {
                if (const auto& stage = maybeStage.Cast(); !(stage.Ref().StartsExecution() || stage.Ref().HasResult())) {
                    if (const auto publicId = State->TypeCtx->TranslateOperationId(node->UniqueId())) {
                        if (const auto settings = NDq::TDqStageSettings::Parse(stage); settings.LogicalId) {
                            publicIds->Stage2publicId[settings.LogicalId] = *publicId;
                        }
                        publicIds->AllPublicIds.emplace(*publicId, 0U);
                    }
                }
            } else if (TDqCnResult::Match(node.Get()) || TDqQuery::Match(node.Get())) {
                ++publicIds->GraphsCount;
            }
            return true;
        });
        return publicIds;
    }

    TStatusCallbackPair HandlePull(const TExprNode::TPtr& input, TExprContext& ctx) {
        YQL_CLOG(DEBUG, ProviderDq) << "Executing " << input->Content() << " (UniqueId=" << input->UniqueId() << ")";
        YQL_CLOG(TRACE, ProviderDq) << "HandlePull " << NCommon::ExprToPrettyString(ctx, *input);

        TInstant startTime = TInstant::Now();
        ui64 executionTimeout = State->Settings->_TableTimeout.Get().GetOrElse(TDqSettings::TDefault::TableTimeout);
        auto pull = TPull(input);

        THashMap<TString, TString> pullSettings;
        for (auto s: pull.Settings()) {
            if (auto val = s.Value().Maybe<TCoAtom>()) {
                pullSettings.emplace(s.Name().Value(), val.Cast().Value());
            }
        }

        YQL_ENSURE(!TMaybeNode<TDqQuery>(pull.Input().Ptr()) || State->Settings->EnableComputeActor.Get().GetOrElse(false),
            "DqQuery is not supported with worker actor");

        const bool oneGraphPerQuery = State->Settings->_OneGraphPerQuery.Get().GetOrElse(false);
        auto publicIds = GetPublicIds(pull.Ptr());
        YQL_ENSURE(!oneGraphPerQuery || publicIds->GraphsCount == 1, "Internal error: only one graph per query is allowed");

        auto statusPair = HandlePrecomputes(pull, pullSettings, ctx, executionTimeout);
        if (statusPair.first.Level != TStatus::Ok) {
            return statusPair;
        }

        TString type;
        TVector<TString> columns;
        GetResultType(&type, &columns, pull.Ref(), pull.Input().Ref());

        auto optimizedInput = pull.Input().Ptr();
        optimizedInput = ctx.ShallowCopy(*optimizedInput);
        optimizedInput->SetTypeAnn(pull.Input().Ref().GetTypeAnn());
        optimizedInput->CopyConstraints(pull.Input().Ref());

        auto status = PeepHole(optimizedInput, optimizedInput, ctx, pullSettings, publicIds);
        if (status.Level != TStatus::Ok) {
            return SyncStatus(status);
        }

        // copy-paste {
        TUserDataTable crutches = State->TypeCtx->UserDataStorageCrutches;
        TUserDataTable files;
        StartCounter("FreezeUsedFiles");
        auto filesRes = NCommon::FreezeUsedFiles(*optimizedInput, files, *State->TypeCtx, ctx, [](const TString&){return true;}, crutches);
        if (filesRes.first.Level != TStatus::Ok) {
            if (filesRes.first.Level != TStatus::Error) {
                YQL_CLOG(DEBUG, ProviderDq) << "Freezing files for " << input->Content() << " (UniqueId=" << input->UniqueId() << ")";
            }
            return filesRes;
        }
        FlushCounter("FreezeUsedFiles");
        // copy-paste }

        THashMap<TString, TString> secureParams;
        NCommon::FillSecureParams(optimizedInput, *State->TypeCtx, secureParams);

        auto settings = std::make_shared<TDqSettings>(*State->Settings);

        auto executionPlanner = MakeHolder<TDqsExecutionPlanner>(
            settings, State->TypeCtx, ctx, State->FunctionRegistry,
            optimizedInput);

        // exprRoot must be DqCnResult or DqQuery

        executionPlanner->SetPublicIds(publicIds->Stage2publicId);

        auto tasksPerStage = settings->MaxTasksPerStage.Get().GetOrElse(TDqSettings::TDefault::MaxTasksPerStage);
        const auto maxTasksPerOperation = State->Settings->MaxTasksPerOperation.Get().GetOrElse(TDqSettings::TDefault::MaxTasksPerOperation);

        auto maxDataSizePerJob = settings->MaxDataSizePerJob.Get().GetOrElse(TDqSettings::TDefault::MaxDataSizePerJob);
        auto stagesCount = executionPlanner->StagesCount();

        if (!executionPlanner->CanFallback()) {
            settings->FallbackPolicy = State->TypeCtx->DqFallbackPolicy = EFallbackPolicy::Never;
        }

        bool canFallback = (settings->FallbackPolicy.Get().GetOrElse(EFallbackPolicy::Default) != EFallbackPolicy::Never && !State->TypeCtx->ForceDq);

        if (stagesCount > maxTasksPerOperation && canFallback) {
            return SyncStatus(FallbackWithMessage(
                pull.Ref(),
                TStringBuilder()
                << "Too many stages: "
                << stagesCount << " > "
                << maxTasksPerOperation, ctx, true));
        }

        YQL_ENSURE(stagesCount <= maxTasksPerOperation);

        try {
            while (!executionPlanner->PlanExecution(canFallback) && tasksPerStage > 1) {
                tasksPerStage /= 2;
                settings->MaxTasksPerStage = tasksPerStage;
                executionPlanner->Clear();
            }
        } catch (const TErrorException& err) {
            ctx.IssueManager.RaiseIssue(ExceptionToIssue(err));
            return SyncError();
        } catch (const TFallbackError& err) {
            YQL_ENSURE(canFallback, "Unexpected TFallbackError: " << err.what());
            return SyncStatus(FallbackWithMessage(pull.Ref(), err, ctx, true));
        }

        bool fallbackFlag = false;
        if (executionPlanner->MaxDataSizePerJob() > maxDataSizePerJob && canFallback) {
            return SyncStatus(FallbackWithMessage(
                pull.Ref(),
                TStringBuilder()
                << "MaxDataSizePerJob reached: "
                << executionPlanner->MaxDataSizePerJob() << " > "
                << maxDataSizePerJob, ctx, true));
        }

        bool localRun = false;
        auto& tasks = executionPlanner->GetTasks();
        if (tasks.size() > maxTasksPerOperation && canFallback) {
            return SyncStatus(FallbackWithMessage(
                pull.Ref(),
                TStringBuilder()
                << "Too many tasks: "
                << tasks.size() << " > "
                << maxTasksPerOperation, ctx, true));
        }

        YQL_ENSURE(tasks.size() <= maxTasksPerOperation);

        {
            TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), State->FunctionRegistry->SupportsSizedAllocators());
            TTypeEnvironment typeEnv(alloc);
            for (auto& t : tasks) {
                TUploadList uploadList;
                TString lambda = t.GetProgram().GetRaw();
                fallbackFlag |= BuildUploadList(&uploadList, localRun, &lambda, typeEnv, files);
                t.MutableProgram()->SetRaw(lambda);

                Yql::DqsProto::TTaskMeta taskMeta;
                t.MutableMeta()->UnpackTo(&taskMeta);
                for (const auto& file : uploadList) {
                    *taskMeta.AddFiles() = file;
                }
                t.MutableMeta()->PackFrom(taskMeta);
                if (const auto it = publicIds->AllPublicIds.find(taskMeta.GetStageId()); publicIds->AllPublicIds.cend() != it)
                    ++it->second;
            }
        }

        MarkProgressStarted(publicIds->AllPublicIds, State->ProgressWriter);

        if (fallbackFlag) {
            return SyncStatus(FallbackWithMessage(pull.Ref(), "Too big attachment", ctx, true));
        }

        IDataProvider::TFillSettings fillSettings = NCommon::GetFillSettings(pull.Ref());
        settings = settings->WithFillSettings(fillSettings);

        if (const auto optLLVM = State->TypeCtx->OptLLVM) {
            settings->OptLLVM = *optLLVM;
        }

        auto graphParams = GatherGraphParams(optimizedInput);

        bool ref = NCommon::HasResOrPullOption(pull.Ref(), "ref");
        bool autoRef = NCommon::HasResOrPullOption(pull.Ref(), "autoref");

        bool enableFullResultWrite = settings->EnableFullResultWrite.Get().GetOrElse(false);
        if (enableFullResultWrite) {
            const auto integration = GetDqIntegrationForFullResTable(State);
            enableFullResultWrite = (ref || autoRef)
                && !fillSettings.Discard
                && integration
                && integration->PrepareFullResultTableParams(pull.Ref(), ctx, graphParams, secureParams);
            settings->EnableFullResultWrite = enableFullResultWrite;
        }

        if (ref) {
            if (!enableFullResultWrite) {
                return SyncStatus(FallbackWithMessage(pull.Ref(),
                    TStringBuilder() << "RefSelect mode cannot be used with DQ, because \"" << State->TypeCtx->FullResultDataSink << "\" provider has failed to prepare a result table",
                    ctx, true));
            }
            // Force write to table
            settings->_AllResultsBytesLimit = 0;
            settings->_RowsLimitPerWrite = 0;
        }

        if (State->Metrics) {
            State->Metrics->IncCounter("dq", "HandlePull");
        }
        State->Statistics[State->MetricId++].Entries.push_back(TOperationStatistics::TEntry("HandlePull", 0, 0, 0, 0, 1));

        IDqGateway::TDqProgressWriter progressWriter = MakeDqProgressWriter(publicIds);

        auto future = State->ExecutePlan(State->SessionId, executionPlanner->GetPlan(), columns, secureParams, graphParams,
            settings, progressWriter, UploadCache_->ModulesMapping, fillSettings.Discard, executionTimeout);

        future.Subscribe([publicIds, progressWriter = State->ProgressWriter](const NThreading::TFuture<IDqGateway::TResult>& completedFuture) {
            YQL_ENSURE(!completedFuture.HasException());
            MarkProgressFinished(publicIds->AllPublicIds, completedFuture.GetValueSync().Success(), progressWriter);
        });
        executionPlanner.Destroy();

        TString skiffType;
        NYT::TNode rowSpec;
        if (fillSettings.Format == IDataProvider::EResultFormat::Skiff) {
            auto parsedYtType = SkiffConverter->ParseYTType(pull.Input().Ref(), ctx, TColumnOrder(columns));

            type = parsedYtType.Type;
            rowSpec = parsedYtType.RowSpec;
            skiffType = parsedYtType.SkiffType;
        }

        int level = 0;
        // TODO: remove copy-paste
        return WrapFutureCallback<false>(future, [settings, startTime, localRun, type, rowSpec, skiffType, fillSettings, level, graphParams, columns, enableFullResultWrite, state = State, skiffConverter = SkiffConverter](const IDqGateway::TResult& res, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            auto duration = TInstant::Now() - startTime;
            YQL_CLOG(INFO, ProviderDq) << "Execution Pull complete, duration: " << duration;
            if (state->Metrics) {
                state->Metrics->SetCounter("dq", "TotalExecutionTime", duration.MilliSeconds());
                state->Metrics->SetCounter(
                    "dq",
                    localRun
                        ? "InMemoryExecutionTime"
                        : "RemoteExecutionTime",
                    duration.MilliSeconds());
            }

            state->Statistics[state->MetricId++] = res.Statistics;

            if (res.Fallback) {
                if (state->Metrics) {
                    state->Metrics->IncCounter("dq", "Fallback");
                }
                state->Statistics[state->MetricId++].Entries.push_back(TOperationStatistics::TEntry("Fallback", 0, 0, 0, 0, 1));
                // never fallback will be captured in yql_facade
                auto issues = TIssues{TIssue(ctx.GetPosition(input->Pos()), "Gateway Error").SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_WARNING)};
                issues.AddIssues(res.Issues());
                ctx.AssociativeIssues.emplace(input.Get(), std::move(issues));
                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Error);
            }

            output = input;
            input->SetState(TExprNode::EState::ExecutionComplete);

            TStringStream out;

            IDataProvider::TFillSettings ysonFormatSettings;
            ysonFormatSettings.FormatDetails = fillSettings.FormatDetails;
            ysonFormatSettings.Format = IDataProvider::EResultFormat::Yson;
            NYson::TYsonWriter writer((IOutputStream*)&out, NCommon::GetYsonFormat(ysonFormatSettings), ::NYson::EYsonType::Node, false);
            writer.OnBeginMap();

            if (skiffType) {
                writer.OnKeyedItem("SkiffType");
                writer.OnRaw(skiffType, ::NYson::EYsonType::Node);

                writer.OnKeyedItem("Columns");
                writer.OnBeginList();
                for (auto& column: columns) {
                    writer.OnListItem();
                    writer.OnStringScalar(column);
                }
                writer.OnEndList();
            }

            if (type) {
                writer.OnKeyedItem("Type");
                writer.OnRaw(type);
            }

            writer.OnKeyedItem("Data");
            auto item = NYT::NodeFromYsonString(res.Data);
            for (int i = 0; i < level; ++i) {
                item = item.AsList().at(0);
            }
            auto raw = NYT::NodeToYsonString(item);

            TString trStr = "";
            const bool truncated = res.Truncated;
            const ui64 rowsCount = res.RowsCount;

            if (truncated && !state->TypeCtx->ForceDq && !enableFullResultWrite) {
                auto issue = TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "DQ cannot execute the query. Cause: " << "too big result " <<  trStr).SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_INFO);
                bool error = settings->FallbackPolicy.Get().GetOrElse(EFallbackPolicy::Default) == EFallbackPolicy::Never;
                for (const auto& i : res.Issues()) {
                    TIssuePtr subIssue = new TIssue(i);
                    if (error && subIssue->Severity == TSeverityIds::S_WARNING) {
                        subIssue->Severity = TSeverityIds::S_ERROR;
                    }
                    issue.AddSubIssue(subIssue);
                }

                if (error) {
                    issue.SetMessage("Too big result " + trStr);
                    issue.Severity = TSeverityIds::S_ERROR;
                }
                ctx.IssueManager.RaiseIssue(issue);
                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Error);
            }

            if (truncated) {
                // TODO:
                ui64 bytes = 0;
                ui64 rows = 0;
                switch (fillSettings.Format) {
                    case IDataProvider::EResultFormat::Yson: {
                        writer.OnBeginList();

                        for (auto& node : item.AsList()) {
                            raw = NYT::NodeToYsonString(node);
                            bytes += raw.size();
                            rows += 1;
                            writer.OnListItem();
                            writer.OnRaw(raw);

                            if (fillSettings.AllResultsBytesLimit && bytes >= *fillSettings.AllResultsBytesLimit) {
                                break;
                            }
                            if (fillSettings.RowsLimitPerWrite && rows >= *fillSettings.RowsLimitPerWrite) {
                                break;
                            }
                        }
                        writer.OnEndList();
                        break;
                    }
                    case IDataProvider::EResultFormat::Skiff: {
                        writer.OnStringScalar(skiffConverter->ConvertNodeToSkiff(state, fillSettings, rowSpec, item, columns));
                        break;
                    }
                    default: {
                        YQL_LOG_CTX_THROW yexception() << "Invalid result type: " << fillSettings.Format;
                    }
                }

                if (enableFullResultWrite) {
                    writer.OnKeyedItem("Ref");
                    writer.OnBeginList();
                    writer.OnListItem();
                    const auto integration = GetDqIntegrationForFullResTable(state);
                    YQL_ENSURE(integration);
                    integration->WriteFullResultTableRef(writer, columns, graphParams);
                    writer.OnEndList();
                }

                writer.OnKeyedItem("Truncated");
                writer.OnBooleanScalar(true);
            } else {
                switch (fillSettings.Format) {
                    case IDataProvider::EResultFormat::Yson: {
                        writer.OnRaw(raw);
                        break;
                    }
                    case IDataProvider::EResultFormat::Skiff: {
                        writer.OnStringScalar(skiffConverter->ConvertNodeToSkiff(state, fillSettings, rowSpec, item, columns));
                        break;
                    }
                    default: {
                        YQL_LOG_CTX_THROW yexception() << "Invalid result type: " << fillSettings.Format;
                    }
                }
            }

            if (rowsCount) {
                writer.OnKeyedItem("RowsCount");
                writer.OnUint64Scalar(rowsCount);
            }

            writer.OnEndMap();

            ctx.IssueManager.RaiseIssues(res.Issues());
            input->SetResult(ctx.NewAtom(input->Pos(), out.Str()));
            return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Ok);
        }, "");
    }

    IDqGateway::TDqProgressWriter MakeDqProgressWriter(const TPublicIds::TPtr& publicIds) const {
        IDqGateway::TDqProgressWriter dqProgressWriter = [progressWriter = State->ProgressWriter, publicIds, current = std::make_shared<IDqGateway::TProgressWriterState>()](IDqGateway::TProgressWriterState state) 
        {
            if (*current != state) {
                for (const auto& publicId : publicIds->AllPublicIds) {
                    auto p = TOperationProgress(TString(DqProviderName), publicId.first, TOperationProgress::EState::InProgress, state.Stage);
                    if (publicId.second) {
                        p.Counters.ConstructInPlace();
                        p.Counters->Running = p.Counters->Total = publicId.second;
                        auto maybeStats = state.Stats.find(publicId.first);
                        if (maybeStats != state.Stats.end()) {
                            p.Counters->Custom = maybeStats->second.ToMap();
                        }
                    }
                    progressWriter(p);
                }
                *current = std::move(state);
            }
        };
        return dqProgressWriter;
    }

    static void MarkProgressStarted(const THashMap<ui32, ui32>& allPublicIds, const TOperationProgressWriter& progressWriter) {
        for(const auto& publicId : allPublicIds) {
            auto p = TOperationProgress(TString(DqProviderName), publicId.first, TOperationProgress::EState::InProgress);
            if (publicId.second) {
                p.Counters.ConstructInPlace();
                p.Counters->Running = p.Counters->Total = publicId.second;
            }
            progressWriter(p);
        }
    }

    static void MarkProgressFinished(const THashMap<ui32, ui32>& allPublicIds, bool success, const TOperationProgressWriter& progressWriter) {
        for(const auto& publicId : allPublicIds) {
            auto state = success ? TOperationProgress::EState::Finished : TOperationProgress::EState::Failed;
            auto p = TOperationProgress(TString(DqProviderName), publicId.first, state);
            if (publicId.second) {
                p.Counters.ConstructInPlace();
                (success ? p.Counters->Completed : p.Counters->Failed) = p.Counters->Total = publicId.second;
            }
            progressWriter(p);
        }
    }

    void FlushStatisticsToState() {
        TOperationStatistics statistics;
        FlushCounters(statistics);

        TGuard<TMutex> lock(State->Mutex);
        if (!statistics.Entries.empty()) {
            State->Statistics[State->MetricId++] = statistics;
        }
    }

    THashMap<TString, TString> GatherGraphParams(const TExprNode::TPtr& root) {
        THashMap<TString, TString> params;
        VisitExpr(root, [&](const TExprNode::TPtr& node) -> bool {
            if (node->IsCallable()) {
                for (const auto& provider : State->TypeCtx->DataSources) {
                    if (provider->CanParse(*node)) {
                        if (auto dqIntegration = provider->GetDqIntegration()) {
                            dqIntegration->Annotate(*node, params);
                            return false;
                        }
                    }
                }

                for (const auto& provider : State->TypeCtx->DataSinks) {
                    if (provider->CanParse(*node)) {
                        if (auto dqIntegration = provider->GetDqIntegration()) {
                            dqIntegration->Annotate(*node, params);
                            return false;
                        }
                    }
                }
            }
            return true;
        });
        return params;
    }

    static IDqIntegration* GetDqIntegrationForFullResTable(const TDqStatePtr& state) {
        if (auto fullResultTableProvider = state->TypeCtx->DataSinkMap.Value(state->TypeCtx->FullResultDataSink, nullptr)) {
            auto dqIntegration = fullResultTableProvider->GetDqIntegration();
            YQL_ENSURE(dqIntegration);
            return dqIntegration;
        }
        return nullptr;
    }

    TNodeOnNodeOwnedMap FindIndependentPrecomputes(const TExprNode::TPtr& node) const {
        TNodeOnNodeOwnedMap precomputes;
        TNodeMap<bool> visitedNodes;
        FindIndependentPrecomputesImpl(node, precomputes, visitedNodes);
        return precomputes;
    }

    bool FindIndependentPrecomputesImpl(const TExprNode::TPtr& node, TNodeOnNodeOwnedMap& precomputes, TNodeMap<bool>& visitedNodes) const {
        if (auto it = visitedNodes.find(node.Get()); it != visitedNodes.cend()) {
            return it->second;
        }

        bool& hasPrecompute = visitedNodes[node.Get()];
        if (TDqStageBase::Match(node.Get())) {
            auto stage = TDqStageBase(node);
            for (const auto& input : stage.Inputs()) {
                hasPrecompute = FindIndependentPrecomputesImpl(input.Ptr(), precomputes, visitedNodes) || hasPrecompute;
            }
        } else if (TDqPhyPrecompute::Match(node.Get())) {
            if (!node->HasResult() && node->GetState() != TExprNode::EState::Error) {
                hasPrecompute = true;
                if (node->StartsExecution() || !FindIndependentPrecomputesImpl(node->HeadPtr(), precomputes, visitedNodes)) {
                    precomputes[node.Get()] = node;
                }
            }
        } else {
            for (auto child: node->Children()) {
                hasPrecompute = FindIndependentPrecomputesImpl(child, precomputes, visitedNodes) || hasPrecompute;
            }
        }
        return hasPrecompute;
    }

    static void CompleteNode(const TExecPrecomputeStatePtr& execState, TExprNode* node, const TAsyncTransformCallback& callback) {
        auto item = MakeHolder<TExecPrecomputeState::TItem>();
        item->Node = node;
        item->Callback = callback;

        with_lock(execState->Lock) {
            execState->Completed.PushBack(item.Release());
        }
    }

    static TAsyncTransformCallback HandlePrecomputeAsyncComplete(TExecPrecomputeStatePtr execState) {
        return TAsyncTransformCallback([execState](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            output = input;
            input->SetState(TExprNode::EState::ExecutionRequired);
            TStatus combinedStatus = TStatus::Repeat;
            TExecPrecomputeState::TQueueType completed;
            with_lock(execState->Lock) {
                completed.Swap(execState->Completed);
            }

            for (auto& item : completed) {
                TExprNode::TPtr callableOutput;
                execState->PrecomputeFutures.erase(item.Node);
                auto status = item.Callback(item.Node, callableOutput, ctx);
                if (status.Level != TStatus::Error) {
                    YQL_ENSURE(callableOutput == item.Node, "Unsupported node rewrite");
                }
                combinedStatus = combinedStatus.Combine(status);
            }

            return combinedStatus;
        });
    }

    IGraphTransformer::TStatus RunPrecomputes(
        const TNodeOnNodeOwnedMap& precomputes,
        TExprContext& ctx,
        const THashMap<TString, TString>& providerParams,
        ui64 executionTimeout,
        std::vector<NThreading::TFuture<void>>& futures
    ) {
        IDataProvider::TFillSettings fillSettings;
        fillSettings.AllResultsBytesLimit.Clear();
        fillSettings.RowsLimitPerWrite.Clear();
        auto commonSettings = State->Settings->WithFillSettings(fillSettings);
        commonSettings->EnableFullResultWrite = false;

        IGraphTransformer::TStatus combinedStatus = TStatus::Ok;
        futures.clear();

        for (auto [_, input]: precomputes) {
            TString uniqId = TStringBuilder() << input->Content() << "(#" << input->UniqueId() << ')';
            YQL_LOG_CTX_SCOPE(uniqId);
            NThreading::TFuture<void>& precomputeFuture = ExecPrecomputeState_->PrecomputeFutures[input.Get()];
            if (input->GetState() > TExprNode::EState::ExecutionRequired) {
                YQL_CLOG(DEBUG, ProviderDq) << "Continue async execution";
                combinedStatus = combinedStatus.Combine(TStatus::Async);
                YQL_ENSURE(precomputeFuture.Initialized());
                futures.push_back(precomputeFuture);
                continue;
            }

            auto logCtx = NLog::CurrentLogContextPath();
            TInstant startTime = TInstant::Now();
            YQL_CLOG(DEBUG, ProviderDq) << "Executing " << input->Content();

            auto publicIds = GetPublicIds(input);

            auto optimizedInput = input;
            optimizedInput->SetState(TExprNode::EState::ConstrComplete);
            auto status = PeepHole(optimizedInput, optimizedInput, ctx, providerParams, publicIds);
            if (status.Level != TStatus::Ok) {
                return combinedStatus.Combine(status);
            }

            input->SetState(TExprNode::EState::ExecutionInProgress);

            THashMap<TString, TString> secureParams;
            NCommon::FillSecureParams(optimizedInput, *State->TypeCtx, secureParams);

            // copy-paste {
            TUserDataTable crutches = State->TypeCtx->UserDataStorageCrutches;
            TUserDataTable files;
            StartCounter("FreezeUsedFiles");

            auto filesRes = NCommon::FreezeUsedFiles(*optimizedInput, files, *State->TypeCtx, ctx, [](const TString&){ return true; }, crutches);
            if (filesRes.first.Level != TStatus::Ok) {
                combinedStatus = combinedStatus.Combine(filesRes.first);
                if (filesRes.first.Level == TStatus::Error) {
                    return filesRes.first;
                }
                YQL_CLOG(DEBUG, ProviderDq) << "Freezing files for " << input->Content();
                if (filesRes.first.Level == TStatus::Async) {
                    precomputeFuture = filesRes.second.Apply([execState = ExecPrecomputeState_, node = input.Get(), logCtx](const TAsyncTransformCallbackFuture& future) {
                        YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                        YQL_ENSURE(!future.HasException());
                        YQL_CLOG(DEBUG, ProviderDq) << "Finishing freezing files";
                        CompleteNode(execState, node, future.GetValue());
                    });
                    futures.push_back(precomputeFuture);
                }
                continue;
            }
            FlushCounter("FreezeUsedFiles");
            // copy-paste }

            auto settings = std::make_shared<TDqSettings>(*commonSettings);

            auto executionPlanner = MakeHolder<TDqsExecutionPlanner>(
                settings, State->TypeCtx, ctx, State->FunctionRegistry,
                optimizedInput);

            // exprRoot must be DqCnResult or DqQuery

            executionPlanner->SetPublicIds(publicIds->Stage2publicId);

            auto tasksPerStage = settings->MaxTasksPerStage.Get().GetOrElse(TDqSettings::TDefault::MaxTasksPerStage);
            const auto maxTasksPerOperation = State->Settings->MaxTasksPerOperation.Get().GetOrElse(TDqSettings::TDefault::MaxTasksPerOperation);

            auto maxDataSizePerJob = settings->MaxDataSizePerJob.Get().GetOrElse(TDqSettings::TDefault::MaxDataSizePerJob);
            auto stagesCount = executionPlanner->StagesCount();

            if (!executionPlanner->CanFallback()) {
                settings->FallbackPolicy = State->TypeCtx->DqFallbackPolicy = EFallbackPolicy::Never;
            }

            bool canFallback = (settings->FallbackPolicy.Get().GetOrElse(EFallbackPolicy::Default) != EFallbackPolicy::Never && !State->TypeCtx->ForceDq);

            if (stagesCount > maxTasksPerOperation && canFallback) {
                return FallbackWithMessage(
                    *input,
                    TStringBuilder()
                    << "Too many stages: "
                    << stagesCount << " > "
                    << maxTasksPerOperation, ctx, false);
            }

            YQL_ENSURE(stagesCount <= maxTasksPerOperation);

            try {
                while (!executionPlanner->PlanExecution(canFallback) && tasksPerStage > 1) {
                    tasksPerStage /= 2;
                    settings->MaxTasksPerStage = tasksPerStage;
                    executionPlanner->Clear();
                }
            } catch (const TFallbackError& err) {
                YQL_ENSURE(canFallback, "Unexpected TFallbackError: " << err.what());
                return FallbackWithMessage(*input, err, ctx, false);
            }

            bool fallbackFlag = false;
            if (executionPlanner->MaxDataSizePerJob() > maxDataSizePerJob && canFallback) {
                return FallbackWithMessage(
                    *input,
                    TStringBuilder()
                    << "MaxDataSizePerJob reached: "
                    << executionPlanner->MaxDataSizePerJob() << " > "
                    << maxDataSizePerJob, ctx, false);
            }

            auto& tasks = executionPlanner->GetTasks();
            if (tasks.size() > maxTasksPerOperation && canFallback) {
                return FallbackWithMessage(
                    *input,
                    TStringBuilder()
                    << "Too many tasks: "
                    << tasks.size() << " > "
                    << maxTasksPerOperation, ctx, false);
            }

            YQL_ENSURE(tasks.size() <= maxTasksPerOperation);

            {
                TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), State->FunctionRegistry->SupportsSizedAllocators());
                TTypeEnvironment typeEnv(alloc);
                for (auto& t : tasks) {
                    TUploadList uploadList;
                    TString lambda = t.GetProgram().GetRaw();
                    fallbackFlag |= BuildUploadList(&uploadList, false, &lambda, typeEnv, files);
                    t.MutableProgram()->SetRaw(lambda);

                    Yql::DqsProto::TTaskMeta taskMeta;
                    t.MutableMeta()->UnpackTo(&taskMeta);
                    for (const auto& file : uploadList) {
                        *taskMeta.AddFiles() = file;
                    }
                    t.MutableMeta()->PackFrom(taskMeta);
                    if (const auto it = publicIds->AllPublicIds.find(taskMeta.GetStageId()); publicIds->AllPublicIds.cend() != it)
                        ++it->second;
                }
            }

            if (fallbackFlag) {
                return FallbackWithMessage(*input, "Too big attachment", ctx, false);
            }

            if (State->Metrics) {
                State->Metrics->IncCounter("dq", "Precompute");
            }
            State->Statistics[State->MetricId++].Entries.push_back(TOperationStatistics::TEntry("Precompute", 0, 0, 0, 0, 1));

            auto graphParams = GatherGraphParams(optimizedInput);
            graphParams["Precompute"] = ToString(true);
            MarkProgressStarted(publicIds->AllPublicIds, State->ProgressWriter);

            if (const auto optLLVM = State->TypeCtx->OptLLVM) {
                settings->OptLLVM = *optLLVM;
            }

            IDqGateway::TDqProgressWriter progressWriter = MakeDqProgressWriter(publicIds);

            auto future = State->ExecutePlan(State->SessionId, executionPlanner->GetPlan(), {}, secureParams, graphParams,
                settings, progressWriter, UploadCache_->ModulesMapping, false, executionTimeout);

            executionPlanner.Destroy();

            bool neverFallback = settings->FallbackPolicy.Get().GetOrElse(EFallbackPolicy::Default) == EFallbackPolicy::Never;
            precomputeFuture = future.Apply([publicIds, state = State, startTime, execState = ExecPrecomputeState_, node = input.Get(), neverFallback, logCtx](const NThreading::TFuture<IDqGateway::TResult>& completedFuture) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                YQL_ENSURE(!completedFuture.HasException());
                const IDqGateway::TResult& res = completedFuture.GetValueSync();

                MarkProgressFinished(publicIds->AllPublicIds, res.Success(), state->ProgressWriter);

                auto duration = TInstant::Now() - startTime;
                YQL_CLOG(INFO, ProviderDq) << "Execution precomputes complete, duration: " << duration;
                if (state->Metrics) {
                    state->Metrics->SetCounter("dq", "PrecomputeExecutionTime", duration.MilliSeconds());
                }

                state->Statistics[state->MetricId++] = res.Statistics;

                if (res.Fallback || !res.Success()) {
                    YQL_CLOG(DEBUG, ProviderDq) << "Finished with error";
                    if (res.Fallback) {
                        if (state->Metrics) {
                            state->Metrics->IncCounter("dq", "Fallback");
                        }
                        state->Statistics[state->MetricId++].Entries.push_back(TOperationStatistics::TEntry("Fallback", 0, 0, 0, 0, 1));
                    }

                    CompleteNode(execState, node, [resIssues = res.Issues(), fallback = res.Fallback](const TExprNode::TPtr& input, TExprNode::TPtr&, TExprContext& ctx) -> IGraphTransformer::TStatus {
                        auto issue = TIssue(ctx.GetPosition(input->Pos()), "Gateway Error");
                        if (fallback) {
                            issue.SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_WARNING);
                        } else {
                            issue.SetCode(TIssuesIds::UNEXPECTED, TSeverityIds::S_ERROR);
                        }
                        auto issues = TIssues{};
                        issues.AddIssue(issue);
                        issues.AddIssues(resIssues);
                        ctx.IssueManager.RaiseIssues(issues);
                        input->SetState(TExprNode::EState::Error);
                        return IGraphTransformer::TStatus::Error;
                    });
                } else if (res.Truncated) {
                    YQL_CLOG(DEBUG, ProviderDq) << "Finished with truncated result";
                    CompleteNode(execState, node, [neverFallback, resIssues = res.Issues()](const TExprNode::TPtr& input, TExprNode::TPtr&, TExprContext& ctx) -> IGraphTransformer::TStatus {
                        auto issue = TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "DQ cannot execute the query. Cause: " << "too big precompute result").SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_INFO);
                        for (const auto& i : resIssues) {
                            TIssuePtr subIssue = new TIssue(i);
                            if (neverFallback && subIssue->Severity == TSeverityIds::S_WARNING) {
                                subIssue->Severity = TSeverityIds::S_ERROR;
                            }
                            issue.AddSubIssue(subIssue);
                        }

                        if (neverFallback) {
                            issue.SetMessage("Too big precompute result");
                            issue.Severity = TSeverityIds::S_ERROR;
                        }
                        ctx.IssueManager.RaiseIssue(issue);
                        input->SetState(TExprNode::EState::Error);
                        return IGraphTransformer::TStatus::Error;
                    });
                } else {
                    YQL_CLOG(DEBUG, ProviderDq) << "Finished";
                    CompleteNode(execState, node, [resIssues = res.Issues(), data = res.Data, success = res.Success()](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> IGraphTransformer::TStatus {
                        output = input;
                        ctx.IssueManager.RaiseIssues(resIssues);
                        input->SetResult(ctx.NewAtom(input->Pos(), data));
                        input->SetState(TExprNode::EState::ExecutionComplete);
                        return IGraphTransformer::TStatus::Ok;
                    });
                }
            });
            combinedStatus = combinedStatus.Combine(IGraphTransformer::TStatus::Async);
            futures.push_back(precomputeFuture);
        }
        return combinedStatus;
    }

    TStatusCallbackPair HandlePrecomputes(const TResOrPullBase& resOrPull, const THashMap<TString, TString>& settings, TExprContext& ctx, ui64 executionTimeout) {
        TStatus status = TStatus::Ok;
        auto precomputes = FindIndependentPrecomputes(resOrPull.Input().Ptr());
        if (!precomputes.empty()) {
            std::vector<NThreading::TFuture<void>> futures;
            status = RunPrecomputes(precomputes, ctx, settings, executionTimeout, futures);
            YQL_CLOG(TRACE, ProviderDq) << "RunPrecomputes returns status " << status << ", with " << futures.size() << " futures";
            if (status == TStatus::Async) {
                return std::make_pair(status, NThreading::WaitAny(futures).Apply([execState = ExecPrecomputeState_](const TFuture<void>& completedFuture) {
                    completedFuture.TryRethrow();
                    return HandlePrecomputeAsyncComplete(execState);
                }));
            }
        }
        return SyncStatus(status);
    }

    IGraphTransformer::TStatus PeepHole(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx,
        const THashMap<TString, TString>& providerParams, const TPublicIds::TPtr& publicIds) const
    {
        TDqsPipelineConfigurator peepholeConfig(State, providerParams);
        TDqsFinalPipelineConfigurator finalPeepholeConfg(State, publicIds);
        TPeepholeSettings peepholeSettings;
        peepholeSettings.CommonConfig = &peepholeConfig;
        peepholeSettings.FinalConfig = &finalPeepholeConfg;
        bool hasNonDeterministicFunctions;
        auto status = PeepHoleOptimizeNode(input, output, ctx, *State->TypeCtx, nullptr, hasNonDeterministicFunctions, peepholeSettings);
        if (status.Level != TStatus::Ok) {
            ctx.AddError(TIssue(ctx.GetPosition(output->Pos()), TString("Peephole optimization failed for Dq stage")));
            return status;
        }
        YQL_CLOG(TRACE, ProviderDq) << "After PeepHole\n" << NCommon::ExprToPrettyString(ctx, *output);
        return status;
    }

private:
    TDqStatePtr State;
    ISkiffConverterPtr SkiffConverter;
    TExecPrecomputeStatePtr ExecPrecomputeState_;
    TUploadCache::TPtr UploadCache_;

    const ui64 MaxFileReadSize = 1_MB;
};

}

IGraphTransformer* CreateDqExecTransformer(const TDqStatePtr& state) {
    return new TDqExecTransformer(state, MakeIntrusive<TSimpleSkiffConverter>());
}

TExecTransformerFactory CreateDqExecTransformerFactory(const ISkiffConverterPtr& skiffConverter) {
    return [skiffConverter] (const TDqStatePtr& state) { return new TDqExecTransformer(state, skiffConverter); };
}

} // namespace NYql
