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
#include <ydb/library/yql/providers/dq/interface/yql_dq_integration.h>
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

#include <util/system/env.h>
#include <util/generic/size_literals.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

#include <memory>

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
        settings.CollectBasicStats = true;
        settings.CollectProfileStats = true;
        settings.AllowGeneratorsInUnboxedValues = true;
        auto runner = NDq::MakeDqTaskRunner(executionContext, settings, {});

        {
            auto guard = runner->BindAllocator(State->Settings->MemoryLimit.Get().GetOrElse(0));
            runner->Prepare(task, limits);
        }

        TVector<NDqProto::TData> rows;
        {
            auto guard = runner->BindAllocator(State->Settings->MemoryLimit.Get().GetOrElse(0));
            YQL_CLOG(DEBUG, ProviderDq) << " NDq::ERunStatus " << runner->Run();

            NDq::ERunStatus status;
            while ((status = runner->Run()) == NDq::ERunStatus::PendingOutput || status == NDq::ERunStatus::Finished) {
                NDqProto::TData data;
                if (runner->GetOutputChannel(0)->PopAll(data) && !fillSettings.Discard) {
                    rows.push_back(data);
                }
                if (status == NDq::ERunStatus::Finished) {
                    break;
                }
                if (!fillSettings.Discard) {
                    if (fillSettings.AllResultsBytesLimit && runner->GetOutputChannel(0)->GetStats()->Bytes >= *fillSettings.AllResultsBytesLimit) {
                        result.Truncated = true;
                        break;
                    }
                    if (fillSettings.RowsLimitPerWrite && runner->GetOutputChannel(0)->GetStats()->RowsOut >= *fillSettings.RowsLimitPerWrite) {
                        result.Truncated = true;
                        break;
                    }
                }
            }

            YQL_ENSURE(status == NDq::ERunStatus::Finished || status == NDq::ERunStatus::PendingOutput);
        }

        auto serializedResultType = GetSerializedResultType(lambda);
        NYql::NDqs::TProtoBuilder protoBuilder(serializedResultType, columns);

        result.Data = protoBuilder.BuildYson(rows);

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

struct TDqsPipelineConfigurator : public IPipelineConfigurator {
public:
    TDqsPipelineConfigurator(const TDqStatePtr& state)
        : State_(state)
    {
    }
private:
    void AfterCreate(TTransformationPipeline*) const final {}

    void AfterTypeAnnotation(TTransformationPipeline* pipeline) const final {
        pipeline->Add(NDqs::CreateDqsReplacePrecomputesTransformer(pipeline->GetTypeAnnotationContext().Get(), State_->FunctionRegistry), "ReplacePrecomputes");
        pipeline->Add(NDq::CreateDqBuildPhyStagesTransformer(false), "BuildPhy");
        pipeline->Add(NDqs::CreateDqsRewritePhyCallablesTransformer(), "RewritePhyCallables");
    }

    void AfterOptimize(TTransformationPipeline*) const final {}

private:
    TDqStatePtr State_;
};

class TDqExecTransformer: public TExecTransformerBase, TCounters
{
public:
    TDqExecTransformer(const TDqStatePtr& state)
        : State(state)
        , ExecState(MakeIntrusive<TExecState>())
    {
        AddHandler({TStringBuf("Result")}, RequireNone(), Hndl(&TDqExecTransformer::HandleResult));
        AddHandler({TStringBuf("Pull")}, RequireNone(), Hndl(&TDqExecTransformer::HandlePull));
        AddHandler({TDqCnResult::CallableName()}, RequireNone(), Pass());
        AddHandler({TDqQuery::CallableName()}, RequireFirst(), Pass());
    }

private:
    struct TExecState : public TThrRefBase {
        TAdaptiveLock Lock;

        struct TItem : public TIntrusiveListItem<TItem> {
            TExprNode* Node = nullptr;
            TAsyncTransformCallback Callback;
        };

        using TQueueType = TIntrusiveListWithAutoDelete<TExecState::TItem, TDelete>;
        TQueueType Completed;
        NThreading::TPromise<void> Promise = NThreading::NewPromise();
        bool HasResult = false;
    };

    using TExecStatePtr = TIntrusivePtr<TExecState>;

    void GetResultType(TString* type, TVector<TString>* columns, const TExprNode& resOrPull, const TExprNode& resOrPullInput) const
    {
        *columns = NCommon::GetResOrPullColumnHints(resOrPull);
        if (columns->empty()) {
            *columns = NCommon::GetStructFields(resOrPullInput.GetTypeAnn());
        }

        if (NCommon::HasResOrPullOption(resOrPull, "type")) {
            TStringStream typeYson;
            NYson::TYsonWriter typeWriter(&typeYson);
            NCommon::WriteResOrPullType(typeWriter, resOrPullInput.GetTypeAnn(), *columns);
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
            ModulesMapping.emplace(objectId, path);
            return std::make_tuple(path, objectId);
        }

        TFileLinkPtr& fileLink = FileLinks[objectId];
        if (!fileLink) {
            fileLink = State->FileStorage->PutFileStripped(path, md5);
        }

        ModulesMapping.emplace(objectId  + DqStrippedSuffied, path);

        return std::make_tuple(fileLink->GetPath(), objectId + DqStrippedSuffied);
    }

    std::tuple<TString, TString> GetPathAndObjectId(const TFilePathWithMd5& pathWithMd5) const {
        if (pathWithMd5.Md5.empty()) {
            YQL_CLOG(WARN, ProviderDq) << "Empty md5 for " << pathWithMd5.Path;
        }
        return GetPathAndObjectId(pathWithMd5.Path,
            pathWithMd5.Md5.empty()
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

        i64 dataLimit = static_cast<i64>(4_GB);
        bool fallbackFlag = false;
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

        TScopedAlloc alloc(NKikimr::TAlignedPagePoolCounters(), State->FunctionRegistry->SupportsSizedAllocators());
        TTypeEnvironment typeEnv(alloc);
        NCommon::TMkqlCommonCallableCompiler compiler;

        {
            auto block = MeasureBlock("BuildProgram");
            auto programLambda = TDqPhyStage(input).Program();

            TVector<TExprBase> fakeReads;
            auto paramsType = NDq::CollectParameters(programLambda, ctx);
            *lambda = NDq::BuildProgram(
                programLambda, *paramsType, compiler, typeEnv, *State->FunctionRegistry,
                ctx, fakeReads);
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

        try {
            auto result = TMaybeNode<TResult>(input).Cast();

            auto precomputes = FindIndependentPrecomputes(result.Input().Ptr());
            if (!precomputes.empty()) {
                auto status = HandlePrecomputes(precomputes, ctx);
                if (status.Level != TStatus::Ok) {
                    if (status == TStatus::Async) {
                        return std::make_pair(status, ExecState->Promise.GetFuture().Apply([execState = ExecState](const TFuture<void>& completedFuture) {
                            completedFuture.GetValue();
                            return HandlePrecomputeAsyncComplete(execState);
                        }));
                    } else {
                        return SyncStatus(status);
                    }
                }
            }

            IDataProvider::TFillSettings fillSettings = NCommon::GetFillSettings(result.Ref());
            auto settings = State->Settings->WithFillSettings(fillSettings);
            if (!settings->_RowsLimitPerWrite.Get() && !settings->_AllResultsBytesLimit.Get()) {
                settings->_AllResultsBytesLimit = 64_MB;
            }

            THashMap<TString, TString> secureParams;
            NCommon::FillSecureParams(result.Input().Ptr(), *State->TypeCtx, secureParams);

            auto graphParams = GatherGraphParams(result.Input().Ptr());
            bool hasGraphParams = !graphParams.empty();

            TString type;
            TVector<TString> columns;
            GetResultType(&type, &columns, result.Ref(), result.Input().Ref());

            TPublicIds::TPtr publicIds = std::make_shared<TPublicIds>();
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

            int level;
            TExprNode::TPtr resInput = WrapLambdaBody(level, result.Input().Ptr(), ctx);
            {
                auto block = MeasureBlock("PeepHole");
                if (const auto status = PeepHole(resInput, resInput, ctx); status.Level != TStatus::Ok) {
                    return SyncStatus(status);
                }
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
                    if (State->Settings->FallbackPolicy.Get().GetOrElse("default") == "never"
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
                        NActors::TActorId(1, 0, 1, 0), State->FunctionRegistry,
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
                    graphParams["Evaluation"] = ToString(!ctx.Step.IsDone(TExprStep::ExprEval));
                    future = State->DqGateway->ExecutePlan(
                        State->SessionId, executionPlanner->GetPlan(), columns, secureParams, graphParams,
                        settings, progressWriter, ModulesMapping, fillSettings.Discard);
                }
            }

            if (State->Metrics) {
                State->Metrics->IncCounter("dq", localRun
                    ? "InMemory"
                    : "Remote");
            }

            YQL_ENSURE(future.Initialized());

            FlushStatisticsToState();

            return WrapFutureCallback(future, [localRun, startTime, type, fillSettings, level, settings, enableFullResultWrite, columns, graphParams, state = State](const IDqGateway::TResult& res, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                YQL_CLOG(DEBUG, ProviderDq) << state->SessionId <<  " WrapFutureCallback";

                auto duration = TInstant::Now() - startTime;
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
                    if (state->Settings->FallbackPolicy.Get().GetOrElse("default") == "never" || state->TypeCtx->ForceDq) {
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
                    writer.OnRaw("[]");
                    writer.OnKeyedItem("Truncated");
                    writer.OnBooleanScalar(true);
                } else {
                    writer.OnRaw(raw);
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

    IGraphTransformer::TStatus FallbackWithMessage(const TExprNode& node, const TString& message, TExprContext& ctx, bool isRoot) {
        if (State->Metrics) {
            State->Metrics->IncCounter("dq", "Fallback");
        }
        State->Statistics[State->MetricId++].Entries.push_back(TOperationStatistics::TEntry("Fallback", 0, 0, 0, 0, 1));
        auto issue = TIssue(ctx.GetPosition(node.Pos()), message).SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_WARNING);
        if (isRoot) {
            ctx.AssociativeIssues.emplace(&node, TIssues{std::move(issue)});
        } else {
            ctx.IssueManager.RaiseIssue(issue);
        }
        return IGraphTransformer::TStatus::Error;
    }

    TPublicIds::TPtr GetPublicIds(const TExprNode::TPtr& root) const {
        TPublicIds::TPtr publicIds = std::make_shared<TPublicIds>();
        VisitExpr(root, [&](const TExprNode::TPtr& node) {
            if (TResTransientBase::Match(node.Get())) {
                return false;
            } else if (TDqReadWrapBase::Match(node.Get())) {
                return false;
            } else if (TDqConnection::Match(node.Get())) {
                if (const auto publicId = State->TypeCtx->TranslateOperationId(node->UniqueId())) {
                    publicIds->AllPublicIds.emplace(*publicId, 0U);
                }
            } else if (TDqPhyPrecompute::Match(node.Get())) {
                if (node->HasResult()) {
                    return false;
                }
            } else if (TDqStage::Match(node.Get())) {
                const auto& stage = TDqStage(node);
                if (!(stage.Ref().StartsExecution() || stage.Ref().HasResult())) {
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
        auto pull = TPull(input);

        YQL_ENSURE(!TMaybeNode<TDqQuery>(pull.Input().Ptr()) || State->Settings->EnableComputeActor.Get().GetOrElse(false),
            "DqQuery is not supported with worker actor");

        const bool oneGraphPerQuery = State->Settings->_OneGraphPerQuery.Get().GetOrElse(false);
        auto publicIds = GetPublicIds(pull.Ptr());
        YQL_ENSURE(!oneGraphPerQuery || publicIds->GraphsCount == 1, "Internal error: only one graph per query is allowed");

        auto precomputes = FindIndependentPrecomputes(pull.Input().Ptr());
        if (!precomputes.empty()) {
            auto status = HandlePrecomputes(precomputes, ctx);
            if (status.Level != TStatus::Ok) {
                if (status == TStatus::Async) {
                    return std::make_pair(status, ExecState->Promise.GetFuture().Apply([execState = ExecState](const TFuture<void>& completedFuture) {
                        completedFuture.GetValue();
                        return HandlePrecomputeAsyncComplete(execState);
                    }));
                } else {
                    return SyncStatus(status);
                }
            }
        }

        TString type;
        TVector<TString> columns;
        GetResultType(&type, &columns, pull.Ref(), pull.Input().Ref());

        auto optimizedInput = pull.Input().Ptr();
        THashMap<TString, TString> secureParams;
        NCommon::FillSecureParams(optimizedInput, *State->TypeCtx, secureParams);

        optimizedInput = ctx.ShallowCopy(*optimizedInput);
        optimizedInput->SetTypeAnn(pull.Input().Ref().GetTypeAnn());
        optimizedInput->CopyConstraints(pull.Input().Ref());

        auto status = PeepHole(optimizedInput, optimizedInput, ctx);
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

        auto executionPlanner = MakeHolder<TDqsExecutionPlanner>(
            State->TypeCtx, ctx, State->FunctionRegistry,
            optimizedInput);

        // exprRoot must be DqCnResult or DqQuery

        executionPlanner->SetPublicIds(publicIds->Stage2publicId);

        auto settings = std::make_shared<TDqSettings>(*State->Settings);
        auto tasksPerStage = settings->MaxTasksPerStage.Get().GetOrElse(TDqSettings::TDefault::MaxTasksPerStage);
        const auto maxTasksPerOperation = State->Settings->MaxTasksPerOperation.Get().GetOrElse(TDqSettings::TDefault::MaxTasksPerOperation);

        auto maxDataSizePerJob = settings->MaxDataSizePerJob.Get().GetOrElse(TDqSettings::TDefault::MaxDataSizePerJob);
        auto stagesCount = executionPlanner->StagesCount();

        if (!executionPlanner->CanFallback()) {
            settings->FallbackPolicy = State->TypeCtx->DqFallbackPolicy = "never";
        }

        bool canFallback = (settings->FallbackPolicy.Get().GetOrElse("default") != "never" && !State->TypeCtx->ForceDq);

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
            while (executionPlanner->PlanExecution(settings, canFallback) > maxTasksPerOperation && tasksPerStage > 1) {
                tasksPerStage /= 2;
                settings->MaxTasksPerStage = tasksPerStage;
                executionPlanner->Clear();
            }
        } catch (const TFallbackError& err) {
            YQL_ENSURE(canFallback, "Unexpected TFallbackError: " << err.what());
            return SyncStatus(FallbackWithMessage(pull.Ref(), err.what(), ctx, true));
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
            TScopedAlloc alloc(NKikimr::TAlignedPagePoolCounters(), State->FunctionRegistry->SupportsSizedAllocators());
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

        IDqGateway::TDqProgressWriter progressWriter = MakeDqProgressWriter(publicIds);

        auto future = State->DqGateway->ExecutePlan(State->SessionId, executionPlanner->GetPlan(), columns, secureParams, graphParams,
            settings, progressWriter, ModulesMapping, fillSettings.Discard);

        future.Subscribe([publicIds, progressWriter = State->ProgressWriter](const NThreading::TFuture<IDqGateway::TResult>& completedFuture) {
            YQL_ENSURE(!completedFuture.HasException());
            MarkProgressFinished(publicIds->AllPublicIds, completedFuture.GetValueSync().Success(), progressWriter);
        });
        executionPlanner.Destroy();

        int level = 0;
        // TODO: remove copy-paste
        return WrapFutureCallback(future, [settings, startTime, localRun, type, fillSettings, level, graphParams, columns, enableFullResultWrite, state = State](const IDqGateway::TResult& res, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            auto duration = TInstant::Now() - startTime;
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
                ctx.AssociativeIssues.emplace(input.Get(), std::move(issues));
                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Error);
            }

            output = input;
            input->SetState(TExprNode::EState::ExecutionComplete);

            TStringStream out;
            NYson::TYsonWriter writer((IOutputStream*)&out, NCommon::GetYsonFormat(fillSettings), ::NYson::EYsonType::Node, false);
            writer.OnBeginMap();
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
                bool error = settings->FallbackPolicy.Get().GetOrElse("default") == "never";
                for (const auto& i : res.Issues()) {
                    TIssuePtr subIssue = new TIssue(i);
                    if (error && subIssue->Severity == TSeverityIds::S_WARNING) {
                        subIssue->Severity = TSeverityIds::S_ERROR;
                    }
                    issue.AddSubIssue(subIssue);
                }

                if (error) {
                    issue.Message = "Too big result " + trStr;
                    issue.Severity = TSeverityIds::S_ERROR;
                }
                ctx.IssueManager.RaiseIssue(issue);
                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Error);
            }

            if (truncated) {
                // TODO:
                ui64 bytes = 0;
                ui64 rows = 0;
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
                writer.OnRaw(raw);
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
        IDqGateway::TDqProgressWriter dqProgressWriter = [progressWriter = State->ProgressWriter, publicIds](const TString& stage) {
            for (const auto& publicId : publicIds->AllPublicIds) {
                auto p = TOperationProgress(TString(DqProviderName), publicId.first, TOperationProgress::EState::InProgress, stage);
                if (publicId.second) {
                    p.Counters.ConstructInPlace();
                    p.Counters->Running = p.Counters->Total = publicId.second;
                }
                progressWriter(p);
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

    static void CompleteNode(const TExecStatePtr& execState, TExprNode* node, const TAsyncTransformCallback& callback) {
        auto item = MakeHolder<TExecState::TItem>();
        item->Node = node;
        item->Callback = callback;

        NThreading::TPromise<void> promiseToSet;
        with_lock(execState->Lock) {
            execState->Completed.PushBack(item.Release());
            if (!execState->HasResult) {
                execState->HasResult = true;
                promiseToSet = execState->Promise;
            }
        }

        if (promiseToSet.Initialized()) {
            promiseToSet.SetValue();
        }
    }

    static TAsyncTransformCallback HandlePrecomputeAsyncComplete(TExecStatePtr execState) {
        return TAsyncTransformCallback([execState](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            output = input;
            input->SetState(TExprNode::EState::ExecutionRequired);
            TStatus combinedStatus = TStatus::Repeat;
            TExecState::TQueueType completed;
            auto newPromise = NThreading::NewPromise();
            {
                TGuard<TAdaptiveLock> guard(execState->Lock);
                completed.Swap(execState->Completed);
                execState->Promise.Swap(newPromise);
                execState->HasResult = false;
            }

            for (auto& item : completed) {
                TExprNode::TPtr callableOutput;
                auto status = item.Callback(item.Node, callableOutput, ctx);
                if (status.Level != TStatus::Error) {
                    YQL_ENSURE(callableOutput == item.Node, "Unsupported node rewrite");
                }
                combinedStatus = combinedStatus.Combine(status);
            }

            return combinedStatus;
        });
    }

    IGraphTransformer::TStatus HandlePrecomputes(const TNodeOnNodeOwnedMap& precomputes, TExprContext& ctx) {

        IDataProvider::TFillSettings fillSettings;
        fillSettings.AllResultsBytesLimit.Clear();
        fillSettings.RowsLimitPerWrite.Clear();
        auto commonSettings = State->Settings->WithFillSettings(fillSettings);
        commonSettings->EnableFullResultWrite = false;

        IGraphTransformer::TStatus combinedStatus = TStatus::Ok;

        for (auto [_, input]: precomputes) {
            TString uniqId = TStringBuilder() << input->Content() << "(#" << input->UniqueId() << ')';
            YQL_LOG_CTX_SCOPE(uniqId);
            if (input->GetState() > TExprNode::EState::ExecutionRequired) {
                YQL_CLOG(DEBUG, ProviderDq) << "Continue async execution";
                combinedStatus = combinedStatus.Combine(TStatus::Async);
                continue;
            }

            auto logCtx = NLog::CurrentLogContextPath();
            TInstant startTime = TInstant::Now();
            YQL_CLOG(DEBUG, ProviderDq) << "Executing " << input->Content();

            auto publicIds = GetPublicIds(input);

            auto optimizedInput = input;
            optimizedInput->SetState(TExprNode::EState::ConstrComplete);
            auto status = PeepHole(optimizedInput, optimizedInput, ctx);
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
                    filesRes.second.Subscribe([execState = ExecState, node = input.Get(), logCtx](const TAsyncTransformCallbackFuture& future) {
                        YQL_LOG_CTX_ROOT_SCOPE(logCtx);
                        YQL_ENSURE(!future.HasException());
                        YQL_CLOG(DEBUG, ProviderDq) << "Finishing freezing files";
                        CompleteNode(execState, node, future.GetValue());
                    });
                }
                continue;
            }
            FlushCounter("FreezeUsedFiles");
            // copy-paste }

            auto executionPlanner = MakeHolder<TDqsExecutionPlanner>(
                State->TypeCtx, ctx, State->FunctionRegistry,
                optimizedInput);

            // exprRoot must be DqCnResult or DqQuery

            executionPlanner->SetPublicIds(publicIds->Stage2publicId);

            auto settings = std::make_shared<TDqSettings>(*commonSettings);

            auto tasksPerStage = settings->MaxTasksPerStage.Get().GetOrElse(TDqSettings::TDefault::MaxTasksPerStage);
            const auto maxTasksPerOperation = State->Settings->MaxTasksPerOperation.Get().GetOrElse(TDqSettings::TDefault::MaxTasksPerOperation);

            auto maxDataSizePerJob = settings->MaxDataSizePerJob.Get().GetOrElse(TDqSettings::TDefault::MaxDataSizePerJob);
            auto stagesCount = executionPlanner->StagesCount();

            if (!executionPlanner->CanFallback()) {
                settings->FallbackPolicy = State->TypeCtx->DqFallbackPolicy = "never";
            }

            bool canFallback = (settings->FallbackPolicy.Get().GetOrElse("default") != "never" && !State->TypeCtx->ForceDq);

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
                while (executionPlanner->PlanExecution(settings, canFallback) > maxTasksPerOperation && tasksPerStage > 1) {
                    tasksPerStage /= 2;
                    settings->MaxTasksPerStage = tasksPerStage;
                    executionPlanner->Clear();
                }
            } catch (const TFallbackError& err) {
                YQL_ENSURE(canFallback, "Unexpected TFallbackError: " << err.what());
                return FallbackWithMessage(*input, err.what(), ctx, false);
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
                TScopedAlloc alloc(NKikimr::TAlignedPagePoolCounters(), State->FunctionRegistry->SupportsSizedAllocators());
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

            auto graphParams = GatherGraphParams(optimizedInput);
            graphParams["Precompute"] = ToString(true);
            MarkProgressStarted(publicIds->AllPublicIds, State->ProgressWriter);

            IDqGateway::TDqProgressWriter progressWriter = MakeDqProgressWriter(publicIds);

            auto future = State->DqGateway->ExecutePlan(State->SessionId, executionPlanner->GetPlan(), {}, secureParams, graphParams,
                settings, progressWriter, ModulesMapping, false);

            executionPlanner.Destroy();

            bool neverFallback = settings->FallbackPolicy.Get().GetOrElse("default") == "never";
            future.Subscribe([publicIds, state = State, startTime, execState = ExecState, node = input.Get(), neverFallback, logCtx](const NThreading::TFuture<IDqGateway::TResult>& completedFuture) {
                YQL_LOG_CTX_ROOT_SCOPE(logCtx);
                YQL_ENSURE(!completedFuture.HasException());
                const IDqGateway::TResult& res = completedFuture.GetValueSync();

                MarkProgressFinished(publicIds->AllPublicIds, res.Success(), state->ProgressWriter);

                auto duration = TInstant::Now() - startTime;
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
                            issue.Message = "Too big precompute result";
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
        }
        return combinedStatus;
    }

    IGraphTransformer::TStatus PeepHole(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) const {
        TDqsPipelineConfigurator peepholeConfig(State);
        TPeepholeSettings peepholeSettings;
        peepholeSettings.CommonConfig = &peepholeConfig;
        bool hasNonDeterministicFunctions;
        auto status = PeepHoleOptimizeNode<true>(input, output, ctx, *State->TypeCtx, nullptr, hasNonDeterministicFunctions, peepholeSettings);
        if (status.Level != TStatus::Ok) {
            ctx.AddError(TIssue(ctx.GetPosition(output->Pos()), TString("Peephole optimization failed for Dq stage")));
            return status;
        }
        YQL_CLOG(TRACE, ProviderDq) << "After PeepHole\n" << NCommon::ExprToPrettyString(ctx, *output);
        return status;
    }

private:
    TDqStatePtr State;
    TExecStatePtr ExecState;
    mutable THashMap<TString, TFileLinkPtr> FileLinks;
    mutable THashMap<TString, TString> ModulesMapping;

    const ui64 MaxFileReadSize = 1_MB;
};

}

IGraphTransformer* CreateDqExecTransformer(const TDqStatePtr& state) {
    return new TDqExecTransformer(state);
}

} // namespace NYql
