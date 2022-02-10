#include "dq_tasks_runner.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/runtime/dq_input_channel.h>
#include <ydb/library/yql/dq/runtime/dq_input_producer.h>
#include <ydb/library/yql/dq/runtime/dq_source.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/core/user_data/yql_user_data.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>

#include <util/generic/scope.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NYql::NDqProto;

namespace NYql::NDq {

namespace {

void ValidateParamValue(std::string_view paramName, const TType* type, const NUdf::TUnboxedValuePod& value) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
            break;
        case TType::EKind::Null:
            break;
        case TType::EKind::EmptyList:
            break;
        case TType::EKind::EmptyDict:
            break;

        case TType::EKind::Data: {
            auto dataType = static_cast<const TDataType*>(type);
            auto slot = dataType->GetDataSlot();
            YQL_ENSURE(slot);
            YQL_ENSURE(IsValidValue(*slot, value), "Error parsing task parameter, malformed value"
                << ", parameter: " << paramName
                << ", type: " << NUdf::GetDataTypeInfo(*slot).Name
                << ", value: " << value);
            break;
        }

        case TType::EKind::Optional: {
            auto optionalType = static_cast<const TOptionalType*>(type);
            if (value) {
                ValidateParamValue(paramName, optionalType->GetItemType(), value.GetOptionalValue());
            }
            break;
        }

        case TType::EKind::List: {
            auto listType = static_cast<const TListType*>(type);
            auto itemType = listType->GetItemType();
            const auto iter = value.GetListIterator();
            for (NUdf::TUnboxedValue item; iter.Next(item);) {
                ValidateParamValue(paramName, itemType, item);
            }
            break;
        }

        case TType::EKind::Struct: {
            auto structType = static_cast<const TStructType*>(type);
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                auto memberType = structType->GetMemberType(index);
                ValidateParamValue(paramName, memberType, value.GetElement(index));
            }
            break;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<const TTupleType*>(type);
            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                auto elementType = tupleType->GetElementType(index);
                ValidateParamValue(paramName, elementType, value.GetElement(index));
            }
            break;
        }

        case TType::EKind::Dict:  {
            auto dictType = static_cast<const TDictType*>(type);
            auto keyType = dictType->GetKeyType();
            auto payloadType = dictType->GetPayloadType();

            const auto iter = value.GetDictIterator();
            for (NUdf::TUnboxedValue key, payload; iter.NextPair(key, payload);) {
                ValidateParamValue(paramName, keyType, key);
                ValidateParamValue(paramName, payloadType, payload);
            }
            break;
        }

        case TType::EKind::Variant: {
            auto variantType = static_cast<const TVariantType*>(type);
            ui32 variantIndex = value.GetVariantIndex();
            TType* innerType = variantType->GetUnderlyingType();
            if (innerType->IsStruct()) {
                innerType = static_cast<TStructType*>(innerType)->GetMemberType(variantIndex);
            } else {
                YQL_ENSURE(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
                innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
            }
            ValidateParamValue(paramName, innerType, value.GetVariantItem());
            break;
        }

        default:
            YQL_ENSURE(false, "Unexpected value type in parameter"
                << ", parameter: " << paramName
                << ", type: " << type->GetKindAsStr());
    }
}

} // namespace

#define LOG(...) do { if (Y_UNLIKELY(LogFunc)) { LogFunc(__VA_ARGS__); } } while (0)

NUdf::TUnboxedValue DqBuildInputValue(const NDqProto::TTaskInput& inputDesc, const NKikimr::NMiniKQL::TType* type,
    TVector<IDqInput::TPtr>&& inputs, const THolderFactory& holderFactory)
{
    switch (inputDesc.GetTypeCase()) {
        case NYql::NDqProto::TTaskInput::kSource:
            Y_VERIFY(inputs.size() == 1);
            [[fallthrough]];
        case NYql::NDqProto::TTaskInput::kUnionAll:
            return CreateInputUnionValue(std::move(inputs), holderFactory);
        case NYql::NDqProto::TTaskInput::kMerge: {
            const auto& protoSortCols = inputDesc.GetMerge().GetSortColumns();
            TVector<TSortColumnInfo> sortColsInfo;
            GetColumnsInfo(type, protoSortCols, sortColsInfo);
            YQL_ENSURE(!sortColsInfo.empty());

            return CreateInputMergeValue(std::move(inputs), std::move(sortColsInfo), holderFactory);
        }
        default:
            YQL_ENSURE(false, "Unknown input type: " << (ui32) inputDesc.GetTypeCase());
    }
}

IDqOutputConsumer::TPtr DqBuildOutputConsumer(const NDqProto::TTaskOutput& outputDesc, const NMiniKQL::TType* type,
    const NMiniKQL::TTypeEnvironment& typeEnv, TVector<IDqOutput::TPtr>&& outputs)
{
    switch (outputDesc.GetTypeCase()) {
        case NDqProto::TTaskOutput::kSink:
            Y_VERIFY(outputDesc.ChannelsSize() == 0);
            [[fallthrough]];
        case NDqProto::TTaskOutput::kMap: {
            YQL_ENSURE(outputs.size() == 1);
            return CreateOutputMapConsumer(outputs[0]);
        }

        case NDqProto::TTaskOutput::kHashPartition: {
            TVector<NUdf::TDataTypeId> keyColumnTypes;
            TVector<ui32> keyColumnIndices;
            GetColumnsInfo(type, outputDesc.GetHashPartition().GetKeyColumns(), keyColumnTypes, keyColumnIndices);
            YQL_ENSURE(!keyColumnTypes.empty());

            YQL_ENSURE(outputDesc.GetHashPartition().GetPartitionsCount() == outputDesc.ChannelsSize());
            TVector<ui64> channelIds(outputDesc.GetHashPartition().GetPartitionsCount());
            for (ui32 i = 0; i < outputDesc.ChannelsSize(); ++i) {
                channelIds[i] = outputDesc.GetChannels(i).GetId();
            }

            return CreateOutputHashPartitionConsumer(std::move(outputs), std::move(keyColumnTypes),
                std::move(keyColumnIndices), typeEnv);
        }

        case NDqProto::TTaskOutput::kBroadcast: {
            return CreateOutputBroadcastConsumer(std::move(outputs));
        }

        case NDqProto::TTaskOutput::kRangePartition: {
            YQL_ENSURE(false, "Unsupported partition type: `NYql::NDqProto::TTaskOutput::kRangePartition`");
        }

        case NDqProto::TTaskOutput::kEffects: {
            YQL_ENSURE(false, "Unsupported partition type: `NYql::NDqProto::TTaskOutput::kEffect`");
        }

        case NDqProto::TTaskOutput::TYPE_NOT_SET: {
            YQL_ENSURE(false, "Unexpected output type: `NYql::NDqProto::TDqTaskOutput::TYPE_NOT_SET`");
        }
    }
}

IDqOutputConsumer::TPtr TDqTaskRunnerExecutionContext::CreateOutputConsumer(const TTaskOutput& outputDesc,
    const NKikimr::NMiniKQL::TType* type, NUdf::IApplyContext*, const TTypeEnvironment& typeEnv,
    TVector<IDqOutput::TPtr>&& outputs) const
{
    return DqBuildOutputConsumer(outputDesc, type, typeEnv, std::move(outputs));
}

IDqChannelStorage::TPtr TDqTaskRunnerExecutionContext::CreateChannelStorage(ui64 /* channelId */) const {
    return {};
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/// TDqTaskRunner
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TDqTaskRunner : public IDqTaskRunner {
public:
    TDqTaskRunner(const TDqTaskRunnerContext& context, const TDqTaskRunnerSettings& settings, const TLogFunc& logFunc)
        : Context(context)
        , Settings(settings)
        , LogFunc(logFunc)
        , CollectBasicStats(Settings.CollectBasicStats)
        , CollectProfileStats(Settings.CollectProfileStats)
    {
        if (CollectBasicStats) {
            Stats = std::make_unique<TDqTaskRunnerStats>();
            if (Y_UNLIKELY(CollectProfileStats)) {
                Stats->ComputeCpuTimeByRun = NMonitoring::ExponentialHistogram(6, 10, 10);
            }
        } else {
            YQL_ENSURE(!CollectProfileStats, "CollectProfileStats requires CollectBasicStats to be set as well");
        }

        if (!Context.Alloc) {
            SelfAlloc = std::make_unique<TScopedAlloc>(TAlignedPagePoolCounters(), Context.FuncRegistry->SupportsSizedAllocators());
        }

        if (!Context.TypeEnv) {
            SelfTypeEnv = std::make_unique<TTypeEnvironment>(Context.Alloc ? *Context.Alloc : *SelfAlloc);
        }

        if (SelfAlloc) {
            SelfAlloc->Release();
        }
    }

    ~TDqTaskRunner() {
        if (SelfAlloc) {
            SelfAlloc->Acquire();
        }
    }

    ui64 GetTaskId() const override {
        Y_VERIFY(TaskId, "Not prepared yet");
        return TaskId;
    }

    void BuildTask(const NDqProto::TDqTask& task, const TDqTaskRunnerParameterProvider& parameterProvider) {
        LOG(TStringBuilder() << "Build task: " << TaskId);
        auto startTime = TInstant::Now();

        auto& typeEnv = TypeEnv();

        auto& program = task.GetProgram();
        YQL_ENSURE(program.GetRuntimeVersion());
        YQL_ENSURE(program.GetRuntimeVersion() <= NYql::NDqProto::ERuntimeVersion::RUNTIME_VERSION_YQL_1_0);

        ProgramParsed.ProgramNode = DeserializeRuntimeNode(program.GetRaw(), typeEnv);
        YQL_ENSURE(ProgramParsed.ProgramNode.IsImmediate() && ProgramParsed.ProgramNode.GetNode()->GetType()->IsStruct());
        auto& programStruct = static_cast<TStructLiteral&>(*ProgramParsed.ProgramNode.GetNode());
        auto programType = programStruct.GetType();
        YQL_ENSURE(programType);

        auto programRootIdx = programType->FindMemberIndex("Program");
        YQL_ENSURE(programRootIdx);
        TRuntimeNode programRoot = programStruct.GetValue(*programRootIdx);
        if (Context.FuncProvider) {
            TExploringNodeVisitor explorer;
            explorer.Walk(programRoot.GetNode(), typeEnv);
            bool wereChanges = false;
            programRoot = SinglePassVisitCallables(programRoot, explorer, Context.FuncProvider, typeEnv, true, wereChanges);
        }

        ProgramParsed.OutputItemTypes.resize(task.OutputsSize());

        if (programRoot.GetNode()->GetType()->IsCallable()) {
            auto programResultType = static_cast<const TCallableType*>(programRoot.GetNode()->GetType());
            YQL_ENSURE(programResultType->GetReturnType()->IsStream());
            auto programResultItemType = static_cast<const TStreamType*>(programResultType->GetReturnType())->GetItemType();

            if (programResultItemType->IsVariant()) {
                auto variantType = static_cast<const TVariantType*>(programResultItemType);
                YQL_ENSURE(variantType->GetUnderlyingType()->IsTuple());
                auto variantTupleType = static_cast<const TTupleType*>(variantType->GetUnderlyingType());
                YQL_ENSURE(task.OutputsSize() == variantTupleType->GetElementsCount(),
                    "" << task.OutputsSize() << " != " << variantTupleType->GetElementsCount());
                for (ui32 i = 0; i < variantTupleType->GetElementsCount(); ++i) {
                    ProgramParsed.OutputItemTypes[i] = variantTupleType->GetElementType(i);
                }
            }
            else {
                YQL_ENSURE(task.OutputsSize() == 1);
                ProgramParsed.OutputItemTypes[0] = programResultItemType;
            }
        }
        else {
            YQL_ENSURE(programRoot.GetNode()->GetType()->IsVoid());
            YQL_ENSURE(task.OutputsSize() == 0);
        }

        auto programInputsIdx = programType->FindMemberIndex("Inputs");
        YQL_ENSURE(programInputsIdx);
        TRuntimeNode programInputs = programStruct.GetValue(*programInputsIdx);
        YQL_ENSURE(programInputs.IsImmediate() && programInputs.GetNode()->GetType()->IsTuple());
        auto& programInputsTuple = static_cast<TTupleLiteral&>(*programInputs.GetNode());
        auto programInputsCount = programInputsTuple.GetValuesCount();
        YQL_ENSURE(task.InputsSize() == programInputsCount);

        ProgramParsed.InputItemTypes.resize(programInputsCount);
        ProgramParsed.EntryPoints.resize(programInputsCount + 1 /* parameters */);
        for (ui32 i = 0; i < programInputsCount; ++i) {
            auto input = programInputsTuple.GetValue(i);
            TType* type = input.GetStaticType();
            YQL_ENSURE(type->GetKind() == TType::EKind::Stream);
            ProgramParsed.InputItemTypes[i] = static_cast<TStreamType&>(*type).GetItemType();
            ProgramParsed.EntryPoints[i] = input.GetNode();
        }

        auto programParamsIdx = programType->FindMemberIndex("Parameters");
        YQL_ENSURE(programParamsIdx);
        TRuntimeNode programParams = programStruct.GetValue(*programParamsIdx);
        YQL_ENSURE(programParams.GetNode()->GetType()->IsCallable());
        auto paramsType = static_cast<TCallableType*>(programParams.GetNode()->GetType())->GetReturnType();
        YQL_ENSURE(paramsType->IsStruct());
        ProgramParsed.EntryPoints[programInputsCount] = programParams.GetNode();
        auto paramsStruct = static_cast<TStructType*>(paramsType);
        auto paramsCount = paramsStruct->GetMembersCount();

        TExploringNodeVisitor programExplorer;
        programExplorer.Walk(programRoot.GetNode(), typeEnv);
        auto programSize = programExplorer.GetNodes().size();

        LOG(TStringBuilder() << "task: " << TaskId << ", program size: " << programSize
            << ", llvm: `" << Settings.OptLLVM << "`.");

        if (Y_UNLIKELY(CollectProfileStats)) { 
            ProgramParsed.StatsRegistry = NMiniKQL::CreateDefaultStatsRegistry(); 
        } 
 
        auto validatePolicy = Settings.TerminateOnError ? NUdf::EValidatePolicy::Fail : NUdf::EValidatePolicy::Exception;
        TComputationPatternOpts opts(Alloc().Ref(), typeEnv, Context.ComputationFactory, Context.FuncRegistry,
            NUdf::EValidateMode::None, validatePolicy, Settings.OptLLVM,
            EGraphPerProcess::Multi, ProgramParsed.StatsRegistry.Get());
        SecureParamsProvider = MakeSimpleSecureParamsProvider(Settings.SecureParams);
        opts.SecureParamsProvider = SecureParamsProvider.get();
        ProgramParsed.CompPattern = MakeComputationPattern(programExplorer, programRoot, ProgramParsed.EntryPoints, opts);

        ProgramParsed.CompGraph = ProgramParsed.CompPattern->Clone(
            opts.ToComputationOptions(*Context.RandomProvider, *Context.TimeProvider));

        TBindTerminator term(ProgramParsed.CompGraph->GetTerminator());

        auto paramNode = ProgramParsed.CompGraph->GetEntryPoint(programInputsCount, /* require */ false);
        if (paramNode) {
            // TODO: Remove serialized parameters that are used in OLAP program and not used in current program
            const auto& graphHolderFactory = ProgramParsed.CompGraph->GetHolderFactory();
            NUdf::TUnboxedValue* structMembers;
            auto paramsStructValue = graphHolderFactory.CreateDirectArrayHolder(paramsCount, structMembers);

            for (ui32 i = 0; i < paramsStruct->GetMembersCount(); ++i) {
                std::string_view name = paramsStruct->GetMemberName(i);
                TType* type = paramsStruct->GetMemberType(i);

                if (parameterProvider && parameterProvider(name, type, typeEnv, graphHolderFactory, structMembers[i])) {
#ifndef NDEBUG
                    YQL_ENSURE(!task.GetParameters().contains(name), "param: " << name);
#endif
                } else {
                    auto it = task.GetParameters().find(name);
                    YQL_ENSURE(it != task.GetParameters().end());

                    TDqDataSerializer::DeserializeParam(it->second, type, graphHolderFactory, structMembers[i]);
                }

                ValidateParamValue(name, type, structMembers[i]);
            }

            paramNode->SetValue(ProgramParsed.CompGraph->GetContext(), std::move(paramsStructValue));
        } else {
            /*
             * This situation is ok, when there are OLAP parameters only. There is no parameter node
             * because there is no parameters in program. But there are parameters in paramsStruct, they are
             * serialized somewhere before in executor.
             */
        }

        auto buildTime = TInstant::Now() - startTime;
        if (Stats) {
            Stats->BuildCpuTime = buildTime;
        }
        LOG(TStringBuilder() << "Build task: " << TaskId << " takes " << buildTime.MicroSeconds() << " us");
    }

    void Prepare(const NDqProto::TDqTask& task, const TDqTaskRunnerMemoryLimits& memoryLimits,
        const IDqTaskRunnerExecutionContext& execCtx, const TDqTaskRunnerParameterProvider& parameterProvider) override
    {
        TaskId = task.GetId();
        BuildTask(task, parameterProvider);

        LOG(TStringBuilder() << "Prepare task: " << TaskId);
        auto startTime = TInstant::Now();

        auto& holderFactory = ProgramParsed.CompGraph->GetHolderFactory();
        TBindTerminator term(ProgramParsed.CompGraph->GetTerminator());

        auto& typeEnv = TypeEnv();

        for (ui32 i = 0; i < task.InputsSize(); ++i) {
            auto& inputDesc = task.GetInputs(i);

            TVector<IDqInput::TPtr> inputs{Reserve(std::max<ui64>(inputDesc.ChannelsSize(), 1))}; // 1 is for "source" type of input.

            if (inputDesc.HasSource()) {
                auto source = CreateDqSource(i, ProgramParsed.InputItemTypes[i],
                    memoryLimits.ChannelBufferSize, Settings.CollectProfileStats);
                auto [_, inserted] = Sources.emplace(i, source);
                Y_VERIFY(inserted);
                inputs.emplace_back(source);
            } else {
                for (auto& inputChannelDesc : inputDesc.GetChannels()) {
                    ui64 channelId = inputChannelDesc.GetId();
                    auto inputChannel = CreateDqInputChannel(channelId, ProgramParsed.InputItemTypes[i],
                        memoryLimits.ChannelBufferSize, Settings.CollectProfileStats, typeEnv, holderFactory,
                        inputChannelDesc.GetTransportVersion());
                    auto ret = InputChannels.emplace(channelId, inputChannel);
                    YQL_ENSURE(ret.second, "task: " << TaskId << ", duplicated input channelId: " << channelId);
                    inputs.emplace_back(inputChannel);
                }
            }

            auto entryNode = ProgramParsed.CompGraph->GetEntryPoint(i, true);
            entryNode->SetValue(ProgramParsed.CompGraph->GetContext(),
                DqBuildInputValue(inputDesc, ProgramParsed.InputItemTypes[i], std::move(inputs), holderFactory));
        }

        ResultStream = ProgramParsed.CompGraph->GetValue();

        TVector<IDqOutputConsumer::TPtr> outputConsumers(task.OutputsSize());
        for (ui32 i = 0; i < task.OutputsSize(); ++i) {
            auto& outputDesc = task.GetOutputs(i);

            if (outputDesc.GetTypeCase() == NDqProto::TTaskOutput::kEffects) {
                TaskHasEffects = true;
            }

            TVector<IDqOutput::TPtr> outputs{Reserve(std::max<ui64>(outputDesc.ChannelsSize(), 1))};
            if (outputDesc.HasSink()) {
                auto sink = CreateDqSink(i, ProgramParsed.OutputItemTypes[i], memoryLimits.ChannelBufferSize,
                    Settings.CollectProfileStats);
                auto [_, inserted] = Sinks.emplace(i, sink);
                Y_VERIFY(inserted);
                outputs.emplace_back(sink);
            } else {
                for (auto& outputChannelDesc : outputDesc.GetChannels()) {
                    ui64 channelId = outputChannelDesc.GetId();

                    TDqOutputChannelSettings settings;
                    settings.MaxStoredBytes = memoryLimits.ChannelBufferSize;
                    settings.MaxChunkBytes = memoryLimits.OutputChunkMaxSize;
                    settings.TransportVersion = outputChannelDesc.GetTransportVersion();
                    settings.CollectProfileStats = Settings.CollectProfileStats;
                    settings.AllowGeneratorsInUnboxedValues = Settings.AllowGeneratorsInUnboxedValues;

                    if (!outputChannelDesc.GetInMemory()) {
                        settings.ChannelStorage = execCtx.CreateChannelStorage(channelId);
                    }

                    auto outputChannel = CreateDqOutputChannel(channelId, ProgramParsed.OutputItemTypes[i], typeEnv,
                        holderFactory, settings, LogFunc);

                    auto ret = OutputChannels.emplace(channelId, outputChannel);
                    YQL_ENSURE(ret.second, "task: " << TaskId << ", duplicated output channelId: " << channelId);
                    outputs.emplace_back(outputChannel);
                }
            }

            outputConsumers[i] = execCtx.CreateOutputConsumer(outputDesc, ProgramParsed.OutputItemTypes[i],
                Context.ApplyCtx, typeEnv, std::move(outputs));
        }

        if (outputConsumers.empty()) {
            Output = nullptr;
        } else if (outputConsumers.size() == 1) {
            Output = std::move(outputConsumers[0]);
        } else {
            Output = CreateOutputMultiConsumer(std::move(outputConsumers));
        }

        auto prepareTime = TInstant::Now() - startTime;
        if (LogFunc) {
            TLogFunc logger = [taskId = TaskId, log = LogFunc](const TString& message) {
                log(TStringBuilder() << "Run task: " << taskId << ", " << message);
            };
            LogFunc = logger;

        }

        LOG(TStringBuilder() << "Prepare task: " << TaskId << ", takes " << prepareTime.MicroSeconds() << " us");
        if (Stats) {
            Stats->BuildCpuTime += prepareTime;

            for (auto&[channelId, inputChannel] : InputChannels) {
                Stats->InputChannels.emplace(channelId, inputChannel->GetStats());
            }
            for (auto&[inputIndex, source] : Sources) {
                Stats->Sources.emplace(inputIndex, source->GetStats());
            }
            for (auto&[channelId, outputChannel] : OutputChannels) {
                Stats->OutputChannels.emplace(channelId, outputChannel->GetStats());
            }
        }
    }

    ERunStatus Run() final {
        LOG(TStringBuilder() << "Run task: " << TaskId);

        RunComputeTime = TDuration::Zero();

        auto runStatus = FetchAndDispatch();
        if (Stats) {
            Stats->RunStatusTimeMetrics.SetCurrentStatus(runStatus, RunComputeTime);
        }

        if (Y_UNLIKELY(CollectProfileStats)) {
            Stats->ComputeCpuTimeByRun->Collect(RunComputeTime.MilliSeconds());

            if (ProgramParsed.StatsRegistry) {
                Stats->MkqlStats.clear();
                ProgramParsed.StatsRegistry->ForEachStat([this](const TStatKey& key, i64 value) {
                    Stats->MkqlStats.emplace_back(TMkqlStat{key, value});
                });
            }
        }

        if (runStatus == ERunStatus::Finished) {
            if (Stats) {
                Stats->FinishTs = TInstant::Now();
            }
            if (Y_UNLIKELY(CollectProfileStats)) {
                StopWaiting(Stats->FinishTs);
            }

            return ERunStatus::Finished;
        }

        if (Y_UNLIKELY(CollectProfileStats)) {
            auto now = TInstant::Now();
            StartWaiting(now);
            if (runStatus == ERunStatus::PendingOutput) {
                StartWaitingOutput(now);
            }
        }

        return runStatus; // PendingInput or PendingOutput
    }

    bool HasEffects() const final {
        return TaskHasEffects;
    }

    IDqInputChannel::TPtr GetInputChannel(ui64 channelId) override {
        auto ptr = InputChannels.FindPtr(channelId);
        YQL_ENSURE(ptr, "task: " << TaskId << " does not have input channelId: " << channelId);
        return *ptr;
    }

    IDqSource::TPtr GetSource(ui64 inputIndex) override {
        auto ptr = Sources.FindPtr(inputIndex);
        YQL_ENSURE(ptr, "task: " << TaskId << " does not have input index: " << inputIndex);
        return *ptr;
    }

    IDqOutputChannel::TPtr GetOutputChannel(ui64 channelId) override {
        auto ptr = OutputChannels.FindPtr(channelId);
        YQL_ENSURE(ptr, "task: " << TaskId << " does not have output channelId: " << channelId);
        return *ptr;
    }

    IDqSink::TPtr GetSink(ui64 outputIndex) override {
        auto ptr = Sinks.FindPtr(outputIndex);
        YQL_ENSURE(ptr, "task: " << TaskId << " does not have output index: " << outputIndex);
        return *ptr;
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator(TMaybe<ui64> memoryLimit) override {
        auto guard = Context.TypeEnv ? Context.TypeEnv->BindAllocator() : SelfTypeEnv->BindAllocator();
        if (memoryLimit) {
            guard.GetMutex()->SetLimit(*memoryLimit);
        }
        return guard;
    }

    bool IsAllocatorAttached() override {
        return Context.TypeEnv ? Context.TypeEnv->GetAllocator().IsAttached() : SelfTypeEnv->GetAllocator().IsAttached();
    }

    const NKikimr::NMiniKQL::TTypeEnvironment& GetTypeEnv() const override {
        return Context.TypeEnv ? *Context.TypeEnv : *SelfTypeEnv;
    }

    const NKikimr::NMiniKQL::THolderFactory& GetHolderFactory() const override {
        return ProgramParsed.CompGraph->GetHolderFactory();
    }

    const THashMap<TString, TString>& GetSecureParams() const override {
        return Settings.SecureParams;
    }

    const THashMap<TString, TString>& GetTaskParams() const override {
        return Settings.TaskParams;
    }

    void UpdateStats() override {
        if (Stats) {
            Stats->RunStatusTimeMetrics.UpdateStatusTime();
        }
    }

    const TDqTaskRunnerStats* GetStats() const override {
        return Stats.get();
    }

    TString Save() const override {
        return ProgramParsed.CompGraph->SaveGraphState();
    }

    void Load(TStringBuf in) override {
        ProgramParsed.CompGraph->LoadGraphState(in);
    }

private:
    NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv() {
        return Context.TypeEnv ? *Context.TypeEnv : *SelfTypeEnv;
    }

    NKikimr::NMiniKQL::TScopedAlloc& Alloc() {
        return Context.Alloc ? *Context.Alloc : *SelfAlloc;
    }

    ERunStatus FetchAndDispatch() {
        if (!Output) {
            LOG("no consumers, Finish execution");
            return ERunStatus::Finished;
        }

        TBindTerminator term(ProgramParsed.CompGraph->GetTerminator());

        auto startComputeTime = TInstant::Now();
        Y_DEFER {
            if (Stats) {
                auto duration = TInstant::Now() - startComputeTime;
                Stats->ComputeCpuTime += duration;
                if (Y_UNLIKELY(CollectProfileStats)) {
                    RunComputeTime = duration;
                }
            }
        };

        while (!Output->IsFull()) {
            if (Y_UNLIKELY(CollectProfileStats)) {
                auto now = TInstant::Now();
                StopWaitingOutput(now);
                StopWaiting(now);
            }

            NUdf::TUnboxedValue value;
            auto fetchStatus = ResultStream.Fetch(value);

            switch (fetchStatus) {
                case NUdf::EFetchStatus::Ok: {
                    Output->Consume(std::move(value));
                    break;
                }
                case NUdf::EFetchStatus::Finish: {
                    LOG(TStringBuilder() << "task" << TaskId << ", execution finished, finish consumers");
                    Output->Finish();
                    return ERunStatus::Finished;
                }
                case NUdf::EFetchStatus::Yield: {
                    return ERunStatus::PendingInput;
                }
            }
        }

        return ERunStatus::PendingOutput;
    }

private:
    ui64 TaskId = 0;
    TDqTaskRunnerContext Context;
    TDqTaskRunnerSettings Settings;
    TLogFunc LogFunc;
    std::unique_ptr<NUdf::ISecureParamsProvider> SecureParamsProvider;

    std::unique_ptr<NKikimr::NMiniKQL::TScopedAlloc> SelfAlloc;       // if not set -> use Context.Alloc
    std::unique_ptr<NKikimr::NMiniKQL::TTypeEnvironment> SelfTypeEnv; // if not set -> use Context.TypeEnv

    struct TProgramParsed {
        TRuntimeNode ProgramNode;
        TVector<TType*> InputItemTypes;
        TVector<TType*> OutputItemTypes;
        TVector<TNode*> EntryPoints; // last entry node stands for parameters

        IStatsRegistryPtr StatsRegistry;
        IComputationPattern::TPtr CompPattern;
        THolder<IComputationGraph> CompGraph;
    };
    TProgramParsed ProgramParsed;

    THashMap<ui64, IDqInputChannel::TPtr> InputChannels; // Channel id -> Channel
    THashMap<ui64, IDqSource::TPtr> Sources; // Input index -> Source
    THashMap<ui64, IDqOutputChannel::TPtr> OutputChannels; // Channel id -> Channel
    THashMap<ui64, IDqSink::TPtr> Sinks; // Output index -> Sink
    IDqOutputConsumer::TPtr Output;
    NUdf::TUnboxedValue ResultStream;

    bool TaskHasEffects = false;

    bool CollectBasicStats = false;
    bool CollectProfileStats = false;
    std::unique_ptr<TDqTaskRunnerStats> Stats;
    TDuration RunComputeTime;

private:
    // statistics support
    std::optional<TInstant> StartWaitOutputTime;
    std::optional<TInstant> StartWaitTime;

    void StartWaitingOutput(TInstant now) {
        if (Y_UNLIKELY(CollectProfileStats) && !StartWaitOutputTime) {
            StartWaitOutputTime = now;
        }
    }

    void StopWaitingOutput(TInstant now) {
        if (Y_UNLIKELY(CollectProfileStats) && StartWaitOutputTime) {
            Stats->WaitOutputTime += (now - *StartWaitOutputTime);
            StartWaitOutputTime.reset();
        }
    }

    void StartWaiting(TInstant now) {
        if (Y_UNLIKELY(CollectProfileStats) && !StartWaitTime) {
            StartWaitTime = now;
        }
    }

    void StopWaiting(TInstant now) {
        if (Y_UNLIKELY(CollectProfileStats) && StartWaitTime) {
            Stats->WaitTime += (now - *StartWaitTime);
            StartWaitTime.reset();
        }
    }
};

TIntrusivePtr<IDqTaskRunner> MakeDqTaskRunner(const TDqTaskRunnerContext& ctx, const TDqTaskRunnerSettings& settings,
    const TLogFunc& logFunc)
{
    return new TDqTaskRunner(ctx, settings, logFunc);
}

} // namespace NYql::NDq
