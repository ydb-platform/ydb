#include "dq_tasks_runner.h"

#include <ydb/library/yql/minikql/comp_nodes/mkql_multihopping.h>

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/runtime/dq_input_channel.h>
#include <ydb/library/yql/dq/runtime/dq_input_producer.h>
#include <ydb/library/yql/dq/runtime/dq_async_input.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_pattern_cache.h>

#include <ydb/library/yql/parser/pg_wrapper/interface/codec.h>

#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/core/user_data/yql_user_data.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>

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

        case TType::EKind::Pg: {
            auto pgType = static_cast<const TPgType*>(type);
            if (value) {
                Y_UNUSED(NYql::NCommon::PgValueToNativeBinary(value, pgType->GetTypeId()));
            }
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
    TVector<IDqInput::TPtr>&& inputs, const THolderFactory& holderFactory, TDqMeteringStats::TInputStatsMeter stats)
{
    switch (inputDesc.GetTypeCase()) {
        case NYql::NDqProto::TTaskInput::kSource:
            Y_VERIFY(inputs.size() == 1);
            [[fallthrough]];
        case NYql::NDqProto::TTaskInput::kUnionAll:
            return CreateInputUnionValue(std::move(inputs), holderFactory, stats);
        case NYql::NDqProto::TTaskInput::kMerge: {
            const auto& protoSortCols = inputDesc.GetMerge().GetSortColumns();
            TVector<TSortColumnInfo> sortColsInfo;
            GetColumnsInfo(type, protoSortCols, sortColsInfo);
            YQL_ENSURE(!sortColsInfo.empty());

            return CreateInputMergeValue(std::move(inputs), std::move(sortColsInfo), holderFactory, stats);
        }
        default:
            YQL_ENSURE(false, "Unknown input type: " << (ui32) inputDesc.GetTypeCase());
    }
}

IDqOutputConsumer::TPtr DqBuildOutputConsumer(const NDqProto::TTaskOutput& outputDesc, const NMiniKQL::TType* type,
    const NMiniKQL::TTypeEnvironment& typeEnv, TVector<IDqOutput::TPtr>&& outputs)
{
    auto guard = typeEnv.BindAllocator();
    switch (outputDesc.GetTypeCase()) {
        case NDqProto::TTaskOutput::kSink:
            Y_VERIFY(outputDesc.ChannelsSize() == 0);
            [[fallthrough]];
        case NDqProto::TTaskOutput::kMap: {
            YQL_ENSURE(outputs.size() == 1);
            return CreateOutputMapConsumer(outputs[0]);
        }

        case NDqProto::TTaskOutput::kHashPartition: {
            TVector<TType*> keyColumnTypes;
            TVector<ui32> keyColumnIndices;
            GetColumnsInfo(type, outputDesc.GetHashPartition().GetKeyColumns(), keyColumnTypes, keyColumnIndices);
            YQL_ENSURE(!keyColumnTypes.empty());

            YQL_ENSURE(outputDesc.GetHashPartition().GetPartitionsCount() == outputDesc.ChannelsSize());
            TVector<ui64> channelIds(outputDesc.GetHashPartition().GetPartitionsCount());
            for (ui32 i = 0; i < outputDesc.ChannelsSize(); ++i) {
                channelIds[i] = outputDesc.GetChannels(i).GetId();
            }

            return CreateOutputHashPartitionConsumer(std::move(outputs), std::move(keyColumnTypes),
                std::move(keyColumnIndices));
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
        , AllocatedHolder(std::make_optional<TAllocatedHolder>())
        , CollectBasicStats(Settings.CollectBasicStats)
        , CollectProfileStats(Settings.CollectProfileStats)
    {
        if (CollectBasicStats) {
            Stats = std::make_unique<TDqTaskRunnerStats>();
            Stats->StartTs = TInstant::Now();
            if (Y_UNLIKELY(CollectProfileStats)) {
                Stats->ComputeCpuTimeByRun = NMonitoring::ExponentialHistogram(6, 10, 10);
            }
        } else {
            YQL_ENSURE(!CollectProfileStats, "CollectProfileStats requires CollectBasicStats to be set as well");
        }

        if (!Context.Alloc) {
            SelfAlloc = std::shared_ptr<TScopedAlloc>(new TScopedAlloc(__LOCATION__, TAlignedPagePoolCounters(),
                Context.FuncRegistry->SupportsSizedAllocators()), [](TScopedAlloc* ptr) {
                    ptr->Acquire();
                    delete ptr;
            });
        }

        if (!Context.TypeEnv) {
            AllocatedHolder->SelfTypeEnv = std::make_unique<TTypeEnvironment>(Context.Alloc ? *Context.Alloc : *SelfAlloc);
        }

        if (SelfAlloc) {
            SelfAlloc->Release();
        }
    }

    ~TDqTaskRunner() {
        if (SelfAlloc) {
            SelfAlloc->Acquire();
            AllocatedHolder.reset();
            SelfAlloc->Release();
        }
    }

    const TDqMeteringStats* GetMeteringStats() const override {
        return &BillingStats;
    }

    ui64 GetTaskId() const override {
        Y_VERIFY(TaskId, "Not prepared yet");
        return TaskId;
    }

    bool UseSeparatePatternAlloc() const {
        return Context.PatternCache && (Settings.OptLLVM == "OFF" || Settings.UseCacheForLLVM);
    }

    TComputationPatternOpts CreatePatternOpts(TScopedAlloc& alloc, TTypeEnvironment& typeEnv) {
        auto validatePolicy = Settings.TerminateOnError ? NUdf::EValidatePolicy::Fail : NUdf::EValidatePolicy::Exception;

        auto taskRunnerFactory = [this](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            auto& computationFactory = Context.ComputationFactory;
            if (auto res = computationFactory(callable, ctx)) {
                return res;
            }
            if (callable.GetType()->GetName() == "MultiHoppingCore") {
                return WrapMultiHoppingCore(callable, ctx, Watermark);
            }
            return nullptr;
        };

        if (Y_UNLIKELY(CollectProfileStats && !AllocatedHolder->ProgramParsed.StatsRegistry)) {
            AllocatedHolder->ProgramParsed.StatsRegistry = NMiniKQL::CreateDefaultStatsRegistry();
        }
        TComputationPatternOpts opts(alloc.Ref(), typeEnv, taskRunnerFactory,
            Context.FuncRegistry, NUdf::EValidateMode::None, validatePolicy, Settings.OptLLVM, EGraphPerProcess::Multi,
            AllocatedHolder->ProgramParsed.StatsRegistry.Get());

        if (!SecureParamsProvider) {
            SecureParamsProvider = MakeSimpleSecureParamsProvider(Settings.SecureParams);
        }
        opts.SecureParamsProvider = SecureParamsProvider.get();

        return opts;
    }

    std::shared_ptr<TPatternCacheEntry> CreateComputationPattern(const NDqProto::TDqTask& task, const TString& rawProgram, bool forCache, bool& canBeCached) {
        canBeCached = true;
        auto entry = TComputationPatternLRUCache::CreateCacheEntry(UseSeparatePatternAlloc());
        auto& patternAlloc = UseSeparatePatternAlloc() ? entry->Alloc : Alloc();
        auto& patternEnv = UseSeparatePatternAlloc() ? entry->Env : TypeEnv();
        patternAlloc.Ref().UseRefLocking = forCache;

        {
            auto guard = patternEnv.BindAllocator();
            entry->ProgramNode = DeserializeRuntimeNode(rawProgram, patternEnv);
        }

        YQL_ENSURE(entry->ProgramNode.IsImmediate() && entry->ProgramNode.GetNode()->GetType()->IsStruct());
        auto& programStruct = static_cast<TStructLiteral&>(*entry->ProgramNode.GetNode());
        auto programType = programStruct.GetType();
        YQL_ENSURE(programType);
        auto programRootIdx = programType->FindMemberIndex("Program");
        YQL_ENSURE(programRootIdx);
        TRuntimeNode programRoot = programStruct.GetValue(*programRootIdx);

        if (Context.FuncProvider) {
            auto guard = patternEnv.BindAllocator();
            TExploringNodeVisitor explorer;
            explorer.Walk(programRoot.GetNode(), patternEnv);
            bool wereChanges = false;
            programRoot = SinglePassVisitCallables(programRoot, explorer, Context.FuncProvider, patternEnv, true, wereChanges);
            if (wereChanges) {
                canBeCached = false;
            }
        }

        entry->OutputItemTypes.resize(task.OutputsSize());
        entry->OutputItemTypesRaw.resize(task.OutputsSize());

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
                    entry->OutputItemTypes[i] = variantTupleType->GetElementType(i);
                    entry->OutputItemTypesRaw[i] = SerializeNode(entry->OutputItemTypes[i], entry->Env);
                }
            }
            else {
                YQL_ENSURE(task.OutputsSize() == 1);
                entry->OutputItemTypes[0] = programResultItemType;
                entry->OutputItemTypesRaw[0] = SerializeNode(entry->OutputItemTypes[0], entry->Env);
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
        entry->ProgramInputsCount = programInputsCount;
        YQL_ENSURE(task.InputsSize() == programInputsCount);

        entry->InputItemTypes.resize(programInputsCount);
        entry->InputItemTypesRaw.resize(programInputsCount);
        entry->EntryPoints.resize(programInputsCount + 1 /* parameters */);
        for (ui32 i = 0; i < programInputsCount; ++i) {
            auto input = programInputsTuple.GetValue(i);
            TType* type = input.GetStaticType();
            YQL_ENSURE(type->GetKind() == TType::EKind::Stream);
            entry->InputItemTypes[i] = static_cast<TStreamType&>(*type).GetItemType();
            entry->InputItemTypesRaw[i] = SerializeNode(entry->InputItemTypes[i], entry->Env);
            entry->EntryPoints[i] = input.GetNode();
        }

        auto programParamsIdx = programType->FindMemberIndex("Parameters");
        YQL_ENSURE(programParamsIdx);
        entry->ProgramParams = programStruct.GetValue(*programParamsIdx);
        YQL_ENSURE(entry->ProgramParams.GetNode()->GetType()->IsCallable());
        auto paramsType = static_cast<TCallableType*>(entry->ProgramParams.GetNode()->GetType())->GetReturnType();
        YQL_ENSURE(paramsType->IsStruct());
        entry->EntryPoints[programInputsCount] = entry->ProgramParams.GetNode();
        entry->ParamsStruct = static_cast<TStructType*>(paramsType);

        TExploringNodeVisitor programExplorer;
        programExplorer.Walk(programRoot.GetNode(), patternEnv);
        auto programSize = programExplorer.GetNodes().size();

        LOG(TStringBuilder() << "task: " << TaskId << ", program size: " << programSize
            << ", llvm: `" << Settings.OptLLVM << "`.");

        auto opts = CreatePatternOpts(patternAlloc, patternEnv);
        opts.SetPatternEnv(entry);

        {
            auto guard = patternEnv.BindAllocator();
            entry->Pattern = MakeComputationPattern(programExplorer, programRoot, entry->EntryPoints, opts);
        }
        return entry;
    }

    std::shared_ptr<TPatternCacheEntry> BuildTask(const NDqProto::TDqTask& task, const TDqTaskRunnerParameterProvider& parameterProvider) {
        LOG(TStringBuilder() << "Build task: " << TaskId);
        auto startTime = TInstant::Now();

        const NDqProto::TProgram& program = task.GetProgram();
        YQL_ENSURE(program.GetRuntimeVersion());
        YQL_ENSURE(program.GetRuntimeVersion() <= NYql::NDqProto::ERuntimeVersion::RUNTIME_VERSION_YQL_1_0);

        std::shared_ptr<TPatternCacheEntry> entry;
        bool canBeCached;
        if (UseSeparatePatternAlloc() && Context.PatternCache) {
            auto& cache = Context.PatternCache;
            auto ticket = cache->FindOrSubscribe(program.GetRaw());
            if (!ticket.HasFuture()) {
                entry = CreateComputationPattern(task, program.GetRaw(), true, canBeCached);
                if (canBeCached && entry->Pattern->GetSuitableForCache()) {
                    cache->EmplacePattern(task.GetProgram().GetRaw(), entry);
                    ticket.Close();
                } else {
                    cache->IncNotSuitablePattern();
                }
            } else {
                entry = ticket.GetValueSync();
            }
        } 

        if (!entry) {
            entry = CreateComputationPattern(task, program.GetRaw(), false, canBeCached);
        }

        AllocatedHolder->ProgramParsed.PatternCacheEntry = entry;

        // clone pattern using TDqTaskRunner's alloc
        auto opts = CreatePatternOpts(Alloc(), TypeEnv());

        AllocatedHolder->ProgramParsed.CompGraph = AllocatedHolder->ProgramParsed.GetPattern()->Clone(
            opts.ToComputationOptions(*Context.RandomProvider, *Context.TimeProvider, &TypeEnv()));

        TBindTerminator term(AllocatedHolder->ProgramParsed.CompGraph->GetTerminator());

        auto paramNode = AllocatedHolder->ProgramParsed.CompGraph->GetEntryPoint(entry->ProgramInputsCount, /* require */ false);
        if (paramNode) {
            // TODO: Remove serialized parameters that are used in OLAP program and not used in current program
            const auto& graphHolderFactory = AllocatedHolder->ProgramParsed.CompGraph->GetHolderFactory();
            NUdf::TUnboxedValue* structMembers;
            auto paramsCount = entry->ParamsStruct->GetMembersCount();
            auto paramsStructValue = graphHolderFactory.CreateDirectArrayHolder(paramsCount, structMembers);

            for (ui32 i = 0; i < entry->ParamsStruct->GetMembersCount(); ++i) {
                std::string_view name = entry->ParamsStruct->GetMemberName(i);
                TType* type = entry->ParamsStruct->GetMemberType(i);

                if (parameterProvider && parameterProvider(name, type, TypeEnv(), graphHolderFactory, structMembers[i])) {
#ifndef NDEBUG
                    YQL_ENSURE(!task.GetParameters().contains(name), "param: " << name);
#endif
                } else {
                    auto it = task.GetParameters().find(name);
                    YQL_ENSURE(it != task.GetParameters().end());

                    auto guard = TypeEnv().BindAllocator();
                    TDqDataSerializer::DeserializeParam(it->second, type, graphHolderFactory, structMembers[i]);
                }

                {
                    auto guard = TypeEnv().BindAllocator();
                    ValidateParamValue(name, type, structMembers[i]);
                }
            }

            paramNode->SetValue(AllocatedHolder->ProgramParsed.CompGraph->GetContext(), std::move(paramsStructValue));
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
        return entry;
    }

    void Prepare(const NDqProto::TDqTask& task, const TDqTaskRunnerMemoryLimits& memoryLimits,
        const IDqTaskRunnerExecutionContext& execCtx, const TDqTaskRunnerParameterProvider& parameterProvider) override
    {
        TaskId = task.GetId();
        auto entry = BuildTask(task, parameterProvider);

        LOG(TStringBuilder() << "Prepare task: " << TaskId);
        auto startTime = TInstant::Now();

        auto& holderFactory = AllocatedHolder->ProgramParsed.CompGraph->GetHolderFactory();
        TBindTerminator term(AllocatedHolder->ProgramParsed.CompGraph->GetTerminator());

        auto& typeEnv = TypeEnv();

        for (ui32 i = 0; i < task.InputsSize(); ++i) {
            auto& inputDesc = task.GetInputs(i);
            auto& inputStats = BillingStats.AddInputs();

            TVector<IDqInput::TPtr> inputs{Reserve(std::max<ui64>(inputDesc.ChannelsSize(), 1))}; // 1 is for "source" type of input.
            TInputTransformInfo* transform = nullptr;
            TType** inputType = &entry->InputItemTypes[i];
            if (inputDesc.HasTransform()) {
                const auto& transformDesc = inputDesc.GetTransform();
                transform = &AllocatedHolder->InputTransforms[i];
                Y_VERIFY(!transform->TransformInput);
                Y_VERIFY(!transform->TransformOutput);

                auto inputTypeNode = NMiniKQL::DeserializeNode(TStringBuf{transformDesc.GetInputType()}, typeEnv);
                YQL_ENSURE(inputTypeNode, "Failed to deserialize transform input type");
                transform->TransformInputType = static_cast<TType*>(inputTypeNode);

                TStringBuf outputTypeNodeRaw(transformDesc.GetOutputType());
                auto outputTypeNode = NMiniKQL::DeserializeNode(outputTypeNodeRaw, typeEnv);
                YQL_ENSURE(outputTypeNode, "Failed to deserialize transform output type");
                TType* outputType = transform->TransformOutputType = static_cast<TType*>(outputTypeNode);
                auto typeCheckLog = [&] () {
                    TStringStream out;
                    out << *outputType << " != " << *entry->InputItemTypes[i];
                    LOG(TStringBuilder() << "Task: " << TaskId << " types is not the same: " << out.Str() << " has NOT been transformed by "
                        << transformDesc.GetType() << " with input type: " << *transform->TransformInputType
                        << " , output type: " << *outputType);
                    return out.Str();
                };
                YQL_ENSURE(outputTypeNodeRaw == entry->InputItemTypesRaw[i], "" << typeCheckLog());
                LOG(TStringBuilder() << "Task: " << TaskId << " has transform by "
                    << transformDesc.GetType() << " with input type: " << *transform->TransformInputType
                    << " , output type: " << *outputType);

                transform->TransformOutput = CreateDqAsyncInputBuffer(i, outputType,
                    memoryLimits.ChannelBufferSize, Settings.CollectProfileStats);

                inputType = &transform->TransformInputType;
            }

            if (inputDesc.HasSource()) {
                auto source = CreateDqAsyncInputBuffer(i, *inputType,
                    memoryLimits.ChannelBufferSize, Settings.CollectProfileStats);
                auto [_, inserted] = AllocatedHolder->Sources.emplace(i, source);
                Y_VERIFY(inserted);
                inputs.emplace_back(source);
            } else {
                for (auto& inputChannelDesc : inputDesc.GetChannels()) {
                    ui64 channelId = inputChannelDesc.GetId();
                    auto inputChannel = CreateDqInputChannel(channelId, *inputType,
                        memoryLimits.ChannelBufferSize, Settings.CollectProfileStats, typeEnv, holderFactory,
                        inputChannelDesc.GetTransportVersion());
                    auto ret = AllocatedHolder->InputChannels.emplace(channelId, inputChannel);
                    YQL_ENSURE(ret.second, "task: " << TaskId << ", duplicated input channelId: " << channelId);
                    inputs.emplace_back(inputChannel);
                }
            }

            auto entryNode = AllocatedHolder->ProgramParsed.CompGraph->GetEntryPoint(i, true);
            if (transform) {
                transform->TransformInput = DqBuildInputValue(inputDesc, transform->TransformInputType, std::move(inputs), holderFactory, {});
                inputs.clear();
                inputs.emplace_back(transform->TransformOutput);
                entryNode->SetValue(AllocatedHolder->ProgramParsed.CompGraph->GetContext(),
                    CreateInputUnionValue(std::move(inputs), holderFactory, 
                        {&inputStats, transform->TransformOutputType}));
            } else {
                entryNode->SetValue(AllocatedHolder->ProgramParsed.CompGraph->GetContext(),
                    DqBuildInputValue(inputDesc, entry->InputItemTypes[i], std::move(inputs), holderFactory,
                        {&inputStats, entry->InputItemTypes[i]}));
            }
        }

        TVector<IDqOutputConsumer::TPtr> outputConsumers(task.OutputsSize());
        for (ui32 i = 0; i < task.OutputsSize(); ++i) {
            const auto& outputDesc = task.GetOutputs(i);

            if (outputDesc.GetTypeCase() == NDqProto::TTaskOutput::kEffects) {
                TaskHasEffects = true;
            }

            TVector<IDqOutput::TPtr> outputs{Reserve(std::max<ui64>(outputDesc.ChannelsSize(), 1))};
            TOutputTransformInfo* transform = nullptr;
            TType** taskOutputType = &entry->OutputItemTypes[i];
            if (outputDesc.HasTransform()) {
                const auto& transformDesc = outputDesc.GetTransform();
                transform = &AllocatedHolder->OutputTransforms[i];
                Y_VERIFY(!transform->TransformInput);
                Y_VERIFY(!transform->TransformOutput);

                auto outputTypeNode = NMiniKQL::DeserializeNode(TStringBuf{transformDesc.GetOutputType()}, typeEnv);
                YQL_ENSURE(outputTypeNode, "Failed to deserialize transform output type");
                transform->TransformOutputType = static_cast<TType*>(outputTypeNode);

                TStringBuf inputTypeNodeRaw(transformDesc.GetInputType());
                auto inputTypeNode = NMiniKQL::DeserializeNode(inputTypeNodeRaw, typeEnv);
                YQL_ENSURE(inputTypeNode, "Failed to deserialize transform input type");
                TType* inputType = static_cast<TType*>(inputTypeNode);
                YQL_ENSURE(inputTypeNodeRaw == entry->OutputItemTypesRaw[i]);
                LOG(TStringBuilder() << "Task: " << TaskId << " has transform by "
                    << transformDesc.GetType() << " with input type: " << *inputType
                    << " , output type: " << *transform->TransformOutputType);

                transform->TransformInput = CreateDqAsyncOutputBuffer(i, entry->OutputItemTypes[i], memoryLimits.ChannelBufferSize,
                    Settings.CollectProfileStats);

                taskOutputType = &transform->TransformOutputType;
            }
            if (outputDesc.HasSink()) {
                auto sink = CreateDqAsyncOutputBuffer(i, *taskOutputType, memoryLimits.ChannelBufferSize,
                    Settings.CollectProfileStats);
                auto [_, inserted] = AllocatedHolder->Sinks.emplace(i, sink);
                Y_VERIFY(inserted);
                outputs.emplace_back(sink);
            } else {
                for (auto& outputChannelDesc : outputDesc.GetChannels()) {
                    ui64 channelId = outputChannelDesc.GetId();

                    TDqOutputChannelSettings settings;
                    settings.MaxStoredBytes = memoryLimits.ChannelBufferSize;
                    settings.MaxChunkBytes = memoryLimits.OutputChunkMaxSize;
                    settings.ChunkSizeLimit = memoryLimits.ChunkSizeLimit;
                    settings.TransportVersion = outputChannelDesc.GetTransportVersion();
                    settings.CollectProfileStats = Settings.CollectProfileStats;
                    settings.AllowGeneratorsInUnboxedValues = Settings.AllowGeneratorsInUnboxedValues;

                    if (!outputChannelDesc.GetInMemory()) {
                        settings.ChannelStorage = execCtx.CreateChannelStorage(channelId);
                    }

                    auto outputChannel = CreateDqOutputChannel(channelId, *taskOutputType, typeEnv,
                        holderFactory, settings, LogFunc);

                    auto ret = AllocatedHolder->OutputChannels.emplace(channelId, outputChannel);
                    YQL_ENSURE(ret.second, "task: " << TaskId << ", duplicated output channelId: " << channelId);
                    outputs.emplace_back(outputChannel);
                }
            }

            if (transform) {
                auto guard = BindAllocator();
                transform->TransformOutput = execCtx.CreateOutputConsumer(outputDesc, transform->TransformOutputType,
                    Context.ApplyCtx, typeEnv, std::move(outputs));

                outputs.clear();
                outputs.emplace_back(transform->TransformInput);
            }

            {
                auto guard = BindAllocator();
                outputConsumers[i] = execCtx.CreateOutputConsumer(outputDesc, entry->OutputItemTypes[i],
                    Context.ApplyCtx, typeEnv, std::move(outputs));
            }
        }

        if (outputConsumers.empty()) {
            AllocatedHolder->Output = nullptr;
        } else if (outputConsumers.size() == 1) {
            AllocatedHolder->Output = std::move(outputConsumers[0]);
        } else {
            auto guard = BindAllocator();
            AllocatedHolder->Output = CreateOutputMultiConsumer(std::move(outputConsumers));
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

            for (auto&[channelId, inputChannel] : AllocatedHolder->InputChannels) {
                Stats->InputChannels.emplace(channelId, inputChannel->GetStats());
            }
            for (auto&[inputIndex, source] : AllocatedHolder->Sources) {
                Stats->Sources.emplace(inputIndex, source->GetStats());
            }
            for (auto&[channelId, outputChannel] : AllocatedHolder->OutputChannels) {
                Stats->OutputChannels.emplace(channelId, outputChannel->GetStats());
            }
        }
    }

    ERunStatus Run() final {
        LOG(TStringBuilder() << "Run task: " << TaskId);
        if (!AllocatedHolder->ResultStream) {
            auto guard = BindAllocator();
            TBindTerminator term(AllocatedHolder->ProgramParsed.CompGraph->GetTerminator());
            AllocatedHolder->ResultStream = AllocatedHolder->ProgramParsed.CompGraph->GetValue();
        }

        RunComputeTime = TDuration::Zero();

        auto runStatus = FetchAndDispatch();
        if (Stats) {
            Stats->RunStatusTimeMetrics.SetCurrentStatus(runStatus, RunComputeTime);
        }

        if (Y_UNLIKELY(CollectProfileStats)) {
            Stats->ComputeCpuTimeByRun->Collect(RunComputeTime.MilliSeconds());

            if (AllocatedHolder->ProgramParsed.StatsRegistry) {
                Stats->MkqlStats.clear();
                AllocatedHolder->ProgramParsed.StatsRegistry->ForEachStat([this](const TStatKey& key, i64 value) {
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

    void SetWatermarkIn(TInstant time) override {
        Watermark.WatermarkIn = std::move(time);
    }

    const NKikimr::NMiniKQL::TWatermark& GetWatermark() const override {
        return Watermark;
    }

    IDqInputChannel::TPtr GetInputChannel(ui64 channelId) override {
        auto ptr = AllocatedHolder->InputChannels.FindPtr(channelId);
        YQL_ENSURE(ptr, "task: " << TaskId << " does not have input channelId: " << channelId);
        return *ptr;
    }

    IDqAsyncInputBuffer::TPtr GetSource(ui64 inputIndex) override {
        auto ptr = AllocatedHolder->Sources.FindPtr(inputIndex);
        YQL_ENSURE(ptr, "task: " << TaskId << " does not have input index: " << inputIndex);
        return *ptr;
    }

    IDqOutputChannel::TPtr GetOutputChannel(ui64 channelId) override {
        auto ptr = AllocatedHolder->OutputChannels.FindPtr(channelId);
        YQL_ENSURE(ptr, "task: " << TaskId << " does not have output channelId: " << channelId);
        return *ptr;
    }

    IDqAsyncOutputBuffer::TPtr GetSink(ui64 outputIndex) override {
        auto ptr = AllocatedHolder->Sinks.FindPtr(outputIndex);
        YQL_ENSURE(ptr, "task: " << TaskId << " does not have output index: " << outputIndex);
        return *ptr;
    }

    std::pair<NUdf::TUnboxedValue, IDqAsyncInputBuffer::TPtr> GetInputTransform(ui64 inputIndex) override {
        auto ptr = AllocatedHolder->InputTransforms.FindPtr(inputIndex);
        YQL_ENSURE(ptr, "task: " << TaskId << " does not have input index: " << inputIndex << " or such transform");
        return {ptr->TransformInput, ptr->TransformOutput};
    }

    std::pair<IDqAsyncOutputBuffer::TPtr, IDqOutputConsumer::TPtr> GetOutputTransform(ui64 outputIndex) override {
        auto ptr = AllocatedHolder->OutputTransforms.FindPtr(outputIndex);
        YQL_ENSURE(ptr, "task: " << TaskId << " does not have output index: " << outputIndex << " or such transform");
        return {ptr->TransformInput, ptr->TransformOutput};
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator(TMaybe<ui64> memoryLimit = {}) override {
        auto guard = Context.TypeEnv ? Context.TypeEnv->BindAllocator() : AllocatedHolder->SelfTypeEnv->BindAllocator();
        if (memoryLimit) {
            guard.GetMutex()->SetLimit(*memoryLimit);
        }
        return guard;
    }

    bool IsAllocatorAttached() override {
        return Context.TypeEnv ? Context.TypeEnv->GetAllocator().IsAttached() : AllocatedHolder->SelfTypeEnv->GetAllocator().IsAttached();
    }

    const NKikimr::NMiniKQL::TTypeEnvironment& GetTypeEnv() const override {
        return Context.TypeEnv ? *Context.TypeEnv : *AllocatedHolder->SelfTypeEnv;
    }

    const NKikimr::NMiniKQL::THolderFactory& GetHolderFactory() const override {
        return AllocatedHolder->ProgramParsed.CompGraph->GetHolderFactory();
    }

    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> GetAllocatorPtr() const override {
        return SelfAlloc;
    }

    const THashMap<TString, TString>& GetSecureParams() const override {
        return Settings.SecureParams;
    }

    const THashMap<TString, TString>& GetTaskParams() const override {
        return Settings.TaskParams;
    }

    IRandomProvider* GetRandomProvider() const override {
        return Context.RandomProvider;
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
        return AllocatedHolder->ProgramParsed.CompGraph->SaveGraphState();
    }

    void Load(TStringBuf in) override {
        Y_VERIFY(!AllocatedHolder->ResultStream);
        AllocatedHolder->ProgramParsed.CompGraph->LoadGraphState(in);
    }

private:
    NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv() {
        return Context.TypeEnv ? *Context.TypeEnv : *AllocatedHolder->SelfTypeEnv;
    }

    NKikimr::NMiniKQL::TScopedAlloc& Alloc() {
        return Context.Alloc ? *Context.Alloc : *SelfAlloc;
    }

    void FinishImpl() {
        LOG(TStringBuilder() << "task" << TaskId << ", execution finished, finish consumers");
        AllocatedHolder->Output->Finish();
    }

    ERunStatus FetchAndDispatch() {
        if (!AllocatedHolder->Output) {
            LOG("no consumers, Finish execution");
            return ERunStatus::Finished;
        }

        TBindTerminator term(AllocatedHolder->ProgramParsed.CompGraph->GetTerminator());

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

        auto guard = BindAllocator();
        if (AllocatedHolder->Output->IsFinishing()) {
            if (AllocatedHolder->Output->TryFinish()) {
                FinishImpl();
                return ERunStatus::Finished;
            } else {
                return ERunStatus::PendingOutput;
            }
        }
        while (!AllocatedHolder->Output->IsFull()) {
            if (Y_UNLIKELY(CollectProfileStats)) {
                auto now = TInstant::Now();
                StopWaitingOutput(now);
                StopWaiting(now);
            }

            NUdf::TUnboxedValue value;
            auto fetchStatus = AllocatedHolder->ResultStream.Fetch(value);

            switch (fetchStatus) {
                case NUdf::EFetchStatus::Ok: {
                    AllocatedHolder->Output->Consume(std::move(value));
                    break;
                }
                case NUdf::EFetchStatus::Finish: {
                    if (!AllocatedHolder->Output->TryFinish()) {
                        break;
                    }
                    FinishImpl();
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

    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> SelfAlloc;       // if not set -> use Context.Alloc

    struct TInputTransformInfo {
        NUdf::TUnboxedValue TransformInput;
        IDqAsyncInputBuffer::TPtr TransformOutput;
        TType* TransformInputType = nullptr;
        TType* TransformOutputType = nullptr;
    };

    struct TOutputTransformInfo {
        IDqAsyncOutputBuffer::TPtr TransformInput;
        IDqOutputConsumer::TPtr TransformOutput;
        TType* TransformOutputType = nullptr;
    };

    struct TProgramParsed {
        IStatsRegistryPtr StatsRegistry;
        std::shared_ptr<TPatternCacheEntry> PatternCacheEntry;
        THolder<IComputationGraph> CompGraph;

        IComputationPattern* GetPattern() {
            return PatternCacheEntry->Pattern.Get();
        }
    };

    struct TAllocatedHolder {
        std::unique_ptr<NKikimr::NMiniKQL::TTypeEnvironment> SelfTypeEnv; // if not set -> use Context.TypeEnv

        TProgramParsed ProgramParsed;

        THashMap<ui64, IDqInputChannel::TPtr> InputChannels; // Channel id -> Channel
        THashMap<ui64, IDqAsyncInputBuffer::TPtr> Sources; // Input index -> Source
        THashMap<ui64, TInputTransformInfo> InputTransforms; // Output index -> Transform
        THashMap<ui64, IDqOutputChannel::TPtr> OutputChannels; // Channel id -> Channel
        THashMap<ui64, IDqAsyncOutputBuffer::TPtr> Sinks; // Output index -> Sink
        THashMap<ui64, TOutputTransformInfo> OutputTransforms; // Output index -> Transform

        IDqOutputConsumer::TPtr Output;
        NUdf::TUnboxedValue ResultStream;
    };

    std::optional<TAllocatedHolder> AllocatedHolder;
    NKikimr::NMiniKQL::TWatermark Watermark;

    bool TaskHasEffects = false;

    bool CollectBasicStats = false;
    bool CollectProfileStats = false;
    std::unique_ptr<TDqTaskRunnerStats> Stats;
    TDqMeteringStats BillingStats;
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
