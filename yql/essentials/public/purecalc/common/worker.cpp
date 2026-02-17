#include "worker.h"
#include "compile_mkql.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_user_data.h>
#include <yql/essentials/core/yql_user_data_storage.h>
#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>
#include <yql/essentials/public/purecalc/common/names.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <yql/essentials/providers/common/mkql/yql_type_mkql.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/stream/file.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>

using namespace NYql;
using namespace NYql::NPureCalc;

TWorkerGraph::TWorkerGraph(
    const TExprNode::TPtr& exprRoot,
    TExprContext& exprCtx,
    const TString& serializedProgram,
    const NKikimr::NMiniKQL::IFunctionRegistry& funcRegistry,
    const TUserDataTable& userData,
    const TVector<const TStructExprType*>& inputTypes,
    const TVector<const TStructExprType*>& originalInputTypes,
    const TVector<const TStructExprType*>& rawInputTypes,
    const TTypeAnnotationNode* outputType,
    const TTypeAnnotationNode* rawOutputType,
    const TString& LLVMSettings,
    NKikimr::NUdf::ICountersProvider* countersProvider,
    ui64 nativeYtTypeFlags,
    TMaybe<ui64> deterministicTimeProviderSeed,
    TLangVersion langver,
    bool insideEvaluation)
    : ScopedAlloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), funcRegistry.SupportsSizedAllocators())
    , Env(ScopedAlloc)
    , FuncRegistry(funcRegistry)
    , RandomProvider(CreateDefaultRandomProvider())
    , TimeProvider(deterministicTimeProviderSeed ? CreateDeterministicTimeProvider(*deterministicTimeProviderSeed) : CreateDefaultTimeProvider())
    , LLVMSettings(LLVMSettings)
    , NativeYtTypeFlags(nativeYtTypeFlags)
{
    // Build the root MKQL node
    NCommon::TMemoizedTypesMap typeMemoization;
    NKikimr::NMiniKQL::TRuntimeNode rootNode;
    if (exprRoot) {
        rootNode = CompileMkql(exprRoot, exprCtx, FuncRegistry, Env, userData, &typeMemoization);
    } else {
        rootNode = NKikimr::NMiniKQL::DeserializeRuntimeNode(serializedProgram, Env);
    }

    // Prepare container for input nodes

    const ui32 inputsCount = inputTypes.size();

    YQL_ENSURE(inputTypes.size() == originalInputTypes.size());

    SelfNodes.resize(inputsCount, nullptr);

    YQL_ENSURE(SelfNodes.size() == inputsCount);

    // Setup struct types

    NKikimr::NMiniKQL::TProgramBuilder pgmBuilder(Env, FuncRegistry, false, langver);
    for (ui32 i = 0; i < inputsCount; ++i) {
        const auto* type = static_cast<NKikimr::NMiniKQL::TStructType*>(NCommon::BuildType(TPositionHandle(), *inputTypes[i], pgmBuilder, typeMemoization));
        const auto* originalType = type;
        const auto* rawType = static_cast<NKikimr::NMiniKQL::TStructType*>(NCommon::BuildType(TPositionHandle(), *rawInputTypes[i], pgmBuilder, typeMemoization));
        if (inputTypes[i] != originalInputTypes[i]) {
            YQL_ENSURE(inputTypes[i]->GetSize() >= originalInputTypes[i]->GetSize());
            originalType = static_cast<NKikimr::NMiniKQL::TStructType*>(NCommon::BuildType(TPositionHandle(), *originalInputTypes[i], pgmBuilder, typeMemoization));
        }

        InputTypes.push_back(type);
        OriginalInputTypes.push_back(originalType);
        RawInputTypes.push_back(rawType);
    }

    if (outputType) {
        OutputType = NCommon::BuildType(TPositionHandle(), *outputType, pgmBuilder, typeMemoization);
    }
    if (rawOutputType) {
        RawOutputType = NCommon::BuildType(TPositionHandle(), *rawOutputType, pgmBuilder, typeMemoization);
    }

    if (!exprRoot) {
        auto outMkqlType = rootNode.GetStaticType();
        if (outMkqlType->GetKind() == NKikimr::NMiniKQL::TType::EKind::List) {
            outMkqlType = static_cast<NKikimr::NMiniKQL::TListType*>(outMkqlType)->GetItemType();
        } else if (outMkqlType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Stream) {
            outMkqlType = static_cast<NKikimr::NMiniKQL::TStreamType*>(outMkqlType)->GetItemType();
        } else {
            ythrow TCompileError("", "") << "unexpected mkql output type " << NKikimr::NMiniKQL::TType::KindAsStr(outMkqlType->GetKind());
        }
        if (OutputType) {
            if (!OutputType->IsSameType(*outMkqlType)) {
                ythrow TCompileError("", "") << "precompiled program output type doesn't match the output schema";
            }
        } else {
            OutputType = outMkqlType;
            RawOutputType = outMkqlType;
        }
    }

    // Compile computation pattern

    const THashSet<NKikimr::NMiniKQL::TInternName> selfCallableNames = {
        Env.InternName(PurecalcInputCallableName),
        Env.InternName(PurecalcBlockInputCallableName)};

    NKikimr::NMiniKQL::TExploringNodeVisitor explorer;
    explorer.Walk(rootNode.GetNode(), Env.GetNodeStack());

    auto compositeNodeFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory(
        {NKikimr::NMiniKQL::GetYqlFactory(), NYql::GetPgFactory()});

    auto nodeFactory = [&](
                           NKikimr::NMiniKQL::TCallable& callable, const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx) -> NKikimr::NMiniKQL::IComputationNode* {
        if (selfCallableNames.contains(callable.GetType()->GetNameStr())) {
            if (insideEvaluation) {
                throw TErrorException(0) << "Inputs aren't available during evaluation";
            }

            YQL_ENSURE(callable.GetInputsCount() == 1, "Self takes exactly 1 argument");
            const auto inputIndex = AS_VALUE(NKikimr::NMiniKQL::TDataLiteral, callable.GetInput(0))->AsValue().Get<ui32>();
            YQL_ENSURE(inputIndex < inputsCount, "Self index is out of range");
            YQL_ENSURE(!SelfNodes[inputIndex], "Self can be called at most once with each index");
            return SelfNodes[inputIndex] = new NKikimr::NMiniKQL::TExternalComputationNode(ctx.Mutables);
        } else {
            return compositeNodeFactory(callable, ctx);
        }
    };

    NKikimr::NMiniKQL::TComputationPatternOpts computationPatternOpts(
        ScopedAlloc.Ref(),
        Env,
        nodeFactory,
        &funcRegistry,
        NKikimr::NUdf::EValidateMode::None,
        NKikimr::NUdf::EValidatePolicy::Exception,
        LLVMSettings,
        NKikimr::NMiniKQL::EGraphPerProcess::Multi,
        nullptr,
        countersProvider,
        nullptr,
        nullptr,
        langver);

    ComputationPattern = NKikimr::NMiniKQL::MakeComputationPattern(
        explorer,
        rootNode,
        {rootNode.GetNode()},
        computationPatternOpts);

    ComputationGraph = ComputationPattern->Clone(
        computationPatternOpts.ToComputationOptions(*RandomProvider, *TimeProvider));

    ComputationGraph->Prepare();

    // Scoped alloc acquires itself on construction. We need to release it before returning control to user.
    // Note that scoped alloc releases itself on destruction so it is no problem if the above code throws.
    ScopedAlloc.Release();
}

TWorkerGraph::~TWorkerGraph() {
    // Remember, we've released scoped alloc in constructor? Now, we need to acquire it back before destroying.
    ScopedAlloc.Acquire();
}

template <typename TBase>
TWorker<TBase>::TWorker(
    TWorkerFactoryPtr factory,
    const TExprNode::TPtr& exprRoot,
    TExprContext& exprCtx,
    const TString& serializedProgram,
    const NKikimr::NMiniKQL::IFunctionRegistry& funcRegistry,
    const TUserDataTable& userData,
    const TVector<const TStructExprType*>& inputTypes,
    const TVector<const TStructExprType*>& originalInputTypes,
    const TVector<const TStructExprType*>& rawInputTypes,
    const TTypeAnnotationNode* outputType,
    const TTypeAnnotationNode* rawOutputType,
    const TString& LLVMSettings,
    NKikimr::NUdf::ICountersProvider* countersProvider,
    ui64 nativeYtTypeFlags,
    TMaybe<ui64> deterministicTimeProviderSeed,
    TLangVersion langver)
    : WorkerFactory_(std::move(factory))
    , Graph_(exprRoot, exprCtx, serializedProgram, funcRegistry, userData,
             inputTypes, originalInputTypes, rawInputTypes, outputType, rawOutputType,
             LLVMSettings, countersProvider, nativeYtTypeFlags, deterministicTimeProviderSeed, langver, false)
{
}

template <typename TBase>
inline ui32 TWorker<TBase>::GetInputsCount() const {
    return Graph_.InputTypes.size();
}

template <typename TBase>
inline const NKikimr::NMiniKQL::TStructType* TWorker<TBase>::GetInputType(ui32 inputIndex, bool original) const {
    const auto& container = original ? Graph_.OriginalInputTypes : Graph_.InputTypes;

    YQL_ENSURE(inputIndex < container.size(), "invalid input index (" << inputIndex << ") in GetInputType call");

    return container[inputIndex];
}

template <typename TBase>
inline const NKikimr::NMiniKQL::TStructType* TWorker<TBase>::GetInputType(bool original) const {
    const auto& container = original ? Graph_.OriginalInputTypes : Graph_.InputTypes;

    YQL_ENSURE(container.size() == 1, "GetInputType() can be used only for single-input programs");

    return container[0];
}

template <typename TBase>
inline const NKikimr::NMiniKQL::TStructType* TWorker<TBase>::GetRawInputType(ui32 inputIndex) const {
    const auto& container = Graph_.RawInputTypes;
    YQL_ENSURE(inputIndex < container.size(), "invalid input index (" << inputIndex << ") in GetInputType call");
    return container[inputIndex];
}

template <typename TBase>
inline const NKikimr::NMiniKQL::TStructType* TWorker<TBase>::GetRawInputType() const {
    const auto& container = Graph_.RawInputTypes;
    YQL_ENSURE(container.size() == 1, "GetInputType() can be used only for single-input programs");
    return container[0];
}

template <typename TBase>
inline const NKikimr::NMiniKQL::TType* TWorker<TBase>::GetOutputType() const {
    return Graph_.OutputType;
}

template <typename TBase>
inline const NKikimr::NMiniKQL::TType* TWorker<TBase>::GetRawOutputType() const {
    return Graph_.RawOutputType;
}

template <typename TBase>
NYT::TNode TWorker<TBase>::MakeInputSchema(ui32 inputIndex) const {
    auto p = WorkerFactory_.lock();
    YQL_ENSURE(p, "Access to destroyed worker factory");
    return p->MakeInputSchema(inputIndex);
}

template <typename TBase>
NYT::TNode TWorker<TBase>::MakeInputSchema() const {
    auto p = WorkerFactory_.lock();
    YQL_ENSURE(p, "Access to destroyed worker factory");
    return p->MakeInputSchema();
}

template <typename TBase>
NYT::TNode TWorker<TBase>::MakeOutputSchema() const {
    auto p = WorkerFactory_.lock();
    YQL_ENSURE(p, "Access to destroyed worker factory");
    return p->MakeOutputSchema();
}

template <typename TBase>
NYT::TNode TWorker<TBase>::MakeOutputSchema(ui32) const {
    auto p = WorkerFactory_.lock();
    YQL_ENSURE(p, "Access to destroyed worker factory");
    return p->MakeOutputSchema();
}

template <typename TBase>
NYT::TNode TWorker<TBase>::MakeOutputSchema(TStringBuf) const {
    auto p = WorkerFactory_.lock();
    YQL_ENSURE(p, "Access to destroyed worker factory");
    return p->MakeOutputSchema();
}

template <typename TBase>
NYT::TNode TWorker<TBase>::MakeFullOutputSchema() const {
    auto p = WorkerFactory_.lock();
    YQL_ENSURE(p, "Access to destroyed worker factory");
    return p->MakeFullOutputSchema();
}

template <typename TBase>
inline NKikimr::NMiniKQL::TScopedAlloc& TWorker<TBase>::GetScopedAlloc() {
    return Graph_.ScopedAlloc;
}

template <typename TBase>
inline NKikimr::NMiniKQL::IComputationGraph& TWorker<TBase>::GetGraph() {
    return *Graph_.ComputationGraph;
}

template <typename TBase>
inline const NKikimr::NMiniKQL::IFunctionRegistry&
TWorker<TBase>::GetFunctionRegistry() const {
    return Graph_.FuncRegistry;
}

template <typename TBase>
inline NKikimr::NMiniKQL::TTypeEnvironment&
TWorker<TBase>::GetTypeEnvironment() {
    return Graph_.Env;
}

template <typename TBase>
inline const TString& TWorker<TBase>::GetLLVMSettings() const {
    return Graph_.LLVMSettings;
}

template <typename TBase>
inline ui64 TWorker<TBase>::GetNativeYtTypeFlags() const {
    return Graph_.NativeYtTypeFlags;
}

template <typename TBase>
ITimeProvider* TWorker<TBase>::GetTimeProvider() const {
    return Graph_.TimeProvider.Get();
}

template <typename TBase>
void TWorker<TBase>::Release() {
    if (auto p = WorkerFactory_.lock()) {
        p->ReturnWorker(this);
    } else {
        delete this;
    }
}

template <typename TBase>
void TWorker<TBase>::Invalidate() {
    auto& ctx = Graph_.ComputationGraph->GetContext();
    for (const auto* selfNode : Graph_.SelfNodes) {
        if (selfNode) {
            selfNode->InvalidateValue(ctx);
        }
    }
    Graph_.ComputationGraph->InvalidateCaches();
}

template <typename TBase>
void TWorker<TBase>::CheckState(bool finish) {
    PrepareCheckState(finish);
    if (finish) {
        Graph_.ComputationGraph->Invalidate();
    }

    if (auto pos = Graph_.ComputationGraph->GetNotConsumedLinear()) {
        UdfTerminate((TStringBuilder() << pos << " Linear value is not consumed").c_str());
    }
}

TPullStreamWorker::~TPullStreamWorker() {
    auto guard = Guard(GetScopedAlloc());
    Output_.Clear();
}

void TPullStreamWorker::SetInput(NKikimr::NUdf::TUnboxedValue&& value, ui32 inputIndex) {
    const auto inputsCount = Graph_.SelfNodes.size();

    if (Y_UNLIKELY(inputIndex >= inputsCount)) {
        ythrow yexception() << "invalid input index (" << inputIndex << ") in SetInput call";
    }

    if (HasInput_.size() < inputsCount) {
        HasInput_.resize(inputsCount, false);
    }

    if (Y_UNLIKELY(HasInput_[inputIndex])) {
        ythrow yexception() << "input value for #" << inputIndex << " input is already set";
    }

    auto selfNode = Graph_.SelfNodes[inputIndex];

    if (selfNode) {
        YQL_ENSURE(value);
        selfNode->SetValue(Graph_.ComputationGraph->GetContext(), std::move(value));
    }

    HasInput_[inputIndex] = true;

    if (CheckAllInputsSet()) {
        NKikimr::NMiniKQL::TBindTerminator bind(Graph_.ComputationGraph->GetTerminator());
        Output_ = Graph_.ComputationGraph->GetValue();
    }
}

NKikimr::NUdf::TUnboxedValue& TPullStreamWorker::GetOutput() {
    if (Y_UNLIKELY(!CheckAllInputsSet())) {
        ythrow yexception() << "some input values have not been set";
    }

    return Output_;
}

void TPullStreamWorker::PrepareCheckState(bool finish) {
    if (finish) {
        Output_ = NKikimr::NUdf::TUnboxedValue::Invalid();
    }
}

void TPullStreamWorker::Release() {
    with_lock (GetScopedAlloc()) {
        Output_ = NKikimr::NUdf::TUnboxedValue::Invalid();
        for (auto selfNode : Graph_.SelfNodes) {
            if (selfNode) {
                selfNode->SetValue(Graph_.ComputationGraph->GetContext(), NKikimr::NUdf::TUnboxedValue::Invalid());
            }
        }
    }
    HasInput_.clear();
    TWorker<IPullStreamWorker>::Release();
}

TPullListWorker::~TPullListWorker() {
    auto guard = Guard(GetScopedAlloc());
    Output_.Clear();
    OutputIterator_.Clear();
}

void TPullListWorker::SetInput(NKikimr::NUdf::TUnboxedValue&& value, ui32 inputIndex) {
    const auto inputsCount = Graph_.SelfNodes.size();

    if (Y_UNLIKELY(inputIndex >= inputsCount)) {
        ythrow yexception() << "invalid input index (" << inputIndex << ") in SetInput call";
    }

    if (HasInput_.size() < inputsCount) {
        HasInput_.resize(inputsCount, false);
    }

    if (Y_UNLIKELY(HasInput_[inputIndex])) {
        ythrow yexception() << "input value for #" << inputIndex << " input is already set";
    }

    auto selfNode = Graph_.SelfNodes[inputIndex];

    if (selfNode) {
        YQL_ENSURE(value);
        selfNode->SetValue(Graph_.ComputationGraph->GetContext(), std::move(value));
    }

    HasInput_[inputIndex] = true;

    if (CheckAllInputsSet()) {
        NKikimr::NMiniKQL::TBindTerminator bind(Graph_.ComputationGraph->GetTerminator());
        Output_ = Graph_.ComputationGraph->GetValue();
        ResetOutputIterator();
    }
}

NKikimr::NUdf::TUnboxedValue& TPullListWorker::GetOutput() {
    if (Y_UNLIKELY(!CheckAllInputsSet())) {
        ythrow yexception() << "some input values have not been set";
    }

    return Output_;
}

NKikimr::NUdf::TUnboxedValue& TPullListWorker::GetOutputIterator() {
    if (Y_UNLIKELY(!CheckAllInputsSet())) {
        ythrow yexception() << "some input values have not been set";
    }

    return OutputIterator_;
}

void TPullListWorker::ResetOutputIterator() {
    if (Y_UNLIKELY(!CheckAllInputsSet())) {
        ythrow yexception() << "some input values have not been set";
    }

    OutputIterator_ = Output_.GetListIterator();
}

void TPullListWorker::Release() {
    with_lock (GetScopedAlloc()) {
        Output_ = NKikimr::NUdf::TUnboxedValue::Invalid();
        OutputIterator_ = NKikimr::NUdf::TUnboxedValue::Invalid();

        for (auto selfNode : Graph_.SelfNodes) {
            if (selfNode) {
                selfNode->SetValue(Graph_.ComputationGraph->GetContext(), NKikimr::NUdf::TUnboxedValue::Invalid());
            }
        }
    }
    HasInput_.clear();
    TWorker<IPullListWorker>::Release();
}

void TPullListWorker::PrepareCheckState(bool finish) {
    if (finish) {
        Output_ = NKikimr::NUdf::TUnboxedValue::Invalid();
        OutputIterator_ = NKikimr::NUdf::TUnboxedValue::Invalid();
    }
}

namespace {
class TPushStream final: public NKikimr::NMiniKQL::TCustomListValue {
private:
    mutable bool HasIterator_ = false;
    bool HasValue_ = false;
    bool IsFinished_ = false;
    NKikimr::NUdf::TUnboxedValue Value_ = NKikimr::NUdf::TUnboxedValue::Invalid();

public:
    using TCustomListValue::TCustomListValue;

public:
    void SetValue(NKikimr::NUdf::TUnboxedValue&& value) {
        Value_ = std::move(value);
        HasValue_ = true;
    }

    void SetFinished() {
        IsFinished_ = true;
    }

    NKikimr::NUdf::TUnboxedValue GetListIterator() const override {
        YQL_ENSURE(!HasIterator_, "only one pass over input is supported");
        HasIterator_ = true;
        return NKikimr::NUdf::TUnboxedValuePod(const_cast<TPushStream*>(this));
    }

    NKikimr::NUdf::EFetchStatus Fetch(NKikimr::NUdf::TUnboxedValue& result) override {
        if (IsFinished_) {
            return NKikimr::NUdf::EFetchStatus::Finish;
        } else if (!HasValue_) {
            return NKikimr::NUdf::EFetchStatus::Yield;
        } else {
            result = std::move(Value_);
            HasValue_ = false;
            return NKikimr::NUdf::EFetchStatus::Ok;
        }
    }
};
} // namespace

void TPushStreamWorker::FeedToConsumer() {
    NKikimr::NMiniKQL::TBindTerminator bind(Graph_.ComputationGraph->GetTerminator());
    auto value = Graph_.ComputationGraph->GetValue();

    for (;;) {
        NKikimr::NUdf::TUnboxedValue item;
        auto status = value.Fetch(item);

        if (status != NKikimr::NUdf::EFetchStatus::Ok) {
            break;
        }

        Consumer_->OnObject(&item);
    }
}

NYql::NUdf::IBoxedValue* TPushStreamWorker::GetPushStream() const {
    auto& ctx = Graph_.ComputationGraph->GetContext();
    NUdf::TUnboxedValue pushStream = SelfNode_->GetValue(ctx);

    if (Y_UNLIKELY(pushStream.IsInvalid())) {
        SelfNode_->SetValue(ctx, Graph_.ComputationGraph->GetHolderFactory().Create<TPushStream>());
        pushStream = SelfNode_->GetValue(ctx);
    }

    return pushStream.AsBoxed().Get();
}

void TPushStreamWorker::SetConsumer(THolder<IConsumer<const NKikimr::NUdf::TUnboxedValue*>> consumer) {
    auto guard = Guard(GetScopedAlloc());
    const auto inputsCount = Graph_.SelfNodes.size();

    YQL_ENSURE(inputsCount < 2, "push stream mode doesn't support several inputs");
    YQL_ENSURE(!Consumer_, "consumer is already set");

    Consumer_ = std::move(consumer);

    if (inputsCount == 1) {
        SelfNode_ = Graph_.SelfNodes[0];
    }

    if (SelfNode_) {
        SelfNode_->SetValue(
            Graph_.ComputationGraph->GetContext(),
            Graph_.ComputationGraph->GetHolderFactory().Create<TPushStream>());
    }

    FeedToConsumer();
}

void TPushStreamWorker::Push(NKikimr::NUdf::TUnboxedValue&& value) {
    YQL_ENSURE(Consumer_, "consumer is not set");
    YQL_ENSURE(!Finished_, "OnFinish has already been sent to the consumer; no new values can be pushed");

    if (Y_LIKELY(SelfNode_)) {
        static_cast<TPushStream*>(GetPushStream())->SetValue(std::move(value));
    }

    FeedToConsumer();
}

void TPushStreamWorker::OnFinish() {
    YQL_ENSURE(Consumer_, "consumer is not set");
    YQL_ENSURE(!Finished_, "already finished");

    if (Y_LIKELY(SelfNode_)) {
        static_cast<TPushStream*>(GetPushStream())->SetFinished();
    }

    FeedToConsumer();

    Consumer_->OnFinish();

    Finished_ = true;
}

void TPushStreamWorker::Release() {
    with_lock (GetScopedAlloc()) {
        Consumer_.Destroy();
        if (SelfNode_) {
            SelfNode_->SetValue(Graph_.ComputationGraph->GetContext(), NKikimr::NUdf::TUnboxedValue::Invalid());
        }
        SelfNode_ = nullptr;
    }
    Finished_ = false;
    TWorker<IPushStreamWorker>::Release();
}

void TPushStreamWorker::PrepareCheckState(bool finish) {
    Y_UNUSED(finish);
}

namespace NYql::NPureCalc {
template class TWorker<IPullStreamWorker>;

template class TWorker<IPullListWorker>;

template class TWorker<IPushStreamWorker>;
} // namespace NYql::NPureCalc
