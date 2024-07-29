#include "worker.h"
#include "compile_mkql.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_user_data.h>
#include <ydb/library/yql/core/yql_user_data_storage.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/public/purecalc/common/names.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/stream/file.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>

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
    TMaybe<ui64> deterministicTimeProviderSeed
)
    : ScopedAlloc_(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), funcRegistry.SupportsSizedAllocators())
    , Env_(ScopedAlloc_)
    , FuncRegistry_(funcRegistry)
    , RandomProvider_(CreateDefaultRandomProvider())
    , TimeProvider_(deterministicTimeProviderSeed ?
        CreateDeterministicTimeProvider(*deterministicTimeProviderSeed) :
        CreateDefaultTimeProvider())
    , LLVMSettings_(LLVMSettings)
    , NativeYtTypeFlags_(nativeYtTypeFlags)
{
    // Build the root MKQL node

    NKikimr::NMiniKQL::TRuntimeNode rootNode;
    if (exprRoot) {
        rootNode = CompileMkql(exprRoot, exprCtx, FuncRegistry_, Env_, userData);
    } else {
        rootNode = NKikimr::NMiniKQL::DeserializeRuntimeNode(serializedProgram, Env_);
    }

    // Prepare container for input nodes

    const ui32 inputsCount = inputTypes.size();

    YQL_ENSURE(inputTypes.size() == originalInputTypes.size());

    SelfNodes_.resize(inputsCount, nullptr);

    YQL_ENSURE(SelfNodes_.size() == inputsCount);

    // Setup struct types

    NKikimr::NMiniKQL::TProgramBuilder pgmBuilder(Env_, FuncRegistry_);
    for (ui32 i = 0; i < inputsCount; ++i) {
        const auto* type = static_cast<NKikimr::NMiniKQL::TStructType*>(NCommon::BuildType(TPositionHandle(), *inputTypes[i], pgmBuilder));
        const auto* originalType = type;
        const auto* rawType = static_cast<NKikimr::NMiniKQL::TStructType*>(NCommon::BuildType(TPositionHandle(), *rawInputTypes[i], pgmBuilder));
        if (inputTypes[i] != originalInputTypes[i]) {
            YQL_ENSURE(inputTypes[i]->GetSize() >= originalInputTypes[i]->GetSize());
            originalType = static_cast<NKikimr::NMiniKQL::TStructType*>(NCommon::BuildType(TPositionHandle(), *originalInputTypes[i], pgmBuilder));
        }

        InputTypes_.push_back(type);
        OriginalInputTypes_.push_back(originalType);
        RawInputTypes_.push_back(rawType);
    }

    if (outputType) {
        OutputType_ = NCommon::BuildType(TPositionHandle(), *outputType, pgmBuilder);
    }
    if (rawOutputType) {
        RawOutputType_ = NCommon::BuildType(TPositionHandle(), *rawOutputType, pgmBuilder);
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
        if (OutputType_) {
            if (!OutputType_->IsSameType(*outMkqlType)) {
                ythrow TCompileError("", "") << "precompiled program output type doesn't match the output schema";
            }
        } else {
            OutputType_ = outMkqlType;
            RawOutputType_ = outMkqlType;
        }
    }

    // Compile computation pattern

    const THashSet<NKikimr::NMiniKQL::TInternName> selfCallableNames = {
        Env_.InternName(PurecalcInputCallableName),
        Env_.InternName(PurecalcBlockInputCallableName)
    };

    NKikimr::NMiniKQL::TExploringNodeVisitor explorer;
    explorer.Walk(rootNode.GetNode(), Env_);

    auto compositeNodeFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory(
        {NKikimr::NMiniKQL::GetYqlFactory(), NYql::GetPgFactory()}
    );

    auto nodeFactory = [&](
        NKikimr::NMiniKQL::TCallable& callable, const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx
        ) -> NKikimr::NMiniKQL::IComputationNode* {
        if (selfCallableNames.contains(callable.GetType()->GetNameStr())) {
            YQL_ENSURE(callable.GetInputsCount() == 1, "Self takes exactly 1 argument");
            const auto inputIndex = AS_VALUE(NKikimr::NMiniKQL::TDataLiteral, callable.GetInput(0))->AsValue().Get<ui32>();
            YQL_ENSURE(inputIndex < inputsCount, "Self index is out of range");
            YQL_ENSURE(!SelfNodes_[inputIndex], "Self can be called at most once with each index");
            return SelfNodes_[inputIndex] = new NKikimr::NMiniKQL::TExternalComputationNode(ctx.Mutables);
        }
        else {
            return compositeNodeFactory(callable, ctx);
        }
    };

    NKikimr::NMiniKQL::TComputationPatternOpts computationPatternOpts(
        ScopedAlloc_.Ref(),
        Env_,
        nodeFactory,
        &funcRegistry,
        NKikimr::NUdf::EValidateMode::None,
        NKikimr::NUdf::EValidatePolicy::Exception,
        LLVMSettings,
        NKikimr::NMiniKQL::EGraphPerProcess::Multi,
        nullptr,
        countersProvider);

    ComputationPattern_ = NKikimr::NMiniKQL::MakeComputationPattern(
        explorer,
        rootNode,
        { rootNode.GetNode() },
        computationPatternOpts);

    ComputationGraph_ = ComputationPattern_->Clone(
        computationPatternOpts.ToComputationOptions(*RandomProvider_, *TimeProvider_));

    ComputationGraph_->Prepare();

    // Scoped alloc acquires itself on construction. We need to release it before returning control to user.
    // Note that scoped alloc releases itself on destruction so it is no problem if the above code throws.
    ScopedAlloc_.Release();
}

TWorkerGraph::~TWorkerGraph() {
    // Remember, we've released scoped alloc in constructor? Now, we need to acquire it back before destroying.
    ScopedAlloc_.Acquire();
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
    TMaybe<ui64> deterministicTimeProviderSeed
)
    : WorkerFactory_(std::move(factory))
    , Graph_(exprRoot, exprCtx, serializedProgram, funcRegistry, userData,
         inputTypes, originalInputTypes, rawInputTypes, outputType, rawOutputType,
         LLVMSettings, countersProvider, nativeYtTypeFlags, deterministicTimeProviderSeed)
{
}

template <typename TBase>
inline ui32 TWorker<TBase>::GetInputsCount() const {
    return Graph_.InputTypes_.size();
}

template <typename TBase>
inline const NKikimr::NMiniKQL::TStructType* TWorker<TBase>::GetInputType(ui32 inputIndex, bool original) const {
    const auto& container = original ? Graph_.OriginalInputTypes_ : Graph_.InputTypes_;

    YQL_ENSURE(inputIndex < container.size(), "invalid input index (" << inputIndex << ") in GetInputType call");

    return container[inputIndex];
}

template <typename TBase>
inline const NKikimr::NMiniKQL::TStructType* TWorker<TBase>::GetInputType(bool original) const {
    const auto& container = original ? Graph_.OriginalInputTypes_ : Graph_.InputTypes_;

    YQL_ENSURE(container.size() == 1, "GetInputType() can be used only for single-input programs");

    return container[0];
}

template <typename TBase>
inline const NKikimr::NMiniKQL::TStructType* TWorker<TBase>::GetRawInputType(ui32 inputIndex) const {
    const auto& container = Graph_.RawInputTypes_;
    YQL_ENSURE(inputIndex < container.size(), "invalid input index (" << inputIndex << ") in GetInputType call");
    return container[inputIndex];
}

template <typename TBase>
inline const NKikimr::NMiniKQL::TStructType* TWorker<TBase>::GetRawInputType() const {
    const auto& container = Graph_.RawInputTypes_;
    YQL_ENSURE(container.size() == 1, "GetInputType() can be used only for single-input programs");
    return container[0];
}

template <typename TBase>
inline const NKikimr::NMiniKQL::TType* TWorker<TBase>::GetOutputType() const {
    return Graph_.OutputType_;
}

template <typename TBase>
inline const NKikimr::NMiniKQL::TType* TWorker<TBase>::GetRawOutputType() const {
    return Graph_.RawOutputType_;
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
    return Graph_.ScopedAlloc_;
}

template <typename TBase>
inline NKikimr::NMiniKQL::IComputationGraph& TWorker<TBase>::GetGraph() {
    return *Graph_.ComputationGraph_;
}

template <typename TBase>
inline const NKikimr::NMiniKQL::IFunctionRegistry&
TWorker<TBase>::GetFunctionRegistry() const {
    return Graph_.FuncRegistry_;
}

template <typename TBase>
inline NKikimr::NMiniKQL::TTypeEnvironment&
TWorker<TBase>::GetTypeEnvironment() {
    return Graph_.Env_;
}

template <typename TBase>
inline const TString& TWorker<TBase>::GetLLVMSettings() const {
    return Graph_.LLVMSettings_;
}

template <typename TBase>
inline ui64 TWorker<TBase>::GetNativeYtTypeFlags() const {
    return Graph_.NativeYtTypeFlags_;
}

template <typename TBase>
ITimeProvider* TWorker<TBase>::GetTimeProvider() const {
    return Graph_.TimeProvider_.Get();
}

template <typename TBase>
void TWorker<TBase>::Release() {
    if (auto p = WorkerFactory_.lock()) {
        p->ReturnWorker(this);
    } else {
        delete this;
    }
}

TPullStreamWorker::~TPullStreamWorker() {
    auto guard = Guard(GetScopedAlloc());
    Output_.Clear();
}

void TPullStreamWorker::SetInput(NKikimr::NUdf::TUnboxedValue&& value, ui32 inputIndex) {
    const auto inputsCount = Graph_.SelfNodes_.size();

    if (Y_UNLIKELY(inputIndex >= inputsCount)) {
        ythrow yexception() << "invalid input index (" << inputIndex << ") in SetInput call";
    }

    if (HasInput_.size() < inputsCount) {
        HasInput_.resize(inputsCount, false);
    }

    if (Y_UNLIKELY(HasInput_[inputIndex])) {
        ythrow yexception() << "input value for #" << inputIndex << " input is already set";
    }

    auto selfNode = Graph_.SelfNodes_[inputIndex];

    if (selfNode) {
        YQL_ENSURE(value);
        selfNode->SetValue(Graph_.ComputationGraph_->GetContext(), std::move(value));
    }

    HasInput_[inputIndex] = true;

    if (CheckAllInputsSet()) {
        Output_ = Graph_.ComputationGraph_->GetValue();
    }
}

NKikimr::NUdf::TUnboxedValue& TPullStreamWorker::GetOutput() {
    if (Y_UNLIKELY(!CheckAllInputsSet())) {
        ythrow yexception() << "some input values have not been set";
    }

    return Output_;
}

void TPullStreamWorker::Release() {
    with_lock(GetScopedAlloc()) {
        Output_ = NKikimr::NUdf::TUnboxedValue::Invalid();
        for (auto selfNode: Graph_.SelfNodes_) {
            if (selfNode) {
                selfNode->SetValue(Graph_.ComputationGraph_->GetContext(), NKikimr::NUdf::TUnboxedValue::Invalid());
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
    const auto inputsCount = Graph_.SelfNodes_.size();

    if (Y_UNLIKELY(inputIndex >= inputsCount)) {
        ythrow yexception() << "invalid input index (" << inputIndex << ") in SetInput call";
    }

    if (HasInput_.size() < inputsCount) {
        HasInput_.resize(inputsCount, false);
    }

    if (Y_UNLIKELY(HasInput_[inputIndex])) {
        ythrow yexception() << "input value for #" << inputIndex << " input is already set";
    }

    auto selfNode = Graph_.SelfNodes_[inputIndex];

    if (selfNode) {
        YQL_ENSURE(value);
        selfNode->SetValue(Graph_.ComputationGraph_->GetContext(), std::move(value));
    }

    HasInput_[inputIndex] = true;

    if (CheckAllInputsSet()) {
        Output_ = Graph_.ComputationGraph_->GetValue();
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
    with_lock(GetScopedAlloc()) {
        Output_ = NKikimr::NUdf::TUnboxedValue::Invalid();
        OutputIterator_ = NKikimr::NUdf::TUnboxedValue::Invalid();

        for (auto selfNode: Graph_.SelfNodes_) {
            if (selfNode) {
                selfNode->SetValue(Graph_.ComputationGraph_->GetContext(), NKikimr::NUdf::TUnboxedValue::Invalid());
            }
        }
    }
    HasInput_.clear();
    TWorker<IPullListWorker>::Release();
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
}

void TPushStreamWorker::FeedToConsumer() {
    auto value = Graph_.ComputationGraph_->GetValue();

    for (;;) {
        NKikimr::NUdf::TUnboxedValue item;
        auto status = value.Fetch(item);

        if (status != NKikimr::NUdf::EFetchStatus::Ok) {
            break;
        }

        Consumer_->OnObject(&item);
    }
}

void TPushStreamWorker::SetConsumer(THolder<IConsumer<const NKikimr::NUdf::TUnboxedValue*>> consumer) {
    auto guard = Guard(GetScopedAlloc());
    const auto inputsCount = Graph_.SelfNodes_.size();

    YQL_ENSURE(inputsCount < 2, "push stream mode doesn't support several inputs");
    YQL_ENSURE(!Consumer_, "consumer is already set");

    Consumer_ = std::move(consumer);

    if (inputsCount == 1) {
        SelfNode_ = Graph_.SelfNodes_[0];
    }

    if (SelfNode_) {
        SelfNode_->SetValue(
            Graph_.ComputationGraph_->GetContext(),
            Graph_.ComputationGraph_->GetHolderFactory().Create<TPushStream>());
    }

    FeedToConsumer();
}

void TPushStreamWorker::Push(NKikimr::NUdf::TUnboxedValue&& value) {
    YQL_ENSURE(Consumer_, "consumer is not set");
    YQL_ENSURE(!Finished_, "OnFinish has already been sent to the consumer; no new values can be pushed");

    if (Y_LIKELY(SelfNode_)) {
        static_cast<TPushStream*>(SelfNode_->GetValue(Graph_.ComputationGraph_->GetContext()).AsBoxed().Get())->SetValue(std::move(value));
    }

    FeedToConsumer();
}

void TPushStreamWorker::OnFinish() {
    YQL_ENSURE(Consumer_, "consumer is not set");
    YQL_ENSURE(!Finished_, "already finished");

    if (Y_LIKELY(SelfNode_)) {
        static_cast<TPushStream*>(SelfNode_->GetValue(Graph_.ComputationGraph_->GetContext()).AsBoxed().Get())->SetFinished();
    }

    FeedToConsumer();

    Consumer_->OnFinish();

    Finished_ = true;
}

void TPushStreamWorker::Release() {
    with_lock(GetScopedAlloc()) {
        Consumer_.Destroy();
        if (SelfNode_) {
            SelfNode_->SetValue(Graph_.ComputationGraph_->GetContext(), NKikimr::NUdf::TUnboxedValue::Invalid());
        }
        SelfNode_ = nullptr;
    }
    Finished_ = false;
    TWorker<IPushStreamWorker>::Release();
}


namespace NYql {
    namespace NPureCalc {
        template
        class TWorker<IPullStreamWorker>;

        template
        class TWorker<IPullListWorker>;

        template
        class TWorker<IPushStreamWorker>;
    }
}
