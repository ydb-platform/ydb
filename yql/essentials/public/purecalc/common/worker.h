#pragma once

#include <yql/essentials/public/purecalc/common/interface.h>

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_user_data.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>

#include <memory>

namespace NYql::NPureCalc {
struct TWorkerGraph {
    TWorkerGraph(
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
        bool insideEvaluation);

    ~TWorkerGraph();

    NKikimr::NMiniKQL::TScopedAlloc ScopedAlloc;
    NKikimr::NMiniKQL::TTypeEnvironment Env;
    const NKikimr::NMiniKQL::IFunctionRegistry& FuncRegistry;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    NKikimr::NMiniKQL::IComputationPattern::TPtr ComputationPattern;
    THolder<NKikimr::NMiniKQL::IComputationGraph> ComputationGraph;
    TString LLVMSettings;
    ui64 NativeYtTypeFlags;
    TMaybe<TString> TimestampColumn;
    const NKikimr::NMiniKQL::TType* OutputType;
    const NKikimr::NMiniKQL::TType* RawOutputType;
    TVector<NKikimr::NMiniKQL::IComputationExternalNode*> SelfNodes;
    TVector<const NKikimr::NMiniKQL::TStructType*> InputTypes;
    TVector<const NKikimr::NMiniKQL::TStructType*> OriginalInputTypes;
    TVector<const NKikimr::NMiniKQL::TStructType*> RawInputTypes;
};

template <typename TBase>
class TWorker: public TBase {
public:
    using TWorkerFactoryPtr = std::weak_ptr<IWorkerFactory>;

private:
    // Worker factory implementation should stay alive for this worker to operate correctly.
    TWorkerFactoryPtr WorkerFactory_;

protected:
    TWorkerGraph Graph_;

public:
    TWorker(
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
        TLangVersion langver);

public:
    ui32 GetInputsCount() const override;
    const NKikimr::NMiniKQL::TStructType* GetInputType(ui32, bool) const override;
    const NKikimr::NMiniKQL::TStructType* GetInputType(bool) const override;
    const NKikimr::NMiniKQL::TStructType* GetRawInputType(ui32) const override;
    const NKikimr::NMiniKQL::TStructType* GetRawInputType() const override;
    const NKikimr::NMiniKQL::TType* GetOutputType() const override;
    const NKikimr::NMiniKQL::TType* GetRawOutputType() const override;
    NYT::TNode MakeInputSchema() const override;
    NYT::TNode MakeInputSchema(ui32) const override;
    NYT::TNode MakeOutputSchema() const override;
    NYT::TNode MakeOutputSchema(ui32) const override;
    NYT::TNode MakeOutputSchema(TStringBuf) const override;
    NYT::TNode MakeFullOutputSchema() const override;
    NKikimr::NMiniKQL::TScopedAlloc& GetScopedAlloc() override;
    NKikimr::NMiniKQL::IComputationGraph& GetGraph() override;
    const NKikimr::NMiniKQL::IFunctionRegistry& GetFunctionRegistry() const override;
    NKikimr::NMiniKQL::TTypeEnvironment& GetTypeEnvironment() override;
    const TString& GetLLVMSettings() const override;
    ui64 GetNativeYtTypeFlags() const override;
    ITimeProvider* GetTimeProvider() const override;
    void Invalidate() override;
    void CheckState(bool finish) override;
    virtual void PrepareCheckState(bool finish) = 0;

protected:
    void Release() override;
};

class TPullStreamWorker final: public TWorker<IPullStreamWorker> {
private:
    NKikimr::NUdf::TUnboxedValue Output_ = NKikimr::NUdf::TUnboxedValue::Invalid();
    TVector<bool> HasInput_;

    inline bool CheckAllInputsSet() {
        return AllOf(HasInput_, [](bool x) { return x; });
    }

public:
    using TWorker::TWorker;
    ~TPullStreamWorker() override;

public:
    void SetInput(NKikimr::NUdf::TUnboxedValue&&, ui32) override;
    NKikimr::NUdf::TUnboxedValue& GetOutput() override;
    void PrepareCheckState(bool finish) final;

protected:
    void Release() override;
};

class TPullListWorker final: public TWorker<IPullListWorker> {
private:
    NKikimr::NUdf::TUnboxedValue Output_ = NKikimr::NUdf::TUnboxedValue::Invalid();
    NKikimr::NUdf::TUnboxedValue OutputIterator_ = NKikimr::NUdf::TUnboxedValue::Invalid();
    TVector<bool> HasInput_;

    inline bool CheckAllInputsSet() {
        return AllOf(HasInput_, [](bool x) { return x; });
    }

public:
    using TWorker::TWorker;
    ~TPullListWorker() override;

public:
    void SetInput(NKikimr::NUdf::TUnboxedValue&&, ui32) override;
    NKikimr::NUdf::TUnboxedValue& GetOutput() override;
    NKikimr::NUdf::TUnboxedValue& GetOutputIterator() override;
    void ResetOutputIterator() override;
    void PrepareCheckState(bool finish) final;

protected:
    void Release() override;
};

class TPushStreamWorker final: public TWorker<IPushStreamWorker> {
private:
    THolder<IConsumer<const NKikimr::NUdf::TUnboxedValue*>> Consumer_{};
    bool Finished_ = false;
    NKikimr::NMiniKQL::IComputationExternalNode* SelfNode_ = nullptr;

public:
    using TWorker::TWorker;

private:
    void FeedToConsumer();
    NYql::NUdf::IBoxedValue* GetPushStream() const;
    void PrepareCheckState(bool finish) final;

public:
    void SetConsumer(THolder<IConsumer<const NKikimr::NUdf::TUnboxedValue*>>) override;
    void Push(NKikimr::NUdf::TUnboxedValue&&) override;
    void OnFinish() override;

protected:
    void Release() override;
};
} // namespace NYql::NPureCalc
