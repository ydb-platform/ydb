#pragma once

#include <ydb/library/yql/public/purecalc/common/interface.h>

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_user_data.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>

#include <memory>

namespace NYql {
    namespace NPureCalc {
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
                TMaybe<ui64> deterministicTimeProviderSeed
            );

            ~TWorkerGraph();

            NKikimr::NMiniKQL::TScopedAlloc ScopedAlloc_;
            NKikimr::NMiniKQL::TTypeEnvironment Env_;
            const NKikimr::NMiniKQL::IFunctionRegistry& FuncRegistry_;
            TIntrusivePtr<IRandomProvider> RandomProvider_;
            TIntrusivePtr<ITimeProvider> TimeProvider_;
            NKikimr::NMiniKQL::IComputationPattern::TPtr ComputationPattern_;
            THolder<NKikimr::NMiniKQL::IComputationGraph> ComputationGraph_;
            TString LLVMSettings_;
            ui64 NativeYtTypeFlags_;
            TMaybe<TString> TimestampColumn_;
            const NKikimr::NMiniKQL::TType* OutputType_;
            const NKikimr::NMiniKQL::TType* RawOutputType_;
            TVector<NKikimr::NMiniKQL::IComputationExternalNode*> SelfNodes_;
            TVector<const NKikimr::NMiniKQL::TStructType*> InputTypes_;
            TVector<const NKikimr::NMiniKQL::TStructType*> OriginalInputTypes_;
            TVector<const NKikimr::NMiniKQL::TStructType*> RawInputTypes_;
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
                TMaybe<ui64> deterministicTimeProviderSeed
            );

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
            ~TPullStreamWorker();

        public:
            void SetInput(NKikimr::NUdf::TUnboxedValue&&, ui32) override;
            NKikimr::NUdf::TUnboxedValue& GetOutput() override;

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
            ~TPullListWorker();

        public:
            void SetInput(NKikimr::NUdf::TUnboxedValue&&, ui32) override;
            NKikimr::NUdf::TUnboxedValue& GetOutput() override;
            NKikimr::NUdf::TUnboxedValue& GetOutputIterator() override;
            void ResetOutputIterator() override;

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

        public:
            void SetConsumer(THolder<IConsumer<const NKikimr::NUdf::TUnboxedValue*>>) override;
            void Push(NKikimr::NUdf::TUnboxedValue&&) override;
            void OnFinish() override;

        protected:
            void Release() override;
        };
    }
}
