#pragma once

#include <ydb/library/yql/public/purecalc/common/interface.h>

#include "processor_mode.h"

#include <util/generic/ptr.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_user_data.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <utility>

namespace NYql {
    namespace NPureCalc {
        struct TWorkerFactoryOptions {
            IProgramFactoryPtr Factory;
            const TInputSpecBase& InputSpec;
            const TOutputSpecBase& OutputSpec;
            TStringBuf Query;
            TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FuncRegistry;
            IModuleResolver::TPtr ModuleResolver;
            const TUserDataTable& UserData;
            const THashMap<TString, TString>& Modules;
            TString LLVMSettings;
            EBlockEngineMode BlockEngineMode;
            IOutputStream* ExprOutputStream;
            NKikimr::NUdf::ICountersProvider* CountersProvider_;
            ETranslationMode TranslationMode_;
            ui16 SyntaxVersion_;
            ui64 NativeYtTypeFlags_;
            TMaybe<ui64> DeterministicTimeProviderSeed_;
            bool UseSystemColumns;
            bool UseWorkerPool;

            TWorkerFactoryOptions(
                IProgramFactoryPtr Factory,
                const TInputSpecBase& InputSpec,
                const TOutputSpecBase& OutputSpec,
                TStringBuf Query,
                TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FuncRegistry,
                IModuleResolver::TPtr ModuleResolver,
                const TUserDataTable& UserData,
                const THashMap<TString, TString>& Modules,
                TString LLVMSettings,
                EBlockEngineMode BlockEngineMode,
                IOutputStream* ExprOutputStream,
                NKikimr::NUdf::ICountersProvider* CountersProvider,
                ETranslationMode translationMode,
                ui16 syntaxVersion,
                ui64 nativeYtTypeFlags,
                TMaybe<ui64> deterministicTimeProviderSeed,
                bool useSystemColumns,
                bool useWorkerPool
            )
                : Factory(std::move(Factory))
                , InputSpec(InputSpec)
                , OutputSpec(OutputSpec)
                , Query(Query)
                , FuncRegistry(std::move(FuncRegistry))
                , ModuleResolver(std::move(ModuleResolver))
                , UserData(UserData)
                , Modules(Modules)
                , LLVMSettings(std::move(LLVMSettings))
                , BlockEngineMode(BlockEngineMode)
                , ExprOutputStream(ExprOutputStream)
                , CountersProvider_(CountersProvider)
                , TranslationMode_(translationMode)
                , SyntaxVersion_(syntaxVersion)
                , NativeYtTypeFlags_(nativeYtTypeFlags)
                , DeterministicTimeProviderSeed_(deterministicTimeProviderSeed)
                , UseSystemColumns(useSystemColumns)
                , UseWorkerPool(useWorkerPool)
            {
            }
        };

        template <typename TBase>
        class TWorkerFactory: public TBase {
        private:
            IProgramFactoryPtr Factory_;

        protected:
            TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> FuncRegistry_;
            const TUserDataTable& UserData_;
            TExprContext ExprContext_;
            TExprNode::TPtr ExprRoot_;
            TString SerializedProgram_;
            TVector<const TStructExprType*> InputTypes_;
            TVector<const TStructExprType*> OriginalInputTypes_;
            const TTypeAnnotationNode* OutputType_;
            TVector<THashSet<TString>> AllColumns_;
            TVector<THashSet<TString>> UsedColumns_;
            TString LLVMSettings_;
            EBlockEngineMode BlockEngineMode_;
            IOutputStream* ExprOutputStream_;
            NKikimr::NUdf::ICountersProvider* CountersProvider_;
            ui64 NativeYtTypeFlags_;
            TMaybe<ui64> DeterministicTimeProviderSeed_;
            bool UseSystemColumns_;
            bool UseWorkerPool_;
            TVector<THolder<IWorker>> WorkerPool_;

        public:
            TWorkerFactory(TWorkerFactoryOptions, EProcessorMode);

        public:
            NYT::TNode MakeInputSchema(ui32) const override;
            NYT::TNode MakeInputSchema() const override;
            NYT::TNode MakeOutputSchema() const override;
            NYT::TNode MakeOutputSchema(ui32) const override;
            NYT::TNode MakeOutputSchema(TStringBuf) const override;
            NYT::TNode MakeFullOutputSchema() const override;
            const THashSet<TString>& GetUsedColumns(ui32 inputIndex) const override;
            const THashSet<TString>& GetUsedColumns() const override;
            TIssues GetIssues() const override;
            TString GetCompiledProgram() override;

        protected:
            void ReturnWorker(IWorker* worker) override;

        private:
            TExprNode::TPtr Compile(TStringBuf query,
                ETranslationMode mode,
                IModuleResolver::TPtr moduleResolver,
                ui16 syntaxVersion,
                const THashMap<TString, TString>& modules,
                const TOutputSpecBase& outputSpec,
                EProcessorMode processorMode);
        };

        class TPullStreamWorkerFactory final: public TWorkerFactory<IPullStreamWorkerFactory> {
        public:
            explicit TPullStreamWorkerFactory(TWorkerFactoryOptions options)
                : TWorkerFactory(std::move(options), EProcessorMode::PullStream)
            {
            }

        public:
            TWorkerHolder<IPullStreamWorker> MakeWorker() override;
        };

        class TPullListWorkerFactory final: public TWorkerFactory<IPullListWorkerFactory> {
        public:
            explicit TPullListWorkerFactory(TWorkerFactoryOptions options)
                : TWorkerFactory(std::move(options), EProcessorMode::PullList)
            {
            }

        public:
            TWorkerHolder<IPullListWorker> MakeWorker() override;
        };

        class TPushStreamWorkerFactory final: public TWorkerFactory<IPushStreamWorkerFactory> {
        public:
            explicit TPushStreamWorkerFactory(TWorkerFactoryOptions options)
                : TWorkerFactory(std::move(options), EProcessorMode::PushStream)
            {
            }

        public:
            TWorkerHolder<IPushStreamWorker> MakeWorker() override;
        };
    }
}
