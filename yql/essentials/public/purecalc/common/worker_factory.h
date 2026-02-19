#pragma once

#include <yql/essentials/public/purecalc/common/interface.h>

#include "processor_mode.h"

#include <util/generic/ptr.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_user_data.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <utility>

namespace NYql::NPureCalc {
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
    NKikimr::NUdf::ICountersProvider* CountersProvider;
    ETranslationMode TranslationMode;
    ui16 SyntaxVersion;
    TLangVersion LangVer;
    ui64 NativeYtTypeFlags;
    TMaybe<ui64> DeterministicTimeProviderSeed;
    bool UseSystemColumns;
    bool UseWorkerPool;
    TInternalProgramSettings InternalSettings;
    TString IssueReportTarget;

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
        TLangVersion langver,
        ui64 nativeYtTypeFlags,
        TMaybe<ui64> deterministicTimeProviderSeed,
        bool useSystemColumns,
        bool useWorkerPool,
        const TInternalProgramSettings& internalSettings,
        const TString& issueReportTarget)
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
        , CountersProvider(CountersProvider)
        , TranslationMode(translationMode)
        , SyntaxVersion(syntaxVersion)
        , LangVer(langver)
        , NativeYtTypeFlags(nativeYtTypeFlags)
        , DeterministicTimeProviderSeed(deterministicTimeProviderSeed)
        , UseSystemColumns(useSystemColumns)
        , UseWorkerPool(useWorkerPool)
        , InternalSettings(internalSettings)
        , IssueReportTarget(issueReportTarget)
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
    TVector<const TStructExprType*> RawInputTypes_;
    const TTypeAnnotationNode* OutputType_;
    const TTypeAnnotationNode* RawOutputType_;
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
    TLangVersion LangVer_;
    TVector<THolder<IWorker>> WorkerPool_;
    const TString IssueReportTarget_;

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
    void HandleInternalSettings(const TInternalProgramSettings& settings);
    TIntrusivePtr<TTypeAnnotationContext> PrepareTypeContext(
        IModuleResolver::TPtr factoryModuleResolver);

    TExprNode::TPtr Compile(TStringBuf query,
                            ETranslationMode mode,
                            ui16 syntaxVersion,
                            const THashMap<TString, TString>& modules,
                            const TInputSpecBase& inputSpec,
                            const TOutputSpecBase& outputSpec,
                            EProcessorMode processorMode,
                            TTypeAnnotationContext* typeContext);
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
} // namespace NYql::NPureCalc
