#include "program_factory.h"
#include "logger_init.h"
#include "names.h"
#include "worker_factory.h"

#include <yql/essentials/utils/log/log.h>

using namespace NYql;
using namespace NYql::NPureCalc;

TProgramFactory::TProgramFactory(const TProgramFactoryOptions& options)
    : Options_(options)
    , ExprOutputStream_(Options_.ExprOutputStream)
    , CountersProvider_(nullptr)
{
    EnsureLoggingInitialized();

    if (!TryFromString(Options_.BlockEngineSettings, BlockEngineMode_)) {
        ythrow TCompileError("", "") << "Unknown BlockEngineSettings value: expected "
                                     << GetEnumAllNames<EBlockEngineMode>()
                                     << ", but got: "
                                     << Options_.BlockEngineSettings;
    }

    NUserData::TUserData::UserDataToLibraries(Options_.UserData, Modules_);

    UserData_ = GetYqlModuleResolver(ExprContext_, ModuleResolver_, Options_.UserData, {}, {});

    if (!ModuleResolver_) {
        auto issues = ExprContext_.IssueManager.GetIssues();
        CheckFatalIssues(issues);
        ythrow TCompileError("", issues.ToString()) << "failed to compile modules";
    }

    TVector<TString> UDFsPaths;
    for (const auto& item: Options_.UserData) {
        if (
            item.Type == NUserData::EType::UDF &&
            item.Disposition == NUserData::EDisposition::FILESYSTEM
        ) {
            UDFsPaths.push_back(item.Content);
        }
    }

    if (!Options_.UdfsDir.empty()) {
        NKikimr::NMiniKQL::FindUdfsInDir(Options_.UdfsDir, &UDFsPaths);
    }

    FuncRegistry_ = NKikimr::NMiniKQL::CreateFunctionRegistry(
        &NYql::NBacktrace::KikimrBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, UDFsPaths)->Clone();

    NKikimr::NMiniKQL::FillStaticModules(*FuncRegistry_);
}

TProgramFactory::~TProgramFactory() {
}

void TProgramFactory::AddUdfModule(
    const TStringBuf& moduleName,
    NKikimr::NUdf::TUniquePtr<NKikimr::NUdf::IUdfModule>&& module
) {
    FuncRegistry_->AddModule(
        TString::Join(PurecalcUdfModulePrefix, moduleName), moduleName, std::move(module)
    );
}

void TProgramFactory::SetCountersProvider(NKikimr::NUdf::ICountersProvider* provider) {
    CountersProvider_ = provider;
}

IPullStreamWorkerFactoryPtr TProgramFactory::MakePullStreamWorkerFactory(
    const TInputSpecBase& inputSpec,
    const TOutputSpecBase& outputSpec,
    TString query,
    ETranslationMode mode,
    ui16 syntaxVersion
) {
    return std::make_shared<TPullStreamWorkerFactory>(TWorkerFactoryOptions(
        TIntrusivePtr<TProgramFactory>(this),
        inputSpec,
        outputSpec,
        query,
        FuncRegistry_,
        ModuleResolver_,
        UserData_,
        Modules_,
        Options_.LLVMSettings,
        BlockEngineMode_,
        ExprOutputStream_,
        CountersProvider_,
        mode,
        syntaxVersion,
        Options_.LangVer,
        Options_.NativeYtTypeFlags,
        Options_.DeterministicTimeProviderSeed,
        Options_.UseSystemColumns,
        Options_.UseWorkerPool,
        Options_.UseAntlr4
    ));
}

IPullListWorkerFactoryPtr TProgramFactory::MakePullListWorkerFactory(
    const TInputSpecBase& inputSpec,
    const TOutputSpecBase& outputSpec,
    TString query,
    ETranslationMode mode,
    ui16 syntaxVersion
) {
    return std::make_shared<TPullListWorkerFactory>(TWorkerFactoryOptions(
        TIntrusivePtr<TProgramFactory>(this),
        inputSpec,
        outputSpec,
        query,
        FuncRegistry_,
        ModuleResolver_,
        UserData_,
        Modules_,
        Options_.LLVMSettings,
        BlockEngineMode_,
        ExprOutputStream_,
        CountersProvider_,
        mode,
        syntaxVersion,
        Options_.LangVer,
        Options_.NativeYtTypeFlags,
        Options_.DeterministicTimeProviderSeed,
        Options_.UseSystemColumns,
        Options_.UseWorkerPool,
        Options_.UseAntlr4
    ));
}

IPushStreamWorkerFactoryPtr TProgramFactory::MakePushStreamWorkerFactory(
    const TInputSpecBase& inputSpec,
    const TOutputSpecBase& outputSpec,
    TString query,
    ETranslationMode mode,
    ui16 syntaxVersion
) {
    if (inputSpec.GetSchemas().size() > 1) {
        ythrow yexception() << "push stream mode doesn't support several inputs";
    }

    return std::make_shared<TPushStreamWorkerFactory>(TWorkerFactoryOptions(
        TIntrusivePtr<TProgramFactory>(this),
        inputSpec,
        outputSpec,
        query,
        FuncRegistry_,
        ModuleResolver_,
        UserData_,
        Modules_,
        Options_.LLVMSettings,
        BlockEngineMode_,
        ExprOutputStream_,
        CountersProvider_,
        mode,
        syntaxVersion,
        Options_.LangVer,
        Options_.NativeYtTypeFlags,
        Options_.DeterministicTimeProviderSeed,
        Options_.UseSystemColumns,
        Options_.UseWorkerPool,
        Options_.UseAntlr4
    ));
}
