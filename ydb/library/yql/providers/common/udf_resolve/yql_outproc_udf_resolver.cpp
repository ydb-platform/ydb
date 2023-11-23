#include "yql_outproc_udf_resolver.h"
#include "yql_simple_udf_resolver.h"
#include "yql_files_box.h"

#include <ydb/library/yql/providers/common/proto/udf_resolver.pb.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/core/yql_holding_file_storage.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/retry.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_utils.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/scope.h>
#include <util/stream/str.h>
#include <util/string/strip.h>
#include <util/system/shellcommand.h>
#include <util/string/split.h>

#include <regex>

namespace NYql {
namespace NCommon {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

namespace {
template <typename F>
void RunResolver(
    const TString& resolverPath,
    const TList<TString>& args,
    IInputStream* input,
    const F& outputHandler,
    const TString& ldLibraryPath = {}) {

    TShellCommandOptions shellOptions;
    shellOptions
        .SetUseShell(false)
        .SetDetachSession(false)
        .SetInputStream(input); // input can be nullptr

    if (ldLibraryPath) {
        YQL_LOG(DEBUG) << "Using LD_LIBRARY_PATH = " << ldLibraryPath << " for Udf resolver";
        shellOptions.Environment["LD_LIBRARY_PATH"] = ldLibraryPath;
    }

    TShellCommand shell(resolverPath, args, shellOptions);

    switch (shell.Run().GetStatus()) {
    case TShellCommand::SHELL_INTERNAL_ERROR:
        ythrow yexception() << "Udf resolver internal error: "
            << shell.GetInternalError();
    case TShellCommand::SHELL_ERROR:
        ythrow yexception() << "Udf resolver shell error: "
            << StripString(shell.GetError());
    case TShellCommand::SHELL_FINISHED:
        break;
    default:
        ythrow yexception() << "Unexpected udf resolver state: "
            << int(shell.GetStatus());
    }

    if (shell.GetError()) {
        YQL_LOG(INFO) << "UdfResolver stderr: " << shell.GetError();
    }

    outputHandler(shell.GetOutput());
}

template <typename F>
void RunResolver(
    const TString& resolverPath,
    const TList<TString>& args,
    const TResolve& request,
    const F& outputHandler,
    const TString& ldLibraryPath = {}) {

    TStringStream input;
    YQL_ENSURE(request.SerializeToArcadiaStream(&input), "Cannot serialize TResolve proto message");
    RunResolver(resolverPath, args, &input, outputHandler, ldLibraryPath);
}

TString ExtractSharedObjectNameFromErrorMessage(const char* message) {
    if (!message) {
        return "";
    }

    // example:
    // util/system/dynlib.cpp:56: libcuda.so.1: cannot open shared object file: No such file or directory
    static std::regex re(".*: (.+): cannot open shared object file: No such file or directory");
    std::cmatch match;
    if (!std::regex_match(message, match, re)) {
        return "";
    }

    return TString(match[1].str());
}
}

class TOutProcUdfResolver : public IUdfResolver {
public:
    TOutProcUdfResolver(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TFileStoragePtr& fileStorage, const TString& resolverPath,
        const TString& user, const TString& group, bool filterSyscalls,
        const TString& udfDependencyStubPath, const TMap<TString, TString>& path2md5)
        : FunctionRegistry_(functionRegistry)
        , TypeInfoHelper_(new TTypeInfoHelper)
        , FileStorage_(fileStorage)
        , ResolverPath_(resolverPath)
        , UdfDependencyStubPath_(udfDependencyStubPath)
        , Path2Md5_(path2md5)
    {
        if (user) {
            UserGroupArgs_ = { "-U", user, "-G", group };
        }

        if (filterSyscalls) {
            UserGroupArgs_.push_back("-F");
        }
    }

    TMaybe<TFilePathWithMd5> GetSystemModulePath(const TStringBuf& moduleName) const override {
        auto path = FunctionRegistry_->FindUdfPath(moduleName);
        if (!path) {
            return Nothing();
        }

        const TString md5 = Path2Md5_.Value(*path, "");
        return MakeMaybe<TFilePathWithMd5>(*path, md5);
    }

    bool ContainsModule(const TStringBuf& moduleName) const override {
        return FunctionRegistry_->IsLoadedUdfModule(moduleName);
    }

    bool LoadMetadata(const TVector<TImport*>& imports, const TVector<TFunction*>& functions, TExprContext& ctx) const override {
        THashSet<TString> requiredLoadedModules;
        THashSet<TString> requiredExternalModules;
        TVector<TFunction*> loadedFunctions;
        TVector<TFunction*> externalFunctions;

        bool hasErrors = false;
        for (auto udf : functions) {
            TStringBuf moduleName, funcName;
            if (!SplitUdfName(udf->Name, moduleName, funcName) || moduleName.empty() || funcName.empty()) {
                ctx.AddError(TIssue(udf->Pos, TStringBuilder() <<
                    "Incorrect format of function name: " << udf->Name));
                hasErrors = true;
            } else {
                if (FunctionRegistry_->IsLoadedUdfModule(moduleName)) {
                    requiredLoadedModules.insert(TString(moduleName));
                    loadedFunctions.push_back(udf);
                } else {
                    requiredExternalModules.insert(TString(moduleName));
                    externalFunctions.push_back(udf);
                }
            }
        }

        TResolve request;
        TVector<TImport*> usedImports;
        THoldingFileStorage holdingFileStorage(FileStorage_);
        THolder<TFilesBox> filesBox = CreateFilesBoxOverFileStorageTemp();

        THashMap<TString, TImport*> path2LoadedImport;
        for (auto import : imports) {
            if (import->Modules) {
                bool needLibrary = false;
                for (auto& m : *import->Modules) {
                    if (requiredLoadedModules.contains(m)) {
                        YQL_ENSURE(import->Block->Type == EUserDataType::PATH);
                        path2LoadedImport[import->Block->Data] = import;
                    }

                    if (requiredExternalModules.contains(m)) {
                        needLibrary = true;
                        break;
                    }
                }

                if (!needLibrary) {
                    continue;
                }
            } else {
                import->Modules.ConstructInPlace();
            }

            try {
                LoadImport(holdingFileStorage, *filesBox, *import, request);
                usedImports.push_back(import);
            } catch (const std::exception& e) {
                ctx.AddError(ExceptionToIssue(e));
                hasErrors = true;
            }
        }

        for (auto& module : requiredExternalModules) {
            if (auto path = FunctionRegistry_->FindUdfPath(module)) {
                auto importRequest = request.AddImports();
                const TString hiddenPath = filesBox->MakeLinkFrom(*path);
                importRequest->SetFileAlias(hiddenPath);
                importRequest->SetPath(hiddenPath);
                importRequest->SetSystem(true);
            }
        }


        for (auto udf : externalFunctions) {
            auto udfRequest = request.AddUdfs();
            udfRequest->SetName(udf->Name);
            udfRequest->SetTypeConfig(udf->TypeConfig);
            if (udf->UserType) {
                udfRequest->SetUserType(WriteTypeToYson(udf->UserType));
            }
        }

        TResolveResult response;
        try {
            response = RunResolverAndParseResult(request, { }, *filesBox);
            filesBox->Destroy();
        } catch (const std::exception& e) {
            ctx.AddError(ExceptionToIssue(e));
            return false;
        }

        // extract regardless of hasErrors value
        hasErrors = !ExtractMetadata(response, usedImports, externalFunctions, ctx) || hasErrors;
        hasErrors = !LoadFunctionsMetadata(loadedFunctions, *FunctionRegistry_, TypeInfoHelper_, ctx) || hasErrors;

        if (!hasErrors) {
            for (auto& m : FunctionRegistry_->GetAllModuleNames()) {
                auto path = *FunctionRegistry_->FindUdfPath(m);
                if (auto import = path2LoadedImport.FindPtr(path)) {
                    (*import)->Modules->push_back(m);
                }
            }
        }

        return !hasErrors;
    }

    TResolveResult LoadRichMetadata(const TVector<TImport>& imports) const override {
        TResolve request;
        THoldingFileStorage holdingFileStorage(FileStorage_);
        THolder<TFilesBox> filesBox = CreateFilesBoxOverFileStorageTemp();
        Y_DEFER {
            filesBox->Destroy();
        };

        for (auto import : imports) {
            LoadImport(holdingFileStorage, *filesBox, import, request);
        }

        return RunResolverAndParseResult(request, { "--discover-proto" }, *filesBox);
    }

private:
    THolder<TFilesBox> CreateFilesBoxOverFileStorageTemp() const {
        return CreateFilesBox(FileStorage_->GetTemp());
    }

    void LoadImport(THoldingFileStorage& holdingFileStorage, TFilesBox& filesBox, const TImport& import, TResolve& request) const {
        const TString path = (import.Block->Type == EUserDataType::PATH) ? import.Block->Data : holdingFileStorage.FreezeFile(*import.Block)->GetPath().GetPath();

        const TString hiddenPath = filesBox.MakeLinkFrom(path);
        auto importRequest = request.AddImports();
        importRequest->SetFileAlias(import.FileAlias);
        importRequest->SetPath(hiddenPath);
        importRequest->SetCustomUdfPrefix(import.Block->CustomUdfPrefix);
    }

    TResolveResult RunResolverAndParseResult(const TResolve& request, const TVector<TString>& additionalArgs, TFilesBox& filesBox) const {
        auto args = UserGroupArgs_;
        args.insert(args.end(), additionalArgs.begin(), additionalArgs.end());

        TString ldLibraryPath;
        TSet<TString> stubbedLibraries;
        return WithRetry<yexception>(10, [&]() {
            TResolveResult response;
            RunResolver(ResolverPath_, args, request, [&](const TString& output) {
                YQL_ENSURE(response.ParseFromString(output), "Cannot deserialize TResolveResult proto message");
            }, ldLibraryPath);
            return response;
        }, [&](const yexception& e, int, int) {
            TStringStream stream;
            SerializeToTextFormat(request, stream);
            YQL_LOG(DEBUG) << "Exception from UdfResolver: " << e.what() << " for request " << stream.Str();
            if (!UdfDependencyStubPath_) {
                YQL_LOG(DEBUG) << "UdfDependencyStubPath is not specified, unable to recover error " << e.what();
                throw e;
            }

            TString sharedLibrary = ExtractSharedObjectNameFromErrorMessage(e.what());
            if (!sharedLibrary) {
                throw e;
            }

            YQL_LOG(DEBUG) << "UdfResolver needs shared library " << sharedLibrary;
            if (!stubbedLibraries.emplace(sharedLibrary).second) {
                // already tried, giving up
                YQL_LOG(ERROR) << "Unable to load shared library " << sharedLibrary << " even after using dependency stub";
                throw e;
            }

            YQL_LOG(DEBUG) << "Using dependency stub for shared library " << sharedLibrary;
            PutSharedLibraryStub(sharedLibrary, filesBox);
            ldLibraryPath = filesBox.GetDir();
        });
    }

    void PutSharedLibraryStub(const TString& sharedLibrary, TFilesBox& filesBox) const {
        YQL_ENSURE(UdfDependencyStubPath_);
        filesBox.MakeLinkFrom(UdfDependencyStubPath_, sharedLibrary);
    }

    static bool ExtractMetadata(const TResolveResult& response, const TVector<TImport*>& usedImports, const TVector<TFunction*>& functions, TExprContext& ctx) {
        bool hasErrors = false;
        YQL_ENSURE(response.UdfsSize() == functions.size(), "Number of returned udf signatures doesn't match original one");
        YQL_ENSURE(response.ImportsSize() >= usedImports.size(), "Number of returned udf modules is too low");

        for (size_t i = 0; i < usedImports.size(); ++i) {
            const TImportResult& importRes = response.GetImports(i);

            TImport* import = usedImports[i];
            if (importRes.HasError()) {
                ctx.AddError(TIssue(import ? import->Pos : TPosition(), importRes.GetError()));
                hasErrors = true;
            } else {
                import->Modules.ConstructInPlace();
                for (auto& module : importRes.GetModules()) {
                    import->Modules->push_back(module);
                }
            }
        }

        for (size_t i = 0; i < response.UdfsSize(); ++i) {
            TFunction* udf = functions[i];
            const TFunctionResult& udfRes = response.GetUdfs(i);
            if (udfRes.HasError()) {
                ctx.AddError(TIssue(udf->Pos, udfRes.GetError()));
                hasErrors = true;
            } else {
                udf->CallableType = ParseTypeFromYson(TStringBuf{udfRes.GetCallableType()}, ctx, udf->Pos);
                if (!udf->CallableType) {
                    hasErrors = true;
                    continue;
                }
                if (udfRes.HasNormalizedUserType()) {
                    udf->NormalizedUserType = ParseTypeFromYson(TStringBuf{udfRes.GetNormalizedUserType()}, ctx, udf->Pos);
                    if (!udf->NormalizedUserType) {
                        hasErrors = true;
                        continue;
                    }
                }
                if (udfRes.HasRunConfigType()) {
                    udf->RunConfigType = ParseTypeFromYson(TStringBuf{udfRes.GetRunConfigType()}, ctx, udf->Pos);
                    if (!udf->RunConfigType) {
                        hasErrors = true;
                        continue;
                    }
                }
                udf->SupportsBlocks = udfRes.GetSupportsBlocks();
                udf->IsStrict = udfRes.GetIsStrict();
            }
        }

        return !hasErrors;
    }

private:
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry_;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper_;
    TFileStoragePtr FileStorage_;
    const TString ResolverPath_;
    const TString UdfDependencyStubPath_;
    TList<TString> UserGroupArgs_;
    const TMap<TString, TString> Path2Md5_;
};

void LoadSystemModulePaths(
        const TString& resolverPath,
        const TString& dir,
        TUdfModulePathsMap* paths)
{
    const TList<TString> args = { TString("--list"), dir };
    RunResolver(resolverPath, args, nullptr, [&](const TString& output) {
        // output format is:
        // {{module_name}}\t{{module_path}}\n

        for (const auto& it : StringSplitter(output).Split('\n')) {
            TStringBuf moduleName, modulePath;
            const TStringBuf& line = it.Token();
            if (!line.empty()) {
                line.Split('\t', moduleName, modulePath);
                paths->emplace(moduleName, modulePath);
            }
        }
    });
}

IUdfResolver::TPtr CreateOutProcUdfResolver(
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TFileStoragePtr& fileStorage,
    const TString& resolverPath,
    const TString& user,
    const TString& group,
    bool filterSyscalls,
    const TString& udfDependencyStubPath,
    const TMap<TString, TString>& path2md5) {
    return new TOutProcUdfResolver(functionRegistry, fileStorage, resolverPath, user, group, filterSyscalls, udfDependencyStubPath, path2md5);
}

} // namespace NCommon
} // namespace NYql
