#include "yql_simple_udf_resolver.h"

#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/core/yql_holding_file_storage.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_utils.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

#include <util/generic/vector.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

namespace NYql {
namespace NCommon {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

class TSimpleUdfResolver : public IUdfResolver {
public:
    TSimpleUdfResolver(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, const TFileStoragePtr& fileStorage, bool useFakeMD5)
        : FunctionRegistry_(functionRegistry)
        , FileStorage_(fileStorage)
        , TypeInfoHelper_(new TTypeInfoHelper)
        , UseFakeMD5_(useFakeMD5)
    {}

    TMaybe<TFilePathWithMd5> GetSystemModulePath(const TStringBuf& moduleName) const override {
        with_lock(Lock_) {
            auto path = FunctionRegistry_->FindUdfPath(moduleName);
            return path ? MakeMaybe<TFilePathWithMd5>(*path, UseFakeMD5_ ? *path : TString()) : Nothing();
        }
    }

    bool LoadMetadata(const TVector<TImport*>& imports,
        const TVector<TFunction*>& functions, TExprContext& ctx) const override {

        with_lock(Lock_) {
            bool hasErrors = false;
            THashSet<TString> requiredModules;
            for (auto udfPtr : functions) {
                auto& udf = *udfPtr;
                TStringBuf moduleName, funcName;
                if (!SplitUdfName(udf.Name, moduleName, funcName) || moduleName.empty() || funcName.empty()) {
                    ctx.AddError(TIssue(udf.Pos, TStringBuilder() <<
                        "Incorrect format of function name: " << udf.Name));
                    hasErrors = true;
                } else {
                    requiredModules.insert(TString(moduleName));
                }
            }

            THoldingFileStorage holdingFileStorage(FileStorage_);
            auto newRegistry = FunctionRegistry_->Clone();
            THashMap<TString, TImport*> path2import;
            for (auto import: imports) {
                if (import->Modules) {
                    bool needLibrary = false;
                    for (auto& m : *import->Modules) {
                        if (requiredModules.contains(m)) {
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
                const TString& customUdfPrefix = import->Block->CustomUdfPrefix;
                try {
                    if (FileStorage_) {
                        auto link = holdingFileStorage.FreezeFile(*import->Block);
                        auto path = link->GetPath().GetPath();
                        newRegistry->LoadUdfs(
                                path,
                                {},
                                NUdf::IRegistrator::TFlags::TypesOnly,
                                customUdfPrefix);
                        path2import[path] = import;
                    } else {
                        if (import->Block->Type != EUserDataType::PATH) {
                            ctx.AddError(TIssue(import->Pos, TStringBuilder() <<
                                "Only path file type is supported, cannot load file with alias: " << import->FileAlias));
                            hasErrors = true;
                            continue;
                        }
                        newRegistry->LoadUdfs(
                                import->Block->Data,
                                {},
                                NUdf::IRegistrator::TFlags::TypesOnly,
                                customUdfPrefix);
                        path2import[import->Block->Data] = import;
                    }
                }
                catch (yexception& e) {
                    ctx.AddError(TIssue(import->Pos, TStringBuilder()
                        << "Internal error of loading udf module: " << import->FileAlias
                        << ", reason: " << e.what()));
                    hasErrors = true;
                }
            }

            if (!hasErrors) {
                for (auto& m : newRegistry->GetAllModuleNames()) {
                    auto path = *newRegistry->FindUdfPath(m);
                    if (auto import = path2import.FindPtr(path)) {
                        (*import)->Modules->push_back(m);
                    }
                }
            }

            hasErrors = !LoadFunctionsMetadata(functions, *newRegistry, TypeInfoHelper_, ctx) || hasErrors;
            return !hasErrors;
        }
    }

    TResolveResult LoadRichMetadata(const TVector<TImport>& imports) const override {
        Y_UNUSED(imports);
        ythrow yexception() << "LoadRichMetadata is not supported in SimpleUdfResolver";
    }

private:
    mutable TAdaptiveLock Lock_;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry_;
    TFileStoragePtr FileStorage_;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper_;
    const bool UseFakeMD5_;
};

IUdfResolver::TPtr CreateSimpleUdfResolver(
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TFileStoragePtr& fileStorage,
    bool useFakeMD5
) {
    return new TSimpleUdfResolver(functionRegistry, fileStorage, useFakeMD5);
}

bool LoadFunctionsMetadata(const TVector<IUdfResolver::TFunction*>& functions,
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    NUdf::ITypeInfoHelper::TPtr typeInfoHelper,
    TExprContext& ctx) {

    bool hasErrors = false;
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);

    for (auto udfPtr : functions) {
        auto& udf = *udfPtr;
        try {
            TProgramBuilder pgmBuilder(env, functionRegistry);
            TType* mkqlUserType = nullptr;
            if (udf.UserType) {
                TStringStream err;
                mkqlUserType = BuildType(*udf.UserType, pgmBuilder, err, true);
                if (!mkqlUserType) {
                    ctx.AddError(TIssue(udf.Pos, TStringBuilder() << "Invalid user type for function: "
                        << udf.Name << ", error: " << err.Str()));
                    hasErrors = true;
                    continue;
                }
            }

            auto secureParamsProvider = MakeSimpleSecureParamsProvider(udf.SecureParams);

            TFunctionTypeInfo funcInfo;
            auto status = functionRegistry.FindFunctionTypeInfo(env, typeInfoHelper, nullptr,
                udf.Name, mkqlUserType, udf.TypeConfig, NUdf::IUdfModule::TFlags::TypesOnly, {}, secureParamsProvider.get(), &funcInfo);
            if (!status.IsOk()) {
                ctx.AddError(TIssue(udf.Pos, TStringBuilder() << "Failed to find UDF function: " << udf.Name
                    << ", reason: " << status.GetError()));
                hasErrors = true;
                continue;
            }

            udf.CallableType = ConvertMiniKQLType(udf.Pos, funcInfo.FunctionType, ctx);
            YQL_ENSURE(udf.CallableType);
            if (funcInfo.RunConfigType) {
                udf.RunConfigType = ConvertMiniKQLType(udf.Pos, const_cast<TType*>(funcInfo.RunConfigType), ctx);
                YQL_ENSURE(udf.RunConfigType);
            }

            if (funcInfo.UserType) {
                udf.NormalizedUserType = ConvertMiniKQLType(udf.Pos, const_cast<TType*>(funcInfo.UserType), ctx);
                YQL_ENSURE(udf.NormalizedUserType);
            }

            udf.SupportsBlocks = funcInfo.SupportsBlocks;
            udf.IsStrict = funcInfo.IsStrict;
        } catch (const std::exception& e) {
            ctx.AddError(TIssue(udf.Pos, TStringBuilder()
                << "Internal error was found when udf metadata is loading for function: " << udf.Name
                << ", reason: " << e.what()));
            hasErrors = true;
        }
    }

    return !hasErrors;
}

} // namespace NCommon
} // namespace NYql
