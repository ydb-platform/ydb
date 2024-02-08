#include "yql_udf_resolver_with_index.h"

#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_utils.h>

#include <util/generic/hash_set.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

namespace NYql {
namespace NCommon {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

class TUdfResolverWithIndex : public IUdfResolver {
    class TResourceFile : public TThrRefBase {
    public:
        typedef TIntrusivePtr<TResourceFile> TPtr;

    public:
        TResourceFile(TString alias, const TVector<TString>& modules, TFileLinkPtr link)
            : Link_(std::move(link))
        {
            Import_.FileAlias = alias;
            Import_.Block = &Block_;
            Import_.Modules = MakeMaybe(modules);

            Block_.Type = EUserDataType::PATH;
            Block_.Data = Link_->GetPath();
            Block_.Usage.Set(EUserDataBlockUsage::Udf);
        }

        static TResourceFile::TPtr Create(const TString& packageName, const TSet<TString>& modules, TFileLinkPtr link) {
            // assume package name has no bad symbols for file name
            TString basename =  link->GetPath().Basename();
            TString alias = basename.StartsWith("lib") ? basename : ("lib_" + packageName + "_udf.so");
            alias.to_lower();
            return MakeIntrusive<TResourceFile>(std::move(alias), TVector<TString>(modules.begin(), modules.end()), std::move(link));
        }

    public:
        TFileLinkPtr Link_;
        TUserDataBlock Block_;
        TImport Import_;
    };

public:
    TUdfResolverWithIndex(TUdfIndex::TPtr udfIndex, IUdfResolver::TPtr fallback, TFileStoragePtr fileStorage)
        : UdfIndex_(udfIndex)
        , Fallback_(fallback)
        , FileStorage_(fileStorage)
    {
        Y_ENSURE(UdfIndex_);
        Y_ENSURE(FileStorage_);
        // fallback is required only to handle type aware functions and loading rich metadata
        Y_ENSURE(Fallback_);
    }

    TMaybe<TFilePathWithMd5> GetSystemModulePath(const TStringBuf& moduleName) const override {
        with_lock(Lock_) {
            TString moduleNameStr(moduleName);
            if (!UdfIndex_->ContainsModule(moduleNameStr)) {
                return Nothing();
            }

            auto file = DownloadFileWithModule(moduleNameStr);
            return MakeMaybe<TFilePathWithMd5>(file->Link_->GetPath(), file->Link_->GetMd5());
        }
    }

    bool LoadMetadata(const TVector<TImport*>& imports, const TVector<TFunction*>& functions, TExprContext& ctx) const override {
        with_lock(Lock_) {
            bool hasErrors = false;
            THashSet<TString> requiredModules;
            TVector<TFunction*> fallbackFunctions;
            TVector<TImport*> fallbackImports = imports;
            TSet<TImport*> additionalImports; // avoid duplicates

            for (auto udfPtr : functions) {
                TImport* additionalImport = nullptr;
                TFunction* fallbackFunction = nullptr;
                if (!LoadFunctionMetadata(*udfPtr, ctx, fallbackFunction, additionalImport)) {
                    hasErrors = true;
                    continue;
                }

                if (additionalImport) {
                    additionalImports.insert(additionalImport);
                }

                if (fallbackFunction) {
                    fallbackFunctions.push_back(fallbackFunction);
                }
            }

            fallbackImports.insert(fallbackImports.end(), additionalImports.begin(), additionalImports.end());

            return Fallback_->LoadMetadata(fallbackImports, fallbackFunctions, ctx) && !hasErrors;
        }
    }

    TResolveResult LoadRichMetadata(const TVector<TImport>& imports) const override {
        return Fallback_->LoadRichMetadata(imports);
    }

    bool ContainsModule(const TStringBuf& moduleName) const override {
        TString moduleNameStr = TString(moduleName);
        if (UdfIndex_->ContainsModule(moduleNameStr)) {
            return true;
        }

        return Fallback_->ContainsModule(moduleName);
    }

private:
    bool LoadFunctionMetadata(TFunction& function, TExprContext& ctx, TFunction*& fallbackFunction, TImport*& additionalImport) const {
        TStringBuf moduleName, funcName;
        if (!SplitUdfName(function.Name, moduleName, funcName) || moduleName.empty() || funcName.empty()) {
            ctx.AddError(TIssue(function.Pos, TStringBuilder() << "Incorrect format of function name: " << function.Name));
            return false;
        }

        /*
        the order is really important:
        1) check we have such module
            no-> fallback function
        2) check we have such function
            no -> error
        3) download resource file
            fail -> error
        4) if polymorphic function -> fallback function with additional Import for downloaded file
        */

        TString moduleNameStr = TString(moduleName);
        if (!UdfIndex_->ContainsModule(moduleNameStr)) {
            fallbackFunction = &function;
            return true;
        }

        TFunctionInfo info;
        if (!UdfIndex_->FindFunction(moduleNameStr, function.Name, info)) {
            ctx.AddError(TIssue(function.Pos, TStringBuilder() << "Function not found: " << function.Name));
            return false;
        }

        TResourceFile::TPtr file = DownloadFileWithModule(moduleName, function.Pos, ctx);
        if (!file) {
            return false;
        }

        additionalImport = &file->Import_;

        if (info.IsTypeAwareness) {
            fallbackFunction = &function;
            return true;
        }

        if (!info.CallableType) {
            ctx.AddError(TIssue(function.Pos, TStringBuilder() << "CallableType for function " << function.Name << " is empty. Check UDF source code for errors."));
            return false;
        }

        function.CallableType = ParseTypeFromYson(TStringBuf{info.CallableType}, ctx, function.Pos);
        if (!function.CallableType) {
            ctx.AddError(TIssue(function.Pos, TStringBuilder() << "Failed to build callable type from YSON for function " << function.Name));
            return false;
        }

        if (info.RunConfigType) {
            function.RunConfigType = ParseTypeFromYson(TStringBuf{info.RunConfigType}, ctx, function.Pos);
            if (!function.RunConfigType) {
                ctx.AddError(TIssue(function.Pos, TStringBuilder() << "Failed to build run config type from YSON for function " << function.Name));
                return false;
            }
        } else {
            function.RunConfigType = std::get<0>(ctx.SingletonTypeCache);
        }

        function.NormalizedUserType = std::get<0>(ctx.SingletonTypeCache);
        function.IsStrict = info.IsStrict;
        function.SupportsBlocks = info.SupportsBlocks;
        return true;
    }

    TResourceFile::TPtr DownloadFileWithModule(const TStringBuf& module, const TPosition& pos, TExprContext& ctx) const {
        try {
            return DownloadFileWithModule(module);
        } catch (const std::exception& e) {
            ctx.AddError(ExceptionToIssue(e, pos));
        }

        return nullptr;
    }

    TResourceFile::TPtr DownloadFileWithModule(const TStringBuf& module) const {
        TString moduleName(module);

        const auto it = DownloadedFiles_.find(module);
        if (it != DownloadedFiles_.end()) {
            return it->second;
        }

        auto resource = UdfIndex_->FindResourceByModule(moduleName);
        if (!resource) {
            ythrow yexception() << "No resource has been found for registered module " << moduleName;
        }

        // token is empty for urls for now
        // assumption: file path is frozen already, no need to put into file storage
        const TDownloadLink& downloadLink = resource->Link;
        TFileLinkPtr link = downloadLink.IsUrl ? FileStorage_->PutUrl(downloadLink.Path, {}) : CreateFakeFileLink(downloadLink.Path, downloadLink.Md5);
        TResourceFile::TPtr file = TResourceFile::Create(moduleName, resource->Modules, link);
        for (auto& d : resource->Modules) {
            auto p = DownloadedFiles_.emplace(d, file);
            if (!p.second) {
                // should not happen because UdfIndex handles conflicts
                ythrow yexception() << "file already downloaded for module " << moduleName << ", conflicting path " << downloadLink.Path << ", existing local file " << p.first->second->Link_->GetPath();
            }
        }

        return file;
    }

private:
    mutable TMutex Lock_;
    const TUdfIndex::TPtr UdfIndex_;
    const IUdfResolver::TPtr Fallback_;
    const TFileStoragePtr FileStorage_;
    // module -> downloaded resource file
    mutable TMap<TString, TResourceFile::TPtr> DownloadedFiles_;
};

IUdfResolver::TPtr CreateUdfResolverWithIndex(TUdfIndex::TPtr udfIndex, IUdfResolver::TPtr fallback, TFileStoragePtr fileStorage) {
    return new TUdfResolverWithIndex(udfIndex, fallback, fileStorage);
}

} // namespace NCommon
} // namespace NYql
