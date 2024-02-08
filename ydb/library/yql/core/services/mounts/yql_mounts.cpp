#include "yql_mounts.h"

#include <ydb/library/yql/core/yql_library_compiler.h>

#include <library/cpp/resource/resource.h>

#include <util/folder/path.h>
#include <util/stream/file.h>

namespace NYql {
    namespace {
        using namespace NUserData;

        void AddLibraryFromResource(TUserDataTable& userDataTable, const TString& resourceName) {
            auto& block = userDataTable[TUserDataKey::File(resourceName)];
            block.Data = NResource::Find(resourceName);
            block.Type = EUserDataType::RAW_INLINE_DATA;
            block.Usage.Set(EUserDataBlockUsage::Library, true);
        }

        TUserDataKey CreateKey(const NUserData::TUserData& item) {
            TString name = (item.Disposition_ == EDisposition::RESOURCE) ? item.Content_ : item.Name_;

            if (!name.StartsWith('/')) {
                name = GetDefaultFilePrefix() + name;
            }

            if (item.Type_ == EType::UDF) {
                return TUserDataKey::Udf(name);
            } else {
                return TUserDataKey::File(name);
            }
        }

        void AddUserDataToTable(
            TUserDataTable& userDataTable,
            const NUserData::TUserData& item) {

            auto& block = userDataTable[CreateKey(item)];

            switch (item.Disposition_) {
                case EDisposition::INLINE:
                    block.Data = item.Content_;
                    block.Type = EUserDataType::RAW_INLINE_DATA;
                    break;
                case EDisposition::RESOURCE:
                case EDisposition::RESOURCE_FILE:
                    block.Data = NResource::Find(item.Content_);
                    block.Type = EUserDataType::RAW_INLINE_DATA;
                    break;
                case EDisposition::FILESYSTEM:
                    block.Data = item.Content_;
                    block.Type = EUserDataType::PATH;
                    break;
                case EDisposition::URL:
                    block.Data = item.Content_;
                    block.Type = EUserDataType::URL;
                    break;
                default:
                    ythrow yexception() << "Unknown disposition for user data \"" << item.Name_ << "\": " << item.Disposition_;
            }

            switch (item.Type_) {
                case EType::LIBRARY:
                    block.Usage.Set(EUserDataBlockUsage::Library, true);
                    break;
                case EType::FILE:
                    block.Usage.Set(EUserDataBlockUsage::Content, true);
                    break;
                case EType::UDF:
                    block.Usage.Set(EUserDataBlockUsage::Udf, true);
                    break;
                default:
                    ythrow yexception() << "Unknown type for user data \"" << item.Name_ << "\": " << item.Type_;
            }

            if (item.Type_ == EType::LIBRARY) {
                switch (item.Disposition_) {
                    case EDisposition::INLINE:
                    case EDisposition::RESOURCE:
                    case EDisposition::RESOURCE_FILE:
                    case EDisposition::FILESYSTEM: {
                        if (item.Disposition_ == EDisposition::FILESYSTEM) {
                            TFsPath path(block.Data);
                            if (path.Exists() && path.IsFile()) {
                                TFileInput input(path);
                                block.Data = input.ReadAll();
                            } else {
                                ythrow yexception() << "File for user data \"" << item.Name_ << "\" does not exist: " << block.Data;
                            }
                            block.Type = EUserDataType::RAW_INLINE_DATA;
                        }
                    } break;
                    default:
                        ythrow yexception() << item.Disposition_ << " disposition is not yet supported for libraries (specified for \"" << item.Name_ << "\")";
                }
            }
        }

    }

    void LoadYqlDefaultMounts(TUserDataTable& userData) {
        AddLibraryFromResource(userData, "/lib/yql/aggregate.yql");
        AddLibraryFromResource(userData, "/lib/yql/window.yql");
        AddLibraryFromResource(userData, "/lib/yql/id.yql");
        AddLibraryFromResource(userData, "/lib/yql/sqr.yql");
        AddLibraryFromResource(userData, "/lib/yql/core.yql");
        AddLibraryFromResource(userData, "/lib/yql/walk_folders.yql");
    }

    TUserDataTable GetYqlModuleResolverImpl(
        TExprContext* rawCtx,
        IModuleResolver::TPtr& moduleResolver,
        const TVector<NUserData::TUserData>& userData,
        const THashMap<TString, TString>& clusterMapping,
        const THashSet<TString>& sqlFlags,
        bool optimizeLibraries,
        THolder<TExprContext> ownedCtx)
    {
        auto ctx = rawCtx ? rawCtx : ownedCtx.Get();
        Y_ENSURE(ctx);
        TUserDataTable mounts;
        LoadYqlDefaultMounts(mounts);

        TModulesTable modulesTable;
        if (!CompileLibraries(mounts, *ctx, modulesTable, optimizeLibraries)) {
            return {};
        }

        for (const auto& item : userData) {
            AddUserDataToTable(mounts, item);
        }

        moduleResolver = std::make_shared<TModuleResolver>(std::move(modulesTable), ctx->NextUniqueId,
            clusterMapping, sqlFlags, optimizeLibraries, std::move(ownedCtx));
        return mounts;
    }

    TUserDataTable GetYqlModuleResolver(
        TExprContext& ctx,
        IModuleResolver::TPtr& moduleResolver,
        const TVector<NUserData::TUserData>& userData,
        const THashMap<TString, TString>& clusterMapping,
        const THashSet<TString>& sqlFlags,
        bool optimizeLibraries) {
        return GetYqlModuleResolverImpl(&ctx, moduleResolver, userData, clusterMapping, sqlFlags, optimizeLibraries, nullptr);
    }

    bool GetYqlDefaultModuleResolver(
        TExprContext& ctx,
        IModuleResolver::TPtr& moduleResolver,
        const THashMap<TString, TString>& clusterMapping,
        bool optimizeLibraries) {
        return !GetYqlModuleResolverImpl(&ctx, moduleResolver, {}, clusterMapping, {}, optimizeLibraries, nullptr).empty();
    }

    bool GetYqlDefaultModuleResolverWithContext(
        IModuleResolver::TPtr& moduleResolver,
        const THashMap<TString, TString>& clusterMapping,
        bool optimizeLibraries) {
        return !GetYqlModuleResolverImpl(nullptr, moduleResolver, {}, clusterMapping, {}, optimizeLibraries, MakeHolder<TExprContext>()).empty();
    }
}
