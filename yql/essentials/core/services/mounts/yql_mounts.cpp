#include "yql_mounts.h"

#include <yql/essentials/core/yql_library_compiler.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/utils/log/profile.h>

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
            TString name = (item.Disposition == EDisposition::RESOURCE) ? item.Content : item.Name;

            if (!name.StartsWith('/')) {
                name = GetDefaultFilePrefix() + name;
            }

            if (item.Type == EType::UDF) {
                return TUserDataKey::Udf(name);
            } else {
                return TUserDataKey::File(name);
            }
        }

        void AddUserDataToTable(
            TUserDataTable& userDataTable,
            const NUserData::TUserData& item) {

            auto& block = userDataTable[CreateKey(item)];

            switch (item.Disposition) {
                case EDisposition::INLINE:
                    block.Data = item.Content;
                    block.Type = EUserDataType::RAW_INLINE_DATA;
                    break;
                case EDisposition::RESOURCE:
                case EDisposition::RESOURCE_FILE:
                    block.Data = NResource::Find(item.Content);
                    block.Type = EUserDataType::RAW_INLINE_DATA;
                    break;
                case EDisposition::FILESYSTEM:
                    block.Data = item.Content;
                    block.Type = EUserDataType::PATH;
                    break;
                case EDisposition::URL:
                    block.Data = item.Content;
                    block.Type = EUserDataType::URL;
                    break;
                default:
                    ythrow yexception() << "Unknown disposition for user data \"" << item.Name << "\": " << item.Disposition;
            }

            switch (item.Type) {
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
                    ythrow yexception() << "Unknown type for user data \"" << item.Name << "\": " << item.Type;
            }

            if (item.Type == EType::LIBRARY) {
                switch (item.Disposition) {
                    case EDisposition::INLINE:
                    case EDisposition::RESOURCE:
                    case EDisposition::RESOURCE_FILE:
                    case EDisposition::FILESYSTEM: {
                        if (item.Disposition == EDisposition::FILESYSTEM) {
                            TFsPath path(block.Data);
                            if (path.Exists() && path.IsFile()) {
                                TFileInput input(path);
                                block.Data = input.ReadAll();
                            } else {
                                ythrow yexception() << "File for user data \"" << item.Name << "\" does not exist: " << block.Data;
                            }
                            block.Type = EUserDataType::RAW_INLINE_DATA;
                        }
                    } break;
                    default:
                        ythrow yexception() << item.Disposition << " disposition is not yet supported for libraries (specified for \"" << item.Name << "\")";
                }
            }
        }

    }

    void LoadYqlDefaultMounts(TUserDataTable& userData) {
        AddLibraryFromResource(userData, "/lib/yql/aggregate.yqls");
        AddLibraryFromResource(userData, "/lib/yql/window.yqls");
        AddLibraryFromResource(userData, "/lib/yql/id.yqls");
        AddLibraryFromResource(userData, "/lib/yql/sqr.yqls");
        AddLibraryFromResource(userData, "/lib/yql/core.yqls");
        AddLibraryFromResource(userData, "/lib/yql/walk_folders.yqls");
    }

    TUserDataTable GetYqlModuleResolverImpl(
        TExprContext* rawCtx,
        IModuleResolver::TPtr& moduleResolver,
        const TVector<NUserData::TUserData>& userData,
        const THashMap<TString, TString>& clusterMapping,
        const THashSet<TString>& sqlFlags,
        bool optimizeLibraries,
        THolder<TExprContext> ownedCtx,
        TModuleResolver::TModuleChecker moduleChecker)
    {
        YQL_PROFILE_FUNC(DEBUG);
        auto ctx = rawCtx ? rawCtx : ownedCtx.Get();
        Y_ENSURE(ctx);
        TUserDataTable mounts;
        LoadYqlDefaultMounts(mounts);

        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
        parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
        NSQLTranslation::TTranslators translators(
            nullptr,
            NSQLTranslationV1::MakeTranslator(lexers, parsers),
            nullptr
        );

        TModulesTable modulesTable;
        if (!CompileLibraries(translators, mounts, *ctx, modulesTable, optimizeLibraries)) {
            return {};
        }

        for (const auto& item : userData) {
            AddUserDataToTable(mounts, item);
        }

        moduleResolver = std::make_shared<TModuleResolver>(translators, std::move(modulesTable), ctx->NextUniqueId,
            clusterMapping, sqlFlags, optimizeLibraries, std::move(ownedCtx), moduleChecker);
        return mounts;
    }

    TUserDataTable GetYqlModuleResolver(
        TExprContext& ctx,
        IModuleResolver::TPtr& moduleResolver,
        const TVector<NUserData::TUserData>& userData,
        const THashMap<TString, TString>& clusterMapping,
        const THashSet<TString>& sqlFlags,
        bool optimizeLibraries,
        TModuleResolver::TModuleChecker moduleChecker) {
        return GetYqlModuleResolverImpl(&ctx, moduleResolver, userData, clusterMapping, sqlFlags, optimizeLibraries, nullptr, moduleChecker);
    }

    bool GetYqlDefaultModuleResolver(
        TExprContext& ctx,
        IModuleResolver::TPtr& moduleResolver,
        const THashMap<TString, TString>& clusterMapping,
        bool optimizeLibraries,
        TModuleResolver::TModuleChecker moduleChecker) {
        return !GetYqlModuleResolverImpl(&ctx, moduleResolver, {}, clusterMapping, {}, optimizeLibraries, nullptr, moduleChecker).empty();
    }

    bool GetYqlDefaultModuleResolverWithContext(
        IModuleResolver::TPtr& moduleResolver,
        const THashMap<TString, TString>& clusterMapping,
        bool optimizeLibraries,
        TModuleResolver::TModuleChecker moduleChecker) {
        return !GetYqlModuleResolverImpl(nullptr, moduleResolver, {}, clusterMapping, {}, optimizeLibraries, MakeHolder<TExprContext>(), moduleChecker).empty();
    }
}
