#include "yql_library_compiler.h"
#include "yql_expr_optimize.h"

#include <ydb/library/yql/sql/sql.h>

#include <util/system/file.h>

#include <unordered_set>
#include <unordered_map>

namespace NYql {

namespace {

bool ReplaceNodes(TExprNode& node, const TNodeOnNodeOwnedMap& replaces, bool& hasChanges, TNodeSet& visited, TNodeSet& parents)
{
    if (!node.ChildrenSize()) {
        return true;
    }

    const auto pair = parents.emplace(&node);
    if (!pair.second) {
        return false;
    }

    if (!visited.emplace(&node).second) {
        parents.erase(pair.first);
        return true;
    }

    for (ui32 i = 0U; i < node.ChildrenSize(); ++i) {
        auto& child = node.ChildRef(i);
        if (const auto it = replaces.find(node.Child(i)); replaces.cend() != it) {
            child = it->second;
            hasChanges = true;
        }

        if (!ReplaceNodes(*node.Child(i), replaces, hasChanges, visited, parents)) {
            child.Reset();
            return false;
        }
    }
    parents.erase(pair.first);
    return true;
}

bool ReplaceNodes(TExprNode& node, const TNodeOnNodeOwnedMap& replaces, bool& hasChanges) {
    TNodeSet visited;
    TNodeSet parents;
    return ReplaceNodes(node, replaces, hasChanges, visited, parents);
}

TString Load(const TString& path)
{
    TFile file(path, EOpenModeFlag::RdOnly);
    if (file.GetLength() <= 0)
        return TString();
    std::vector<TString::value_type> buffer(file.GetLength());
    file.Load(buffer.data(), buffer.size());
    return TString(buffer.data(), buffer.size());
}

}

bool OptimizeLibrary(TLibraryCohesion& cohesion, TExprContext& ctx) {
    TExprNode::TListType tupleItems;
    for (const auto& x : cohesion.Exports.Symbols()) {
        tupleItems.push_back(x.second);
    }

    auto root = ctx.NewList(TPositionHandle(), std::move(tupleItems));
    for (;;) {
        auto status = ExpandApply(root, root, ctx);
        if (status == IGraphTransformer::TStatus::Error) {
            return false;
        }

        if (status == IGraphTransformer::TStatus::Ok) {
            ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
            break;
        }
    }

    ui32 index = 0;
    for (auto& x : cohesion.Exports.Symbols(ctx)) {
        x.second = root->ChildPtr(index++);
    }

    return true;
}

bool CompileLibrary(const TString& alias, const TString& script, TExprContext& ctx, TLibraryCohesion& cohesion, bool optimize)
{
    TAstParseResult res;
    if (alias.EndsWith(".sql")) {
        NSQLTranslation::TTranslationSettings translationSettings;
        translationSettings.SyntaxVersion = 1;
        translationSettings.Mode = NSQLTranslation::ESqlMode::LIBRARY;
        res = NSQLTranslation::SqlToYql(script, translationSettings);
    } else {
        res = ParseAst(script, nullptr, alias);
    }
    if (!res.IsOk()) {
        for (const auto& originalError : res.Issues) {
            TIssue error(originalError);
            TStringBuilder message;
            message << error.GetMessage() << " (at " << alias << ")";
            error.SetMessage(message);
            ctx.AddError(error);
        }
        return false;
    }

    if (!CompileExpr(*res.Root, cohesion, ctx))
        return false;

    if (!optimize) {
        return true;
    }

    return OptimizeLibrary(cohesion, ctx);
}

bool LinkLibraries(THashMap<TString, TLibraryCohesion>& libs, TExprContext& ctx, TExprContext& ctxToClone, const TModulesTable* loadedModules) {
    std::function<const TExportTable*(const TString&)> f = [loadedModules](const TString& normalizedModuleName) -> const TExportTable* {
        if (!loadedModules) {
            return nullptr;
        }

        return loadedModules->FindPtr(normalizedModuleName);
    };

    return LinkLibraries(libs, ctx, ctxToClone, f);
}

bool LinkLibraries(THashMap<TString, TLibraryCohesion>& libs, TExprContext& ctx, TExprContext& ctxToClone, const std::function<const TExportTable*(const TString&)>& module2ExportTable)
{
    TNodeOnNodeOwnedMap clones, replaces;
    for (const auto& lib : libs) {
        for (const auto& import : lib.second.Imports) {
            if (import.first->Dead()) {
                continue;
            }

            if (import.second.first == lib.first) {
                ctx.AddError(TIssue(ctxToClone.GetPosition(import.first->Pos()),
                    TStringBuilder() << "Library '" << lib.first << "' tries to import itself."));
                return false;
            }

            const auto* exportTable = module2ExportTable(TModuleResolver::NormalizeModuleName(import.second.first));
            const bool externalModule = exportTable;

            if (!exportTable) {
                if (const auto it = libs.find(import.second.first); libs.cend() != it) {
                    exportTable = &it->second.Exports;
                }
            }

            if (!exportTable) {
                ctx.AddError(TIssue(ctxToClone.GetPosition(import.first->Pos()),
                    TStringBuilder() << "Library '" << lib.first << "' has unresolved dependency from '" << import.second.first << "'."));
                return false;
            }

            if (const auto ex = exportTable->Symbols().find(import.second.second); exportTable->Symbols().cend() != ex) {
                replaces[import.first] = externalModule ? ctxToClone.DeepCopy(*ex->second, exportTable->ExprCtx(), clones, true, false) : ex->second;
            } else {
                ctx.AddError(TIssue(ctxToClone.GetPosition(import.first->Pos()),
                    TStringBuilder() << "Library '" << lib.first << "' has unresolved symbol '" << import.second.second << "' from '" << import.second.first << "'."));
                return false;
            }
        }
    }

    if (!replaces.empty()) {
        for (auto& lib : libs) {
            for (auto& expo : lib.second.Exports.Symbols(lib.second.Exports.ExprCtx())) {
                if (const auto find = replaces.find(expo.second.Get()); replaces.cend() != find)
                    expo.second = find->second;
            }
        }
    }

    for (bool hasChanges = !replaces.empty(); hasChanges;) {
        hasChanges = false;
        for (const auto& lib : libs) {
            for (const auto& expo : lib.second.Exports.Symbols()) {
                if (!ReplaceNodes(*expo.second, replaces, hasChanges)) {
                    ctx.AddError(TIssue(ctxToClone.GetPosition(expo.second->Pos()),
                        TStringBuilder() << "Cross reference detected under '" << expo.first << "' in '" << lib.first << "'."));
                    return false;
                }
            }
        }
    }

    return true;
}

bool CompileLibraries(const TUserDataTable& userData, TExprContext& ctx, TModulesTable& modules, bool optimize)
{
    THashMap<TString, TLibraryCohesion> libs;
    for (const auto& data : userData) {
        if (data.first.IsFile() && data.second.Usage.Test(EUserDataBlockUsage::Library)) {
            TString libraryData;
            const TString& alias = data.first.Alias();
            if (data.second.Type == EUserDataType::PATH) {
                libraryData = Load(data.second.Data);
            } else if (data.second.Type == EUserDataType::RAW_INLINE_DATA) {
                libraryData = data.second.Data;
            }

            if (!libraryData.empty()) {
                if (CompileLibrary(alias, libraryData, ctx, libs[alias], optimize))
                    modules[TModuleResolver::NormalizeModuleName(alias)] = libs[alias].Exports;
                else
                    return false;
            }
        }
    }

    return LinkLibraries(libs, ctx, ctx);
}

}
