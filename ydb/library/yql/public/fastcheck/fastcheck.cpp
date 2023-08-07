#include "fastcheck.h"
#include <ydb/library/yql/ast/yql_ast.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/core/user_data/yql_user_data.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/yql_user_data_storage.h>
#include <ydb/library/yql/sql/sql.h>

namespace NYql {
namespace NFastCheck {

bool CheckProgram(const TString& program, const TOptions& options, TIssues& errors) {
    TAstParseResult astRes;
    if (options.IsSql) {
        NSQLTranslation::TTranslationSettings settings;
        settings.ClusterMapping = options.ClusterMapping;
        settings.SyntaxVersion = options.SyntaxVersion;
        settings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
        if (options.IsLibrary) {
            settings.Mode = NSQLTranslation::ESqlMode::LIBRARY;
        }

        astRes = SqlToYql(program, settings);
    } else {
        astRes = ParseAst(program);
    }

    if (!astRes.IsOk()) {
        errors = std::move(astRes.Issues);
        return false;
    }

    if (options.IsLibrary) {
        return true;
    }

    if (options.ParseOnly) {
        // parse SQL libs
        for (const auto& x : options.SqlLibs) {
            NSQLTranslation::TTranslationSettings settings;
            settings.ClusterMapping = options.ClusterMapping;
            settings.SyntaxVersion = options.SyntaxVersion;
            settings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
            settings.File = x.first;
            settings.Mode = NSQLTranslation::ESqlMode::LIBRARY;

            astRes = SqlToYql(x.second, settings);
            if (!astRes.IsOk()) {
                errors = std::move(astRes.Issues);
                return false;
            }
        }

        return true;
    }

    TVector<NUserData::TUserData> userData;
    for (const auto& x : options.SqlLibs) {
        NUserData::TUserData data;
        data.Type_ = NUserData::EType::LIBRARY;
        data.Disposition_ = NUserData::EDisposition::INLINE;
        data.Name_ = x.first;
        data.Content_ = x.second;
        userData.push_back(data);
    }

    TExprContext libCtx;
    libCtx.IssueManager.AddIssues(std::move(astRes.Issues));
    IModuleResolver::TPtr moduleResolver;
    TUserDataTable userDataTable = GetYqlModuleResolver(libCtx, moduleResolver, userData, options.ClusterMapping, {});
    if (!userDataTable) {
        errors = libCtx.IssueManager.GetIssues();
        libCtx.IssueManager.Reset();
        return false;
    }

    auto userDataStorage = MakeIntrusive<TUserDataStorage>(nullptr, userDataTable, nullptr, nullptr);
    if (auto modules = dynamic_cast<TModuleResolver*>(moduleResolver.get())) {
        modules->AttachUserData(userDataStorage);
    }

    TExprContext exprCtx(libCtx.NextUniqueId);
    TExprNode::TPtr exprRoot;
    if (!CompileExpr(*astRes.Root, exprRoot, exprCtx, moduleResolver.get(), nullptr, false, Max<ui32>(), options.SyntaxVersion)) {
        errors = exprCtx.IssueManager.GetIssues();
        exprCtx.IssueManager.Reset();
        return false;
    }

    return true;
}

}
}
