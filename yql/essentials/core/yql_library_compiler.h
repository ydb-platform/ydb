#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include "yql_type_annotation.h"
#include <yql/essentials/sql/sql.h>

namespace NYql {

bool OptimizeLibrary(TLibraryCohesion& cohesion, TExprContext& ctx);
bool CompileLibrary(const NSQLTranslation::TTranslators& translators, const TString& alias,
    const TString& script, TExprContext& ctx, TLibraryCohesion& cohesion, bool optimize = true);

bool LinkLibraries(THashMap<TString, TLibraryCohesion>& libs, TExprContext& ctx, TExprContext& ctxToClone, const std::function<const TExportTable*(const TString&)>& module2ExportTable);
bool LinkLibraries(THashMap<TString, TLibraryCohesion>& libs, TExprContext& ctx, TExprContext& ctxToClone, const TModulesTable* loadedModules = nullptr);

bool CompileLibraries(const NSQLTranslation::TTranslators& translators, const TUserDataTable& userData,
    TExprContext& ctx, TModulesTable& modules, bool optimize = true);
//FIXME remove
bool CompileLibraries(const TUserDataTable& userData, TExprContext& ctx, TModulesTable& modules, bool optimize = true);
}
