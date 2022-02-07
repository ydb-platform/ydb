#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include "yql_type_annotation.h"

namespace NYql {

bool OptimizeLibrary(TLibraryCohesion& cohesion, TExprContext& ctx);
bool CompileLibrary(const TString& alias, const TString& script, TExprContext& ctx, TLibraryCohesion& cohesion, bool optimize = true);

bool LinkLibraries(THashMap<TString, TLibraryCohesion>& libs, TExprContext& ctx, TExprContext& ctxToClone, const std::function<const TExportTable*(const TString&)>& module2ExportTable);
bool LinkLibraries(THashMap<TString, TLibraryCohesion>& libs, TExprContext& ctx, TExprContext& ctxToClone, const TModulesTable* loadedModules = nullptr);

bool CompileLibraries(const TUserDataTable& userData, TExprContext& ctx, TModulesTable& modules, bool optimize = true);
}
