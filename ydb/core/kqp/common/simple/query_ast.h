#pragma once

#include <ydb/library/yql/ast/yql_ast.h>

#include <util/generic/maybe.h>

namespace NKikimr::NKqp {

struct TQueryAst {
    TQueryAst(std::shared_ptr<NYql::TAstParseResult> ast, const TMaybe<ui16>& sqlVersion, const TMaybe<bool>& deprecatedSQL,
        bool keepInCache, const TMaybe<TString>& commandTagName)
        : Ast(std::move(ast))
        , SqlVersion(sqlVersion)
        , DeprecatedSQL(deprecatedSQL)
        , KeepInCache(keepInCache)
        , CommandTagName(commandTagName) {}

    std::shared_ptr<NYql::TAstParseResult> Ast;
    TMaybe<ui16> SqlVersion;
    TMaybe<bool> DeprecatedSQL;
    bool KeepInCache;
    TMaybe<TString> CommandTagName;
};

} // namespace NKikimr::NKqp
