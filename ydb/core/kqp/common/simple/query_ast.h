#pragma once

#include <ydb/library/yql/ast/yql_ast.h>

#include <util/generic/maybe.h>

namespace NKikimr::NKqp {

struct TQueryAst {
    TQueryAst(std::shared_ptr<NYql::TAstParseResult> ast, const TMaybe<ui16>& sqlVersion, const TMaybe<bool>& deprecatedSQL)
        : Ast(std::move(ast))
        , SqlVersion(sqlVersion)
        , DeprecatedSQL(deprecatedSQL) {}

    std::shared_ptr<NYql::TAstParseResult> Ast;
    TMaybe<ui16> SqlVersion;
    TMaybe<bool> DeprecatedSQL;
};

} // namespace NKikimr::NKqp
