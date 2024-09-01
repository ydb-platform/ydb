#pragma once

#include <ydb/library/yql/ast/yql_ast.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

namespace NSQLTranslation {

struct TTranslationSettings;

} // NSQLTranslation

namespace NSQLTranslationPG {

NYql::TAstParseResult PGToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TStmtParseInfo* stmtParseInfo = nullptr);
TVector<NYql::TAstParseResult> PGToYqlStatements(const TString& query, const NSQLTranslation::TTranslationSettings& settings, TVector<NYql::TStmtParseInfo>* stmtParseInfo = nullptr);
std::unique_ptr<NYql::NPg::IExtensionSqlParser> CreateExtensionSqlParser();
std::unique_ptr<NYql::NPg::ISystemFunctionsParser> CreateSystemFunctionsParser();
std::unique_ptr<NYql::NPg::ISqlLanguageParser> CreateSqlLanguageParser();

} // NSQLTranslationPG
