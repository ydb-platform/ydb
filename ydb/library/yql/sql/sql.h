#pragma once

#include <ydb/library/yql/parser/lexer_common/hints.h>
#include <ydb/library/yql/parser/lexer_common/lexer.h>
#include <ydb/library/yql/parser/proto_ast/proto_ast.h>
#include <ydb/library/yql/public/issue/yql_warning.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>
#include <ydb/library/yql/ast/yql_ast.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <google/protobuf/message.h>

namespace NSQLTranslation {

    NYql::TAstParseResult SqlToYql(const TString& query, const TTranslationSettings& settings,
        NYql::TWarningRules* warningRules = nullptr, NYql::TStmtParseInfo* stmtParseInfo = nullptr);
    google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, NYql::TIssues& issues, size_t maxErrors,
        const TTranslationSettings& settings = {}, ui16* actualSyntaxVersion = nullptr);
    ILexer::TPtr SqlLexer(const TString& query, NYql::TIssues& issues, const TTranslationSettings& settings = {}, ui16* actualSyntaxVersion = nullptr);
    NYql::TAstParseResult SqlASTToYql(const google::protobuf::Message& protoAst, const TSQLHints& hints, const TTranslationSettings& settings);
    TVector<NYql::TAstParseResult> SqlToAstStatements(const TString& query, const TTranslationSettings& settings, NYql::TIssues& issues,
        NYql::TWarningRules* warningRules = nullptr, ui16* actualSyntaxVersion = nullptr, TVector<NYql::TStmtParseInfo>* stmtParseInfo = nullptr);

}  // namespace NSQLTranslationV0
