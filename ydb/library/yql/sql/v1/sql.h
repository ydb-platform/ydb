#pragma once

#include <ydb/library/yql/ast/yql_ast.h>
#include <ydb/library/yql/parser/lexer_common/hints.h>
#include <ydb/library/yql/parser/proto_ast/proto_ast.h>
#include <ydb/library/yql/public/issue/yql_warning.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>

#include <google/protobuf/message.h>

namespace NSQLTranslation {
    struct TTranslationSettings;
}

namespace NSQLTranslationV1 {

    NYql::TAstParseResult SqlToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules = nullptr);
    NYql::TAstParseResult SqlASTToYql(const google::protobuf::Message& protoAst, const NSQLTranslation::TSQLHints& hints, const NSQLTranslation::TTranslationSettings& settings);
    TVector<NYql::TAstParseResult> SqlToAstStatements(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TIssues& issues,
        NYql::TWarningRules* warningRules, TVector<NYql::TStmtParseInfo>* stmtParseInfo = nullptr);

}  // namespace NSQLTranslationV1
